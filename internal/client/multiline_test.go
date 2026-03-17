package client

import (
	"testing"
	"time"

	"github.com/ydb-platform/loglugger/internal/models"
)

func TestNewMultilineMergerDisabledWithoutMessageRegex(t *testing.T) {
	merger, err := NewMultilineMerger("", time.Second, 1000)
	if err != nil {
		t.Fatalf("NewMultilineMerger() error = %v", err)
	}
	if merger != nil {
		t.Fatal("expected nil merger when regex is empty")
	}
}

func TestMultilineMergerSplitsOnNextParsedMessage(t *testing.T) {
	merger, err := NewMultilineMerger(`^(?P<P_LEVEL>[A-Z]+): (?P<P_MESSAGE>.*)$`, time.Second, 1000)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(10, 0)
	out := merger.Add(&JournalEntry{
		Record:   models.Record{Message: "INFO: first"},
		Position: "p0",
		Cursor:   "c1",
	}, now)
	if len(out) != 0 {
		t.Fatalf("unexpected output len = %d", len(out))
	}

	out = merger.Add(&JournalEntry{
		Record:   models.Record{Message: "trace line"},
		Position: "c1",
		Cursor:   "c2",
	}, now.Add(50*time.Millisecond))
	if len(out) != 0 {
		t.Fatalf("unexpected output len = %d", len(out))
	}

	out = merger.Add(&JournalEntry{
		Record:   models.Record{Message: "ERROR: second"},
		Position: "c2",
		Cursor:   "c3",
	}, now.Add(100*time.Millisecond))
	if len(out) != 1 {
		t.Fatalf("output len = %d, want 1", len(out))
	}
	if out[0].Record.Message != "INFO: first\ntrace line" {
		t.Fatalf("merged message = %q", out[0].Record.Message)
	}
	if out[0].Position != "p0" || out[0].Cursor != "c2" {
		t.Fatalf("positions = (%q, %q), want (p0, c2)", out[0].Position, out[0].Cursor)
	}

	rest := merger.Drain()
	if rest == nil || rest.Record.Message != "ERROR: second" {
		t.Fatalf("unexpected pending drain: %+v", rest)
	}
}

func TestMultilineMergerTimeoutFlushesPending(t *testing.T) {
	merger, err := NewMultilineMerger(`^INFO:.*$`, time.Second, 1000)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(10, 0)
	_ = merger.Add(&JournalEntry{Record: models.Record{Message: "INFO: first"}, Position: "p0", Cursor: "c1"}, now)
	_ = merger.Add(&JournalEntry{Record: models.Record{Message: "continuation"}, Position: "c1", Cursor: "c2"}, now.Add(10*time.Millisecond))

	if got := merger.DrainExpired(now.Add(900 * time.Millisecond)); got != nil {
		t.Fatalf("DrainExpired() before timeout = %+v, want nil", got)
	}

	got := merger.DrainExpired(now.Add(2 * time.Second))
	if got == nil {
		t.Fatal("expected pending message after timeout")
	}
	if got.Record.Message != "INFO: first\ncontinuation" {
		t.Fatalf("merged message = %q", got.Record.Message)
	}
}

func TestMultilineMergerSplitsWhenLimitReached(t *testing.T) {
	merger, err := NewMultilineMerger(`^INFO:.*$`, time.Second, 2)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(10, 0)
	_ = merger.Add(&JournalEntry{Record: models.Record{Message: "INFO: first"}, Position: "p0", Cursor: "c1"}, now)
	_ = merger.Add(&JournalEntry{Record: models.Record{Message: "cont-1"}, Position: "c1", Cursor: "c2"}, now.Add(10*time.Millisecond))
	out := merger.Add(&JournalEntry{Record: models.Record{Message: "cont-2"}, Position: "c2", Cursor: "c3"}, now.Add(20*time.Millisecond))
	if len(out) != 1 {
		t.Fatalf("output len = %d, want 1", len(out))
	}
	if out[0].Record.Message != "INFO: first\ncont-1" {
		t.Fatalf("first merged message = %q", out[0].Record.Message)
	}

	pending := merger.Drain()
	if pending == nil || pending.Record.Message != "cont-2" {
		t.Fatalf("unexpected pending: %+v", pending)
	}
}
