//go:build linux

package client

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"syscall"
	"testing"
)

func TestIsJournalCorruptionError(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "EBADMSG", err: syscall.EBADMSG, want: true},
		{name: "wrapped EBADMSG", err: fmt.Errorf("get entry: %w", syscall.EBADMSG), want: true},
		{name: "parse field", err: errors.New("failed to parse field"), want: false},
		{name: "wrapped parse field", err: fmt.Errorf("get entry: %w", errors.New("failed to parse field")), want: false},
		{name: "canceled", err: context.Canceled, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isJournalCorruptionError(tc.err); got != tc.want {
				t.Fatalf("isJournalCorruptionError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestJournalReaderAckRestampsAndAdvances(t *testing.T) {
	r := &journalReader{acked: "p0", last: "p0"}

	skippedShape := &JournalEntry{Position: "stale", Cursor: "c-skip"}
	// Simulate parser skip: do not Ack. Read cursor may move independently.
	r.last = "c-skip"
	if r.acked != "p0" {
		t.Fatalf("acked changed without Ack: %q", r.acked)
	}

	accepted := &JournalEntry{Position: "stale-pending", Cursor: "c2"}
	r.Ack(accepted)
	if accepted.Position != "p0" {
		t.Fatalf("Ack Position = %q, want p0", accepted.Position)
	}
	if r.acked != "c2" {
		t.Fatalf("acked = %q, want c2", r.acked)
	}

	synthetic := &JournalEntry{Position: "x", Cursor: ""}
	r.Ack(synthetic)
	if synthetic.Position != "c2" {
		t.Fatalf("synthetic Position = %q, want c2", synthetic.Position)
	}
	if r.acked != "c2" {
		t.Fatalf("acked advanced on empty Cursor: %q", r.acked)
	}
	_ = skippedShape
}

func TestRecoveryJournalEntryIncludesMessage(t *testing.T) {
	entry := newRecoveryJournalEntry("p1", 1000)
	if entry == nil {
		t.Fatal("newRecoveryJournalEntry() returned nil")
	}
	if entry.Record.Message == "" {
		t.Fatal("recovery journal entry must include a message")
	}
	if entry.Record.SeqNo == nil || *entry.Record.SeqNo <= 0 {
		t.Fatalf("SeqNo = %v, want positive seqno", entry.Record.SeqNo)
	}
	if len(entry.Record.Fields) != 0 {
		t.Fatalf("Fields = %+v, want no duplicated top-level fields", entry.Record.Fields)
	}
	uuidLike := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	if !uuidLike.MatchString(entry.Record.SyslogIdentifier) {
		t.Fatalf("SyslogIdentifier = %q, want UUIDv4-like format", entry.Record.SyslogIdentifier)
	}
	if entry.Record.SystemdUnit != "LOGLUGGER" {
		t.Fatalf("SystemdUnit = %q, want LOGLUGGER", entry.Record.SystemdUnit)
	}
}
