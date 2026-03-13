package client

import (
	"strings"
	"testing"

	"github.com/ydb-platform/loglugger/internal/models"
)

func TestBatcher_Flush(t *testing.T) {
	b := NewBatcher(3, 0).(*batcher)
	if b == nil {
		t.Fatal("NewBatcher returned wrong type")
	}

	// Add 2 entries
	b.Add(&JournalEntry{Record: models.Record{Message: "a"}, Position: "p1", Cursor: "p1"})
	b.Add(&JournalEntry{Record: models.Record{Message: "b"}, Position: "p2", Cursor: "p2"})

	batch := b.Flush()
	if batch == nil {
		t.Fatal("Flush returned nil")
	}
	if len(batch.Records) != 2 {
		t.Errorf("records = %d, want 2", len(batch.Records))
	}
	if batch.CurrentPosition != "p1" || batch.NextPosition != "p2" {
		t.Errorf("positions = %q, %q", batch.CurrentPosition, batch.NextPosition)
	}

	// Flush empty returns nil
	if b.Flush() != nil {
		t.Error("Flush empty should return nil")
	}
}

func TestBatcher_ShouldFlush(t *testing.T) {
	b := NewBatcher(2, 0).(*batcher)
	if b.ShouldFlush() {
		t.Error("should not flush with 0 entries")
	}
	b.Add(&JournalEntry{Record: models.Record{}, Position: "p1", Cursor: "p1"})
	if b.ShouldFlush() {
		t.Error("should not flush with 1 entry")
	}
	b.Add(&JournalEntry{Record: models.Record{}, Position: "p2", Cursor: "p2"})
	if !b.ShouldFlush() {
		t.Error("should flush with 2 entries")
	}
}

func TestBatcher_ShouldFlushByLogDataSize(t *testing.T) {
	b := NewBatcher(100, 0).(*batcher)
	b.maxDataBytes = 10

	b.Add(&JournalEntry{Record: models.Record{Message: "12345"}, Position: "p1", Cursor: "p1"})
	if b.ShouldFlush() {
		t.Fatal("should not flush before reaching byte limit")
	}
	b.Add(&JournalEntry{Record: models.Record{Message: "12345"}, Position: "p2", Cursor: "p2"})
	if !b.ShouldFlush() {
		t.Fatal("expected flush when byte limit is reached")
	}
}

func TestBatcher_FlushSplitsByLogDataSize(t *testing.T) {
	b := NewBatcher(100, 0).(*batcher)
	b.maxDataBytes = 10

	b.Add(&JournalEntry{Record: models.Record{Message: "123456"}, Position: "p1", Cursor: "p1"})
	b.Add(&JournalEntry{Record: models.Record{Message: "123456"}, Position: "p2", Cursor: "p2"})

	first := b.Flush()
	if first == nil {
		t.Fatal("expected first flush result")
	}
	if len(first.Records) != 1 || first.CurrentPosition != "p1" || first.NextPosition != "p1" {
		t.Fatalf("unexpected first batch: %+v", first)
	}

	second := b.Flush()
	if second == nil {
		t.Fatal("expected second flush result")
	}
	if len(second.Records) != 1 || second.CurrentPosition != "p2" || second.NextPosition != "p2" {
		t.Fatalf("unexpected second batch: %+v", second)
	}
}

func TestBatcher_AllowsSingleOversizedRecord(t *testing.T) {
	b := NewBatcher(10, 0).(*batcher)
	b.maxDataBytes = 10

	b.Add(&JournalEntry{
		Record:   models.Record{Message: strings.Repeat("a", 11)},
		Position: "p1",
		Cursor:   "p1",
	})
	if !b.ShouldFlush() {
		t.Fatal("expected flush signal for oversized single record")
	}
	batch := b.Flush()
	if batch == nil {
		t.Fatal("expected oversized single record to flush")
	}
	if len(batch.Records) != 1 || batch.CurrentPosition != "p1" || batch.NextPosition != "p1" {
		t.Fatalf("unexpected batch: %+v", batch)
	}
}
