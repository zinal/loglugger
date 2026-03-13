package client

import (
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
