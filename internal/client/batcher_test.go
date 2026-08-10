package client

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ydb-platform/loglugger/internal/models"
)

func TestBatcher_Flush(t *testing.T) {
	b := NewBatcher(3, 0, "client-1").(*batcher)
	if b == nil {
		t.Fatal("NewBatcher returned wrong type")
	}

	// Add 2 entries
	if err := b.Add(&JournalEntry{Record: models.Record{Message: "a"}, Position: "p1", Cursor: "p1"}); err != nil {
		t.Fatal(err)
	}
	if err := b.Add(&JournalEntry{Record: models.Record{Message: "b"}, Position: "p2", Cursor: "p2"}); err != nil {
		t.Fatal(err)
	}

	batch := b.Flush()
	if batch == nil {
		t.Fatal("Flush returned nil")
	}
	if len(batch.Records) != 2 {
		t.Errorf("records = %d, want 2", len(batch.Records))
	}
	if len(batch.RecordJSONs) != 2 {
		t.Errorf("recordJSONs = %d, want 2", len(batch.RecordJSONs))
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
	b := NewBatcher(2, 0, "client-1").(*batcher)
	if b.ShouldFlush() {
		t.Error("should not flush with 0 entries")
	}
	if err := b.Add(&JournalEntry{Record: models.Record{}, Position: "p1", Cursor: "p1"}); err != nil {
		t.Fatal(err)
	}
	if b.ShouldFlush() {
		t.Error("should not flush with 1 entry")
	}
	if err := b.Add(&JournalEntry{Record: models.Record{}, Position: "p2", Cursor: "p2"}); err != nil {
		t.Fatal(err)
	}
	if !b.ShouldFlush() {
		t.Error("should flush with 2 entries")
	}
}

func TestBatcher_ShouldFlushByJSONSize(t *testing.T) {
	b := NewBatcher(100, 0, "c").(*batcher)
	b.maxJSONBytes = 120

	if err := b.Add(&JournalEntry{Record: models.Record{Message: "12345"}, Position: "p1", Cursor: "p1"}); err != nil {
		t.Fatal(err)
	}
	if b.ShouldFlush() {
		t.Fatalf("should not flush before reaching JSON byte limit; size=%d", b.bufferedJSONBytes)
	}
	if err := b.Add(&JournalEntry{Record: models.Record{Message: "12345"}, Position: "p2", Cursor: "p2"}); err != nil {
		t.Fatal(err)
	}
	if !b.ShouldFlush() {
		t.Fatalf("expected flush when JSON byte limit is reached; size=%d", b.bufferedJSONBytes)
	}
}

func TestBatcher_ShouldFlushByTimeout(t *testing.T) {
	b := NewBatcher(100, 50*time.Millisecond, "client-1").(*batcher)
	if err := b.Add(&JournalEntry{Record: models.Record{Message: "a"}, Position: "p1", Cursor: "p1"}); err != nil {
		t.Fatal(err)
	}
	if b.ShouldFlush() {
		t.Fatal("should not flush before batch_timeout")
	}
	b.startedAt = time.Now().Add(-b.timeout)
	if !b.ShouldFlush() {
		t.Fatal("expected flush once batch_timeout has elapsed")
	}
	batch := b.Flush()
	if batch == nil {
		t.Fatal("expected timeout flush to produce a batch")
	}
	if b.ShouldFlush() {
		t.Fatal("should not flush after timeout batch was sent")
	}
	if !b.startedAt.IsZero() {
		t.Fatal("startedAt should reset after a full flush")
	}
}

func TestBatcher_FlushSplitsByJSONSize(t *testing.T) {
	b := NewBatcher(100, 0, "c").(*batcher)
	b.maxJSONBytes = 120

	if err := b.Add(&JournalEntry{Record: models.Record{Message: "123456"}, Position: "p1", Cursor: "p1"}); err != nil {
		t.Fatal(err)
	}
	if err := b.Add(&JournalEntry{Record: models.Record{Message: "123456"}, Position: "p2", Cursor: "p2"}); err != nil {
		t.Fatal(err)
	}

	first := b.Flush()
	if first == nil {
		t.Fatal("expected first flush result")
	}
	if len(first.Records) != 1 || first.CurrentPosition != "p1" || first.NextPosition != "p1" {
		t.Fatalf("unexpected first batch: %+v", first)
	}
	assertBatchJSONSize(t, "c", false, first)

	second := b.Flush()
	if second == nil {
		t.Fatal("expected second flush result")
	}
	if len(second.Records) != 1 || second.CurrentPosition != "p2" || second.NextPosition != "p2" {
		t.Fatalf("unexpected second batch: %+v", second)
	}
	assertBatchJSONSize(t, "c", false, second)
}

func TestBatcher_RejectsSingleOversizedRecord(t *testing.T) {
	b := NewBatcher(10, 0, "c").(*batcher)
	b.maxJSONBytes = 80

	err := b.Add(&JournalEntry{
		Record:   models.Record{Message: strings.Repeat("a", 200)},
		Position: "p1",
		Cursor:   "p1",
	})
	if !errors.Is(err, ErrRecordJSONTooLarge) {
		t.Fatalf("Add() error = %v, want ErrRecordJSONTooLarge", err)
	}
	if b.ShouldFlush() {
		t.Fatal("rejected record must not remain buffered")
	}
	if batch := b.Flush(); batch != nil {
		t.Fatalf("Flush() = %+v, want nil after rejected Add", batch)
	}
}

func TestBatcher_RecoveryMessageWaitsForRealCursor(t *testing.T) {
	b := NewBatcher(1, 0, "client-1").(*batcher)

	if err := b.Add(&JournalEntry{Record: models.Record{Message: "synthetic"}, Position: "p1"}); err != nil {
		t.Fatal(err)
	}
	if b.ShouldFlush() {
		t.Fatal("synthetic recovery record alone must not flush")
	}
	if batch := b.Flush(); batch != nil {
		t.Fatalf("Flush() = %+v, want nil until a real cursor-bearing entry arrives", batch)
	}

	if err := b.Add(&JournalEntry{Record: models.Record{Message: "real"}, Position: "p1", Cursor: "p2"}); err != nil {
		t.Fatal(err)
	}
	if !b.ShouldFlush() {
		t.Fatal("expected flush once a real cursor-bearing entry is present")
	}

	batch := b.Flush()
	if batch == nil {
		t.Fatal("expected batch with recovery record and first recovered journal record")
	}
	if len(batch.Records) != 2 {
		t.Fatalf("len(batch.Records) = %d, want 2", len(batch.Records))
	}
	if batch.CurrentPosition != "p1" || batch.NextPosition != "p2" {
		t.Fatalf("positions = %q, %q, want p1, p2", batch.CurrentPosition, batch.NextPosition)
	}
}

func assertBatchJSONSize(t *testing.T, clientID string, reset bool, batch *Batch) {
	t.Helper()
	body, err := encodeBatchRequestJSON(clientID, reset, batch.CurrentPosition, batch.NextPosition, batch.RecordJSONs)
	if err != nil {
		t.Fatal(err)
	}
	want := batchRequestJSONSize(
		jsonStringWireLen(clientID),
		reset,
		batch.CurrentPosition,
		batch.NextPosition,
		recordsArrayJSONBytes(batch.RecordJSONs),
	)
	if want != len(body) {
		t.Fatalf("size estimate = %d, actual JSON = %d (%s)", want, len(body), body)
	}
	var decoded models.BatchRequest
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("encoded batch is not valid BatchRequest JSON: %v", err)
	}
}
