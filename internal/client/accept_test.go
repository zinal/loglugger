package client

import (
	"context"
	"testing"

	"github.com/ydb-platform/loglugger/internal/models"
)

// scriptedJournal is an in-memory JournalReader for position-protocol tests.
type scriptedJournal struct {
	entries []*JournalEntry
	idx     int
	acked   string
	last    string
}

func (j *scriptedJournal) SeekToPosition(ctx context.Context, position string) error {
	j.acked = position
	j.last = position
	j.idx = 0
	return nil
}

func (j *scriptedJournal) Next(ctx context.Context) (*JournalEntry, error) {
	if j.idx >= len(j.entries) {
		return nil, nil
	}
	src := j.entries[j.idx]
	j.idx++
	entry := &JournalEntry{
		Record:   src.Record,
		Position: j.acked,
		Cursor:   src.Cursor,
	}
	if src.Cursor != "" {
		j.last = src.Cursor
	}
	return entry, nil
}

func (j *scriptedJournal) Ack(entry *JournalEntry) {
	if entry == nil {
		return
	}
	entry.Position = j.acked
	if entry.Cursor != "" {
		j.acked = entry.Cursor
	}
}

func (j *scriptedJournal) Recover(ctx context.Context) (bool, error) {
	return false, nil
}

func TestAcceptEntrySkipDoesNotAdvanceProtocolPosition(t *testing.T) {
	journal := &scriptedJournal{
		entries: []*JournalEntry{
			{Record: models.Record{Message: "noise"}, Cursor: "c1"},
			{Record: models.Record{Message: "42"}, Cursor: "c2"},
			{Record: models.Record{Message: "also-noise"}, Cursor: "c3"},
			{Record: models.Record{Message: "7"}, Cursor: "c4"},
		},
	}
	if err := journal.SeekToPosition(context.Background(), "p0"); err != nil {
		t.Fatal(err)
	}
	parser, err := NewRecordParser(`^(?P<P_NUM>\d+)$`, NoMatchSkip, "")
	if err != nil {
		t.Fatal(err)
	}
	batcher := NewBatcher(10, 0, "client-1")

	for {
		entry, err := journal.Next(context.Background())
		if err != nil {
			t.Fatalf("Next() error = %v", err)
		}
		if entry == nil {
			break
		}
		if !AcceptEntry(journal, parser, entry) {
			continue
		}
		if err := batcher.Add(entry); err != nil {
			t.Fatalf("Add() error = %v", err)
		}
	}

	batch := batcher.Flush()
	if batch == nil {
		t.Fatal("expected batch with accepted records")
	}
	if len(batch.Records) != 2 {
		t.Fatalf("records = %d, want 2", len(batch.Records))
	}
	if batch.CurrentPosition != "p0" {
		t.Fatalf("CurrentPosition = %q, want p0 (skip must not break continuity)", batch.CurrentPosition)
	}
	if batch.NextPosition != "c4" {
		t.Fatalf("NextPosition = %q, want c4", batch.NextPosition)
	}
	if journal.acked != "c4" {
		t.Fatalf("acked = %q, want c4", journal.acked)
	}
	// Read cursor may advance past skipped entries; protocol ack must not.
	if journal.last != "c4" {
		t.Fatalf("last read cursor = %q, want c4", journal.last)
	}
}

func TestAcceptEntrySkipPreservesPositionAcrossBatchBoundary(t *testing.T) {
	journal := &scriptedJournal{
		entries: []*JournalEntry{
			{Record: models.Record{Message: "1"}, Cursor: "c1"},
			{Record: models.Record{Message: "skip-me"}, Cursor: "c2"},
			{Record: models.Record{Message: "2"}, Cursor: "c3"},
		},
	}
	if err := journal.SeekToPosition(context.Background(), "p0"); err != nil {
		t.Fatal(err)
	}
	parser, err := NewRecordParser(`^(?P<P_NUM>\d+)$`, NoMatchSkip, "")
	if err != nil {
		t.Fatal(err)
	}

	firstBatcher := NewBatcher(1, 0, "client-1")
	entry, err := journal.Next(context.Background())
	if err != nil || entry == nil {
		t.Fatalf("Next() = %v, %v", entry, err)
	}
	if !AcceptEntry(journal, parser, entry) {
		t.Fatal("expected first entry to be accepted")
	}
	if err := firstBatcher.Add(entry); err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	first := firstBatcher.Flush()
	if first == nil {
		t.Fatal("expected first batch")
	}
	if first.CurrentPosition != "p0" || first.NextPosition != "c1" {
		t.Fatalf("first batch positions = %q, %q; want p0, c1", first.CurrentPosition, first.NextPosition)
	}

	// Skip the non-matching entry between batches.
	skipped, err := journal.Next(context.Background())
	if err != nil || skipped == nil {
		t.Fatalf("Next() skipped = %v, %v", skipped, err)
	}
	if AcceptEntry(journal, parser, skipped) {
		t.Fatal("expected skip for non-matching entry")
	}
	if journal.acked != "c1" {
		t.Fatalf("acked after skip = %q, want c1", journal.acked)
	}

	secondBatcher := NewBatcher(1, 0, "client-1")
	next, err := journal.Next(context.Background())
	if err != nil || next == nil {
		t.Fatalf("Next() next = %v, %v", next, err)
	}
	if !AcceptEntry(journal, parser, next) {
		t.Fatal("expected matching entry after skip to be accepted")
	}
	if err := secondBatcher.Add(next); err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	second := secondBatcher.Flush()
	if second == nil {
		t.Fatal("expected second batch")
	}
	if second.CurrentPosition != "c1" {
		t.Fatalf("second batch CurrentPosition = %q, want c1 (server expected next after first batch)", second.CurrentPosition)
	}
	if second.NextPosition != "c3" {
		t.Fatalf("second batch NextPosition = %q, want c3", second.NextPosition)
	}
}

func TestAcceptEntrySendRawStillAcks(t *testing.T) {
	journal := &scriptedJournal{
		entries: []*JournalEntry{
			{Record: models.Record{Message: "not-a-number"}, Cursor: "c1"},
		},
	}
	if err := journal.SeekToPosition(context.Background(), "p0"); err != nil {
		t.Fatal(err)
	}
	parser, err := NewRecordParser(`^(?P<P_NUM>\d+)$`, NoMatchSendRaw, "")
	if err != nil {
		t.Fatal(err)
	}
	entry, err := journal.Next(context.Background())
	if err != nil || entry == nil {
		t.Fatalf("Next() = %v, %v", entry, err)
	}
	if !AcceptEntry(journal, parser, entry) {
		t.Fatal("send_raw must accept non-matching messages")
	}
	if entry.Position != "p0" {
		t.Fatalf("Position = %q, want p0", entry.Position)
	}
	if journal.acked != "c1" {
		t.Fatalf("acked = %q, want c1", journal.acked)
	}
}

func TestAcceptEntryNilParserAcks(t *testing.T) {
	journal := &scriptedJournal{
		entries: []*JournalEntry{
			{Record: models.Record{Message: "raw"}, Cursor: "c1"},
		},
	}
	if err := journal.SeekToPosition(context.Background(), "p0"); err != nil {
		t.Fatal(err)
	}
	entry, _ := journal.Next(context.Background())
	if !AcceptEntry(journal, nil, entry) {
		t.Fatal("nil parser must accept entry")
	}
	if journal.acked != "c1" {
		t.Fatalf("acked = %q, want c1", journal.acked)
	}
}

func TestAcceptEntryMultilineSkipKeepsNextBatchCurrent(t *testing.T) {
	// Simulate multiline merger output: a merged non-matching message ending at c2,
	// then a matching message at c3. Skip must leave the next accepted entry with
	// Position equal to the seek/ack base (p0), not the skipped merge cursor.
	journal := &scriptedJournal{acked: "p0", last: "p0"}
	parser, err := NewRecordParser(`^INFO:`, NoMatchSkip, "")
	if err != nil {
		t.Fatal(err)
	}

	merged := &JournalEntry{
		Record:   models.Record{Message: "continuation\nmore"},
		Position: "stale",
		Cursor:   "c2",
	}
	if AcceptEntry(journal, parser, merged) {
		t.Fatal("expected merged non-matching entry to be skipped")
	}
	if journal.acked != "p0" {
		t.Fatalf("acked after multiline skip = %q, want p0", journal.acked)
	}

	next := &JournalEntry{
		Record:   models.Record{Message: "INFO: ok"},
		Position: "stale-from-pending",
		Cursor:   "c3",
	}
	if !AcceptEntry(journal, parser, next) {
		t.Fatal("expected matching entry to be accepted")
	}
	if next.Position != "p0" {
		t.Fatalf("Position after skip = %q, want p0 (Ack must restamp pending Position)", next.Position)
	}
	if journal.acked != "c3" {
		t.Fatalf("acked = %q, want c3", journal.acked)
	}
}

func TestSeekToPositionResetsAcked(t *testing.T) {
	journal := &scriptedJournal{
		entries: []*JournalEntry{
			{Record: models.Record{Message: "1"}, Cursor: "c1"},
		},
		acked: "old",
		last:  "old",
	}
	if err := journal.SeekToPosition(context.Background(), "expected"); err != nil {
		t.Fatal(err)
	}
	if journal.acked != "expected" || journal.last != "expected" {
		t.Fatalf("after seek acked/last = %q/%q, want expected/expected", journal.acked, journal.last)
	}
	entry, _ := journal.Next(context.Background())
	if !AcceptEntry(journal, nil, entry) {
		t.Fatal("accept failed")
	}
	if entry.Position != "expected" {
		t.Fatalf("Position = %q, want expected", entry.Position)
	}
}
