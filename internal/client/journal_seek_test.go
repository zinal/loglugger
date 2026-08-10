package client

import (
	"errors"
	"strings"
	"testing"
)

type fakeCursorJournal struct {
	seekErr   error
	nextN     uint64
	nextErr   error
	testErr   error
	seekCalls int
	nextCalls int
	testCalls int
	lastSeek  string
	lastTest  string
}

func (f *fakeCursorJournal) SeekCursor(cursor string) error {
	f.seekCalls++
	f.lastSeek = cursor
	return f.seekErr
}

func (f *fakeCursorJournal) Next() (uint64, error) {
	f.nextCalls++
	return f.nextN, f.nextErr
}

func (f *fakeCursorJournal) TestCursor(cursor string) error {
	f.testCalls++
	f.lastTest = cursor
	return f.testErr
}

func TestAdvanceToExactCursorSuccess(t *testing.T) {
	j := &fakeCursorJournal{nextN: 1}
	if err := advanceToExactCursor(j, "cursor-1"); err != nil {
		t.Fatalf("advanceToExactCursor() error = %v", err)
	}
	if j.seekCalls != 1 || j.lastSeek != "cursor-1" {
		t.Fatalf("SeekCursor calls=%d last=%q, want 1/cursor-1", j.seekCalls, j.lastSeek)
	}
	if j.nextCalls != 1 {
		t.Fatalf("Next calls=%d, want 1", j.nextCalls)
	}
	if j.testCalls != 1 || j.lastTest != "cursor-1" {
		t.Fatalf("TestCursor calls=%d last=%q, want 1/cursor-1", j.testCalls, j.lastTest)
	}
}

func TestAdvanceToExactCursorRejectsStaleNearestNeighbor(t *testing.T) {
	j := &fakeCursorJournal{
		nextN:   1,
		testErr: errors.New("Cursor parameter is not the same as current position"),
	}
	err := advanceToExactCursor(j, "stale-cursor")
	if err == nil {
		t.Fatal("expected error when TestCursor rejects nearest-neighbor match")
	}
	if !strings.Contains(err.Error(), "no longer valid") {
		t.Fatalf("error = %v, want stale-cursor wording", err)
	}
	if j.testCalls != 1 {
		t.Fatalf("TestCursor calls=%d, want 1", j.testCalls)
	}
}

func TestAdvanceToExactCursorRejectsEmptyJournalAfterSeek(t *testing.T) {
	j := &fakeCursorJournal{nextN: 0}
	err := advanceToExactCursor(j, "missing-cursor")
	if err == nil {
		t.Fatal("expected error when Next returns 0 after SeekCursor")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("error = %v, want not-found wording", err)
	}
	if j.testCalls != 0 {
		t.Fatalf("TestCursor calls=%d, want 0 when Next yields no entry", j.testCalls)
	}
}

func TestAdvanceToExactCursorPropagatesSeekAndNextErrors(t *testing.T) {
	seekErr := errors.New("seek failed")
	if err := advanceToExactCursor(&fakeCursorJournal{seekErr: seekErr}, "c"); !errors.Is(err, seekErr) {
		t.Fatalf("seek error = %v, want wrapped %v", err, seekErr)
	}

	nextErr := errors.New("next failed")
	if err := advanceToExactCursor(&fakeCursorJournal{nextN: 1, nextErr: nextErr}, "c"); !errors.Is(err, nextErr) {
		t.Fatalf("next error = %v, want wrapped %v", err, nextErr)
	}
}
