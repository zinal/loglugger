package server

import (
	"context"
	"testing"
	"time"
)

func TestMockWriter_PreservesSeqNoAndTSOrigWhenUpdateOmitsThem(t *testing.T) {
	ctx := context.Background()
	w := NewMockWriter()
	seq := int64(100)
	tsOrig := time.UnixMicro(5000).UTC()

	if err := w.SetPositionUnconditional(ctx, "c1", "p1", PositionUpdate{
		TSWall:    time.Unix(10, 0).UTC(),
		MaxSeqNo:  &seq,
		MaxTSOrig: &tsOrig,
	}); err != nil {
		t.Fatal(err)
	}

	if err := w.SetPosition(ctx, "c1", "p1", "p2", PositionUpdate{
		TSWall: time.Unix(20, 0).UTC(),
	}); err != nil {
		t.Fatal(err)
	}

	got := w.positions["c1"]
	if got.expected != "p2" {
		t.Fatalf("expected = %q, want p2", got.expected)
	}
	if !got.tsWall.Equal(time.Unix(20, 0).UTC()) {
		t.Fatalf("ts_wall = %v, want updated wall clock", got.tsWall)
	}
	if got.seqNo == nil || *got.seqNo != seq {
		t.Fatalf("seqno = %v, want preserved %d", got.seqNo, seq)
	}
	if got.tsOrig == nil || !got.tsOrig.Equal(tsOrig) {
		t.Fatalf("ts_orig = %v, want preserved %v", got.tsOrig, tsOrig)
	}
}

func TestMockWriter_UpdatesSeqNoAndTSOrigWhenPresent(t *testing.T) {
	ctx := context.Background()
	w := NewMockWriter()
	oldSeq := int64(1)
	newSeq := int64(2)
	oldTS := time.UnixMicro(10).UTC()
	newTS := time.UnixMicro(20).UTC()

	if err := w.SetPositionUnconditional(ctx, "c1", "p1", PositionUpdate{
		TSWall:    time.Unix(1, 0).UTC(),
		MaxSeqNo:  &oldSeq,
		MaxTSOrig: &oldTS,
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.SetPositionUnconditional(ctx, "c1", "p2", PositionUpdate{
		TSWall:    time.Unix(2, 0).UTC(),
		MaxSeqNo:  &newSeq,
		MaxTSOrig: &newTS,
	}); err != nil {
		t.Fatal(err)
	}

	got := w.positions["c1"]
	if got.seqNo == nil || *got.seqNo != newSeq {
		t.Fatalf("seqno = %v, want %d", got.seqNo, newSeq)
	}
	if got.tsOrig == nil || !got.tsOrig.Equal(newTS) {
		t.Fatalf("ts_orig = %v, want %v", got.tsOrig, newTS)
	}
}
