package main

import (
	"context"
	"testing"

	"github.com/mzinal/loglugger/internal/client"
	"github.com/mzinal/loglugger/internal/models"
)

func TestLoadStoredPositionNilStore(t *testing.T) {
	position, reset := loadStoredPosition(nil)
	if position != "" {
		t.Fatalf("position = %q, want empty", position)
	}
	if !reset {
		t.Fatal("expected reset=true when no position store is configured")
	}
}

func TestSendBatchReseeksOnPositionMismatch(t *testing.T) {
	journal := &stubJournalReader{}
	sender := stubSender{
		resp: &models.BatchResponse{
			Status:           "position_mismatch",
			ExpectedPosition: "cursor-42",
		},
	}

	reset := sendBatch(context.Background(), journal, sender, &client.Batch{
		CurrentPosition: "cursor-10",
		NextPosition:    "cursor-11",
	}, false)

	if reset {
		t.Fatal("expected reset=false after successful reseek")
	}
	if len(journal.seekCalls) != 1 || journal.seekCalls[0] != "cursor-42" {
		t.Fatalf("seek calls = %v, want [cursor-42]", journal.seekCalls)
	}
}

func TestSendBatchFallsBackToResetOnSeekFailure(t *testing.T) {
	journal := &stubJournalReader{failPositions: map[string]bool{"cursor-42": true}}
	sender := stubSender{
		resp: &models.BatchResponse{
			Status:           "position_mismatch",
			ExpectedPosition: "cursor-42",
		},
	}

	reset := sendBatch(context.Background(), journal, sender, &client.Batch{
		CurrentPosition: "cursor-10",
		NextPosition:    "cursor-11",
	}, false)

	if !reset {
		t.Fatal("expected reset=true when reseek fails")
	}
	if len(journal.seekCalls) != 2 {
		t.Fatalf("seek calls = %v, want reseek then head", journal.seekCalls)
	}
	if journal.seekCalls[1] != "" {
		t.Fatalf("second seek = %q, want head reset", journal.seekCalls[1])
	}
}

type stubSender struct {
	resp *models.BatchResponse
	err  error
}

func (s stubSender) Send(ctx context.Context, req *models.BatchRequest) (*models.BatchResponse, error) {
	return s.resp, s.err
}

type stubJournalReader struct {
	seekCalls     []string
	failPositions map[string]bool
}

func (r *stubJournalReader) SeekToPosition(ctx context.Context, position string) error {
	r.seekCalls = append(r.seekCalls, position)
	if r.failPositions[position] {
		return context.DeadlineExceeded
	}
	return nil
}

func (r *stubJournalReader) Next(ctx context.Context) (*client.JournalEntry, error) {
	return nil, nil
}

func (r *stubJournalReader) GetCursor() (string, error) {
	return "", nil
}
