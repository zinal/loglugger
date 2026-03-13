package main

import (
	"context"
	"flag"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/ydb-platform/loglugger/internal/client"
	"github.com/ydb-platform/loglugger/internal/models"
)

func TestFetchStartupPositionNotFound(t *testing.T) {
	position, reset, err := fetchStartupPosition(context.Background(), stubSender{
		positionResp: &models.PositionResponse{Status: "not_found"},
	})
	if err != nil {
		t.Fatalf("fetchStartupPosition() error = %v", err)
	}
	if position != "" {
		t.Fatalf("position = %q, want empty", position)
	}
	if !reset {
		t.Fatal("expected reset=true when server has no stored position")
	}
}

func TestFetchStartupPositionFound(t *testing.T) {
	position, reset, err := fetchStartupPosition(context.Background(), stubSender{
		positionResp: &models.PositionResponse{Status: "ok", CurrentPosition: "cursor-100"},
	})
	if err != nil {
		t.Fatalf("fetchStartupPosition() error = %v", err)
	}
	if position != "cursor-100" {
		t.Fatalf("position = %q, want cursor-100", position)
	}
	if reset {
		t.Fatal("expected reset=false when server returns stored position")
	}
}

func TestFetchStartupPositionCanceled(t *testing.T) {
	position, reset, err := fetchStartupPosition(context.Background(), stubSender{
		err: context.Canceled,
	})
	if err == nil {
		t.Fatal("expected cancellation error")
	}
	if position != "" {
		t.Fatalf("position = %q, want empty", position)
	}
	if !reset {
		t.Fatal("expected reset=true on cancellation")
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

func TestBuildClientTLSConfigRejectsNonHTTPS(t *testing.T) {
	_, err := buildClientTLSConfig(clientConfig{
		ServerURLs:  []string{"http://localhost:8080"},
		HTTPTimeout: 5 * time.Second,
	})
	if err == nil {
		t.Fatal("expected error for non-https server URL")
	}
}

func TestBuildClientTLSConfigSetsServerNameFromURLHost(t *testing.T) {
	tlsCfg, err := buildClientTLSConfig(clientConfig{
		ServerURLs:       []string{"https://localhost:8443"},
		TLSUseSystemPool: true,
	})
	if err != nil {
		t.Fatalf("buildClientTLSConfig() error = %v", err)
	}
	if tlsCfg == nil {
		t.Fatal("expected non-nil tls config")
	}
}

func TestBuildClientTLSConfigRejectsMissingHost(t *testing.T) {
	_, err := buildClientTLSConfig(clientConfig{
		ServerURLs:       []string{"https://"},
		TLSUseSystemPool: true,
	})
	if err == nil {
		t.Fatal("expected error for missing host")
	}
}

func TestParseServerURLs(t *testing.T) {
	got := parseServerURLs(" https://a:8443,https://b:8443 , , https://c:8443 ")
	want := []string{"https://a:8443", "https://b:8443", "https://c:8443"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("parseServerURLs() = %v, want %v", got, want)
	}
}

func TestParseClientConfigParsesServerList(t *testing.T) {
	prev := flag.CommandLine
	prevArgs := os.Args
	defer func() {
		flag.CommandLine = prev
		os.Args = prevArgs
	}()
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)

	os.Args = []string{
		"client",
		"-server", "https://a:8443,https://b:8443",
	}
	cfg := parseClientConfig()
	want := []string{"https://a:8443", "https://b:8443"}
	if !reflect.DeepEqual(cfg.ServerURLs, want) {
		t.Fatalf("ServerURLs = %v, want %v", cfg.ServerURLs, want)
	}
}

type stubSender struct {
	resp         *models.BatchResponse
	positionResp *models.PositionResponse
	err          error
}

func (s stubSender) Send(ctx context.Context, req *models.BatchRequest) (*models.BatchResponse, error) {
	return s.resp, s.err
}

func (s stubSender) CurrentPosition(ctx context.Context) (*models.PositionResponse, error) {
	return s.positionResp, s.err
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
