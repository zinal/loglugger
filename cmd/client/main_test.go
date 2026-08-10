package main

import (
	"context"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"syscall"
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
	if reset {
		t.Fatal("expected reset=false on cancellation error")
	}
}

func TestFetchStartupPositionPropagatesLookupError(t *testing.T) {
	position, reset, err := fetchStartupPosition(context.Background(), stubSender{
		err: client.ErrClientError{Message: "bad request"},
	})
	if err == nil {
		t.Fatal("expected lookup error")
	}
	if position != "" {
		t.Fatalf("position = %q, want empty", position)
	}
	if reset {
		t.Fatal("expected reset=false on lookup error")
	}
}

func TestFetchStartupPositionRejectsMalformedOKResponse(t *testing.T) {
	position, reset, err := fetchStartupPosition(context.Background(), stubSender{
		positionResp: &models.PositionResponse{Status: "ok"},
	})
	if err == nil {
		t.Fatal("expected malformed response error")
	}
	if position != "" {
		t.Fatalf("position = %q, want empty", position)
	}
	if reset {
		t.Fatal("expected reset=false on malformed response")
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

	reset, streamRestarted, err := sendBatch(context.Background(), journal, sender, &client.Batch{
		CurrentPosition: "cursor-10",
		NextPosition:    "cursor-11",
	}, false)
	if err != nil {
		t.Fatalf("sendBatch() error = %v", err)
	}

	if reset {
		t.Fatal("expected reset=false after successful reseek")
	}
	if !streamRestarted {
		t.Fatal("expected streamRestarted=true after mismatch reseek")
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

	reset, streamRestarted, err := sendBatch(context.Background(), journal, sender, &client.Batch{
		CurrentPosition: "cursor-10",
		NextPosition:    "cursor-11",
	}, false)
	if err != nil {
		t.Fatalf("sendBatch() error = %v", err)
	}

	if !reset {
		t.Fatal("expected reset=true when reseek fails")
	}
	if !streamRestarted {
		t.Fatal("expected streamRestarted=true after mismatch reset")
	}
	if len(journal.seekCalls) != 2 {
		t.Fatalf("seek calls = %v, want reseek then head", journal.seekCalls)
	}
	if journal.seekCalls[1] != "" {
		t.Fatalf("second seek = %q, want head reset", journal.seekCalls[1])
	}
}

func TestSendBatchFailsWhenFallbackHeadSeekFails(t *testing.T) {
	journal := &stubJournalReader{failPositions: map[string]bool{
		"cursor-42": true,
		"":          true,
	}}
	sender := stubSender{
		resp: &models.BatchResponse{
			Status:           "position_mismatch",
			ExpectedPosition: "cursor-42",
		},
	}

	reset, streamRestarted, err := sendBatch(context.Background(), journal, sender, &client.Batch{
		CurrentPosition: "cursor-10",
		NextPosition:    "cursor-11",
	}, false)
	if err == nil {
		t.Fatal("expected error when fallback head seek fails")
	}
	if !strings.Contains(err.Error(), "seek head after position mismatch failed") {
		t.Fatalf("error = %q, want head-seek failure", err)
	}
	if reset {
		t.Fatal("expected incoming reset preserved when stream was not restarted")
	}
	if streamRestarted {
		t.Fatal("expected streamRestarted=false when journal was not repositioned")
	}
	if len(journal.seekCalls) != 2 {
		t.Fatalf("seek calls = %v, want reseek then head", journal.seekCalls)
	}
	if journal.seekCalls[0] != "cursor-42" || journal.seekCalls[1] != "" {
		t.Fatalf("seek calls = %v, want [cursor-42 \"\"]", journal.seekCalls)
	}
}

func TestSendBatchFailsWhenHeadSeekFailsWithoutExpectedPosition(t *testing.T) {
	journal := &stubJournalReader{failPositions: map[string]bool{"": true}}
	sender := stubSender{
		resp: &models.BatchResponse{
			Status: "position_mismatch",
		},
	}

	_, streamRestarted, err := sendBatch(context.Background(), journal, sender, &client.Batch{
		CurrentPosition: "cursor-10",
		NextPosition:    "cursor-11",
	}, true)
	if err == nil {
		t.Fatal("expected error when head seek fails")
	}
	if streamRestarted {
		t.Fatal("expected streamRestarted=false when journal was not repositioned")
	}
	if len(journal.seekCalls) != 1 || journal.seekCalls[0] != "" {
		t.Fatalf("seek calls = %v, want [\"\"]", journal.seekCalls)
	}
}

func TestSendBatchReturnsErrorOnUnexpectedStatus(t *testing.T) {
	journal := &stubJournalReader{}
	sender := stubSender{
		resp: &models.BatchResponse{
			Status:  "error",
			Message: "gateway returned 409 with garbage body",
		},
	}

	reset, streamRestarted, err := sendBatch(context.Background(), journal, sender, &client.Batch{
		CurrentPosition: "cursor-10",
		NextPosition:    "cursor-11",
		Records:         []models.Record{{Message: "must-not-skip"}},
	}, false)
	if err == nil {
		t.Fatal("expected non-retryable send failure for unexpected status")
	}
	if !strings.Contains(err.Error(), "non-retryable send failure") {
		t.Fatalf("error = %v, want non-retryable send failure wrapper", err)
	}
	var clientErr client.ErrClientError
	if !errors.As(err, &clientErr) {
		t.Fatalf("error = %v, want ErrClientError via errors.As", err)
	}
	if clientErr.Message != "gateway returned 409 with garbage body" {
		t.Fatalf("client error message = %q", clientErr.Message)
	}
	if reset {
		t.Fatal("expected original reset=false to be preserved on failure")
	}
	if streamRestarted {
		t.Fatal("expected streamRestarted=false on unexpected status")
	}
	if len(journal.seekCalls) != 0 {
		t.Fatalf("seek calls = %v, want none on unexpected status", journal.seekCalls)
	}
}

func TestSendBatchReturnsErrorOnClientError(t *testing.T) {
	journal := &stubJournalReader{}
	sender := stubSender{
		err: client.ErrClientError{Message: "current_position is required when reset is false"},
	}

	reset, streamRestarted, err := sendBatch(context.Background(), journal, sender, &client.Batch{
		CurrentPosition: "cursor-10",
		NextPosition:    "cursor-11",
		Records:         []models.Record{{Message: "lost-if-continued"}},
	}, false)
	if err == nil {
		t.Fatal("expected non-retryable send failure")
	}
	if !strings.Contains(err.Error(), "non-retryable send failure") {
		t.Fatalf("error = %v, want non-retryable send failure wrapper", err)
	}
	var clientErr client.ErrClientError
	if !errors.As(err, &clientErr) {
		t.Fatalf("error = %v, want ErrClientError via errors.As", err)
	}
	if clientErr.Message != "current_position is required when reset is false" {
		t.Fatalf("client error message = %q", clientErr.Message)
	}
	if reset {
		t.Fatal("expected original reset=false to be preserved on failure")
	}
	if streamRestarted {
		t.Fatal("expected streamRestarted=false on client error")
	}
	if len(journal.seekCalls) != 0 {
		t.Fatalf("seek calls = %v, want none on client error", journal.seekCalls)
	}
}

func TestSendBatchReturnsErrorOnGenericNonRetryableFailure(t *testing.T) {
	journal := &stubJournalReader{}
	sender := stubSender{err: errors.New("HTTP 404: not found")}

	_, streamRestarted, err := sendBatch(context.Background(), journal, sender, &client.Batch{
		CurrentPosition: "cursor-10",
		NextPosition:    "cursor-11",
	}, true)
	if err == nil {
		t.Fatal("expected non-retryable send failure")
	}
	if !strings.Contains(err.Error(), "non-retryable send failure") {
		t.Fatalf("error = %v, want non-retryable send failure wrapper", err)
	}
	if streamRestarted {
		t.Fatal("expected streamRestarted=false on non-retryable failure")
	}
}

func TestSendBatchIgnoresContextCancellation(t *testing.T) {
	journal := &stubJournalReader{}
	sender := stubSender{err: context.Canceled}

	reset, streamRestarted, err := sendBatch(context.Background(), journal, sender, &client.Batch{
		CurrentPosition: "cursor-10",
		NextPosition:    "cursor-11",
	}, true)
	if err != nil {
		t.Fatalf("sendBatch() error = %v, want nil on cancellation", err)
	}
	if !reset {
		t.Fatal("expected reset flag preserved on interruption")
	}
	if streamRestarted {
		t.Fatal("expected streamRestarted=false on interruption")
	}
}

func TestSendBatchIgnoresContextDeadlineExceeded(t *testing.T) {
	journal := &stubJournalReader{}
	sender := stubSender{err: context.DeadlineExceeded}

	reset, streamRestarted, err := sendBatch(context.Background(), journal, sender, &client.Batch{
		CurrentPosition: "cursor-10",
		NextPosition:    "cursor-11",
	}, true)
	if err != nil {
		t.Fatalf("sendBatch() error = %v, want nil on deadline exceeded", err)
	}
	if !reset {
		t.Fatal("expected reset flag preserved on interruption")
	}
	if streamRestarted {
		t.Fatal("expected streamRestarted=false on interruption")
	}
}

func TestDiscardUnsentBuffersClearsBatcherRemainderAndMultiline(t *testing.T) {
	// maxSize=1 forces a partial flush remainder, the same class of leftover
	// state JSON-size splits leave behind on 409/reseek.
	batcher := client.NewBatcher(1, 0, "c")
	if err := batcher.Add(&client.JournalEntry{
		Record: models.Record{Message: "first"}, Position: "p1", Cursor: "p1",
	}); err != nil {
		t.Fatal(err)
	}
	if err := batcher.Add(&client.JournalEntry{
		Record: models.Record{Message: "second"}, Position: "p2", Cursor: "p2",
	}); err != nil {
		t.Fatal(err)
	}
	first := batcher.Flush()
	if first == nil || len(first.Records) != 1 {
		t.Fatalf("first flush = %+v, want 1 record", first)
	}
	if !batcher.ShouldFlush() {
		t.Fatal("remainder should still be flushable before discard")
	}

	merger, err := client.NewMultilineMerger(`^START`, time.Second, 100)
	if err != nil {
		t.Fatal(err)
	}
	if out := merger.Add(&client.JournalEntry{
		Record: models.Record{Message: "START pending"}, Cursor: "c-pending",
	}, time.Unix(1, 0)); len(out) != 0 {
		t.Fatalf("unexpected multiline output: %+v", out)
	}

	discardUnsentBuffers(batcher, merger)
	if batcher.ShouldFlush() {
		t.Fatal("batcher remainder must be cleared after stream restart")
	}
	if batch := batcher.Flush(); batch != nil {
		t.Fatalf("Flush() after Clear = %+v, want nil", batch)
	}
	if drained := merger.Drain(); drained != nil {
		t.Fatalf("multiline pending survived Discard: %+v", drained)
	}
}

func TestFinishJournalCorruptionRecoveryRetainsUnsentBuffers(t *testing.T) {
	// Corruption recovery must not call discardUnsentBuffers: Recover resumes
	// after journalReader.last, so buffered records would never be re-read.
	batcher := client.NewBatcher(10, 0, "c")
	if err := batcher.Add(&client.JournalEntry{
		Record: models.Record{Message: "buffered"}, Position: "p0", Cursor: "c10",
	}); err != nil {
		t.Fatal(err)
	}
	merger, err := client.NewMultilineMerger(`^START`, time.Second, 100)
	if err != nil {
		t.Fatal(err)
	}
	if out := merger.Add(&client.JournalEntry{
		Record: models.Record{Message: "START pending"}, Cursor: "c-pending",
	}, time.Unix(1, 0)); len(out) != 0 {
		t.Fatalf("unexpected multiline output: %+v", out)
	}

	reset := finishJournalCorruptionRecovery(false, true)
	if !reset {
		t.Fatal("recovery must force reset=true for the next send")
	}
	// finishJournalCorruptionRecovery only updates reset; callers must not
	// discardUnsentBuffers. The buffered record and multiline pending remain.
	batch := batcher.Flush()
	if batch == nil || len(batch.Records) != 1 || batch.NextPosition != "c10" {
		t.Fatalf("retained flush = %+v, want buffered record through c10", batch)
	}
	if drained := merger.Drain(); drained == nil || drained.Cursor != "c-pending" {
		t.Fatalf("multiline pending must be retained after recovery: %+v", drained)
	}
}

func TestNewShutdownFlushContextHasDeadline(t *testing.T) {
	if shutdownFlushTimeout != 10*time.Second {
		t.Fatalf("shutdownFlushTimeout = %v, want 10s", shutdownFlushTimeout)
	}
	ctx, cancel := newShutdownFlushContext()
	defer cancel()
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("shutdown flush context has no deadline")
	}
	remaining := time.Until(deadline)
	if remaining < 9*time.Second || remaining > shutdownFlushTimeout {
		t.Fatalf("deadline remaining = %v, want about %v", remaining, shutdownFlushTimeout)
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
		ServerURLs:       []string{"https://localhost:27312"},
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

func TestBuildClientTLSConfigRejectsUserinfo(t *testing.T) {
	_, err := buildClientTLSConfig(clientConfig{
		ServerURLs:       []string{"https://user:pass@localhost:27312"},
		TLSUseSystemPool: true,
	})
	if err == nil {
		t.Fatal("expected error for server URL with userinfo")
	}
	if !strings.Contains(err.Error(), "userinfo") {
		t.Fatalf("error = %v, want userinfo mention", err)
	}
	if strings.Contains(err.Error(), "pass") {
		t.Fatalf("error = %v, must not echo password from URL", err)
	}
}

func TestValidateClientConfigRejectsInvalidTimingAndBatchFields(t *testing.T) {
	valid := defaultClientConfig()
	valid.ServerURLs = []string{"https://localhost:27312"}

	tests := []struct {
		name    string
		mutate  func(*clientConfig)
		wantErr string
	}{
		{
			name: "zero batch_size",
			mutate: func(cfg *clientConfig) {
				cfg.BatchSize = 0
			},
			wantErr: "batch_size must be greater than zero",
		},
		{
			name: "negative batch_timeout",
			mutate: func(cfg *clientConfig) {
				cfg.BatchTimeout = -time.Second
			},
			wantErr: "batch_timeout must be greater than zero",
		},
		{
			name: "zero http_timeout",
			mutate: func(cfg *clientConfig) {
				cfg.HTTPTimeout = 0
			},
			wantErr: "http_timeout must be greater than zero",
		},
		{
			name: "negative retry_delay",
			mutate: func(cfg *clientConfig) {
				cfg.RetryDelay = -time.Millisecond
			},
			wantErr: "retry_delay must be greater than zero",
		},
		{
			name: "url with userinfo",
			mutate: func(cfg *clientConfig) {
				cfg.ServerURLs = []string{"https://user:secret@localhost:27312"}
			},
			wantErr: "userinfo",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := valid
			tt.mutate(&cfg)
			err := validateClientConfig(cfg)
			if err == nil {
				t.Fatal("expected validation error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want substring %q", err, tt.wantErr)
			}
		})
	}
}

func TestParseClientConfigRejectsNonPositiveBatchTimeout(t *testing.T) {
	prev := flag.CommandLine
	prevArgs := os.Args
	defer func() {
		flag.CommandLine = prev
		os.Args = prevArgs
	}()
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)

	configPath := writeTempClientConfig(t, `
server_url: https://localhost:27312
batch_timeout: -1s
`)
	os.Args = []string{"client", "-config", configPath}
	_, err := parseClientConfig()
	if err == nil {
		t.Fatal("expected error for negative batch_timeout")
	}
	if !strings.Contains(err.Error(), "batch_timeout must be greater than zero") {
		t.Fatalf("error = %v, want batch_timeout validation", err)
	}
}

func TestParseServerURLs(t *testing.T) {
	got := parseServerURLs(" https://a:27312,https://b:27312 , , https://c:27312 ")
	want := []string{"https://a:27312", "https://b:27312", "https://c:27312"}
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

	configPath := writeTempClientConfig(t, `
server_urls:
  - https://a:27312
  - https://b:27312
`)
	os.Args = []string{"client", "-config", configPath}
	cfg, err := parseClientConfig()
	if err != nil {
		t.Fatalf("parseClientConfig() error = %v", err)
	}
	want := []string{"https://a:27312", "https://b:27312"}
	if !sameStringsIgnoringOrder(cfg.ServerURLs, want) {
		t.Fatalf("ServerURLs = %v, want %v", cfg.ServerURLs, want)
	}
}

func TestParseClientConfigParsesJournalNamespace(t *testing.T) {
	prev := flag.CommandLine
	prevArgs := os.Args
	defer func() {
		flag.CommandLine = prev
		os.Args = prevArgs
	}()
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)

	configPath := writeTempClientConfig(t, "server_url: https://localhost:27312\njournal_namespace: my_namespace\n")
	os.Args = []string{"client", "-config", configPath}
	cfg, err := parseClientConfig()
	if err != nil {
		t.Fatalf("parseClientConfig() error = %v", err)
	}
	if cfg.JournalNamespace != "my_namespace" {
		t.Fatalf("JournalNamespace = %q, want my_namespace", cfg.JournalNamespace)
	}
}

func TestParseClientConfigParsesJournalRecovery(t *testing.T) {
	prev := flag.CommandLine
	prevArgs := os.Args
	defer func() {
		flag.CommandLine = prev
		os.Args = prevArgs
	}()
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)

	configPath := writeTempClientConfig(t, "server_url: https://localhost:27312\njournal_recovery: true\n")
	os.Args = []string{"client", "-config", configPath}
	cfg, err := parseClientConfig()
	if err != nil {
		t.Fatalf("parseClientConfig() error = %v", err)
	}
	if !cfg.JournalRecovery {
		t.Fatal("JournalRecovery = false, want true")
	}
}

func TestParseClientConfigLoadsDurationsAndBatchSize(t *testing.T) {
	prev := flag.CommandLine
	prevArgs := os.Args
	defer func() {
		flag.CommandLine = prev
		os.Args = prevArgs
	}()
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)

	configPath := writeTempClientConfig(t, `
server_url: https://a:27312,https://b:27312
debug: true
batch_size: 123
batch_timeout: 7s
multiline_timeout: 2s
multiline_max_messages: 42
`)
	os.Args = []string{"client", "-config", configPath}
	cfg, err := parseClientConfig()
	if err != nil {
		t.Fatalf("parseClientConfig() error = %v", err)
	}
	if !cfg.Debug {
		t.Fatal("Debug = false, want true")
	}
	if cfg.BatchSize != 123 {
		t.Fatalf("BatchSize = %d, want 123", cfg.BatchSize)
	}
	if cfg.BatchTimeout != 7*time.Second {
		t.Fatalf("BatchTimeout = %v, want 7s", cfg.BatchTimeout)
	}
	if cfg.MultilineTimeout != 2*time.Second {
		t.Fatalf("MultilineTimeout = %v, want 2s", cfg.MultilineTimeout)
	}
	if cfg.MultilineMaxMessages != 42 {
		t.Fatalf("MultilineMaxMessages = %d, want 42", cfg.MultilineMaxMessages)
	}
	want := []string{"https://a:27312", "https://b:27312"}
	if !sameStringsIgnoringOrder(cfg.ServerURLs, want) {
		t.Fatalf("ServerURLs = %v, want %v", cfg.ServerURLs, want)
	}
}

func TestShuffleServerURLsWith(t *testing.T) {
	input := []string{"https://a:27312", "https://b:27312", "https://c:27312"}
	got := shuffleServerURLsWith(input, func(n int, swap func(i, j int)) {
		if n != len(input) {
			t.Fatalf("shuffle n = %d, want %d", n, len(input))
		}
		swap(0, 2)
	})
	want := []string{"https://c:27312", "https://b:27312", "https://a:27312"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("shuffleServerURLsWith() = %v, want %v", got, want)
	}
	if !reflect.DeepEqual(input, []string{"https://a:27312", "https://b:27312", "https://c:27312"}) {
		t.Fatalf("input mutated = %v", input)
	}
}

func TestBuildRecordParserRejectsInvalidNoMatchAction(t *testing.T) {
	_, err := buildRecordParser(clientConfig{
		MessageRegex:   `^(?P<P_LEVEL>[A-Z]+): (?P<P_MESSAGE>.*)$`,
		MessageNoMatch: "drop",
	})
	if err == nil {
		t.Fatal("expected invalid no-match action error")
	}
}

func TestBuildMultilineMergerDisabledWithoutMessageRegex(t *testing.T) {
	merger, err := buildMultilineMerger(clientConfig{
		MultilineTimeout:     time.Second,
		MultilineMaxMessages: 1000,
	})
	if err != nil {
		t.Fatalf("buildMultilineMerger() error = %v", err)
	}
	if merger != nil {
		t.Fatal("expected nil merger when message_regex is empty")
	}
}

func TestBuildMultilineMergerValidatesSettings(t *testing.T) {
	_, err := buildMultilineMerger(clientConfig{
		MessageRegex:         `^INFO:.*$`,
		MultilineTimeout:     0,
		MultilineMaxMessages: 1000,
	})
	if err == nil {
		t.Fatal("expected timeout validation error")
	}
	_, err = buildMultilineMerger(clientConfig{
		MessageRegex:         `^INFO:.*$`,
		MultilineTimeout:     time.Second,
		MultilineMaxMessages: 0,
	})
	if err == nil {
		t.Fatal("expected max-messages validation error")
	}
}

func writeTempClientConfig(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "client.yaml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	return path
}

func TestLoadClientConfigFileRejectsUnknownKeys(t *testing.T) {
	t.Parallel()

	path := writeTempClientConfig(t, `
server_url: https://localhost:27312
bath_size: 10
`)
	var cfg clientConfig
	err := loadClientConfigFile(path, &cfg)
	if err == nil {
		t.Fatal("expected error for unknown config key")
	}
	if !strings.Contains(err.Error(), "bath_size") {
		t.Fatalf("error = %v, want mention of bath_size", err)
	}
}

func TestIsJournalCorruption(t *testing.T) {
	if !isJournalCorruption(syscall.EBADMSG) {
		t.Fatal("EBADMSG should be treated as journal corruption")
	}
	if isJournalCorruption(context.Canceled) {
		t.Fatal("context.Canceled should not be treated as journal corruption")
	}
	if isJournalCorruption(nil) {
		t.Fatal("nil error should not be treated as journal corruption")
	}
}

func TestJournalReadFailureBudgetExceeded(t *testing.T) {
	if journalReadFailureBudgetExceeded(0) {
		t.Fatal("zero duration should not exceed the failure budget")
	}
	if journalReadFailureBudgetExceeded(maxJournalReadFailureDuration - time.Millisecond) {
		t.Fatal("duration below budget should not stop the client")
	}
	if !journalReadFailureBudgetExceeded(maxJournalReadFailureDuration) {
		t.Fatal("duration at budget should stop the client")
	}
	if !journalReadFailureBudgetExceeded(maxJournalReadFailureDuration + time.Second) {
		t.Fatal("duration above budget should stop the client")
	}
}

func TestRecoverFromJournalCorruptionDisabled(t *testing.T) {
	_, err := recoverFromJournalCorruption(context.Background(), &stubJournalReader{}, false)
	if err == nil {
		t.Fatal("expected error when recovery is disabled")
	}
	if !strings.Contains(err.Error(), "journal_recovery") {
		t.Fatalf("error = %q, want recovery option hint", err)
	}
}

func TestRecoverFromJournalCorruptionEnabled(t *testing.T) {
	journal := &stubJournalReader{recoverReset: true}
	reset, err := recoverFromJournalCorruption(context.Background(), journal, true)
	if err != nil {
		t.Fatalf("recoverFromJournalCorruption() error = %v", err)
	}
	if !reset {
		t.Fatal("reset = false, want true")
	}
	if journal.recoverCalls != 1 {
		t.Fatalf("recoverCalls = %d, want 1", journal.recoverCalls)
	}
}

func TestRecoverFromJournalCorruptionFailure(t *testing.T) {
	journal := &stubJournalReader{recoverErr: syscall.EBADMSG}
	_, err := recoverFromJournalCorruption(context.Background(), journal, true)
	if err == nil {
		t.Fatal("expected recovery error")
	}
	if !strings.Contains(err.Error(), "recovery is not possible") {
		t.Fatalf("error = %q, want not possible message", err)
	}
}

func sameStringsIgnoringOrder(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	leftCopy := append([]string(nil), left...)
	rightCopy := append([]string(nil), right...)
	sort.Strings(leftCopy)
	sort.Strings(rightCopy)
	return reflect.DeepEqual(leftCopy, rightCopy)
}

type stubSender struct {
	resp         *models.BatchResponse
	positionResp *models.PositionResponse
	err          error
}

func (s stubSender) Send(ctx context.Context, req *models.BatchRequest) (*models.BatchResponse, error) {
	return s.resp, s.err
}

func (s stubSender) SendBatch(ctx context.Context, batch *client.Batch, reset bool) (*models.BatchResponse, error) {
	return s.Send(ctx, &models.BatchRequest{
		Reset:           reset,
		CurrentPosition: batch.CurrentPosition,
		NextPosition:    batch.NextPosition,
		Records:         batch.Records,
	})
}

func (s stubSender) CurrentPosition(ctx context.Context) (*models.PositionResponse, error) {
	return s.positionResp, s.err
}

type stubJournalReader struct {
	seekCalls     []string
	failPositions map[string]bool
	recoverCalls  int
	recoverReset  bool
	recoverErr    error
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

func (r *stubJournalReader) Ack(entry *client.JournalEntry) {}

func (r *stubJournalReader) Recover(ctx context.Context) (bool, error) {
	r.recoverCalls++
	return r.recoverReset, r.recoverErr
}
