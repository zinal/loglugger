package main

import (
	"context"
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

func (r *stubJournalReader) Recover(ctx context.Context) (bool, error) {
	r.recoverCalls++
	return r.recoverReset, r.recoverErr
}
