package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/ydb-platform/loglugger/internal/buildinfo"
	"github.com/ydb-platform/loglugger/internal/client"
	"github.com/ydb-platform/loglugger/internal/models"
	"gopkg.in/yaml.v3"
)

type clientConfig struct {
	ServerURL            string        `json:"server_url" yaml:"server_url"`
	ServerURLs           []string      `json:"server_urls" yaml:"server_urls"`
	ClientID             string        `json:"client_id" yaml:"client_id"`
	ServiceMask          string        `json:"service_mask" yaml:"service_mask"`
	JournalNamespace     string        `json:"journal_namespace" yaml:"journal_namespace"`
	JournalRecovery      bool          `json:"journal_recovery" yaml:"journal_recovery"`
	MessageRegex         string        `json:"message_regex" yaml:"message_regex"`
	SystemdUnitRegex     string        `json:"systemd_unit_regex" yaml:"systemd_unit_regex"`
	MessageNoMatch       string        `json:"message_regex_no_match" yaml:"message_regex_no_match"`
	MultilineTimeout     time.Duration `json:"multiline_timeout" yaml:"multiline_timeout"`
	MultilineMaxMessages int           `json:"multiline_max_messages" yaml:"multiline_max_messages"`
	Debug                bool          `json:"debug" yaml:"debug"`
	BatchSize            int           `json:"batch_size" yaml:"batch_size"`
	BatchTimeout         time.Duration `json:"batch_timeout" yaml:"batch_timeout"`
	HTTPTimeout          time.Duration `json:"http_timeout" yaml:"http_timeout"`
	RetryDelay           time.Duration `json:"retry_delay" yaml:"retry_delay"`
	TLSCAFile            string        `json:"tls_ca_file" yaml:"tls_ca_file"`
	TLSCertFile          string        `json:"tls_cert_file" yaml:"tls_cert_file"`
	TLSKeyFile           string        `json:"tls_key_file" yaml:"tls_key_file"`
	TLSUseSystemPool     bool          `json:"tls_use_system_pool" yaml:"tls_use_system_pool"`
}

func main() {
	cfg, err := parseClientConfig()
	if err != nil {
		slog.Error("parse client config", "error", err)
		os.Exit(1)
	}
	setupClientLogger(cfg.Debug)
	slog.Debug("startup server order after shuffle", "shuffled_servers", strings.Join(cfg.ServerURLs, ","))

	if cfg.ClientID == "" {
		hostname, _ := os.Hostname()
		cfg.ClientID = hostname
	}
	slog.Info("starting client", "version", buildinfo.Version, "client_id", cfg.ClientID, "servers", strings.Join(cfg.ServerURLs, ","), "debug", cfg.Debug)

	tlsConfig, err := buildClientTLSConfig(cfg)
	if err != nil {
		slog.Error("load TLS config", "error", err)
		os.Exit(1)
	}

	journal, err := client.NewJournalReader(client.JournalConfig{
		ServiceMask:      cfg.ServiceMask,
		JournalNamespace: cfg.JournalNamespace,
	})
	if err != nil {
		slog.Error("open journal", "error", err)
		os.Exit(1)
	}
	defer func() {
		if c, ok := journal.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	batcher := client.NewBatcher(cfg.BatchSize, cfg.BatchTimeout, cfg.ClientID)
	seqnoGenerator := client.NewSeqNoGenerator(time.Now())
	sender := client.NewSender(client.SenderConfig{
		ServerURLs:  cfg.ServerURLs,
		ClientID:    cfg.ClientID,
		HTTPTimeout: cfg.HTTPTimeout,
		RetryDelay:  cfg.RetryDelay,
		TLSConfig:   tlsConfig,
	})
	parser, err := buildRecordParser(cfg)
	if err != nil {
		slog.Error("create client parser", "error", err)
		os.Exit(1)
	}
	multilineMerger, err := buildMultilineMerger(cfg)
	if err != nil {
		slog.Error("create multiline merger", "error", err)
		os.Exit(1)
	}
	slog.Info("client parser settings",
		"enabled", parser != nil,
		"message_regex", strings.TrimSpace(cfg.MessageRegex),
		"systemd_unit_regex", strings.TrimSpace(cfg.SystemdUnitRegex),
		"message_regex_no_match", strings.TrimSpace(cfg.MessageNoMatch),
		"multiline_enabled", multilineMerger != nil,
		"multiline_timeout", cfg.MultilineTimeout,
		"multiline_max_messages", cfg.MultilineMaxMessages,
	)

	ctx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()

	position, reset, err := fetchStartupPosition(ctx, sender)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			slog.Info("shutting down")
			return
		}
		slog.Error("fetch startup position", "error", err)
		os.Exit(1)
	}
	if err := journal.SeekToPosition(ctx, position); err != nil {
		slog.Warn("unable to seek to stored journal position; resetting to head", "position", position, "error", err)
		reset = true
		if err := journal.SeekToPosition(ctx, ""); err != nil {
			slog.Error("seek head", "error", err)
			os.Exit(1)
		}
	}

	flushTicker := time.NewTicker(cfg.BatchTimeout)
	defer flushTicker.Stop()
	idleTimer := time.NewTimer(100 * time.Millisecond)
	defer idleTimer.Stop()
	stopIdleTimer := func() {
		if !idleTimer.Stop() {
			select {
			case <-idleTimer.C:
			default:
			}
		}
	}
	const (
		resumeLogMinEmptyReads = 10
		resumeLogMinEmptyFor   = 2 * time.Second
	)
	emptyReads := 0
	var emptySince time.Time
	var journalReadFailSince time.Time
	processEntry := func(sendCtx context.Context, entry *client.JournalEntry) error {
		if entry == nil {
			return nil
		}
		if !client.AcceptEntry(journal, parser, entry) {
			// Parser skip must not advance the acknowledged protocol position;
			// journal.Next already moved the read cursor past this entry.
			return nil
		}
		seqno := seqnoGenerator.Next()
		entry.Record.SeqNo = &seqno
		if err := batcher.Add(entry); err != nil {
			return err
		}

		if batcher.ShouldFlush() {
			if batch := batcher.Flush(); batch != nil {
				slog.Debug("flush by batch limit", "records", len(batch.Records), "current_position", batch.CurrentPosition, "next_position", batch.NextPosition, "reset", reset)
				var sendErr error
				var streamRestarted bool
				reset, streamRestarted, sendErr = sendBatch(sendCtx, journal, sender, batch, reset)
				if sendErr != nil {
					return sendErr
				}
				if streamRestarted {
					discardUnsentBuffers(batcher, multilineMerger)
				}
			}
		}
		return nil
	}
	drainExpiredMultiline := func(now time.Time) error {
		if multilineMerger == nil {
			return nil
		}
		if ready := multilineMerger.DrainExpired(now); ready != nil {
			return processEntry(ctx, ready)
		}
		return nil
	}
	stopOnSendFailure := func(err error) {
		slog.Error("send batch failed; stopping to avoid dropping the flushed batch", "error", err)
		os.Exit(1)
	}
	shutdown := func() {
		// Final flush must not inherit the long-lived run context: on journal
		// fail-stop that context is still open, and Sender retries forever while
		// the server is down — blocking os.Exit and supervisor restart. Use a
		// dedicated deadline so shutdown always completes.
		flushCtx, cancelFlush := newShutdownFlushContext()
		defer cancelFlush()
		if multilineMerger != nil {
			if err := processEntry(flushCtx, multilineMerger.Drain()); err != nil {
				stopOnSendFailure(err)
			}
		}
		if batch := batcher.Flush(); batch != nil {
			slog.Debug("flush on shutdown", "records", len(batch.Records), "current_position", batch.CurrentPosition, "next_position", batch.NextPosition, "reset", reset)
			if _, streamRestarted, err := sendBatch(flushCtx, journal, sender, batch, reset); err != nil {
				stopOnSendFailure(err)
			} else if streamRestarted {
				discardUnsentBuffers(batcher, multilineMerger)
			}
		}
		slog.Info("shutting down")
	}
	flushByTimeout := func() {
		if err := drainExpiredMultiline(time.Now()); err != nil {
			stopOnSendFailure(err)
		}
		if batch := batcher.Flush(); batch != nil {
			slog.Debug("flush by timeout", "records", len(batch.Records), "current_position", batch.CurrentPosition, "next_position", batch.NextPosition, "reset", reset)
			var sendErr error
			var streamRestarted bool
			reset, streamRestarted, sendErr = sendBatch(ctx, journal, sender, batch, reset)
			if sendErr != nil {
				stopOnSendFailure(sendErr)
			} else if streamRestarted {
				discardUnsentBuffers(batcher, multilineMerger)
			}
		}
	}

	// Avoid select{..., default:} around journal.Next: under a continuous
	// journal stream that pattern busy-polls and races the flush ticker.
	// batch_timeout is enforced by Batcher.ShouldFlush while reading; the
	// ticker only wakes idle waits (no new entries) and multiline expiry.
	for {
		entry, err := journal.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				shutdown()
				return
			}
			if isJournalCorruption(err) {
				journalReadFailSince = time.Time{}
				recoveredReset, recoveryErr := recoverFromJournalCorruption(ctx, journal, cfg.JournalRecovery)
				if recoveryErr != nil {
					slog.Error(recoveryErr.Error(), "error", err)
					os.Exit(1)
				}
				reset = finishJournalCorruptionRecovery(reset, recoveredReset)
				// Recovery resumes after the last read cursor. Keep unsent
				// buffers and flush what is already batched with reset:true;
				// discarding here would permanently drop records journald will
				// not re-emit (unlike a §4.3 position-mismatch reseek).
				if batch := batcher.Flush(); batch != nil {
					var sendErr error
					var streamRestarted bool
					reset, streamRestarted, sendErr = sendBatch(ctx, journal, sender, batch, reset)
					if sendErr != nil {
						stopOnSendFailure(sendErr)
					} else if streamRestarted {
						discardUnsentBuffers(batcher, multilineMerger)
					}
				}
				continue
			}
			if journalReadFailSince.IsZero() {
				journalReadFailSince = time.Now()
			}
			failingFor := time.Since(journalReadFailSince)
			slog.Error("read journal", "error", err, "failing_for", failingFor)
			if journalReadFailureBudgetExceeded(failingFor) {
				// Fail-stop so a process supervisor can restart/alert; endless
				// 100ms retries leave the client "alive" while shipping stalls.
				slog.Error("persistent journal read failures; stopping", "error", err, "failing_for", failingFor)
				shutdown()
				os.Exit(1)
			}
			select {
			case <-ctx.Done():
				shutdown()
				return
			case <-time.After(journalReadRetryDelay):
			}
			continue
		}
		journalReadFailSince = time.Time{}
		if entry == nil {
			emptyReads++
			if emptyReads == 1 {
				emptySince = time.Now()
			}
			if emptyReads%100 == 0 {
				slog.Debug("journal has no new entries", "empty_reads", emptyReads)
			}
			// Flush partial batches whose batch_timeout already elapsed while
			// we were reading; do not wait for the next ticker under load gaps.
			if batcher.ShouldFlush() {
				flushByTimeout()
				continue
			}
			if err := drainExpiredMultiline(time.Now()); err != nil {
				stopOnSendFailure(err)
			}
			// Block until flush tick, short idle poll, or shutdown. journal.Next
			// already waited briefly for appends; avoid a select/default spin.
			stopIdleTimer()
			idleTimer.Reset(100 * time.Millisecond)
			select {
			case <-ctx.Done():
				shutdown()
				return
			case <-flushTicker.C:
				flushByTimeout()
			case <-idleTimer.C:
			}
			continue
		}
		if emptyReads > 0 {
			emptyFor := time.Duration(0)
			if !emptySince.IsZero() {
				emptyFor = time.Since(emptySince)
			}
			if emptyReads >= resumeLogMinEmptyReads || emptyFor >= resumeLogMinEmptyFor {
				attrs := []any{"empty_reads", emptyReads}
				if emptyFor > 0 {
					attrs = append(attrs, "empty_for", emptyFor)
				}
				slog.Debug("journal resumed with new entries", attrs...)
			}
			emptyReads = 0
			emptySince = time.Time{}
		}

		if multilineMerger != nil {
			ready := multilineMerger.Add(entry, time.Now())
			for _, merged := range ready {
				if err := processEntry(ctx, merged); err != nil {
					stopOnSendFailure(err)
				}
			}
			continue
		}
		if err := processEntry(ctx, entry); err != nil {
			stopOnSendFailure(err)
		}
	}
}

func parseClientConfig() (clientConfig, error) {
	configPath := flag.String("config", "", "Path to client YAML/JSON config file")
	flag.Parse()
	if strings.TrimSpace(*configPath) == "" {
		return clientConfig{}, fmt.Errorf("config file is required (-config)")
	}
	cfg := defaultClientConfig()
	if err := loadClientConfigFile(*configPath, &cfg); err != nil {
		return clientConfig{}, err
	}
	cfg.ServerURLs = normalizeConfiguredServerURLs(cfg.ServerURLs, cfg.ServerURL)
	if err := validateClientConfig(cfg); err != nil {
		return clientConfig{}, err
	}
	cfg.ServerURLs = shuffleServerURLs(cfg.ServerURLs)
	return cfg, nil
}

// validateClientConfig rejects misconfigured numeric/timing fields and unsafe
// server URLs before any of them are used or logged.
func validateClientConfig(cfg clientConfig) error {
	if len(cfg.ServerURLs) == 0 {
		return fmt.Errorf("at least one server URL is required (server_url/server_urls)")
	}
	if cfg.BatchSize <= 0 {
		return fmt.Errorf("batch_size must be greater than zero")
	}
	if cfg.BatchTimeout <= 0 {
		return fmt.Errorf("batch_timeout must be greater than zero")
	}
	if cfg.HTTPTimeout <= 0 {
		return fmt.Errorf("http_timeout must be greater than zero")
	}
	if cfg.RetryDelay <= 0 {
		return fmt.Errorf("retry_delay must be greater than zero")
	}
	for _, raw := range cfg.ServerURLs {
		if err := validateServerURL(raw); err != nil {
			return err
		}
	}
	return nil
}

func validateServerURL(raw string) error {
	serverURL, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("invalid server URL %q: %w", raw, err)
	}
	if serverURL.Scheme != "https" {
		return fmt.Errorf("server URL must use https: %q", raw)
	}
	if serverURL.Hostname() == "" {
		return fmt.Errorf("server URL must include host name: %q", raw)
	}
	if serverURL.User != nil {
		// Do not echo the URL: it may contain a password that would otherwise
		// end up in process logs via the parse-config error path.
		return fmt.Errorf("server URL must not include userinfo")
	}
	return nil
}

const (
	journalReadRetryDelay         = 100 * time.Millisecond
	maxJournalReadFailureDuration = 15 * time.Second
	shutdownFlushTimeout          = 10 * time.Second
)

// newShutdownFlushContext returns a context for the final batch flush on
// shutdown. It is independent of the run context so a cancelled signal context
// or an still-open fail-stop path cannot leave Sender retrying forever.
func newShutdownFlushContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), shutdownFlushTimeout)
}

// journalReadFailureBudgetExceeded reports whether non-corruption journal read
// errors have persisted long enough that the client should stop rather than
// retry forever while appearing healthy.
func journalReadFailureBudgetExceeded(failingFor time.Duration) bool {
	return failingFor >= maxJournalReadFailureDuration
}

func isJournalCorruption(err error) bool {
	return errors.Is(err, syscall.EBADMSG)
}

func recoverFromJournalCorruption(ctx context.Context, journal client.JournalReader, enabled bool) (bool, error) {
	if !enabled {
		return false, fmt.Errorf("journal corruption detected; stopping. Enable journal_recovery in client config to attempt best-effort recovery with possible data loss")
	}
	slog.Warn("journal corruption detected; attempting best-effort recovery, some data loss is possible")
	reset, err := journal.Recover(ctx)
	if err != nil {
		return false, fmt.Errorf("journal corruption recovery is not possible; stopping: %w", err)
	}
	return reset, nil
}

func setupClientLogger(debug bool) {
	level := slog.LevelInfo
	if debug {
		level = slog.LevelDebug
	}
	handler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: level,
	})
	slog.SetDefault(slog.New(handler))
}

func defaultClientConfig() clientConfig {
	return clientConfig{
		BatchSize:            50000,
		BatchTimeout:         5 * time.Second,
		HTTPTimeout:          30 * time.Second,
		RetryDelay:           time.Second,
		MessageNoMatch:       string(client.NoMatchSendRaw),
		MultilineTimeout:     time.Second,
		MultilineMaxMessages: 1000,
	}
}

func loadClientConfigFile(path string, cfg *clientConfig) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config file: %w", err)
	}
	switch strings.ToLower(filepath.Ext(path)) {
	case ".json":
		if err := json.Unmarshal(data, cfg); err != nil {
			return fmt.Errorf("decode JSON config file: %w", err)
		}
	default:
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return fmt.Errorf("decode YAML config file: %w", err)
		}
	}
	return nil
}

func normalizeConfiguredServerURLs(serverURLs []string, serverURL string) []string {
	out := make([]string, 0, len(serverURLs)+1)
	for _, raw := range serverURLs {
		out = append(out, parseServerURLs(raw)...)
	}
	out = append(out, parseServerURLs(serverURL)...)
	return out
}

func buildRecordParser(cfg clientConfig) (client.MessageParser, error) {
	action := client.NoMatchAction(strings.TrimSpace(cfg.MessageNoMatch))
	if action == "" {
		action = client.NoMatchSendRaw
	}
	if action != client.NoMatchSendRaw && action != client.NoMatchSkip {
		return nil, fmt.Errorf("message_regex_no_match must be send_raw or skip")
	}
	return client.NewRecordParser(strings.TrimSpace(cfg.MessageRegex), action, strings.TrimSpace(cfg.SystemdUnitRegex))
}

func buildMultilineMerger(cfg clientConfig) (*client.MultilineMerger, error) {
	if strings.TrimSpace(cfg.MessageRegex) == "" {
		return nil, nil
	}
	if cfg.MultilineTimeout <= 0 {
		return nil, fmt.Errorf("multiline_timeout must be greater than zero")
	}
	if cfg.MultilineMaxMessages <= 0 {
		return nil, fmt.Errorf("multiline_max_messages must be greater than zero")
	}
	return client.NewMultilineMerger(cfg.MessageRegex, cfg.MultilineTimeout, cfg.MultilineMaxMessages)
}

func buildClientTLSConfig(cfg clientConfig) (*tls.Config, error) {
	if len(cfg.ServerURLs) == 0 {
		return nil, fmt.Errorf("at least one server URL is required")
	}
	for _, raw := range cfg.ServerURLs {
		if err := validateServerURL(raw); err != nil {
			return nil, err
		}
	}
	tlsCfg, err := client.LoadClientTLSConfig(cfg.TLSCAFile, cfg.TLSCertFile, cfg.TLSKeyFile, cfg.TLSUseSystemPool)
	if err != nil {
		return nil, err
	}
	return tlsCfg, nil
}

func parseServerURLs(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		out = append(out, trimmed)
	}
	return out
}

func shuffleServerURLs(urls []string) []string {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	return shuffleServerURLsWith(urls, rng.Shuffle)
}

func shuffleServerURLsWith(urls []string, shuffle func(n int, swap func(i, j int))) []string {
	shuffled := append([]string(nil), urls...)
	if len(shuffled) < 2 {
		return shuffled
	}
	shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})
	return shuffled
}

func fetchStartupPosition(ctx context.Context, sender client.Sender) (string, bool, error) {
	resp, err := sender.CurrentPosition(ctx)
	if err != nil {
		return "", false, err
	}
	if resp == nil {
		return "", false, fmt.Errorf("position lookup returned empty response")
	}
	if resp.Status == "not_found" {
		slog.Debug("startup position not found; client reset required")
		return "", true, nil
	}
	if resp.Status != "ok" {
		if resp.Message != "" {
			return "", false, fmt.Errorf("position lookup failed: %s", resp.Message)
		}
		return "", false, fmt.Errorf("position lookup failed: unexpected status %q", resp.Status)
	}
	if resp.CurrentPosition == "" {
		return "", false, fmt.Errorf("position lookup returned empty current_position")
	}
	slog.Debug("startup position fetched", "current_position", resp.CurrentPosition)
	return resp.CurrentPosition, false, nil
}

// finishJournalCorruptionRecovery updates protocol state after journal.Recover
// succeeds. Unsent batcher/multiline buffers must be retained by the caller:
// recovery resumes after the last successfully read cursor, so clearing those
// buffers would drop records that journald will not re-emit.
func finishJournalCorruptionRecovery(reset bool, recoveredReset bool) bool {
	if recoveredReset {
		return true
	}
	return reset
}

// discardUnsentBuffers drops leftover batcher/multiline state after a
// position-mismatch stream restart (HTTP 409 reseek/reset to expected_position
// or head). Partial JSON/count flushes can leave records whose CurrentPosition
// still reflects the pre-mismatch stream; sending them next would repeat 409s
// or attach stale records to a reset batch. Do not use after journal corruption
// recovery — that path must retain unsent buffers (see finishJournalCorruptionRecovery).
func discardUnsentBuffers(batcher client.Batcher, multiline *client.MultilineMerger) {
	if batcher != nil {
		batcher.Clear()
	}
	if multiline != nil {
		multiline.Discard()
	}
}

// sendBatch submits a flushed batch. On success it returns the updated reset flag.
// streamRestarted is true when the journal was reseeked or reset after a position
// mismatch; the caller must discard any remaining unsent local buffers.
// Transient failures are retried inside Sender; a returned error is non-retryable
// (typically HTTP 4xx) and the caller must stop so the flushed batch is not skipped.
func sendBatch(ctx context.Context, journal client.JournalReader, sender client.Sender, batch *client.Batch, reset bool) (bool, bool, error) {
	slog.Debug("sending batch",
		"messages", len(batch.Records),
		"message_bytes", totalMessageBytes(batch.Records),
		"current_position", batch.CurrentPosition,
		"next_position", batch.NextPosition,
		"reset", reset,
	)
	resp, err := sender.SendBatch(ctx, batch, reset)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			slog.Info("send interrupted", "error", err)
			return reset, false, nil
		}
		return reset, false, fmt.Errorf("non-retryable send failure: %w", err)
	}
	slog.Debug("batch response received", "status", resp.Status, "expected_position", resp.ExpectedPosition, "message", resp.Message)
	if resp.Status == "position_mismatch" {
		slog.Info("server reported position mismatch",
			"current_position", batch.CurrentPosition,
			"next_position", batch.NextPosition,
			"expected_position", resp.ExpectedPosition,
		)
		if resp.ExpectedPosition != "" {
			if err := journal.SeekToPosition(ctx, resp.ExpectedPosition); err == nil {
				return false, true, nil
			}
			slog.Warn("unable to seek to expected journal position; resetting to head", "expected_position", resp.ExpectedPosition)
		}
		if err := journal.SeekToPosition(ctx, ""); err != nil {
			// Do not report streamRestarted: the journal was not repositioned, so
			// the caller must not discard buffers and continue from an unknown offset.
			return reset, false, fmt.Errorf("seek head after position mismatch failed: %w", err)
		}
		return true, true, nil
	}
	if resp.Status != "ok" {
		// Defense in depth: Sender should already fail non-actionable responses,
		// but never treat an unexpected status as an accepted batch.
		msg := resp.Message
		if msg == "" {
			msg = fmt.Sprintf("unexpected batch status %q", resp.Status)
		}
		return reset, false, fmt.Errorf("non-retryable send failure: %w", client.ErrClientError{Message: msg})
	}
	return false, false, nil
}

func totalMessageBytes(records []models.Record) int {
	total := 0
	for _, record := range records {
		total += len(record.Message)
	}
	return total
}
