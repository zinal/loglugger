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

	batcher := client.NewBatcher(cfg.BatchSize, cfg.BatchTimeout)
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
	emptyReads := 0
	processEntry := func(entry *client.JournalEntry) {
		if entry == nil {
			return
		}
		record := entry.Record
		if parser != nil {
			parsedRecord, ok := parser.Parse(record)
			if !ok {
				return
			}
			record = parsedRecord
		}
		seqno := seqnoGenerator.Next()
		record.SeqNo = &seqno
		entry.Record = record
		batcher.Add(entry)
		slog.Debug("journal entry received", "systemd_unit", entry.Record.SystemdUnit, "cursor", entry.Cursor, "position", entry.Position)

		if batcher.ShouldFlush() {
			if batch := batcher.Flush(); batch != nil {
				slog.Debug("flush by batch limit", "records", len(batch.Records), "current_position", batch.CurrentPosition, "next_position", batch.NextPosition, "reset", reset)
				reset = sendBatch(ctx, journal, sender, batch, reset)
			}
		}
	}
	drainExpiredMultiline := func(now time.Time) {
		if multilineMerger == nil {
			return
		}
		if ready := multilineMerger.DrainExpired(now); ready != nil {
			processEntry(ready)
		}
	}

	for {
		select {
		case <-ctx.Done():
			if multilineMerger != nil {
				processEntry(multilineMerger.Drain())
			}
			if batch := batcher.Flush(); batch != nil {
				slog.Debug("flush on shutdown", "records", len(batch.Records), "current_position", batch.CurrentPosition, "next_position", batch.NextPosition, "reset", reset)
				_ = sendBatch(ctx, journal, sender, batch, reset)
			}
			slog.Info("shutting down")
			return
		case <-flushTicker.C:
			drainExpiredMultiline(time.Now())
			if batch := batcher.Flush(); batch != nil {
				slog.Debug("flush by timeout", "records", len(batch.Records), "current_position", batch.CurrentPosition, "next_position", batch.NextPosition, "reset", reset)
				reset = sendBatch(ctx, journal, sender, batch, reset)
			}
		default:
		}

		entry, err := journal.Next(ctx)
		if err != nil {
			if isJournalCorruption(err) {
				recoveredReset, recoveryErr := recoverFromJournalCorruption(ctx, journal, cfg.JournalRecovery)
				if recoveryErr != nil {
					slog.Error(recoveryErr.Error(), "error", err)
					os.Exit(1)
				}
				if recoveredReset {
					reset = true
				}
				continue
			}
			slog.Error("read journal", "error", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if entry == nil {
			emptyReads++
			if emptyReads == 1 || emptyReads%100 == 0 {
				slog.Debug("journal has no new entries", "empty_reads", emptyReads)
			}
			drainExpiredMultiline(time.Now())
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if emptyReads > 0 {
			slog.Debug("journal resumed with new entries", "empty_reads", emptyReads)
			emptyReads = 0
		}

		if multilineMerger != nil {
			ready := multilineMerger.Add(entry, time.Now())
			for _, merged := range ready {
				processEntry(merged)
			}
			continue
		}
		processEntry(entry)
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
	if len(cfg.ServerURLs) == 0 {
		return clientConfig{}, fmt.Errorf("at least one server URL is required (server_url/server_urls)")
	}
	cfg.ServerURLs = shuffleServerURLs(cfg.ServerURLs)
	return cfg, nil
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
		serverURL, err := url.Parse(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid server URL %q: %w", raw, err)
		}
		if serverURL.Scheme != "https" {
			return nil, fmt.Errorf("server URL must use https: %q", raw)
		}
		if serverURL.Hostname() == "" {
			return nil, fmt.Errorf("server URL must include host name: %q", raw)
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

func sendBatch(ctx context.Context, journal client.JournalReader, sender client.Sender, batch *client.Batch, reset bool) bool {
	slog.Debug("sending batch", "records", len(batch.Records), "current_position", batch.CurrentPosition, "next_position", batch.NextPosition, "reset", reset)
	req := &models.BatchRequest{
		Reset:           reset,
		CurrentPosition: batch.CurrentPosition,
		NextPosition:    batch.NextPosition,
		Records:         batch.Records,
	}
	resp, err := sender.Send(ctx, req)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			slog.Info("send interrupted", "error", err)
			return reset
		}
		slog.Error("send batch", "error", err)
		return reset
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
				return false
			}
			slog.Warn("unable to seek to expected journal position; resetting to head", "expected_position", resp.ExpectedPosition)
		}
		if err := journal.SeekToPosition(ctx, ""); err != nil {
			slog.Error("seek head after mismatch", "error", err)
			return true
		}
		return true
	}
	return false
}
