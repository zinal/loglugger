package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ydb-platform/loglugger/internal/buildinfo"
	"github.com/ydb-platform/loglugger/internal/client"
	"github.com/ydb-platform/loglugger/internal/models"
)

type clientConfig struct {
	ServerURLs       []string
	ClientID         string
	ServiceMask      string
	JournalNamespace string
	JournalRecovery  bool
	Debug            bool
	BatchSize        int
	BatchTimeout     time.Duration
	HTTPTimeout      time.Duration
	RetryDelay       time.Duration
	TLSCAFile        string
	TLSCertFile      string
	TLSKeyFile       string
	TLSUseSystemPool bool
}

func main() {
	cfg := parseClientConfig()
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
	sender := client.NewSender(client.SenderConfig{
		ServerURLs:  cfg.ServerURLs,
		ClientID:    cfg.ClientID,
		HTTPTimeout: cfg.HTTPTimeout,
		RetryDelay:  cfg.RetryDelay,
		TLSConfig:   tlsConfig,
	})

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

	for {
		select {
		case <-ctx.Done():
			slog.Info("shutting down")
			return
		case <-flushTicker.C:
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
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if emptyReads > 0 {
			slog.Debug("journal resumed with new entries", "empty_reads", emptyReads)
			emptyReads = 0
		}

		batcher.Add(entry)
		slog.Debug("journal entry received", "systemd_unit", entry.Record.SystemdUnit, "cursor", entry.Cursor, "position", entry.Position)

		if batcher.ShouldFlush() {
			if batch := batcher.Flush(); batch != nil {
				slog.Debug("flush by batch limit", "records", len(batch.Records), "current_position", batch.CurrentPosition, "next_position", batch.NextPosition, "reset", reset)
				reset = sendBatch(ctx, journal, sender, batch, reset)
			}
		}
	}
}

func parseClientConfig() clientConfig {
	cfg := clientConfig{}
	serverList := flag.String("server", "https://localhost:27312", "Server URL or comma-separated server URLs")
	flag.StringVar(&cfg.ClientID, "client-id", "", "Client ID (default: hostname)")
	flag.StringVar(&cfg.ServiceMask, "service-mask", "", "Filter for _SYSTEMD_UNIT")
	flag.StringVar(&cfg.JournalNamespace, "journal-namespace", "", "journald namespace to read from (empty = default)")
	flag.BoolVar(&cfg.JournalRecovery, "journal-recovery", false, "Attempt best-effort recovery from corrupted journal entries (may lose data)")
	flag.IntVar(&cfg.BatchSize, "batch-size", 50000, "Max records per batch")
	flag.DurationVar(&cfg.BatchTimeout, "batch-timeout", 5*time.Second, "Batch flush timeout")
	flag.DurationVar(&cfg.HTTPTimeout, "http-timeout", 30*time.Second, "HTTP timeout")
	flag.DurationVar(&cfg.RetryDelay, "retry-delay", time.Second, "Base retry delay")
	flag.StringVar(&cfg.TLSCAFile, "tls-ca-file", "", "CA cert file for server verification")
	flag.StringVar(&cfg.TLSCertFile, "tls-cert-file", "", "Client cert for mTLS")
	flag.StringVar(&cfg.TLSKeyFile, "tls-key-file", "", "Client key for mTLS")
	flag.BoolVar(&cfg.TLSUseSystemPool, "tls-use-system-pool", false, "Use system CA pool")
	flag.BoolVar(&cfg.Debug, "debug", parseEnvBool("LOGLUGGER_DEBUG", false), "Enable debug logging (or set LOGLUGGER_DEBUG=true)")
	args := normalizeBoolFlagArgs(os.Args[1:], "debug")
	args = normalizeBoolFlagArgs(args, "journal-recovery")
	_ = flag.CommandLine.Parse(args)
	cfg.ServerURLs = shuffleServerURLs(parseServerURLs(*serverList))
	return cfg
}

func isJournalCorruption(err error) bool {
	return errors.Is(err, syscall.EBADMSG)
}

func recoverFromJournalCorruption(ctx context.Context, journal client.JournalReader, enabled bool) (bool, error) {
	if !enabled {
		return false, fmt.Errorf("journal corruption detected; stopping. Re-run with -journal-recovery=true to attempt best-effort recovery with possible data loss")
	}
	slog.Warn("journal corruption detected; attempting best-effort recovery, some data loss is possible")
	reset, err := journal.Recover(ctx)
	if err != nil {
		return false, fmt.Errorf("journal corruption recovery is not possible; stopping: %w", err)
	}
	return reset, nil
}

func normalizeBoolFlagArgs(args []string, flagName string) []string {
	if len(args) == 0 {
		return nil
	}
	shortName := "-" + flagName
	longName := "--" + flagName
	out := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		current := args[i]
		if current == shortName || current == longName {
			if i+1 < len(args) {
				next := strings.TrimSpace(args[i+1])
				if _, err := strconv.ParseBool(next); err == nil {
					out = append(out, current+"="+next)
					i++
					continue
				}
			}
		}
		out = append(out, current)
	}
	return out
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

func parseEnvBool(name string, defaultValue bool) bool {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return defaultValue
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		slog.Warn("invalid boolean env value, using default", "name", name, "value", raw, "default", defaultValue)
		return defaultValue
	}
	return value
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
