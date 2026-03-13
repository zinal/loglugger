package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ydb-platform/loglugger/internal/client"
	"github.com/ydb-platform/loglugger/internal/models"
)

type clientConfig struct {
	ServerURLs       []string
	ClientID         string
	ServiceMask      string
	MessageRegex     string
	NoMatchAction    client.NoMatchAction
	BatchSize        int
	BatchTimeout     time.Duration
	HTTPTimeout      time.Duration
	RetryDelay       time.Duration
	TLSCAFile        string
	TLSCAPath        string
	TLSCertFile      string
	TLSKeyFile       string
	TLSUseSystemPool bool
}

func main() {
	cfg := parseClientConfig()

	if cfg.ClientID == "" {
		hostname, _ := os.Hostname()
		cfg.ClientID = hostname
	}

	tlsConfig, err := buildClientTLSConfig(cfg)
	if err != nil {
		slog.Error("load TLS config", "error", err)
		os.Exit(1)
	}

	parser, err := client.NewMessageParser(cfg.MessageRegex, cfg.NoMatchAction)
	if err != nil {
		slog.Error("create parser", "error", err)
		os.Exit(1)
	}

	journal, err := client.NewJournalReader(client.JournalConfig{ServiceMask: cfg.ServiceMask})
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
		slog.Warn("seek failed, using reset", "position", position, "error", err)
		reset = true
		if err := journal.SeekToPosition(ctx, ""); err != nil {
			slog.Error("seek head", "error", err)
			os.Exit(1)
		}
	}

	flushTicker := time.NewTicker(cfg.BatchTimeout)
	defer flushTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("shutting down")
			return
		case <-flushTicker.C:
			if batch := batcher.Flush(); batch != nil {
				reset = sendBatch(ctx, journal, sender, batch, reset)
			}
		default:
		}

		entry, err := journal.Next(ctx)
		if err != nil {
			slog.Error("read journal", "error", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if entry == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		rec := entry.Record
		if parser != nil {
			parsed, ok := parser.Parse(rec)
			if !ok {
				continue
			}
			rec = parsed
		}
		entry.Record = rec
		batcher.Add(entry)

		if batcher.ShouldFlush() {
			if batch := batcher.Flush(); batch != nil {
				reset = sendBatch(ctx, journal, sender, batch, reset)
			}
		}
	}
}

func parseClientConfig() clientConfig {
	cfg := clientConfig{}
	serverList := flag.String("server", "https://localhost:8443", "Server URL or comma-separated server URLs")
	flag.StringVar(&cfg.ClientID, "client-id", "", "Client ID (default: hostname)")
	flag.StringVar(&cfg.ServiceMask, "service-mask", "", "Filter for _SYSTEMD_UNIT")
	flag.StringVar(&cfg.MessageRegex, "message-regex", "", "Regex to parse MESSAGE (named groups)")
	noMatch := flag.String("message-regex-no-match", "send_raw", "When regex fails: send_raw or skip")
	flag.IntVar(&cfg.BatchSize, "batch-size", 50000, "Max records per batch")
	flag.DurationVar(&cfg.BatchTimeout, "batch-timeout", 5*time.Second, "Batch flush timeout")
	flag.DurationVar(&cfg.HTTPTimeout, "http-timeout", 30*time.Second, "HTTP timeout")
	flag.DurationVar(&cfg.RetryDelay, "retry-delay", time.Second, "Base retry delay")
	flag.StringVar(&cfg.TLSCAFile, "tls-ca-file", "", "CA cert file for server verification")
	flag.StringVar(&cfg.TLSCAPath, "tls-ca-path", "", "CA cert directory for server verification")
	flag.StringVar(&cfg.TLSCertFile, "tls-cert-file", "", "Client cert for mTLS")
	flag.StringVar(&cfg.TLSKeyFile, "tls-key-file", "", "Client key for mTLS")
	flag.BoolVar(&cfg.TLSUseSystemPool, "tls-use-system-pool", false, "Use system CA pool")
	flag.Parse()
	cfg.NoMatchAction = client.NoMatchAction(*noMatch)
	cfg.ServerURLs = parseServerURLs(*serverList)
	return cfg
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
	tlsCfg, err := client.LoadClientTLSConfig(cfg.TLSCAFile, cfg.TLSCAPath, cfg.TLSCertFile, cfg.TLSKeyFile, cfg.TLSUseSystemPool)
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

func fetchStartupPosition(ctx context.Context, sender client.Sender) (string, bool, error) {
	resp, err := sender.CurrentPosition(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return "", true, err
		}
		slog.Warn("fetch startup position", "error", err)
		return "", true, nil
	}
	if resp == nil || resp.Status == "not_found" || resp.CurrentPosition == "" {
		return "", true, nil
	}
	return resp.CurrentPosition, false, nil
}

func sendBatch(ctx context.Context, journal client.JournalReader, sender client.Sender, batch *client.Batch, reset bool) bool {
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
	if resp.Status == "position_mismatch" {
		slog.Info("position mismatch", "expected", resp.ExpectedPosition)
		if resp.ExpectedPosition != "" {
			if err := journal.SeekToPosition(ctx, resp.ExpectedPosition); err == nil {
				return false
			}
			slog.Warn("seek to expected position failed, using reset", "expected", resp.ExpectedPosition)
		}
		if err := journal.SeekToPosition(ctx, ""); err != nil {
			slog.Error("seek head after mismatch", "error", err)
			return reset
		}
		return true
	}
	return false
}
