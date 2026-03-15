package main

import (
	"strings"
	"testing"
)

func TestDefaultServerConfigIncludesYDBOpenTimeout(t *testing.T) {
	cfg := defaultServerConfig()
	if cfg.YDBOpenTimeout != "10s" {
		t.Fatalf("YDBOpenTimeout = %q, want %q", cfg.YDBOpenTimeout, "10s")
	}
}

func TestNewWriterRejectsInvalidYDBOpenTimeout(t *testing.T) {
	cfg := defaultServerConfig()
	cfg.WriterBackend = "ydb"
	cfg.YDBOpenTimeout = "not-a-duration"

	_, err := newWriter(cfg)
	if err == nil {
		t.Fatal("expected error for invalid ydb_open_timeout")
	}
	if !strings.Contains(err.Error(), "parse ydb_open_timeout") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewWriterRejectsNonPositiveYDBOpenTimeout(t *testing.T) {
	cfg := defaultServerConfig()
	cfg.WriterBackend = "ydb"
	cfg.YDBOpenTimeout = "0s"

	_, err := newWriter(cfg)
	if err == nil {
		t.Fatal("expected error for non-positive ydb_open_timeout")
	}
	if !strings.Contains(err.Error(), "must be greater than zero") {
		t.Fatalf("unexpected error: %v", err)
	}
}
