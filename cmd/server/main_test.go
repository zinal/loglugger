package main

import (
	"strings"
	"testing"
	"time"
)

func TestDefaultServerConfigIncludesYDBOpenTimeout(t *testing.T) {
	cfg := defaultServerConfig()
	if cfg.YDBOpenTimeout != "10s" {
		t.Fatalf("YDBOpenTimeout = %q, want %q", cfg.YDBOpenTimeout, "10s")
	}
}

func TestDefaultServerConfigIncludesHTTPTimeouts(t *testing.T) {
	cfg := defaultServerConfig()
	if cfg.ReadHeaderTimeout != "10s" {
		t.Fatalf("ReadHeaderTimeout = %q, want %q", cfg.ReadHeaderTimeout, "10s")
	}
	if cfg.ReadTimeout != "60s" {
		t.Fatalf("ReadTimeout = %q, want %q", cfg.ReadTimeout, "60s")
	}
	if cfg.WriteTimeout != "60s" {
		t.Fatalf("WriteTimeout = %q, want %q", cfg.WriteTimeout, "60s")
	}
	if cfg.IdleTimeout != "120s" {
		t.Fatalf("IdleTimeout = %q, want %q", cfg.IdleTimeout, "120s")
	}
}

func TestDefaultServerConfigIncludesRequestBodyLimits(t *testing.T) {
	cfg := defaultServerConfig()
	const wantCompressed = int64(16 << 20)
	const wantDecompressed = int64(32 << 20)
	if cfg.MaxCompressedBodyBytes != wantCompressed {
		t.Fatalf("MaxCompressedBodyBytes = %d, want %d", cfg.MaxCompressedBodyBytes, wantCompressed)
	}
	if cfg.MaxDecompressedBodyBytes != wantDecompressed {
		t.Fatalf("MaxDecompressedBodyBytes = %d, want %d", cfg.MaxDecompressedBodyBytes, wantDecompressed)
	}
}

func TestParseHTTPServerTimeoutsDefaults(t *testing.T) {
	timeouts, err := parseHTTPServerTimeouts(serverConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if timeouts.ReadHeaderTimeout != 10*time.Second {
		t.Fatalf("ReadHeaderTimeout = %v, want 10s", timeouts.ReadHeaderTimeout)
	}
	if timeouts.ReadTimeout != 60*time.Second {
		t.Fatalf("ReadTimeout = %v, want 60s", timeouts.ReadTimeout)
	}
	if timeouts.WriteTimeout != 60*time.Second {
		t.Fatalf("WriteTimeout = %v, want 60s", timeouts.WriteTimeout)
	}
	if timeouts.IdleTimeout != 120*time.Second {
		t.Fatalf("IdleTimeout = %v, want 120s", timeouts.IdleTimeout)
	}
}

func TestParseHTTPServerTimeoutsOverrides(t *testing.T) {
	timeouts, err := parseHTTPServerTimeouts(serverConfig{
		ReadHeaderTimeout: "5s",
		ReadTimeout:       "30s",
		WriteTimeout:      "45s",
		IdleTimeout:       "90s",
	})
	if err != nil {
		t.Fatal(err)
	}
	if timeouts.ReadHeaderTimeout != 5*time.Second {
		t.Fatalf("ReadHeaderTimeout = %v, want 5s", timeouts.ReadHeaderTimeout)
	}
	if timeouts.ReadTimeout != 30*time.Second {
		t.Fatalf("ReadTimeout = %v, want 30s", timeouts.ReadTimeout)
	}
	if timeouts.WriteTimeout != 45*time.Second {
		t.Fatalf("WriteTimeout = %v, want 45s", timeouts.WriteTimeout)
	}
	if timeouts.IdleTimeout != 90*time.Second {
		t.Fatalf("IdleTimeout = %v, want 90s", timeouts.IdleTimeout)
	}
}

func TestParseHTTPServerTimeoutsRejectsInvalid(t *testing.T) {
	_, err := parseHTTPServerTimeouts(serverConfig{ReadHeaderTimeout: "not-a-duration"})
	if err == nil {
		t.Fatal("expected error for invalid read_header_timeout")
	}
	if !strings.Contains(err.Error(), "parse read_header_timeout") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParseHTTPServerTimeoutsRejectsNonPositive(t *testing.T) {
	_, err := parseHTTPServerTimeouts(serverConfig{IdleTimeout: "0s"})
	if err == nil {
		t.Fatal("expected error for non-positive idle_timeout")
	}
	if !strings.Contains(err.Error(), "must be greater than zero") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFullTablePath(t *testing.T) {
	tests := []struct {
		name     string
		database string
		table    string
		want     string
	}{
		{
			name:     "relative table under absolute database",
			database: "/local",
			table:    "ydblogs",
			want:     "/local/ydblogs",
		},
		{
			name:     "nested database path",
			database: "/Root/db",
			table:    "logs",
			want:     "/Root/db/logs",
		},
		{
			name:     "absolute table path unchanged",
			database: "/local",
			table:    "/Root/db/logs",
			want:     "/Root/db/logs",
		},
		{
			name:     "empty database keeps table as-is",
			database: "",
			table:    "logs",
			want:     "logs",
		},
		{
			name:     "trims surrounding whitespace",
			database: "  /local  ",
			table:    "  ydblogs  ",
			want:     "/local/ydblogs",
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := fullTablePath(tc.database, tc.table); got != tc.want {
				t.Fatalf("fullTablePath(%q, %q) = %q, want %q", tc.database, tc.table, got, tc.want)
			}
		})
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
