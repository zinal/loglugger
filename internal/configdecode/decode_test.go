package configdecode

import (
	"strings"
	"testing"
)

type sampleConfig struct {
	ServerURL string `json:"server_url" yaml:"server_url"`
	BatchSize int    `json:"batch_size" yaml:"batch_size"`
}

func TestDecodeStrictYAMLAndJSON(t *testing.T) {
	t.Parallel()

	var cfg sampleConfig
	if err := DecodeStrict("cfg.yaml", []byte("server_url: https://example\nbatch_size: 10\n"), &cfg); err != nil {
		t.Fatalf("yaml decode: %v", err)
	}
	if cfg.ServerURL != "https://example" || cfg.BatchSize != 10 {
		t.Fatalf("yaml cfg = %#v", cfg)
	}

	cfg = sampleConfig{}
	if err := DecodeStrict("cfg.json", []byte(`{"server_url":"https://json","batch_size":7}`), &cfg); err != nil {
		t.Fatalf("json decode: %v", err)
	}
	if cfg.ServerURL != "https://json" || cfg.BatchSize != 7 {
		t.Fatalf("json cfg = %#v", cfg)
	}
}

func TestDecodeStrictRejectsUnknownFields(t *testing.T) {
	t.Parallel()

	var cfg sampleConfig
	err := DecodeStrict("cfg.yaml", []byte("server_url: https://example\nbatch_sze: 10\n"), &cfg)
	if err == nil {
		t.Fatal("yaml unknown field: expected error")
	}
	if !strings.Contains(err.Error(), "batch_sze") && !strings.Contains(err.Error(), "field") {
		t.Fatalf("yaml unknown field error = %v", err)
	}

	err = DecodeStrict("cfg.json", []byte(`{"server_url":"https://example","batch_sze":10}`), &cfg)
	if err == nil {
		t.Fatal("json unknown field: expected error")
	}
	if !strings.Contains(err.Error(), "batch_sze") {
		t.Fatalf("json unknown field error = %v", err)
	}
}

func TestDecodeStrictRejectsTrailingData(t *testing.T) {
	t.Parallel()

	var cfg sampleConfig
	err := DecodeStrict("cfg.json", []byte(`{"server_url":"https://example"}{"x":1}`), &cfg)
	if err == nil {
		t.Fatal("json trailing: expected error")
	}

	err = DecodeStrict("cfg.yaml", []byte("server_url: https://example\n---\nbatch_size: 1\n"), &cfg)
	if err == nil {
		t.Fatal("yaml trailing document: expected error")
	}
}
