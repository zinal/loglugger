package server

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ydb-platform/loglugger/internal/models"
)

func TestMapperDoesNotInjectClientIDWhenNotMapped(t *testing.T) {
	mapper := NewMapper([]FieldMapping{
		{Source: "message", Destination: "msg"},
		{Source: "systemd_unit", Destination: "unit"},
	})

	row, err := mapper.MapRecord("host-01", models.Record{
		Message:     "hello",
		SystemdUnit: "ydbd.service",
	})
	if err != nil {
		t.Fatalf("MapRecord() error = %v", err)
	}
	if got := row["msg"]; got != "hello" {
		t.Fatalf("msg = %v, want hello", got)
	}
	if got := row["unit"]; got != "ydbd.service" {
		t.Fatalf("unit = %v, want ydbd.service", got)
	}
	if _, has := row["client_id"]; has {
		t.Fatalf("row contains unexpected client_id: %+v", row)
	}
}

func TestMapperMapsClientIDToCustomDestination(t *testing.T) {
	mapper := NewMapper([]FieldMapping{
		{Source: "client_id", Destination: "hostname"},
	})

	row, err := mapper.MapRecord("host-01", models.Record{Message: "hello"})
	if err != nil {
		t.Fatalf("MapRecord() error = %v", err)
	}
	if got := row["hostname"]; got != "host-01" {
		t.Fatalf("hostname = %v, want host-01", got)
	}
	if _, has := row["client_id"]; has {
		t.Fatalf("row contains unexpected client_id: %+v", row)
	}
}

func TestLoadFieldMappingsJSONArrayAndEmpty(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "mapping.json")
	if err := os.WriteFile(path, []byte(`[
		{"source":"message","destination":"msg","transform":"string"}
	]`), 0o600); err != nil {
		t.Fatal(err)
	}
	mappings, err := LoadFieldMappings(path)
	if err != nil {
		t.Fatalf("LoadFieldMappings() error = %v", err)
	}
	if len(mappings) != 1 || mappings[0].Source != "message" || mappings[0].Destination != "msg" {
		t.Fatalf("mappings = %#v", mappings)
	}

	emptyPath := filepath.Join(dir, "empty.yaml")
	if err := os.WriteFile(emptyPath, []byte("field_mapping: []\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadFieldMappings(emptyPath); err == nil {
		t.Fatal("expected error for empty mapping file")
	}
}

func TestLoadFieldMappingsRejectsUnknownKeys(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	yamlPath := filepath.Join(dir, "mapping.yaml")
	if err := os.WriteFile(yamlPath, []byte(`
field_mapping:
  - source: message
    destination: msg
    destiantion: typo
`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadFieldMappings(yamlPath); err == nil {
		t.Fatal("expected error for unknown mapping key")
	} else if !strings.Contains(err.Error(), "destiantion") {
		t.Fatalf("error = %v, want mention of destiantion", err)
	}

	jsonPath := filepath.Join(dir, "mapping.json")
	if err := os.WriteFile(jsonPath, []byte(`{"field_mapping":[{"source":"message","destination":"msg","destiantion":"typo"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadFieldMappings(jsonPath); err == nil {
		t.Fatal("expected error for unknown JSON mapping key")
	} else if !strings.Contains(err.Error(), "destiantion") {
		t.Fatalf("error = %v, want mention of destiantion", err)
	}
}
