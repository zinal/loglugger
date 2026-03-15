package server

import (
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
