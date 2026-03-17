package client

import (
	"testing"

	"github.com/ydb-platform/loglugger/internal/models"
)

func TestRecordParserMessageAndSystemdUnit(t *testing.T) {
	parser, err := NewRecordParser(
		`^(?P<P_LEVEL>[A-Z]+): (?P<P_MESSAGE>.*)$`,
		NoMatchSendRaw,
		`^(?P<P_UNIT_NAME>[^@]+)@(?P<P_UNIT_SHARD>.+)\.service$`,
	)
	if err != nil {
		t.Fatal(err)
	}

	rec := models.Record{
		Message:     "INFO: Server started",
		SystemdUnit: "ydbd-storage@02.service",
	}
	got, ok := parser.Parse(rec)
	if !ok {
		t.Fatal("Parse() ok = false, want true")
	}

	want := map[string]string{
		"P_LEVEL":      "INFO",
		"P_MESSAGE":    "Server started",
		"P_UNIT_NAME":  "ydbd-storage",
		"P_UNIT_SHARD": "02",
	}
	for k, v := range want {
		if got.Parsed[k] != v {
			t.Fatalf("Parse() parsed[%q] = %q, want %q", k, got.Parsed[k], v)
		}
	}
}

func TestRecordParserOnlySystemdUnitRegex(t *testing.T) {
	parser, err := NewRecordParser("", NoMatchSendRaw, `^(?P<P_UNIT>[^.]+)\.service$`)
	if err != nil {
		t.Fatal(err)
	}

	rec := models.Record{
		Message:     "raw message",
		SystemdUnit: "nginx.service",
	}
	got, ok := parser.Parse(rec)
	if !ok {
		t.Fatal("Parse() ok = false, want true")
	}
	if got.Parsed["P_UNIT"] != "nginx" {
		t.Fatalf("Parse() parsed[P_UNIT] = %q, want nginx", got.Parsed["P_UNIT"])
	}
	if got.Message != rec.Message {
		t.Fatalf("Parse() message = %q, want %q", got.Message, rec.Message)
	}
}

func TestRecordParserMessageNoMatchSkip(t *testing.T) {
	parser, err := NewRecordParser(
		`^(?P<P_NUM>\d+)$`,
		NoMatchSkip,
		`^(?P<P_UNIT>[^.]+)\.service$`,
	)
	if err != nil {
		t.Fatal(err)
	}

	rec := models.Record{
		Message:     "not a number",
		SystemdUnit: "nginx.service",
	}
	_, ok := parser.Parse(rec)
	if ok {
		t.Fatal("Parse() ok = true, want false when message no-match action is skip")
	}
}

func TestRecordParserMessageNoMatchSendRawStillParsesSystemdUnit(t *testing.T) {
	parser, err := NewRecordParser(
		`^(?P<P_NUM>\d+)$`,
		NoMatchSendRaw,
		`^(?P<P_UNIT>[^.]+)\.service$`,
	)
	if err != nil {
		t.Fatal(err)
	}

	rec := models.Record{
		Message:     "not a number",
		SystemdUnit: "nginx.service",
	}
	got, ok := parser.Parse(rec)
	if !ok {
		t.Fatal("Parse() ok = false, want true")
	}
	if got.Message != rec.Message {
		t.Fatalf("Parse() message = %q, want %q", got.Message, rec.Message)
	}
	if got.Parsed["P_UNIT"] != "nginx" {
		t.Fatalf("Parse() parsed[P_UNIT] = %q, want nginx", got.Parsed["P_UNIT"])
	}
}

func TestRecordParserEmptyRegexes(t *testing.T) {
	parser, err := NewRecordParser("", NoMatchSendRaw, "")
	if err != nil {
		t.Fatal(err)
	}
	if parser != nil {
		t.Fatal("expected nil parser when both regexes are empty")
	}
}

func TestRecordParserStandardRegexMultilineAppendsToPMessage(t *testing.T) {
	parser, err := NewRecordParser(
		`^(?:(?P<P_DTTM>[^ ]+)\s+)?:(?P<P_SERVICE>[^ ]+)\s+(?P<P_LEVEL>[^ ]+):\s+(?P<P_MESSAGE>.*)$`,
		NoMatchSendRaw,
		"",
	)
	if err != nil {
		t.Fatal(err)
	}

	rec := models.Record{
		Message: "2025-03-13T10:00:00 :nginx ERROR: request failed\nstack line 1\nstack line 2",
	}
	got, ok := parser.Parse(rec)
	if !ok {
		t.Fatal("Parse() ok = false, want true")
	}
	if got.Parsed["P_DTTM"] != "2025-03-13T10:00:00" {
		t.Fatalf("P_DTTM = %q", got.Parsed["P_DTTM"])
	}
	if got.Parsed["P_SERVICE"] != "nginx" {
		t.Fatalf("P_SERVICE = %q", got.Parsed["P_SERVICE"])
	}
	if got.Parsed["P_LEVEL"] != "ERROR" {
		t.Fatalf("P_LEVEL = %q", got.Parsed["P_LEVEL"])
	}
	wantMessage := "request failed\nstack line 1\nstack line 2"
	if got.Parsed["P_MESSAGE"] != wantMessage {
		t.Fatalf("P_MESSAGE = %q, want %q", got.Parsed["P_MESSAGE"], wantMessage)
	}
}
