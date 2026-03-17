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

func TestRecordParserStandardRegexParsesYDBExampleMessages(t *testing.T) {
	parser, err := NewRecordParser(
		`^(?:(?P<P_DTTM>[^ ]+)\s+)?:(?P<P_SERVICE>[^ ]+)\s+(?P<P_LEVEL>[^ ]+):\s+(?P<P_MESSAGE>.*)$`,
		NoMatchSendRaw,
		"",
	)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name        string
		message     string
		wantService string
		wantLevel   string
	}{
		{
			name: "pdisk",
			message: "2026-03-14T18:01:16.753396Z :BS_PDISK NOTICE: {BPD38@blobstorage_pdisk_impl.cpp:2681} OnDriveStartup Path# \"/dev/disk/by-partlabel/ydb_disk_1\" PDiskId# 1",
			wantService: "BS_PDISK",
			wantLevel:   "NOTICE",
		},
		{
			name: "columnshard",
			message: "2026-03-15T11:21:25.954141Z :TX_COLUMNSHARD WARN: tablet_id=72075186224038915;self_id=[50014:7617440977061045175:3144];tablet_id=72075186224038915;process=TTxInitSchema::Execute;fline=abstract.cpp:11;event=normalizer_register;description=CLASS_NAME=RestoreV2Chunks;",
			wantService: "TX_COLUMNSHARD",
			wantLevel:   "WARN",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := models.Record{Message: tt.message}
			got, ok := parser.Parse(rec)
			if !ok {
				t.Fatal("Parse() ok = false, want true")
			}
			if got.Parsed["P_DTTM"] == "" {
				t.Fatal("P_DTTM is empty, want timestamp")
			}
			if got.Parsed["P_SERVICE"] != tt.wantService {
				t.Fatalf("P_SERVICE = %q, want %q", got.Parsed["P_SERVICE"], tt.wantService)
			}
			if got.Parsed["P_LEVEL"] != tt.wantLevel {
				t.Fatalf("P_LEVEL = %q, want %q", got.Parsed["P_LEVEL"], tt.wantLevel)
			}
			if got.Parsed["P_MESSAGE"] == "" {
				t.Fatal("P_MESSAGE is empty, want parsed message body")
			}
		})
	}
}
