package models

import (
	"testing"
)

func TestRecord_GetField(t *testing.T) {
	ts := int64(1710345600000000)
	seq := int64(1710345600000)
	p := 6
	rec := Record{
		Message:          "hello",
		Parsed:           map[string]string{"P_DTTM": "2025-03-13", "P_LEVEL": "INFO"},
		SeqNo:            &seq,
		Priority:         &p,
		SyslogIdentifier: "nginx",
		SystemdUnit:      "nginx.service",
		RealtimeTS:       &ts,
		Fields:           map[string]string{"CODE_FILE": "main.go"},
	}

	tests := []struct {
		path   string
		want   string
		wantOk bool
	}{
		{"message", "hello", true},
		{"seqno", "1710345600000", true},
		{"parsed.P_DTTM", "2025-03-13", true},
		{"parsed.P_LEVEL", "INFO", true},
		{"parsed.P_MESSAGE", "", false},
		{"syslog_identifier", "nginx", true},
		{"systemd_unit", "nginx.service", true},
		{"realtime_ts", "1710345600000000", true},
		{"priority", "6", true},
		{"fields.CODE_FILE", "main.go", true},
		{"fields.MISSING", "", false},
		{"unknown", "", false},
		{"parse", "", false},
		{"field", "", false},
	}
	for _, tt := range tests {
		got, ok := rec.GetField(tt.path)
		if ok != tt.wantOk || got != tt.want {
			t.Errorf("GetField(%q) = (%q, %v), want (%q, %v)", tt.path, got, ok, tt.want, tt.wantOk)
		}
	}
}

func TestRecord_GetFieldEmptyAndNilValues(t *testing.T) {
	t.Parallel()

	rec := Record{}
	if got, ok := rec.GetField("message"); ok || got != "" {
		t.Fatalf("empty message GetField = (%q, %v), want (\"\", false)", got, ok)
	}
	if got, ok := rec.GetField("seqno"); ok || got != "" {
		t.Fatalf("nil seqno GetField = (%q, %v), want (\"\", false)", got, ok)
	}
	if got, ok := rec.GetField("priority"); ok || got != "" {
		t.Fatalf("nil priority GetField = (%q, %v), want (\"\", false)", got, ok)
	}
	if got, ok := rec.GetField("realtime_ts"); ok || got != "" {
		t.Fatalf("nil realtime_ts GetField = (%q, %v), want (\"\", false)", got, ok)
	}
	if got, ok := rec.GetField("syslog_identifier"); ok || got != "" {
		t.Fatalf("empty syslog_identifier GetField = (%q, %v), want (\"\", false)", got, ok)
	}
	if got, ok := rec.GetField("systemd_unit"); ok || got != "" {
		t.Fatalf("empty systemd_unit GetField = (%q, %v), want (\"\", false)", got, ok)
	}
}

func TestRecord_HasParsed(t *testing.T) {
	t.Parallel()

	empty := Record{}
	if empty.HasParsed() {
		t.Fatal("empty record should not have parsed fields")
	}
	emptyMap := Record{Parsed: map[string]string{}}
	if emptyMap.HasParsed() {
		t.Fatal("empty parsed map should not count as HasParsed")
	}
	withParsed := Record{Parsed: map[string]string{"P_LEVEL": "INFO"}}
	if !withParsed.HasParsed() {
		t.Fatal("non-empty parsed map should count as HasParsed")
	}
}
