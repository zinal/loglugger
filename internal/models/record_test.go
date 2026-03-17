package models

import (
	"testing"
)

func TestRecord_GetField(t *testing.T) {
	ts := int64(1710345600000000)
	tsu := uint64(12345678)
	seq := int64(1710345600000)
	p := 6
	rec := Record{
		Message:            "hello",
		Parsed:             map[string]string{"P_DTTM": "2025-03-13", "P_LEVEL": "INFO"},
		SeqNo:              &seq,
		Priority:           &p,
		SyslogIdentifier:   "nginx",
		SystemdUnit:        "nginx.service",
		RealtimeTimestamp:  &ts,
		MonotonicTimestamp: &tsu,
		Fields:             map[string]string{"CODE_FILE": "main.go"},
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
		{"realtime_timestamp", "1710345600000000", true},
		{"monotonic_timestamp", "12345678", true},
		{"priority", "6", true},
		{"fields.CODE_FILE", "main.go", true},
		{"fields.MISSING", "", false},
	}
	for _, tt := range tests {
		got, ok := rec.GetField(tt.path)
		if ok != tt.wantOk || got != tt.want {
			t.Errorf("GetField(%q) = (%q, %v), want (%q, %v)", tt.path, got, ok, tt.want, tt.wantOk)
		}
	}
}
