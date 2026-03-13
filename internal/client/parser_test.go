package client

import (
	"testing"

	"github.com/ydb-platform/loglugger/internal/models"
)

func TestMessageParser_Parse(t *testing.T) {
	re := `^(?P<P_DTTM>[^ ]*) :(?P<P_SERVICE>[^ ]*) (?P<P_LEVEL>[^ ]*): (?P<P_MESSAGE>.*)$`
	parser, err := NewMessageParser(re, NoMatchSendRaw)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name     string
		message  string
		wantOk   bool
		wantParsed map[string]string
	}{
		{
			name:    "match",
			message: "2025-03-13T10:00:00 :nginx INFO: Server started",
			wantOk:  true,
			wantParsed: map[string]string{
				"P_DTTM":   "2025-03-13T10:00:00",
				"P_SERVICE": "nginx",
				"P_LEVEL":   "INFO",
				"P_MESSAGE": "Server started",
			},
		},
		{
			name:       "no match send_raw",
			message:    "unstructured log line",
			wantOk:     true,
			wantParsed: map[string]string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := models.Record{Message: tt.message}
			got, ok := parser.Parse(rec)
			if ok != tt.wantOk {
				t.Errorf("Parse() ok = %v, want %v", ok, tt.wantOk)
			}
			for k, v := range tt.wantParsed {
				if gotVal := got.Parsed[k]; gotVal != v {
					t.Errorf("Parse() parsed[%s] = %q, want %q", k, gotVal, v)
				}
			}
			if tt.name == "no match send_raw" {
				if got.Message != tt.message {
					t.Errorf("Parse() message = %q, want raw %q", got.Message, tt.message)
				}
				if len(got.Parsed) != 0 {
					t.Errorf("Parse() expected no parsed fields, got %v", got.Parsed)
				}
			}
		})
	}
}

func TestMessageParser_NoMatchSkip(t *testing.T) {
	parser, err := NewMessageParser(`^(?P<X>\d+)$`, NoMatchSkip)
	if err != nil {
		t.Fatal(err)
	}
	rec := models.Record{Message: "not a number"}
	_, ok := parser.Parse(rec)
	if ok {
		t.Error("expected skip (ok=false) for non-matching message")
	}
}

func TestMessageParser_EmptyRegex(t *testing.T) {
	parser, err := NewMessageParser("", NoMatchSendRaw)
	if err != nil {
		t.Fatal(err)
	}
	if parser != nil {
		t.Error("expected nil parser for empty regex")
	}
}
