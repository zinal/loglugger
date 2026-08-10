package server

import (
	"strconv"
	"testing"
	"time"

	"github.com/ydb-platform/loglugger/internal/models"
)

func TestApplyTransformAllTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		value     string
		transform string
		want      interface{}
		wantErr   bool
	}{
		{name: "empty transform is string", value: "abc", transform: "", want: "abc"},
		{name: "string transform", value: "abc", transform: "string", want: "abc"},
		{name: "int", value: "42", transform: "int", want: 42},
		{name: "int invalid", value: "x", transform: "int", wantErr: true},
		{name: "int64", value: "9223372036854775807", transform: "int64", want: int64(9223372036854775807)},
		{name: "int64 invalid", value: "1.5", transform: "int64", wantErr: true},
		{name: "uint64", value: "18446744073709551615", transform: "uint64", want: uint64(18446744073709551615)},
		{name: "uint64 invalid", value: "-1", transform: "uint64", wantErr: true},
		{name: "float64", value: "3.5", transform: "float64", want: 3.5},
		{name: "float64 invalid", value: "nan-ish", transform: "float64", wantErr: true},
		{name: "bool true", value: "true", transform: "bool", want: true},
		{name: "bool false", value: "0", transform: "bool", want: false},
		{name: "bool invalid", value: "maybe", transform: "bool", wantErr: true},
		{name: "rfc3339", value: "2025-03-13T10:00:00Z", transform: "rfc3339", want: time.Date(2025, 3, 13, 10, 0, 0, 0, time.UTC)},
		{name: "rfc3339 invalid", value: "13/03/2025", transform: "rfc3339", wantErr: true},
		{name: "timestamp64_us", value: "1710345600000000", transform: "timestamp64_us", want: time.UnixMicro(1710345600000000).UTC()},
		{name: "timestamp64_us invalid", value: "not-a-number", transform: "timestamp64_us", wantErr: true},
		{name: "timestamp64 numeric", value: "1710345600000000", transform: "timestamp64", want: time.UnixMicro(1710345600000000).UTC()},
		{name: "unsupported", value: "x", transform: "uuid", wantErr: true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := applyTransform(tt.value, tt.transform)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("applyTransform(%q, %q) error = nil, want error", tt.value, tt.transform)
				}
				return
			}
			if err != nil {
				t.Fatalf("applyTransform(%q, %q) error = %v", tt.value, tt.transform, err)
			}
			switch want := tt.want.(type) {
			case time.Time:
				gotTime, ok := got.(time.Time)
				if !ok {
					t.Fatalf("got type %T, want time.Time", got)
				}
				if !gotTime.Equal(want) {
					t.Fatalf("got %v, want %v", gotTime, want)
				}
			default:
				if got != tt.want {
					t.Fatalf("got %#v, want %#v", got, tt.want)
				}
			}
		})
	}
}

func TestParseTimestamp64Formats(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   string
		want    time.Time
		wantErr bool
	}{
		{
			name:  "unix microseconds",
			value: "1710345600000000",
			want:  time.UnixMicro(1710345600000000).UTC(),
		},
		{
			name:  "rfc3339 nano",
			value: "2025-03-13T10:00:00.123456789Z",
			want:  time.Date(2025, 3, 13, 10, 0, 0, 123456789, time.UTC),
		},
		{
			name:  "space separated with zone",
			value: "2025-03-13 10:00:00+00:00",
			want:  time.Date(2025, 3, 13, 10, 0, 0, 0, time.UTC),
		},
		{
			name:  "space separated fractional with zone",
			value: "2025-03-13 10:00:00.123456Z",
			want:  time.Date(2025, 3, 13, 10, 0, 0, 123456000, time.UTC),
		},
		{
			name:  "timezone-less datetime is UTC",
			value: "2025-03-13T10:00:00",
			want:  time.Date(2025, 3, 13, 10, 0, 0, 0, time.UTC),
		},
		{
			name:  "timezone-less space datetime",
			value: "2025-03-13 10:00:00.500000",
			want:  time.Date(2025, 3, 13, 10, 0, 0, 500000000, time.UTC),
		},
		{
			name:  "date only",
			value: "2025-03-13",
			want:  time.Date(2025, 3, 13, 0, 0, 0, 0, time.UTC),
		},
		{
			name:    "invalid",
			value:   "not-a-timestamp",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseTimestamp64(tt.value)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("parseTimestamp64() error = %v", err)
			}
			if !got.Equal(tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMapperUsesDefaultAndSkipsMissingWithoutDefault(t *testing.T) {
	t.Parallel()

	def := "fallback"
	mapper := NewMapper([]FieldMapping{
		{Source: "parsed.MISSING", Destination: "with_default", Default: &def},
		{Source: "parsed.ALSO_MISSING", Destination: "without_default"},
		{Source: "message", Destination: "msg"},
	})

	row, err := mapper.MapRecord("client", models.Record{Message: "hello"})
	if err != nil {
		t.Fatalf("MapRecord() error = %v", err)
	}
	if row["with_default"] != "fallback" {
		t.Fatalf("with_default = %#v, want fallback", row["with_default"])
	}
	if _, ok := row["without_default"]; ok {
		t.Fatalf("unexpected without_default in row: %#v", row)
	}
	if row["msg"] != "hello" {
		t.Fatalf("msg = %#v, want hello", row["msg"])
	}
}

func TestLogTimestampMicrosecondsFallsBackToNow(t *testing.T) {
	t.Parallel()

	before := time.Now().UTC().UnixMicro()
	got := logTimestampMicroseconds(models.Record{})
	after := time.Now().UTC().UnixMicro()

	value, err := strconv.ParseInt(got, 10, 64)
	if err != nil {
		t.Fatalf("logTimestampMicroseconds() = %q, not an int64: %v", got, err)
	}
	if value < before || value > after {
		t.Fatalf("fallback timestamp %d outside [%d, %d]", value, before, after)
	}
}
