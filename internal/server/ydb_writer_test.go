package server

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestStructFieldsForRowFillsMissingNullableColumnsWithNull(t *testing.T) {
	columns := []options.Column{
		{Name: "msg", Type: types.Optional(types.TypeUTF8)},
		{Name: "hostname", Type: types.TypeUTF8},
	}
	row := map[string]interface{}{
		"hostname": "host-01",
	}

	fields, err := structFieldsForRow(row, columns)
	if err != nil {
		t.Fatalf("structFieldsForRow() error = %v", err)
	}
	value := types.StructValue(fields...)
	parsed, err := types.StructFields(value)
	if err != nil {
		t.Fatalf("types.StructFields() error = %v", err)
	}
	msg, ok := parsed["msg"]
	if !ok {
		t.Fatalf("missing struct field msg: %+v", parsed)
	}
	if !types.IsNull(msg) {
		t.Fatalf("msg is not NULL: %#v", msg)
	}
}

func TestStructFieldsForRowErrorsOnMissingRequiredColumn(t *testing.T) {
	columns := []options.Column{
		{Name: "ts_log", Type: types.TypeTimestamp64},
		{Name: "hostname", Type: types.TypeUTF8},
	}
	row := map[string]interface{}{
		"hostname": "host-01",
	}

	_, err := structFieldsForRow(row, columns)
	if err == nil {
		t.Fatal("expected error for missing required column")
	}
}

func TestEncodeYDBValueSupportsMappedTypes(t *testing.T) {
	t.Parallel()

	ts := time.Date(2025, 3, 13, 10, 0, 0, 123456000, time.UTC)
	cases := []struct {
		name       string
		value      interface{}
		columnType types.Type
	}{
		{name: "string", value: "hello", columnType: types.TypeUTF8},
		{name: "bytes", value: []byte("raw"), columnType: types.TypeBytes},
		{name: "bool", value: true, columnType: types.TypeBool},
		{name: "int to int64", value: 7, columnType: types.TypeInt64},
		{name: "int8", value: int8(8), columnType: types.TypeInt8},
		{name: "int16", value: int16(16), columnType: types.TypeInt16},
		{name: "int32", value: int32(32), columnType: types.TypeInt32},
		{name: "int64", value: int64(64), columnType: types.TypeInt64},
		{name: "uint", value: uint(7), columnType: types.TypeUint64},
		{name: "uint8", value: uint8(8), columnType: types.TypeUint8},
		{name: "uint16", value: uint16(16), columnType: types.TypeUint16},
		{name: "uint32", value: uint32(32), columnType: types.TypeUint32},
		{name: "uint64", value: uint64(64), columnType: types.TypeUint64},
		{name: "float32", value: float32(1.5), columnType: types.TypeFloat},
		{name: "float64", value: 2.5, columnType: types.TypeDouble},
		{name: "time", value: ts, columnType: types.TypeTimestamp64},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, err := encodeYDBValue(tc.value, tc.columnType); err != nil {
				t.Fatalf("encodeYDBValue(%T, %s) error = %v", tc.value, tc.columnType.Yql(), err)
			}
		})
	}

	if _, err := encodeYDBValue(struct{}{}, types.TypeUTF8); err == nil {
		t.Fatal("expected unsupported type error")
	}
}

func TestEncodeYDBValueUsesColumnType(t *testing.T) {
	t.Parallel()

	t.Run("string to Timestamp64 without transform", func(t *testing.T) {
		t.Parallel()
		got, err := encodeYDBValue("2025-03-13T10:00:00.123456Z", types.TypeTimestamp64)
		if err != nil {
			t.Fatalf("encodeYDBValue() error = %v", err)
		}
		if got.Type().Yql() != types.TypeTimestamp64.Yql() {
			t.Fatalf("type = %s, want Timestamp64", got.Type().Yql())
		}
		var ts time.Time
		if err := types.CastTo(got, &ts); err != nil {
			t.Fatalf("CastTo: %v", err)
		}
		want := time.Date(2025, 3, 13, 10, 0, 0, 123456000, time.UTC)
		if !ts.Equal(want) {
			t.Fatalf("ts = %v, want %v", ts, want)
		}
	})

	t.Run("Go int to Int32 column", func(t *testing.T) {
		t.Parallel()
		got, err := encodeYDBValue(42, types.TypeInt32)
		if err != nil {
			t.Fatalf("encodeYDBValue() error = %v", err)
		}
		if got.Type().Yql() != types.TypeInt32.Yql() {
			t.Fatalf("type = %s, want Int32", got.Type().Yql())
		}
		var n int32
		if err := types.CastTo(got, &n); err != nil {
			t.Fatalf("CastTo: %v", err)
		}
		if n != 42 {
			t.Fatalf("n = %d, want 42", n)
		}
	})

	t.Run("Go int overflow Int32", func(t *testing.T) {
		t.Parallel()
		_, err := encodeYDBValue(int64(math.MaxInt32)+1, types.TypeInt32)
		if err == nil {
			t.Fatal("expected overflow error")
		}
		if !strings.Contains(err.Error(), "Int32") {
			t.Fatalf("error = %v, want Int32 mention", err)
		}
	})

	t.Run("optional Timestamp64 from string", func(t *testing.T) {
		t.Parallel()
		got, err := encodeYDBValue("1710345600000000", types.Optional(types.TypeTimestamp64))
		if err != nil {
			t.Fatalf("encodeYDBValue() error = %v", err)
		}
		// Optional wrapping is applied by structFieldsForRow; encode returns bare type.
		if got.Type().Yql() != types.TypeTimestamp64.Yql() {
			t.Fatalf("type = %s, want Timestamp64", got.Type().Yql())
		}
	})
}

func TestStructFieldsForRowEncodesStringIntoTimestamp64(t *testing.T) {
	t.Parallel()

	columns := []options.Column{
		{Name: "log_dttm", Type: types.TypeTimestamp64},
	}
	row := map[string]interface{}{
		"log_dttm": "2025-03-13 10:00:00",
	}
	fields, err := structFieldsForRow(row, columns)
	if err != nil {
		t.Fatalf("structFieldsForRow() error = %v", err)
	}
	value := types.StructValue(fields...)
	parsed, err := types.StructFields(value)
	if err != nil {
		t.Fatalf("types.StructFields() error = %v", err)
	}
	field := parsed["log_dttm"]
	if field.Type().Yql() != types.TypeTimestamp64.Yql() {
		t.Fatalf("type = %s, want Timestamp64", field.Type().Yql())
	}
}

func TestStructFieldsForRowEncodesIntIntoInt32(t *testing.T) {
	t.Parallel()

	columns := []options.Column{
		{Name: "priority", Type: types.TypeInt32},
	}
	row := map[string]interface{}{
		"priority": 6, // mapper transform "int" yields Go int
	}
	fields, err := structFieldsForRow(row, columns)
	if err != nil {
		t.Fatalf("structFieldsForRow() error = %v", err)
	}
	value := types.StructValue(fields...)
	parsed, err := types.StructFields(value)
	if err != nil {
		t.Fatalf("types.StructFields() error = %v", err)
	}
	field := parsed["priority"]
	if field.Type().Yql() != types.TypeInt32.Yql() {
		t.Fatalf("type = %s, want Int32", field.Type().Yql())
	}
}

func TestStructFieldsForRowEncodesPresentValues(t *testing.T) {
	t.Parallel()

	columns := []options.Column{
		{Name: "msg", Type: types.TypeUTF8},
		{Name: "priority", Type: types.TypeInt64},
	}
	row := map[string]interface{}{
		"msg":      "hello",
		"priority": 6,
	}
	fields, err := structFieldsForRow(row, columns)
	if err != nil {
		t.Fatalf("structFieldsForRow() error = %v", err)
	}
	if len(fields) != 2 {
		t.Fatalf("len(fields) = %d, want 2", len(fields))
	}
	value := types.StructValue(fields...)
	parsed, err := types.StructFields(value)
	if err != nil {
		t.Fatalf("types.StructFields() error = %v", err)
	}
	msg := parsed["msg"]
	if optional, _ := types.IsOptional(msg.Type()); optional {
		t.Fatalf("non-optional column msg was wrapped as optional: %s", msg.Type().Yql())
	}
	var msgText string
	if err := types.CastTo(msg, &msgText); err != nil {
		t.Fatalf("cast msg: %v", err)
	}
	if msgText != "hello" {
		t.Fatalf("msg = %q, want hello", msgText)
	}
}

func TestStructFieldsForRowWrapsPresentNullableColumnsWithOptional(t *testing.T) {
	t.Parallel()

	ts := time.Date(2025, 3, 13, 10, 0, 0, 123456000, time.UTC)
	columns := []options.Column{
		{Name: "msg", Type: types.Optional(types.TypeUTF8)},
		{Name: "ts_orig", Type: types.Optional(types.TypeTimestamp64)},
		{Name: "hostname", Type: types.TypeUTF8},
	}
	row := map[string]interface{}{
		"msg":      "hello",
		"ts_orig":  ts,
		"hostname": "host-01",
	}

	fields, err := structFieldsForRow(row, columns)
	if err != nil {
		t.Fatalf("structFieldsForRow() error = %v", err)
	}
	value := types.StructValue(fields...)
	parsed, err := types.StructFields(value)
	if err != nil {
		t.Fatalf("types.StructFields() error = %v", err)
	}

	msg, ok := parsed["msg"]
	if !ok {
		t.Fatalf("missing struct field msg: %+v", parsed)
	}
	if types.IsNull(msg) {
		t.Fatal("msg unexpectedly NULL")
	}
	if optional, inner := types.IsOptional(msg.Type()); !optional {
		t.Fatalf("msg type = %s, want Optional(Utf8)", msg.Type().Yql())
	} else if inner.Yql() != types.TypeUTF8.Yql() {
		t.Fatalf("msg inner type = %s, want Utf8", inner.Yql())
	}
	var msgText string
	if err := types.CastTo(types.Unwrap(msg), &msgText); err != nil {
		t.Fatalf("cast unwrapped msg: %v", err)
	}
	if msgText != "hello" {
		t.Fatalf("msg = %q, want hello", msgText)
	}

	tsOrig, ok := parsed["ts_orig"]
	if !ok {
		t.Fatalf("missing struct field ts_orig: %+v", parsed)
	}
	if types.IsNull(tsOrig) {
		t.Fatal("ts_orig unexpectedly NULL")
	}
	if optional, _ := types.IsOptional(tsOrig.Type()); !optional {
		t.Fatalf("ts_orig type = %s, want Optional(Timestamp64)", tsOrig.Type().Yql())
	}

	hostname, ok := parsed["hostname"]
	if !ok {
		t.Fatalf("missing struct field hostname: %+v", parsed)
	}
	if optional, _ := types.IsOptional(hostname.Type()); optional {
		t.Fatalf("required hostname was wrapped as optional: %s", hostname.Type().Yql())
	}
}
