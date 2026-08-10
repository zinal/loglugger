package server

import (
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
		name  string
		value interface{}
	}{
		{name: "string", value: "hello"},
		{name: "bytes", value: []byte("raw")},
		{name: "bool", value: true},
		{name: "int", value: 7},
		{name: "int8", value: int8(8)},
		{name: "int16", value: int16(16)},
		{name: "int32", value: int32(32)},
		{name: "int64", value: int64(64)},
		{name: "uint", value: uint(7)},
		{name: "uint8", value: uint8(8)},
		{name: "uint16", value: uint16(16)},
		{name: "uint32", value: uint32(32)},
		{name: "uint64", value: uint64(64)},
		{name: "float32", value: float32(1.5)},
		{name: "float64", value: 2.5},
		{name: "time", value: ts},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, err := encodeYDBValue(tc.value); err != nil {
				t.Fatalf("encodeYDBValue(%T) error = %v", tc.value, err)
			}
		})
	}

	if _, err := encodeYDBValue(struct{}{}); err == nil {
		t.Fatal("expected unsupported type error")
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
