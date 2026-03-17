package server

import (
	"testing"

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
