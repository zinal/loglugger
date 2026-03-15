package server

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
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

func TestOpenYDBDriverAppliesDefaultOpenTimeoutWithoutDeadline(t *testing.T) {
	previousOpen := ydbOpen
	t.Cleanup(func() {
		ydbOpen = previousOpen
	})

	openTimeout := 20 * time.Millisecond
	ydbOpen = func(ctx context.Context, _ string, _ ...ydb.Option) (*ydb.Driver, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	_, err := openYDBDriver(context.Background(), "grpcs://localhost:2135", "/local", YDBAuthOptions{}, openTimeout)
	if err == nil {
		t.Fatal("expected openYDBDriver() to fail with timeout")
	}
	if !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("expected timeout error, got %v", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline exceeded, got %v", err)
	}
}

func TestOpenYDBDriverKeepsExistingContextDeadline(t *testing.T) {
	previousOpen := ydbOpen
	t.Cleanup(func() {
		ydbOpen = previousOpen
	})

	openTimeout := 2 * time.Second
	callerTimeout := 25 * time.Millisecond
	var gotDeadline time.Time
	var gotDeadlineSet bool
	ydbOpen = func(ctx context.Context, _ string, _ ...ydb.Option) (*ydb.Driver, error) {
		gotDeadline, gotDeadlineSet = ctx.Deadline()
		<-ctx.Done()
		return nil, ctx.Err()
	}

	ctx, cancel := context.WithTimeout(context.Background(), callerTimeout)
	defer cancel()

	_, err := openYDBDriver(ctx, "grpcs://localhost:2135", "/local", YDBAuthOptions{}, openTimeout)
	if err == nil {
		t.Fatal("expected openYDBDriver() to fail with deadline exceeded")
	}
	if !gotDeadlineSet {
		t.Fatal("expected open context to have deadline")
	}
	remaining := time.Until(gotDeadline)
	if remaining > 500*time.Millisecond {
		t.Fatalf("expected existing short deadline to be preserved, remaining=%s", remaining)
	}
	if strings.Contains(err.Error(), "timed out after") {
		t.Fatalf("did not expect default-timeout error when caller deadline exists, got %v", err)
	}
}
