package server

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

// YDBWriter writes rows to a YDB table via BulkUpsert.
type YDBWriter struct {
	driver *ydb.Driver
}

// NewYDBWriter connects to YDB.
func NewYDBWriter(ctx context.Context, endpoint, database string) (*YDBWriter, error) {
	driver, err := openYDBDriver(ctx, endpoint, database)
	if err != nil {
		return nil, err
	}
	return &YDBWriter{driver: driver}, nil
}

func (w *YDBWriter) BulkUpsert(ctx context.Context, tableName string, rows []map[string]interface{}) error {
	if len(rows) == 0 {
		return nil
	}
	grouped, err := groupRowsBySchema(rows)
	if err != nil {
		return err
	}
	for _, batch := range grouped {
		if err := w.driver.Table().Do(ctx, func(ctx context.Context, session table.Session) error {
			values := make([]types.Value, 0, len(batch.rows))
			for _, row := range batch.rows {
				structFields := make([]types.StructValueOption, 0, len(batch.columns))
				for _, column := range batch.columns {
					value, err := encodeYDBValue(row[column])
					if err != nil {
						return fmt.Errorf("encode %s: %w", column, err)
					}
					structFields = append(structFields, types.StructFieldValue(column, value))
				}
				values = append(values, types.StructValue(structFields...))
			}
			return session.BulkUpsert(ctx, tableName, types.ListValue(values...))
		}); err != nil {
			return fmt.Errorf("bulk upsert rows into %s: %w", tableName, err)
		}
	}
	return nil
}

func (w *YDBWriter) Close(ctx context.Context) error {
	return w.driver.Close(ctx)
}

type ydbRowBatch struct {
	columns []string
	rows    []map[string]interface{}
}

func groupRowsBySchema(rows []map[string]interface{}) ([]ydbRowBatch, error) {
	grouped := make(map[string]*ydbRowBatch)
	order := make([]string, 0)
	for _, row := range rows {
		columns := make([]string, 0, len(row))
		for column := range row {
			columns = append(columns, column)
		}
		sort.Strings(columns)
		key := strings.Join(columns, "\x00")
		batch, ok := grouped[key]
		if !ok {
			batch = &ydbRowBatch{columns: columns, rows: make([]map[string]interface{}, 0)}
			grouped[key] = batch
			order = append(order, key)
		}
		batch.rows = append(batch.rows, row)
	}
	batches := make([]ydbRowBatch, 0, len(order))
	for _, key := range order {
		batches = append(batches, *grouped[key])
	}
	return batches, nil
}

func encodeYDBValue(value interface{}) (types.Value, error) {
	switch v := value.(type) {
	case string:
		return types.UTF8Value(v), nil
	case []byte:
		return types.BytesValue(v), nil
	case bool:
		return types.BoolValue(v), nil
	case int:
		return types.Int64Value(int64(v)), nil
	case int8:
		return types.Int8Value(v), nil
	case int16:
		return types.Int16Value(v), nil
	case int32:
		return types.Int32Value(v), nil
	case int64:
		return types.Int64Value(v), nil
	case uint:
		return types.Uint64Value(uint64(v)), nil
	case uint8:
		return types.Uint8Value(v), nil
	case uint16:
		return types.Uint16Value(v), nil
	case uint32:
		return types.Uint32Value(v), nil
	case uint64:
		return types.Uint64Value(v), nil
	case float32:
		return types.FloatValue(v), nil
	case float64:
		return types.DoubleValue(v), nil
	case time.Time:
		return types.TimestampValueFromTime(v), nil
	default:
		return nil, fmt.Errorf("unsupported YDB value type %T", value)
	}
}

func openYDBDriver(ctx context.Context, endpoint, database string) (*ydb.Driver, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("ydb endpoint is required")
	}
	if database == "" {
		return nil, fmt.Errorf("ydb database is required")
	}
	driver, err := ydb.Open(ctx, endpoint, ydb.WithDatabase(database), ydb.WithAnonymousCredentials())
	if err != nil {
		return nil, fmt.Errorf("open ydb connection: %w", err)
	}
	return driver, nil
}
