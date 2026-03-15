package server

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	yc "github.com/ydb-platform/ydb-go-yc"
)

// YDBWriter writes rows to a YDB table via BulkUpsert.
type YDBWriter struct {
	driver        *ydb.Driver
	positionTable string
	schemaMu      sync.RWMutex
	tableSchemas  map[string]tableSchema
}

type tableSchema struct {
	columns []options.Column
}

type YDBAuthOptions struct {
	Mode                  string
	Login                 string
	Password              string
	ServiceAccountKeyFile string
	MetadataURL           string
	CACertPath            string
}

// NewYDBWriter connects to YDB.
func NewYDBWriter(ctx context.Context, endpoint, database, positionTable string, auth YDBAuthOptions) (*YDBWriter, error) {
	driver, err := openYDBDriver(ctx, endpoint, database, auth)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(positionTable) == "" {
		_ = driver.Close(ctx)
		return nil, fmt.Errorf("position table is required")
	}
	return &YDBWriter{
		driver:        driver,
		positionTable: positionTable,
		tableSchemas:  make(map[string]tableSchema),
	}, nil
}

func (w *YDBWriter) BulkUpsert(ctx context.Context, tableName string, rows []map[string]interface{}) error {
	if len(rows) == 0 {
		return nil
	}
	schema, err := w.getTableSchema(ctx, tableName)
	if err != nil {
		return err
	}
	values := make([]types.Value, 0, len(rows))
	for _, row := range rows {
		structFields, err := structFieldsForRow(row, schema.columns)
		if err != nil {
			return err
		}
		values = append(values, types.StructValue(structFields...))
	}
	rowsValue := types.ListValue(values...)

	if err := w.driver.Table().Do(ctx, func(ctx context.Context, session table.Session) error {
		return session.BulkUpsert(ctx, tableName, rowsValue)
	}); err != nil {
		return fmt.Errorf("bulk upsert rows into %s: %w", tableName, err)
	}
	return nil
}

func (w *YDBWriter) Close(ctx context.Context) error {
	return w.driver.Close(ctx)
}

func (w *YDBWriter) GetPosition(ctx context.Context, clientID string) (string, bool, error) {
	var expectedPosition string
	var found bool
	err := w.driver.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
		result, err := tx.Execute(
			ctx,
			fmt.Sprintf(`
DECLARE $client_id AS Utf8;
SELECT expected_position
FROM %s
WHERE client_id = $client_id
LIMIT 1;
`, quoteYDBPath(w.positionTable)),
			ydb.ParamsBuilder().
				Param("$client_id").Text(clientID).
				Build(),
		)
		if err != nil {
			return err
		}
		defer result.Close()

		if result.NextResultSet(ctx, "expected_position") && result.NextRow() {
			if err := result.Scan(&expectedPosition); err != nil {
				return err
			}
			found = true
		}
		return result.Err()
	},
		table.WithIdempotent(),
	)
	if err != nil {
		return "", false, fmt.Errorf("get expected position from %s: %w", w.positionTable, err)
	}
	return expectedPosition, found, nil
}

func (w *YDBWriter) SetPosition(ctx context.Context, clientID, expectedPosition, nextPosition string) error {
	err := w.driver.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
		_, err := tx.Execute(
			ctx,
			fmt.Sprintf(`
DECLARE $client_id AS Utf8;
DECLARE $old_expected_position AS Utf8;
DECLARE $new_expected_position AS Utf8;

$current_position = (
    SELECT expected_position
    FROM %s
    WHERE client_id = $client_id
    LIMIT 1
);

SELECT Ensure(
    COALESCE($current_position, "") == $old_expected_position,
    "position mismatch"
);

UPSERT INTO %s (client_id, expected_position)
VALUES ($client_id, $new_expected_position);
`, quoteYDBPath(w.positionTable), quoteYDBPath(w.positionTable)),
			ydb.ParamsBuilder().
				Param("$client_id").Text(clientID).
				Param("$old_expected_position").Text(expectedPosition).
				Param("$new_expected_position").Text(nextPosition).
				Build(),
		)
		return err
	},
		table.WithIdempotent(),
	)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "position mismatch") {
			current, found, getErr := w.GetPosition(ctx, clientID)
			if getErr != nil {
				return fmt.Errorf("store expected position in %s: %w", w.positionTable, err)
			}
			return &PositionMismatchError{CurrentPosition: current, Found: found}
		}
		return fmt.Errorf("store expected position in %s: %w", w.positionTable, err)
	}
	return nil
}

func (w *YDBWriter) SetPositionUnconditional(ctx context.Context, clientID, nextPosition string) error {
	err := w.driver.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
		_, err := tx.Execute(
			ctx,
			fmt.Sprintf(`
DECLARE $client_id AS Utf8;
DECLARE $new_expected_position AS Utf8;

UPSERT INTO %s (client_id, expected_position)
VALUES ($client_id, $new_expected_position);
`, quoteYDBPath(w.positionTable)),
			ydb.ParamsBuilder().
				Param("$client_id").Text(clientID).
				Param("$new_expected_position").Text(nextPosition).
				Build(),
		)
		return err
	},
		table.WithIdempotent(),
	)
	if err != nil {
		return fmt.Errorf("store expected position in %s: %w", w.positionTable, err)
	}
	return nil
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
		// Mapping transforms `timestamp64` and `timestamp64_us` produce `time.Time`.
		// Encode as YDB Timestamp64 (microseconds since Unix epoch), not Timestamp.
		return types.Timestamp64Value(v.UTC().UnixMicro()), nil
	default:
		return nil, fmt.Errorf("unsupported YDB value type %T", value)
	}
}

func structFieldsForRow(row map[string]interface{}, columns []options.Column) ([]types.StructValueOption, error) {
	structFields := make([]types.StructValueOption, 0, len(columns))
	for _, column := range columns {
		raw, ok := row[column.Name]
		if !ok {
			optional, innerType := types.IsOptional(column.Type)
			if !optional {
				return nil, fmt.Errorf("missing required column %q", column.Name)
			}
			structFields = append(structFields, types.StructFieldValue(column.Name, types.NullValue(innerType)))
			continue
		}
		value, err := encodeYDBValue(raw)
		if err != nil {
			return nil, fmt.Errorf("encode %s: %w", column.Name, err)
		}
		structFields = append(structFields, types.StructFieldValue(column.Name, value))
	}
	return structFields, nil
}

func (w *YDBWriter) getTableSchema(ctx context.Context, tableName string) (tableSchema, error) {
	w.schemaMu.RLock()
	cached, ok := w.tableSchemas[tableName]
	w.schemaMu.RUnlock()
	if ok {
		return cached, nil
	}

	var desc options.Description
	if err := w.driver.Table().Do(ctx, func(ctx context.Context, session table.Session) error {
		var err error
		desc, err = session.DescribeTable(ctx, tableName)
		return err
	}); err != nil {
		return tableSchema{}, fmt.Errorf("describe table %s: %w", tableName, err)
	}
	schema := tableSchema{columns: desc.Columns}

	w.schemaMu.Lock()
	w.tableSchemas[tableName] = schema
	w.schemaMu.Unlock()
	return schema, nil
}

func openYDBDriver(ctx context.Context, endpoint, database string, auth YDBAuthOptions) (*ydb.Driver, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("ydb endpoint is required")
	}
	if database == "" {
		return nil, fmt.Errorf("ydb database is required")
	}
	authOption, err := ydbAuthOption(auth)
	if err != nil {
		return nil, err
	}
	opts := []ydb.Option{
		ydb.WithDatabase(database),
		authOption,
	}
	if caPath := strings.TrimSpace(auth.CACertPath); caPath != "" {
		opts = append(opts, ydb.WithCertificatesFromFile(caPath))
	}
	driver, err := ydb.Open(ctx, endpoint, opts...)
	if err != nil {
		return nil, fmt.Errorf("open ydb connection: %w", err)
	}
	return driver, nil
}

func ydbAuthOption(auth YDBAuthOptions) (ydb.Option, error) {
	mode := strings.ToLower(strings.TrimSpace(auth.Mode))
	switch mode {
	case "", "anonymous":
		return ydb.WithAnonymousCredentials(), nil
	case "static":
		if strings.TrimSpace(auth.Login) == "" {
			return nil, fmt.Errorf("ydb static auth login is required")
		}
		if strings.TrimSpace(auth.Password) == "" {
			return nil, fmt.Errorf("ydb static auth password is required")
		}
		return ydb.WithStaticCredentials(auth.Login, auth.Password), nil
	case "service-account-key", "service_account_key", "sa-key":
		if strings.TrimSpace(auth.ServiceAccountKeyFile) == "" {
			return nil, fmt.Errorf("ydb service account key file is required")
		}
		return yc.WithServiceAccountKeyFileCredentials(auth.ServiceAccountKeyFile), nil
	case "metadata":
		if strings.TrimSpace(auth.MetadataURL) != "" {
			return yc.WithMetadataCredentialsURL(auth.MetadataURL), nil
		}
		return yc.WithMetadataCredentials(), nil
	default:
		return nil, fmt.Errorf("unsupported ydb auth mode %q", auth.Mode)
	}
}

func quoteYDBPath(path string) string {
	return "`" + strings.ReplaceAll(path, "`", "_") + "`"
}
