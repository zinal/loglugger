package server

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
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

var (
	defaultYDBOpenTimeout = 10 * time.Second
)

type YDBAuthOptions struct {
	Mode                  string
	Login                 string
	Password              string
	ServiceAccountKeyFile string
	MetadataURL           string
	CACertPath            string
}

// NewYDBWriter connects to YDB.
func NewYDBWriter(ctx context.Context, endpoint, database, positionTable string, auth YDBAuthOptions, openTimeout time.Duration) (*YDBWriter, error) {
	driver, err := openYDBDriver(ctx, endpoint, database, auth, openTimeout)
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

// ValidateMappings checks field mappings against the live table schema so
// transform/column mismatches fail at startup instead of during BulkUpsert.
func (w *YDBWriter) ValidateMappings(ctx context.Context, tableName string, mappings []FieldMapping) error {
	schema, err := w.getTableSchema(ctx, tableName)
	if err != nil {
		return err
	}
	return ValidateFieldMappingsAgainstColumns(mappings, schema.columns)
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

	if err := w.driver.Table().BulkUpsert(ctx, tableName, table.BulkUpsertDataRows(rowsValue)); err != nil {
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
SELECT exp_pos
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

		if result.NextResultSet(ctx, "exp_pos") && result.NextRow() {
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

func (w *YDBWriter) SetPosition(ctx context.Context, clientID, expectedPosition, nextPosition string, update PositionUpdate) error {
	err := w.driver.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
		// COALESCE keeps previously stored seqno/ts_orig when the batch did not
		// carry those fields (empty batch / heartbeat / optional fields omitted).
		// UPSERT updates every listed column, so binding SQL NULL would wipe them.
		// One AsTuple read covers exp_pos + seqno + ts_orig.
		_, err := tx.Execute(
			ctx,
			fmt.Sprintf(`
DECLARE $client_id AS Utf8;
DECLARE $old_exp_pos AS Utf8;
DECLARE $new_exp_pos AS Utf8;
DECLARE $ts_wall_us AS Timestamp64;
DECLARE $ts_orig AS Timestamp64?;
DECLARE $seqno AS Int64?;

$current = (
    SELECT AsTuple(exp_pos, seqno, ts_orig)
    FROM %s
    WHERE client_id = $client_id
    LIMIT 1
);

UPSERT INTO %s (client_id, exp_pos, ts_wall, seqno, ts_orig)
VALUES ($client_id, Ensure($new_exp_pos,
			COALESCE($current.0, ""u) == $old_exp_pos,
			"position mismatch"),
	$ts_wall_us, COALESCE($seqno, $current.1), COALESCE($ts_orig, $current.2));
`, quoteYDBPath(w.positionTable), quoteYDBPath(w.positionTable)),
			ydb.ParamsBuilder().
				Param("$client_id").Text(clientID).
				Param("$old_exp_pos").Text(expectedPosition).
				Param("$new_exp_pos").Text(nextPosition).
				Param("$ts_wall_us").Timestamp64(update.TSWall).
				Param("$ts_orig").BeginOptional().Timestamp64(update.MaxTSOrig).EndOptional().
				Param("$seqno").BeginOptional().Int64(update.MaxSeqNo).EndOptional().
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

func (w *YDBWriter) SetPositionUnconditional(ctx context.Context, clientID, nextPosition string, update PositionUpdate) error {
	err := w.driver.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
		// Same preserve-on-NULL semantics as SetPosition (see comment there).
		_, err := tx.Execute(
			ctx,
			fmt.Sprintf(`
DECLARE $client_id AS Utf8;
DECLARE $exp_pos AS Utf8;
DECLARE $ts_wall_us AS Timestamp64;
DECLARE $ts_orig AS Timestamp64?;
DECLARE $seqno AS Int64?;

$current = (
    SELECT AsTuple(seqno, ts_orig)
    FROM %s
    WHERE client_id = $client_id
    LIMIT 1
);

UPSERT INTO %s (client_id, exp_pos, ts_wall, seqno, ts_orig)
VALUES ($client_id, $exp_pos, $ts_wall_us, COALESCE($seqno, $current.0), COALESCE($ts_orig, $current.1));
`, quoteYDBPath(w.positionTable), quoteYDBPath(w.positionTable)),
			ydb.ParamsBuilder().
				Param("$client_id").Text(clientID).
				Param("$exp_pos").Text(nextPosition).
				Param("$ts_wall_us").Timestamp64(update.TSWall).
				Param("$ts_orig").BeginOptional().Timestamp64(update.MaxTSOrig).EndOptional().
				Param("$seqno").BeginOptional().Int64(update.MaxSeqNo).EndOptional().
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

// encodeYDBValue converts a mapper Go value into a YDB typed value matching
// the table column type from DescribeTable (Optional wrappers are stripped).
// Go type alone is not enough: e.g. transform "int" yields Go int (would become
// Int64), and a bare string must become Timestamp64 when the column requires it.
func encodeYDBValue(value interface{}, columnType types.Type) (types.Value, error) {
	targetType := columnType
	if optional, inner := types.IsOptional(columnType); optional {
		targetType = inner
	}
	if targetType == nil {
		return nil, fmt.Errorf("nil YDB column type for Go value %T", value)
	}

	switch {
	case types.Equal(targetType, types.TypeUTF8):
		s, err := valueAsString(value)
		if err != nil {
			return nil, encodeTypeError(value, targetType, err)
		}
		return types.UTF8Value(s), nil
	case types.Equal(targetType, types.TypeBytes):
		b, err := valueAsBytes(value)
		if err != nil {
			return nil, encodeTypeError(value, targetType, err)
		}
		return types.BytesValue(b), nil
	case types.Equal(targetType, types.TypeBool):
		b, err := valueAsBool(value)
		if err != nil {
			return nil, encodeTypeError(value, targetType, err)
		}
		return types.BoolValue(b), nil
	case types.Equal(targetType, types.TypeInt8):
		n, err := valueAsSignedInt(value, math.MinInt8, math.MaxInt8)
		if err != nil {
			return nil, encodeTypeError(value, targetType, err)
		}
		return types.Int8Value(int8(n)), nil
	case types.Equal(targetType, types.TypeInt16):
		n, err := valueAsSignedInt(value, math.MinInt16, math.MaxInt16)
		if err != nil {
			return nil, encodeTypeError(value, targetType, err)
		}
		return types.Int16Value(int16(n)), nil
	case types.Equal(targetType, types.TypeInt32):
		n, err := valueAsSignedInt(value, math.MinInt32, math.MaxInt32)
		if err != nil {
			return nil, encodeTypeError(value, targetType, err)
		}
		return types.Int32Value(int32(n)), nil
	case types.Equal(targetType, types.TypeInt64):
		n, err := valueAsSignedInt(value, math.MinInt64, math.MaxInt64)
		if err != nil {
			return nil, encodeTypeError(value, targetType, err)
		}
		return types.Int64Value(n), nil
	case types.Equal(targetType, types.TypeUint8):
		n, err := valueAsUnsignedInt(value, math.MaxUint8)
		if err != nil {
			return nil, encodeTypeError(value, targetType, err)
		}
		return types.Uint8Value(uint8(n)), nil
	case types.Equal(targetType, types.TypeUint16):
		n, err := valueAsUnsignedInt(value, math.MaxUint16)
		if err != nil {
			return nil, encodeTypeError(value, targetType, err)
		}
		return types.Uint16Value(uint16(n)), nil
	case types.Equal(targetType, types.TypeUint32):
		n, err := valueAsUnsignedInt(value, math.MaxUint32)
		if err != nil {
			return nil, encodeTypeError(value, targetType, err)
		}
		return types.Uint32Value(uint32(n)), nil
	case types.Equal(targetType, types.TypeUint64):
		n, err := valueAsUnsignedInt(value, math.MaxUint64)
		if err != nil {
			return nil, encodeTypeError(value, targetType, err)
		}
		return types.Uint64Value(n), nil
	case types.Equal(targetType, types.TypeFloat):
		f, err := valueAsFloat64(value)
		if err != nil {
			return nil, encodeTypeError(value, targetType, err)
		}
		return types.FloatValue(float32(f)), nil
	case types.Equal(targetType, types.TypeDouble):
		f, err := valueAsFloat64(value)
		if err != nil {
			return nil, encodeTypeError(value, targetType, err)
		}
		return types.DoubleValue(f), nil
	case types.Equal(targetType, types.TypeTimestamp64):
		ts, err := valueAsTimestamp(value)
		if err != nil {
			return nil, encodeTypeError(value, targetType, err)
		}
		return types.Timestamp64Value(ts.UTC().UnixMicro()), nil
	case types.Equal(targetType, types.TypeTimestamp):
		ts, err := valueAsTimestamp(value)
		if err != nil {
			return nil, encodeTypeError(value, targetType, err)
		}
		return types.TimestampValueFromTime(ts.UTC()), nil
	case types.Equal(targetType, types.TypeJSON):
		s, err := valueAsString(value)
		if err != nil {
			return nil, encodeTypeError(value, targetType, err)
		}
		return types.JSONValue(s), nil
	case types.Equal(targetType, types.TypeJSONDocument):
		s, err := valueAsString(value)
		if err != nil {
			return nil, encodeTypeError(value, targetType, err)
		}
		return types.JSONDocumentValue(s), nil
	case types.Equal(targetType, types.TypeYSON):
		b, err := valueAsBytes(value)
		if err != nil {
			return nil, encodeTypeError(value, targetType, err)
		}
		return types.YSONValueFromBytes(b), nil
	default:
		return nil, fmt.Errorf("unsupported YDB column type %s for Go value %T", targetType.Yql(), value)
	}
}

func encodeTypeError(value interface{}, targetType types.Type, err error) error {
	return fmt.Errorf("cannot encode Go %T as YDB %s: %w", value, targetType.Yql(), err)
}

func valueAsString(value interface{}) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	case bool:
		return strconv.FormatBool(v), nil
	case int:
		return strconv.Itoa(v), nil
	case int8:
		return strconv.FormatInt(int64(v), 10), nil
	case int16:
		return strconv.FormatInt(int64(v), 10), nil
	case int32:
		return strconv.FormatInt(int64(v), 10), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case uint:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint8:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint16:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint64:
		return strconv.FormatUint(v, 10), nil
	case float32:
		return strconv.FormatFloat(float64(v), 'g', -1, 32), nil
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64), nil
	case time.Time:
		return v.UTC().Format(time.RFC3339Nano), nil
	default:
		return "", fmt.Errorf("unsupported source type")
	}
}

func valueAsBytes(value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	default:
		return nil, fmt.Errorf("unsupported source type")
	}
}

func valueAsBool(value interface{}) (bool, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case string:
		b, err := strconv.ParseBool(strings.TrimSpace(v))
		if err != nil {
			return false, err
		}
		return b, nil
	default:
		return false, fmt.Errorf("unsupported source type")
	}
}

func valueAsSignedInt(value interface{}, min, max int64) (int64, error) {
	var n int64
	switch v := value.(type) {
	case int:
		n = int64(v)
	case int8:
		n = int64(v)
	case int16:
		n = int64(v)
	case int32:
		n = int64(v)
	case int64:
		n = v
	case uint:
		if uint64(v) > math.MaxInt64 {
			return 0, fmt.Errorf("value %d overflows signed range", v)
		}
		n = int64(v)
	case uint8:
		n = int64(v)
	case uint16:
		n = int64(v)
	case uint32:
		n = int64(v)
	case uint64:
		if v > math.MaxInt64 {
			return 0, fmt.Errorf("value %d overflows signed range", v)
		}
		n = int64(v)
	case float32:
		if float64(v) != math.Trunc(float64(v)) {
			return 0, fmt.Errorf("value %v is not an integer", v)
		}
		n = int64(v)
	case float64:
		if v != math.Trunc(v) {
			return 0, fmt.Errorf("value %v is not an integer", v)
		}
		n = int64(v)
	case string:
		parsed, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		if err != nil {
			return 0, err
		}
		n = parsed
	default:
		return 0, fmt.Errorf("unsupported source type")
	}
	if n < min || n > max {
		return 0, fmt.Errorf("value %d out of range [%d, %d]", n, min, max)
	}
	return n, nil
}

func valueAsUnsignedInt(value interface{}, max uint64) (uint64, error) {
	var n uint64
	switch v := value.(type) {
	case uint:
		n = uint64(v)
	case uint8:
		n = uint64(v)
	case uint16:
		n = uint64(v)
	case uint32:
		n = uint64(v)
	case uint64:
		n = v
	case int:
		if v < 0 {
			return 0, fmt.Errorf("value %d is negative", v)
		}
		n = uint64(v)
	case int8:
		if v < 0 {
			return 0, fmt.Errorf("value %d is negative", v)
		}
		n = uint64(v)
	case int16:
		if v < 0 {
			return 0, fmt.Errorf("value %d is negative", v)
		}
		n = uint64(v)
	case int32:
		if v < 0 {
			return 0, fmt.Errorf("value %d is negative", v)
		}
		n = uint64(v)
	case int64:
		if v < 0 {
			return 0, fmt.Errorf("value %d is negative", v)
		}
		n = uint64(v)
	case float32:
		if v < 0 || float64(v) != math.Trunc(float64(v)) {
			return 0, fmt.Errorf("value %v is not a non-negative integer", v)
		}
		n = uint64(v)
	case float64:
		if v < 0 || v != math.Trunc(v) {
			return 0, fmt.Errorf("value %v is not a non-negative integer", v)
		}
		n = uint64(v)
	case string:
		parsed, err := strconv.ParseUint(strings.TrimSpace(v), 10, 64)
		if err != nil {
			return 0, err
		}
		n = parsed
	default:
		return 0, fmt.Errorf("unsupported source type")
	}
	if n > max {
		return 0, fmt.Errorf("value %d out of range [0, %d]", n, max)
	}
	return n, nil
}

func valueAsFloat64(value interface{}) (float64, error) {
	switch v := value.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil {
			return 0, err
		}
		return f, nil
	default:
		return 0, fmt.Errorf("unsupported source type")
	}
}

// valueAsTimestamp accepts time.Time from timestamp transforms, numeric Unix
// microseconds, or datetime strings (same rules as transform timestamp64).
func valueAsTimestamp(value interface{}) (time.Time, error) {
	switch v := value.(type) {
	case time.Time:
		return v.UTC(), nil
	case int:
		return time.UnixMicro(int64(v)).UTC(), nil
	case int32:
		return time.UnixMicro(int64(v)).UTC(), nil
	case int64:
		return time.UnixMicro(v).UTC(), nil
	case uint:
		return time.UnixMicro(int64(v)).UTC(), nil
	case uint32:
		return time.UnixMicro(int64(v)).UTC(), nil
	case uint64:
		if v > math.MaxInt64 {
			return time.Time{}, fmt.Errorf("value %d overflows timestamp microseconds", v)
		}
		return time.UnixMicro(int64(v)).UTC(), nil
	case string:
		return parseTimestamp64(strings.TrimSpace(v))
	case []byte:
		return parseTimestamp64(strings.TrimSpace(string(v)))
	default:
		return time.Time{}, fmt.Errorf("unsupported source type")
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
		value, err := encodeYDBValue(raw, column.Type)
		if err != nil {
			return nil, fmt.Errorf("encode %s: %w", column.Name, err)
		}
		// Nullable YDB columns are Optional(T). Present values must be wrapped
		// with OptionalValue; bare primitives mismatch the column type.
		if optional, _ := types.IsOptional(column.Type); optional {
			value = types.OptionalValue(value)
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

func openYDBDriver(ctx context.Context, endpoint, database string, auth YDBAuthOptions, openTimeout time.Duration) (*ydb.Driver, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("ydb endpoint setting is required")
	}
	if database == "" {
		return nil, fmt.Errorf("ydb database setting is required")
	}
	dsn := fmt.Sprintf("%s/%s", endpoint, database)
	authOption, err := ydbAuthOption(auth)
	if err != nil {
		return nil, err
	}
	opts := []ydb.Option{
		authOption,
	}
	if caPath := strings.TrimSpace(auth.CACertPath); caPath != "" {
		opts = append(opts, ydb.WithCertificatesFromFile(caPath))
	}
	effectiveOpenTimeout := openTimeout
	if effectiveOpenTimeout <= 0 {
		effectiveOpenTimeout = defaultYDBOpenTimeout
	}
	openCtx := ctx
	cancel := func() {}
	appliedDefaultTimeout := false
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		openCtx, cancel = context.WithTimeout(ctx, effectiveOpenTimeout)
		appliedDefaultTimeout = true
	}
	defer cancel()

	driver, err := ydb.Open(openCtx, dsn, opts...)
	if err != nil {
		if appliedDefaultTimeout && errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("open ydb connection timed out after %s: %w", effectiveOpenTimeout, err)
		}
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
