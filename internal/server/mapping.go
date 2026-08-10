package server

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-faster/city"
	"github.com/ydb-platform/loglugger/internal/models"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

// FieldMapping maps source field path to destination column.
type FieldMapping struct {
	Source      string  `json:"source" yaml:"source"`
	Destination string  `json:"destination" yaml:"destination"`
	Default     *string `json:"default,omitempty" yaml:"default,omitempty"`
	Transform   string  `json:"transform,omitempty" yaml:"transform,omitempty"`
}

// Mapper maps records to table rows using field mapping.
type Mapper interface {
	MapRecord(clientID string, rec models.Record) (map[string]interface{}, error)
}

type mapper struct {
	mappings []FieldMapping
}

// NewMapper creates a mapper from field mappings.
func NewMapper(mappings []FieldMapping) Mapper {
	return &mapper{
		mappings: mappings,
	}
}

func (m *mapper) MapRecord(clientID string, rec models.Record) (map[string]interface{}, error) {
	row := make(map[string]interface{})
	for _, fm := range m.mappings {
		var v string
		var ok bool
		var err error
		if fm.Source == "client_id" {
			v, ok = clientID, clientID != ""
		} else if fm.Source == "log_timestamp_us" {
			v, ok = logTimestampMicroseconds(rec), true
		} else if fm.Source == "message_cityhash64" {
			v, err = messageCityHash64(rec)
			if err != nil {
				return nil, fmt.Errorf("map %s -> %s: %w", fm.Source, fm.Destination, err)
			}
			ok = true
		} else {
			v, ok = rec.GetField(fm.Source)
		}
		if !ok && fm.Default != nil {
			v = *fm.Default
			ok = true
		}
		if !ok {
			continue
		}
		value, err := applyTransform(v, fm.Transform)
		if err != nil {
			return nil, fmt.Errorf("map %s -> %s: %w", fm.Source, fm.Destination, err)
		}
		row[fm.Destination] = value
	}
	return row, nil
}

func logTimestampMicroseconds(rec models.Record) string {
	if rec.RealtimeTS != nil {
		return strconv.FormatInt(*rec.RealtimeTS, 10)
	}
	return strconv.FormatInt(time.Now().UTC().UnixMicro(), 10)
}

func messageCityHash64(rec models.Record) (string, error) {
	data, err := fullMessageBytes(rec)
	if err != nil {
		return "", err
	}
	return strconv.FormatUint(city.CH64(data), 10), nil
}

func fullMessageBytes(rec models.Record) ([]byte, error) {
	payload := map[string]interface{}{
		"message":           rec.Message,
		"syslog_identifier": rec.SyslogIdentifier,
		"systemd_unit":      rec.SystemdUnit,
		"realtime_ts":       rec.RealtimeTS,
		"priority":          rec.Priority,
	}
	if rec.SeqNo != nil {
		payload["seqno"] = rec.SeqNo
	}
	if len(rec.Parsed) > 0 {
		payload["parsed"] = stableStringMap(rec.Parsed)
	}
	if len(rec.Fields) > 0 {
		payload["fields"] = stableStringMap(rec.Fields)
	}
	return json.Marshal(payload)
}

func stableStringMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	keys := make([]string, 0, len(in))
	for key := range in {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		out[key] = in[key]
	}
	return out
}

func applyTransform(value string, transform string) (interface{}, error) {
	switch strings.TrimSpace(transform) {
	case "", "string":
		// Remains a Go string; YDB encoding uses the destination column type
		// (e.g. Timestamp64 parses datetime strings, Int32 parses digits).
		return value, nil
	case "int":
		// Go int is platform-sized; encodeYDBValue narrows/widens to the
		// destination integer column (Int32/Int64/…).
		v, err := strconv.Atoi(value)
		if err != nil {
			return nil, err
		}
		return v, nil
	case "int64":
		v, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, err
		}
		return v, nil
	case "uint64":
		v, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return nil, err
		}
		return v, nil
	case "float64":
		v, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return nil, err
		}
		return v, nil
	case "bool":
		v, err := strconv.ParseBool(value)
		if err != nil {
			return nil, err
		}
		return v, nil
	case "rfc3339":
		v, err := time.Parse(time.RFC3339, value)
		if err != nil {
			return nil, err
		}
		return v, nil
	case "timestamp64_us":
		us, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		if err != nil {
			return nil, err
		}
		return time.UnixMicro(us).UTC(), nil
	case "timestamp64":
		return parseTimestamp64(strings.TrimSpace(value))
	default:
		return nil, fmt.Errorf("unsupported transform %q", transform)
	}
}

// ValidateFieldMappingsAgainstColumns checks that each mapping destination
// exists in the table schema and that the configured transform is compatible
// with the column type. Call this after DescribeTable so mismatches surface
// before BulkUpsert.
func ValidateFieldMappingsAgainstColumns(mappings []FieldMapping, columns []options.Column) error {
	byName := make(map[string]types.Type, len(columns))
	for _, column := range columns {
		byName[column.Name] = column.Type
	}
	for _, fm := range mappings {
		dest := strings.TrimSpace(fm.Destination)
		if dest == "" {
			return fmt.Errorf("mapping %s: empty destination", fm.Source)
		}
		columnType, ok := byName[dest]
		if !ok {
			return fmt.Errorf("mapping %s -> %s: destination column not found in table schema", fm.Source, dest)
		}
		if err := checkTransformCompatibleWithColumn(fm.Transform, columnType); err != nil {
			return fmt.Errorf("mapping %s -> %s: %w", fm.Source, dest, err)
		}
	}
	return nil
}

func checkTransformCompatibleWithColumn(transform string, columnType types.Type) error {
	targetType := columnType
	if optional, inner := types.IsOptional(columnType); optional {
		targetType = inner
	}
	if targetType == nil {
		return fmt.Errorf("nil column type")
	}
	switch strings.TrimSpace(transform) {
	case "", "string":
		// Encoder coerces strings using the column type (Utf8, numbers, timestamps, …).
		return nil
	case "int", "int64":
		if isSignedIntegerColumn(targetType) || isUnsignedIntegerColumn(targetType) || isFloatColumn(targetType) || isTextualColumn(targetType) {
			return nil
		}
		return fmt.Errorf("transform %q is incompatible with column type %s", transform, targetType.Yql())
	case "uint64":
		if isUnsignedIntegerColumn(targetType) || isSignedIntegerColumn(targetType) || isFloatColumn(targetType) || isTextualColumn(targetType) {
			return nil
		}
		return fmt.Errorf("transform %q is incompatible with column type %s", transform, targetType.Yql())
	case "float64":
		if isFloatColumn(targetType) || isTextualColumn(targetType) {
			return nil
		}
		return fmt.Errorf("transform %q is incompatible with column type %s", transform, targetType.Yql())
	case "bool":
		if types.Equal(targetType, types.TypeBool) || isTextualColumn(targetType) {
			return nil
		}
		return fmt.Errorf("transform %q is incompatible with column type %s", transform, targetType.Yql())
	case "rfc3339", "timestamp64", "timestamp64_us":
		if isTimestampColumn(targetType) || isTextualColumn(targetType) {
			return nil
		}
		return fmt.Errorf("transform %q is incompatible with column type %s", transform, targetType.Yql())
	default:
		return fmt.Errorf("unsupported transform %q", transform)
	}
}

func isTextualColumn(t types.Type) bool {
	return types.Equal(t, types.TypeUTF8) ||
		types.Equal(t, types.TypeBytes) ||
		types.Equal(t, types.TypeJSON) ||
		types.Equal(t, types.TypeJSONDocument) ||
		types.Equal(t, types.TypeYSON)
}

func isSignedIntegerColumn(t types.Type) bool {
	return types.Equal(t, types.TypeInt8) ||
		types.Equal(t, types.TypeInt16) ||
		types.Equal(t, types.TypeInt32) ||
		types.Equal(t, types.TypeInt64)
}

func isUnsignedIntegerColumn(t types.Type) bool {
	return types.Equal(t, types.TypeUint8) ||
		types.Equal(t, types.TypeUint16) ||
		types.Equal(t, types.TypeUint32) ||
		types.Equal(t, types.TypeUint64)
}

func isFloatColumn(t types.Type) bool {
	return types.Equal(t, types.TypeFloat) || types.Equal(t, types.TypeDouble)
}

func isTimestampColumn(t types.Type) bool {
	return types.Equal(t, types.TypeTimestamp) || types.Equal(t, types.TypeTimestamp64)
}

func parseTimestamp64(value string) (time.Time, error) {
	// Numeric input is interpreted as microseconds since Unix epoch.
	if us, err := strconv.ParseInt(value, 10, 64); err == nil {
		return time.UnixMicro(us).UTC(), nil
	}

	withZone := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05Z07:00",
		"2006-01-02 15:04:05.999999Z07:00",
	}
	for _, layout := range withZone {
		if ts, err := time.Parse(layout, value); err == nil {
			return ts, nil
		}
	}

	withoutZone := []string{
		"2006-01-02T15:04:05.999999",
		"2006-01-02 15:04:05.999999",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
	for _, layout := range withoutZone {
		if ts, err := time.ParseInLocation(layout, value, time.UTC); err == nil {
			return ts, nil
		}
	}

	return time.Time{}, fmt.Errorf("cannot parse timestamp64 value %q", value)
}
