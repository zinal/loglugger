package server

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ydb-platform/loglugger/internal/models"
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
	return &mapper{mappings: mappings}
}

func (m *mapper) MapRecord(clientID string, rec models.Record) (map[string]interface{}, error) {
	row := make(map[string]interface{})
	for _, fm := range m.mappings {
		var v string
		var ok bool
		if fm.Source == "client_id" {
			v, ok = clientID, clientID != ""
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
	// Always include client_id if not already set by mapping
	if _, has := row["client_id"]; !has && clientID != "" {
		row["client_id"] = clientID
	}
	return row, nil
}

func applyTransform(value string, transform string) (interface{}, error) {
	switch strings.TrimSpace(transform) {
	case "", "string":
		return value, nil
	case "int":
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
	default:
		return nil, fmt.Errorf("unsupported transform %q", transform)
	}
}
