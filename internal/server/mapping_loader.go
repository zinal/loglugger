package server

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

type fieldMappingFile struct {
	FieldMapping []FieldMapping `json:"field_mapping" yaml:"field_mapping"`
}

// LoadFieldMappings reads mappings from a YAML or JSON file.
func LoadFieldMappings(path string) ([]FieldMapping, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read mapping file: %w", err)
	}

	var mappings []FieldMapping
	if err := decodeFieldMappings(path, data, &mappings); err == nil && len(mappings) > 0 {
		return mappings, nil
	}

	var wrapped fieldMappingFile
	if err := decodeFieldMappings(path, data, &wrapped); err != nil {
		return nil, err
	}
	if len(wrapped.FieldMapping) == 0 {
		return nil, fmt.Errorf("mapping file %q does not contain any field mappings", path)
	}
	return wrapped.FieldMapping, nil
}

func decodeFieldMappings(path string, data []byte, out interface{}) error {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".json":
		if err := json.Unmarshal(data, out); err != nil {
			return fmt.Errorf("decode JSON mapping file: %w", err)
		}
	default:
		if err := yaml.Unmarshal(data, out); err != nil {
			return fmt.Errorf("decode YAML mapping file: %w", err)
		}
	}
	return nil
}
