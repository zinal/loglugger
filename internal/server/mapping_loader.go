package server

import (
	"fmt"
	"os"

	"github.com/ydb-platform/loglugger/internal/configdecode"
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
	if err := configdecode.DecodeStrict(path, data, &mappings); err == nil && len(mappings) > 0 {
		return mappings, nil
	}

	var wrapped fieldMappingFile
	if err := configdecode.DecodeStrict(path, data, &wrapped); err != nil {
		return nil, fmt.Errorf("decode mapping file: %w", err)
	}
	if len(wrapped.FieldMapping) == 0 {
		return nil, fmt.Errorf("mapping file %q does not contain any field mappings", path)
	}
	return wrapped.FieldMapping, nil
}
