// Package configdecode provides strict YAML/JSON decoding for operator configs.
// Unknown fields are rejected so typos surface at startup instead of being ignored.
package configdecode

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// DecodeStrict unmarshals data into out, rejecting unknown fields.
// The format is selected by path extension: ".json" uses JSON; anything else uses YAML.
func DecodeStrict(path string, data []byte, out any) error {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".json":
		if err := decodeJSON(data, out); err != nil {
			return fmt.Errorf("decode JSON: %w", err)
		}
	default:
		if err := decodeYAML(data, out); err != nil {
			return fmt.Errorf("decode YAML: %w", err)
		}
	}
	return nil
}

func decodeJSON(data []byte, out any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return err
	}
	var trailing json.RawMessage
	if err := dec.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("extra data after top-level value")
		}
		return err
	}
	return nil
}

func decodeYAML(data []byte, out any) error {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(out); err != nil {
		return err
	}
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("extra document after top-level value")
		}
		return err
	}
	return nil
}
