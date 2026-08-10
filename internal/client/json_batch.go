package client

import (
	"encoding/json"
)

// encodeBatchRequestJSON builds the POST /v1/batches body from pre-marshaled
// record JSON objects. Record bytes are embedded as-is (no re-marshal).
func encodeBatchRequestJSON(clientID string, reset bool, currentPos, nextPos string, recordJSONs []json.RawMessage) ([]byte, error) {
	return json.Marshal(batchRequestWire{
		ClientID:        clientID,
		Reset:           reset,
		CurrentPosition: currentPos,
		NextPosition:    nextPos,
		Records:         recordJSONs,
	})
}

// batchRequestWire mirrors models.BatchRequest field order and omitempty rules
// so size estimates match encoding/json output.
type batchRequestWire struct {
	ClientID        string            `json:"client_id"`
	Reset           bool              `json:"reset"`
	CurrentPosition string            `json:"current_position,omitempty"`
	NextPosition    string            `json:"next_position"`
	Records         []json.RawMessage `json:"records"`
}

func boolJSONLen(v bool) int {
	if v {
		return len("true")
	}
	return len("false")
}

func jsonStringWireLen(s string) int {
	// json.Marshal on a string matches the escaping used for struct string fields
	// (including \u0026 / \u003c / \u003e).
	raw, err := json.Marshal(s)
	if err != nil {
		// encoding/json does not fail for string values.
		return len(`""`) + len(s)
	}
	return len(raw)
}

// batchRequestJSONSize returns the exact byte length of encodeBatchRequestJSON
// for the given envelope and records-array size (including '[' / ']' / commas).
func batchRequestJSONSize(clientIDJSONLen int, reset bool, currentPos, nextPos string, recordsArrayBytes int) int {
	// {"client_id":...,"reset":...,"next_position":...,"records":...}
	n := len(`{"client_id":`) + clientIDJSONLen +
		len(`,"reset":`) + boolJSONLen(reset) +
		len(`,"next_position":`) + jsonStringWireLen(nextPos) +
		len(`,"records":`) + recordsArrayBytes +
		len(`}`)
	if currentPos != "" {
		n += len(`,"current_position":`) + jsonStringWireLen(currentPos)
	}
	return n
}

func recordsArrayJSONBytes(recordJSONs []json.RawMessage) int {
	if len(recordJSONs) == 0 {
		return len("[]")
	}
	n := len("[]")
	for i, raw := range recordJSONs {
		if i > 0 {
			n++ // comma
		}
		n += len(raw)
	}
	return n
}
