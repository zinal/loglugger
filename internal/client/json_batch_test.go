package client

import (
	"encoding/json"
	"testing"

	"github.com/ydb-platform/loglugger/internal/models"
)

func TestBatchRequestJSONSizeMatchesEncode(t *testing.T) {
	cases := []struct {
		name       string
		clientID   string
		reset      bool
		currentPos string
		nextPos    string
		records    []models.Record
	}{
		{
			name:     "reset without current position",
			clientID: "host-1",
			reset:    true,
			nextPos:  "cursor-1",
			records:  []models.Record{{Message: "hello"}},
		},
		{
			name:       "normal batch with escaped characters",
			clientID:   "host&1",
			reset:      false,
			currentPos: "cur<1>",
			nextPos:    "cur\"2\"",
			records: []models.Record{
				{Message: "a&b", Fields: map[string]string{"x": "1"}},
				{Message: "line\n2", SyslogIdentifier: "app"},
			},
		},
		{
			name:       "empty message still emits record object",
			clientID:   "c",
			reset:      false,
			currentPos: "p0",
			nextPos:    "p1",
			records:    []models.Record{{}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			recordJSONs := make([]json.RawMessage, 0, len(tc.records))
			for _, record := range tc.records {
				raw, err := json.Marshal(record)
				if err != nil {
					t.Fatal(err)
				}
				recordJSONs = append(recordJSONs, raw)
			}
			body, err := encodeBatchRequestJSON(tc.clientID, tc.reset, tc.currentPos, tc.nextPos, recordJSONs)
			if err != nil {
				t.Fatal(err)
			}
			got := batchRequestJSONSize(
				jsonStringWireLen(tc.clientID),
				tc.reset,
				tc.currentPos,
				tc.nextPos,
				recordsArrayJSONBytes(recordJSONs),
			)
			if got != len(body) {
				t.Fatalf("size = %d, len(body) = %d\nbody=%s", got, len(body), body)
			}

			// Cross-check against marshaling models.BatchRequest with the same records.
			ref, err := json.Marshal(&models.BatchRequest{
				ClientID:        tc.clientID,
				Reset:           tc.reset,
				CurrentPosition: tc.currentPos,
				NextPosition:    tc.nextPos,
				Records:         tc.records,
			})
			if err != nil {
				t.Fatal(err)
			}
			var viaRaw, viaModels models.BatchRequest
			if err := json.Unmarshal(body, &viaRaw); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(ref, &viaModels); err != nil {
				t.Fatal(err)
			}
			if viaRaw.ClientID != viaModels.ClientID || viaRaw.Reset != viaModels.Reset ||
				viaRaw.CurrentPosition != viaModels.CurrentPosition || viaRaw.NextPosition != viaModels.NextPosition ||
				len(viaRaw.Records) != len(viaModels.Records) {
				t.Fatalf("decoded mismatch:\n raw=%+v\n models=%+v", viaRaw, viaModels)
			}
		})
	}
}
