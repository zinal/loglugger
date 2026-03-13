package loglugger_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ydb-platform/loglugger/internal/models"
	"github.com/ydb-platform/loglugger/internal/server"
)

// TestFunctional_ClientServerFlow tests the full batch submission flow:
// client sends batch -> server validates position -> server writes to store.
func TestFunctional_ClientServerFlow(t *testing.T) {
	positions := server.NewMemoryPositionStore()
	mapper := server.NewMapper([]server.FieldMapping{
		{Source: "message", Destination: "message"},
		{Source: "parsed.P_DTTM", Destination: "log_dttm"},
		{Source: "parsed.P_SERVICE", Destination: "service_name"},
		{Source: "parsed.P_LEVEL", Destination: "log_level"},
		{Source: "parsed.P_MESSAGE", Destination: "log_message"},
		{Source: "client_id", Destination: "client_id"},
	})
	writer := server.NewMockWriter()
	handler := server.NewHandler(positions, mapper, writer, "logs")

	srv := httptest.NewServer(handler)
	defer srv.Close()

	client := &http.Client{}

	// 1. First batch with reset
	req1 := &models.BatchRequest{
		ClientID:       "test-client-1",
		Reset:          true,
		NextPosition:   "cursor-001",
		Records: []models.Record{
			{Message: "raw log line"},
			{
				Parsed: map[string]string{
					"P_DTTM":   "2025-03-13T10:00:00",
					"P_SERVICE": "nginx",
					"P_LEVEL":   "INFO",
					"P_MESSAGE": "Server started",
				},
			},
		},
	}
	body1, _ := json.Marshal(req1)
	resp1, err := client.Post(srv.URL+"/v1/batches", "application/json", bytes.NewReader(body1))
	if err != nil {
		t.Fatal(err)
	}
	defer resp1.Body.Close()

	if resp1.StatusCode != http.StatusOK {
		t.Fatalf("batch 1 status = %d", resp1.StatusCode)
	}
	var batchResp1 models.BatchResponse
	if err := json.NewDecoder(resp1.Body).Decode(&batchResp1); err != nil {
		t.Fatal(err)
	}
	if batchResp1.Status != "ok" {
		t.Fatalf("batch 1 status = %q", batchResp1.Status)
	}
	if batchResp1.NextPosition != "cursor-001" {
		t.Errorf("next_position = %q", batchResp1.NextPosition)
	}

	// 2. Second batch with position continuity
	req2 := &models.BatchRequest{
		ClientID:       "test-client-1",
		Reset:          false,
		CurrentPosition: "cursor-001",
		NextPosition:   "cursor-002",
		Records: []models.Record{{Message: "second batch"}},
	}
	body2, _ := json.Marshal(req2)
	resp2, err := client.Post(srv.URL+"/v1/batches", "application/json", bytes.NewReader(body2))
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("batch 2 status = %d", resp2.StatusCode)
	}

	// 3. Position mismatch - wrong current_position
	req3 := &models.BatchRequest{
		ClientID:       "test-client-1",
		Reset:          false,
		CurrentPosition: "wrong-cursor",
		NextPosition:   "cursor-003",
		Records:        []models.Record{{Message: "should reject"}},
	}
	body3, _ := json.Marshal(req3)
	resp3, err := client.Post(srv.URL+"/v1/batches", "application/json", bytes.NewReader(body3))
	if err != nil {
		t.Fatal(err)
	}
	defer resp3.Body.Close()

	if resp3.StatusCode != http.StatusConflict {
		t.Errorf("expected 409 on position mismatch, got %d", resp3.StatusCode)
	}
	var batchResp3 models.BatchResponse
	if err := json.NewDecoder(resp3.Body).Decode(&batchResp3); err != nil {
		t.Fatal(err)
	}
	if batchResp3.Status != "position_mismatch" {
		t.Errorf("status = %q", batchResp3.Status)
	}
	if batchResp3.ExpectedPosition != "cursor-002" {
		t.Errorf("expected_position = %q", batchResp3.ExpectedPosition)
	}

	// 4. Verify stored records
	if len(writer.Rows) != 3 {
		t.Errorf("writer rows = %d, want 3", len(writer.Rows))
	}
	ctx := context.Background()
	pos, ok, _ := positions.Get(ctx, "test-client-1")
	if !ok || pos != "cursor-002" {
		t.Errorf("stored position = %q (ok=%v), want cursor-002", pos, ok)
	}
}
