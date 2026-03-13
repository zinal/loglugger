package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mzinal/loglugger/internal/models"
)

func TestHandler_ResetBatch(t *testing.T) {
	positions := NewMemoryPositionStore()
	mapper := NewMapper([]FieldMapping{
		{Source: "message", Destination: "msg"},
		{Source: "client_id", Destination: "client_id"},
	})
	writer := NewMockWriter()
	handler := NewHandler(positions, mapper, writer, "logs")

	req := &models.BatchRequest{
		ClientID:     "test-client",
		Reset:        true,
		NextPosition: "pos-001",
		Records: []models.Record{
			{Message: "hello"},
		},
	}
	body, _ := json.Marshal(req)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/batches", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")

	handler.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
	var resp models.BatchResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.Status != "ok" {
		t.Errorf("status = %q, want ok", resp.Status)
	}
	if resp.NextPosition != "pos-001" {
		t.Errorf("next_position = %q, want pos-001", resp.NextPosition)
	}
	if len(writer.Rows) != 1 {
		t.Errorf("writer rows = %d, want 1", len(writer.Rows))
	}
	if writer.Rows[0]["msg"] != "hello" {
		t.Errorf("row msg = %v, want hello", writer.Rows[0]["msg"])
	}
}

func TestHandler_PositionMismatch(t *testing.T) {
	ctx := context.Background()
	positions := NewMemoryPositionStore()
	_ = positions.Set(ctx, "client-1", "expected-pos")

	mapper := NewMapper([]FieldMapping{{Source: "message", Destination: "msg"}})
	writer := NewMockWriter()
	handler := NewHandler(positions, mapper, writer, "logs")

	req := &models.BatchRequest{
		ClientID:        "client-1",
		Reset:           false,
		CurrentPosition: "wrong-pos",
		NextPosition:    "next-pos",
		Records:         []models.Record{{Message: "x"}},
	}
	body, _ := json.Marshal(req)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/batches", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")

	handler.ServeHTTP(w, r)

	if w.Code != http.StatusConflict {
		t.Errorf("status = %d, want 409", w.Code)
	}
	var resp models.BatchResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.Status != "position_mismatch" {
		t.Errorf("status = %q, want position_mismatch", resp.Status)
	}
	if resp.ExpectedPosition != "expected-pos" {
		t.Errorf("expected_position = %q, want expected-pos", resp.ExpectedPosition)
	}
	if len(writer.Rows) != 0 {
		t.Errorf("writer should have 0 rows on mismatch, got %d", len(writer.Rows))
	}
}

func TestHandler_SequentialBatches(t *testing.T) {
	ctx := context.Background()
	positions := NewMemoryPositionStore()
	mapper := NewMapper([]FieldMapping{
		{Source: "message", Destination: "msg"},
		{Source: "client_id", Destination: "client_id"},
	})
	writer := NewMockWriter()
	handler := NewHandler(positions, mapper, writer, "logs")

	// First batch with reset
	sendBatch := func(req *models.BatchRequest) *models.BatchResponse {
		body, _ := json.Marshal(req)
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, "/v1/batches", bytes.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		handler.ServeHTTP(w, r)
		var resp models.BatchResponse
		_ = json.NewDecoder(w.Body).Decode(&resp)
		return &resp
	}

	resp1 := sendBatch(&models.BatchRequest{
		ClientID: "c1", Reset: true, NextPosition: "pos-1",
		Records: []models.Record{{Message: "first"}},
	})
	if resp1.Status != "ok" {
		t.Fatalf("batch 1: %s", resp1.Status)
	}

	resp2 := sendBatch(&models.BatchRequest{
		ClientID: "c1", Reset: false,
		CurrentPosition: "pos-1", NextPosition: "pos-2",
		Records: []models.Record{{Message: "second"}},
	})
	if resp2.Status != "ok" {
		t.Fatalf("batch 2: %s", resp2.Status)
	}

	if len(writer.Rows) != 2 {
		t.Errorf("rows = %d, want 2", len(writer.Rows))
	}
	exp, _, _ := positions.Get(ctx, "c1")
	if exp != "pos-2" {
		t.Errorf("stored position = %q, want pos-2", exp)
	}
}

func TestHandler_FieldMappingParsed(t *testing.T) {
	mapper := NewMapper([]FieldMapping{
		{Source: "parsed.P_DTTM", Destination: "log_dttm"},
		{Source: "parsed.P_SERVICE", Destination: "service_name"},
		{Source: "parsed.P_LEVEL", Destination: "log_level"},
		{Source: "parsed.P_MESSAGE", Destination: "log_message"},
		{Source: "client_id", Destination: "client_id"},
	})
	writer := NewMockWriter()
	handler := NewHandler(NewMemoryPositionStore(), mapper, writer, "logs")

	req := &models.BatchRequest{
		ClientID: "host-01", Reset: true, NextPosition: "p1",
		Records: []models.Record{
			{
				Parsed: map[string]string{
					"P_DTTM":    "2025-03-13T10:00:00",
					"P_SERVICE": "nginx",
					"P_LEVEL":   "INFO",
					"P_MESSAGE": "Server started",
				},
			},
		},
	}
	body, _ := json.Marshal(req)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/batches", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")

	handler.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d", w.Code)
	}
	if len(writer.Rows) != 1 {
		t.Fatalf("rows = %d", len(writer.Rows))
	}
	row := writer.Rows[0]
	if row["log_dttm"] != "2025-03-13T10:00:00" {
		t.Errorf("log_dttm = %v", row["log_dttm"])
	}
	if row["service_name"] != "nginx" {
		t.Errorf("service_name = %v", row["service_name"])
	}
	if row["log_level"] != "INFO" {
		t.Errorf("log_level = %v", row["log_level"])
	}
	if row["log_message"] != "Server started" {
		t.Errorf("log_message = %v", row["log_message"])
	}
}

func TestHandler_GetPositionFound(t *testing.T) {
	ctx := context.Background()
	positions := NewMemoryPositionStore()
	_ = positions.Set(ctx, "client-1", "cursor-9")
	handler := NewHandler(positions, NewMapper([]FieldMapping{{Source: "message", Destination: "msg"}}), NewMockWriter(), "logs")

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/v1/positions?client_id=client-1", nil)
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var resp models.PositionResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.Status != "ok" || resp.CurrentPosition != "cursor-9" {
		t.Fatalf("response = %+v, want ok/cursor-9", resp)
	}
}

func TestHandler_GetPositionNotFound(t *testing.T) {
	handler := NewHandler(NewMemoryPositionStore(), NewMapper([]FieldMapping{{Source: "message", Destination: "msg"}}), NewMockWriter(), "logs")

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/v1/positions?client_id=missing", nil)
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var resp models.PositionResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.Status != "not_found" {
		t.Fatalf("status = %q, want not_found", resp.Status)
	}
}

func TestHandler_ContentTypeWithCharset(t *testing.T) {
	handler := NewHandler(NewMemoryPositionStore(), NewMapper([]FieldMapping{{Source: "message", Destination: "msg"}}), NewMockWriter(), "logs")
	req := &models.BatchRequest{
		ClientID:     "client-1",
		Reset:        true,
		NextPosition: "pos-1",
		Records:      []models.Record{{Message: "hello"}},
	}
	body, _ := json.Marshal(req)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/batches", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json; charset=utf-8")

	handler.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
}

func TestHandler_WriteFailureDoesNotAdvancePosition(t *testing.T) {
	ctx := context.Background()
	positions := NewMemoryPositionStore()
	_ = positions.Set(ctx, "client-1", "pos-1")
	handler := NewHandler(positions, NewMapper([]FieldMapping{{Source: "message", Destination: "msg"}}), errorWriter{err: errors.New("boom")}, "logs")

	resp := handler.handle(ctx, &models.BatchRequest{
		ClientID:        "client-1",
		CurrentPosition: "pos-1",
		NextPosition:    "pos-2",
		Records:         []models.Record{{Message: "x"}},
	})

	if resp.Status != "error" {
		t.Fatalf("status = %q, want error", resp.Status)
	}
	stored, _, _ := positions.Get(ctx, "client-1")
	if stored != "pos-1" {
		t.Fatalf("stored position = %q, want pos-1", stored)
	}
}

func TestHandler_PositionStoreErrorReturnsFailure(t *testing.T) {
	handler := NewHandler(positionStoreStub{setErr: errors.New("store failed")}, NewMapper([]FieldMapping{{Source: "message", Destination: "msg"}}), NewMockWriter(), "logs")

	resp := handler.handle(context.Background(), &models.BatchRequest{
		ClientID:     "client-1",
		Reset:        true,
		NextPosition: "pos-1",
		Records:      []models.Record{{Message: "hello"}},
	})

	if resp.Status != "error" {
		t.Fatalf("status = %q, want error", resp.Status)
	}
}

type errorWriter struct{ err error }

func (w errorWriter) BulkUpsert(ctx context.Context, table string, rows []map[string]interface{}) error {
	return w.err
}

type positionStoreStub struct {
	getPos string
	getOK  bool
	getErr error
	setErr error
}

func (s positionStoreStub) Get(ctx context.Context, clientID string) (string, bool, error) {
	return s.getPos, s.getOK, s.getErr
}

func (s positionStoreStub) Set(ctx context.Context, clientID, position string) error {
	return s.setErr
}
