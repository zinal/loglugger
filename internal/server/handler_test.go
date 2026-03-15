package server

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/ydb-platform/loglugger/internal/models"
)

func TestHandler_ResetBatch(t *testing.T) {
	mapper := NewMapper([]FieldMapping{
		{Source: "message", Destination: "msg"},
		{Source: "client_id", Destination: "client_id"},
	})
	writer := NewMockWriter()
	handler := NewHandler(mapper, writer, "logs")

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

func TestHandler_ResetBatchSkipsPositionValidation(t *testing.T) {
	writer := &resetWriterSpy{}
	handler := NewHandler(NewMapper([]FieldMapping{{Source: "message", Destination: "msg"}}), writer, "logs")

	resp := handler.handle(context.Background(), &models.BatchRequest{
		ClientID:     "client-1",
		Reset:        true,
		NextPosition: "pos-1",
		Records:      []models.Record{{Message: "hello"}},
	})

	if resp.Status != "ok" {
		t.Fatalf("status = %q, want ok", resp.Status)
	}
	if writer.getCalled {
		t.Fatal("reset batch should not read stored position")
	}
	if writer.setCalled {
		t.Fatal("reset batch should not use compare-and-set position update")
	}
	if !writer.setUnconditionalCalled {
		t.Fatal("reset batch should use unconditional position update")
	}
}

func TestHandler_PositionMismatch(t *testing.T) {
	ctx := context.Background()
	mapper := NewMapper([]FieldMapping{{Source: "message", Destination: "msg"}})
	writer := NewMockWriter()
	_ = writer.SetPosition(ctx, "client-1", "", "expected-pos")
	handler := NewHandler(mapper, writer, "logs")

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
	mapper := NewMapper([]FieldMapping{
		{Source: "message", Destination: "msg"},
		{Source: "client_id", Destination: "client_id"},
	})
	writer := NewMockWriter()
	handler := NewHandler(mapper, writer, "logs")

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
	exp, _, _ := writer.GetPosition(ctx, "c1")
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
	parser, err := NewMessageParser(`^(?P<P_DTTM>[^ ]*) :(?P<P_SERVICE>[^ ]*) (?P<P_LEVEL>[^ ]*): (?P<P_MESSAGE>.*)$`, NoMatchSendRaw)
	if err != nil {
		t.Fatal(err)
	}
	handler := NewHandlerWithParser(mapper, writer, "logs", parser)

	req := &models.BatchRequest{
		ClientID: "host-01", Reset: true, NextPosition: "p1",
		Records: []models.Record{
			{
				Message: "2025-03-13T10:00:00 :nginx INFO: Server started",
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
	writer := NewMockWriter()
	_ = writer.SetPosition(ctx, "client-1", "", "cursor-9")
	handler := NewHandler(NewMapper([]FieldMapping{{Source: "message", Destination: "msg"}}), writer, "logs")

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
	handler := NewHandler(NewMapper([]FieldMapping{{Source: "message", Destination: "msg"}}), NewMockWriter(), "logs")

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
	handler := NewHandler(NewMapper([]FieldMapping{{Source: "message", Destination: "msg"}}), NewMockWriter(), "logs")
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
	writer := &errorWriter{positions: map[string]string{"client-1": "pos-1"}, err: errors.New("boom")}
	handler := NewHandler(NewMapper([]FieldMapping{{Source: "message", Destination: "msg"}}), writer, "logs")

	resp := handler.handle(ctx, &models.BatchRequest{
		ClientID:        "client-1",
		CurrentPosition: "pos-1",
		NextPosition:    "pos-2",
		Records:         []models.Record{{Message: "x"}},
	})

	if resp.Status != "error" {
		t.Fatalf("status = %q, want error", resp.Status)
	}
	stored, _, _ := writer.GetPosition(ctx, "client-1")
	if stored != "pos-1" {
		t.Fatalf("stored position = %q, want pos-1", stored)
	}
}

func TestHandler_PositionStoreErrorReturnsFailure(t *testing.T) {
	handler := NewHandler(NewMapper([]FieldMapping{{Source: "message", Destination: "msg"}}), positionWriterStub{resetErr: errors.New("store failed")}, "logs")

	resp := handler.handle(context.Background(), &models.BatchRequest{
		ClientID:     "client-1",
		Reset:        true,
		NextPosition: "pos-1",
		Records:      []models.Record{{Message: "hello"}},
	})

	if resp.Status != "error" {
		t.Fatalf("status = %q, want error", resp.Status)
	}
	if resp.Message != genericStorageErrorMessage {
		t.Fatalf("message = %q, want %q", resp.Message, genericStorageErrorMessage)
	}
}

func TestHandler_RejectsRecordWithoutMessage(t *testing.T) {
	handler := NewHandler(NewMapper([]FieldMapping{{Source: "message", Destination: "msg"}}), NewMockWriter(), "logs")
	resp := handler.handle(context.Background(), &models.BatchRequest{
		ClientID:     "client-1",
		Reset:        true,
		NextPosition: "pos-1",
		Records: []models.Record{
			{},
		},
	})
	if resp.Status != "error" {
		t.Fatalf("status = %q, want error", resp.Status)
	}
}

func TestHandler_BulkUpsertErrorReturnsGenericMessage(t *testing.T) {
	handler := NewHandler(NewMapper([]FieldMapping{{Source: "message", Destination: "msg"}}), errorWriter{err: errors.New("ydb: permission denied"), positions: map[string]string{}}, "logs")
	resp := handler.handle(context.Background(), &models.BatchRequest{
		ClientID:     "client-1",
		Reset:        true,
		NextPosition: "pos-1",
		Records:      []models.Record{{Message: "hello"}},
	})
	if resp.Status != "error" {
		t.Fatalf("status = %q, want error", resp.Status)
	}
	if resp.Message != genericStorageErrorMessage {
		t.Fatalf("message = %q, want %q", resp.Message, genericStorageErrorMessage)
	}
}

func TestHandler_GetPositionErrorReturnsGenericMessage(t *testing.T) {
	handler := NewHandler(NewMapper([]FieldMapping{{Source: "message", Destination: "msg"}}), positionWriterStub{getErr: errors.New("ydb: timeout")}, "logs")

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/v1/positions?client_id=client-1", nil)
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
	var resp models.PositionResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.Status != "error" {
		t.Fatalf("status = %q, want error", resp.Status)
	}
	if resp.Message != genericStorageErrorMessage {
		t.Fatalf("message = %q, want %q", resp.Message, genericStorageErrorMessage)
	}
}

func TestHandler_InternalMappingErrorIsSanitizedForClient(t *testing.T) {
	handler := NewHandler(
		NewMapper([]FieldMapping{{Source: "message", Destination: "x", Transform: "int"}}),
		NewMockWriter(),
		"logs",
	)
	req := &models.BatchRequest{
		ClientID:     "client-1",
		Reset:        true,
		NextPosition: "pos-1",
		Records:      []models.Record{{Message: "hello"}},
	}
	body, _ := json.Marshal(req)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/batches", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
	var resp models.BatchResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.Status != "error" {
		t.Fatalf("status = %q, want error", resp.Status)
	}
	if resp.Message != genericStorageErrorMessage {
		t.Fatalf("message = %q, want %q", resp.Message, genericStorageErrorMessage)
	}
}

func TestHandler_RejectsMissingCurrentPositionWithoutReset(t *testing.T) {
	handler := NewHandler(NewMapper([]FieldMapping{{Source: "message", Destination: "msg"}}), NewMockWriter(), "logs")
	req := &models.BatchRequest{
		ClientID:     "client-1",
		NextPosition: "pos-1",
		Records:      []models.Record{{Message: "hello"}},
	}
	body, _ := json.Marshal(req)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/batches", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
	var resp models.BatchResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.Message != "current_position is required when reset is false" {
		t.Fatalf("message = %q", resp.Message)
	}
}

func TestHandler_AcceptsGzipEncodedBatch(t *testing.T) {
	handler := NewHandler(NewMapper([]FieldMapping{{Source: "message", Destination: "msg"}}), NewMockWriter(), "logs")
	req := &models.BatchRequest{
		ClientID:     "client-1",
		Reset:        true,
		NextPosition: "pos-1",
		Records:      []models.Record{{Message: "hello"}},
	}
	raw, _ := json.Marshal(req)
	compressed := gzipData(t, raw)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/batches", bytes.NewReader(compressed))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Content-Encoding", "gzip")
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
}

func TestHandler_RejectsUnsupportedContentEncoding(t *testing.T) {
	handler := NewHandler(NewMapper([]FieldMapping{{Source: "message", Destination: "msg"}}), NewMockWriter(), "logs")
	req := &models.BatchRequest{
		ClientID:     "client-1",
		Reset:        true,
		NextPosition: "pos-1",
		Records:      []models.Record{{Message: "hello"}},
	}
	body, _ := json.Marshal(req)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/batches", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Content-Encoding", "br")
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestHandler_RejectsTooLargeIdentityBody(t *testing.T) {
	handler := NewHandlerWithOptions(
		NewMapper([]FieldMapping{{Source: "message", Destination: "msg"}}),
		NewMockWriter(),
		"logs",
		nil,
		HandlerOptions{
			MaxCompressedBodyBytes:   128,
			MaxDecompressedBodyBytes: 1024,
		},
	)
	req := &models.BatchRequest{
		ClientID:     "client-1",
		Reset:        true,
		NextPosition: "pos-1",
		Records: []models.Record{
			{Message: strings.Repeat("a", 2048)},
		},
	}
	body, _ := json.Marshal(req)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/batches", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", w.Code)
	}
}

func TestHandler_RejectsTooLargeGzipDecodedBody(t *testing.T) {
	handler := NewHandlerWithOptions(
		NewMapper([]FieldMapping{{Source: "message", Destination: "msg"}}),
		NewMockWriter(),
		"logs",
		nil,
		HandlerOptions{
			MaxCompressedBodyBytes:   1 << 20,
			MaxDecompressedBodyBytes: 256,
		},
	)
	req := &models.BatchRequest{
		ClientID:     "client-1",
		Reset:        true,
		NextPosition: "pos-1",
		Records: []models.Record{
			{Message: strings.Repeat("x", 4096)},
		},
	}
	raw, _ := json.Marshal(req)
	compressed := gzipData(t, raw)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/batches", bytes.NewReader(compressed))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Content-Encoding", "gzip")
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", w.Code)
	}
}

func TestHandler_ConcurrentDifferentClients(t *testing.T) {
	mapper := NewMapper([]FieldMapping{
		{Source: "message", Destination: "msg"},
		{Source: "client_id", Destination: "client_id"},
	})
	writer := NewMockWriter()
	handler := NewHandler(mapper, writer, "logs")

	const workers = 32
	const totalRequests = 200

	var wg sync.WaitGroup
	errCh := make(chan string, totalRequests)
	for i := 0; i < totalRequests; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			clientID := "client-" + strconv.Itoa(i)
			req := &models.BatchRequest{
				ClientID:     clientID,
				Reset:        true,
				NextPosition: "pos-" + clientID + "-" + strconv.Itoa(i),
				Records:      []models.Record{{Message: "m-" + strconv.Itoa(i)}},
			}
			body, _ := json.Marshal(req)
			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodPost, "/v1/batches", bytes.NewReader(body))
			r.Header.Set("Content-Type", "application/json")
			handler.ServeHTTP(w, r)
			if w.Code != http.StatusOK {
				errCh <- "unexpected status code"
				return
			}
			var resp models.BatchResponse
			if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
				errCh <- "decode response failed"
				return
			}
			if resp.Status != "ok" {
				errCh <- "unexpected response status"
			}
		}(i)
		if (i+1)%workers == 0 {
			// Keep a bounded amount of concurrent goroutines for stable CI runtime.
			wg.Wait()
		}
	}
	wg.Wait()
	close(errCh)
	for errMsg := range errCh {
		t.Fatal(errMsg)
	}
	if len(writer.Rows) != totalRequests {
		t.Fatalf("rows = %d, want %d", len(writer.Rows), totalRequests)
	}
}

func gzipData(t *testing.T, in []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(in); err != nil {
		t.Fatalf("gzip write failed: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("gzip close failed: %v", err)
	}
	out, err := io.ReadAll(&buf)
	if err != nil {
		t.Fatalf("read compressed buffer failed: %v", err)
	}
	return out
}

type errorWriter struct {
	err       error
	positions map[string]string
}

func (w errorWriter) BulkUpsert(ctx context.Context, table string, rows []map[string]interface{}) error {
	return w.err
}

func (w errorWriter) GetPosition(ctx context.Context, clientID string) (string, bool, error) {
	position, ok := w.positions[clientID]
	return position, ok, nil
}

func (w errorWriter) SetPosition(ctx context.Context, clientID, expectedPosition, nextPosition string) error {
	current, ok := w.positions[clientID]
	if current != expectedPosition {
		return &PositionMismatchError{CurrentPosition: current, Found: ok}
	}
	w.positions[clientID] = nextPosition
	return nil
}

func (w errorWriter) SetPositionUnconditional(ctx context.Context, clientID, nextPosition string) error {
	w.positions[clientID] = nextPosition
	return nil
}

type positionWriterStub struct {
	getPos   string
	getOK    bool
	getErr   error
	setErr   error
	resetErr error
}

func (s positionWriterStub) BulkUpsert(ctx context.Context, table string, rows []map[string]interface{}) error {
	return nil
}

func (s positionWriterStub) GetPosition(ctx context.Context, clientID string) (string, bool, error) {
	return s.getPos, s.getOK, s.getErr
}

func (s positionWriterStub) SetPosition(ctx context.Context, clientID, expectedPosition, nextPosition string) error {
	return s.setErr
}

func (s positionWriterStub) SetPositionUnconditional(ctx context.Context, clientID, nextPosition string) error {
	return s.resetErr
}

type resetWriterSpy struct {
	getCalled              bool
	setCalled              bool
	setUnconditionalCalled bool
}

func (w *resetWriterSpy) BulkUpsert(ctx context.Context, table string, rows []map[string]interface{}) error {
	return nil
}

func (w *resetWriterSpy) GetPosition(ctx context.Context, clientID string) (string, bool, error) {
	w.getCalled = true
	return "pos-old", true, nil
}

func (w *resetWriterSpy) SetPosition(ctx context.Context, clientID, expectedPosition, nextPosition string) error {
	w.setCalled = true
	return &PositionMismatchError{CurrentPosition: "pos-new", Found: true}
}

func (w *resetWriterSpy) SetPositionUnconditional(ctx context.Context, clientID, nextPosition string) error {
	w.setUnconditionalCalled = true
	return nil
}
