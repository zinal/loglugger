package server

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/ydb-platform/loglugger/internal/models"
)

// Handler handles batch submission requests.
type Handler struct {
	mapper                   Mapper
	writer                   Writer
	table                    string
	maxCompressedBodyBytes   int64
	maxDecompressedBodyBytes int64
}

const (
	defaultMaxCompressedBodyBytes   int64 = 8 << 20  // 8 MiB
	defaultMaxDecompressedBodyBytes int64 = 32 << 20 // 32 MiB
	genericStorageErrorMessage            = "internal storage error"
)

// HandlerOptions controls request bounds for batch ingestion.
type HandlerOptions struct {
	// MaxCompressedBodyBytes limits the incoming HTTP request body size.
	MaxCompressedBodyBytes int64
	// MaxDecompressedBodyBytes limits the decoded payload size after Content-Encoding processing.
	MaxDecompressedBodyBytes int64
}

// NewHandler creates a batch handler.
func NewHandler(mapper Mapper, writer Writer, table string) *Handler {
	return NewHandlerWithOptions(mapper, writer, table, HandlerOptions{})
}

// NewHandlerWithOptions creates a batch handler with request size limits.
func NewHandlerWithOptions(mapper Mapper, writer Writer, table string, opts HandlerOptions) *Handler {
	opts = normalizeHandlerOptions(opts)
	return &Handler{
		mapper:                   mapper,
		writer:                   writer,
		table:                    table,
		maxCompressedBodyBytes:   opts.MaxCompressedBodyBytes,
		maxDecompressedBodyBytes: opts.MaxDecompressedBodyBytes,
	}
}

func normalizeHandlerOptions(opts HandlerOptions) HandlerOptions {
	if opts.MaxCompressedBodyBytes <= 0 {
		opts.MaxCompressedBodyBytes = defaultMaxCompressedBodyBytes
	}
	if opts.MaxDecompressedBodyBytes <= 0 {
		opts.MaxDecompressedBodyBytes = defaultMaxDecompressedBodyBytes
	}
	return opts
}

// ServeHTTP implements http.Handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/v1/positions":
		resp := h.handlePosition(r.Context(), r.URL.Query().Get("client_id"))
		h.writePositionResponse(w, resp)
		return
	case r.Method == http.MethodPost && r.URL.Path == "/v1/batches":
		h.serveBatch(w, r)
		return
	default:
		http.NotFound(w, r)
		return
	}
}

func (h *Handler) serveBatch(w http.ResponseWriter, r *http.Request) {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		h.writeError(w, http.StatusBadRequest, "Content-Type must be application/json")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, h.maxCompressedBodyBytes)
	body, closeFn, err := decodeRequestBody(r)
	if err != nil {
		if isRequestTooLarge(err) {
			h.writeError(w, http.StatusRequestEntityTooLarge, "request body is too large")
			return
		}
		h.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	defer closeFn()

	var req models.BatchRequest
	limitedBody := newDecompressedBodyLimiter(body, h.maxDecompressedBodyBytes)
	dec := json.NewDecoder(limitedBody)
	if err := dec.Decode(&req); err != nil {
		if isRequestTooLarge(err) {
			h.writeError(w, http.StatusRequestEntityTooLarge, "request body is too large")
			return
		}
		h.writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if err := ensureNoTrailingJSON(dec); err != nil {
		if isRequestTooLarge(err) {
			h.writeError(w, http.StatusRequestEntityTooLarge, "request body is too large")
			return
		}
		h.writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	resp := h.handle(r.Context(), &req)
	h.writeResponse(w, resp)
}

func isRequestTooLarge(err error) bool {
	var maxBytesErr *http.MaxBytesError
	return errors.As(err, &maxBytesErr)
}

func decodeRequestBody(r *http.Request) (io.Reader, func(), error) {
	encoding := strings.TrimSpace(r.Header.Get("Content-Encoding"))
	if encoding == "" || strings.EqualFold(encoding, "identity") {
		return r.Body, func() {}, nil
	}
	if strings.EqualFold(encoding, "gzip") {
		gz, err := gzip.NewReader(r.Body)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid gzip body: %w", err)
		}
		return gz, func() { _ = gz.Close() }, nil
	}
	return nil, nil, fmt.Errorf("unsupported Content-Encoding %q", encoding)
}

func ensureNoTrailingJSON(dec *json.Decoder) error {
	var trailing json.RawMessage
	if err := dec.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("extra data after top-level JSON value")
		}
		return err
	}
	return nil
}

type decompressedBodyLimiter struct {
	reader    io.Reader
	remaining int64
	limit     int64
}

func newDecompressedBodyLimiter(reader io.Reader, limit int64) *decompressedBodyLimiter {
	return &decompressedBodyLimiter{
		reader:    reader,
		remaining: limit,
		limit:     limit,
	}
}

func (l *decompressedBodyLimiter) Read(p []byte) (int, error) {
	if l.remaining > 0 {
		if int64(len(p)) > l.remaining {
			p = p[:l.remaining]
		}
		n, err := l.reader.Read(p)
		l.remaining -= int64(n)
		return n, err
	}
	var probe [1]byte
	n, err := l.reader.Read(probe[:])
	if n > 0 {
		return 0, &http.MaxBytesError{Limit: l.limit}
	}
	return 0, err
}

func (h *Handler) handlePosition(ctx context.Context, clientID string) *models.PositionResponse {
	if clientID == "" {
		return &models.PositionResponse{Status: "error", Message: "client_id is required"}
	}
	position, ok, err := h.writer.GetPosition(ctx, clientID)
	if err != nil {
		return h.positionStorageError("get_position", clientID, err)
	}
	if !ok {
		return &models.PositionResponse{Status: "not_found"}
	}
	return &models.PositionResponse{Status: "ok", CurrentPosition: position}
}

func (h *Handler) handle(ctx context.Context, req *models.BatchRequest) *models.BatchResponse {
	if req.ClientID == "" {
		return &models.BatchResponse{Status: "error", Message: "client_id is required"}
	}
	if req.NextPosition == "" {
		return &models.BatchResponse{Status: "error", Message: "next_position is required"}
	}
	if !req.Reset && req.CurrentPosition == "" {
		return &models.BatchResponse{Status: "error", Message: "current_position is required when reset is false"}
	}
	if req.Reset {
		positionUpdate := positionUpdateFromRecords(req.Records)
		slog.Info("reset batch received from client", "client_id", req.ClientID, "next_position", req.NextPosition, "records", len(req.Records))
		if len(req.Records) > 0 {
			rows, err := h.validateAndMapRecords(req.ClientID, req.Records)
			if err != nil {
				return &models.BatchResponse{Status: "error", Message: err.Error()}
			}
			if err := h.writer.BulkUpsert(ctx, h.table, rows); err != nil {
				return h.batchStorageError("bulk_upsert", req.ClientID, err)
			}
		}
		if err := h.writer.SetPositionUnconditional(ctx, req.ClientID, req.NextPosition, positionUpdate); err != nil {
			return h.batchStorageError("set_position_unconditional", req.ClientID, err)
		}
		return &models.BatchResponse{Status: "ok", NextPosition: req.NextPosition}
	}

	expected, ok, err := h.writer.GetPosition(ctx, req.ClientID)
	if err != nil {
		return h.batchStorageError("get_position", req.ClientID, err)
	}
	if !ok {
		return &models.BatchResponse{Status: "error", Message: "missing current_position or reset required"}
	}
	if req.CurrentPosition != expected {
		slog.Info("client position mismatch",
			"client_id", req.ClientID,
			"provided_current_position", req.CurrentPosition,
			"expected_position", expected,
			"next_position", req.NextPosition,
		)
		return &models.BatchResponse{
			Status:           "position_mismatch",
			ExpectedPosition: expected,
		}
	}

	if len(req.Records) > 0 {
		rows, err := h.validateAndMapRecords(req.ClientID, req.Records)
		if err != nil {
			return &models.BatchResponse{Status: "error", Message: err.Error()}
		}
		if err := h.writer.BulkUpsert(ctx, h.table, rows); err != nil {
			return h.batchStorageError("bulk_upsert", req.ClientID, err)
		}
	}
	if err := h.writer.SetPosition(ctx, req.ClientID, expected, req.NextPosition, positionUpdateFromRecords(req.Records)); err != nil {
		if resp := h.positionMismatchResponse(req.ClientID, err); resp != nil {
			return resp
		}
		return h.batchStorageError("set_position", req.ClientID, err)
	}
	return &models.BatchResponse{Status: "ok", NextPosition: req.NextPosition}
}

func (h *Handler) batchStorageError(operation, clientID string, err error) *models.BatchResponse {
	slog.Error("storage operation failed", "operation", operation, "client_id", clientID, "table", h.table, "error", err)
	return &models.BatchResponse{Status: "error", Message: genericStorageErrorMessage}
}

func (h *Handler) positionStorageError(operation, clientID string, err error) *models.PositionResponse {
	slog.Error("position storage operation failed", "operation", operation, "client_id", clientID, "error", err)
	return &models.PositionResponse{Status: "error", Message: genericStorageErrorMessage}
}

func (h *Handler) positionMismatchResponse(clientID string, err error) *models.BatchResponse {
	var mismatch *PositionMismatchError
	if errors.As(err, &mismatch) {
		expected := ""
		if mismatch.Found {
			expected = mismatch.CurrentPosition
		}
		slog.Info("client position mismatch during position update",
			"client_id", clientID,
			"expected_position", expected,
			"position_found", mismatch.Found,
		)
		return &models.BatchResponse{
			Status:           "position_mismatch",
			ExpectedPosition: expected,
		}
	}
	return nil
}

func validateRecord(rec models.Record) error {
	if rec.Message == "" {
		return fmt.Errorf("record must include message")
	}
	return nil
}

func (h *Handler) validateAndMapRecords(clientID string, records []models.Record) ([]map[string]interface{}, error) {
	rows := make([]map[string]interface{}, 0, len(records))
	for i := range records {
		rec := records[i]
		if err := validateRecord(rec); err != nil {
			return nil, fmt.Errorf("invalid records[%d]: %w", i, err)
		}
		row, err := h.mapper.MapRecord(clientID, rec)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func positionUpdateFromRecords(records []models.Record) PositionUpdate {
	update := PositionUpdate{
		TSWall: time.Now().UTC(),
	}
	for i := range records {
		rec := records[i]
		if rec.SeqNo != nil && (update.MaxSeqNo == nil || *rec.SeqNo > *update.MaxSeqNo) {
			maxSeqNo := *rec.SeqNo
			update.MaxSeqNo = &maxSeqNo
		}
		if rec.RealtimeTS != nil {
			tsOrig := time.UnixMicro(*rec.RealtimeTS).UTC()
			if update.MaxTSOrig == nil || tsOrig.After(*update.MaxTSOrig) {
				maxTSOrig := tsOrig
				update.MaxTSOrig = &maxTSOrig
			}
		}
	}
	return update
}

func (h *Handler) writeResponse(w http.ResponseWriter, resp *models.BatchResponse) {
	status := http.StatusOK
	respOut := *resp
	switch resp.Status {
	case "position_mismatch":
		status = http.StatusConflict
	case "error":
		if respOut.Message == "client_id is required" || respOut.Message == "next_position is required" ||
			respOut.Message == "current_position is required when reset is false" ||
			respOut.Message == "missing current_position or reset required" ||
			(len(respOut.Message) > 6 && respOut.Message[:6] == "invalid") {
			status = http.StatusBadRequest
		} else {
			status = http.StatusInternalServerError
		}
	}
	if status == http.StatusInternalServerError && respOut.Status == "error" {
		if respOut.Message != genericStorageErrorMessage {
			slog.Error("internal batch processing failure", "message", respOut.Message)
		}
		respOut.Message = genericStorageErrorMessage
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(&respOut)
}

func (h *Handler) writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(&models.BatchResponse{Status: "error", Message: msg})
}

func (h *Handler) writePositionResponse(w http.ResponseWriter, resp *models.PositionResponse) {
	status := http.StatusOK
	respOut := *resp
	if respOut.Status == "error" {
		if respOut.Message == "client_id is required" {
			status = http.StatusBadRequest
		} else {
			status = http.StatusInternalServerError
		}
	}
	if status == http.StatusInternalServerError && respOut.Status == "error" {
		if respOut.Message != genericStorageErrorMessage {
			slog.Error("internal position processing failure", "message", respOut.Message)
		}
		respOut.Message = genericStorageErrorMessage
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(&respOut)
}
