package server

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"

	"github.com/ydb-platform/loglugger/internal/models"
)

// Handler handles batch submission requests.
type Handler struct {
	mapper                  Mapper
	writer                  Writer
	parser                  MessageParser
	table                   string
	maxCompressedBodyBytes  int64
	maxDecompressedBodyBytes int64
}

const (
	defaultMaxCompressedBodyBytes   int64 = 8 << 20  // 8 MiB
	defaultMaxDecompressedBodyBytes int64 = 32 << 20 // 32 MiB
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
	return NewHandlerWithParser(mapper, writer, table, nil)
}

// NewHandlerWithParser creates a batch handler with optional server-side message parser.
func NewHandlerWithParser(mapper Mapper, writer Writer, table string, parser MessageParser) *Handler {
	return NewHandlerWithOptions(mapper, writer, table, parser, HandlerOptions{})
}

// NewHandlerWithOptions creates a batch handler with optional parser and request size limits.
func NewHandlerWithOptions(mapper Mapper, writer Writer, table string, parser MessageParser, opts HandlerOptions) *Handler {
	opts = normalizeHandlerOptions(opts)
	return &Handler{
		mapper:                   mapper,
		writer:                   writer,
		parser:                   parser,
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

	payload, err := io.ReadAll(io.LimitReader(body, h.maxDecompressedBodyBytes+1))
	if err != nil {
		if isRequestTooLarge(err) {
			h.writeError(w, http.StatusRequestEntityTooLarge, "request body is too large")
			return
		}
		h.writeError(w, http.StatusBadRequest, "failed to read request body: "+err.Error())
		return
	}
	if int64(len(payload)) > h.maxDecompressedBodyBytes {
		h.writeError(w, http.StatusRequestEntityTooLarge, "request body is too large")
		return
	}

	var req models.BatchRequest
	if err := json.Unmarshal(payload, &req); err != nil {
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

func (h *Handler) handlePosition(ctx context.Context, clientID string) *models.PositionResponse {
	if clientID == "" {
		return &models.PositionResponse{Status: "error", Message: "client_id is required"}
	}
	position, ok, err := h.writer.GetPosition(ctx, clientID)
	if err != nil {
		return &models.PositionResponse{Status: "error", Message: err.Error()}
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
	for i := range req.Records {
		if err := validateRecord(req.Records[i]); err != nil {
			return &models.BatchResponse{Status: "error", Message: fmt.Sprintf("invalid records[%d]: %v", i, err)}
		}
	}

	if req.Reset {
		if len(req.Records) > 0 {
			rows, err := h.mapRecords(req.ClientID, req.Records)
			if err != nil {
				return &models.BatchResponse{Status: "error", Message: err.Error()}
			}
			if err := h.writer.BulkUpsert(ctx, h.table, rows); err != nil {
				return &models.BatchResponse{Status: "error", Message: err.Error()}
			}
		}
		if err := h.writer.SetPositionUnconditional(ctx, req.ClientID, req.NextPosition); err != nil {
			return &models.BatchResponse{Status: "error", Message: fmt.Sprintf("store next position: %v", err)}
		}
		return &models.BatchResponse{Status: "ok", NextPosition: req.NextPosition}
	}

	expected, ok, err := h.writer.GetPosition(ctx, req.ClientID)
	if err != nil {
		return &models.BatchResponse{Status: "error", Message: err.Error()}
	}
	if !ok {
		return &models.BatchResponse{Status: "error", Message: "missing current_position or reset required"}
	}
	if req.CurrentPosition != expected {
		return &models.BatchResponse{
			Status:           "position_mismatch",
			ExpectedPosition: expected,
		}
	}

	if len(req.Records) > 0 {
		rows, err := h.mapRecords(req.ClientID, req.Records)
		if err != nil {
			return &models.BatchResponse{Status: "error", Message: err.Error()}
		}
		if err := h.writer.BulkUpsert(ctx, h.table, rows); err != nil {
			return &models.BatchResponse{Status: "error", Message: err.Error()}
		}
	}
	if err := h.writer.SetPosition(ctx, req.ClientID, expected, req.NextPosition); err != nil {
		if resp := h.positionMismatchResponse(err); resp != nil {
			return resp
		}
		return &models.BatchResponse{Status: "error", Message: fmt.Sprintf("store next position: %v", err)}
	}
	return &models.BatchResponse{Status: "ok", NextPosition: req.NextPosition}
}

func (h *Handler) positionMismatchResponse(err error) *models.BatchResponse {
	var mismatch *PositionMismatchError
	if errors.As(err, &mismatch) {
		expected := ""
		if mismatch.Found {
			expected = mismatch.CurrentPosition
		}
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

func (h *Handler) mapRecords(clientID string, records []models.Record) ([]map[string]interface{}, error) {
	rows := make([]map[string]interface{}, 0, len(records))
	for _, rec := range records {
		if h.parser != nil {
			parsed, ok := h.parser.Parse(rec)
			if !ok {
				continue
			}
			rec = parsed
		}
		row, err := h.mapper.MapRecord(clientID, rec)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (h *Handler) writeResponse(w http.ResponseWriter, resp *models.BatchResponse) {
	status := http.StatusOK
	switch resp.Status {
	case "position_mismatch":
		status = http.StatusConflict
	case "error":
		if resp.Message == "client_id is required" || resp.Message == "next_position is required" ||
			resp.Message == "current_position is required when reset is false" ||
			resp.Message == "missing current_position or reset required" ||
			(len(resp.Message) > 6 && resp.Message[:6] == "invalid") {
			status = http.StatusBadRequest
		} else {
			status = http.StatusInternalServerError
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(resp)
}

func (h *Handler) writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(&models.BatchResponse{Status: "error", Message: msg})
}

func (h *Handler) writePositionResponse(w http.ResponseWriter, resp *models.PositionResponse) {
	status := http.StatusOK
	if resp.Status == "error" {
		if resp.Message == "client_id is required" {
			status = http.StatusBadRequest
		} else {
			status = http.StatusInternalServerError
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(resp)
}
