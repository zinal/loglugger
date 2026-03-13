package server

import (
	"context"
	"encoding/json"
	"fmt"
	"mime"
	"net/http"

	"github.com/mzinal/loglugger/internal/models"
)

// Handler handles batch submission requests.
type Handler struct {
	positions PositionStore
	mapper    Mapper
	writer    Writer
	table     string
}

// NewHandler creates a batch handler.
func NewHandler(positions PositionStore, mapper Mapper, writer Writer, table string) *Handler {
	return &Handler{
		positions: positions,
		mapper:    mapper,
		writer:    writer,
		table:     table,
	}
}

// ServeHTTP implements http.Handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.URL.Path != "/v1/batches" {
		http.NotFound(w, r)
		return
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		h.writeError(w, http.StatusBadRequest, "Content-Type must be application/json")
		return
	}

	var req models.BatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	resp := h.handle(r.Context(), &req)
	h.writeResponse(w, resp)
}

func (h *Handler) handle(ctx context.Context, req *models.BatchRequest) *models.BatchResponse {
	if req.ClientID == "" {
		return &models.BatchResponse{Status: "error", Message: "client_id is required"}
	}
	if req.NextPosition == "" {
		return &models.BatchResponse{Status: "error", Message: "next_position is required"}
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
		if err := h.positions.Set(ctx, req.ClientID, req.NextPosition); err != nil {
			return &models.BatchResponse{Status: "error", Message: fmt.Sprintf("store next position: %v", err)}
		}
		return &models.BatchResponse{Status: "ok", NextPosition: req.NextPosition}
	}

	expected, ok, err := h.positions.Get(ctx, req.ClientID)
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
	if err := h.positions.Set(ctx, req.ClientID, req.NextPosition); err != nil {
		return &models.BatchResponse{Status: "error", Message: fmt.Sprintf("store next position: %v", err)}
	}
	return &models.BatchResponse{Status: "ok", NextPosition: req.NextPosition}
}

func (h *Handler) mapRecords(clientID string, records []models.Record) ([]map[string]interface{}, error) {
	rows := make([]map[string]interface{}, 0, len(records))
	for _, rec := range records {
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
