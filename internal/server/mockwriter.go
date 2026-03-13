package server

import (
	"context"
	"sync"
)

// MockWriter is an in-memory writer for testing.
type MockWriter struct {
	mu   sync.Mutex
	Rows []map[string]interface{}
}

// NewMockWriter creates a mock writer.
func NewMockWriter() *MockWriter {
	return &MockWriter{Rows: make([]map[string]interface{}, 0)}
}

// BulkUpsert appends rows to the mock store.
func (w *MockWriter) BulkUpsert(ctx context.Context, table string, rows []map[string]interface{}) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, row := range rows {
		cp := make(map[string]interface{})
		for k, v := range row {
			cp[k] = v
		}
		w.Rows = append(w.Rows, cp)
	}
	return nil
}
