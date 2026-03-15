package server

import (
	"context"
	"sync"
)

// MockWriter is an in-memory writer for testing.
type MockWriter struct {
	mu        sync.Mutex
	Rows      []map[string]interface{}
	positions map[string]string
}

// NewMockWriter creates a mock writer.
func NewMockWriter() *MockWriter {
	return &MockWriter{
		Rows:      make([]map[string]interface{}, 0),
		positions: make(map[string]string),
	}
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

func (w *MockWriter) GetPosition(ctx context.Context, clientID string) (string, bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	position, ok := w.positions[clientID]
	return position, ok, nil
}

func (w *MockWriter) SetPosition(ctx context.Context, clientID, expectedPosition, nextPosition string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	current, ok := w.positions[clientID]
	if current != expectedPosition {
		return &PositionMismatchError{CurrentPosition: current, Found: ok}
	}
	w.positions[clientID] = nextPosition
	return nil
}
