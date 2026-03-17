package server

import (
	"context"
	"sync"
	"time"
)

// MockWriter is an in-memory writer for testing.
type MockWriter struct {
	mu        sync.Mutex
	Rows      []map[string]interface{}
	positions map[string]mockPosition
}

type mockPosition struct {
	expected string
	tsWall   time.Time
	seqNo    *int64
	tsOrig   *time.Time
}

// NewMockWriter creates a mock writer.
func NewMockWriter() *MockWriter {
	return &MockWriter{
		Rows:      make([]map[string]interface{}, 0),
		positions: make(map[string]mockPosition),
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
	return position.expected, ok, nil
}

func (w *MockWriter) SetPosition(ctx context.Context, clientID, expectedPosition, nextPosition string, update PositionUpdate) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	current, ok := w.positions[clientID]
	if current.expected != expectedPosition {
		return &PositionMismatchError{CurrentPosition: current.expected, Found: ok}
	}
	w.positions[clientID] = mockPosition{
		expected: nextPosition,
		tsWall:   update.TSWall.UTC(),
		seqNo:    cloneInt64Ptr(update.MaxSeqNo),
		tsOrig:   cloneTimePtr(update.MaxTSOrig),
	}
	return nil
}

func (w *MockWriter) SetPositionUnconditional(ctx context.Context, clientID, nextPosition string, update PositionUpdate) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.positions[clientID] = mockPosition{
		expected: nextPosition,
		tsWall:   update.TSWall.UTC(),
		seqNo:    cloneInt64Ptr(update.MaxSeqNo),
		tsOrig:   cloneTimePtr(update.MaxTSOrig),
	}
	return nil
}

func cloneInt64Ptr(v *int64) *int64 {
	if v == nil {
		return nil
	}
	cp := *v
	return &cp
}

func cloneTimePtr(v *time.Time) *time.Time {
	if v == nil {
		return nil
	}
	cp := *v
	return &cp
}
