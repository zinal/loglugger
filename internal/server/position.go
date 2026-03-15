package server

import (
	"context"
	"fmt"
	"sync"
)

// PositionStore stores expected position per client.
type PositionStore interface {
	Get(ctx context.Context, clientID string) (string, bool, error)
	Set(ctx context.Context, clientID, expectedPosition, nextPosition string) error
}

// PositionMismatchError indicates optimistic position update conflict.
type PositionMismatchError struct {
	CurrentPosition string
	Found           bool
}

func (e *PositionMismatchError) Error() string {
	if e == nil {
		return "position mismatch"
	}
	if !e.Found {
		return "position mismatch: no current position"
	}
	return fmt.Sprintf("position mismatch: current=%q", e.CurrentPosition)
}

// MemoryPositionStore is an in-memory position store.
type MemoryPositionStore struct {
	mu   sync.RWMutex
	data map[string]string
}

// NewMemoryPositionStore creates an in-memory position store.
func NewMemoryPositionStore() *MemoryPositionStore {
	return &MemoryPositionStore{data: make(map[string]string)}
}

func (s *MemoryPositionStore) Get(ctx context.Context, clientID string) (string, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	pos, ok := s.data[clientID]
	return pos, ok, nil
}

func (s *MemoryPositionStore) Set(ctx context.Context, clientID, expectedPosition, nextPosition string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.data[clientID]
	if !ok {
		current = ""
	}
	if current != expectedPosition {
		return &PositionMismatchError{CurrentPosition: current, Found: ok}
	}
	s.data[clientID] = nextPosition
	return nil
}
