package server

import (
	"context"
	"sync"
)

// PositionStore stores expected position per client.
type PositionStore interface {
	Get(ctx context.Context, clientID string) (string, bool, error)
	Set(ctx context.Context, clientID, position string) error
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

func (s *MemoryPositionStore) Set(ctx context.Context, clientID, position string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[clientID] = position
	return nil
}
