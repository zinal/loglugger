package server

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// FilePositionStore persists expected positions on disk.
type FilePositionStore struct {
	path string
	mu   sync.RWMutex
	data map[string]string
}

// NewFilePositionStore creates a file-backed position store.
func NewFilePositionStore(path string) (*FilePositionStore, error) {
	store := &FilePositionStore{
		path: path,
		data: make(map[string]string),
	}
	if err := store.load(); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *FilePositionStore) Get(ctx context.Context, clientID string) (string, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	pos, ok := s.data[clientID]
	return pos, ok, nil
}

func (s *FilePositionStore) Set(ctx context.Context, clientID, position string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[clientID] = position
	return s.persistLocked()
}

func (s *FilePositionStore) load() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read position file: %w", err)
	}
	if len(data) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, &s.data); err != nil {
		return fmt.Errorf("decode position file: %w", err)
	}
	return nil
}

func (s *FilePositionStore) persistLocked() error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return fmt.Errorf("create position store directory: %w", err)
	}
	data, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return fmt.Errorf("encode position file: %w", err)
	}
	if err := os.WriteFile(s.path, data, 0o600); err != nil {
		return fmt.Errorf("write position file: %w", err)
	}
	return nil
}
