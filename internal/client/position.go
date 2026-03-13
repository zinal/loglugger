package client

import (
	"os"
	"sync"
)

// FilePositionStore stores position in a file.
type FilePositionStore struct {
	path string
	mu   sync.RWMutex
}

// NewFilePositionStore creates a file-based position store.
func NewFilePositionStore(path string) *FilePositionStore {
	return &FilePositionStore{path: path}
}

func (s *FilePositionStore) Get() (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}
	return string(data), nil
}

func (s *FilePositionStore) Set(position string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return os.WriteFile(s.path, []byte(position), 0600)
}
