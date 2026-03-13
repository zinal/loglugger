//go:build !linux

package client

import "fmt"

// NewJournalReader returns an error on non-Linux. Journald is only available on Linux.
func NewJournalReader(cfg JournalConfig) (JournalReader, error) {
	return nil, fmt.Errorf("journald is only supported on Linux")
}
