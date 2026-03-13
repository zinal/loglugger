package client

import (
	"context"

	"github.com/mzinal/loglugger/internal/models"
)

// JournalEntry holds a single journal record with position.
type JournalEntry struct {
	Record   models.Record
	Position string
	Cursor   string
}

// JournalReader reads log records from journald.
type JournalReader interface {
	// SeekToPosition seeks to the given position. Empty position means start from head.
	SeekToPosition(ctx context.Context, position string) error
	// Next reads the next entry. Returns nil when no more entries (would block).
	Next(ctx context.Context) (*JournalEntry, error)
	// GetCursor returns the current cursor (for position tracking).
	GetCursor() (string, error)
}

// JournalConfig configures the journal reader.
type JournalConfig struct {
	ServiceMask string // Filter for _SYSTEMD_UNIT (empty = no filter)
}
