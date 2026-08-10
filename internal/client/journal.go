package client

import (
	"context"

	"github.com/ydb-platform/loglugger/internal/models"
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
	// It also resets the acknowledged protocol position used for batch continuity.
	// For a non-empty journald cursor, the implementation must verify the entry
	// still exists (sd_journal_test_cursor after seek); a stale cursor must be
	// reported as an error so callers can reset rather than skip the nearest entry.
	SeekToPosition(ctx context.Context, position string) error
	// Next reads the next entry. Returns nil when no more entries (would block).
	// Next advances the journal read cursor but does not acknowledge the entry for
	// the position protocol; call Ack only for entries that will be sent.
	Next(ctx context.Context) (*JournalEntry, error)
	// Ack marks an entry as accepted for sending. It stamps entry.Position from the
	// last acknowledged protocol position and, when entry.Cursor is non-empty,
	// advances that acknowledged position to entry.Cursor. Skipped entries must
	// not be Ack'd so position continuity stays aligned with the server (§4.3).
	Ack(entry *JournalEntry)
	// Recover attempts best-effort recovery after journal corruption. The returned
	// boolean reports whether the caller should send the next batch with reset=true.
	Recover(ctx context.Context) (bool, error)
}

// JournalConfig configures the journal reader.
type JournalConfig struct {
	ServiceMask      string // Filter for _SYSTEMD_UNIT (empty = no filter)
	JournalNamespace string // journald namespace (empty = default namespace)
}
