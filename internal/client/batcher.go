package client

import (
	"sync"
	"time"

	"github.com/mzinal/loglugger/internal/models"
)

// Batch holds records and position info.
type Batch struct {
	Records         []models.Record
	CurrentPosition string
	NextPosition    string
}

// Batcher collects records into batches.
type Batcher interface {
	Add(entry *JournalEntry)
	Flush() *Batch
	ShouldFlush() bool
	Timeout() time.Duration
}

type batcher struct {
	maxSize int
	timeout time.Duration
	entries []*JournalEntry
	mu      sync.Mutex
}

// NewBatcher creates a batcher.
func NewBatcher(maxSize int, timeout time.Duration) Batcher {
	return &batcher{
		maxSize: maxSize,
		timeout: timeout,
		entries: make([]*JournalEntry, 0, maxSize),
	}
}

func (b *batcher) Add(entry *JournalEntry) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.entries = append(b.entries, entry)
}

func (b *batcher) Flush() *Batch {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.entries) == 0 {
		return nil
	}
	records := make([]models.Record, len(b.entries))
	for i, e := range b.entries {
		records[i] = e.Record
	}
	currentPos := b.entries[0].Position
	nextPos := b.entries[len(b.entries)-1].Cursor
	batch := &Batch{
		Records:         records,
		CurrentPosition: currentPos,
		NextPosition:    nextPos,
	}
	b.entries = b.entries[:0]
	return batch
}

func (b *batcher) ShouldFlush() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.entries) >= b.maxSize
}

func (b *batcher) Timeout() time.Duration {
	return b.timeout
}
