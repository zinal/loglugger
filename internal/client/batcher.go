package client

import (
	"time"

	"github.com/ydb-platform/loglugger/internal/models"
)

const maxBatchLogDataBytes = 10 * 1024 * 1024

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
	maxSize      int
	maxDataBytes int
	timeout      time.Duration
	entries      []*JournalEntry
	entrySizes   []int
	dataBytes    int
}

// NewBatcher creates a batcher.
func NewBatcher(maxSize int, timeout time.Duration) Batcher {
	return &batcher{
		maxSize:      maxSize,
		maxDataBytes: maxBatchLogDataBytes,
		timeout:      timeout,
		entries:      make([]*JournalEntry, 0, maxSize),
		entrySizes:   make([]int, 0, maxSize),
	}
}

func (b *batcher) Add(entry *JournalEntry) {
	size := recordLogDataSize(entry.Record)
	b.entries = append(b.entries, entry)
	b.entrySizes = append(b.entrySizes, size)
	b.dataBytes += size
}

func (b *batcher) Flush() *Batch {
	if len(b.entries) == 0 {
		return nil
	}

	count := len(b.entries)
	if b.maxSize > 0 && count > b.maxSize {
		count = b.maxSize
	}
	batchBytes := 0
	fitCount := 0
	for i := 0; i < count; i++ {
		size := b.entrySizes[i]
		if fitCount > 0 && batchBytes+size > b.maxDataBytes {
			break
		}
		batchBytes += size
		fitCount++
	}
	if fitCount == 0 {
		return nil
	}

	records := make([]models.Record, fitCount)
	for i, e := range b.entries[:fitCount] {
		records[i] = e.Record
	}
	currentPos := b.entries[0].Position
	nextPos := b.entries[fitCount-1].Cursor
	batch := &Batch{
		Records:         records,
		CurrentPosition: currentPos,
		NextPosition:    nextPos,
	}

	b.dataBytes -= batchBytes
	if fitCount >= len(b.entries) {
		b.entries = b.entries[:0]
		b.entrySizes = b.entrySizes[:0]
		b.dataBytes = 0
	} else {
		b.entries = b.entries[fitCount:]
		b.entrySizes = b.entrySizes[fitCount:]
	}

	return batch
}

func (b *batcher) ShouldFlush() bool {
	if b.maxSize > 0 && len(b.entries) >= b.maxSize {
		return true
	}
	return b.dataBytes >= b.maxDataBytes
}

func (b *batcher) Timeout() time.Duration {
	return b.timeout
}

func recordLogDataSize(record models.Record) int {
	size := len(record.Message)
	size += len(record.SyslogIdentifier)
	size += len(record.SystemdUnit)
	for _, value := range record.Parsed {
		size += len(value)
	}
	for _, value := range record.Fields {
		size += len(value)
	}
	return size
}
