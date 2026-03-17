package client

import (
	"strconv"
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

	batchBytes := 0
	fitCount := 0
	realCount := 0
	nextPos := ""
	for i := 0; i < len(b.entries); i++ {
		entry := b.entries[i]
		size := b.entrySizes[i]
		isJournalPosition := entry.Cursor != ""
		if fitCount > 0 && batchBytes+size > b.maxDataBytes && realCount > 0 {
			break
		}
		if b.maxSize > 0 && isJournalPosition && realCount >= b.maxSize {
			break
		}
		batchBytes += size
		fitCount++
		if isJournalPosition {
			realCount++
			nextPos = entry.Cursor
		}
	}
	if fitCount == 0 || nextPos == "" {
		return nil
	}

	records := make([]models.Record, fitCount)
	for i, e := range b.entries[:fitCount] {
		records[i] = e.Record
	}
	currentPos := b.entries[0].Position
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
	hasJournalPosition := false
	for _, entry := range b.entries {
		if entry.Cursor != "" {
			hasJournalPosition = true
			break
		}
	}
	if !hasJournalPosition {
		return false
	}
	if b.maxSize > 0 && len(b.entries) >= b.maxSize {
		return true
	}
	return b.dataBytes >= b.maxDataBytes
}

func recordLogDataSize(record models.Record) int {
	size := len(record.Message)
	if record.SeqNo != nil {
		size += len(strconv.FormatInt(*record.SeqNo, 10))
	}
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
