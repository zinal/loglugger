package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ydb-platform/loglugger/internal/models"
)

// maxBatchJSONBytes is the maximum uncompressed JSON body the client will send.
// Kept below the server compressed limit (16 MiB) so even poorly compressible
// payloads fit after gzip, and well below the 32 MiB decompressed limit.
const maxBatchJSONBytes = 15 * 1024 * 1024

// ErrRecordJSONTooLarge is returned when a single record cannot fit into a batch
// under maxBatchJSONBytes together with the request envelope.
var ErrRecordJSONTooLarge = errors.New("record JSON exceeds maximum batch request size")

// Batch holds records, pre-serialized record JSON, and position info.
type Batch struct {
	Records         []models.Record
	RecordJSONs     []json.RawMessage
	CurrentPosition string
	NextPosition    string
}

// Batcher collects records into batches.
type Batcher interface {
	Add(entry *JournalEntry) error
	Flush() *Batch
	ShouldFlush() bool
	// Clear drops all buffered entries without producing a batch. Used when the
	// journal stream is restarted (position mismatch reseek/reset, recovery) so
	// leftover records cannot be sent with stale CurrentPosition values.
	Clear()
}

type batcher struct {
	maxSize      int
	maxJSONBytes int
	timeout      time.Duration
	clientIDLen  int

	entries           []*JournalEntry
	recordJSONs       []json.RawMessage
	recordsArrayBytes int
	bufferedJSONBytes int
	journalCount      int
	startedAt         time.Time
}

// NewBatcher creates a batcher. clientID is included in JSON size accounting so
// the flush limit matches the body that will be sent.
func NewBatcher(maxSize int, timeout time.Duration, clientID string) Batcher {
	return &batcher{
		maxSize:      maxSize,
		maxJSONBytes: maxBatchJSONBytes,
		timeout:      timeout,
		clientIDLen:  jsonStringWireLen(clientID),
		entries:      make([]*JournalEntry, 0, maxSize),
		recordJSONs:  make([]json.RawMessage, 0, maxSize),
	}
}

func (b *batcher) Add(entry *JournalEntry) error {
	raw, err := json.Marshal(entry.Record)
	if err != nil {
		return fmt.Errorf("marshal record: %w", err)
	}

	aloneNext := entry.Cursor
	if aloneNext == "" {
		aloneNext = entry.Position
	}
	aloneSize := batchRequestJSONSize(b.clientIDLen, false, entry.Position, aloneNext, len("[]")+len(raw))
	if aloneSize > b.maxJSONBytes {
		return fmt.Errorf("%w: %d bytes (limit %d)", ErrRecordJSONTooLarge, aloneSize, b.maxJSONBytes)
	}

	if len(b.entries) == 0 {
		b.startedAt = time.Now()
		b.recordsArrayBytes = len("[]") + len(raw)
	} else {
		b.recordsArrayBytes += len(",") + len(raw)
	}
	b.entries = append(b.entries, entry)
	b.recordJSONs = append(b.recordJSONs, raw)
	if entry.Cursor != "" {
		b.journalCount++
	}
	b.bufferedJSONBytes = batchRequestJSONSize(
		b.clientIDLen,
		false, // reset=false is the longer encoding; actual reset cannot exceed this
		b.entries[0].Position,
		b.nextPosition(),
		b.recordsArrayBytes,
	)
	return nil
}

func (b *batcher) Flush() *Batch {
	if len(b.entries) == 0 {
		return nil
	}

	arrayBytes := 0
	fitCount := 0
	realCount := 0
	nextPos := ""
	currentPos := b.entries[0].Position
	for i := 0; i < len(b.entries); i++ {
		entry := b.entries[i]
		recLen := len(b.recordJSONs[i])
		nextArrayBytes := arrayBytes
		if fitCount == 0 {
			nextArrayBytes = len("[]") + recLen
		} else {
			nextArrayBytes = arrayBytes + len(",") + recLen
		}
		isJournalPosition := entry.Cursor != ""
		candidateNext := nextPos
		if isJournalPosition {
			candidateNext = entry.Cursor
		}
		total := batchRequestJSONSize(b.clientIDLen, false, currentPos, candidateNext, nextArrayBytes)
		if fitCount > 0 && total > b.maxJSONBytes && realCount > 0 {
			break
		}
		if fitCount == 0 && total > b.maxJSONBytes {
			// Defensive: Add() rejects oversized singles; do not emit an over-limit batch.
			return nil
		}
		if b.maxSize > 0 && isJournalPosition && realCount >= b.maxSize {
			break
		}
		arrayBytes = nextArrayBytes
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
	recordJSONs := make([]json.RawMessage, fitCount)
	for i := 0; i < fitCount; i++ {
		records[i] = b.entries[i].Record
		recordJSONs[i] = b.recordJSONs[i]
	}
	batch := &Batch{
		Records:         records,
		RecordJSONs:     recordJSONs,
		CurrentPosition: currentPos,
		NextPosition:    nextPos,
	}

	if fitCount >= len(b.entries) {
		b.entries = b.entries[:0]
		b.recordJSONs = b.recordJSONs[:0]
		b.recordsArrayBytes = 0
		b.bufferedJSONBytes = 0
		b.journalCount = 0
		b.startedAt = time.Time{}
	} else {
		b.entries = b.entries[fitCount:]
		b.recordJSONs = b.recordJSONs[fitCount:]
		b.journalCount -= realCount
		b.recomputeSizes()
		// Remaining entries start a new batch timeout window.
		b.startedAt = time.Now()
	}

	return batch
}

func (b *batcher) ShouldFlush() bool {
	if b.journalCount == 0 {
		return false
	}
	if b.maxSize > 0 && len(b.entries) >= b.maxSize {
		return true
	}
	if b.bufferedJSONBytes >= b.maxJSONBytes {
		return true
	}
	// Honor batch_timeout even under a continuous journal stream, where a
	// select/default loop may never observe the flush ticker.
	if b.timeout > 0 && !b.startedAt.IsZero() && time.Since(b.startedAt) >= b.timeout {
		return true
	}
	return false
}

func (b *batcher) Clear() {
	b.entries = b.entries[:0]
	b.recordJSONs = b.recordJSONs[:0]
	b.recordsArrayBytes = 0
	b.bufferedJSONBytes = 0
	b.journalCount = 0
	b.startedAt = time.Time{}
}

func (b *batcher) nextPosition() string {
	for i := len(b.entries) - 1; i >= 0; i-- {
		if b.entries[i].Cursor != "" {
			return b.entries[i].Cursor
		}
	}
	return ""
}

func (b *batcher) recomputeSizes() {
	if len(b.entries) == 0 {
		b.recordsArrayBytes = 0
		b.bufferedJSONBytes = 0
		return
	}
	b.recordsArrayBytes = recordsArrayJSONBytes(b.recordJSONs)
	b.bufferedJSONBytes = batchRequestJSONSize(
		b.clientIDLen,
		false,
		b.entries[0].Position,
		b.nextPosition(),
		b.recordsArrayBytes,
	)
}
