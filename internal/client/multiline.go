package client

import (
	"regexp"
	"strings"
	"time"
)

// MultilineMerger merges continuation journal lines into a single message.
type MultilineMerger struct {
	startRe      *regexp.Regexp
	timeout      time.Duration
	maxMessages  int
	pending      *JournalEntry
	pendingCount int
	lastAppendAt time.Time
}

// NewMultilineMerger creates multiline merger.
// If messageRegex is empty, returns nil (multiline mode disabled).
func NewMultilineMerger(messageRegex string, timeout time.Duration, maxMessages int) (*MultilineMerger, error) {
	regex := strings.TrimSpace(messageRegex)
	if regex == "" {
		return nil, nil
	}
	re, err := regexp.Compile(regex)
	if err != nil {
		return nil, err
	}
	return &MultilineMerger{
		startRe:     re,
		timeout:     timeout,
		maxMessages: maxMessages,
	}, nil
}

// Add consumes next journal entry and returns completed merged entries.
func (m *MultilineMerger) Add(entry *JournalEntry, now time.Time) []*JournalEntry {
	if entry == nil {
		return nil
	}
	if m.pending == nil {
		m.pending = entry
		m.pendingCount = 1
		m.lastAppendAt = now
		return nil
	}

	if m.startRe.MatchString(entry.Record.Message) || m.pendingCount >= m.maxMessages {
		out := []*JournalEntry{m.pending}
		m.pending = entry
		m.pendingCount = 1
		m.lastAppendAt = now
		return out
	}

	m.pending.Record.Message += "\n" + entry.Record.Message
	m.pending.Cursor = entry.Cursor
	m.pendingCount++
	m.lastAppendAt = now
	return nil
}

// DrainExpired returns pending entry when multiline timeout has passed.
func (m *MultilineMerger) DrainExpired(now time.Time) *JournalEntry {
	if m.pending == nil {
		return nil
	}
	if now.Sub(m.lastAppendAt) < m.timeout {
		return nil
	}
	out := m.pending
	m.pending = nil
	m.pendingCount = 0
	return out
}

// Drain returns pending entry immediately.
func (m *MultilineMerger) Drain() *JournalEntry {
	if m.pending == nil {
		return nil
	}
	out := m.pending
	m.pending = nil
	m.pendingCount = 0
	return out
}
