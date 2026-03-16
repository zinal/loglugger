//go:build linux

package client

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/coreos/go-systemd/v22/sdjournal"
	"github.com/ydb-platform/loglugger/internal/models"
)

type journalReader struct {
	j              *sdjournal.Journal
	cfg            JournalConfig
	last           string
	lastRealtime   uint64
	lastMonotonic  uint64
	exactMatch     string
	pending        []*JournalEntry
	serviceMatcher serviceMatcher
}

const recoveryMessageText = "log reading has been recovered from a severe error; some log data may have been lost"

// NewJournalReader creates a journal reader. Only available on Linux.
func NewJournalReader(cfg JournalConfig) (JournalReader, error) {
	matcher, exactMatch, err := newServiceMatcher(cfg.ServiceMask)
	if err != nil {
		return nil, err
	}
	j, err := openMatchedJournal(cfg.JournalNamespace, exactMatch)
	if err != nil {
		return nil, err
	}
	return &journalReader{j: j, cfg: cfg, exactMatch: exactMatch, serviceMatcher: matcher}, nil
}

func openJournal(namespace string) (*sdjournal.Journal, error) {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return sdjournal.NewJournal()
	}
	if strings.ContainsRune(namespace, os.PathSeparator) {
		return nil, fmt.Errorf("invalid journald namespace %q: must not contain path separators", namespace)
	}
	return sdjournal.NewJournalInNamespace(namespace)
}

func openMatchedJournal(namespace, exactMatch string) (*sdjournal.Journal, error) {
	j, err := openJournal(namespace)
	if err != nil {
		return nil, fmt.Errorf("open journal: %w", err)
	}
	if exactMatch == "" {
		return j, nil
	}
	if err := j.AddMatch("_SYSTEMD_UNIT=" + exactMatch); err != nil {
		j.Close()
		return nil, fmt.Errorf("add match: %w", err)
	}
	return j, nil
}

func (r *journalReader) SeekToPosition(ctx context.Context, position string) error {
	if position == "" {
		if err := r.j.SeekHead(); err != nil {
			return fmt.Errorf("seek head: %w", err)
		}
		r.last = ""
		r.lastRealtime = 0
		r.lastMonotonic = 0
		r.pending = nil
		return nil
	}
	if err := r.j.SeekCursor(position); err != nil {
		return fmt.Errorf("seek cursor %q: %w", position, err)
	}
	_, _ = r.j.Next()
	r.last = position
	r.pending = nil
	return nil
}

func (r *journalReader) Next(ctx context.Context) (*JournalEntry, error) {
	if len(r.pending) > 0 {
		entry := r.pending[0]
		r.pending = r.pending[1:]
		return entry, nil
	}
	entry, nextCursor, nextRealtime, nextMonotonic, err := r.readNext(ctx, r.j, r.last)
	if err != nil {
		return nil, err
	}
	if entry != nil {
		r.last = nextCursor
		r.lastRealtime = nextRealtime
		r.lastMonotonic = nextMonotonic
	}
	return entry, nil
}

func (r *journalReader) readNext(ctx context.Context, j *sdjournal.Journal, last string) (*JournalEntry, string, uint64, uint64, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, "", 0, 0, ctx.Err()
		default:
		}
		n, err := j.Next()
		if err != nil {
			return nil, "", 0, 0, err
		}
		if n == 0 {
			return nil, last, r.lastRealtime, r.lastMonotonic, nil
		}
		entry, err := j.GetEntry()
		if err != nil {
			return nil, "", 0, 0, fmt.Errorf("get entry: %w", err)
		}
		rec := journalEntryToRecord(entry)
		if !r.serviceMatcher.Match(rec.SystemdUnit) {
			// Keep continuity relative to last sent record only.
			continue
		}
		nextRealtime := entry.RealtimeTimestamp
		if rec.RealtimeTimestamp != nil && *rec.RealtimeTimestamp >= 0 {
			nextRealtime = uint64(*rec.RealtimeTimestamp)
		}
		nextMonotonic := entry.MonotonicTimestamp
		if rec.MonotonicTimestamp != nil {
			nextMonotonic = *rec.MonotonicTimestamp
		}
		return &JournalEntry{Record: rec, Position: last, Cursor: entry.Cursor}, entry.Cursor, nextRealtime, nextMonotonic, nil
	}
}

func (r *journalReader) Recover(ctx context.Context) (bool, error) {
	if r.j == nil {
		return false, fmt.Errorf("journal is not open")
	}
	previousCursor := r.last
	previousRealtime := r.lastRealtime
	previousMonotonic := r.lastMonotonic
	cursorErr := r.recoverFromCursor(ctx)
	if cursorErr == nil {
		r.prependRecoveryMessage(previousCursor, previousRealtime, previousMonotonic)
		return true, nil
	}
	if r.lastRealtime == 0 {
		return false, fmt.Errorf("resume from last known cursor failed: %w; no later timestamp is available after the last good entry", cursorErr)
	}
	realtimeErr := r.recoverFromRealtime(ctx)
	if realtimeErr != nil {
		return false, fmt.Errorf("resume from last known cursor failed: %w; resume after the last good timestamp failed: %w", cursorErr, realtimeErr)
	}
	r.prependRecoveryMessage(previousCursor, previousRealtime, previousMonotonic)
	return true, nil
}

func (r *journalReader) recoverFromCursor(ctx context.Context) error {
	if r.last == "" {
		return fmt.Errorf("no last known cursor is available")
	}
	j, err := openMatchedJournal(r.cfg.JournalNamespace, r.exactMatch)
	if err != nil {
		return err
	}
	if err := j.SeekCursor(r.last); err != nil {
		j.Close()
		return fmt.Errorf("seek last known cursor %q: %w", r.last, err)
	}
	if err := r.swapRecoveredJournal(ctx, j, r.last); err != nil {
		return fmt.Errorf("resume from last known cursor %q: %w", r.last, err)
	}
	return nil
}

func (r *journalReader) recoverFromRealtime(ctx context.Context) error {
	j, err := openMatchedJournal(r.cfg.JournalNamespace, r.exactMatch)
	if err != nil {
		return err
	}
	if err := j.SeekRealtimeUsec(r.lastRealtime + 1); err != nil {
		j.Close()
		return fmt.Errorf("seek past last good entry timestamp %d: %w", r.lastRealtime, err)
	}
	if err := r.swapRecoveredJournal(ctx, j, r.last); err != nil {
		return fmt.Errorf("resume after timestamp %d: %w", r.lastRealtime, err)
	}
	return nil
}

func (r *journalReader) swapRecoveredJournal(ctx context.Context, j *sdjournal.Journal, last string) error {
	entry, nextCursor, nextRealtime, nextMonotonic, err := r.readNext(ctx, j, last)
	if err != nil {
		j.Close()
		return err
	}
	if r.j != nil {
		if closeErr := r.j.Close(); closeErr != nil {
			j.Close()
			return fmt.Errorf("close old journal: %w", closeErr)
		}
	}
	r.j = j
	r.pending = nil
	if entry != nil {
		r.pending = append(r.pending, entry)
	}
	if entry != nil {
		r.last = nextCursor
		r.lastRealtime = nextRealtime
		r.lastMonotonic = nextMonotonic
	}
	return nil
}

func (r *journalReader) prependRecoveryMessage(position string, realtimeUsec, monotonicUsec uint64) {
	entry := newRecoveryJournalEntry(position, realtimeUsec, monotonicUsec)
	r.pending = append([]*JournalEntry{entry}, r.pending...)
}

func newRecoveryJournalEntry(position string, realtimeUsec, monotonicUsec uint64) *JournalEntry {
	const criticalPriority = 2
	eventRealtime := int64(realtimeUsec)
	if eventRealtime > 0 {
		eventRealtime += 1000
	}
	eventMonotonic := monotonicUsec
	if eventMonotonic > 0 {
		eventMonotonic += 1000
	}
	record := models.Record{
		Message:          recoveryLogMessage(eventRealtime),
		Priority:         intPtr(criticalPriority),
		SyslogIdentifier: "LOGLUGGER",
		Fields:           make(map[string]string),
	}
	if eventRealtime > 0 {
		record.RealtimeTimestamp = &eventRealtime
	}
	if eventMonotonic > 0 {
		record.MonotonicTimestamp = &eventMonotonic
	}
	return &JournalEntry{
		Record:   record,
		Position: position,
	}
}

func recoveryLogMessage(realtimeUsec int64) string {
	if realtimeUsec <= 0 {
		return ":LOGLUGGER CRITICAL: " + recoveryMessageText
	}
	timestamp := time.UnixMicro(realtimeUsec).UTC().Format(time.RFC3339Nano)
	return fmt.Sprintf("%s :LOGLUGGER CRITICAL: %s", timestamp, recoveryMessageText)
}

func intPtr(value int) *int {
	return &value
}

func journalEntryToRecord(e *sdjournal.JournalEntry) models.Record {
	rec := models.Record{Fields: make(map[string]string)}
	rec.Message = e.Fields["MESSAGE"]
	if v, ok := e.Fields["PRIORITY"]; ok {
		if p, err := strconv.Atoi(v); err == nil {
			rec.Priority = &p
		}
	}
	rec.SyslogIdentifier = e.Fields["SYSLOG_IDENTIFIER"]
	rec.SystemdUnit = e.Fields["_SYSTEMD_UNIT"]
	if v, ok := e.Fields["__REALTIME_TIMESTAMP"]; ok {
		if t, err := strconv.ParseInt(v, 10, 64); err == nil {
			rec.RealtimeTimestamp = &t
		}
	}
	if v, ok := e.Fields["__MONOTONIC_TIMESTAMP"]; ok {
		if t, err := strconv.ParseUint(v, 10, 64); err == nil {
			rec.MonotonicTimestamp = &t
		}
	}
	if rec.RealtimeTimestamp == nil {
		t := int64(e.RealtimeTimestamp)
		rec.RealtimeTimestamp = &t
	}
	if rec.MonotonicTimestamp == nil {
		rec.MonotonicTimestamp = &e.MonotonicTimestamp
	}
	for k, v := range e.Fields {
		if k != "MESSAGE" && k != "PRIORITY" && k != "SYSLOG_IDENTIFIER" &&
			k != "_SYSTEMD_UNIT" && k != "__REALTIME_TIMESTAMP" && k != "__MONOTONIC_TIMESTAMP" {
			rec.Fields[k] = v
		}
	}
	return rec
}

func (r *journalReader) Close() error {
	if r.j != nil {
		return r.j.Close()
	}
	return nil
}

type serviceMatcher interface {
	Match(unit string) bool
}

type matchAllUnits struct{}

func (matchAllUnits) Match(string) bool { return true }

type exactUnitMatcher struct{ value string }

func (m exactUnitMatcher) Match(unit string) bool { return unit == m.value }

type globUnitMatcher struct{ pattern string }

func (m globUnitMatcher) Match(unit string) bool {
	ok, err := filepath.Match(m.pattern, unit)
	return err == nil && ok
}

type regexUnitMatcher struct{ pattern *regexp.Regexp }

func (m regexUnitMatcher) Match(unit string) bool { return m.pattern.MatchString(unit) }

func newServiceMatcher(mask string) (serviceMatcher, string, error) {
	mask = strings.TrimSpace(mask)
	if mask == "" {
		return matchAllUnits{}, "", nil
	}
	if strings.HasPrefix(mask, "regex:") {
		re, err := regexp.Compile(strings.TrimPrefix(mask, "regex:"))
		if err != nil {
			return nil, "", fmt.Errorf("compile service regex: %w", err)
		}
		return regexUnitMatcher{pattern: re}, "", nil
	}
	if strings.ContainsAny(mask, "*?[") {
		return globUnitMatcher{pattern: mask}, "", nil
	}
	return exactUnitMatcher{value: mask}, mask, nil
}
