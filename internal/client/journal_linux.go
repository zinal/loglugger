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

	"github.com/coreos/go-systemd/v22/sdjournal"
	"github.com/ydb-platform/loglugger/internal/models"
)

type journalReader struct {
	j              *sdjournal.Journal
	cfg            JournalConfig
	last           string
	serviceMatcher serviceMatcher
}

// NewJournalReader creates a journal reader. Only available on Linux.
func NewJournalReader(cfg JournalConfig) (JournalReader, error) {
	j, err := openJournal(cfg.JournalNamespace)
	if err != nil {
		return nil, fmt.Errorf("open journal: %w", err)
	}
	matcher, exactMatch, err := newServiceMatcher(cfg.ServiceMask)
	if err != nil {
		j.Close()
		return nil, err
	}
	if exactMatch != "" {
		if err := j.AddMatch("_SYSTEMD_UNIT=" + exactMatch); err != nil {
			j.Close()
			return nil, fmt.Errorf("add match: %w", err)
		}
	}
	return &journalReader{j: j, cfg: cfg, serviceMatcher: matcher}, nil
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

func (r *journalReader) SeekToPosition(ctx context.Context, position string) error {
	if position == "" {
		if err := r.j.SeekHead(); err != nil {
			return fmt.Errorf("seek head: %w", err)
		}
		r.last = ""
		return nil
	}
	if err := r.j.SeekCursor(position); err != nil {
		return fmt.Errorf("seek cursor %q: %w", position, err)
	}
	_, _ = r.j.Next()
	r.last = position
	return nil
}

func (r *journalReader) Next(ctx context.Context) (*JournalEntry, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		n, err := r.j.Next()
		if err != nil {
			return nil, err
		}
		if n == 0 {
			return nil, nil
		}
		entry, err := r.j.GetEntry()
		if err != nil {
			return nil, fmt.Errorf("get entry: %w", err)
		}
		rec := journalEntryToRecord(entry)
		if !r.serviceMatcher.Match(rec.SystemdUnit) {
			// Keep continuity relative to last sent record only.
			continue
		}
		positionBefore := r.last
		r.last = entry.Cursor
		return &JournalEntry{Record: rec, Position: positionBefore, Cursor: entry.Cursor}, nil
	}
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
