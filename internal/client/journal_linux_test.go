//go:build linux

package client

import (
	"regexp"
	"testing"
)

func TestRecoveryJournalEntryIncludesMessage(t *testing.T) {
	entry := newRecoveryJournalEntry("p1", 1000)
	if entry == nil {
		t.Fatal("newRecoveryJournalEntry() returned nil")
	}
	if entry.Record.Message == "" {
		t.Fatal("recovery journal entry must include a message")
	}
	if entry.Record.SeqNo == nil || *entry.Record.SeqNo <= 0 {
		t.Fatalf("SeqNo = %v, want positive seqno", entry.Record.SeqNo)
	}
	if len(entry.Record.Fields) != 0 {
		t.Fatalf("Fields = %+v, want no duplicated top-level fields", entry.Record.Fields)
	}
	uuidLike := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	if !uuidLike.MatchString(entry.Record.SyslogIdentifier) {
		t.Fatalf("SyslogIdentifier = %q, want UUIDv4-like format", entry.Record.SyslogIdentifier)
	}
	if entry.Record.SystemdUnit != "LOGLUGGER" {
		t.Fatalf("SystemdUnit = %q, want LOGLUGGER", entry.Record.SystemdUnit)
	}
}
