//go:build linux

package client

import "testing"

func TestRecoveryJournalEntryIncludesMessage(t *testing.T) {
	entry := newRecoveryJournalEntry("p1", 1000, 2000)
	if entry == nil {
		t.Fatal("newRecoveryJournalEntry() returned nil")
	}
	if entry.Record.Message == "" {
		t.Fatal("recovery journal entry must include a message")
	}
	if entry.Record.Fields["MESSAGE"] == "" {
		t.Fatal("recovery journal entry must also populate MESSAGE field")
	}
	if entry.Record.SyslogIdentifier != "LOGLUGGER" {
		t.Fatalf("SyslogIdentifier = %q, want LOGLUGGER", entry.Record.SyslogIdentifier)
	}
}
