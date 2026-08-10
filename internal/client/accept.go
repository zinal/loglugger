package client

// AcceptEntry applies optional message parsing and acknowledges the entry for
// position-protocol tracking when it should be sent.
//
// When the parser skips a record (message_regex_no_match: skip), AcceptEntry
// returns false and does not call Ack, so the acknowledged protocol position
// stays aligned with the last sent record.
func AcceptEntry(journal JournalReader, parser MessageParser, entry *JournalEntry) bool {
	if entry == nil {
		return false
	}
	if parser != nil {
		parsed, ok := parser.Parse(entry.Record)
		if !ok {
			return false
		}
		entry.Record = parsed
	}
	journal.Ack(entry)
	return true
}
