package client

import "fmt"

// journalCursorNavigator is the subset of sdjournal.Journal used to resume from
// a stored cursor while verifying that the entry actually exists.
type journalCursorNavigator interface {
	SeekCursor(cursor string) error
	Next() (uint64, error)
	TestCursor(cursor string) error
}

// advanceToExactCursor seeks to position and confirms the journal is positioned
// on that exact entry.
//
// sd_journal_seek_cursor succeeds even when the entry is gone by landing on the
// nearest following entry. Without sd_journal_test_cursor, the caller's next
// Next() would skip that nearest entry and violate at-least-once delivery.
//
// On success the journal is left positioned on the matched entry; callers that
// want the first unread record must call Next again.
func advanceToExactCursor(j journalCursorNavigator, position string) error {
	if err := j.SeekCursor(position); err != nil {
		return fmt.Errorf("seek cursor %q: %w", position, err)
	}
	n, err := j.Next()
	if err != nil {
		return fmt.Errorf("advance to cursor %q: %w", position, err)
	}
	if n == 0 {
		return fmt.Errorf("cursor %q not found: no matching or following journal entry", position)
	}
	if err := j.TestCursor(position); err != nil {
		return fmt.Errorf("cursor %q is no longer valid in journal: %w", position, err)
	}
	return nil
}
