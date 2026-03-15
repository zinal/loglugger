package server

import "fmt"

// PositionMismatchError indicates optimistic position update conflict.
type PositionMismatchError struct {
	CurrentPosition string
	Found           bool
}

func (e *PositionMismatchError) Error() string {
	if e == nil {
		return "position mismatch"
	}
	if !e.Found {
		return "position mismatch: no current position"
	}
	return fmt.Sprintf("position mismatch: current=%q", e.CurrentPosition)
}

