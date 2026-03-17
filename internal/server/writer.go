package server

import (
	"context"
	"time"
)

// PositionUpdate carries metadata persisted together with position updates.
type PositionUpdate struct {
	TSWall    time.Time
	MaxSeqNo  *int64
	MaxTSOrig *time.Time
}

// Writer writes rows to the destination (YDB or mock).
type Writer interface {
	BulkUpsert(ctx context.Context, table string, rows []map[string]interface{}) error
	GetPosition(ctx context.Context, clientID string) (string, bool, error)
	SetPosition(ctx context.Context, clientID, expectedPosition, nextPosition string, update PositionUpdate) error
	SetPositionUnconditional(ctx context.Context, clientID, nextPosition string, update PositionUpdate) error
}
