package server

import (
	"context"
)

// Writer writes rows to the destination (YDB or mock).
type Writer interface {
	BulkUpsert(ctx context.Context, table string, rows []map[string]interface{}) error
	GetPosition(ctx context.Context, clientID string) (string, bool, error)
	SetPosition(ctx context.Context, clientID, expectedPosition, nextPosition string) error
}
