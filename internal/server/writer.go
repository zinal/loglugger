package server

import (
	"context"
)

// Writer writes rows to the destination (YDB or mock).
type Writer interface {
	BulkUpsert(ctx context.Context, table string, rows []map[string]interface{}) error
}
