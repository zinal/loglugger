package server

import (
	"context"
	"fmt"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

// YDBPositionStore persists expected positions in YDB.
type YDBPositionStore struct {
	driver *ydb.Driver
	table  string
}

// NewYDBPositionStore creates a YDB-backed position store.
func NewYDBPositionStore(ctx context.Context, endpoint, database, tablePath string, auth YDBAuthOptions) (*YDBPositionStore, error) {
	driver, err := openYDBDriver(ctx, endpoint, database, auth)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(tablePath) == "" {
		_ = driver.Close(ctx)
		return nil, fmt.Errorf("position table is required")
	}
	return &YDBPositionStore{
		driver: driver,
		table:  tablePath,
	}, nil
}

func (s *YDBPositionStore) Get(ctx context.Context, clientID string) (string, bool, error) {
	var expectedPosition string
	var found bool
	err := s.driver.Table().Do(ctx, func(ctx context.Context, session table.Session) error {
		_, result, err := session.Execute(
			ctx,
			table.SerializableReadWriteTxControl(table.CommitTx()),
			fmt.Sprintf(`
DECLARE $client_id AS Utf8;
SELECT expected_position
FROM %s
WHERE client_id = $client_id
LIMIT 1;
`, quoteYDBPath(s.table)),
			ydb.ParamsBuilder().
				Param("$client_id").Text(clientID).
				Build(),
		)
		if err != nil {
			return err
		}
		defer result.Close()

		if result.NextResultSet(ctx, "expected_position") && result.NextRow() {
			if err := result.Scan(&expectedPosition); err != nil {
				return err
			}
			found = true
		}
		return result.Err()
	})
	if err != nil {
		return "", false, fmt.Errorf("get expected position from %s: %w", s.table, err)
	}
	return expectedPosition, found, nil
}

func (s *YDBPositionStore) Set(ctx context.Context, clientID, expectedPosition, nextPosition string) error {
	err := s.driver.Table().Do(ctx, func(ctx context.Context, session table.Session) error {
		_, _, err := session.Execute(
			ctx,
			table.SerializableReadWriteTxControl(table.CommitTx()),
			fmt.Sprintf(`
DECLARE $client_id AS Utf8;
DECLARE $old_expected_position AS Utf8;
DECLARE $new_expected_position AS Utf8;

$current_position = COALESCE((
    SELECT expected_position
    FROM %s
    WHERE client_id = $client_id
    LIMIT 1
), "");

SELECT Ensure(
    $current_position == $old_expected_position,
    "position mismatch"
);

UPSERT INTO %s (client_id, expected_position)
VALUES ($client_id, $new_expected_position);
`, quoteYDBPath(s.table), quoteYDBPath(s.table)),
			ydb.ParamsBuilder().
				Param("$client_id").Text(clientID).
				Param("$old_expected_position").Text(expectedPosition).
				Param("$new_expected_position").Text(nextPosition).
				Build(),
		)
		return err
	})
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "position mismatch") {
			current, found, getErr := s.Get(ctx, clientID)
			if getErr != nil {
				return fmt.Errorf("store expected position in %s: %w", s.table, err)
			}
			return &PositionMismatchError{CurrentPosition: current, Found: found}
		}
		return fmt.Errorf("store expected position in %s: %w", s.table, err)
	}
	return nil
}

func (s *YDBPositionStore) Close(ctx context.Context) error {
	return s.driver.Close(ctx)
}

func quoteYDBPath(path string) string {
	return "`" + strings.ReplaceAll(path, "`", "_") + "`"
}
