package crawshaw

import (
	"crawshaw.io/sqlite"
	"crawshaw.io/sqlite/sqlitex"
	"fmt"
	"github.com/caasmo/restinpieces/db"
	"io"
	"time"
)

func (d *Db) LatestConfig(scope string) ([]byte, error) {
	conn := d.pool.Get(nil)
	if conn == nil {
		return nil, fmt.Errorf("failed to get db connection for scope '%s': connection is nil", scope)
	}
	defer d.pool.Put(conn)

	var contentData []byte
	err := sqlitex.Exec(conn,
		`SELECT content FROM app_config
		 WHERE scope = ?
		 ORDER BY created_at DESC
		 LIMIT 1;`,
		func(stmt *sqlite.Stmt) error {
			if stmt.ColumnCount() > 0 && stmt.ColumnType(0) != sqlite.SQLITE_NULL {
				reader := stmt.ColumnReader(0)
				var readErr error
				contentData, readErr = io.ReadAll(reader)
				return readErr
			}
			return nil
		},
		scope,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get latest config content for scope '%s': %w", scope, err)
	}

	return contentData, nil
}

func (d *Db) InsertConfig(scope string, contentData []byte, format string, description string) error {
	conn := d.pool.Get(nil)
	if conn == nil {
		return fmt.Errorf("failed to get db connection for config insert: connection is nil")
	}
	defer d.pool.Put(conn)

	now := db.TimeFormat(time.Now())

	err := sqlitex.Exec(conn,
		`INSERT INTO app_config (
			scope,
			content,
			format,
			description,
			created_at
		) VALUES (?, ?, ?, ?, ?)`,
		nil, // No result function needed for INSERT
		scope,
		contentData,
		format,
		description,
		now,
	)

	if err != nil {
		return fmt.Errorf("failed to insert config for scope '%s': %w", scope, err)
	}

	return nil
}
