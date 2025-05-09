package crawshaw

import (
	"crawshaw.io/sqlite"
	"crawshaw.io/sqlite/sqlitex"
	"encoding/json"
	"fmt"
	"github.com/caasmo/restinpieces/db"
	"time"
)

func (d *Db) InsertJob(job db.Job) error {
	conn := d.pool.Get(nil)
	defer d.pool.Put(conn)

	var scheduledForStr string
	if !job.ScheduledFor.IsZero() {
		scheduledForStr = db.TimeFormat(job.ScheduledFor)
	}

	err := sqlitex.Exec(conn, `INSERT INTO job_queue
		(job_type, payload, payload_extra, attempts, max_attempts, recurrent, interval, scheduled_for)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		nil,
		job.JobType,
		string(job.Payload),
		string(job.PayloadExtra),
		job.Attempts,
		job.MaxAttempts,
		job.Recurrent,
		job.Interval.String(),
		scheduledForStr,
	)

	if err != nil {
		return fmt.Errorf("queue insert failed: %w", err)
	}
	return nil
}

func (d *Db) MarkCompleted(jobID int64) error {
	conn := d.pool.Get(nil)
	defer d.pool.Put(conn)

	err := sqlitex.Exec(conn,
		`UPDATE job_queue
		SET status = 'completed',
			completed_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
			updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
			locked_at = '',
			last_error = ''
		WHERE id = ?`,
		nil,
		jobID,
	)

	if err != nil {
		return fmt.Errorf("failed to mark job as completed: %w", err)
	}
	return nil
}

func (d *Db) MarkFailed(jobID int64, errMsg string) error {
	conn := d.pool.Get(nil)
	defer d.pool.Put(conn)

	err := sqlitex.Exec(conn,
		`UPDATE job_queue
		SET status = 'failed',
			updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
			locked_at = '',
			last_error = ?
		WHERE id = ?`,
		nil,
		errMsg,
		jobID,
	)

	if err != nil {
		return fmt.Errorf("failed to mark job as failed: %w", err)
	}
	return nil
}

func (d *Db) Claim(limit int) ([]*db.Job, error) {
	conn := d.pool.Get(nil)
	defer d.pool.Put(conn)

	var jobs []*db.Job
	sql := `UPDATE job_queue
		SET status = 'processing',
			locked_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
			attempts = attempts + 1
		WHERE id IN (
			SELECT id
			FROM job_queue
			WHERE status IN ('pending', 'failed')
			  AND scheduled_for <= strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
			ORDER BY id ASC
			LIMIT ?
		)
		RETURNING id, job_type, payload, payload_extra, status, attempts, max_attempts, created_at, updated_at,
			scheduled_for, locked_by, locked_at, completed_at, last_error, recurrent, interval`

	err := sqlitex.Exec(conn, sql,
		func(stmt *sqlite.Stmt) error {
			createdAt, err := db.TimeParse(stmt.GetText("created_at"))
			if err != nil {
				return fmt.Errorf("error parsing created_at time: %w", err)
			}

			updatedAt, err := db.TimeParse(stmt.GetText("updated_at"))
			if err != nil {
				return fmt.Errorf("error parsing updated_at time: %w", err)
			}

			var scheduledFor time.Time
			if scheduledForStr := stmt.GetText("scheduled_for"); scheduledForStr != "" {
				scheduledFor, err = db.TimeParse(scheduledForStr)
				if err != nil {
					return fmt.Errorf("error parsing scheduled_for time: %w", err)
				}
			}

			var lockedAt time.Time
			if lockedAtStr := stmt.GetText("locked_at"); lockedAtStr != "" {
				lockedAt, err = db.TimeParse(lockedAtStr)
				if err != nil {
					return fmt.Errorf("error parsing locked_at time: %w", err)
				}
			}

			var completedAt time.Time
			if completedAtStr := stmt.GetText("completed_at"); completedAtStr != "" {
				completedAt, err = db.TimeParse(completedAtStr)
				if err != nil {
					return fmt.Errorf("error parsing completed_at time: %w", err)
				}
			}

			var interval time.Duration
			if intervalStr := stmt.GetText("interval"); intervalStr != "" {
				interval, err = time.ParseDuration(intervalStr)
				if err != nil {
					return fmt.Errorf("error parsing interval duration '%s': %w", intervalStr, err)
				}
			}

			job := &db.Job{
				ID:           stmt.GetInt64("id"),
				JobType:      stmt.GetText("job_type"),
				Payload:      json.RawMessage(stmt.GetText("payload")),
				PayloadExtra: json.RawMessage(stmt.GetText("payload_extra")),
				Status:       stmt.GetText("status"),
				Attempts:     int(stmt.GetInt64("attempts")),
				MaxAttempts:  int(stmt.GetInt64("max_attempts")),
				CreatedAt:    createdAt,
				UpdatedAt:    updatedAt,
				ScheduledFor: scheduledFor,
				LockedBy:     stmt.GetText("locked_by"),
				LockedAt:     lockedAt,
				CompletedAt:  completedAt,
				LastError:    stmt.GetText("last_error"),
				Recurrent:    stmt.GetInt64("recurrent") != 0,
				Interval:     interval,
			}
			jobs = append(jobs, job)
			return nil
		}, limit)

	if err != nil {
		return nil, fmt.Errorf("failed to claim jobs: %w", err)
	}
	if jobs == nil {
		jobs = []*db.Job{}
	}
	return jobs, nil
}

func (d *Db) MarkRecurrentCompleted(completedJobID int64, newJob db.Job) error {
	conn := d.pool.Get(nil)
	if conn == nil {
		return fmt.Errorf("failed to get connection for mark recurrent completed: connection is nil")
	}
	defer d.pool.Put(conn)

	err := sqlitex.Exec(conn, "BEGIN IMMEDIATE;", nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction for mark recurrent completed: %w", err)
	}

	err = sqlitex.Exec(conn,
		`UPDATE job_queue
		SET status = 'completed',
			completed_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
			updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
			locked_at = '',
			last_error = ''
		WHERE id = ?`,
		nil,
		completedJobID,
	)
	if err != nil {
		_ = sqlitex.Exec(conn, "ROLLBACK;", nil)
		return fmt.Errorf("failed to mark job %d completed in transaction: %w", completedJobID, err)
	}

	var scheduledForStr string
	if !newJob.ScheduledFor.IsZero() {
		scheduledForStr = db.TimeFormat(newJob.ScheduledFor)
	}

	err = sqlitex.Exec(conn, `INSERT INTO job_queue
		(job_type, payload, payload_extra, attempts, max_attempts, recurrent, interval, scheduled_for)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		nil,
		newJob.JobType,
		string(newJob.Payload),
		string(newJob.PayloadExtra),
		newJob.Attempts,
		newJob.MaxAttempts,
		newJob.Recurrent,
		newJob.Interval.String(),
		scheduledForStr,
	)
	if err != nil {
		_ = sqlitex.Exec(conn, "ROLLBACK;", nil)
		return fmt.Errorf("failed to re-insert job in transaction: %w", err)
	}

	err = sqlitex.Exec(conn, "COMMIT;", nil)
	if err != nil {
		return fmt.Errorf("failed to commit transaction for mark recurrent completed: %w", err)
	}

	return nil
}
