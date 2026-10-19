// Package durable stores jobs and attempt ownership in PostgreSQL. All queue
// growth is serialized by one settings-row lock; execution never holds a
// database transaction open. Time-sensitive ownership decisions use DB time.
package durable

import (
	"context"
	"crypto/rand"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/neev-rao/distributed-agent-service/internal/jobs"
)

var ErrIdempotencyConflict = errors.New("idempotency key already belongs to a different request")

//go:embed migrations/*.sql
var migrations embed.FS

type Config struct {
	QueueCapacity, MaxRecords, MaxAttempts, MaxResultBytes   int
	ExecutionTimeout, LeaseDuration, RetryBackoff, Retention time.Duration
	// ExecutionProfile identifies the executor and its model/prompt/budget
	// policy. It must never contain credentials or other secret material.
	ExecutionProfile string
}

func DefaultConfig() Config {
	return Config{
		QueueCapacity: 8, MaxRecords: 1000, MaxAttempts: 3, MaxResultBytes: 64 << 10,
		ExecutionTimeout: 5 * time.Second, LeaseDuration: 10 * time.Second,
		RetryBackoff: time.Second, Retention: 24 * time.Hour,
		ExecutionProfile: "fixture-v1",
	}
}

type Store struct {
	pool   *pgxpool.Pool
	config Config
}

// Attempt is a lease granted to one worker, not permission for exactly-once
// external effects. Its random token fences writes from earlier attempts.
type Attempt struct {
	Job                  jobs.Job
	Token, WorkerID      string
	ExecutionTimeout     time.Duration
	LeaseUntil, Deadline time.Time
}

// Open connects and checks reachability. Migrate must succeed before use.
func Open(ctx context.Context, dsn string, config Config) (*Store, error) {
	if config.QueueCapacity <= 0 || config.MaxRecords <= 0 || config.MaxAttempts <= 0 || config.MaxResultBytes <= 0 ||
		config.ExecutionTimeout < time.Microsecond || config.LeaseDuration < time.Microsecond ||
		config.RetryBackoff < time.Microsecond || config.Retention < time.Microsecond {
		return nil, errors.New("durable capacities and durations must be positive (durations at least one microsecond)")
	}
	if len(config.ExecutionProfile) < 1 || len(config.ExecutionProfile) > 256 {
		return nil, errors.New("execution profile must contain 1..256 printable ASCII bytes")
	}
	for _, b := range []byte(config.ExecutionProfile) {
		if b < 0x20 || b > 0x7e {
			return nil, errors.New("execution profile must contain 1..256 printable ASCII bytes")
		}
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("create database pool: %w", err)
	}
	if err = pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connect database: %w", err)
	}
	return &Store{pool: pool, config: config}, nil
}

func (s *Store) Close() { s.pool.Close() }

// Migrate atomically applies versioned embedded SQL. An advisory transaction
// lock coordinates concurrent process startup before the settings row exists.
// A mismatched runtime configuration is rejected rather than silently changing
// shared admission, recovery, or execution rules underneath another process.
func (s *Store) Migrate(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(71460382111901)"); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT clock_timestamp())`); err != nil {
		return err
	}
	var version int
	if err = tx.QueryRow(ctx, "SELECT COALESCE(MAX(version),0) FROM schema_migrations").Scan(&version); err != nil {
		return err
	}
	paths := []string{"migrations/001_initial.sql", "migrations/002_execution_profile.sql"}
	if version > len(paths) {
		return fmt.Errorf("database schema version %d is newer than this binary", version)
	}
	for version < len(paths) {
		sql, readErr := migrations.ReadFile(paths[version])
		if readErr != nil {
			return readErr
		}
		if _, err = tx.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("migration %d: %w", version+1, err)
		}
		version++
		if _, err = tx.Exec(ctx, "INSERT INTO schema_migrations(version) VALUES ($1)", version); err != nil {
			return err
		}
	}
	c := s.config
	if _, err = tx.Exec(ctx, `INSERT INTO service_settings
		(singleton,queue_capacity,max_records,max_attempts,max_result_bytes,execution_timeout_us,lease_duration_us,retry_backoff_us,retention_us,execution_profile)
		VALUES (true,$1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT (singleton) DO NOTHING`,
		c.QueueCapacity, c.MaxRecords, c.MaxAttempts, c.MaxResultBytes, c.ExecutionTimeout.Microseconds(), c.LeaseDuration.Microseconds(), c.RetryBackoff.Microseconds(), c.Retention.Microseconds(), c.ExecutionProfile); err != nil {
		return err
	}
	var matches bool
	err = tx.QueryRow(ctx, `SELECT queue_capacity=$1 AND max_records=$2 AND max_attempts=$3 AND max_result_bytes=$4
		AND execution_timeout_us=$5 AND lease_duration_us=$6 AND retry_backoff_us=$7 AND retention_us=$8
		AND execution_profile=$9
		FROM service_settings WHERE singleton`, c.QueueCapacity, c.MaxRecords, c.MaxAttempts, c.MaxResultBytes,
		c.ExecutionTimeout.Microseconds(), c.LeaseDuration.Microseconds(), c.RetryBackoff.Microseconds(), c.Retention.Microseconds(), c.ExecutionProfile).Scan(&matches)
	if err != nil {
		return err
	}
	if !matches {
		return errors.New("database service settings differ from runtime configuration")
	}
	return tx.Commit(ctx)
}

// Submit commits the record and waiting position together. Key matching uses
// the canonical input (currently scenario_id), not JSON whitespace or ordering.
// A commit error is ambiguous to a caller; retrying the same key resolves it.
func (s *Store) Submit(ctx context.Context, scenario, key string) (jobs.Job, error) {
	if scenario == "" || len(scenario) > 128 || len(key) > 128 {
		return jobs.Job{}, errors.New("scenario must contain 1..128 bytes and key at most 128 bytes")
	}
	tx, now, err := s.beginAdmission(ctx)
	if err != nil {
		return jobs.Job{}, err
	}
	defer tx.Rollback(ctx)
	if _, err = s.cleanup(ctx, tx, now); err != nil {
		return jobs.Job{}, err
	}
	if key != "" {
		job, lookupErr := scanJob(tx.QueryRow(ctx, selectJob+" WHERE idempotency_key=$1", key))
		if lookupErr == nil {
			if job.ScenarioID != scenario {
				return jobs.Job{}, ErrIdempotencyConflict
			}
			if err = tx.Commit(ctx); err != nil {
				return jobs.Job{}, err
			}
			return job, nil
		}
		if !errors.Is(lookupErr, pgx.ErrNoRows) {
			return jobs.Job{}, lookupErr
		}
	}
	var total, queued int
	if err = tx.QueryRow(ctx, "SELECT count(*),count(*) FILTER (WHERE state='queued') FROM jobs").Scan(&total, &queued); err != nil {
		return jobs.Job{}, err
	}
	if total >= s.config.MaxRecords {
		return jobs.Job{}, jobs.ErrRecordLimit
	}
	if queued >= s.config.QueueCapacity {
		return jobs.Job{}, jobs.ErrQueueFull
	}
	id, err := randomToken()
	if err != nil {
		return jobs.Job{}, err
	}
	var storedKey any
	if key != "" {
		storedKey = key
	}
	_, err = tx.Exec(ctx, `INSERT INTO jobs
		(id,scenario_id,idempotency_key,state,created_at,available_at,execution_timeout_us,max_attempts)
		VALUES ($1,$2,$3,'queued',$4,$4,$5,$6)`, id, scenario, storedKey, now, s.config.ExecutionTimeout.Microseconds(), s.config.MaxAttempts)
	if err != nil {
		return jobs.Job{}, err
	}
	job, err := scanJob(tx.QueryRow(ctx, selectJob+" WHERE id=$1", id))
	if err != nil {
		return jobs.Job{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return jobs.Job{}, err
	}
	return job, nil
}

func (s *Store) Get(ctx context.Context, id string) (jobs.Job, bool, error) {
	job, err := scanJob(s.pool.QueryRow(ctx, selectJob+" WHERE id=$1", id))
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.Job{}, false, nil
	}
	return job, err == nil, err
}

// Claim increments the attempt count and commits ownership before returning.
// SKIP LOCKED avoids waiting on a row touched by a heartbeat or completion.
func (s *Store) Claim(ctx context.Context, workerID string) (Attempt, bool, error) {
	if workerID == "" || len(workerID) > 128 {
		return Attempt{}, false, errors.New("worker ID must contain 1..128 bytes")
	}
	tx, now, err := s.beginAdmission(ctx)
	if err != nil {
		return Attempt{}, false, err
	}
	defer tx.Rollback(ctx)
	if _, err = s.recoverExpired(ctx, tx, now); err != nil {
		return Attempt{}, false, err
	}
	var id string
	var timeoutUS int64
	err = tx.QueryRow(ctx, `SELECT id,execution_timeout_us FROM jobs
		WHERE state='queued' AND available_at <= $1 ORDER BY available_at,created_at,id
		FOR UPDATE SKIP LOCKED LIMIT 1`, now).Scan(&id, &timeoutUS)
	if errors.Is(err, pgx.ErrNoRows) {
		if err = tx.Commit(ctx); err != nil {
			return Attempt{}, false, err
		}
		return Attempt{}, false, nil
	}
	if err != nil {
		return Attempt{}, false, err
	}
	token, err := randomToken()
	if err != nil {
		return Attempt{}, false, err
	}
	timeout := time.Duration(timeoutUS) * time.Microsecond
	deadline, leaseUntil := now.Add(timeout), now.Add(s.config.LeaseDuration)
	_, err = tx.Exec(ctx, `UPDATE jobs SET state='running',started_at=$2,attempt=attempt+1,
		attempt_token=$3,worker_id=$4,lease_until=$5,execution_deadline=$6,error=NULL WHERE id=$1`, id, now, token, workerID, leaseUntil, deadline)
	if err != nil {
		return Attempt{}, false, err
	}
	job, err := scanJob(tx.QueryRow(ctx, selectJob+" WHERE id=$1", id))
	if err != nil {
		return Attempt{}, false, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO job_attempts(token,job_id,attempt,worker_id,started_at,deadline,lease_until)
		VALUES ($1,$2,$3,$4,$5,$6,$7)`, token, id, job.Attempt, workerID, now, deadline, leaseUntil)
	if err != nil {
		return Attempt{}, false, err
	}
	// Reserve the slot this claim freed for expired work before a fresh Submit
	// can take it. Otherwise steady arrivals can starve recovery at saturation.
	if _, err = s.recoverExpired(ctx, tx, now); err != nil {
		return Attempt{}, false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Attempt{}, false, err
	}
	return Attempt{Job: job, Token: token, WorkerID: workerID, ExecutionTimeout: timeout, LeaseUntil: leaseUntil, Deadline: deadline}, true, nil
}

func (s *Store) Renew(ctx context.Context, attempt Attempt) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	_, valid, now, err := lockAttempt(ctx, tx, attempt)
	if err != nil || !valid {
		return false, err
	}
	until := now.Add(s.config.LeaseDuration)
	if _, err = tx.Exec(ctx, "UPDATE jobs SET lease_until=$2 WHERE id=$1", attempt.Job.ID, until); err != nil {
		return false, err
	}
	if _, err = tx.Exec(ctx, "UPDATE job_attempts SET lease_until=$2 WHERE token=$1", attempt.Token, until); err != nil {
		return false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

// Complete accepts only the current, unexpired attempt. Deadline enforcement is
// also performed here, so a slow caller cannot turn a timed-out job into success.
// Ordinary execution errors are terminal; automatic retries recover lost leases.
func (s *Store) Complete(ctx context.Context, attempt Attempt, result jobs.Result, failure *jobs.JobError) (bool, error) {
	var encoded []byte
	if failure == nil {
		var err error
		encoded, err = json.Marshal(result)
		if err != nil || len(encoded) > s.config.MaxResultBytes {
			failure = &jobs.JobError{Code: "result_too_large", Message: "serialized result exceeds configured limit"}
		}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	deadline, valid, now, err := lockAttempt(ctx, tx, attempt)
	if err != nil || !valid {
		return false, err
	}
	if !now.Before(deadline) {
		failure = &jobs.JobError{Code: "execution_timeout", Message: "execution deadline exceeded"}
	}
	state := jobs.Succeeded
	var resultJSON, errorJSON any
	if failure != nil {
		state = jobs.Failed
		data, marshalErr := json.Marshal(failure)
		if marshalErr != nil {
			return false, marshalErr
		}
		errorJSON = data
	} else {
		resultJSON = encoded
	}
	_, err = tx.Exec(ctx, `UPDATE jobs SET state=$2,finished_at=$3,result=$4,error=$5,
		attempt_token=NULL,worker_id=NULL,lease_until=NULL,execution_deadline=NULL WHERE id=$1`,
		attempt.Job.ID, string(state), now, resultJSON, errorJSON)
	if err != nil {
		return false, err
	}
	if _, err = tx.Exec(ctx, `UPDATE job_attempts SET finished_at=$2,outcome=$3 WHERE token=$1`, attempt.Token, now, string(state)); err != nil {
		return false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) RecoverExpired(ctx context.Context) (int64, error) {
	tx, now, err := s.beginAdmission(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	count, err := s.recoverExpired(ctx, tx, now)
	if err != nil {
		return 0, err
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, err
	}
	return count, nil
}

// recoverExpired holds the same admission lock as Submit and Claim. If waiting
// slots are full, recoverable attempts remain expired until a slot opens. They
// cannot renew or finish during that wait. Exhaustion never needs a queue slot.
func (s *Store) recoverExpired(ctx context.Context, tx pgx.Tx, now time.Time) (int64, error) {
	var queued int
	if err := tx.QueryRow(ctx, "SELECT count(*) FROM jobs WHERE state='queued'").Scan(&queued); err != nil {
		return 0, err
	}
	rows, err := tx.Query(ctx, `SELECT id,attempt_token,attempt,max_attempts FROM jobs
		WHERE state='running' AND lease_until <= $1 ORDER BY lease_until,id FOR UPDATE`, now)
	if err != nil {
		return 0, err
	}
	type expired struct {
		id, token   string
		number, max int
	}
	var expiredJobs []expired
	for rows.Next() {
		var item expired
		if err = rows.Scan(&item.id, &item.token, &item.number, &item.max); err != nil {
			rows.Close()
			return 0, err
		}
		expiredJobs = append(expiredJobs, item)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return 0, err
	}
	var count int64
	for _, item := range expiredJobs {
		exhausted := item.number >= item.max
		if !exhausted && queued >= s.config.QueueCapacity {
			continue
		}
		if _, err = tx.Exec(ctx, `UPDATE job_attempts SET finished_at=$2,outcome='lease_expired' WHERE token=$1`, item.token, now); err != nil {
			return 0, err
		}
		if exhausted {
			failure, _ := json.Marshal(jobs.JobError{Code: "attempts_exhausted", Message: "worker ownership expired after the maximum number of attempts"})
			_, err = tx.Exec(ctx, `UPDATE jobs SET state='failed',finished_at=$2,error=$3,
				attempt_token=NULL,worker_id=NULL,lease_until=NULL,execution_deadline=NULL WHERE id=$1`, item.id, now, failure)
		} else {
			_, err = tx.Exec(ctx, `UPDATE jobs SET state='queued',available_at=$2,started_at=NULL,
				attempt_token=NULL,worker_id=NULL,lease_until=NULL,execution_deadline=NULL WHERE id=$1`, item.id, now.Add(s.config.RetryBackoff))
			queued++
		}
		if err != nil {
			return 0, err
		}
		count++
	}
	return count, nil
}

// Cleanup removes at most 100 terminal records whose retention period ended.
// Cascading attempt history and the key disappear with the job. Active jobs,
// including expired attempts awaiting recovery capacity, are never deleted.
func (s *Store) Cleanup(ctx context.Context) (int64, error) {
	tx, now, err := s.beginAdmission(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	count, err := s.cleanup(ctx, tx, now)
	if err != nil {
		return 0, err
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, err
	}
	return count, nil
}

func (s *Store) cleanup(ctx context.Context, tx pgx.Tx, now time.Time) (int64, error) {
	tag, err := tx.Exec(ctx, `DELETE FROM jobs WHERE id IN (SELECT id FROM jobs
		WHERE state IN ('succeeded','failed') AND finished_at <= $1
		ORDER BY finished_at,id LIMIT 100 FOR UPDATE)`, now.Add(-s.config.Retention))
	return tag.RowsAffected(), err
}

func (s *Store) beginAdmission(ctx context.Context) (pgx.Tx, time.Time, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, time.Time{}, err
	}
	var singleton bool
	if err = tx.QueryRow(ctx, "SELECT singleton FROM service_settings WHERE singleton FOR UPDATE").Scan(&singleton); err != nil {
		tx.Rollback(ctx)
		return nil, time.Time{}, err
	}
	var now time.Time
	if err = tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		tx.Rollback(ctx)
		return nil, time.Time{}, err
	}
	return tx, now.UTC(), nil
}

// Lock first, then read the database clock. A statement-start timestamp taken
// before waiting on a lock could otherwise authorize an already expired lease.
func lockAttempt(ctx context.Context, tx pgx.Tx, attempt Attempt) (time.Time, bool, time.Time, error) {
	var token, worker string
	var lease, deadline time.Time
	err := tx.QueryRow(ctx, `SELECT attempt_token,worker_id,lease_until,execution_deadline
		FROM jobs WHERE id=$1 AND state='running' FOR UPDATE`, attempt.Job.ID).Scan(&token, &worker, &lease, &deadline)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, false, time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, false, time.Time{}, err
	}
	var now time.Time
	if err = tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return time.Time{}, false, time.Time{}, err
	}
	return deadline.UTC(), token == attempt.Token && worker == attempt.WorkerID && now.Before(lease), now.UTC(), nil
}

const selectJob = `SELECT id,scenario_id,state,created_at,started_at,finished_at,result,error,attempt,max_attempts FROM jobs`

func scanJob(row pgx.Row) (jobs.Job, error) {
	var job jobs.Job
	var result, failure []byte
	err := row.Scan(&job.ID, &job.ScenarioID, &job.State, &job.CreatedAt, &job.StartedAt, &job.FinishedAt, &result, &failure, &job.Attempt, &job.MaxAttempts)
	if err != nil {
		return jobs.Job{}, err
	}
	job.CreatedAt = job.CreatedAt.UTC()
	if job.StartedAt != nil {
		t := job.StartedAt.UTC()
		job.StartedAt = &t
	}
	if job.FinishedAt != nil {
		t := job.FinishedAt.UTC()
		job.FinishedAt = &t
	}
	if len(result) > 0 {
		job.Result = &jobs.Result{}
		if err = json.Unmarshal(result, job.Result); err != nil {
			return jobs.Job{}, err
		}
	}
	if len(failure) > 0 {
		job.Error = &jobs.JobError{}
		if err = json.Unmarshal(failure, job.Error); err != nil {
			return jobs.Job{}, err
		}
	}
	return job, nil
}

func randomToken() (string, error) {
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", fmt.Errorf("generate random token: %w", err)
	}
	return hex.EncodeToString(token[:]), nil
}
