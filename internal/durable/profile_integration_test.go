package durable_test

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/neev-rao/distributed-agent-service/internal/durable"
)

func TestExecutionProfileValidationBeforeDatabaseAccess(t *testing.T) {
	for name, value := range map[string]string{
		"empty": "", "too_long": strings.Repeat("x", 257), "newline": "fixture\nv1",
		"nul": "fixture\x00v1", "delete": "fixture\x7fv1", "unicode": "fixture-λ",
	} {
		t.Run(name, func(t *testing.T) {
			config := durable.DefaultConfig()
			config.ExecutionProfile = value
			s, err := durable.Open(context.Background(), "invalid database connection string", config)
			if s != nil {
				s.Close()
			}
			if err == nil || !strings.Contains(err.Error(), "execution profile must contain") {
				t.Fatalf("profile validation did not precede database access: %v", err)
			}
			if value != "" && strings.Contains(err.Error(), value) {
				t.Fatal("validation error disclosed the supplied profile")
			}
		})
	}
}

func TestPostgresProfileMigrationPreservesVersionOneWork(t *testing.T) {
	config := durable.DefaultConfig()
	config.QueueCapacity = 4
	config.MaxRecords = 25
	f := newDatabase(t, config)
	finished := f.submit("finished-before-upgrade")
	f.complete(f.claim(f.store))
	queued := f.submit("queued-before-upgrade")
	beforeFinished, beforeQueued := f.get(finished.ID), f.get(queued.ID)
	// 001_initial.sql remains unchanged. Removing only the v2 addition recreates
	// the exact v1 layout around real queued work and a completed attempt/result.
	f.exec("ALTER TABLE service_settings DROP COLUMN execution_profile")
	f.exec("DELETE FROM schema_migrations WHERE version=2")

	wrong := config
	wrong.ExecutionProfile = "gemini-v1:other-executor"
	other, err := durable.Open(f.ctx, f.dsn, wrong)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if err = other.Migrate(f.ctx); err == nil || !strings.Contains(err.Error(), "settings differ") {
		t.Fatalf("v1 fixture work accepted another executor: %v", err)
	}
	var version int
	var profileExists bool
	if err = f.db.QueryRow(f.ctx, "SELECT max(version) FROM schema_migrations").Scan(&version); err != nil || version != 1 {
		t.Fatalf("rejected migration changed version: %d %v", version, err)
	}
	if err = f.db.QueryRow(f.ctx, `SELECT EXISTS (SELECT 1 FROM information_schema.columns
		WHERE table_schema=current_schema() AND table_name='service_settings' AND column_name='execution_profile')`).Scan(&profileExists); err != nil || profileExists {
		t.Fatalf("rejected migration left schema changes: exists=%v err=%v", profileExists, err)
	}

	upgraded := f.open(config)
	if after := f.get(finished.ID); !reflect.DeepEqual(after, beforeFinished) {
		t.Fatalf("migration changed completed work: before=%+v after=%+v", beforeFinished, after)
	}
	if after := f.get(queued.ID); !reflect.DeepEqual(after, beforeQueued) {
		t.Fatalf("migration changed queued work: before=%+v after=%+v", beforeQueued, after)
	}
	replayed, err := upgraded.Submit(f.ctx, "checkout-db-timeout", "finished-before-upgrade")
	if err != nil || !reflect.DeepEqual(replayed, beforeFinished) {
		t.Fatalf("migration broke retained key replay: job=%+v err=%v", replayed, err)
	}
	attempt := f.claim(upgraded)
	if attempt.Job.ID != queued.ID || attempt.ExecutionTimeout != 5*time.Second {
		t.Fatalf("migration changed queued job or its execution budget: %+v", attempt)
	}
	var profile string
	if err = f.db.QueryRow(f.ctx, "SELECT execution_profile FROM service_settings WHERE singleton").Scan(&profile); err != nil || profile != "fixture-v1" {
		t.Fatalf("migration did not pin the existing fixture executor: %q %v", profile, err)
	}
	var attemptCount int
	if err = f.db.QueryRow(f.ctx, "SELECT count(*) FROM job_attempts WHERE job_id=$1 AND outcome='succeeded'", finished.ID).Scan(&attemptCount); err != nil || attemptCount != 1 {
		t.Fatalf("migration changed completed attempt history: count=%d err=%v", attemptCount, err)
	}
}

func TestPostgresExecutionProfileMismatchIsRejected(t *testing.T) {
	config := durable.DefaultConfig()
	config.ExecutionProfile = strings.Repeat("x", 256)
	f := newDatabase(t, config)
	job := f.submit("retained-key")
	otherConfig := config
	otherConfig.ExecutionProfile = "fixture-v1"
	other, err := durable.Open(f.ctx, f.dsn, otherConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if err = other.Migrate(f.ctx); err == nil || !strings.Contains(err.Error(), "settings differ") {
		t.Fatalf("different execution profile accepted: %v", err)
	}
	if got := f.get(job.ID); got.ID != job.ID {
		t.Fatal("rejected profile changed retained work")
	}
	var profile string
	if err = f.db.QueryRow(f.ctx, "SELECT execution_profile FROM service_settings WHERE singleton").Scan(&profile); err != nil || profile != config.ExecutionProfile {
		t.Fatalf("rejected profile changed shared settings: %q %v", profile, err)
	}
}

func TestPostgresConcurrentFreshProfileMigration(t *testing.T) {
	config := durable.DefaultConfig()
	config.ExecutionProfile = "gemini-v1:test-model-prompt-budget"
	config.ExecutionTimeout = 30 * time.Second
	f := newDatabase(t, config)
	// Reset only this test's private schema, then start several independent
	// connections without tables or a settings row to lock yet.
	f.exec("DROP TABLE job_attempts,jobs,service_settings,schema_migrations")
	const count = 4
	results := make(chan error, count)
	for range count {
		go func() {
			s, err := durable.Open(f.ctx, f.dsn, config)
			if err != nil {
				results <- err
				return
			}
			defer s.Close()
			results <- s.Migrate(f.ctx)
		}()
	}
	for range count {
		if err := <-results; err != nil {
			t.Errorf("concurrent migration: %v", err)
		}
	}
	var versions int
	var profile string
	var timeout int64
	if err := f.db.QueryRow(f.ctx, "SELECT count(*) FROM schema_migrations").Scan(&versions); err != nil || versions != 2 {
		t.Fatalf("migration versions: %d %v", versions, err)
	}
	if err := f.db.QueryRow(f.ctx, "SELECT execution_profile,execution_timeout_us FROM service_settings WHERE singleton").Scan(&profile, &timeout); err != nil || profile != config.ExecutionProfile || timeout != config.ExecutionTimeout.Microseconds() {
		t.Fatalf("fresh shared profile/budget mismatch: %q %d %v", profile, timeout, err)
	}
}

func TestPostgresFutureSchemaVersionIsRejected(t *testing.T) {
	f := newDatabase(t, durable.DefaultConfig())
	f.exec("INSERT INTO schema_migrations(version) VALUES (3)")
	err := f.store.Migrate(f.ctx)
	if err == nil || !strings.Contains(err.Error(), "version 3 is newer") {
		t.Fatalf("future schema version accepted: %v", err)
	}
}
