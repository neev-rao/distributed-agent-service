package durable_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/neev-rao/distributed-agent-service/internal/durable"
	"github.com/neev-rao/distributed-agent-service/internal/jobs"
)

// Every test owns one randomly named schema. TEST_DATABASE_URL must identify a
// disposable development/test database where this user may create schemas.
// Tests never migrate or delete the database's public schema.
type databaseFixture struct {
	t      *testing.T
	ctx    context.Context
	dsn    string
	config durable.Config
	db     *pgxpool.Pool
	store  *durable.Store
}

func newDatabase(t *testing.T, config durable.Config) *databaseFixture {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	var suffix [12]byte
	if _, err = rand.Read(suffix[:]); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	schema := "test_" + hex.EncodeToString(suffix[:])
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		admin.Close()
		t.Fatalf("create isolated test schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if _, err := admin.Exec(cleanupCtx, "DROP SCHEMA "+quoted+" CASCADE"); err != nil {
			t.Errorf("drop owned test schema: %v", err)
		}
		admin.Close()
	})
	scoped := dsn + " search_path=" + schema
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		parsed, parseErr := url.Parse(dsn)
		if parseErr != nil {
			t.Fatal(parseErr)
		}
		query := parsed.Query()
		query.Set("search_path", schema)
		parsed.RawQuery = query.Encode()
		scoped = parsed.String()
	}
	db, err := pgxpool.New(ctx, scoped)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	f := &databaseFixture{t: t, ctx: ctx, dsn: scoped, config: config, db: db}
	f.store = f.open(config)
	return f
}

func (f *databaseFixture) open(config durable.Config) *durable.Store {
	f.t.Helper()
	s, err := durable.Open(f.ctx, f.dsn, config)
	if err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(s.Close)
	if err = s.Migrate(f.ctx); err != nil {
		f.t.Fatal(err)
	}
	return s
}

func (f *databaseFixture) exec(sql string, args ...any) {
	f.t.Helper()
	if _, err := f.db.Exec(f.ctx, sql, args...); err != nil {
		f.t.Fatal(err)
	}
}

func (f *databaseFixture) submit(key string) jobs.Job {
	f.t.Helper()
	j, err := f.store.Submit(f.ctx, "checkout-db-timeout", key)
	if err != nil {
		f.t.Fatal(err)
	}
	return j
}

func (f *databaseFixture) claim(s *durable.Store) durable.Attempt {
	f.t.Helper()
	a, ok, err := s.Claim(f.ctx, "test-worker")
	if err != nil || !ok {
		f.t.Fatalf("claim: found=%v error=%v", ok, err)
	}
	return a
}

func (f *databaseFixture) get(id string) jobs.Job {
	f.t.Helper()
	j, ok, err := f.store.Get(f.ctx, id)
	if err != nil || !ok {
		f.t.Fatalf("get: found=%v error=%v", ok, err)
	}
	return j
}

func (f *databaseFixture) complete(a durable.Attempt) {
	f.t.Helper()
	ok, err := f.store.Complete(f.ctx, a, jobs.Result{Diagnosis: "database timeout", EvidenceIDs: []string{"metric-1"}}, nil)
	if err != nil || !ok {
		f.t.Fatalf("complete: accepted=%v error=%v", ok, err)
	}
}

func TestPostgresPersistsAcrossIndependentPools(t *testing.T) {
	f := newDatabase(t, durable.DefaultConfig())
	j := f.submit("persistent-request")
	second := f.open(f.config)
	a := f.claim(second)
	if a.Job.ID != j.ID || a.Job.Attempt != 1 || a.Job.MaxAttempts != f.config.MaxAttempts {
		t.Fatalf("unexpected attempt: %+v", a)
	}
	f.complete(a)
	f.store.Close()
	reopened := f.open(f.config)
	got, ok, err := reopened.Get(f.ctx, j.ID)
	if err != nil || !ok || got.State != jobs.Succeeded || got.Result == nil || got.Result.Diagnosis != "database timeout" || got.StartedAt == nil || got.FinishedAt == nil {
		t.Fatalf("persisted result: job=%+v found=%v error=%v", got, ok, err)
	}
	if _, ok, err := reopened.Get(f.ctx, "missing"); ok || err != nil {
		t.Fatalf("unknown job: found=%v error=%v", ok, err)
	}
	// A process cannot silently change shared limits established by migration.
	mismatch := f.config
	mismatch.QueueCapacity++
	other, err := durable.Open(f.ctx, f.dsn, mismatch)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if err = other.Migrate(f.ctx); err == nil {
		t.Fatal("mismatched shared settings accepted")
	}
}

func TestPostgresConcurrentIdempotencyBeforeCapacityChecks(t *testing.T) {
	cfg := durable.DefaultConfig()
	cfg.QueueCapacity = 1
	cfg.MaxRecords = 1
	f := newDatabase(t, cfg)
	second := f.open(cfg)
	const requests = 24
	ids := make(chan string, requests)
	errorsCh := make(chan error, requests)
	start := make(chan struct{})
	var group sync.WaitGroup
	for n := 0; n < requests; n++ {
		group.Add(1)
		go func(n int) {
			defer group.Done()
			<-start
			s := f.store
			if n%2 != 0 {
				s = second
			}
			j, err := s.Submit(f.ctx, "checkout-db-timeout", "same-key")
			if err != nil {
				errorsCh <- err
				return
			}
			ids <- j.ID
		}(n)
	}
	close(start)
	group.Wait()
	close(ids)
	close(errorsCh)
	for err := range errorsCh {
		t.Error(err)
	}
	first := ""
	count := 0
	for id := range ids {
		if first == "" {
			first = id
		}
		if id != first {
			t.Errorf("duplicate submission created %s and %s", first, id)
		}
		count++
	}
	if count != requests {
		t.Fatalf("successful submissions=%d want %d", count, requests)
	}
	if _, err := second.Submit(f.ctx, "different-scenario", "same-key"); !errors.Is(err, durable.ErrIdempotencyConflict) {
		t.Fatalf("conflicting reuse: %v", err)
	}
	var records int
	if err := f.db.QueryRow(f.ctx, "SELECT count(*) FROM jobs").Scan(&records); err != nil {
		t.Fatal(err)
	}
	if records != 1 {
		t.Fatalf("records=%d want 1", records)
	}
}

func TestPostgresConcurrentAdmissionKeepsSharedBounds(t *testing.T) {
	for _, limit := range []string{"queue", "records"} {
		t.Run(limit, func(t *testing.T) {
			cfg := durable.DefaultConfig()
			cfg.QueueCapacity = 3
			cfg.MaxRecords = 30
			wantErr := jobs.ErrQueueFull
			if limit == "records" {
				cfg.QueueCapacity = 30
				cfg.MaxRecords = 3
				wantErr = jobs.ErrRecordLimit
			}
			f := newDatabase(t, cfg)
			second := f.open(cfg)
			results := make(chan error, 30)
			start := make(chan struct{})
			var group sync.WaitGroup
			for n := 0; n < 30; n++ {
				group.Add(1)
				go func(n int) {
					defer group.Done()
					<-start
					s := f.store
					if n%2 != 0 {
						s = second
					}
					_, err := s.Submit(f.ctx, "checkout-db-timeout", fmt.Sprint(n))
					results <- err
				}(n)
			}
			close(start)
			group.Wait()
			close(results)
			accepted := 0
			for err := range results {
				if err == nil {
					accepted++
				} else if !errors.Is(err, wantErr) {
					t.Errorf("unexpected admission error: %v", err)
				}
			}
			if accepted != 3 {
				t.Fatalf("accepted=%d want 3", accepted)
			}
			var total, queued int
			if err := f.db.QueryRow(f.ctx, "SELECT count(*),count(*) FILTER(WHERE state='queued') FROM jobs").Scan(&total, &queued); err != nil {
				t.Fatal(err)
			}
			if total != 3 || queued != 3 {
				t.Fatalf("orphan or overflow: total=%d queued=%d", total, queued)
			}
		})
	}
}

func TestPostgresCompetingWorkersClaimEachJobOnce(t *testing.T) {
	f := newDatabase(t, durable.DefaultConfig())
	second := f.open(f.config)
	for n := 0; n < 8; n++ {
		f.submit(fmt.Sprint(n))
	}
	type outcome struct {
		a   durable.Attempt
		ok  bool
		err error
	}
	results := make(chan outcome, 24)
	start := make(chan struct{})
	var group sync.WaitGroup
	for n := 0; n < 24; n++ {
		group.Add(1)
		go func(n int) {
			defer group.Done()
			<-start
			s := f.store
			if n%2 != 0 {
				s = second
			}
			a, ok, err := s.Claim(f.ctx, fmt.Sprintf("worker-%d", n))
			results <- outcome{a, ok, err}
		}(n)
	}
	close(start)
	group.Wait()
	close(results)
	seen := map[string]bool{}
	for result := range results {
		if result.err != nil {
			t.Error(result.err)
			continue
		}
		if !result.ok {
			continue
		}
		if seen[result.a.Job.ID] {
			t.Errorf("job claimed twice: %s", result.a.Job.ID)
		}
		seen[result.a.Job.ID] = true
		if result.a.Token == "" || result.a.Job.State != jobs.Running || result.a.Job.Attempt != 1 {
			t.Errorf("invalid attempt: %+v", result.a)
		}
	}
	if len(seen) != 8 {
		t.Fatalf("claimed %d unique jobs want 8", len(seen))
	}
}

func TestPostgresOwnershipExpiryAndStaleCompletion(t *testing.T) {
	cfg := durable.DefaultConfig()
	cfg.RetryBackoff = time.Hour
	f := newDatabase(t, cfg)
	j := f.submit("ownership")
	a := f.claim(f.store)
	if ok, err := f.store.Renew(f.ctx, a); err != nil || !ok {
		t.Fatalf("valid renewal: %v %v", ok, err)
	}
	for _, kind := range []string{"token", "worker"} {
		forged := a
		if kind == "token" {
			forged.Token = "wrong"
		} else {
			forged.WorkerID = "wrong"
		}
		if ok, err := f.store.Complete(f.ctx, forged, jobs.Result{Diagnosis: "forged"}, nil); err != nil || ok {
			t.Fatalf("forged %s accepted=%v error=%v", kind, ok, err)
		}
	}
	// Move DB-owned timestamps instead of waiting on a wall-clock sleep.
	f.exec("UPDATE jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", j.ID)
	if ok, err := f.store.Renew(f.ctx, a); err != nil || ok {
		t.Fatalf("expired lease renewed: %v %v", ok, err)
	}
	if ok, err := f.store.Complete(f.ctx, a, jobs.Result{Diagnosis: "expired"}, nil); err != nil || ok {
		t.Fatalf("expired completion: %v %v", ok, err)
	}
	if n, err := f.store.RecoverExpired(f.ctx); err != nil || n != 1 {
		t.Fatalf("recovery count=%d error=%v", n, err)
	}
	if got := f.get(j.ID); got.State != jobs.Queued || got.Attempt != 1 {
		t.Fatalf("recovered job=%+v", got)
	}
	if _, ok, err := f.store.Claim(f.ctx, "too-soon"); err != nil || ok {
		t.Fatalf("backoff bypass: %v %v", ok, err)
	}
	f.exec("UPDATE jobs SET available_at=clock_timestamp()-interval '1 second' WHERE id=$1", j.ID)
	b := f.claim(f.open(f.config))
	if b.Token == a.Token || b.Job.Attempt != 2 {
		t.Fatalf("retry failed to change ownership: %+v", b)
	}
	if ok, err := f.store.Complete(f.ctx, a, jobs.Result{Diagnosis: "stale"}, nil); err != nil || ok {
		t.Fatalf("stale token accepted=%v error=%v", ok, err)
	}
	f.complete(b)
	if ok, err := f.store.Complete(f.ctx, b, jobs.Result{Diagnosis: "overwrite"}, nil); err != nil || ok {
		t.Fatalf("terminal state overwritten=%v error=%v", ok, err)
	}
	if got := f.get(j.ID); got.Result == nil || got.Result.Diagnosis != "database timeout" {
		t.Fatalf("terminal result=%+v", got)
	}
}

func TestPostgresRecoveryBoundedByQueueAndAttemptLimit(t *testing.T) {
	cfg := durable.DefaultConfig()
	cfg.QueueCapacity = 1
	cfg.MaxAttempts = 2
	f := newDatabase(t, cfg)
	j := f.submit("retry")
	first := f.claim(f.store)
	f.submit("waiting")
	f.exec("UPDATE jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", j.ID)
	if n, err := f.store.RecoverExpired(f.ctx); err != nil || n != 0 {
		t.Fatalf("full queue recovery=%d error=%v", n, err)
	}
	if got := f.get(j.ID); got.State != jobs.Running {
		t.Fatalf("expired running job dropped: %+v", got)
	}
	blocker := f.claim(f.store)
	f.complete(blocker)
	// Claim reserves its newly freed slot for expired work before admitting
	// further submissions. The following recovery has nothing left to move.
	if n, err := f.store.RecoverExpired(f.ctx); err != nil || n != 0 {
		t.Fatalf("claim did not reserve available recovery slot: count=%d error=%v", n, err)
	}
	if got := f.get(j.ID); got.State != jobs.Queued {
		t.Fatalf("claim did not recover expired job: %+v", got)
	}
	f.exec("UPDATE jobs SET available_at=clock_timestamp()-interval '1 second' WHERE id=$1", j.ID)
	second := f.claim(f.store)
	if second.Job.Attempt != 2 {
		t.Fatalf("attempt=%d", second.Job.Attempt)
	}
	f.submit("fill-queue-again")
	f.exec("UPDATE jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", j.ID)
	if n, err := f.store.RecoverExpired(f.ctx); err != nil || n != 1 {
		t.Fatalf("exhaustion recovery=%d error=%v", n, err)
	}
	got := f.get(j.ID)
	if got.State != jobs.Failed || got.Error == nil || got.Error.Code != "attempts_exhausted" || got.Attempt != 2 {
		t.Fatalf("exhaustion job=%+v", got)
	}
	if ok, err := f.store.Complete(f.ctx, first, jobs.Result{}, nil); err != nil || ok {
		t.Fatalf("original attempt revived: %v %v", ok, err)
	}
}

func TestPostgresCompletionEnforcesDeadlineAndResultLimit(t *testing.T) {
	for _, kind := range []string{"timeout", "result_size", "executor_error"} {
		t.Run(kind, func(t *testing.T) {
			cfg := durable.DefaultConfig()
			cfg.MaxResultBytes = 128
			f := newDatabase(t, cfg)
			j := f.submit("")
			a := f.claim(f.store)
			result := jobs.Result{Diagnosis: "success"}
			var failure *jobs.JobError
			want := ""
			switch kind {
			case "timeout":
				f.exec("UPDATE jobs SET execution_deadline=clock_timestamp()-interval '1 second' WHERE id=$1", j.ID)
				want = "execution_timeout"
			case "result_size":
				result.Diagnosis = strings.Repeat("x", 129)
				want = "result_too_large"
			case "executor_error":
				failure = &jobs.JobError{Code: "execution_error", Message: "fixture failed"}
				want = "execution_error"
			}
			if ok, err := f.store.Complete(f.ctx, a, result, failure); err != nil || !ok {
				t.Fatalf("complete accepted=%v error=%v", ok, err)
			}
			got := f.get(j.ID)
			if got.State != jobs.Failed || got.Error == nil || got.Error.Code != want || got.Result != nil {
				t.Fatalf("failure result=%+v want code=%s", got, want)
			}
			if n, err := f.store.RecoverExpired(f.ctx); err != nil || n != 0 {
				t.Fatalf("ordinary terminal failure retried: %d %v", n, err)
			}
		})
	}
}

func TestPostgresRetentionPreservesActiveKeysAndBoundsCleanup(t *testing.T) {
	cfg := durable.DefaultConfig()
	cfg.MaxRecords = 200
	f := newDatabase(t, cfg)
	active := f.submit("active-key")
	f.exec("UPDATE jobs SET created_at=clock_timestamp()-interval '2 days' WHERE id=$1", active.ID)
	finished := f.submit("finished-key")
	a := f.claim(f.store)
	// Claim order follows created_at, so the old active job is claimed first.
	if a.Job.ID != active.ID {
		t.Fatalf("claim order: %+v", a.Job)
	}
	b := f.claim(f.store)
	if b.Job.ID != finished.ID {
		t.Fatalf("claim finished job: %+v", b.Job)
	}
	f.complete(b)
	if n, err := f.store.Cleanup(f.ctx); err != nil || n != 0 {
		t.Fatalf("retention deleted recent result: %d %v", n, err)
	}
	if same, err := f.store.Submit(f.ctx, active.ScenarioID, "active-key"); err != nil || same.ID != active.ID {
		t.Fatalf("active key expired: %+v %v", same, err)
	}
	if same, err := f.store.Submit(f.ctx, finished.ScenarioID, "finished-key"); err != nil || same.ID != finished.ID {
		t.Fatalf("retained key expired: %+v %v", same, err)
	}
	f.exec("UPDATE jobs SET finished_at=clock_timestamp()-interval '2 days' WHERE id=$1", finished.ID)
	// Bulk seed only this test's isolated schema to exercise the cleanup bound.
	f.exec(`INSERT INTO jobs(id,scenario_id,state,created_at,available_at,finished_at,execution_timeout_us,max_attempts,error)
		SELECT 'old-'||n,'fixture','failed',clock_timestamp()-interval '3 days',clock_timestamp()-interval '3 days',
		clock_timestamp()-interval '3 days',5000000,3,'{"code":"fixture","message":"old"}'::jsonb FROM generate_series(1,101) n`)
	if n, err := f.store.Cleanup(f.ctx); err != nil || n != 100 {
		t.Fatalf("cleanup batch=%d error=%v", n, err)
	}
	if _, ok, err := f.store.Get(f.ctx, active.ID); err != nil || !ok {
		t.Fatalf("cleanup removed active job: %v %v", ok, err)
	}
	if n, err := f.store.Cleanup(f.ctx); err != nil || n != 2 {
		t.Fatalf("remaining cleanup=%d error=%v", n, err)
	}
	if _, ok, err := f.store.Get(f.ctx, finished.ID); err != nil || ok {
		t.Fatalf("expired terminal retained: %v %v", ok, err)
	}
	var attempts int
	if err := f.db.QueryRow(f.ctx, "SELECT count(*) FROM job_attempts WHERE job_id=$1", finished.ID).Scan(&attempts); err != nil || attempts != 0 {
		t.Fatalf("orphan attempts=%d error=%v", attempts, err)
	}
	reused, err := f.store.Submit(f.ctx, finished.ScenarioID, "finished-key")
	if err != nil || reused.ID == finished.ID {
		t.Fatalf("expired key did not permit new job: %+v %v", reused, err)
	}
}

func TestPostgresClaimsReserveFreedCapacityForExpiredWork(t *testing.T) {
	config := durable.DefaultConfig()
	config.QueueCapacity = 1
	f := newDatabase(t, config)
	old := f.submit("old")
	oldAttempt := f.claim(f.store)
	f.exec("UPDATE jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", old.ID)
	// Admission fills the queue while the previous attempt is expired.
	newer := f.submit("newer")
	claimed := f.claim(f.store)
	if claimed.Job.ID != newer.ID {
		t.Fatalf("claimed %s, wanted queued job %s", claimed.Job.ID, newer.ID)
	}
	// Claim must reserve the released waiting slot for old work within its tx.
	if _, err := f.store.Submit(f.ctx, "checkout-db-timeout", "fresh-arrival"); !errors.Is(err, jobs.ErrQueueFull) {
		t.Fatalf("fresh submission displaced recovery: %v", err)
	}
	recovered := f.get(old.ID)
	if recovered.State != jobs.Queued {
		t.Fatalf("expired job not queued: %+v", recovered)
	}
	if ok, err := f.store.Complete(f.ctx, oldAttempt, jobs.Result{Diagnosis: "stale"}, nil); err != nil || ok {
		t.Fatalf("old completion authorized after reserved recovery: ok=%v err=%v", ok, err)
	}
}

func TestPostgresCancelledOperationsReturnErrors(t *testing.T) {
	f := newDatabase(t, durable.DefaultConfig())
	j := f.submit("exists")
	a := f.claim(f.store)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := f.store.Submit(ctx, "checkout-db-timeout", "cancelled"); err == nil {
		t.Error("cancelled submission falsely accepted")
	}
	if _, _, err := f.store.Get(ctx, j.ID); err == nil {
		t.Error("cancelled lookup hid database failure")
	}
	if _, _, err := f.store.Claim(ctx, "worker"); err == nil {
		t.Error("cancelled claim hid database failure")
	}
	if _, err := f.store.Renew(ctx, a); err == nil {
		t.Error("cancelled renewal hid database failure")
	}
	if _, err := f.store.Complete(ctx, a, jobs.Result{}, nil); err == nil {
		t.Error("cancelled completion hid database failure")
	}
	if _, err := f.store.RecoverExpired(ctx); err == nil {
		t.Error("cancelled recovery hid database failure")
	}
	if _, err := f.store.Cleanup(ctx); err == nil {
		t.Error("cancelled cleanup hid database failure")
	}
	var records int
	if err := f.db.QueryRow(f.ctx, "SELECT count(*) FROM jobs").Scan(&records); err != nil || records != 1 {
		t.Fatalf("rejected submission left record: %d %v", records, err)
	}
}

func TestPostgresAdmissionReclaimsExpiredRecordCapacity(t *testing.T) {
	cfg := durable.DefaultConfig()
	cfg.MaxRecords = 1
	f := newDatabase(t, cfg)
	old := f.submit("old-key")
	f.complete(f.claim(f.store))
	if _, err := f.store.Submit(f.ctx, "checkout-db-timeout", "new-key"); !errors.Is(err, jobs.ErrRecordLimit) {
		t.Fatalf("recent result did not retain capacity: %v", err)
	}
	f.exec("UPDATE jobs SET finished_at=clock_timestamp()-interval '2 days' WHERE id=$1", old.ID)
	fresh := f.submit("new-key")
	if fresh.ID == old.ID {
		t.Fatal("new request reused expired record")
	}
	if _, ok, err := f.store.Get(f.ctx, old.ID); err != nil || ok {
		t.Fatalf("admission cleanup retained expired record: %v %v", ok, err)
	}
}

func TestPostgresDatabaseErrorsAreNotReportedAsMissingOrAccepted(t *testing.T) {
	f := newDatabase(t, durable.DefaultConfig())
	j := f.submit("existing")
	// Break only this test's private schema. This exercises actual server SQL
	// errors, without stopping a shared server or disrupting other test schemas.
	f.exec("ALTER TABLE jobs RENAME TO unavailable_jobs")
	if _, err := f.store.Submit(f.ctx, "checkout-db-timeout", "must-not-exist"); err == nil {
		t.Error("database error falsely accepted submission")
	}
	if _, _, err := f.store.Get(f.ctx, j.ID); err == nil {
		t.Error("database error misreported as missing job")
	}
	if _, _, err := f.store.Claim(f.ctx, "worker"); err == nil {
		t.Error("database error misreported as empty queue")
	}
	f.exec("ALTER TABLE unavailable_jobs RENAME TO jobs")
	var records int
	if err := f.db.QueryRow(f.ctx, "SELECT count(*) FROM jobs").Scan(&records); err != nil || records != 1 {
		t.Fatalf("failed transaction left records: %d %v", records, err)
	}
}
