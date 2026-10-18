// Package worker executes durable jobs with bounded local concurrency.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/neev-rao/distributed-agent-service/internal/durable"
	"github.com/neev-rao/distributed-agent-service/internal/jobs"
)

var ErrDrainTimeout = errors.New("worker shutdown grace period expired")

// Store exposes only the operations required by execution. Complete and Renew
// must condition their writes on the current, unexpired attempt token.
type Store interface {
	Claim(context.Context, string) (durable.Attempt, bool, error)
	Renew(context.Context, durable.Attempt) (bool, error)
	Complete(context.Context, durable.Attempt, jobs.Result, *jobs.JobError) (bool, error)
	RecoverExpired(context.Context) (int64, error)
	Cleanup(context.Context) (int64, error)
}

type Config struct {
	WorkerID            string
	Concurrency         int
	PollInterval        time.Duration
	RenewInterval       time.Duration
	StoreTimeout        time.Duration
	MaintenanceInterval time.Duration
	ShutdownGrace       time.Duration
	MaxResultBytes      int
	Logger              *slog.Logger
}

func DefaultConfig() Config {
	return Config{
		Concurrency: 2, PollInterval: 250 * time.Millisecond,
		RenewInterval: 2 * time.Second, StoreTimeout: time.Second,
		MaintenanceInterval: time.Second, ShutdownGrace: 10 * time.Second,
		MaxResultBytes: 64 << 10,
	}
}

type Runtime struct {
	config   Config
	store    Store
	executor jobs.Executor
	logger   *slog.Logger
}

func New(config Config, store Store, executor jobs.Executor) (*Runtime, error) {
	if config.WorkerID == "" || len(config.WorkerID) > 128 || config.Concurrency <= 0 || config.PollInterval <= 0 ||
		config.RenewInterval <= 0 || config.StoreTimeout <= 0 || config.MaintenanceInterval <= 0 ||
		config.ShutdownGrace <= 0 || config.MaxResultBytes <= 0 || store == nil || executor == nil {
		return nil, errors.New("worker ID must contain 1..128 bytes; store, executor and positive worker limits are required")
	}
	logger := config.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Runtime{config: config, store: store, executor: executor, logger: logger}, nil
}

// Run stops claiming when ctx is canceled. Existing attempts retain their leases
// during the drain period. At the cutoff their contexts are canceled and their
// leases are abandoned, allowing another worker to recover them. Executors must
// honor cancellation: arbitrary Go code cannot be forcibly stopped.
func (r *Runtime) Run(ctx context.Context) error {
	executionRoot, cancelExecutions := context.WithCancel(context.Background())
	defer cancelExecutions()
	var workers sync.WaitGroup
	workers.Add(r.config.Concurrency + 1)
	go func() {
		defer workers.Done()
		r.maintain(ctx)
	}()
	for range r.config.Concurrency {
		go func() {
			defer workers.Done()
			r.work(ctx, executionRoot)
		}()
	}
	done := make(chan struct{})
	go func() { workers.Wait(); close(done) }()
	<-ctx.Done()
	r.logger.Info("worker_draining", "worker_id", r.config.WorkerID, "grace", r.config.ShutdownGrace.String())
	timer := time.NewTimer(r.config.ShutdownGrace)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		cancelExecutions()
		r.logger.Warn("worker_drain_expired", "worker_id", r.config.WorkerID)
		return ErrDrainTimeout
	}
}

func (r *Runtime) work(claimRoot, executionRoot context.Context) {
	for claimRoot.Err() == nil {
		// Starting the local deadline before the database call is conservative:
		// network and claim latency never extend the database's execution budget.
		claimStarted := time.Now()
		claimCtx, cancel := context.WithTimeout(claimRoot, r.config.StoreTimeout)
		attempt, ok, err := r.store.Claim(claimCtx, r.config.WorkerID)
		cancel()
		if err != nil && claimRoot.Err() == nil {
			r.logger.Warn("job_claim_failed", "worker_id", r.config.WorkerID)
		}
		if err != nil || !ok {
			if !wait(claimRoot, r.config.PollInterval) {
				return
			}
			continue
		}
		if claimRoot.Err() != nil {
			// The claim may have committed just before shutdown was observed.
			// Abandon it without starting more execution during drain.
			return
		}
		r.execute(executionRoot, attempt, claimStarted.Add(attempt.ExecutionTimeout))
	}
}

func (r *Runtime) execute(root context.Context, attempt durable.Attempt, deadline time.Time) {
	ctx, cancel := context.WithDeadline(root, deadline)
	defer cancel()
	r.logger.Info("job_claimed", "worker_id", r.config.WorkerID, "job_id", attempt.Job.ID,
		"attempt_token", attempt.Token, "execution_deadline", attempt.Deadline)
	if root.Err() != nil {
		return
	}
	if !time.Now().Before(deadline) {
		r.complete(attempt, jobs.Result{}, &jobs.JobError{Code: "execution_timeout", Message: "execution deadline exceeded"})
		return
	}
	var lost atomic.Bool
	monitorStop := make(chan struct{})
	monitorDone := make(chan struct{})
	go func() {
		defer close(monitorDone)
		r.monitor(ctx, root, cancel, attempt, &lost, monitorStop)
	}()
	// Execute stays on the slot goroutine. A timeout can publish failure while
	// an uncooperative executor continues, but it cannot create extra capacity.
	result, executionErr := r.executor.Execute(ctx, attempt.Job.ScenarioID)
	close(monitorStop)
	<-monitorDone
	if root.Err() != nil || lost.Load() {
		return
	}
	var failure *jobs.JobError
	switch {
	case !time.Now().Before(deadline) || errors.Is(ctx.Err(), context.DeadlineExceeded):
		failure = &jobs.JobError{Code: "execution_timeout", Message: "execution deadline exceeded"}
	case executionErr != nil:
		failure = &jobs.JobError{Code: "execution_failed", Message: "executor returned an error"}
	default:
		encoded, err := json.Marshal(result)
		if err != nil || len(encoded) > r.config.MaxResultBytes {
			failure = &jobs.JobError{Code: "result_too_large", Message: "serialized result exceeds configured limit"}
		}
	}
	r.complete(attempt, result, failure)
}

func (r *Runtime) monitor(ctx, root context.Context, cancel context.CancelFunc, attempt durable.Attempt, lost *atomic.Bool, stop <-chan struct{}) {
	ticker := time.NewTicker(r.config.RenewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			if root.Err() == nil && errors.Is(ctx.Err(), context.DeadlineExceeded) && !lost.Load() {
				r.complete(attempt, jobs.Result{}, &jobs.JobError{Code: "execution_timeout", Message: "execution deadline exceeded"})
			}
			return
		case <-ticker.C:
			renewCtx, cancelRenew := context.WithTimeout(ctx, r.config.StoreTimeout)
			ok, err := r.store.Renew(renewCtx, attempt)
			cancelRenew()
			if ctx.Err() != nil {
				// A heartbeat interrupted by the execution deadline is a timeout,
				// not evidence that a different attempt owns the job.
				continue
			}
			if err != nil || !ok {
				lost.Store(true)
				cancel()
				r.logger.Warn("job_lease_lost", "worker_id", r.config.WorkerID, "job_id", attempt.Job.ID)
				return
			}
		}
	}
}

func (r *Runtime) complete(attempt durable.Attempt, result jobs.Result, failure *jobs.JobError) {
	// Execution contexts may already be expired. The guarded database update
	// receives a fresh, bounded context so timeout outcomes can still be saved.
	ctx, cancel := context.WithTimeout(context.Background(), r.config.StoreTimeout)
	defer cancel()
	ok, err := r.store.Complete(ctx, attempt, result, failure)
	if err != nil {
		r.logger.Warn("job_completion_unconfirmed", "worker_id", r.config.WorkerID, "job_id", attempt.Job.ID)
		return
	}
	if !ok {
		return
	}
	args := []any{"worker_id", r.config.WorkerID, "job_id", attempt.Job.ID}
	if failure != nil {
		args = append(args, "requested_error_code", failure.Code)
	}
	// The store may convert a late success to a timeout using database time.
	// This event confirms a write; clients read the authoritative state via GET.
	r.logger.Info("job_completion_recorded", args...)
}

func (r *Runtime) maintain(ctx context.Context) {
	var nextCleanup time.Time
	for ctx.Err() == nil {
		operationCtx, cancel := context.WithTimeout(ctx, r.config.StoreTimeout)
		count, err := r.store.RecoverExpired(operationCtx)
		cancel()
		if err != nil && ctx.Err() == nil {
			r.logger.Warn("job_recovery_unavailable", "worker_id", r.config.WorkerID)
		} else if count > 0 {
			r.logger.Info("jobs_recovered", "worker_id", r.config.WorkerID, "count", count)
		}
		if time.Now().After(nextCleanup) && ctx.Err() == nil {
			operationCtx, cancel = context.WithTimeout(ctx, r.config.StoreTimeout)
			count, err = r.store.Cleanup(operationCtx)
			cancel()
			if err != nil {
				r.logger.Warn("job_cleanup_unavailable", "worker_id", r.config.WorkerID)
			} else if count > 0 {
				r.logger.Info("jobs_cleaned", "worker_id", r.config.WorkerID, "count", count)
			}
			nextCleanup = time.Now().Add(time.Minute)
		}
		if !wait(ctx, r.config.MaintenanceInterval) {
			return
		}
	}
}

func wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
