// Package jobs owns the volatile job lifecycle and bounded local execution.
package jobs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"
)

type State string

const (
	Queued    State = "queued"
	Running   State = "running"
	Succeeded State = "succeeded"
	Failed    State = "failed"
)

var (
	ErrQueueFull    = errors.New("waiting queue is full")
	ErrRecordLimit  = errors.New("retained job record limit reached")
	ErrShuttingDown = errors.New("service is shutting down")
)

type Result struct {
	Diagnosis   string   `json:"diagnosis"`
	EvidenceIDs []string `json:"evidence_ids"`
}

type JobError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// ExecutionFailure lets a trusted executor publish an inspectable, sanitized
// failure without coupling the worker to a particular model provider.
type ExecutionFailure interface {
	error
	Failure() JobError
}

type Job struct {
	ID          string     `json:"id"`
	ScenarioID  string     `json:"scenario_id"`
	State       State      `json:"state"`
	CreatedAt   time.Time  `json:"created_at"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	FinishedAt  *time.Time `json:"finished_at,omitempty"`
	Result      *Result    `json:"result,omitempty"`
	Error       *JobError  `json:"error,omitempty"`
	Attempt     int        `json:"attempt,omitempty"`
	MaxAttempts int        `json:"max_attempts,omitempty"`
}

// Execute must honor cancellation and must not mutate a result after returning it.
// Context cancellation cannot forcibly stop arbitrary Go code.
type Executor interface {
	Execute(context.Context, string) (Result, error)
}

type ExecutorFunc func(context.Context, string) (Result, error)

func (f ExecutorFunc) Execute(ctx context.Context, scenario string) (Result, error) {
	return f(ctx, scenario)
}

type Config struct {
	Workers          int
	QueueCapacity    int
	MaxRecords       int
	ExecutionTimeout time.Duration
	MaxResultBytes   int
	Logger           *slog.Logger
}

func DefaultConfig() Config {
	return Config{
		Workers: 2, QueueCapacity: 8, MaxRecords: 1000,
		ExecutionTimeout: 5 * time.Second, MaxResultBytes: 64 * 1024,
	}
}

// Manager is a single-process state owner, not a durable queue. The mutex covers
// admission, claiming, terminal transitions, and shutdown so they cannot disagree.
type Manager struct {
	mu            sync.Mutex
	wake          *sync.Cond
	config        Config
	executor      Executor
	logger        *slog.Logger
	jobs          map[string]*Job
	queue         []string
	draining      bool
	stopped       bool
	drainDeadline time.Time
	drainDone     <-chan struct{}
	serviceCtx    context.Context
	cancelService context.CancelFunc
	done          chan struct{}
}

func New(config Config, executor Executor) (*Manager, error) {
	if config.Workers <= 0 || config.QueueCapacity <= 0 || config.MaxRecords <= 0 ||
		config.ExecutionTimeout <= 0 || config.MaxResultBytes <= 0 || executor == nil {
		return nil, errors.New("workers, queue, records, timeout and result limit must be positive; executor is required")
	}
	logger := config.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{
		config: config, executor: executor, logger: logger,
		jobs: make(map[string]*Job), serviceCtx: ctx, cancelService: cancel,
		done: make(chan struct{}),
	}
	m.wake = sync.NewCond(&m.mu)
	var workers sync.WaitGroup
	workers.Add(config.Workers)
	for range config.Workers {
		go func() {
			defer workers.Done()
			m.work()
		}()
	}
	go func() {
		workers.Wait()
		cancel()
		close(m.done)
	}()
	return m, nil
}

// Submit admits a small, already validated scenario. HTTP validation lives at
// the API boundary. Rejection creates neither a record nor an executable job.
func (m *Manager) Submit(scenario string) (Job, error) {
	m.mu.Lock()
	if m.draining {
		m.mu.Unlock()
		return Job{}, ErrShuttingDown
	}
	if len(m.jobs) >= m.config.MaxRecords {
		m.mu.Unlock()
		return Job{}, ErrRecordLimit
	}
	if len(m.queue) >= m.config.QueueCapacity {
		m.mu.Unlock()
		return Job{}, ErrQueueFull
	}
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		m.mu.Unlock()
		return Job{}, fmt.Errorf("generate job ID: %w", err)
	}
	id := hex.EncodeToString(token[:])
	job := Job{ID: id, ScenarioID: scenario, State: Queued, CreatedAt: time.Now().UTC()}
	m.jobs[id] = &job
	m.queue = append(m.queue, id)
	snapshot := clone(job)
	m.wake.Signal()
	m.mu.Unlock()
	m.logState(snapshot)
	return snapshot, nil
}

// Get returns a detached snapshot, including copies of slices and pointers.
func (m *Manager) Get(id string) (Job, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	job, ok := m.jobs[id]
	if !ok {
		return Job{}, false
	}
	return clone(*job), true
}

// Shutdown closes admission and drains. Expiration fails unfinished jobs and
// cancels execution, returning promptly even if an executor ignores cancellation.
// Calling again with a live context can wait for those executors to actually exit.
func (m *Manager) Shutdown(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		m.forceStop()
		return err
	}
	m.mu.Lock()
	if !m.draining {
		// The first shutdown call owns the drain cancellation signal. Keep the
		// signal, not the submitting request context, so workers can check it
		// synchronously before claiming queued work.
		m.drainDone = ctx.Done()
	}
	m.draining = true
	if deadline, ok := ctx.Deadline(); ok && (m.drainDeadline.IsZero() || deadline.Before(m.drainDeadline)) {
		m.drainDeadline = deadline
	}
	m.wake.Broadcast()
	m.mu.Unlock()
	stop := context.AfterFunc(ctx, m.forceStop)
	defer stop()
	select {
	case <-m.done:
		m.mu.Lock()
		drainExpired := m.drainExpiredLocked()
		m.mu.Unlock()
		if drainExpired {
			m.forceStop()
		}
		// Workers can notice the wall-clock cutoff before the context timer's
		// callback runs. Finish forced transitions before returning to the caller.
		if executionExpired(ctx) {
			m.forceStop()
			return context.DeadlineExceeded
		}
		if err := ctx.Err(); err != nil {
			m.forceStop()
			return err
		}
		return nil
	case <-ctx.Done():
		m.forceStop()
		return ctx.Err()
	}
}

func (m *Manager) take() (Job, context.Context, context.CancelFunc, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for {
		// Check wall time here too: the shutdown caller may not have been scheduled
		// at its deadline, and a worker must not start another queued job meanwhile.
		if m.stopped || m.drainExpiredLocked() {
			return Job{}, nil, nil, false
		}
		if len(m.queue) > 0 {
			id := m.queue[0]
			m.queue[0] = ""
			m.queue = m.queue[1:]
			job := m.jobs[id]
			now := time.Now().UTC()
			job.State, job.StartedAt = Running, &now
			ctx, cancel := context.WithTimeout(m.serviceCtx, m.config.ExecutionTimeout)
			return clone(*job), ctx, cancel, true
		}
		if m.draining {
			return Job{}, nil, nil, false
		}
		m.wake.Wait()
	}
}

func (m *Manager) work() {
	for {
		job, ctx, cancel, ok := m.take()
		if !ok {
			return
		}
		m.logState(job)
		// Only the status callback is asynchronous. The executor stays on this
		// worker, so a timeout never creates spare execution capacity.
		stop := context.AfterFunc(ctx, func() { m.complete(job.ID, ctx, Result{}, nil) })
		result, err := m.executor.Execute(ctx, job.ScenarioID)
		stop()
		m.complete(job.ID, ctx, result, err)
		cancel()
	}
}

func (m *Manager) complete(id string, ctx context.Context, result Result, executionErr error) {
	// Validate executor output outside the state lock. Only bounded results are
	// retained; potentially expensive serialization must not block admission.
	resultTooLarge := false
	if executionErr == nil {
		encoded, err := json.Marshal(result)
		resultTooLarge = err != nil || len(encoded) > m.config.MaxResultBytes
	}
	m.mu.Lock()
	job := m.jobs[id]
	if job.State != Running {
		m.mu.Unlock()
		return
	}
	var failure *JobError
	switch {
	case m.stopped || m.drainExpiredLocked():
		failure = &JobError{Code: "service_shutdown", Message: "shutdown grace period ended"}
	case executionExpired(ctx):
		failure = &JobError{Code: "execution_timeout", Message: "execution deadline exceeded"}
	case ctx.Err() != nil:
		failure = &JobError{Code: "service_shutdown", Message: "execution canceled during shutdown"}
	case executionErr != nil:
		failure = &JobError{Code: "execution_failed", Message: "executor returned an error"}
	case resultTooLarge:
		failure = &JobError{Code: "result_too_large", Message: "serialized result exceeds configured limit"}
	}
	now := time.Now().UTC()
	job.FinishedAt = &now
	if failure != nil {
		job.State, job.Error = Failed, failure
	} else {
		job.State, job.Result = Succeeded, &result
		job.Result.EvidenceIDs = append([]string(nil), result.EvidenceIDs...)
	}
	snapshot := clone(*job)
	m.mu.Unlock()
	m.logState(snapshot)
}

func executionExpired(ctx context.Context) bool {
	deadline, ok := ctx.Deadline()
	return errors.Is(ctx.Err(), context.DeadlineExceeded) || (ok && !time.Now().Before(deadline))
}

func (m *Manager) drainExpiredLocked() bool {
	if !m.draining {
		return false
	}
	select {
	case <-m.drainDone:
		return true
	default:
		return !m.drainDeadline.IsZero() && !time.Now().Before(m.drainDeadline)
	}
}

func (m *Manager) forceStop() {
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return
	}
	m.stopped, m.draining = true, true
	m.queue = nil
	var failed []Job
	for _, job := range m.jobs {
		if job.State == Queued || job.State == Running {
			now := time.Now().UTC()
			job.State, job.FinishedAt = Failed, &now
			job.Error = &JobError{Code: "service_shutdown", Message: "shutdown grace period ended"}
			failed = append(failed, clone(*job))
		}
	}
	m.cancelService()
	m.wake.Broadcast()
	m.mu.Unlock()
	for _, job := range failed {
		m.logState(job)
	}
}

func (m *Manager) logState(job Job) {
	args := []any{"job_id", job.ID, "scenario_id", job.ScenarioID, "state", job.State}
	eventAt := job.CreatedAt
	if job.StartedAt != nil {
		eventAt = *job.StartedAt
	}
	if job.FinishedAt != nil {
		eventAt = *job.FinishedAt
	}
	// Logging is outside the state lock. Use the transition timestamp rather than
	// assuming concurrent log writes reach the sink in transition order.
	args = append(args, "event_at", eventAt)
	if job.Error != nil {
		args = append(args, "error_code", job.Error.Code)
	}
	if job.StartedAt != nil {
		args = append(args, "queue_wait_ms", job.StartedAt.Sub(job.CreatedAt).Milliseconds())
		if job.FinishedAt != nil {
			args = append(args, "execution_ms", job.FinishedAt.Sub(*job.StartedAt).Milliseconds())
		}
	}
	m.logger.Info("job state changed", args...)
}

func clone(job Job) Job {
	if job.StartedAt != nil {
		started := *job.StartedAt
		job.StartedAt = &started
	}
	if job.FinishedAt != nil {
		finished := *job.FinishedAt
		job.FinishedAt = &finished
	}
	if job.Result != nil {
		result := *job.Result
		result.EvidenceIDs = append([]string(nil), result.EvidenceIDs...)
		job.Result = &result
	}
	if job.Error != nil {
		failure := *job.Error
		job.Error = &failure
	}
	return job
}
