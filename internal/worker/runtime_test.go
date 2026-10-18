package worker

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/neev-rao/distributed-agent-service/internal/durable"
	"github.com/neev-rao/distributed-agent-service/internal/jobs"
)

type completion struct {
	attempt durable.Attempt
	result  jobs.Result
	failure *jobs.JobError
}

type memoryStore struct {
	mu          sync.Mutex
	queue       []durable.Attempt
	finished    map[string]bool
	claimed     chan durable.Attempt
	completed   chan completion
	renewResult bool
	renewErr    error
	claimErr    error
}

func newMemoryStore(attempts ...durable.Attempt) *memoryStore {
	return &memoryStore{queue: attempts, finished: make(map[string]bool), claimed: make(chan durable.Attempt, 20), completed: make(chan completion, 20), renewResult: true}
}

func (s *memoryStore) Claim(_ context.Context, owner string) (durable.Attempt, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.claimErr != nil {
		return durable.Attempt{}, false, s.claimErr
	}
	if len(s.queue) == 0 {
		return durable.Attempt{}, false, nil
	}
	attempt := s.queue[0]
	s.queue = s.queue[1:]
	attempt.WorkerID = owner
	s.claimed <- attempt
	return attempt, true, nil
}

func (s *memoryStore) Renew(context.Context, durable.Attempt) (bool, error) {
	return s.renewResult, s.renewErr
}

func (s *memoryStore) Complete(_ context.Context, attempt durable.Attempt, result jobs.Result, failure *jobs.JobError) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.finished[attempt.Token] {
		return false, nil
	}
	s.finished[attempt.Token] = true
	s.completed <- completion{attempt: attempt, result: result, failure: failure}
	return true, nil
}

func (*memoryStore) RecoverExpired(context.Context) (int64, error) { return 0, nil }
func (*memoryStore) Cleanup(context.Context) (int64, error)        { return 0, nil }

func testConfig() Config {
	config := DefaultConfig()
	config.WorkerID = "test-worker"
	config.PollInterval = time.Millisecond
	config.RenewInterval = 5 * time.Millisecond
	config.StoreTimeout = time.Second
	config.MaintenanceInterval = time.Millisecond
	config.ShutdownGrace = time.Second
	return config
}

func attempt(id string, timeout time.Duration) durable.Attempt {
	return durable.Attempt{Job: jobs.Job{ID: id, ScenarioID: id, State: jobs.Running}, Token: id, ExecutionTimeout: timeout}
}

func start(t *testing.T, config Config, store Store, executor jobs.Executor) (context.CancelFunc, <-chan error) {
	t.Helper()
	runtime, err := New(config, store, executor)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	return cancel, done
}

func receive[T any](t *testing.T, channel <-chan T) T {
	t.Helper()
	select {
	case value := <-channel:
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for event")
		var zero T
		return zero
	}
}

func TestBoundedExecution(t *testing.T) {
	store := newMemoryStore(attempt("1", time.Second), attempt("2", time.Second), attempt("3", time.Second))
	started := make(chan struct{}, 3)
	release := make(chan struct{})
	var active, maximum atomic.Int32
	executor := jobs.ExecutorFunc(func(ctx context.Context, _ string) (jobs.Result, error) {
		current := active.Add(1)
		for old := maximum.Load(); old < current && !maximum.CompareAndSwap(old, current); old = maximum.Load() {
		}
		defer active.Add(-1)
		started <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
		}
		return jobs.Result{}, nil
	})
	cancel, done := start(t, testConfig(), store, executor)
	receive(t, started)
	receive(t, started)
	select {
	case <-started:
		t.Fatal("third execution started without a free slot")
	case <-time.After(30 * time.Millisecond):
	}
	cancel()
	close(release)
	if err := receive(t, done); err != nil {
		t.Fatal(err)
	}
	if maximum.Load() != 2 {
		t.Fatalf("maximum concurrency = %d", maximum.Load())
	}
}

func TestTimeoutPublishesFailureButRetainsExecutionSlot(t *testing.T) {
	store := newMemoryStore(attempt("slow", 30*time.Millisecond), attempt("next", time.Second))
	config := testConfig()
	config.Concurrency = 1
	release := make(chan struct{})
	started := make(chan string, 2)
	executor := jobs.ExecutorFunc(func(_ context.Context, scenario string) (jobs.Result, error) {
		started <- scenario
		if scenario == "slow" {
			<-release // Deliberately ignore cancellation.
		}
		return jobs.Result{Diagnosis: "late success"}, nil
	})
	cancel, done := start(t, config, store, executor)
	if got := receive(t, started); got != "slow" {
		t.Fatal(got)
	}
	outcome := receive(t, store.completed)
	if outcome.failure == nil || outcome.failure.Code != "execution_timeout" {
		t.Fatalf("timeout outcome = %+v", outcome)
	}
	select {
	case <-started:
		t.Fatal("timeout freed a slot while executor was still running")
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	if got := receive(t, started); got != "next" {
		t.Fatal(got)
	}
	outcome = receive(t, store.completed)
	if outcome.attempt.Job.ID != "next" || outcome.failure != nil {
		t.Fatalf("late result overwrote timeout or next job failed: %+v", outcome)
	}
	cancel()
	if err := receive(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestLeaseLossCancelsWithoutCompleting(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "ownership changed"},
		{name: "database unavailable", err: errors.New("database unavailable")},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newMemoryStore(attempt("job", time.Second))
			store.renewResult, store.renewErr = false, test.err
			canceled := make(chan struct{})
			executor := jobs.ExecutorFunc(func(ctx context.Context, _ string) (jobs.Result, error) {
				<-ctx.Done()
				close(canceled)
				return jobs.Result{}, ctx.Err()
			})
			cancel, done := start(t, testConfig(), store, executor)
			receive(t, canceled)
			cancel()
			if err := receive(t, done); err != nil {
				t.Fatal(err)
			}
			select {
			case <-store.completed:
				t.Fatal("worker completed an attempt after losing ownership")
			default:
			}
		})
	}
}

func TestShutdownDrainsActiveAttemptWithoutClaimingMore(t *testing.T) {
	store := newMemoryStore(attempt("active", time.Second), attempt("queued", time.Second))
	config := testConfig()
	config.Concurrency = 1
	started := make(chan struct{})
	release := make(chan struct{})
	executor := jobs.ExecutorFunc(func(ctx context.Context, _ string) (jobs.Result, error) {
		close(started)
		select {
		case <-release:
			return jobs.Result{Diagnosis: "completed during drain"}, nil
		case <-ctx.Done():
			return jobs.Result{}, ctx.Err()
		}
	})
	cancel, done := start(t, config, store, executor)
	receive(t, started)
	cancel()
	select {
	case <-done:
		t.Fatal("worker did not drain the active attempt")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := receive(t, done); err != nil {
		t.Fatal(err)
	}
	outcome := receive(t, store.completed)
	if outcome.failure != nil {
		t.Fatalf("drained execution failed: %+v", outcome.failure)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.queue) != 1 {
		t.Fatal("shutdown claimed pending work")
	}
}

func TestShutdownCutoffAbandonsAttempt(t *testing.T) {
	store := newMemoryStore(attempt("active", time.Second))
	config := testConfig()
	config.ShutdownGrace = 20 * time.Millisecond
	started, canceled := make(chan struct{}), make(chan struct{})
	executor := jobs.ExecutorFunc(func(ctx context.Context, _ string) (jobs.Result, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		return jobs.Result{}, ctx.Err()
	})
	cancel, done := start(t, config, store, executor)
	receive(t, started)
	cancel()
	if err := receive(t, done); !errors.Is(err, ErrDrainTimeout) {
		t.Fatalf("shutdown error = %v", err)
	}
	receive(t, canceled)
	select {
	case <-store.completed:
		t.Fatal("shutdown terminalized an abandoned attempt")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestExecutionOutcomes(t *testing.T) {
	for _, test := range []struct {
		name    string
		result  jobs.Result
		err     error
		failure string
	}{
		{name: "success", result: jobs.Result{Diagnosis: "evidence-backed answer"}},
		{name: "executor error", err: errors.New("failed"), failure: "execution_failed"},
		{name: "result size", result: jobs.Result{Diagnosis: string(make([]byte, 200))}, failure: "result_too_large"},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newMemoryStore(attempt("job", time.Second))
			config := testConfig()
			config.MaxResultBytes = 100
			executor := jobs.ExecutorFunc(func(context.Context, string) (jobs.Result, error) { return test.result, test.err })
			cancel, done := start(t, config, store, executor)
			outcome := receive(t, store.completed)
			if test.failure == "" && outcome.failure != nil || test.failure != "" && (outcome.failure == nil || outcome.failure.Code != test.failure) {
				t.Fatalf("outcome = %+v; wanted failure %q", outcome, test.failure)
			}
			cancel()
			if err := receive(t, done); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDatabaseFailureDoesNotExecute(t *testing.T) {
	store := newMemoryStore(attempt("job", time.Second))
	store.claimErr = errors.New("database unavailable")
	var called atomic.Bool
	executor := jobs.ExecutorFunc(func(context.Context, string) (jobs.Result, error) {
		called.Store(true)
		return jobs.Result{}, nil
	})
	cancel, done := start(t, testConfig(), store, executor)
	time.Sleep(20 * time.Millisecond)
	cancel()
	if err := receive(t, done); err != nil {
		t.Fatal(err)
	}
	if called.Load() {
		t.Fatal("executor ran without a confirmed database claim")
	}
}

func TestExpiredClaimDoesNotStartExecutor(t *testing.T) {
	store := newMemoryStore()
	var called bool
	executor := jobs.ExecutorFunc(func(context.Context, string) (jobs.Result, error) {
		called = true
		return jobs.Result{}, nil
	})
	runtime, err := New(testConfig(), store, executor)
	if err != nil {
		t.Fatal(err)
	}
	runtime.execute(context.Background(), attempt("late-claim", time.Second), time.Now().Add(-time.Second))
	outcome := receive(t, store.completed)
	if called || outcome.failure == nil || outcome.failure.Code != "execution_timeout" {
		t.Fatalf("expired claim executed or did not timeout: called=%v outcome=%+v", called, outcome)
	}
}

func TestWorkerIdentityValidation(t *testing.T) {
	executor := jobs.ExecutorFunc(func(context.Context, string) (jobs.Result, error) { return jobs.Result{}, nil })
	for _, identity := range []string{"", strings.Repeat("w", 129)} {
		config := testConfig()
		config.WorkerID = identity
		if _, err := New(config, newMemoryStore(), executor); err == nil {
			t.Fatalf("accepted worker identity containing %d bytes", len(identity))
		}
	}
	config := testConfig()
	config.WorkerID = strings.Repeat("w", 128)
	if _, err := New(config, newMemoryStore(), executor); err != nil {
		t.Fatalf("rejected maximum-length worker identity: %v", err)
	}
}
