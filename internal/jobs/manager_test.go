package jobs_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/neev-rao/distributed-agent-service/internal/jobs"
)

func testConfig() jobs.Config {
	cfg := jobs.DefaultConfig()
	cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	return cfg
}

func newManager(t *testing.T, cfg jobs.Config, executor jobs.Executor) *jobs.Manager {
	t.Helper()
	manager, err := jobs.New(cfg, executor)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := manager.Shutdown(ctx); err != nil {
			t.Errorf("cleanup Shutdown: %v", err)
		}
	})
	return manager
}

func submit(t *testing.T, manager *jobs.Manager, scenario string) jobs.Job {
	t.Helper()
	job, err := manager.Submit(scenario)
	if err != nil {
		t.Fatalf("Submit(%q): %v", scenario, err)
	}
	if job.ID == "" {
		t.Fatal("accepted job has an empty ID")
	}
	return job
}

func awaitJob(t *testing.T, manager *jobs.Manager, id string, state jobs.State) jobs.Job {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		job, ok := manager.Get(id)
		if !ok {
			t.Fatalf("accepted job %q is missing", id)
		}
		if job.State == state {
			return job
		}
		if job.State == jobs.Succeeded || job.State == jobs.Failed {
			t.Fatalf("job %s reached %s, wanted %s; error=%+v", id, job.State, state, job.Error)
		}
		select {
		case <-deadline.C:
			t.Fatalf("job %s remained %s, wanted %s", id, job.State, state)
		case <-tick.C:
		}
	}
}

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for executor synchronization")
		var zero T
		return zero
	}
}

func TestDefaultConfigAndValidation(t *testing.T) {
	cfg := jobs.DefaultConfig()
	if cfg.Workers != 2 || cfg.QueueCapacity != 8 || cfg.MaxRecords != 1000 ||
		cfg.ExecutionTimeout != 5*time.Second || cfg.MaxResultBytes != 64*1024 {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	executor := jobs.ExecutorFunc(func(context.Context, string) (jobs.Result, error) {
		return jobs.Result{}, nil
	})
	for _, tt := range []struct {
		name   string
		change func(*jobs.Config)
	}{
		{"zero workers", func(c *jobs.Config) { c.Workers = 0 }},
		{"negative workers", func(c *jobs.Config) { c.Workers = -1 }},
		{"zero queue", func(c *jobs.Config) { c.QueueCapacity = 0 }},
		{"negative queue", func(c *jobs.Config) { c.QueueCapacity = -1 }},
		{"zero records", func(c *jobs.Config) { c.MaxRecords = 0 }},
		{"negative records", func(c *jobs.Config) { c.MaxRecords = -1 }},
		{"zero timeout", func(c *jobs.Config) { c.ExecutionTimeout = 0 }},
		{"negative timeout", func(c *jobs.Config) { c.ExecutionTimeout = -time.Second }},
		{"zero result bound", func(c *jobs.Config) { c.MaxResultBytes = 0 }},
		{"negative result bound", func(c *jobs.Config) { c.MaxResultBytes = -1 }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			invalid := testConfig()
			tt.change(&invalid)
			if manager, err := jobs.New(invalid, executor); err == nil {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				_ = manager.Shutdown(ctx)
				t.Fatal("New accepted invalid configuration")
			}
		})
	}
	if _, err := jobs.New(testConfig(), nil); err == nil {
		t.Fatal("New accepted a nil executor")
	}
}

func TestLifecycleAndUnknownID(t *testing.T) {
	cfg := testConfig()
	cfg.Workers = 1
	started := make(chan string, 2)
	release := make(chan struct{})
	defer close(release)
	manager := newManager(t, cfg, jobs.ExecutorFunc(func(ctx context.Context, scenario string) (jobs.Result, error) {
		started <- scenario
		select {
		case <-release:
		case <-ctx.Done():
			return jobs.Result{}, ctx.Err()
		}
		return jobs.Result{Diagnosis: "database timeout", EvidenceIDs: []string{"metric-db", "log-checkout"}}, nil
	}))
	if _, ok := manager.Get("not-a-job"); ok {
		t.Fatal("unknown ID was found")
	}
	first := submit(t, manager, "first")
	if receive(t, started) != "first" {
		t.Fatal("executor received the wrong scenario")
	}
	running, ok := manager.Get(first.ID)
	if !ok || running.State != jobs.Running || running.StartedAt == nil || running.FinishedAt != nil {
		t.Fatalf("invalid running representation: %+v", running)
	}
	second := submit(t, manager, "second")
	if second.ID == first.ID || second.ScenarioID != "second" || second.CreatedAt.IsZero() {
		t.Fatalf("invalid accepted job: %+v", second)
	}
	queued, _ := manager.Get(second.ID)
	if queued.State != jobs.Queued || queued.StartedAt != nil || queued.FinishedAt != nil {
		t.Fatalf("invalid queued representation: %+v", queued)
	}
	// Release both executions without closing a channel twice in cleanup.
	release <- struct{}{}
	finished := awaitJob(t, manager, first.ID, jobs.Succeeded)
	if finished.Result == nil || finished.Result.Diagnosis != "database timeout" || len(finished.Result.EvidenceIDs) != 2 || finished.Error != nil {
		t.Fatalf("invalid successful result: %+v", finished)
	}
	if finished.StartedAt == nil || finished.FinishedAt == nil || finished.StartedAt.Before(finished.CreatedAt) || finished.FinishedAt.Before(*finished.StartedAt) {
		t.Fatalf("invalid lifecycle timestamps: %+v", finished)
	}
	if receive(t, started) != "second" {
		t.Fatal("queued scenario was not executed")
	}
	release <- struct{}{}
	awaitJob(t, manager, second.ID, jobs.Succeeded)
}

func TestConcurrentAdmissionAndWorkerBound(t *testing.T) {
	cfg := testConfig()
	started := make(chan string, 64)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	var active atomic.Int32
	var peak atomic.Int32
	var executed atomic.Int32
	manager := newManager(t, cfg, jobs.ExecutorFunc(func(ctx context.Context, scenario string) (jobs.Result, error) {
		current := active.Add(1)
		defer active.Add(-1)
		for previous := peak.Load(); current > previous; previous = peak.Load() {
			if peak.CompareAndSwap(previous, current) {
				break
			}
		}
		executed.Add(1)
		started <- scenario
		select {
		case <-release:
			return jobs.Result{Diagnosis: scenario}, nil
		case <-ctx.Done():
			return jobs.Result{}, ctx.Err()
		}
	}))
	accepted := []jobs.Job{submit(t, manager, "active-1")}
	receive(t, started)
	accepted = append(accepted, submit(t, manager, "active-2"))
	receive(t, started)
	const submissions = 48
	type outcome struct {
		job jobs.Job
		err error
	}
	results := make(chan outcome, submissions)
	var submitters sync.WaitGroup
	for range submissions {
		submitters.Add(1)
		go func() {
			defer submitters.Done()
			job, err := manager.Submit("waiting")
			results <- outcome{job, err}
		}()
	}
	submitters.Wait()
	close(results)
	rejected := 0
	ids := map[string]bool{accepted[0].ID: true, accepted[1].ID: true}
	for result := range results {
		if errors.Is(result.err, jobs.ErrQueueFull) {
			rejected++
			continue
		}
		if result.err != nil {
			t.Fatalf("unexpected admission error: %v", result.err)
		}
		if result.job.ID == "" || ids[result.job.ID] {
			t.Fatalf("empty or duplicate accepted ID %q", result.job.ID)
		}
		ids[result.job.ID] = true
		accepted = append(accepted, result.job)
		queued, _ := manager.Get(result.job.ID)
		if queued.State != jobs.Queued {
			t.Fatalf("waiting job state = %s", queued.State)
		}
	}
	if len(accepted) != cfg.Workers+cfg.QueueCapacity || rejected != submissions-cfg.QueueCapacity {
		t.Fatalf("accepted=%d rejected=%d; wanted %d and %d", len(accepted), rejected, cfg.Workers+cfg.QueueCapacity, submissions-cfg.QueueCapacity)
	}
	if active.Load() != int32(cfg.Workers) || executed.Load() != int32(cfg.Workers) {
		t.Fatalf("queue executed while workers occupied: active=%d executed=%d", active.Load(), executed.Load())
	}
	releaseOnce.Do(func() { close(release) })
	for _, job := range accepted {
		awaitJob(t, manager, job.ID, jobs.Succeeded)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := manager.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if peak.Load() != int32(cfg.Workers) || executed.Load() != int32(len(accepted)) {
		t.Fatalf("peak=%d executed=%d; worker limit=%d accepted=%d", peak.Load(), executed.Load(), cfg.Workers, len(accepted))
	}
}

func TestRejectedJobLeavesNoRecordOrExecutableWork(t *testing.T) {
	cfg := testConfig()
	cfg.Workers, cfg.QueueCapacity, cfg.MaxRecords = 1, 1, 3
	started := make(chan string, 4)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	manager := newManager(t, cfg, jobs.ExecutorFunc(func(ctx context.Context, scenario string) (jobs.Result, error) {
		started <- scenario
		select {
		case <-release:
			return jobs.Result{Diagnosis: scenario}, nil
		case <-ctx.Done():
			return jobs.Result{}, ctx.Err()
		}
	}))
	first := submit(t, manager, "first")
	receive(t, started)
	second := submit(t, manager, "second")
	if _, err := manager.Submit("rejected"); !errors.Is(err, jobs.ErrQueueFull) {
		t.Fatalf("full queue returned %v", err)
	}
	releaseOnce.Do(func() { close(release) })
	awaitJob(t, manager, first.ID, jobs.Succeeded)
	awaitJob(t, manager, second.ID, jobs.Succeeded)
	if scenario := receive(t, started); scenario != "second" {
		t.Fatalf("executed %q, wanted second", scenario)
	}
	third := submit(t, manager, "third")
	awaitJob(t, manager, third.ID, jobs.Succeeded)
	if scenario := receive(t, started); scenario != "third" {
		t.Fatalf("executed rejected or unexpected scenario %q", scenario)
	}
	if _, err := manager.Submit("over-record-limit"); !errors.Is(err, jobs.ErrRecordLimit) {
		t.Fatalf("retained record limit returned %v", err)
	}
	for _, job := range []jobs.Job{first, second, third} {
		if _, ok := manager.Get(job.ID); !ok {
			t.Errorf("record limit evicted existing job %s", job.ID)
		}
	}
}

func TestExecutorFailureDoesNotStopWorker(t *testing.T) {
	cfg := testConfig()
	cfg.Workers = 1
	manager := newManager(t, cfg, jobs.ExecutorFunc(func(_ context.Context, scenario string) (jobs.Result, error) {
		if scenario == "fail" {
			return jobs.Result{}, errors.New("fixture dependency failed")
		}
		return jobs.Result{Diagnosis: "recovered"}, nil
	}))
	failed := submit(t, manager, "fail")
	job := awaitJob(t, manager, failed.ID, jobs.Failed)
	if job.Error == nil || job.Error.Code != "execution_failed" || job.Error.Message == "" || job.Result != nil || job.FinishedAt == nil {
		t.Fatalf("invalid executor failure: %+v", job)
	}
	recovered := submit(t, manager, "success")
	awaitJob(t, manager, recovered.ID, jobs.Succeeded)
}

func TestResultSerializedByteLimit(t *testing.T) {
	result := jobs.Result{Diagnosis: "timeout \"quoted\"", EvidenceIDs: []string{"metric-db"}}
	serialized, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name  string
		limit int
		state jobs.State
	}{
		{"at boundary", len(serialized), jobs.Succeeded},
		{"one byte too large", len(serialized) - 1, jobs.Failed},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig()
			cfg.MaxResultBytes = tt.limit
			manager := newManager(t, cfg, jobs.ExecutorFunc(func(context.Context, string) (jobs.Result, error) {
				return result, nil
			}))
			accepted := submit(t, manager, "size")
			job := awaitJob(t, manager, accepted.ID, tt.state)
			if tt.state == jobs.Failed && (job.Error == nil || job.Error.Code != "result_too_large" || job.Result != nil) {
				t.Fatalf("invalid result size failure: %+v", job)
			}
		})
	}
}

func TestDeadlineStartsAtRunningAndRetainsWorkerUntilExecutorReturns(t *testing.T) {
	cfg := testConfig()
	cfg.Workers = 1
	cfg.ExecutionTimeout = 75 * time.Millisecond
	started := make(chan string, 2)
	timedOut := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	manager := newManager(t, cfg, jobs.ExecutorFunc(func(ctx context.Context, scenario string) (jobs.Result, error) {
		started <- scenario
		if scenario == "slow" {
			<-ctx.Done()
			close(timedOut)
			<-release // Deliberately uncooperative after cancellation.
			return jobs.Result{Diagnosis: "late success"}, nil
		}
		return jobs.Result{Diagnosis: "next job succeeded"}, ctx.Err()
	}))
	slow := submit(t, manager, "slow")
	receive(t, started)
	waiting := submit(t, manager, "waiting")
	receive(t, timedOut)
	failed := awaitJob(t, manager, slow.ID, jobs.Failed)
	if failed.Error == nil || failed.Error.Code != "execution_timeout" || failed.Result != nil {
		t.Fatalf("invalid timeout result: %+v", failed)
	}
	// Keep the canceled executor occupied for a second execution interval: waiting
	// work must neither start nor spend its own execution timeout in the queue.
	timer := time.NewTimer(cfg.ExecutionTimeout)
	defer timer.Stop()
	select {
	case scenario := <-started:
		t.Fatalf("worker slot released before executor returned: %q", scenario)
	case <-timer.C:
	}
	queued, _ := manager.Get(waiting.ID)
	if queued.State != jobs.Queued || queued.StartedAt != nil || queued.Error != nil {
		t.Fatalf("queued job spent execution deadline: %+v", queued)
	}
	releaseOnce.Do(func() { close(release) })
	if receive(t, started) != "waiting" {
		t.Fatal("wrong scenario executed after timed-out executor returned")
	}
	awaitJob(t, manager, waiting.ID, jobs.Succeeded)
	stillFailed, _ := manager.Get(slow.ID)
	if stillFailed.State != jobs.Failed || stillFailed.Error == nil || stillFailed.Error.Code != "execution_timeout" || stillFailed.Result != nil {
		t.Fatalf("late success overwrote timeout: %+v", stillFailed)
	}
}

func TestGetAndExecutorResultsDoNotExposeMutableState(t *testing.T) {
	original := jobs.Result{Diagnosis: "original diagnosis", EvidenceIDs: []string{"original evidence"}}
	manager := newManager(t, testConfig(), jobs.ExecutorFunc(func(_ context.Context, scenario string) (jobs.Result, error) {
		if scenario == "fail" {
			return jobs.Result{}, errors.New("original error")
		}
		return original, nil
	}))
	accepted := submit(t, manager, "success")
	// Scalars from the admission response must not control the retained job.
	id := accepted.ID
	accepted.ID, accepted.ScenarioID, accepted.State = "tampered", "tampered", jobs.Failed
	job := awaitJob(t, manager, id, jobs.Succeeded)
	start, finish := *job.StartedAt, *job.FinishedAt
	job.Result.Diagnosis = "tampered diagnosis"
	job.Result.EvidenceIDs[0] = "tampered evidence"
	*job.StartedAt = time.Time{}
	*job.FinishedAt = time.Time{}
	original.EvidenceIDs[0] = "mutated executor-owned slice"
	stored, _ := manager.Get(id)
	if stored.ID != id || stored.ScenarioID != "success" || stored.State != jobs.Succeeded || stored.Result.Diagnosis != "original diagnosis" || stored.Result.EvidenceIDs[0] != "original evidence" || !stored.StartedAt.Equal(start) || !stored.FinishedAt.Equal(finish) {
		t.Fatalf("caller or executor mutated retained state: %+v", stored)
	}
	failed := submit(t, manager, "fail")
	failure := awaitJob(t, manager, failed.ID, jobs.Failed)
	code, message := failure.Error.Code, failure.Error.Message
	failure.Error.Code, failure.Error.Message = "tampered", "tampered"
	storedFailure, _ := manager.Get(failed.ID)
	if storedFailure.Error.Code != code || storedFailure.Error.Message != message {
		t.Fatalf("caller mutated retained error: %+v", storedFailure.Error)
	}
}

func TestGracefulShutdownStopsAdmissionAndDrainsAcceptedJobs(t *testing.T) {
	cfg := testConfig()
	cfg.Workers, cfg.QueueCapacity = 1, 1
	started := make(chan string, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	manager := newManager(t, cfg, jobs.ExecutorFunc(func(ctx context.Context, scenario string) (jobs.Result, error) {
		started <- scenario
		select {
		case <-release:
			return jobs.Result{Diagnosis: scenario}, nil
		case <-ctx.Done():
			return jobs.Result{}, ctx.Err()
		}
	}))
	first := submit(t, manager, "running")
	receive(t, started)
	second := submit(t, manager, "queued")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	shutdown := make(chan error, 1)
	// A caller may choose an unbounded drain; the test watchdog remains bounded.
	go func() { shutdown <- manager.Shutdown(context.Background()) }()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		_, err := manager.Submit("after-shutdown")
		if errors.Is(err, jobs.ErrShuttingDown) {
			break
		}
		if !errors.Is(err, jobs.ErrQueueFull) {
			t.Fatalf("admission during shutdown returned %v", err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("shutdown did not close admission")
		case <-tick.C:
		}
	}
	select {
	case err := <-shutdown:
		t.Fatalf("shutdown returned before accepted jobs drained: %v", err)
	default:
	}
	releaseOnce.Do(func() { close(release) })
	if err := receive(t, shutdown); err != nil {
		t.Fatalf("graceful shutdown: %v", err)
	}
	for _, job := range []jobs.Job{first, second} {
		stored, _ := manager.Get(job.ID)
		if stored.State != jobs.Succeeded || stored.FinishedAt == nil {
			t.Fatalf("shutdown returned with unfinished job: %+v", stored)
		}
	}
	if _, err := manager.Submit("closed"); !errors.Is(err, jobs.ErrShuttingDown) {
		t.Fatalf("admission after shutdown returned %v", err)
	}
}

func TestShutdownDeadlineFailsUnfinishedJobsAndReturnsBeforeUncooperativeExecutor(t *testing.T) {
	cfg := testConfig()
	cfg.Workers, cfg.QueueCapacity = 1, 2
	cfg.ExecutionTimeout = time.Hour
	started := make(chan string, 3)
	canceled := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	manager := newManager(t, cfg, jobs.ExecutorFunc(func(ctx context.Context, scenario string) (jobs.Result, error) {
		started <- scenario
		<-ctx.Done()
		close(canceled)
		<-release
		return jobs.Result{Diagnosis: "late success"}, nil
	}))
	first := submit(t, manager, "running")
	receive(t, started)
	second := submit(t, manager, "queued-1")
	third := submit(t, manager, "queued-2")
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
	defer cancel()
	shutdown := make(chan error, 1)
	go func() { shutdown <- manager.Shutdown(ctx) }()
	if err := receive(t, shutdown); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("forced Shutdown returned %v", err)
	}
	receive(t, canceled)
	for _, job := range []jobs.Job{first, second, third} {
		stored, _ := manager.Get(job.ID)
		if stored.State != jobs.Failed || stored.Error == nil || stored.Error.Code != "service_shutdown" || stored.FinishedAt == nil || stored.Result != nil {
			t.Fatalf("unfinished job not failed at shutdown return: %+v", stored)
		}
		if job.ID != first.ID && stored.StartedAt != nil {
			t.Fatalf("queued job started during forced shutdown: %+v", stored)
		}
	}
	if _, err := manager.Submit("after-deadline"); !errors.Is(err, jobs.ErrShuttingDown) {
		t.Fatalf("admission after forced shutdown returned %v", err)
	}
	releaseOnce.Do(func() { close(release) })
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer waitCancel()
	if err := manager.Shutdown(waitCtx); err != nil {
		t.Fatalf("waiting for executor termination: %v", err)
	}
	select {
	case scenario := <-started:
		t.Fatalf("queued job started after shutdown deadline: %q", scenario)
	default:
	}
	stored, _ := manager.Get(first.ID)
	if stored.State != jobs.Failed || stored.Error.Code != "service_shutdown" || stored.Result != nil {
		t.Fatalf("late completion overwrote forced shutdown: %+v", stored)
	}
}

func TestCanceledShutdownContextFailsRunningAndQueuedJobs(t *testing.T) {
	cfg := testConfig()
	cfg.Workers, cfg.QueueCapacity = 1, 1
	cfg.ExecutionTimeout = time.Hour
	started := make(chan string, 2)
	manager := newManager(t, cfg, jobs.ExecutorFunc(func(ctx context.Context, scenario string) (jobs.Result, error) {
		started <- scenario
		<-ctx.Done()
		return jobs.Result{}, ctx.Err()
	}))
	running := submit(t, manager, "running")
	receive(t, started)
	queued := submit(t, manager, "queued")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	shutdown := make(chan error, 1)
	go func() { shutdown <- manager.Shutdown(ctx) }()
	if err := receive(t, shutdown); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Shutdown returned %v", err)
	}
	for _, accepted := range []jobs.Job{running, queued} {
		job, _ := manager.Get(accepted.ID)
		if job.State != jobs.Failed || job.Error == nil || job.Error.Code != "service_shutdown" || job.FinishedAt == nil {
			t.Fatalf("canceled shutdown left unfinished work: %+v", job)
		}
		if accepted.ID == queued.ID && job.StartedAt != nil {
			t.Fatalf("queued job started after shutdown cancellation: %+v", job)
		}
	}
	if _, err := manager.Submit("closed"); !errors.Is(err, jobs.ErrShuttingDown) {
		t.Fatalf("canceled shutdown did not close admission: %v", err)
	}
}

func TestCancelActiveDrainWithoutDeadlinePreventsQueuedExecution(t *testing.T) {
	cfg := testConfig()
	cfg.Workers, cfg.QueueCapacity = 1, 1
	cfg.ExecutionTimeout = time.Hour
	started := make(chan string, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	manager := newManager(t, cfg, jobs.ExecutorFunc(func(_ context.Context, scenario string) (jobs.Result, error) {
		started <- scenario
		<-release
		return jobs.Result{Diagnosis: "completed after drain cancellation"}, nil
	}))
	running := submit(t, manager, "running")
	receive(t, started)
	queued := submit(t, manager, "queued")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	shutdown := make(chan error, 1)
	go func() { shutdown <- manager.Shutdown(ctx) }()
	watchdog := time.NewTimer(3 * time.Second)
	defer watchdog.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		_, err := manager.Submit("after-drain-start")
		if errors.Is(err, jobs.ErrShuttingDown) {
			break
		}
		if !errors.Is(err, jobs.ErrQueueFull) {
			t.Fatalf("admission while observing drain returned %v", err)
		}
		select {
		case <-watchdog.C:
			t.Fatal("shutdown did not begin draining")
		case <-tick.C:
		}
	}
	// The running executor can now return, but cancellation must prevent the
	// worker claiming the queued job even before an asynchronous callback runs.
	cancel()
	releaseOnce.Do(func() { close(release) })
	if err := receive(t, shutdown); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled active drain returned %v", err)
	}
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer waitCancel()
	if err := manager.Shutdown(waitCtx); err != nil {
		t.Fatalf("waiting for worker after canceled drain: %v", err)
	}
	for _, accepted := range []jobs.Job{running, queued} {
		job, _ := manager.Get(accepted.ID)
		if job.State != jobs.Failed || job.Error == nil || job.Error.Code != "service_shutdown" || job.Result != nil || job.FinishedAt == nil {
			t.Fatalf("canceled drain did not fail accepted work: %+v", job)
		}
		if accepted.ID == queued.ID && job.StartedAt != nil {
			t.Fatalf("queued job started after active drain cancellation: %+v", job)
		}
	}
	select {
	case scenario := <-started:
		t.Fatalf("executor started queued work after drain cancellation: %q", scenario)
	default:
	}
}
