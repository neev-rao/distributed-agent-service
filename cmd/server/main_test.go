package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/neev-rao/distributed-agent-service/internal/fixture"
	"github.com/neev-rao/distributed-agent-service/internal/jobs"
)

func lifecycleManager(t *testing.T, executor jobs.Executor) (*jobs.Manager, *slog.Logger) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	config := jobs.DefaultConfig()
	config.Logger = logger
	manager, err := jobs.New(config, executor)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = manager.Shutdown(ctx)
	})
	return manager, logger
}

func localListener(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}

func TestRunWaitsForAcceptedJobDuringShutdown(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	manager, logger := lifecycleManager(t, jobs.ExecutorFunc(func(ctx context.Context, _ string) (jobs.Result, error) {
		close(started)
		select {
		case <-release:
			return jobs.Result{Diagnosis: "drained"}, nil
		case <-ctx.Done():
			return jobs.Result{}, ctx.Err()
		}
	}))
	job, err := manager.Submit(fixture.ScenarioID)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("executor did not start")
	}
	listener := localListener(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- run(ctx, listener, manager, logger, time.Second) }()
	cancel()
	// Observe the listener closing to establish that shutdown has begun, while
	// the executor remains blocked. The service must still wait for that job.
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	deadline := time.After(time.Second)
	for {
		connection, err := net.DialTimeout("tcp", listener.Addr().String(), 100*time.Millisecond)
		if err != nil {
			break
		}
		connection.Close()
		select {
		case <-deadline:
			t.Fatal("HTTP listener did not stop")
		case <-ticker.C:
		}
	}
	select {
	case err := <-done:
		t.Fatalf("run returned before accepted job finished: %v", err)
	default:
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("service did not stop after draining")
	}
	finished, _ := manager.Get(job.ID)
	if finished.State != jobs.Succeeded || finished.Result == nil || finished.Result.Diagnosis != "drained" {
		t.Fatalf("accepted job was not drained: %+v", finished)
	}
}

func TestRunEnforcesShutdownDeadline(t *testing.T) {
	started, cancelled := make(chan struct{}), make(chan struct{})
	manager, logger := lifecycleManager(t, jobs.ExecutorFunc(func(ctx context.Context, _ string) (jobs.Result, error) {
		close(started)
		<-ctx.Done()
		close(cancelled)
		return jobs.Result{}, ctx.Err()
	}))
	job, err := manager.Submit(fixture.ScenarioID)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("executor did not start")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	listener := localListener(t)
	done := make(chan error, 1)
	go func() { done <- run(ctx, listener, manager, logger, 30*time.Millisecond) }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("got %v, want shutdown deadline", err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown exceeded its bounded grace")
	}
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("executor was not cancelled")
	}
	finished, _ := manager.Get(job.ID)
	if finished.State != jobs.Failed || finished.Error == nil || finished.Error.Code != "service_shutdown" {
		t.Fatalf("unfinished job not failed on shutdown: %+v", finished)
	}
}

type failedListener struct{}

var errAccept = errors.New("test listener failure")

func (failedListener) Accept() (net.Conn, error) { return nil, errAccept }
func (failedListener) Close() error              { return nil }
func (failedListener) Addr() net.Addr            { return &net.TCPAddr{} }

func TestRunStopsManagerAfterServerFailure(t *testing.T) {
	manager, logger := lifecycleManager(t, fixture.Executor{})
	if err := run(context.Background(), failedListener{}, manager, logger, time.Second); !errors.Is(err, errAccept) {
		t.Fatalf("got %v, want listener failure", err)
	}
	if _, err := manager.Submit(fixture.ScenarioID); !errors.Is(err, jobs.ErrShuttingDown) {
		t.Fatalf("manager still accepts jobs after server failure: %v", err)
	}
}
