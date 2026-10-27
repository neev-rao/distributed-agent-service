package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/neev-rao/distributed-agent-service/internal/health"
)

func TestWorkerHealthStaysLiveAndUnreadyDuringDrain(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started, draining, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	done := make(chan error, 1)
	s := health.New(ctx, nil)
	go func() {
		done <- runWithHealth(ctx, func(ctx context.Context) error { close(started); <-ctx.Done(); close(draining); <-release; return nil }, listener, s, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("worker not started")
	}
	client := &http.Client{Timeout: time.Second}
	get := func(path string, want int) {
		t.Helper()
		r, e := client.Get("http://" + listener.Addr().String() + path)
		if e != nil {
			t.Fatal(e)
		}
		r.Body.Close()
		if r.StatusCode != want {
			t.Fatalf("%s got%d want%d", path, r.StatusCode, want)
		}
	}
	get("/readyz", 200)
	cancel()
	select {
	case <-draining:
	case <-time.After(time.Second):
		t.Fatal("runtime did not drain")
	}
	get("/readyz", 503)
	get("/livez", 200)
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker failed to exit")
	}
}

type failedHealthListener struct{}

var errHealthAccept = errors.New("health accept failed")

func (failedHealthListener) Accept() (net.Conn, error) { return nil, errHealthAccept }
func (failedHealthListener) Close() error              { return nil }
func (failedHealthListener) Addr() net.Addr            { return &net.TCPAddr{} }

func TestHealthListenerFailureStopsRuntime(t *testing.T) {
	s := health.New(context.Background(), nil)
	stopped := make(chan struct{})
	err := runWithHealth(context.Background(), func(ctx context.Context) error { <-ctx.Done(); close(stopped); return nil }, failedHealthListener{}, s, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if !errors.Is(err, errHealthAccept) {
		t.Fatalf("listener failure lost: %v", err)
	}
	select {
	case <-stopped:
	default:
		t.Fatal("runtime left running")
	}
}

func TestWorkerHealthListenerIsOptional(t *testing.T) {
	want := errors.New("runtime result")
	err := runWithHealth(context.Background(), func(context.Context) error { return want }, nil, nil, nil)
	if err != want {
		t.Fatal("optional health changed runtime behavior")
	}
}
