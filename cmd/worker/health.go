package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/neev-rao/distributed-agent-service/internal/health"
)

// Health remains available during worker drain. If its listener fails, stop
// claims and drain the runtime rather than leave an unobservable worker running.
func runWithHealth(ctx context.Context, run func(context.Context) error, listener net.Listener, status *health.State, logger *slog.Logger) error {
	if listener == nil {
		return run(ctx)
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	server := &http.Server{Handler: status.Wrap(http.NotFoundHandler()),
		ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 3 * time.Second,
		WriteTimeout: 3 * time.Second, IdleTimeout: 5 * time.Second, MaxHeaderBytes: 8 << 10,
		ErrorLog: slog.NewLogLogger(logger.Handler(), slog.LevelError)}
	healthDone := make(chan error, 1)
	go func() { healthDone <- server.Serve(listener) }()
	defer server.Close()
	defer status.Drain()
	status.MarkReady()
	runtimeDone := make(chan error, 1)
	go func() { runtimeDone <- run(runCtx) }()
	select {
	case err := <-runtimeDone:
		return err
	case err := <-healthDone:
		status.Drain()
		cancel()
		if errors.Is(err, http.ErrServerClosed) {
			err = errors.New("worker health listener stopped unexpectedly")
		}
		return errors.Join(err, <-runtimeDone)
	}
}
