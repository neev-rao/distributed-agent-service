package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/neev-rao/distributed-agent-service/internal/fixture"
	"github.com/neev-rao/distributed-agent-service/internal/httpapi"
	"github.com/neev-rao/distributed-agent-service/internal/jobs"
)

func main() {
	os.Exit(runMain())
}

func runMain() int {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	listen := flag.String("listen", "127.0.0.1:8080", "HTTP listen address (local development; unauthenticated)")
	executionTimeout := flag.Duration("execution-timeout", 5*time.Second, "maximum execution time, excluding queue wait")
	shutdownGrace := flag.Duration("shutdown-grace", 10*time.Second, "shared deadline for draining jobs and HTTP connections")
	flag.Parse()
	if *executionTimeout <= 0 || *shutdownGrace <= 0 {
		logger.Error("execution-timeout and shutdown-grace must be positive")
		return 1
	}
	// Bind before creating workers: a bind failure must not leave a service that
	// accepts or executes work without a reachable HTTP listener.
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		logger.Error("could not listen", "error", err)
		return 1
	}
	defer listener.Close()
	config := jobs.DefaultConfig()
	config.ExecutionTimeout = *executionTimeout
	config.Logger = logger
	manager, err := jobs.New(config, fixture.Executor{})
	if err != nil {
		logger.Error("could not create job manager", "error", err)
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logger.Info("service_listening", "address", listener.Addr().String(), "workers", config.Workers, "queue_capacity", config.QueueCapacity, "max_records", config.MaxRecords)
	if err := run(ctx, listener, manager, logger, *shutdownGrace); err != nil {
		logger.Error("service stopped with error", "error", err)
		return 1
	}
	logger.Info("service_stopped")
	return 0
}

// run owns both lifecycles. HTTP shutdown stops new connections while manager
// shutdown stops admission and drains accepted jobs. Both share one grace period.
func run(ctx context.Context, listener net.Listener, manager *jobs.Manager, logger *slog.Logger, grace time.Duration) error {
	server := &http.Server{
		Handler:           httpapi.New(manager),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	var serveErr error
	serveReturned := false
	select {
	case serveErr = <-serveDone:
		serveReturned = true
	case <-ctx.Done():
	}
	logger.Info("service_shutdown_started", "grace", grace.String())
	shutdownCtx, cancel := context.WithTimeout(context.Background(), grace)
	defer cancel()
	jobDone := make(chan error, 1)
	httpDone := make(chan error, 1)
	go func() { jobDone <- manager.Shutdown(shutdownCtx) }()
	go func() { httpDone <- server.Shutdown(shutdownCtx) }()
	jobErr := <-jobDone
	httpErr := <-httpDone
	if httpErr != nil {
		// Shutdown does not forcibly close active connections when its deadline
		// expires; close them before run returns and the process exits.
		_ = server.Close()
	}
	if !serveReturned {
		serveErr = <-serveDone
	}
	if errors.Is(serveErr, http.ErrServerClosed) {
		serveErr = nil
	}
	if jobErr != nil {
		jobErr = fmt.Errorf("drain jobs: %w", jobErr)
	}
	if httpErr != nil {
		httpErr = fmt.Errorf("drain HTTP connections: %w", httpErr)
	}
	return errors.Join(serveErr, jobErr, httpErr)
}
