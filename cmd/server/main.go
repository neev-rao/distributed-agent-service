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

	"github.com/neev-rao/distributed-agent-service/internal/appconfig"
	"github.com/neev-rao/distributed-agent-service/internal/durable"
	"github.com/neev-rao/distributed-agent-service/internal/fixture"
	"github.com/neev-rao/distributed-agent-service/internal/health"
	"github.com/neev-rao/distributed-agent-service/internal/httpapi"
	"github.com/neev-rao/distributed-agent-service/internal/jobs"
)

func main() {
	os.Exit(runMain())
}

func runMain() int {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	listen := flag.String("listen", "127.0.0.1:8080", "HTTP listen address (local development; unauthenticated)")
	local := flag.Bool("local", false, "explicitly use volatile in-process workers instead of PostgreSQL")
	databaseURL := flag.String("database-url", os.Getenv("DATABASE_URL"), "PostgreSQL URL (prefer DATABASE_URL environment variable)")
	durableConfig := durable.DefaultConfig()
	appconfig.Register(flag.CommandLine, &durableConfig)
	execution := appconfig.RegisterExecution(flag.CommandLine)
	shutdownGrace := flag.Duration("shutdown-grace", 10*time.Second, "shared deadline for draining jobs and HTTP connections")
	flag.Parse()
	if err := execution.Configure(flag.CommandLine, &durableConfig); err != nil {
		logger.Error(err.Error())
		return 1
	}
	if *local && execution.Kind != "fixture" {
		logger.Error("local mode supports only the fixture executor; use durable workers for Gemini")
		return 1
	}
	if durableConfig.ExecutionTimeout <= 0 || *shutdownGrace <= 0 {
		logger.Error("execution-timeout and shutdown-grace must be positive")
		return 1
	}
	if !*local && *databaseURL == "" {
		logger.Error("DATABASE_URL is required for durable mode; use -local explicitly for volatile execution")
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if !*local {
		startupCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		store, err := durable.Open(startupCtx, *databaseURL, durableConfig)
		if err != nil {
			logger.Error("could not connect to job storage")
			return 1
		}
		defer store.Close()
		if err := store.Migrate(startupCtx); err != nil {
			logger.Error("job storage migration or configuration verification failed")
			return 1
		}
		listener, err := net.Listen("tcp", *listen)
		if err != nil {
			logger.Error("could not listen", "error", err)
			return 1
		}
		defer listener.Close()
		logger.Info("service_listening", "address", listener.Addr().String(), "mode", "durable", "execution_profile", durableConfig.ExecutionProfile)
		status := health.New(ctx, store.Ping)
		status.MarkReady()
		defer status.Drain()
		if err := serve(ctx, listener, status.Wrap(httpapi.NewWithBackend(store)), logger, *shutdownGrace, func(context.Context) error { return nil }); err != nil {
			logger.Error("service stopped with error", "error", err)
			return 1
		}
		logger.Info("service_stopped")
		return 0
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
	config.ExecutionTimeout = durableConfig.ExecutionTimeout
	config.QueueCapacity = durableConfig.QueueCapacity
	config.MaxRecords = durableConfig.MaxRecords
	config.MaxResultBytes = durableConfig.MaxResultBytes
	config.Logger = logger
	manager, err := jobs.New(config, fixture.Executor{})
	if err != nil {
		logger.Error("could not create job manager", "error", err)
		return 1
	}
	logger.Info("service_listening", "address", listener.Addr().String(), "mode", "local", "workers", config.Workers, "queue_capacity", config.QueueCapacity, "max_records", config.MaxRecords)
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
	status := health.New(ctx, nil)
	status.MarkReady()
	defer status.Drain()
	return serve(ctx, listener, status.Wrap(httpapi.New(manager)), logger, grace, manager.Shutdown)
}

func serve(ctx context.Context, listener net.Listener, handler http.Handler, logger *slog.Logger, grace time.Duration, drain func(context.Context) error) error {
	server := &http.Server{
		Handler:           handler,
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
	go func() { jobDone <- drain(shutdownCtx) }()
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
