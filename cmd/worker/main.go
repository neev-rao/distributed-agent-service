package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/neev-rao/distributed-agent-service/internal/appconfig"
	"github.com/neev-rao/distributed-agent-service/internal/durable"
	"github.com/neev-rao/distributed-agent-service/internal/fixture"
	"github.com/neev-rao/distributed-agent-service/internal/jobs"
	"github.com/neev-rao/distributed-agent-service/internal/worker"
)

func main() { os.Exit(runMain()) }

func runMain() int {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	storeConfig := durable.DefaultConfig()
	appconfig.Register(flag.CommandLine, &storeConfig)
	workerID := flag.String("worker-id", "", "worker process identity (random by default)")
	shutdownGrace := flag.Duration("shutdown-grace", 10*time.Second, "time to drain active attempts before abandoning their leases")
	fixtureDelay := flag.Duration("fixture-delay", 0, "deterministic delay before fixture execution, for recovery demonstrations")
	flag.Parse()
	if *shutdownGrace <= 0 || *fixtureDelay < 0 {
		logger.Error("shutdown-grace must be positive and fixture-delay must be nonnegative")
		return 1
	}
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		logger.Error("DATABASE_URL is required")
		return 1
	}
	if *workerID == "" {
		var token [16]byte
		if _, err := rand.Read(token[:]); err != nil {
			logger.Error("could not generate worker identity")
			return 1
		}
		*workerID = hex.EncodeToString(token[:])
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	startupCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	store, err := durable.Open(startupCtx, dsn, storeConfig)
	if err != nil {
		cancel()
		logger.Error("database connection failed; verify DATABASE_URL and database availability")
		return 1
	}
	defer store.Close()
	err = store.Migrate(startupCtx)
	cancel()
	if err != nil {
		logger.Error("database migration or shared configuration validation failed")
		return 1
	}
	config := worker.DefaultConfig()
	config.WorkerID, config.ShutdownGrace = *workerID, *shutdownGrace
	config.Logger, config.MaxResultBytes = logger, storeConfig.MaxResultBytes
	// Derive heartbeat timing from the shared lease so demonstrations with short
	// leases preserve the same renewal margin as the default configuration.
	config.RenewInterval = storeConfig.LeaseDuration / 3
	config.StoreTimeout = min(time.Second, storeConfig.LeaseDuration/4)
	runtime, err := worker.New(config, store, delayedFixture{delay: *fixtureDelay})
	if err != nil {
		logger.Error("invalid worker configuration")
		return 1
	}
	logger.Info("worker_ready", "worker_id", *workerID, "concurrency", config.Concurrency,
		"lease_duration", storeConfig.LeaseDuration.String(), "execution_timeout", storeConfig.ExecutionTimeout.String())
	if err := runtime.Run(ctx); err != nil && !errors.Is(err, worker.ErrDrainTimeout) {
		logger.Error("worker stopped with error")
		return 1
	}
	logger.Info("worker_stopped", "worker_id", *workerID)
	return 0
}

type delayedFixture struct{ delay time.Duration }

func (f delayedFixture) Execute(ctx context.Context, scenario string) (jobs.Result, error) {
	if f.delay > 0 {
		timer := time.NewTimer(f.delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return jobs.Result{}, ctx.Err()
		case <-timer.C:
		}
	}
	return (fixture.Executor{}).Execute(ctx, scenario)
}
