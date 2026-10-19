// dbtool applies versioned schema changes or prunes expired terminal jobs.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"time"

	"github.com/neev-rao/distributed-agent-service/internal/appconfig"
	"github.com/neev-rao/distributed-agent-service/internal/durable"
)

func main() { os.Exit(run()) }

func run() int {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	action := flag.String("action", "migrate", "migrate or prune (at most 100 expired terminal jobs)")
	cfg := durable.DefaultConfig()
	appconfig.Register(flag.CommandLine, &cfg)
	execution := appconfig.RegisterExecution(flag.CommandLine)
	flag.Parse()
	if err := execution.Configure(flag.CommandLine, &cfg); err != nil {
		logger.Error(err.Error())
		return 1
	}
	if *action != "migrate" && *action != "prune" {
		logger.Error("action must be migrate or prune")
		return 1
	}
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		logger.Error("DATABASE_URL is required")
		return 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	store, err := durable.Open(ctx, dsn, cfg)
	if err != nil {
		logger.Error("could not connect to job storage")
		return 1
	}
	defer store.Close()
	if err := store.Migrate(ctx); err != nil {
		logger.Error("migration or configuration verification failed")
		return 1
	}
	if *action == "prune" {
		count, err := store.Cleanup(ctx)
		if err != nil {
			logger.Error("retention cleanup failed")
			return 1
		}
		logger.Info("retention_cleanup", "removed", count)
	} else {
		logger.Info("schema_ready")
	}
	return 0
}
