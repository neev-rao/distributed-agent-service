// Package appconfig binds the shared, database-enforced execution policy.
package appconfig

import (
	"flag"

	"github.com/neev-rao/distributed-agent-service/internal/durable"
)

func Register(fs *flag.FlagSet, config *durable.Config) {
	fs.IntVar(&config.QueueCapacity, "queue-capacity", config.QueueCapacity, "maximum globally queued jobs")
	fs.IntVar(&config.MaxRecords, "max-records", config.MaxRecords, "maximum retained job records")
	fs.IntVar(&config.MaxAttempts, "max-attempts", config.MaxAttempts, "maximum claims per job, including crash recovery")
	fs.IntVar(&config.MaxResultBytes, "max-result-bytes", config.MaxResultBytes, "maximum serialized retained result bytes")
	fs.DurationVar(&config.ExecutionTimeout, "execution-timeout", config.ExecutionTimeout, "per-attempt execution budget, excluding queue wait")
	fs.DurationVar(&config.LeaseDuration, "lease-duration", config.LeaseDuration, "renewable worker ownership duration")
	fs.DurationVar(&config.RetryBackoff, "retry-backoff", config.RetryBackoff, "delay before recovered work can be claimed")
	fs.DurationVar(&config.Retention, "retention", config.Retention, "minimum terminal-job and idempotency retention")
}
