// Package fixture provides one deterministic, read-only incident investigation.
package fixture

import (
	"context"
	"fmt"

	"github.com/neev-rao/distributed-agent-service/internal/jobs"
)

const ScenarioID = "checkout-db-timeout"

// Evidence is synthetic data, not live telemetry or a model-generated claim.
var evidence = [...]struct {
	ID   string
	Text string
}{
	{"metrics.checkout.errors", "Checkout errors rise from 0.2% to 18% while request volume stays steady."},
	{"logs.checkout.db-timeout", "Failed checkout requests report database connection acquisition timeouts."},
	{"metrics.database.connections", "All 20 database connections are in use; waiting requests increase."},
	{"runbook.database-pool", "Correlate acquisition timeouts with exhausted connection capacity; investigate slow queries and unreleased connections."},
}

// Executor follows a fixed investigation path. A later milestone can replace it
// with an agent without changing job ownership, admission, or worker limits.
type Executor struct{}

func (Executor) Execute(ctx context.Context, scenario string) (jobs.Result, error) {
	if err := ctx.Err(); err != nil {
		return jobs.Result{}, err
	}
	if scenario != ScenarioID {
		return jobs.Result{}, fmt.Errorf("unknown fixture scenario %q", scenario)
	}
	ids := make([]string, 0, len(evidence))
	for _, item := range evidence {
		if err := ctx.Err(); err != nil {
			return jobs.Result{}, err
		}
		ids = append(ids, item.ID)
	}
	return jobs.Result{
		Diagnosis:   "Checkout requests fail because the database connection pool is exhausted, causing connection acquisition timeouts. The fixture does not establish why connections remain occupied; investigate slow queries and unreleased connections.",
		EvidenceIDs: ids,
	}, nil
}
