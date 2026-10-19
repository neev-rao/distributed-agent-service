// Package fixture provides deterministic, read-only incident evidence and demos.
package fixture

import (
	"context"
	"fmt"

	"github.com/neev-rao/distributed-agent-service/internal/jobs"
)

const ScenarioID = "checkout-db-timeout"

// InsufficientScenarioID deliberately withholds diagnostic signals. It lets
// evaluations distinguish an evidence-based answer from a memorized diagnosis.
const InsufficientScenarioID = "checkout-insufficient-evidence"

type Evidence struct {
	ID   string `json:"id"`
	Text string `json:"text"`
}

func ValidScenario(scenario string) bool {
	return scenario == ScenarioID || scenario == InsufficientScenarioID
}

// Read returns a fresh copy of synthetic evidence for one allowlisted tool.
// It performs no network, filesystem, database, or other external operations.
func Read(ctx context.Context, scenario, tool string) ([]Evidence, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !ValidScenario(scenario) {
		return nil, fmt.Errorf("unknown fixture scenario %q", scenario)
	}
	var items []Evidence
	switch tool {
	case "read_metrics":
		items = []Evidence{{evidence[0].ID, evidence[0].Text}, {evidence[2].ID, evidence[2].Text}}
		if scenario == InsufficientScenarioID {
			items[1] = Evidence{"metrics.database.unavailable", "Database connection metrics are unavailable for this incident window."}
		}
	case "read_logs":
		items = []Evidence{{evidence[1].ID, evidence[1].Text}}
		if scenario == InsufficientScenarioID {
			items[0] = Evidence{"logs.checkout.unavailable", "Checkout request logs are unavailable for this incident window."}
		}
	case "read_runbook":
		items = []Evidence{{evidence[3].ID, evidence[3].Text}}
	default:
		return nil, fmt.Errorf("unknown fixture tool %q", tool)
	}
	return items, nil
}

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

// Executor follows a fixed investigation path. Model-backed execution uses the
// same evidence through tools without changing job ownership or worker limits.
type Executor struct{}

func (Executor) Execute(ctx context.Context, scenario string) (jobs.Result, error) {
	if err := ctx.Err(); err != nil {
		return jobs.Result{}, err
	}
	if !ValidScenario(scenario) {
		return jobs.Result{}, fmt.Errorf("unknown fixture scenario %q", scenario)
	}
	if scenario == InsufficientScenarioID {
		return jobs.Result{
			Diagnosis:   "Checkout errors have increased, but unavailable request logs and database connection metrics prevent identifying the cause. Gather those signals before concluding that the connection pool is exhausted.",
			EvidenceIDs: []string{"metrics.checkout.errors", "metrics.database.unavailable", "logs.checkout.unavailable"},
		}, nil
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
