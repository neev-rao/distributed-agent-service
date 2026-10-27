package evaluation

import (
	"testing"

	"github.com/neev-rao/distributed-agent-service/internal/fixture"
	"github.com/neev-rao/distributed-agent-service/internal/jobs"
)

func TestRubricSeparatesFindingUncertaintyAndEvidence(t *testing.T) {
	result := jobs.Result{Diagnosis: "The database connection pool is exhausted; the underlying cause is unknown.", EvidenceIDs: []string{"logs.checkout.db-timeout", "metrics.database.connections"}}
	if !Assess(fixture.ScenarioID, result).Pass() {
		t.Fatal("supported, cautious finding rejected")
	}
	result.Diagnosis = "A deployment broke checkout."
	if Assess(fixture.ScenarioID, result).Pass() {
		t.Fatal("unsupported finding accepted")
	}
	result = jobs.Result{Diagnosis: "Checkout errors increased, but there is insufficient evidence to determine the cause.", EvidenceIDs: []string{"metrics.checkout.errors", "logs.checkout.unavailable", "metrics.database.unavailable"}}
	if !Assess(fixture.InsufficientScenarioID, result).Pass() {
		t.Fatal("insufficient evidence finding rejected")
	}
	result.Diagnosis = "Checkout errors increased, but their cause cannot be established from the available observations."
	if !Assess(fixture.InsufficientScenarioID, result).Pass() {
		t.Fatal("prompt's explicit uncertainty wording rejected")
	}
	result.EvidenceIDs = []string{"metrics.checkout.errors"}
	if Assess(fixture.InsufficientScenarioID, result).Pass() {
		t.Fatal("missing evidence accepted")
	}
	if Assess("unknown", result).Pass() {
		t.Fatal("unknown evaluation accepted")
	}
}
