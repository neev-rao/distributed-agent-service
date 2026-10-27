// Package evaluation supplies transparent, deliberately limited checks for the
// synthetic incidents. Passing these checks is not proof of factual quality.
package evaluation

import (
	"strings"

	"github.com/neev-rao/distributed-agent-service/internal/fixture"
	"github.com/neev-rao/distributed-agent-service/internal/jobs"
)

type Checks struct {
	ExpectedFinding       bool `json:"expected_finding"`
	StatesUncertainty     bool `json:"states_uncertainty"`
	CitesRelevantEvidence bool `json:"cites_relevant_evidence"`
}

// Case publishes review expectations, never instructions sent to the model.
type Case struct {
	Scenario         string   `json:"scenario_id"`
	ExpectedBehavior string   `json:"expected_behavior"`
	RequiredEvidence []string `json:"required_evidence"`
}

func Cases() []Case {
	return []Case{
		{fixture.ScenarioID, "Identify connection-pool exhaustion; do not assert why connections remain occupied.", []string{"logs.checkout.db-timeout", "metrics.database.connections"}},
		{fixture.InsufficientScenarioID, "Identify missing logs and connection metrics; the cause cannot be established.", []string{"metrics.checkout.errors", "metrics.database.unavailable", "logs.checkout.unavailable"}},
		{fixture.PaymentScenarioID, "Identify observed payment-gateway timeouts and healthy pool capacity; why the gateway timed out is unknown.", []string{"logs.checkout.payment-timeout", "metrics.database.healthy"}},
		{fixture.ConflictingScenarioID, "Acknowledge conflicting pool observations from the same window; reconcile source/pool identity before confirming a cause.", []string{"logs.checkout.db-timeout", "metrics.database.healthy"}},
	}
}

func (c Checks) Pass() bool {
	return c.ExpectedFinding && c.StatesUncertainty && c.CitesRelevantEvidence
}

// Assess checks a small English-language rubric. Inspect the complete diagnosis
// as well: substring checks can miss paraphrases or accept contradictory prose.
func Assess(scenario string, result jobs.Result) Checks {
	text := strings.ToLower(result.Diagnosis)
	uncertain := containsAny(text, "unknown", "unclear", "insufficient", "cannot determine", "can't determine",
		"does not establish", "not established", "not enough", "cannot establish", "cannot be established",
		"unable to determine", "not determined", "uncertain", "not confirmed", "cannot confirm", "unconfirmed")
	cited := make(map[string]bool)
	for _, id := range result.EvidenceIDs {
		cited[id] = true
	}
	// Every required diagnostic signal must be cited. Provenance (whether an ID
	// was actually retrieved) remains the agent runtime's separate responsibility.
	relevant := false
	for _, item := range Cases() {
		if item.Scenario == scenario {
			relevant = true
			for _, id := range item.RequiredEvidence {
				relevant = relevant && cited[id]
			}
		}
	}
	switch scenario {
	case fixture.ScenarioID:
		return Checks{
			ExpectedFinding:       strings.Contains(text, "pool") && containsAny(text, "exhaust", "saturat", "all connections", "capacity"),
			StatesUncertainty:     uncertain,
			CitesRelevantEvidence: relevant,
		}
	case fixture.InsufficientScenarioID:
		return Checks{
			ExpectedFinding:       containsAny(text, "error", "failure") && uncertain,
			StatesUncertainty:     uncertain,
			CitesRelevantEvidence: relevant,
		}
	case fixture.PaymentScenarioID:
		return Checks{
			ExpectedFinding: strings.Contains(text, "payment") && containsAny(text, "timeout", "timed out", "timing out") &&
				strings.Contains(text, "pool") && containsAny(text, "healthy", "available", "spare", "not exhausted"),
			StatesUncertainty:     uncertain,
			CitesRelevantEvidence: relevant,
		}
	case fixture.ConflictingScenarioID:
		return Checks{
			ExpectedFinding:       strings.Contains(text, "pool") && containsAny(text, "conflict", "contradict", "inconsisten", "disagree"),
			StatesUncertainty:     uncertain,
			CitesRelevantEvidence: relevant,
		}
	default:
		return Checks{}
	}
}

func containsAny(text string, choices ...string) bool {
	for _, choice := range choices {
		if strings.Contains(text, choice) {
			return true
		}
	}
	return false
}
