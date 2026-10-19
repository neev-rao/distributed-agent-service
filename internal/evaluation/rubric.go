// Package evaluation supplies transparent, deliberately limited checks for the
// two synthetic incidents. Passing these checks is not proof of factual quality.
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
	switch scenario {
	case fixture.ScenarioID:
		return Checks{
			ExpectedFinding:       strings.Contains(text, "pool") && containsAny(text, "exhaust", "saturat", "all connections", "capacity"),
			StatesUncertainty:     uncertain,
			CitesRelevantEvidence: cited["logs.checkout.db-timeout"] && cited["metrics.database.connections"],
		}
	case fixture.InsufficientScenarioID:
		return Checks{
			ExpectedFinding:       containsAny(text, "error", "failure") && uncertain,
			StatesUncertainty:     uncertain,
			CitesRelevantEvidence: cited["metrics.checkout.errors"] && len(cited) >= 2,
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
