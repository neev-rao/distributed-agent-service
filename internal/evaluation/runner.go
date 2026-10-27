package evaluation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/neev-rao/distributed-agent-service/internal/agent"
	"github.com/neev-rao/distributed-agent-service/internal/fixture"
	"github.com/neev-rao/distributed-agent-service/internal/jobs"
)

const (
	SuiteVersion      = "checkout-evidence-v2"
	MaxRuns           = 10
	AttemptTimeout    = 30 * time.Second
	ReviewRequirement = "Heuristic English-language checks only. Review full diagnoses for unsupported claims, contradictions, and misleading uncertainty; passing does not prove model quality."
)

type Config struct {
	Model     string
	Profile   string
	Scenarios []string
	Runs      int
}

// Validate runs before any provider session is created. A repetition is an
// independent investigation, never a retry of a failed model request.
func (c Config) Validate() error {
	if c.Model == "" || c.Profile == "" || c.Runs < 1 || c.Runs > MaxRuns || len(c.Scenarios) == 0 || len(c.Scenarios) > len(Cases()) {
		return errors.New("model, profile, known scenarios, and 1 to 10 runs are required")
	}
	seen := make(map[string]bool)
	for _, scenario := range c.Scenarios {
		if !fixture.ValidInvestigationScenario(scenario) || seen[scenario] {
			return errors.New("select distinct known evaluation scenarios")
		}
		seen[scenario] = true
	}
	return nil
}

type Outcome struct {
	Scenario   string         `json:"scenario_id"`
	Run        int            `json:"run"`
	DurationMS int64          `json:"duration_ms"`
	Result     *jobs.Result   `json:"result,omitempty"`
	Failure    *jobs.JobError `json:"error,omitempty"`
	Checks     *Checks        `json:"checks,omitempty"`
}

type Report struct {
	SchemaVersion     int       `json:"schema_version"`
	SuiteVersion      string    `json:"suite_version"`
	SuiteFingerprint  string    `json:"suite_fingerprint"`
	Model             string    `json:"model"`
	Profile           string    `json:"execution_profile"`
	StartedAt         time.Time `json:"started_at"`
	Scenarios         []string  `json:"scenarios"`
	Runs              int       `json:"runs_per_scenario"`
	AttemptTimeoutMS  int64     `json:"attempt_timeout_ms"`
	Requested         int       `json:"requested"`
	Completed         int       `json:"completed"`
	ExecutionFailures int       `json:"execution_failures"`
	HeuristicFailures int       `json:"heuristic_failures"`
	HeuristicPasses   int       `json:"heuristic_passes"`
	Interrupted       bool      `json:"interrupted"`
	Passed            bool      `json:"rubric_passed"`
	Review            string    `json:"review_requirement"`
	Outcomes          []Outcome `json:"outcomes"`
}

// SuiteFingerprint identifies fixture observations and published expectations,
// separately from the worker profile. Bump SuiteVersion when rubric logic changes.
func SuiteFingerprint() string {
	type sample struct {
		Scenario, Tool string
		Evidence       []fixture.Evidence
	}
	var samples []sample
	for _, scenario := range fixture.EvaluationScenarios() {
		for _, tool := range []string{"read_metrics", "read_logs", "read_runbook"} {
			items, _ := fixture.Read(context.Background(), scenario, tool)
			samples = append(samples, sample{scenario, tool, items})
		}
	}
	data, _ := json.Marshal(struct {
		Version string
		Cases   []Case
		Samples []sample
	}{SuiteVersion, Cases(), samples})
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// Run executes sequentially and retains failures in the report. It never makes
// parallel calls, retries, or changes the executor's model/tool budgets. The
// executor must honor cancellation; Go cannot forcibly stop arbitrary code.
func Run(ctx context.Context, cfg Config, executor jobs.Executor) (Report, error) {
	if err := cfg.Validate(); err != nil {
		return Report{}, err
	}
	if executor == nil {
		return Report{}, errors.New("an evaluation executor is required")
	}
	report := Report{SchemaVersion: 1, SuiteVersion: SuiteVersion, SuiteFingerprint: SuiteFingerprint(),
		Model: cfg.Model, Profile: cfg.Profile, StartedAt: time.Now().UTC(),
		Scenarios: append([]string(nil), cfg.Scenarios...), Runs: cfg.Runs,
		AttemptTimeoutMS: AttemptTimeout.Milliseconds(), Requested: len(cfg.Scenarios) * cfg.Runs,
		Review: ReviewRequirement, Outcomes: make([]Outcome, 0, len(cfg.Scenarios)*cfg.Runs)}
	for run := 1; run <= cfg.Runs; run++ {
		for _, scenario := range report.Scenarios {
			if ctx.Err() != nil {
				break
			}
			attemptCtx, cancel := context.WithTimeout(ctx, AttemptTimeout)
			started := time.Now()
			result, err := executor.Execute(attemptCtx, scenario)
			contextErr := attemptCtx.Err()
			cancel()
			item := Outcome{Scenario: scenario, Run: run, DurationMS: time.Since(started).Milliseconds()}
			if err != nil || contextErr != nil {
				failure := executionFailure(err, contextErr)
				item.Failure = &failure
				report.ExecutionFailures++
			} else {
				result.EvidenceIDs = append([]string(nil), result.EvidenceIDs...)
				item.Result = &result
				checks := Assess(scenario, result)
				item.Checks = &checks
				if checks.Pass() {
					report.HeuristicPasses++
				} else {
					report.HeuristicFailures++
				}
			}
			report.Outcomes = append(report.Outcomes, item)
			report.Completed++
		}
		if ctx.Err() != nil {
			break
		}
	}
	report.Interrupted = ctx.Err() != nil
	report.Passed = !report.Interrupted && report.Completed == report.Requested && report.HeuristicPasses == report.Requested
	return report, nil
}

func executionFailure(err, contextErr error) jobs.JobError {
	if errors.Is(contextErr, context.Canceled) {
		return jobs.JobError{Code: "evaluation_canceled", Message: "The evaluation was canceled."}
	}
	if errors.Is(contextErr, context.DeadlineExceeded) {
		return jobs.JobError{Code: "evaluation_timeout", Message: "The investigation exceeded the evaluation deadline."}
	}
	var classified *agent.Error
	if errors.As(err, &classified) {
		return classified.Failure()
	}
	if errors.Is(err, context.Canceled) {
		return jobs.JobError{Code: "evaluation_canceled", Message: "The evaluation was canceled."}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return jobs.JobError{Code: "evaluation_timeout", Message: "The investigation exceeded the evaluation deadline."}
	}
	return jobs.JobError{Code: "evaluation_failed", Message: "Investigation did not complete."}
}
