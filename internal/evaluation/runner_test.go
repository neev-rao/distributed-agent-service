package evaluation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/neev-rao/distributed-agent-service/internal/agent"
	"github.com/neev-rao/distributed-agent-service/internal/fixture"
	"github.com/neev-rao/distributed-agent-service/internal/jobs"
)

func passingResult(scenario string) jobs.Result {
	text := map[string]string{
		fixture.ScenarioID:             "The database pool is exhausted; why connections remain occupied is unknown.",
		fixture.InsufficientScenarioID: "Checkout errors increased; unavailable logs and database metrics mean the cause cannot be established.",
		fixture.PaymentScenarioID:      "Payment gateway timeouts explain failures; pool capacity is healthy. The reason for gateway timeouts is unknown.",
		fixture.ConflictingScenarioID:  "Pool observations conflict between logs and metrics; the cause cannot be established until sources are reconciled.",
	}
	for _, c := range Cases() {
		if c.Scenario == scenario {
			return jobs.Result{Diagnosis: text[scenario], EvidenceIDs: append([]string(nil), c.RequiredEvidence...)}
		}
	}
	return jobs.Result{}
}

func testConfig() Config {
	return Config{Model: "test-model", Profile: "test-profile", Scenarios: fixture.EvaluationScenarios(), Runs: 2}
}

func TestRepeatedEvaluationRecordsEveryOutcome(t *testing.T) {
	cfg := testConfig()
	var order []string
	report, err := Run(context.Background(), cfg, jobs.ExecutorFunc(func(ctx context.Context, scenario string) (jobs.Result, error) {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > AttemptTimeout || time.Until(deadline) <= 0 {
			t.Fatal("missing bounded attempt deadline")
		}
		order = append(order, scenario)
		return passingResult(scenario), nil
	}))
	wantOrder := append(fixture.EvaluationScenarios(), fixture.EvaluationScenarios()...)
	if err != nil || !reflect.DeepEqual(order, wantOrder) {
		t.Fatalf("order%v err%v", order, err)
	}
	if !report.Passed || report.Interrupted || report.Requested != 8 || report.Completed != 8 || report.HeuristicPasses != 8 || report.ExecutionFailures != 0 || report.HeuristicFailures != 0 {
		t.Fatalf("wrong accounting: %+v", report)
	}
	if report.Model != cfg.Model || report.Profile != cfg.Profile || report.StartedAt.Location() != time.UTC || report.StartedAt.IsZero() || report.AttemptTimeoutMS != 30000 || report.Review == "" || report.SuiteVersion == "" || len(report.SuiteFingerprint) != 64 {
		t.Fatalf("missing report metadata: %+v", report)
	}
	for i, item := range report.Outcomes {
		if item.Run != 1+i/4 || item.Scenario != order[i] || item.DurationMS < 0 || item.Result == nil || item.Failure != nil || item.Checks == nil || !item.Checks.Pass() {
			t.Fatalf("outcome: %+v", item)
		}
	}
	cfg.Scenarios[0] = "mutated"
	if report.Scenarios[0] != fixture.ScenarioID {
		t.Fatal("report shares configuration slice")
	}
}

func TestFailuresRemainVisibleWithoutRetries(t *testing.T) {
	cfg := testConfig()
	cfg.Scenarios = []string{fixture.ScenarioID}
	cfg.Runs = 4
	calls := 0
	report, err := Run(context.Background(), cfg, jobs.ExecutorFunc(func(context.Context, string) (jobs.Result, error) {
		calls++
		switch calls {
		case 1:
			return jobs.Result{}, errors.New("private?key=secret")
		case 2:
			return jobs.Result{}, fmt.Errorf("wrapped: %w", &agent.Error{Code: "provider_failure"})
		case 3:
			return jobs.Result{Diagnosis: "unsupported", EvidenceIDs: []string{"invented"}}, nil
		default:
			return passingResult(fixture.ScenarioID), nil
		}
	}))
	if err != nil || calls != 4 || report.Passed || report.Completed != 4 || report.ExecutionFailures != 2 || report.HeuristicFailures != 1 || report.HeuristicPasses != 1 {
		t.Fatalf("accounting: %+v calls%d err%v", report, calls, err)
	}
	if report.Outcomes[0].Failure.Code != "evaluation_failed" || report.Outcomes[1].Failure.Code != "provider_failure" || report.Outcomes[0].Checks != nil || report.Outcomes[0].Result != nil {
		t.Fatalf("failures: %+v", report.Outcomes)
	}
	data, _ := json.Marshal(report)
	if strings.Contains(string(data), "secret") {
		t.Fatal("untrusted error text leaked")
	}
}

func TestCancellationPreservesPartialReportAndRejectsLateSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	report, err := Run(ctx, testConfig(), jobs.ExecutorFunc(func(_ context.Context, scenario string) (jobs.Result, error) {
		calls++
		if calls == 2 {
			cancel()
		}
		return passingResult(scenario), nil
	}))
	if err != nil || calls != 2 || !report.Interrupted || report.Passed || report.Requested != 8 || report.Completed != 2 || report.HeuristicPasses != 1 || report.ExecutionFailures != 1 || report.Outcomes[1].Failure.Code != "evaluation_canceled" {
		t.Fatalf("partial report: %+v err%v", report, err)
	}
	before, stop := context.WithCancel(context.Background())
	stop()
	report, err = Run(before, testConfig(), jobs.ExecutorFunc(func(context.Context, string) (jobs.Result, error) {
		t.Fatal("executed after cancellation")
		return jobs.Result{}, nil
	}))
	if err != nil || report.Completed != 0 || !report.Interrupted || report.Passed || report.Outcomes == nil {
		t.Fatalf("pre-canceled: %+v %v", report, err)
	}
}

func TestEvaluationValidationPrecedesExecution(t *testing.T) {
	for name, mutate := range map[string]func(*Config){
		"zero runs":        func(c *Config) { c.Runs = 0 },
		"negative runs":    func(c *Config) { c.Runs = -1 },
		"too many runs":    func(c *Config) { c.Runs = 11 },
		"no scenarios":     func(c *Config) { c.Scenarios = nil },
		"unknown scenario": func(c *Config) { c.Scenarios = []string{"unknown"} },
		"duplicates":       func(c *Config) { c.Scenarios = []string{fixture.ScenarioID, fixture.ScenarioID} },
		"no model":         func(c *Config) { c.Model = "" },
		"no profile":       func(c *Config) { c.Profile = "" },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := testConfig()
			mutate(&cfg)
			_, err := Run(context.Background(), cfg, jobs.ExecutorFunc(func(context.Context, string) (jobs.Result, error) {
				t.Fatal("invalid config executed")
				return jobs.Result{}, nil
			}))
			if err == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
	if _, err := Run(context.Background(), testConfig(), nil); err == nil {
		t.Fatal("nil executor accepted")
	}
	cfg := testConfig()
	cfg.Runs = MaxRuns
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestSelectedScenarioDoesNotRunOthers(t *testing.T) {
	cfg := testConfig()
	cfg.Scenarios = []string{fixture.PaymentScenarioID}
	report, err := Run(context.Background(), cfg, jobs.ExecutorFunc(func(_ context.Context, s string) (jobs.Result, error) {
		if s != fixture.PaymentScenarioID {
			t.Fatalf("unselected scenario %s", s)
		}
		return passingResult(s), nil
	}))
	if err != nil || !report.Passed || report.Completed != 2 || report.Requested != 2 {
		t.Fatalf("%+v err%v", report, err)
	}
}

func TestEvaluationAndProviderTimeoutRemainDistinct(t *testing.T) {
	if got := executionFailure(&agent.Error{Code: "provider_timeout"}, nil); got.Code != "provider_timeout" {
		t.Fatalf("%+v", got)
	}
	if got := executionFailure(nil, context.DeadlineExceeded); got.Code != "evaluation_timeout" {
		t.Fatalf("%+v", got)
	}
}

func TestSuiteMetadataIsStableAndIsolated(t *testing.T) {
	first := SuiteFingerprint()
	cases := Cases()
	cases[0].RequiredEvidence[0] = "mutated"
	cases[0].ExpectedBehavior = "changed"
	if first != SuiteFingerprint() || len(first) != 64 || Cases()[0].RequiredEvidence[0] == "mutated" {
		t.Fatal("shared or unstable suite identity")
	}
	var ids []string
	for _, item := range Cases() {
		ids = append(ids, item.Scenario)
	}
	if !reflect.DeepEqual(ids, fixture.EvaluationScenarios()) {
		t.Fatal("rubric and fixture catalogs disagree")
	}
}

func TestEvidenceVariantsRequireDistinctFindingsAndCitations(t *testing.T) {
	for _, scenario := range fixture.EvaluationScenarios() {
		t.Run(scenario, func(t *testing.T) {
			result := passingResult(scenario)
			if !Assess(scenario, result).Pass() {
				t.Fatalf("expected behavior rejected: %+v", Assess(scenario, result))
			}
			for i := range result.EvidenceIDs {
				missing := result
				missing.EvidenceIDs = append([]string(nil), result.EvidenceIDs[:i]...)
				missing.EvidenceIDs = append(missing.EvidenceIDs, result.EvidenceIDs[i+1:]...)
				if Assess(scenario, missing).CitesRelevantEvidence {
					t.Fatalf("missing required citation %s accepted", result.EvidenceIDs[i])
				}
			}
			result.Diagnosis = "Checkout failed after a deployment."
			if Assess(scenario, result).Pass() {
				t.Fatal("unsupported diagnosis accepted")
			}
		})
	}
	memorized := passingResult(fixture.ScenarioID)
	for _, scenario := range []string{fixture.PaymentScenarioID, fixture.ConflictingScenarioID} {
		if Assess(scenario, memorized).Pass() {
			t.Fatal("original answer passed a contrary-evidence case")
		}
	}
}
