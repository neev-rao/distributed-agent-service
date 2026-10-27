package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/neev-rao/distributed-agent-service/internal/agent"
	"github.com/neev-rao/distributed-agent-service/internal/evaluation"
	"github.com/neev-rao/distributed-agent-service/internal/fixture"
	"github.com/neev-rao/distributed-agent-service/internal/jobs"
)

type executorFunc func(context.Context, string) (jobs.Result, error)

func (f executorFunc) Execute(ctx context.Context, scenario string) (jobs.Result, error) {
	return f(ctx, scenario)
}

const privateTestKey = "private-evaluator-test-key-do-not-print"

func testEnvironment(t *testing.T) {
	t.Helper()
	t.Setenv("GEMINI_MODEL", "test-model")
	t.Setenv("GEMINI_API_KEY", privateTestKey)
}

func supportedResult() jobs.Result {
	return jobs.Result{
		Diagnosis:   "The database connection pool is exhausted; the underlying cause is unknown.",
		EvidenceIDs: []string{"logs.checkout.db-timeout", "metrics.database.connections"},
	}
}

func decodeReport(t *testing.T, output *bytes.Buffer) evaluation.Report {
	t.Helper()
	decoder := json.NewDecoder(output)
	var report evaluation.Report
	if err := decoder.Decode(&report); err != nil {
		t.Fatalf("invalid report JSON: %v", err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		t.Fatalf("stdout contains more than one report: %v", err)
	}
	return report
}

func assertNoKey(t *testing.T, stdout, stderr *bytes.Buffer) {
	t.Helper()
	if strings.Contains(stdout.String(), privateTestKey) || strings.Contains(stderr.String(), privateTestKey) {
		t.Fatal("configured API key appeared in command output")
	}
}

func TestListDoesNotRequireLiveCredentialsOrExecutor(t *testing.T) {
	t.Setenv("GEMINI_MODEL", "")
	t.Setenv("GEMINI_API_KEY", "")
	var stdout, stderr bytes.Buffer
	called := false
	factory := func(string) (jobs.Executor, error) {
		called = true
		return nil, errors.New("factory must not be used for listing")
	}
	if code := run(context.Background(), []string{"-list"}, &stdout, &stderr, factory); code != 0 {
		t.Fatalf("listing failed: exit=%d stderr=%s", code, &stderr)
	}
	if called || stderr.Len() != 0 {
		t.Fatalf("listing accessed the provider or wrote errors: called=%v stderr=%s", called, &stderr)
	}
	var cases []evaluation.Case
	if err := json.Unmarshal(stdout.Bytes(), &cases); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(cases, evaluation.Cases()) {
		t.Fatalf("listing omitted published review expectations: %+v", cases)
	}
}

func TestPreflightRejectsInvalidRequestsBeforeExecutorCreation(t *testing.T) {
	testEnvironment(t)
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"no opt in", nil},
		{"zero runs", []string{"-live", "-runs", "0"}},
		{"too many runs", []string{"-live", "-runs", "11"}},
		{"negative runs", []string{"-live", "-runs", "-1"}},
		{"unknown scenario", []string{"-live", "-scenario", "missing"}},
		{"positional argument", []string{"-live", "unexpected"}},
		{"unknown flag", []string{"-live", "-missing-flag"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			called := false
			factory := func(string) (jobs.Executor, error) {
				called = true
				return nil, errors.New("invalid request reached provider creation")
			}
			if code := run(context.Background(), tc.args, &stdout, &stderr, factory); code != 1 {
				t.Fatalf("invalid command exit=%d, want 1", code)
			}
			if called || stdout.Len() != 0 || stderr.Len() == 0 {
				t.Fatalf("preflight: called=%v stdout=%s stderr=%s", called, &stdout, &stderr)
			}
			assertNoKey(t, &stdout, &stderr)
		})
	}
}

func TestFactoryFailureDoesNotRevealPrivateError(t *testing.T) {
	testEnvironment(t)
	var stdout, stderr bytes.Buffer
	factory := func(string) (jobs.Executor, error) {
		return nil, errors.New("private provider configuration: " + privateTestKey)
	}
	if code := run(context.Background(), []string{"-live"}, &stdout, &stderr, factory); code != 1 {
		t.Fatalf("factory error exit=%d", code)
	}
	if stdout.Len() != 0 || !strings.Contains(stderr.String(), "GEMINI_API_KEY") {
		t.Fatalf("missing actionable configuration failure: stdout=%s stderr=%s", &stdout, &stderr)
	}
	assertNoKey(t, &stdout, &stderr)
}

func TestFilteredRepeatedRunsProduceCompleteJSON(t *testing.T) {
	testEnvironment(t)
	var stdout, stderr bytes.Buffer
	var executed []string
	factoryCalls := 0
	factory := func(model string) (jobs.Executor, error) {
		factoryCalls++
		if model != "selected-model" {
			t.Errorf("model flag did not override environment: %q", model)
		}
		return executorFunc(func(ctx context.Context, scenario string) (jobs.Result, error) {
			if _, ok := ctx.Deadline(); !ok {
				t.Error("investigation has no deadline")
			}
			executed = append(executed, scenario)
			return jobs.Result{
				Diagnosis:   "Checkout payment gateway timeouts are observed; database pool capacity is healthy. The gateway's underlying cause is unknown.",
				EvidenceIDs: []string{"logs.checkout.payment-timeout", "metrics.database.healthy"},
			}, nil
		}), nil
	}
	args := []string{"-live", "-model", "selected-model", "-scenario", fixture.PaymentScenarioID, "-runs", "3"}
	if code := run(context.Background(), args, &stdout, &stderr, factory); code != 0 {
		t.Fatalf("evaluation failed: exit=%d stderr=%s stdout=%s", code, &stderr, &stdout)
	}
	assertNoKey(t, &stdout, &stderr)
	if factoryCalls != 1 || !reflect.DeepEqual(executed, []string{fixture.PaymentScenarioID, fixture.PaymentScenarioID, fixture.PaymentScenarioID}) {
		t.Fatalf("wrong model creation or selected runs: factories=%d executions=%v", factoryCalls, executed)
	}
	report := decodeReport(t, &stdout)
	if !report.Passed || report.Requested != 3 || report.Completed != 3 || report.HeuristicPasses != 3 || report.ExecutionFailures != 0 || report.HeuristicFailures != 0 {
		t.Fatalf("wrong aggregate result: %+v", report)
	}
	if report.Model != "selected-model" || report.Profile == "" || report.SuiteFingerprint == "" || report.StartedAt.IsZero() || len(report.Outcomes) != 3 {
		t.Fatalf("report lacks reproducibility metadata: %+v", report)
	}
	for i, outcome := range report.Outcomes {
		if outcome.Scenario != fixture.PaymentScenarioID || outcome.Run != i+1 || outcome.Result == nil || outcome.Failure != nil || outcome.Checks == nil || !outcome.Checks.Pass() {
			t.Fatalf("unexpected run %d: %+v", i+1, outcome)
		}
	}
	if !strings.Contains(stderr.String(), "3 investigations") || !strings.Contains(stderr.String(), "12 model requests") {
		t.Fatalf("command did not disclose selected provider work: %s", &stderr)
	}
}

func TestHeuristicFailureStillProducesCompleteReport(t *testing.T) {
	testEnvironment(t)
	var stdout, stderr bytes.Buffer
	calls := 0
	factory := func(string) (jobs.Executor, error) {
		return executorFunc(func(context.Context, string) (jobs.Result, error) {
			calls++
			return jobs.Result{Diagnosis: "A deployment broke checkout.", EvidenceIDs: []string{"metrics.checkout.errors"}}, nil
		}), nil
	}
	if code := run(context.Background(), []string{"-live", "-scenario", fixture.ScenarioID, "-runs", "2"}, &stdout, &stderr, factory); code != 1 {
		t.Fatalf("heuristic failure exit=%d", code)
	}
	assertNoKey(t, &stdout, &stderr)
	report := decodeReport(t, &stdout)
	if calls != 2 || report.Passed || report.Completed != 2 || report.HeuristicFailures != 2 || report.ExecutionFailures != 0 || len(report.Outcomes) != 2 {
		t.Fatalf("heuristic failure missing from complete report: calls=%d report=%+v", calls, report)
	}
	for _, outcome := range report.Outcomes {
		if outcome.Result == nil || outcome.Checks == nil || outcome.Failure != nil {
			t.Fatalf("quality failure was confused with execution failure: %+v", outcome)
		}
	}
}

func TestExecutionFailuresAreClassifiedAndSanitized(t *testing.T) {
	testEnvironment(t)
	for _, tc := range []struct {
		name string
		err  error
		code string
	}{
		{"provider error", &agent.Error{Code: "provider_failure"}, "provider_failure"},
		{"private unknown error", errors.New("provider body contains " + privateTestKey), "evaluation_failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			factory := func(string) (jobs.Executor, error) {
				return executorFunc(func(context.Context, string) (jobs.Result, error) { return jobs.Result{}, tc.err }), nil
			}
			if code := run(context.Background(), []string{"-live", "-scenario", fixture.ScenarioID}, &stdout, &stderr, factory); code != 1 {
				t.Fatalf("execution failure exit=%d", code)
			}
			assertNoKey(t, &stdout, &stderr)
			report := decodeReport(t, &stdout)
			if report.Passed || report.ExecutionFailures != 1 || report.HeuristicFailures != 0 || len(report.Outcomes) != 1 {
				t.Fatalf("wrong execution failure count: %+v", report)
			}
			outcome := report.Outcomes[0]
			if outcome.Failure == nil || outcome.Failure.Code != tc.code || outcome.Result != nil || outcome.Checks != nil {
				t.Fatalf("wrong classified failure: %+v", outcome)
			}
		})
	}
}

func TestCancellationPreservesPartialReport(t *testing.T) {
	testEnvironment(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var stdout, stderr bytes.Buffer
	calls := 0
	factory := func(string) (jobs.Executor, error) {
		return executorFunc(func(attempt context.Context, scenario string) (jobs.Result, error) {
			calls++
			if calls == 2 {
				cancel()
				return jobs.Result{}, attempt.Err()
			}
			return supportedResult(), nil
		}), nil
	}
	if code := run(ctx, []string{"-live", "-scenario", fixture.ScenarioID, "-runs", "3"}, &stdout, &stderr, factory); code != 1 {
		t.Fatalf("cancellation exit=%d", code)
	}
	assertNoKey(t, &stdout, &stderr)
	report := decodeReport(t, &stdout)
	if calls != 2 || report.Passed || !report.Interrupted || report.Requested != 3 || report.Completed != 2 || report.HeuristicPasses != 1 || report.ExecutionFailures != 1 || len(report.Outcomes) != 2 {
		t.Fatalf("partial results or cancellation lost: calls=%d report=%+v", calls, report)
	}
	if report.Outcomes[0].Result == nil || report.Outcomes[1].Failure == nil || report.Outcomes[1].Failure.Code != "evaluation_canceled" {
		t.Fatalf("wrong partial outcomes: %+v", report.Outcomes)
	}
}

type failedWriter struct{}

func (failedWriter) Write([]byte) (int, error) { return 0, errors.New("output unavailable") }

func TestOutputFailureReturnsNonzero(t *testing.T) {
	testEnvironment(t)
	var stderr bytes.Buffer
	factory := func(string) (jobs.Executor, error) {
		return executorFunc(func(context.Context, string) (jobs.Result, error) { return supportedResult(), nil }), nil
	}
	if code := run(context.Background(), []string{"-live", "-scenario", fixture.ScenarioID}, failedWriter{}, &stderr, factory); code != 1 {
		t.Fatalf("failed report write exit=%d", code)
	}
	if code := run(context.Background(), []string{"-list"}, failedWriter{}, &stderr, factory); code != 1 {
		t.Fatalf("failed listing write exit=%d", code)
	}
}
