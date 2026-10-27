package fixture

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestEvaluationCatalogDoesNotExpandPublicWorkload(t *testing.T) {
	want := []string{ScenarioID, InsufficientScenarioID, PaymentScenarioID, ConflictingScenarioID}
	first := EvaluationScenarios()
	if !reflect.DeepEqual(first, want) {
		t.Fatalf("evaluation scenarios = %v, want %v", first, want)
	}
	first[0] = "caller mutation"
	if !reflect.DeepEqual(EvaluationScenarios(), want) {
		t.Fatal("caller mutation changed the evaluation catalog")
	}
	for _, scenario := range want {
		if !ValidInvestigationScenario(scenario) {
			t.Fatalf("evaluation scenario %q is unavailable to investigations", scenario)
		}
	}
	for _, scenario := range []string{PaymentScenarioID, ConflictingScenarioID} {
		if ValidScenario(scenario) {
			t.Fatalf("evaluation scenario %q expanded public admission", scenario)
		}
		if _, err := (Executor{}).Execute(context.Background(), scenario); err == nil {
			t.Fatalf("evaluation scenario %q expanded deterministic execution", scenario)
		}
	}
	if ValidInvestigationScenario("unknown") {
		t.Fatal("unknown investigation scenario accepted")
	}
}

func TestEvaluationEvidenceChangesObservationsWithoutChangingTask(t *testing.T) {
	for _, scenario := range []string{PaymentScenarioID, ConflictingScenarioID} {
		t.Run(scenario, func(t *testing.T) {
			metrics, err := Read(context.Background(), scenario, "read_metrics")
			if err != nil || len(metrics) != 2 {
				t.Fatalf("metrics = %+v, err = %v", metrics, err)
			}
			if metrics[0].ID != evidence[0].ID || metrics[0].Text != evidence[0].Text {
				t.Fatal("common checkout observation changed")
			}
			if metrics[1].ID != "metrics.database.healthy" || !strings.Contains(metrics[1].Text, "4 of 20") ||
				!strings.Contains(metrics[1].Text, "zero waiting requests") || !strings.Contains(metrics[1].Text, "zero connection acquisition timeouts") {
				t.Fatalf("available connection capacity not represented: %+v", metrics[1])
			}
			logs, err := Read(context.Background(), scenario, "read_logs")
			if err != nil || len(logs) != 1 {
				t.Fatalf("logs = %+v, err = %v", logs, err)
			}
			if !strings.Contains(metrics[1].Text, "12:00-12:05 UTC") || !strings.Contains(logs[0].Text, "12:00-12:05 UTC") {
				t.Fatal("log and metrics observations do not cover the same incident window")
			}
			if scenario == PaymentScenarioID {
				if logs[0].ID != "logs.checkout.payment-timeout" || !strings.Contains(logs[0].Text, "payment gateway response timeouts") ||
					!strings.Contains(logs[0].Text, "internal telemetry is unavailable") {
					t.Fatalf("alternative failure or uncertainty absent: %+v", logs[0])
				}
			} else if logs[0].ID != "logs.checkout.db-timeout" || !strings.Contains(logs[0].Text, "all 20 connections in use") ||
				!strings.Contains(logs[0].Text, "increasing waiting requests") || !strings.Contains(logs[0].Text, "same pool or collection source") {
				t.Fatalf("conflicting observation or source limitation absent: %+v", logs[0])
			}
			guide, err := Read(context.Background(), scenario, "read_runbook")
			if err != nil || len(guide) != 1 || guide[0].ID != evidence[3].ID || guide[0].Text != evidence[3].Text {
				t.Fatalf("shared runbook changed: %+v, err = %v", guide, err)
			}
		})
	}
}

func TestEveryEvaluationToolReadIsIndependentAndCancellable(t *testing.T) {
	for _, scenario := range EvaluationScenarios() {
		t.Run(scenario, func(t *testing.T) {
			for _, tool := range []string{"read_metrics", "read_logs", "read_runbook"} {
				first, err := Read(context.Background(), scenario, tool)
				if err != nil || len(first) == 0 {
					t.Fatalf("%s evidence = %+v, err = %v", tool, first, err)
				}
				want := append([]Evidence(nil), first...)
				for i := range first {
					first[i] = Evidence{"caller mutation", "caller mutation"}
				}
				again, err := Read(context.Background(), scenario, tool)
				if err != nil || !reflect.DeepEqual(again, want) {
					t.Fatalf("%s was contaminated by caller mutation: %+v, err = %v", tool, again, err)
				}
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				if _, err = Read(ctx, scenario, tool); !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled %s returned %v", tool, err)
				}
			}
			if _, err := Read(context.Background(), scenario, "run_shell"); err == nil {
				t.Fatal("unknown tool accepted")
			}
		})
	}
}
