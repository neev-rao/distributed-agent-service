package fixture

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestExecutorReturnsDeterministicSupportedDiagnosis(t *testing.T) {
	executor := Executor{}
	first, err := executor.Execute(context.Background(), ScenarioID)
	if err != nil {
		t.Fatal(err)
	}
	second, err := executor.Execute(context.Background(), ScenarioID)
	if err != nil || !reflect.DeepEqual(first, second) {
		t.Fatalf("fixture changed across executions: first=%+v second=%+v err=%v", first, second, err)
	}
	if first.Diagnosis == "" || len(first.EvidenceIDs) != len(evidence) {
		t.Fatalf("missing diagnosis or evidence: %+v", first)
	}
	for i, id := range first.EvidenceIDs {
		if id != evidence[i].ID || evidence[i].Text == "" {
			t.Fatalf("evidence %q has no fixture source", id)
		}
	}
	first.EvidenceIDs[0] = "changed by caller"
	third, err := executor.Execute(context.Background(), ScenarioID)
	if err != nil || !reflect.DeepEqual(second, third) {
		t.Fatalf("caller mutation contaminated fixture: result=%+v err=%v", third, err)
	}
}

func TestExecutorRejectsUnknownScenario(t *testing.T) {
	if _, err := (Executor{}).Execute(context.Background(), "missing"); err == nil {
		t.Fatal("unknown scenario unexpectedly succeeded")
	}
}

func TestExecutorHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (Executor{}).Execute(ctx, ScenarioID); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
}
