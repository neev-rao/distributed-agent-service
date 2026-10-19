package fixture

import (
	"context"
	"errors"
	"testing"
)

func TestReadToolsSelectSignalsAndReturnFreshEvidence(t *testing.T) {
	for _, tool := range []string{"read_metrics", "read_logs", "read_runbook"} {
		items, err := Read(context.Background(), ScenarioID, tool)
		if err != nil || len(items) == 0 {
			t.Fatalf("%s: evidence=%v err=%v", tool, items, err)
		}
		original := items[0]
		items[0].Text = "caller mutation"
		again, err := Read(context.Background(), ScenarioID, tool)
		if err != nil || again[0] != original {
			t.Fatalf("%s: shared evidence mutation", tool)
		}
	}
}

func TestInsufficientScenarioWithholdsDiagnosticObservations(t *testing.T) {
	for tool, missing := range map[string]string{"read_metrics": "metrics.database.unavailable", "read_logs": "logs.checkout.unavailable"} {
		items, err := Read(context.Background(), InsufficientScenarioID, tool)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, item := range items {
			if item.ID == missing {
				found = true
			}
			if item.ID == "metrics.database.connections" || item.ID == "logs.checkout.db-timeout" {
				t.Fatal("withheld observations leaked")
			}
		}
		if !found {
			t.Fatalf("missing availability evidence %s", missing)
		}
	}
}

func TestToolsRejectInvalidInputAndCancellation(t *testing.T) {
	if _, err := Read(context.Background(), "unknown", "read_metrics"); err == nil {
		t.Fatal("unknown scenario accepted")
	}
	if _, err := Read(context.Background(), ScenarioID, "run_shell"); err == nil {
		t.Fatal("unknown tool accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Read(ctx, ScenarioID, "read_metrics"); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want cancellation", err)
	}
}
