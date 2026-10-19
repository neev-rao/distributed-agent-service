// evaluate makes explicitly opted-in, potentially billable model calls. It
// evaluates model behavior separately from durable job execution reliability.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/neev-rao/distributed-agent-service/internal/agent"
	"github.com/neev-rao/distributed-agent-service/internal/appconfig"
	"github.com/neev-rao/distributed-agent-service/internal/evaluation"
	"github.com/neev-rao/distributed-agent-service/internal/fixture"
	"github.com/neev-rao/distributed-agent-service/internal/gemini"
	"github.com/neev-rao/distributed-agent-service/internal/jobs"
)

type outcome struct {
	Scenario string            `json:"scenario_id"`
	Result   *jobs.Result      `json:"result,omitempty"`
	Failure  *jobs.JobError    `json:"error,omitempty"`
	Checks   evaluation.Checks `json:"checks"`
}

func main() { os.Exit(run()) }

func run() int {
	live := flag.Bool("live", false, "explicitly permit live Gemini API requests (may incur charges)")
	model := flag.String("model", os.Getenv("GEMINI_MODEL"), "explicit Gemini model ID (or GEMINI_MODEL)")
	flag.Parse()
	if !*live {
		fmt.Fprintln(os.Stderr, "Live evaluation requires explicit -live; normal tests make no provider requests.")
		return 1
	}
	provider, err := gemini.New(gemini.Config{APIKey: os.Getenv("GEMINI_API_KEY"), Model: *model})
	if err != nil {
		fmt.Fprintln(os.Stderr, "Set GEMINI_API_KEY and a valid GEMINI_MODEL locally before live evaluation.")
		return 1
	}
	cfg := agent.DefaultConfig()
	executor, err := agent.New(cfg, provider)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Invalid agent configuration.")
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	report := struct {
		Model    string    `json:"model"`
		Profile  string    `json:"execution_profile"`
		Passed   bool      `json:"rubric_passed"`
		Review   string    `json:"review_requirement"`
		Outcomes []outcome `json:"outcomes"`
	}{Model: *model, Profile: appconfig.GeminiProfile(*model), Passed: true,
		Review: "Heuristic English-language checks only. Review full diagnoses for unsupported claims and misleading uncertainty; passing does not prove model quality."}
	for _, scenario := range []string{fixture.ScenarioID, fixture.InsufficientScenarioID} {
		if ctx.Err() != nil {
			report.Passed = false
			break
		}
		attemptCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		result, err := executor.Execute(attemptCtx, scenario)
		cancel()
		item := outcome{Scenario: scenario}
		if err != nil {
			failure := jobs.JobError{Code: "evaluation_failed", Message: "Investigation did not complete."}
			if typed, ok := err.(jobs.ExecutionFailure); ok {
				failure = typed.Failure()
			}
			item.Failure = &failure
		} else {
			item.Result = &result
			item.Checks = evaluation.Assess(scenario, result)
		}
		if item.Failure != nil || !item.Checks.Pass() {
			report.Passed = false
		}
		report.Outcomes = append(report.Outcomes, item)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		return 1
	}
	if !report.Passed {
		return 1
	}
	return 0
}
