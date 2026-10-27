// evaluate makes explicitly opted-in, potentially billable model calls. It
// evaluates model behavior separately from durable job execution reliability.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/neev-rao/distributed-agent-service/internal/agent"
	"github.com/neev-rao/distributed-agent-service/internal/appconfig"
	"github.com/neev-rao/distributed-agent-service/internal/evaluation"
	"github.com/neev-rao/distributed-agent-service/internal/fixture"
	"github.com/neev-rao/distributed-agent-service/internal/gemini"
	"github.com/neev-rao/distributed-agent-service/internal/jobs"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr, liveExecutor)
	stop()
	os.Exit(code)
}

func liveExecutor(model string) (jobs.Executor, error) {
	provider, err := gemini.New(gemini.Config{APIKey: os.Getenv("GEMINI_API_KEY"), Model: model})
	if err != nil {
		return nil, err
	}
	return agent.New(agent.DefaultConfig(), provider)
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer, newExecutor func(string) (jobs.Executor, error)) int {
	fs := flag.NewFlagSet("evaluate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	live := fs.Bool("live", false, "explicitly permit live Gemini API requests (may incur charges)")
	model := fs.String("model", os.Getenv("GEMINI_MODEL"), "explicit Gemini model ID (or GEMINI_MODEL)")
	runs := fs.Int("runs", 1, "independent runs per selected scenario (1 to 10; no retries)")
	scenario := fs.String("scenario", "", "evaluate one scenario ID; default is all four")
	list := fs.Bool("list", false, "list scenarios and review expectations without provider access")
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 1
	}
	if fs.NArg() != 0 {
		fmt.Fprintln(stderr, "Unexpected positional arguments; use -help for options.")
		return 1
	}
	if *list {
		return writeJSON(stdout, evaluation.Cases())
	}
	if !*live {
		fmt.Fprintln(stderr, "Live evaluation requires explicit -live; normal tests make no provider requests.")
		return 1
	}
	scenarios := fixture.EvaluationScenarios()
	if *scenario != "" {
		scenarios = []string{*scenario}
	}
	cfg := evaluation.Config{Model: *model, Profile: appconfig.GeminiProfile(*model), Scenarios: scenarios, Runs: *runs}
	if err := cfg.Validate(); err != nil {
		fmt.Fprintln(stderr, "Select a model, 1 to 10 runs, and a known scenario from -list.")
		return 1
	}
	executor, err := newExecutor(*model)
	if err != nil {
		fmt.Fprintln(stderr, "Set GEMINI_API_KEY and a valid GEMINI_MODEL locally before live evaluation.")
		return 1
	}
	fmt.Fprintf(stderr, "Evaluating %d investigations sequentially; up to %d model requests. JSON follows when finished or canceled.\n", len(scenarios)*cfg.Runs, len(scenarios)*cfg.Runs*agent.DefaultConfig().MaxModelCalls)
	report, err := evaluation.Run(ctx, cfg, executor)
	if err != nil {
		fmt.Fprintln(stderr, "Invalid evaluation configuration.")
		return 1
	}
	if writeJSON(stdout, report) != 0 || !report.Passed {
		return 1
	}
	return 0
}

func writeJSON(w io.Writer, value any) int {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		return 1
	}
	return 0
}
