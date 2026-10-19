package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/neev-rao/distributed-agent-service/internal/fixture"
	"github.com/neev-rao/distributed-agent-service/internal/jobs"
)

type providerFunc func(string) (Session, error)

func (f providerFunc) NewSession(prompt string) (Session, error) { return f(prompt) }

type sessionFunc func(context.Context, []ToolResult) (Turn, error)

func (f sessionFunc) Next(ctx context.Context, results []ToolResult) (Turn, error) {
	return f(ctx, results)
}

func call(id, name, arguments string) ToolCall {
	return ToolCall{ID: id, Name: name, Arguments: json.RawMessage(arguments)}
}

func executorFor(t *testing.T, config Config, turns ...Turn) *Executor {
	t.Helper()
	e, err := New(config, providerFunc(func(string) (Session, error) {
		index := 0
		return sessionFunc(func(context.Context, []ToolResult) (Turn, error) {
			if index >= len(turns) {
				t.Fatal("unexpected extra model call")
			}
			turn := turns[index]
			index++
			return turn, nil
		}), nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func execute(t *testing.T, e *Executor) (jobs.Result, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return e.Execute(ctx, fixture.ScenarioID)
}

func assertCode(t *testing.T, err error, want string) {
	t.Helper()
	var classified *Error
	if !errors.As(err, &classified) || classified.Failure().Code != want {
		t.Fatalf("got error %v, want code %s", err, want)
	}
}

func TestInvestigationReturnsOnlyRetrievedEvidence(t *testing.T) {
	var prompt string
	e, err := New(DefaultConfig(), providerFunc(func(input string) (Session, error) {
		prompt = input
		turn := 0
		return sessionFunc(func(ctx context.Context, results []ToolResult) (Turn, error) {
			if _, ok := ctx.Deadline(); !ok {
				t.Fatal("provider call missing deadline")
			}
			turn++
			if turn == 1 {
				if len(results) != 0 {
					t.Fatal("first turn has unexpected results")
				}
				return Turn{Calls: []ToolCall{call("m", "read_metrics", `{"service":"checkout"}`), call("l", "read_logs", `{"service":"checkout"}`)}}, nil
			}
			if len(results) != 2 || results[0].ID != "m" || len(results[0].Evidence) != 2 || results[1].ID != "l" {
				t.Fatalf("tool correlation/evidence lost: %+v", results)
			}
			return Turn{Result: &jobs.Result{Diagnosis: "Database pool is exhausted; why connections are held is not established.", EvidenceIDs: []string{"metrics.database.connections", "logs.checkout.db-timeout"}}}, nil
		}), nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	result, err := execute(t, e)
	if err != nil || !strings.Contains(result.Diagnosis, "exhausted") {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	if strings.Contains(prompt, fixture.ScenarioID) || strings.Contains(prompt, "All 20") || strings.Contains(prompt, "0.2%") {
		t.Fatal("prompt leaked scenario identifier or expected evidence")
	}
}

func TestInvalidActions(t *testing.T) {
	valid := call("m", "read_metrics", `{"service":"checkout"}`)
	tests := map[string]Turn{
		"no progress":           {},
		"mixed final and reads": {Result: &jobs.Result{}, Calls: []ToolCall{valid}},
		"unknown tool":          {Calls: []ToolCall{call("m", "run_shell", `{"service":"checkout"}`)}},
		"missing argument":      {Calls: []ToolCall{call("m", "read_metrics", `{}`)}},
		"wrong service":         {Calls: []ToolCall{call("m", "read_metrics", `{"service":"billing"}`)}},
		"extra field":           {Calls: []ToolCall{call("m", "read_metrics", `{"service":"checkout","command":"delete"}`)}},
		"duplicate argument":    {Calls: []ToolCall{call("m", "read_metrics", `{"service":"checkout","service":"checkout"}`)}},
		"trailing input":        {Calls: []ToolCall{call("m", "read_metrics", `{"service":"checkout"} {}`)}},
		"null input":            {Calls: []ToolCall{call("m", "read_metrics", `null`)}},
		"null argument":         {Calls: []ToolCall{call("m", "read_metrics", `{"service":null}`)}},
		"array input":           {Calls: []ToolCall{call("m", "read_metrics", `["checkout"]`)}},
		"missing call id":       {Calls: []ToolCall{call("", "read_metrics", `{"service":"checkout"}`)}},
		"control call id":       {Calls: []ToolCall{call("m\n", "read_metrics", `{"service":"checkout"}`)}},
		"duplicate batch id":    {Calls: []ToolCall{valid, valid}},
		"oversize arguments":    {Calls: []ToolCall{call("m", "read_metrics", strings.Repeat(" ", 1025))}},
		"negative usage":        {Calls: []ToolCall{valid}, InputTokens: -1},
	}
	for name, turn := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := execute(t, executorFor(t, DefaultConfig(), turn))
			assertCode(t, err, "agent_invalid_action")
		})
	}
}

func TestDuplicateCallIDAcrossTurnsRejected(t *testing.T) {
	turn := Turn{Calls: []ToolCall{call("m", "read_metrics", `{"service":"checkout"}`)}}
	_, err := execute(t, executorFor(t, DefaultConfig(), turn, turn))
	assertCode(t, err, "agent_invalid_action")
}

func TestUnsupportedFinalResults(t *testing.T) {
	first := Turn{Calls: []ToolCall{call("m", "read_metrics", `{"service":"checkout"}`)}}
	for name, result := range map[string]jobs.Result{
		"invented citation":   {Diagnosis: "cause", EvidenceIDs: []string{"invented"}},
		"unread log citation": {Diagnosis: "cause", EvidenceIDs: []string{"logs.checkout.db-timeout"}},
		"duplicate citation":  {Diagnosis: "cause", EvidenceIDs: []string{"metrics.checkout.errors", "metrics.checkout.errors"}},
		"no citations":        {Diagnosis: "cause"},
		"blank diagnosis":     {Diagnosis: " \n\t", EvidenceIDs: []string{"metrics.checkout.errors"}},
		"oversize diagnosis":  {Diagnosis: strings.Repeat("x", (8<<10)+1), EvidenceIDs: []string{"metrics.checkout.errors"}},
		"invalid unicode":     {Diagnosis: string([]byte{0xff}), EvidenceIDs: []string{"metrics.checkout.errors"}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := execute(t, executorFor(t, DefaultConfig(), first, Turn{Result: &result}))
			assertCode(t, err, "agent_invalid_result")
		})
	}
	_, err := execute(t, executorFor(t, DefaultConfig(), Turn{Result: &jobs.Result{Diagnosis: "No observations", EvidenceIDs: []string{"metrics.checkout.errors"}}}))
	assertCode(t, err, "agent_invalid_result")
}

func TestBudgets(t *testing.T) {
	t.Run("tool calls", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.MaxToolCalls = 1
		_, err := execute(t, executorFor(t, cfg, Turn{Calls: []ToolCall{call("m", "read_metrics", `{"service":"checkout"}`), call("l", "read_logs", `{"service":"checkout"}`)}}))
		assertCode(t, err, "agent_budget_exhausted")
	})
	t.Run("model turns", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.MaxModelCalls = 2
		_, err := execute(t, executorFor(t, cfg,
			Turn{Calls: []ToolCall{call("m", "read_metrics", `{"service":"checkout"}`)}},
			Turn{Calls: []ToolCall{call("l", "read_logs", `{"service":"checkout"}`)}}))
		assertCode(t, err, "agent_budget_exhausted")
	})
	t.Run("cumulative tool bytes", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.MaxToolOutputBytes = 500
		_, err := execute(t, executorFor(t, cfg,
			Turn{Calls: []ToolCall{call("m", "read_metrics", `{"service":"checkout"}`)}},
			Turn{Calls: []ToolCall{call("m2", "read_metrics", `{"service":"checkout"}`)}}))
		assertCode(t, err, "agent_budget_exhausted")
	})
	t.Run("result bytes", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.MaxResultBytes = 16
		_, err := execute(t, executorFor(t, cfg,
			Turn{Calls: []ToolCall{call("m", "read_metrics", `{"service":"checkout"}`)}},
			Turn{Result: &jobs.Result{Diagnosis: "Too large for this small limit", EvidenceIDs: []string{"metrics.checkout.errors"}}}))
		assertCode(t, err, "agent_invalid_result")
	})
}

func TestProviderErrorsAreSanitizedAndCancellationPropagates(t *testing.T) {
	t.Run("provider failure", func(t *testing.T) {
		e, err := New(DefaultConfig(), providerFunc(func(string) (Session, error) { return nil, errors.New("https://private?key=secret") }))
		if err != nil {
			t.Fatal(err)
		}
		_, err = execute(t, e)
		assertCode(t, err, "provider_failure")
		if strings.Contains(err.Error(), "secret") {
			t.Fatal("provider failure leaked secret")
		}
	})
	t.Run("call timeout", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.CallTimeout = 5 * time.Millisecond
		e, err := New(cfg, providerFunc(func(string) (Session, error) {
			return sessionFunc(func(ctx context.Context, _ []ToolResult) (Turn, error) { <-ctx.Done(); return Turn{}, ctx.Err() }), nil
		}))
		if err != nil {
			t.Fatal(err)
		}
		_, err = execute(t, e)
		assertCode(t, err, "provider_timeout")
	})
	t.Run("parent cancellation", func(t *testing.T) {
		entered := make(chan struct{})
		e, err := New(DefaultConfig(), providerFunc(func(string) (Session, error) {
			return sessionFunc(func(ctx context.Context, _ []ToolResult) (Turn, error) {
				close(entered)
				<-ctx.Done()
				return Turn{}, ctx.Err()
			}), nil
		}))
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		go func() { <-entered; cancel() }()
		_, err = e.Execute(ctx, fixture.ScenarioID)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v, want cancellation", err)
		}
	})
	t.Run("late response rejected", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.CallTimeout = time.Millisecond
		e, err := New(cfg, providerFunc(func(string) (Session, error) {
			return sessionFunc(func(ctx context.Context, _ []ToolResult) (Turn, error) {
				<-ctx.Done()
				return Turn{Calls: []ToolCall{call("m", "read_metrics", `{"service":"checkout"}`)}}, nil
			}), nil
		}))
		if err != nil {
			t.Fatal(err)
		}
		_, err = execute(t, e)
		assertCode(t, err, "provider_timeout")
	})
}

func TestIndependentConcurrentSessions(t *testing.T) {
	var mu sync.Mutex
	created := 0
	e, err := New(DefaultConfig(), providerFunc(func(string) (Session, error) {
		mu.Lock()
		created++
		id := created
		mu.Unlock()
		turn := 0
		return sessionFunc(func(ctx context.Context, results []ToolResult) (Turn, error) {
			turn++
			if turn == 1 {
				return Turn{Calls: []ToolCall{call("m", "read_metrics", `{"service":"checkout"}`)}}, nil
			}
			if len(results) != 1 {
				return Turn{}, errors.New("session contamination")
			}
			return Turn{Result: &jobs.Result{Diagnosis: fmt.Sprintf("Investigation %d", id), EvidenceIDs: []string{"metrics.checkout.errors"}}}, nil
		}), nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := execute(t, e); errs <- err }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if created != 8 {
		t.Fatalf("created %d sessions, want 8", created)
	}
}

func TestRequiresDeadlineAndKnownScenario(t *testing.T) {
	e := executorFor(t, DefaultConfig())
	_, err := e.Execute(context.Background(), fixture.ScenarioID)
	assertCode(t, err, "agent_configuration")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = e.Execute(ctx, "unknown")
	assertCode(t, err, "agent_configuration")
}

func TestFingerprintAndToolDefinitionsStableAndIsolated(t *testing.T) {
	first := Fingerprint("model-a", DefaultConfig())
	if first != Fingerprint("model-a", DefaultConfig()) || first == Fingerprint("model-b", DefaultConfig()) {
		t.Fatal("profile identity not stable or model sensitive")
	}
	cfg := DefaultConfig()
	cfg.MaxToolCalls--
	if first == Fingerprint("model-a", cfg) {
		t.Fatal("profile ignored changed budget")
	}
	tools := Tools()
	tools[0].Parameters[0] = 'x'
	if !json.Valid(Tools()[0].Parameters) {
		t.Fatal("caller mutated shared schema")
	}
}

func TestConstructorRejectsUnboundedConfiguration(t *testing.T) {
	provider := providerFunc(func(string) (Session, error) { return nil, nil })
	for _, modify := range []func(*Config){
		func(c *Config) { c.MaxModelCalls = 0 }, func(c *Config) { c.MaxModelCalls = 5 },
		func(c *Config) { c.MaxToolCalls = 7 }, func(c *Config) { c.CallTimeout = 11 * time.Second },
		func(c *Config) { c.MaxToolOutputBytes = 17 << 10 }, func(c *Config) { c.MaxResultBytes = 65 << 10 },
	} {
		cfg := DefaultConfig()
		modify(&cfg)
		if _, err := New(cfg, provider); err == nil {
			t.Fatal("unbounded configuration accepted")
		}
	}
	if _, err := New(DefaultConfig(), nil); err == nil {
		t.Fatal("nil provider accepted")
	}
}
