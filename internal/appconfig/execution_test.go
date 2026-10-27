package appconfig

import (
	"flag"
	"io"
	"testing"
	"time"

	"github.com/neev-rao/distributed-agent-service/internal/durable"
)

func TestExecutionProfileConfiguration(t *testing.T) {
	t.Setenv("GEMINI_MODEL", "")
	configure := func(args ...string) (durable.Config, error) {
		fs := flag.NewFlagSet("test", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		cfg := durable.DefaultConfig()
		Register(fs, &cfg)
		execution := RegisterExecution(fs)
		if err := fs.Parse(args); err != nil {
			return cfg, err
		}
		err := execution.Configure(fs, &cfg)
		return cfg, err
	}
	fixture, err := configure()
	if err != nil || fixture.ExecutionProfile != "fixture-v1" || fixture.ExecutionTimeout != 5*time.Second {
		t.Fatalf("fixture policy changed: %+v %v", fixture, err)
	}
	agent, err := configure("-executor", "gemini", "-model", "test-model")
	if err != nil || agent.ExecutionTimeout != 30*time.Second || agent.ExecutionProfile == fixture.ExecutionProfile {
		t.Fatalf("agent defaults: %+v %v", agent, err)
	}
	other, err := configure("-executor", "gemini", "-model", "other-model", "-execution-timeout", "45s")
	if err != nil || other.ExecutionTimeout != 45*time.Second || other.ExecutionProfile == agent.ExecutionProfile {
		t.Fatalf("explicit policy/model identity: %+v %v", other, err)
	}
	for _, args := range [][]string{
		{"-executor", "unknown"}, {"-executor", "gemini"},
		{"-executor", "gemini", "-model", "../other?key=secret"},
		{"-model", "test-model"},
	} {
		if _, err := configure(args...); err == nil {
			t.Fatalf("invalid configuration accepted: %v", args)
		}
	}
}

// Evaluation-only observations do not alter existing durable jobs or their
// execution configuration. Expanding public admission would require versioning.
func TestEvaluationIncrementPreservesExistingWorkerProfile(t *testing.T) {
	const baseline = "gemini-rest-v1:661e0818599c67148aad4d0d22eca7131e4cc00f017767ae7d262c45b4c76174"
	if GeminiProfile("gemini-3.8-flash") != baseline {
		t.Fatal("existing durable profile changed; review compatibility before deployment")
	}
}
