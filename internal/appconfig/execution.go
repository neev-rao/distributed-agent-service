package appconfig

import (
	"errors"
	"flag"
	"os"
	"regexp"
	"time"

	"github.com/neev-rao/distributed-agent-service/internal/agent"
	"github.com/neev-rao/distributed-agent-service/internal/durable"
)

// Execution selects one immutable executor profile for the database's worker
// pool. API and database-tool processes need the profile, but never a model key.
type Execution struct {
	Kind  string
	Model string
}

var modelName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

func RegisterExecution(fs *flag.FlagSet) *Execution {
	e := &Execution{}
	fs.StringVar(&e.Kind, "executor", "fixture", "worker pool executor: fixture or gemini")
	fs.StringVar(&e.Model, "model", os.Getenv("GEMINI_MODEL"), "explicit Gemini model ID (or GEMINI_MODEL)")
	return e
}

// Configure applies mode defaults only when the operator did not explicitly set
// a flag. Migrate subsequently verifies the resulting policy against PostgreSQL;
// this never upgrades an existing database's timeout or executor implicitly.
func (e Execution) Configure(fs *flag.FlagSet, cfg *durable.Config) error {
	switch e.Kind {
	case "fixture":
		if WasSet(fs, "model") {
			return errors.New("model requires the gemini executor")
		}
		cfg.ExecutionProfile = "fixture-v1"
	case "gemini":
		if !modelName.MatchString(e.Model) {
			return errors.New("gemini requires an explicit valid model ID via -model or GEMINI_MODEL")
		}
		if !WasSet(fs, "execution-timeout") {
			cfg.ExecutionTimeout = 30 * time.Second
		}
		cfg.ExecutionProfile = GeminiProfile(e.Model)
	default:
		return errors.New("executor must be fixture or gemini")
	}
	return nil
}

// The adapter version pins its fixed transport and generation limits as well as
// the model, prompt, tools, and runtime limits in the fingerprint.
func GeminiProfile(model string) string {
	return "gemini-rest-v1:" + agent.Fingerprint(model, agent.DefaultConfig())
}

func WasSet(fs *flag.FlagSet, name string) bool {
	found := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}
