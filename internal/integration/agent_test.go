package integration_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/neev-rao/distributed-agent-service/internal/agent"
	"github.com/neev-rao/distributed-agent-service/internal/appconfig"
	"github.com/neev-rao/distributed-agent-service/internal/durable"
	"github.com/neev-rao/distributed-agent-service/internal/fixture"
	"github.com/neev-rao/distributed-agent-service/internal/gemini"
	"github.com/neev-rao/distributed-agent-service/internal/httpapi"
	"github.com/neev-rao/distributed-agent-service/internal/jobs"
	"github.com/neev-rao/distributed-agent-service/internal/worker"
)

type database struct {
	store *durable.Store
	pool  *pgxpool.Pool
	ctx   context.Context
}

type lockedBuffer struct {
	mu sync.Mutex
	bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.Write(p)
}
func (b *lockedBuffer) String() string { b.mu.Lock(); defer b.mu.Unlock(); return b.Buffer.String() }

func testDatabase(t *testing.T) database {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set TEST_DATABASE_URL for the agent pipeline integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	var random [12]byte
	if _, err = rand.Read(random[:]); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	schema := "agent_test_" + hex.EncodeToString(random[:])
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := admin.Exec(cleanup, "DROP SCHEMA "+quoted+" CASCADE"); err != nil {
			t.Error(err)
		}
		admin.Close()
	})
	scoped := dsn + " search_path=" + schema
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		parsed, err := url.Parse(dsn)
		if err != nil {
			t.Fatal(err)
		}
		query := parsed.Query()
		query.Set("search_path", schema)
		parsed.RawQuery = query.Encode()
		scoped = parsed.String()
	}
	pool, err := pgxpool.New(ctx, scoped)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	cfg := durable.DefaultConfig()
	cfg.ExecutionTimeout = 30 * time.Second
	cfg.ExecutionProfile = appconfig.GeminiProfile("test-model")
	store, err := durable.Open(ctx, scoped, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	if err = store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return database{store, pool, ctx}
}

type redirectTransport struct {
	target *url.URL
	base   http.RoundTripper
}

func (r redirectTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	copy := request.Clone(request.Context())
	location := *request.URL
	location.Scheme = r.target.Scheme
	location.Host = r.target.Host
	copy.URL = &location
	copy.Host = r.target.Host
	return r.base.RoundTrip(copy)
}

func testExecutor(t *testing.T, handler http.Handler, logger *slog.Logger) *agent.Executor {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	target, _ := url.Parse(server.URL)
	provider, err := gemini.New(gemini.Config{APIKey: "test-key-not-a-credential", Model: "test-model",
		HTTPClient: &http.Client{Transport: redirectTransport{target, http.DefaultTransport}}})
	if err != nil {
		t.Fatal(err)
	}
	cfg := agent.DefaultConfig()
	cfg.Logger = logger
	executor, err := agent.New(cfg, provider)
	if err != nil {
		t.Fatal(err)
	}
	return executor
}

func startWorker(t *testing.T, db database, executor jobs.Executor) context.CancelFunc {
	t.Helper()
	cfg := worker.DefaultConfig()
	cfg.WorkerID = "agent-integration"
	cfg.PollInterval = 10 * time.Millisecond
	cfg.RenewInterval = 25 * time.Millisecond
	cfg.ShutdownGrace = time.Second
	runtime, err := worker.New(cfg, db.store, executor)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(db.ctx)
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("worker did not stop")
		}
	})
	return cancel
}

func respond(w http.ResponseWriter, calls ...map[string]any) {
	parts := make([]map[string]any, 0, len(calls))
	for _, call := range calls {
		parts = append(parts, map[string]any{"functionCall": call})
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"candidates": []any{map[string]any{
		"content": map[string]any{"role": "model", "parts": parts}, "finishReason": "STOP"}},
		"usageMetadata": map[string]int{"promptTokenCount": 10, "candidatesTokenCount": 10}})
}

func submit(t *testing.T, base, scenario string) jobs.Job {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, base+"/jobs", strings.NewReader(`{"scenario_id":"`+scenario+`"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "integration-request")
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var accepted struct {
		ID  string `json:"id"`
		URL string `json:"status_url"`
	}
	if err = json.NewDecoder(response.Body).Decode(&accepted); err != nil || response.StatusCode != 202 {
		t.Fatalf("submit: status%d err%v", response.StatusCode, err)
	}
	return jobs.Job{ID: accepted.ID}
}

func terminal(t *testing.T, base, id string) jobs.Job {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		response, err := http.Get(base + "/jobs/" + id)
		if err != nil {
			t.Fatal(err)
		}
		var job jobs.Job
		err = json.NewDecoder(response.Body).Decode(&job)
		response.Body.Close()
		if err != nil || response.StatusCode != 200 {
			t.Fatalf("lookup: %d %v", response.StatusCode, err)
		}
		if job.State == jobs.Succeeded || job.State == jobs.Failed {
			return job
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("job did not finish")
	return jobs.Job{}
}

// Tests run the actual HTTP handler, PostgreSQL store, worker, agent runtime,
// and Gemini transport. Only Google's server is replaced with a controlled HTTP
// fixture; normal tests never contact a live provider.
func TestAgentPipeline(t *testing.T) {
	for _, scenario := range []string{fixture.ScenarioID, fixture.InsufficientScenarioID, "provider-outage"} {
		t.Run(scenario, func(t *testing.T) {
			db := testDatabase(t)
			var calls atomic.Int32
			var logs lockedBuffer
			logger := slog.New(slog.NewJSONHandler(&logs, nil))
			executor := testExecutor(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				count := calls.Add(1)
				if r.Header.Get("x-goog-api-key") != "test-key-not-a-credential" {
					t.Error("missing provider authentication")
				}
				if scenario == "provider-outage" {
					w.WriteHeader(429)
					_, _ = io.WriteString(w, "private provider failure")
					return
				}
				body, _ := io.ReadAll(r.Body)
				if count == 1 {
					respond(w, map[string]any{"id": "m", "name": "read_metrics", "args": map[string]string{"service": "checkout"}},
						map[string]any{"id": "l", "name": "read_logs", "args": map[string]string{"service": "checkout"}})
					return
				}
				if count != 2 || !bytes.Contains(body, []byte("functionResponse")) {
					t.Error("tool observations missing from model conversation")
				}
				diagnosis := "Connection pool exhaustion causes checkout timeouts; the underlying reason for occupied connections is unknown."
				ids := []string{"metrics.database.connections", "logs.checkout.db-timeout"}
				if scenario == fixture.InsufficientScenarioID {
					if bytes.Contains(body, []byte("All 20 database connections")) {
						t.Error("hidden diagnostic signal leaked")
					}
					diagnosis = "Checkout errors increased, but insufficient evidence prevents identifying the cause."
					ids = []string{"metrics.checkout.errors", "logs.checkout.unavailable"}
				}
				respond(w, map[string]any{"id": "f", "name": "finish_investigation", "args": map[string]any{"diagnosis": diagnosis, "evidence_ids": ids}})
			}), logger)
			api := httptest.NewServer(httpapi.NewWithBackend(db.store))
			t.Cleanup(api.Close)
			cancel := startWorker(t, db, executor)
			input := scenario
			if input == "provider-outage" {
				input = fixture.ScenarioID
			}
			accepted := submit(t, api.URL, input)
			job := terminal(t, api.URL, accepted.ID)
			if scenario == "provider-outage" {
				if job.State != jobs.Failed || job.Error == nil || job.Error.Code != "provider_failure" || calls.Load() != 1 {
					t.Fatalf("provider failure was not terminal and inspectable: %+v calls%d", job, calls.Load())
				}
			} else if job.State != jobs.Succeeded || job.Result == nil || len(job.Result.EvidenceIDs) != 2 || calls.Load() != 2 {
				t.Fatalf("unexpected investigation result: %+v calls%d", job, calls.Load())
			}
			if replayed := submit(t, api.URL, input); replayed.ID != accepted.ID {
				t.Fatal("model submission replay created another job")
			}
			cancel()
			text := logs.String()
			if !strings.Contains(text, accepted.ID) || !strings.Contains(text, "attempt_token") || strings.Contains(text, "test-key-not-a-credential") || strings.Contains(text, "private provider failure") {
				t.Fatalf("missing correlation or leaked sensitive content: %s", text)
			}
		})
	}
}

func TestLeaseLossCancelsAgentProviderRequest(t *testing.T) {
	db := testDatabase(t)
	started, canceled := make(chan struct{}), make(chan struct{})
	executor := testExecutor(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-started:
		default:
			close(started)
		}
		<-r.Context().Done()
		select {
		case <-canceled:
		default:
			close(canceled)
		}
	}), nil)
	api := httptest.NewServer(httpapi.NewWithBackend(db.store))
	t.Cleanup(api.Close)
	cancel := startWorker(t, db, executor)
	accepted := submit(t, api.URL, fixture.ScenarioID)
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("provider was not called")
	}
	if _, err := db.pool.Exec(db.ctx, "UPDATE jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", accepted.ID); err != nil {
		t.Fatal(err)
	}
	select {
	case <-canceled:
		cancel()
	case <-time.After(3 * time.Second):
		t.Fatal("lease loss did not cancel provider request")
	}
}
