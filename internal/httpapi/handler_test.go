package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/neev-rao/distributed-agent-service/internal/fixture"
	"github.com/neev-rao/distributed-agent-service/internal/jobs"
)

const validBody = `{"scenario_id":"checkout-db-timeout"}`

func newTestManager(t *testing.T, config jobs.Config, executor jobs.Executor) *jobs.Manager {
	t.Helper()
	config.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	manager, err := jobs.New(config, executor)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := manager.Shutdown(ctx); err != nil {
			t.Errorf("manager cleanup: %v", err)
		}
	})
	return manager
}

func call(handler http.Handler, method, path, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func assertError(t *testing.T, response *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	if response.Code != status {
		t.Fatalf("status=%d want=%d body=%s", response.Code, status, response.Body.String())
	}
	if response.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("unexpected content type %q", response.Header().Get("Content-Type"))
	}
	var body errorBody
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil || body.Error.Code != code || body.Error.Message == "" {
		t.Fatalf("invalid error response: body=%s err=%v", response.Body.String(), err)
	}
}

func TestSubmitAndPollOverHTTP(t *testing.T) {
	manager := newTestManager(t, jobs.DefaultConfig(), fixture.Executor{})
	server := httptest.NewServer(New(manager))
	t.Cleanup(server.Close)
	client := server.Client()
	response, err := client.Post(server.URL+"/jobs", "application/json", strings.NewReader(validBody))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var accepted struct {
		ID        string `json:"id"`
		StatusURL string `json:"status_url"`
	}
	if err := json.NewDecoder(response.Body).Decode(&accepted); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusAccepted || accepted.ID == "" || accepted.StatusURL != "/jobs/"+accepted.ID || response.Header.Get("Location") != accepted.StatusURL {
		t.Fatalf("invalid acceptance: status=%d body=%+v headers=%v", response.StatusCode, accepted, response.Header)
	}
	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		poll, err := client.Get(server.URL + accepted.StatusURL)
		if err != nil {
			t.Fatal(err)
		}
		var job jobs.Job
		err = json.NewDecoder(poll.Body).Decode(&job)
		poll.Body.Close()
		if err != nil || poll.StatusCode != http.StatusOK {
			t.Fatalf("poll status=%d err=%v", poll.StatusCode, err)
		}
		if job.State == jobs.Succeeded {
			if job.ID != accepted.ID || job.Result == nil || job.Result.Diagnosis == "" || len(job.Result.EvidenceIDs) == 0 || job.Error != nil || job.StartedAt == nil || job.FinishedAt == nil {
				t.Fatalf("invalid successful job: %+v", job)
			}
			return
		}
		if job.State == jobs.Failed {
			t.Fatalf("fixture failed: %+v", job)
		}
		select {
		case <-deadline:
			t.Fatal("job did not finish")
		case <-ticker.C:
		}
	}
}

func TestInvalidSubmissionsAndRoutes(t *testing.T) {
	manager := newTestManager(t, jobs.DefaultConfig(), fixture.Executor{})
	handler := New(manager)
	tests := []struct {
		name, method, path, body string
		status                   int
		code, allow              string
	}{
		{"empty", "POST", "/jobs", "", 400, "invalid_request", ""},
		{"malformed", "POST", "/jobs", "{", 400, "invalid_request", ""},
		{"unknown field", "POST", "/jobs", `{"scenario_id":"checkout-db-timeout","extra":1}`, 400, "invalid_request", ""},
		{"wrong type", "POST", "/jobs", `{"scenario_id":1}`, 400, "invalid_request", ""},
		{"array", "POST", "/jobs", "[]", 400, "invalid_request", ""},
		{"second object", "POST", "/jobs", validBody + " {}", 400, "invalid_request", ""},
		{"trailing garbage", "POST", "/jobs", validBody + " garbage", 400, "invalid_request", ""},
		{"missing scenario", "POST", "/jobs", "{}", 400, "unknown_scenario", ""},
		{"unknown scenario", "POST", "/jobs", `{"scenario_id":"missing"}`, 400, "unknown_scenario", ""},
		{"evaluation-only payment", "POST", "/jobs", `{"scenario_id":"checkout-payment-timeout"}`, 400, "unknown_scenario", ""},
		{"evaluation-only conflict", "POST", "/jobs", `{"scenario_id":"checkout-conflicting-evidence"}`, 400, "unknown_scenario", ""},
		{"null", "POST", "/jobs", "null", 400, "unknown_scenario", ""},
		{"too large", "POST", "/jobs", strings.Repeat(" ", int(MaxRequestBytes)+1), 413, "request_too_large", ""},
		{"oversized suffix", "POST", "/jobs", validBody + strings.Repeat(" ", int(MaxRequestBytes)), 413, "request_too_large", ""},
		{"unknown job", "GET", "/jobs/missing", "", 404, "job_not_found", ""},
		{"unknown route", "GET", "/missing", "", 404, "not_found", ""},
		{"empty ID", "GET", "/jobs/", "", 404, "not_found", ""},
		{"extra path", "GET", "/jobs/a/b", "", 404, "not_found", ""},
		{"collection method", "GET", "/jobs", "", 405, "method_not_allowed", "POST"},
		{"job method", "DELETE", "/jobs/missing", "", 405, "method_not_allowed", "GET"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := call(handler, test.method, test.path, test.body)
			assertError(t, response, test.status, test.code)
			if response.Header().Get("Allow") != test.allow {
				t.Fatalf("Allow=%q want=%q", response.Header().Get("Allow"), test.allow)
			}
		})
	}
	// Exactly 16 KiB is accepted; only exceeding the whole-body limit is 413.
	response := call(handler, "POST", "/jobs", validBody+strings.Repeat(" ", int(MaxRequestBytes)-len(validBody)))
	if response.Code != http.StatusAccepted {
		t.Fatalf("boundary body rejected: status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestQueueSaturationDoesNotCreateWork(t *testing.T) {
	started := make(chan struct{}, 3)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	executor := jobs.ExecutorFunc(func(ctx context.Context, _ string) (jobs.Result, error) {
		started <- struct{}{}
		select {
		case <-release:
			return jobs.Result{Diagnosis: "done"}, nil
		case <-ctx.Done():
			return jobs.Result{}, ctx.Err()
		}
	})
	config := jobs.DefaultConfig()
	config.Workers, config.QueueCapacity = 1, 1
	manager := newTestManager(t, config, executor)
	defer unblock()
	handler := New(manager)
	if response := call(handler, "POST", "/jobs", validBody); response.Code != 202 {
		t.Fatalf("first submission: %s", response.Body.String())
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("worker did not start")
	}
	if response := call(handler, "POST", "/jobs", validBody); response.Code != 202 {
		t.Fatalf("waiting submission: %s", response.Body.String())
	}
	response := call(handler, "POST", "/jobs", validBody)
	assertError(t, response, 503, "queue_full")
	if response.Header().Get("Retry-After") != "1" || response.Header().Get("Location") != "" {
		t.Fatalf("unexpected overload headers: %v", response.Header())
	}
	unblock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := manager.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if len(started) != 1 { // The first execution's start notification was consumed above.
		t.Fatalf("rejected submission executed: remaining execution starts=%d", len(started))
	}
}

func TestExecutionFailureIsInspectableOverHTTP(t *testing.T) {
	manager := newTestManager(t, jobs.DefaultConfig(), jobs.ExecutorFunc(func(context.Context, string) (jobs.Result, error) {
		return jobs.Result{}, errors.New("synthetic dependency failure")
	}))
	handler := New(manager)
	response := call(handler, "POST", "/jobs", validBody)
	if response.Code != 202 {
		t.Fatalf("submission failed: %s", response.Body.String())
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := manager.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	result := call(handler, "GET", response.Header().Get("Location"), "")
	var job jobs.Job
	if err := json.Unmarshal(result.Body.Bytes(), &job); err != nil {
		t.Fatal(err)
	}
	if result.Code != 200 || job.State != jobs.Failed || job.Error == nil || job.Error.Code != "execution_failed" || job.Result != nil {
		t.Fatalf("failure not inspectable: status=%d body=%s", result.Code, result.Body.String())
	}
}

func TestRecordLimitAndShutdownAreInspectable(t *testing.T) {
	config := jobs.DefaultConfig()
	config.MaxRecords = 1
	manager := newTestManager(t, config, fixture.Executor{})
	handler := New(manager)
	if response := call(handler, "POST", "/jobs", validBody); response.Code != 202 {
		t.Fatalf("first submission: %s", response.Body.String())
	}
	response := call(handler, "POST", "/jobs", validBody)
	assertError(t, response, 503, "record_limit")
	if !strings.Contains(response.Body.String(), "restart") || response.Header().Get("Retry-After") == "" {
		t.Fatalf("record limit not actionable: %s", response.Body.String())
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := manager.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	response = call(handler, "POST", "/jobs", validBody)
	assertError(t, response, 503, "service_shutting_down")
}

func TestAcceptedJobOutlivesCancelledRequest(t *testing.T) {
	started := make(chan error, 1)
	manager := newTestManager(t, jobs.DefaultConfig(), jobs.ExecutorFunc(func(ctx context.Context, _ string) (jobs.Result, error) {
		started <- ctx.Err()
		return jobs.Result{Diagnosis: "request-independent"}, nil
	}))
	requestCtx, cancel := context.WithCancel(context.Background())
	cancel()
	request := httptest.NewRequest("POST", "/jobs", strings.NewReader(validBody)).WithContext(requestCtx)
	response := httptest.NewRecorder()
	New(manager).ServeHTTP(response, request)
	if response.Code != 202 {
		t.Fatalf("submission failed: %s", response.Body.String())
	}
	select {
	case err := <-started:
		if err != nil {
			t.Fatalf("request cancellation reached executor: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("accepted job did not execute")
	}
}
