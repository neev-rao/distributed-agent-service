package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/neev-rao/distributed-agent-service/internal/durable"
	"github.com/neev-rao/distributed-agent-service/internal/jobs"
)

type fakeBackend struct {
	submit func(context.Context, string, string) (jobs.Job, error)
	get    func(context.Context, string) (jobs.Job, bool, error)
}

func (f fakeBackend) Submit(ctx context.Context, scenario, key string) (jobs.Job, error) {
	return f.submit(ctx, scenario, key)
}
func (f fakeBackend) Get(ctx context.Context, id string) (jobs.Job, bool, error) {
	return f.get(ctx, id)
}

func TestDurableSubmissionUsesIdempotencyKeyAndBoundedContext(t *testing.T) {
	backend := fakeBackend{submit: func(ctx context.Context, scenario, key string) (jobs.Job, error) {
		if scenario != "checkout-db-timeout" || key != "request-123" {
			t.Fatalf("unexpected submission %q %q", scenario, key)
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("storage operation has no deadline")
		}
		return jobs.Job{ID: "persisted-job"}, nil
	}}
	req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(validBody))
	req.Header.Set("Idempotency-Key", "request-123")
	response := httptest.NewRecorder()
	NewWithBackend(backend).ServeHTTP(response, req)
	if response.Code != 202 || response.Header().Get("Location") != "/jobs/persisted-job" {
		t.Fatalf("unexpected response %d %s", response.Code, response.Body.String())
	}
}

func TestInvalidIdempotencyKeysNeverReachStorage(t *testing.T) {
	for _, key := range []string{"", "has space", strings.Repeat("a", 129), "non-ascii-\u00e9"} {
		t.Run(key, func(t *testing.T) {
			backend := fakeBackend{submit: func(context.Context, string, string) (jobs.Job, error) {
				t.Fatal("invalid key reached storage")
				return jobs.Job{}, nil
			}}
			req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(validBody))
			req.Header.Set("Idempotency-Key", key)
			response := httptest.NewRecorder()
			NewWithBackend(backend).ServeHTTP(response, req)
			assertError(t, response, 400, "invalid_idempotency_key")
		})
	}
	req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(validBody))
	req.Header.Add("Idempotency-Key", "a")
	req.Header.Add("Idempotency-Key", "b")
	response := httptest.NewRecorder()
	NewWithBackend(fakeBackend{}).ServeHTTP(response, req)
	assertError(t, response, 400, "invalid_idempotency_key")
}

func TestDurableErrorsAreExplicitAndDoNotLeakDriverDetails(t *testing.T) {
	for _, tt := range []struct {
		err    error
		status int
		code   string
	}{
		{durable.ErrIdempotencyConflict, 409, "idempotency_conflict"},
		{errors.New("private-database-detail"), 503, "storage_unavailable"},
	} {
		backend := fakeBackend{submit: func(context.Context, string, string) (jobs.Job, error) { return jobs.Job{}, tt.err }}
		response := call(NewWithBackend(backend), http.MethodPost, "/jobs", validBody)
		assertError(t, response, tt.status, tt.code)
		if strings.Contains(response.Body.String(), "private-database-detail") {
			t.Fatal("driver details leaked")
		}
	}
	backend := fakeBackend{get: func(ctx context.Context, id string) (jobs.Job, bool, error) {
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("lookup has no deadline")
		}
		return jobs.Job{}, false, errors.New("offline")
	}}
	response := call(NewWithBackend(backend), http.MethodGet, "/jobs/a", "")
	assertError(t, response, 503, "storage_unavailable")
	if response.Header().Get("Retry-After") == "" {
		t.Fatal("missing retry hint")
	}
}

func TestLocalModeDoesNotSilentlyIgnoreIdempotencyKey(t *testing.T) {
	manager := newTestManager(t, jobs.DefaultConfig(), jobs.ExecutorFunc(func(context.Context, string) (jobs.Result, error) {
		t.Error("rejected local idempotent request executed")
		return jobs.Result{}, nil
	}))
	req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(validBody))
	req.Header.Set("Idempotency-Key", "key")
	response := httptest.NewRecorder()
	New(manager).ServeHTTP(response, req)
	assertError(t, response, 400, "idempotency_unavailable")
}
