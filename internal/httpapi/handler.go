// Package httpapi validates requests and translates job-manager outcomes to HTTP.
package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/neev-rao/distributed-agent-service/internal/durable"
	"github.com/neev-rao/distributed-agent-service/internal/fixture"
	"github.com/neev-rao/distributed-agent-service/internal/jobs"
)

const MaxRequestBytes int64 = 16 << 10

type handler struct {
	backend   Backend
	localMode bool
}

func New(manager *jobs.Manager) http.Handler {
	return &handler{backend: localBackend{manager}, localMode: true}
}

// Backend permits durable admission and lookup without coupling HTTP to workers.
type Backend interface {
	Submit(context.Context, string, string) (jobs.Job, error)
	Get(context.Context, string) (jobs.Job, bool, error)
}

func NewWithBackend(backend Backend) http.Handler {
	return &handler{backend: backend}
}

var errLocalIdempotency = errors.New("idempotency requires durable mode")

type localBackend struct{ manager *jobs.Manager }

func (b localBackend) Submit(_ context.Context, scenario, key string) (jobs.Job, error) {
	if key != "" {
		return jobs.Job{}, errLocalIdempotency
	}
	return b.manager.Submit(scenario)
}

func (b localBackend) Get(_ context.Context, id string) (jobs.Job, bool, error) {
	job, ok := b.manager.Get(id)
	return job, ok, nil
}

type errorBody struct {
	Error jobs.JobError `json:"error"`
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/jobs":
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Use POST to submit a job.")
			return
		}
		h.submit(w, r)
	case strings.HasPrefix(r.URL.Path, "/jobs/"):
		id := strings.TrimPrefix(r.URL.Path, "/jobs/")
		if id == "" || strings.Contains(id, "/") {
			writeError(w, http.StatusNotFound, "not_found", "No such endpoint.")
			return
		}
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Use GET to retrieve a job.")
			return
		}
		operationCtx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		job, found, err := h.backend.Get(operationCtx, id)
		if err != nil {
			writeUnavailable(w)
			return
		}
		if !found {
			writeError(w, http.StatusNotFound, "job_not_found", "No retained job with this ID exists.")
			return
		}
		writeJSON(w, http.StatusOK, job)
	default:
		writeError(w, http.StatusNotFound, "not_found", "No such endpoint.")
	}
}

func (h *handler) submit(w http.ResponseWriter, r *http.Request) {
	// Read the bounded whole body first so an oversized trailing payload cannot
	// be misclassified as a small, malformed request or accidentally admitted.
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxRequestBytes))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, "request_too_large", "The request body must be at most 16 KiB.")
		} else {
			writeError(w, http.StatusBadRequest, "invalid_request", "Could not read the request body.")
		}
		return
	}
	var input struct {
		ScenarioID string `json:"scenario_id"`
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "Provide one JSON object containing only scenario_id.")
		return
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		writeError(w, http.StatusBadRequest, "invalid_request", "Provide exactly one JSON object.")
		return
	}
	if !fixture.ValidScenario(input.ScenarioID) {
		writeError(w, http.StatusBadRequest, "unknown_scenario", "scenario_id must be checkout-db-timeout or checkout-insufficient-evidence.")
		return
	}
	keys := r.Header.Values("Idempotency-Key")
	key := r.Header.Get("Idempotency-Key")
	if len(keys) > 1 || (len(keys) > 0 && !validKey(key)) {
		writeError(w, http.StatusBadRequest, "invalid_idempotency_key", "Use a single Idempotency-Key of 1 to 128 visible ASCII characters without spaces.")
		return
	}
	operationCtx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	job, err := h.backend.Submit(operationCtx, input.ScenarioID, key)
	if err != nil {
		w.Header().Set("Retry-After", "1")
		switch {
		case errors.Is(err, jobs.ErrQueueFull):
			writeError(w, http.StatusServiceUnavailable, "queue_full", "All waiting slots are occupied; retry later.")
		case errors.Is(err, jobs.ErrRecordLimit):
			message := "The retained job limit is reached; clean up expired terminal jobs. Restarting the API does not release durable records."
			if h.localMode {
				message = "The retained job limit is reached; restart the local process to accept more work. Restarting loses all local jobs and results."
			}
			writeError(w, http.StatusServiceUnavailable, "record_limit", message)
		case errors.Is(err, jobs.ErrShuttingDown):
			writeError(w, http.StatusServiceUnavailable, "service_shutting_down", "The service is shutting down and no longer accepts jobs.")
		case errors.Is(err, durable.ErrIdempotencyConflict):
			w.Header().Del("Retry-After")
			writeError(w, http.StatusConflict, "idempotency_conflict", "This Idempotency-Key was already used for a different request.")
		case errors.Is(err, errLocalIdempotency):
			w.Header().Del("Retry-After")
			writeError(w, http.StatusBadRequest, "idempotency_unavailable", "Idempotency-Key requires durable mode.")
		default:
			// Commit acknowledgment can be ambiguous after a connection failure.
			// Clients should retry with the same key, not assume nothing happened.
			writeUnavailable(w)
		}
		return
	}
	location := "/jobs/" + job.ID
	w.Header().Set("Location", location)
	writeJSON(w, http.StatusAccepted, struct {
		ID        string `json:"id"`
		StatusURL string `json:"status_url"`
	}{ID: job.ID, StatusURL: location})
}

func validKey(key string) bool {
	if len(key) == 0 || len(key) > 128 {
		return false
	}
	for _, c := range []byte(key) {
		if c < 33 || c > 126 {
			return false
		}
	}
	return true
}

func writeUnavailable(w http.ResponseWriter) {
	w.Header().Set("Retry-After", "1")
	writeError(w, http.StatusServiceUnavailable, "storage_unavailable", "Job storage is unavailable. Retry submissions with the same Idempotency-Key because their outcome may be uncertain.")
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorBody{Error: jobs.JobError{Code: code, Message: message}})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	// Every response contains a fixed JSON-compatible struct. A write error means
	// the client left; it must not cancel or remove an already accepted job.
	_ = json.NewEncoder(w).Encode(value)
}
