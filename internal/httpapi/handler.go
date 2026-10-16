// Package httpapi validates requests and translates job-manager outcomes to HTTP.
package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/neev-rao/distributed-agent-service/internal/fixture"
	"github.com/neev-rao/distributed-agent-service/internal/jobs"
)

const MaxRequestBytes int64 = 16 << 10

type handler struct {
	manager *jobs.Manager
}

func New(manager *jobs.Manager) http.Handler {
	return &handler{manager: manager}
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
		job, found := h.manager.Get(id)
		if !found {
			writeError(w, http.StatusNotFound, "job_not_found", "No job with this ID exists in this process.")
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
	if input.ScenarioID != fixture.ScenarioID {
		writeError(w, http.StatusBadRequest, "unknown_scenario", "scenario_id must be checkout-db-timeout.")
		return
	}
	job, err := h.manager.Submit(input.ScenarioID)
	if err != nil {
		w.Header().Set("Retry-After", "1")
		switch {
		case errors.Is(err, jobs.ErrQueueFull):
			writeError(w, http.StatusServiceUnavailable, "queue_full", "All waiting slots are occupied; retry later.")
		case errors.Is(err, jobs.ErrRecordLimit):
			writeError(w, http.StatusServiceUnavailable, "record_limit", "The retained job limit is reached; restart the process to accept more work. Restarting loses all jobs and results.")
		case errors.Is(err, jobs.ErrShuttingDown):
			writeError(w, http.StatusServiceUnavailable, "service_shutting_down", "The service is shutting down and no longer accepts jobs.")
		default:
			w.Header().Del("Retry-After")
			writeError(w, http.StatusInternalServerError, "internal_error", "Could not submit the job.")
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
