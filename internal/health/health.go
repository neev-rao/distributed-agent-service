// Package health exposes process liveness and bounded dependency readiness.
// Readiness is an operational signal, not proof of job or model correctness.
package health

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"
)

const CheckTimeout = time.Second

type State struct {
	lifecycle   context.Context
	check       func(context.Context) error
	initialized atomic.Bool
	draining    atomic.Bool
}

// check must honor context cancellation. A nil check is for explicit local mode.
func New(lifecycle context.Context, check func(context.Context) error) *State {
	return &State{lifecycle: lifecycle, check: check}
}

func (s *State) MarkReady() { s.initialized.Store(true) }
func (s *State) Drain()     { s.draining.Store(true) }

func (s *State) available() bool {
	return s.initialized.Load() && !s.draining.Load() && s.lifecycle.Err() == nil
}

func (s *State) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/livez" && r.URL.Path != "/readyz" {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if r.URL.Path == "/livez" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("alive\n"))
			return
		}
		if !s.available() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), CheckTimeout)
		defer cancel()
		if s.check != nil {
			if err := s.check(ctx); err != nil {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
		}
		// A successful late check must not mask timeout or concurrent draining.
		if ctx.Err() != nil || !s.available() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	})
}
