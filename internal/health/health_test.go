package health

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func request(s *State, method, path string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	s.Wrap(http.NotFoundHandler()).ServeHTTP(w, httptest.NewRequest(method, path, nil))
	return w
}

func TestHealthInitializationDrainAndLiveness(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	checks := 0
	s := New(ctx, func(ctx context.Context) error {
		checks++
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > CheckTimeout {
			t.Error("dependency check is unbounded")
		}
		return nil
	})
	if request(s, "GET", "/readyz").Code != 503 || checks != 0 {
		t.Fatal("uninitialized process is ready")
	}
	if request(s, "GET", "/livez").Code != 200 || checks != 0 {
		t.Fatal("liveness called a dependency")
	}
	s.MarkReady()
	if got := request(s, "GET", "/readyz"); got.Code != 200 || got.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("ready response: %+v", got)
	}
	cancel()
	if request(s, "GET", "/readyz").Code != 503 || checks != 1 {
		t.Fatal("canceled lifecycle is ready or checked dependency")
	}
	if request(s, "GET", "/livez").Code != 200 {
		t.Fatal("draining process is not live")
	}
	s = New(context.Background(), nil)
	s.MarkReady()
	s.Drain()
	s.MarkReady()
	if request(s, "GET", "/readyz").Code != 503 {
		t.Fatal("initialization resurrected draining process")
	}
}

func TestDependencyFailureIsSanitizedAndDoesNotAffectLiveness(t *testing.T) {
	s := New(context.Background(), func(context.Context) error { return errors.New("postgres://private:secret@db") })
	s.MarkReady()
	response := request(s, "GET", "/readyz")
	if response.Code != 503 || strings.Contains(response.Body.String(), "secret") || request(s, "GET", "/livez").Code != 200 {
		t.Fatal("failure leaked or changed liveness")
	}
}

func TestReadinessRejectsLateCheckAndConcurrentDrain(t *testing.T) {
	for _, drain := range []bool{false, true} {
		name := "cancellation"
		if drain {
			name = "drain"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var s *State
			s = New(context.Background(), func(context.Context) error {
				if drain {
					s.Drain()
				} else {
					cancel()
				}
				return nil
			})
			s.MarkReady()
			w := httptest.NewRecorder()
			r := httptest.NewRequest("GET", "/readyz", nil).WithContext(ctx)
			s.Wrap(http.NotFoundHandler()).ServeHTTP(w, r)
			if w.Code != 503 {
				t.Fatal("late success reported ready")
			}
		})
	}
}

func TestReadinessDependencyDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	s := New(context.Background(), func(ctx context.Context) error { <-ctx.Done(); return nil })
	s.MarkReady()
	w := httptest.NewRecorder()
	s.Wrap(http.NotFoundHandler()).ServeHTTP(w, httptest.NewRequest("GET", "/readyz", nil).WithContext(ctx))
	if w.Code != 503 {
		t.Fatal("expired dependency check reported success")
	}
}

func TestHealthMethodsAndApplicationRouting(t *testing.T) {
	s := New(context.Background(), nil)
	s.MarkReady()
	for _, path := range []string{"/readyz", "/livez"} {
		w := request(s, "POST", path)
		if w.Code != 405 || w.Header().Get("Allow") != "GET" {
			t.Fatal("wrong method accepted")
		}
	}
	if request(s, "GET", "/readyz/").Code != 404 {
		t.Fatal("unexpected health route")
	}
	w := httptest.NewRecorder()
	s.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(202) })).ServeHTTP(w, httptest.NewRequest("POST", "/jobs", nil))
	if w.Code != 202 {
		t.Fatal("job route intercepted")
	}
}
