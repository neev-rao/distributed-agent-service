package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestProbeAcceptsOnlyOKWithoutFollowingRedirects(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		switch r.URL.Path {
		case "/ok":
			w.WriteHeader(200)
		case "/redirect":
			http.Redirect(w, r, "/ok", 302)
		default:
			w.WriteHeader(503)
		}
	}))
	defer server.Close()
	for _, tc := range []struct {
		path string
		code int
	}{{"/ok", 0}, {"/redirect", 1}, {"/unavailable", 1}} {
		var stderr bytes.Buffer
		before := requests.Load()
		if got := run([]string{"-url", server.URL + tc.path}, &stderr); got != tc.code {
			t.Fatalf("%s: %d", tc.path, got)
		}
		if requests.Load() != before+1 {
			t.Fatal("probe followed redirect or retried")
		}
	}
}

func TestProbeTimesOutAndDoesNotPrintEndpointErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer server.Close()
	var stderr bytes.Buffer
	if run([]string{"-url", server.URL + "/?secret=private", "-timeout", "5ms"}, &stderr) != 1 || strings.Contains(stderr.String(), "private") {
		t.Fatal("timeout succeeded or leaked URL")
	}
}

func TestProbeRejectsInvalidConfiguration(t *testing.T) {
	for _, args := range [][]string{
		{"-url", "file:///private"}, {"-url", "http://user:secret@127.0.0.1"},
		{"-timeout", "0s"}, {"-timeout", "6s"}, {"unexpected"}, {"-missing"},
	} {
		var stderr bytes.Buffer
		if run(args, &stderr) != 1 {
			t.Fatalf("invalid arguments accepted: %v", args)
		}
	}
}
