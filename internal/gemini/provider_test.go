package gemini

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/neev-rao/distributed-agent-service/internal/agent"
)

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func newFixture(t *testing.T, handler http.HandlerFunc) *Provider {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	address, _ := url.Parse(server.URL)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Transport: transportFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Scheme != "https" || request.URL.Host != "generativelanguage.googleapis.com" || request.URL.RawQuery != "" {
			t.Errorf("unexpected production URL: %s", request.URL.Redacted())
		}
		clone := request.Clone(request.Context())
		copyURL := *request.URL
		copyURL.Scheme, copyURL.Host = address.Scheme, address.Host
		clone.URL = &copyURL
		return transport.RoundTrip(clone)
	})}
	provider, err := New(Config{APIKey: "test-secret-do-not-return", Model: "gemini-test-model", HTTPClient: client})
	if err != nil {
		t.Fatal(err)
	}
	return provider
}

func newSession(t *testing.T, provider *Provider) agent.Session {
	t.Helper()
	session, err := provider.NewSession("Investigate checkout using read-only evidence.")
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func assertCode(t *testing.T, err error, code string) {
	t.Helper()
	var failure *agent.Error
	if !errors.As(err, &failure) || failure.Code != code {
		t.Fatalf("error = %v, want code %s", err, code)
	}
	if strings.Contains(err.Error(), "test-secret") || strings.Contains(err.Error(), "private-model-thought") {
		t.Fatal("private provider data escaped through error")
	}
}

func toolResponse(parts string) string {
	return `{"candidates":[{"finishReason":"STOP","content":{"role":"model","parts":` + parts + `}}],"usageMetadata":{"promptTokenCount":20,"candidatesTokenCount":8,"thoughtsTokenCount":2}}`
}

func TestNewRejectsMissingCredentialsAndUnsafeModel(t *testing.T) {
	for _, config := range []Config{
		{Model: "valid"}, {APIKey: " \t", Model: "valid"}, {APIKey: "secret\nvalue", Model: "valid"},
		{APIKey: "secret"}, {APIKey: "secret", Model: "models/example"},
		{APIKey: "secret", Model: "example?key=secret"}, {APIKey: "secret", Model: "../example"},
	} {
		if _, err := New(config); err == nil || strings.Contains(err.Error(), "secret") {
			t.Errorf("bad configuration was accepted or leaked credentials: %v", err)
		}
	}
	client := &http.Client{Timeout: time.Minute}
	provider, err := New(Config{APIKey: "secret", Model: "gemini-2.5-flash", HTTPClient: client})
	if err != nil || provider.client.Timeout != 10*time.Second || client.Timeout != time.Minute {
		t.Fatalf("timeout clamp mutated caller: provider=%v error=%v", provider, err)
	}
}

func TestToolLoopPreservesContentAndMatchesProviderIDs(t *testing.T) {
	var calls atomic.Int32
	const parts = `[{"text":"private-model-thought","thought":true,"thoughtSignature":"opaque-thought"},{"functionCall":{"id":"provider-call-1","name":"read_metrics","args":{"service":"checkout"}},"thoughtSignature":"opaque-call"}]`
	provider := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		call := calls.Add(1)
		if r.Method != http.MethodPost || r.URL.Path != "/v1beta/models/gemini-test-model:generateContent" || r.Header.Get("x-goog-api-key") != "test-secret-do-not-return" {
			t.Errorf("incorrect request method/path/credential header")
		}
		var request struct {
			Contents []json.RawMessage `json:"contents"`
			Tools    []struct {
				Declarations []struct {
					Name       string          `json:"name"`
					Parameters json.RawMessage `json:"parameters"`
					JSONSchema json.RawMessage `json:"parametersJsonSchema"`
				} `json:"functionDeclarations"`
			} `json:"tools"`
			Generation map[string]int `json:"generationConfig"`
			ToolConfig struct {
				Function struct {
					Mode string `json:"mode"`
				} `json:"functionCallingConfig"`
			} `json:"toolConfig"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		if len(request.Tools) != 1 || len(request.Tools[0].Declarations) != 4 || request.Generation["maxOutputTokens"] != 1024 || request.Generation["candidateCount"] != 1 || request.ToolConfig.Function.Mode != "ANY" {
			t.Error("missing tool declarations or generation bounds")
		}
		for _, tool := range request.Tools[0].Declarations {
			if len(tool.Parameters) != 0 || len(tool.JSONSchema) == 0 {
				t.Error("full JSON Schema must use parametersJsonSchema exclusively")
			}
			var schema struct {
				AdditionalProperties *bool `json:"additionalProperties"`
			}
			if json.Unmarshal(tool.JSONSchema, &schema) != nil || schema.AdditionalProperties == nil || *schema.AdditionalProperties {
				t.Error("tool JSON Schema lost its additionalProperties restriction")
			}
		}
		if call == 1 {
			if len(request.Contents) != 1 {
				t.Error("fresh session contains another investigation's history")
			}
			_, _ = io.WriteString(w, toolResponse(parts))
			return
		}
		if len(request.Contents) != 3 {
			t.Errorf("history has %d entries", len(request.Contents))
			w.WriteHeader(500)
			return
		}
		var actual, expected any
		_ = json.Unmarshal(request.Contents[1], &actual)
		_ = json.Unmarshal([]byte(`{"role":"model","parts":`+parts+`}`), &expected)
		if !reflect.DeepEqual(actual, expected) {
			t.Error("original parts or thought signatures were changed")
		}
		var result struct {
			Parts []struct {
				Response struct {
					ID, Name string
					Response struct {
						Evidence []agent.Evidence `json:"evidence"`
					} `json:"response"`
				} `json:"functionResponse"`
			} `json:"parts"`
		}
		if err := json.Unmarshal(request.Contents[2], &result); err != nil || len(result.Parts) != 1 || result.Parts[0].Response.ID != "provider-call-1" || result.Parts[0].Response.Name != "read_metrics" || len(result.Parts[0].Response.Response.Evidence) != 1 || result.Parts[0].Response.Response.Evidence[0].ID != "metric-1" {
			t.Error("tool result did not match the provider call ID and evidence")
		}
		_, _ = io.WriteString(w, toolResponse(`[{"functionCall":{"id":"finish-1","name":"finish_investigation","args":{"diagnosis":"The pool is exhausted.","evidence_ids":["metric-1"]}},"thoughtSignature":"opaque-final"}]`))
	})
	session := newSession(t, provider)
	turn, err := session.Next(context.Background(), nil)
	if err != nil || len(turn.Calls) != 1 || turn.Calls[0].ID != "provider-call-1" || turn.InputTokens != 20 || turn.OutputTokens != 10 {
		t.Fatalf("initial turn = %+v, error %v", turn, err)
	}
	turn, err = session.Next(context.Background(), []agent.ToolResult{{ID: turn.Calls[0].ID, Name: "read_metrics", Evidence: []agent.Evidence{{ID: "metric-1", Text: "All 20 connections are occupied."}}}})
	if err != nil || turn.Result == nil || turn.Result.Diagnosis != "The pool is exhausted." || len(turn.Result.EvidenceIDs) != 1 {
		t.Fatalf("final turn = %+v, error %v", turn, err)
	}
	_, err = session.Next(context.Background(), nil)
	assertCode(t, err, "provider_response_invalid")
	if calls.Load() != 2 {
		t.Fatalf("made %d requests, expected two", calls.Load())
	}
}

func TestLegacyCallIDsAndIsolatedSessions(t *testing.T) {
	var count atomic.Int32
	provider := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		call := count.Add(1)
		body, _ := io.ReadAll(r.Body)
		if call == 2 && strings.Contains(string(body), `"functionResponse":{"id":`) {
			t.Error("invented an upstream ID the model did not supply")
		}
		if call == 3 {
			var request struct {
				Contents []json.RawMessage `json:"contents"`
			}
			_ = json.Unmarshal(body, &request)
			if len(request.Contents) != 1 {
				t.Error("new session reused private history")
			}
		}
		_, _ = io.WriteString(w, toolResponse(`[{"functionCall":{"name":"read_logs","args":{"service":"checkout"}}}]`))
	})
	first := newSession(t, provider)
	turn, err := first.Next(context.Background(), nil)
	if err != nil || len(turn.Calls) != 1 || turn.Calls[0].ID == "" {
		t.Fatalf("legacy ID not synthesized: %+v %v", turn, err)
	}
	if _, err = first.Next(context.Background(), []agent.ToolResult{{ID: turn.Calls[0].ID, Name: "read_logs"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := newSession(t, provider).Next(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPFailuresAreSanitizedAndNeverRetried(t *testing.T) {
	for _, status := range []int{401, 403, 429, 500, 503, 307} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var count atomic.Int32
			provider := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
				count.Add(1)
				w.Header().Set("Location", "https://attacker.invalid/stolen")
				w.WriteHeader(status)
				_, _ = io.WriteString(w, `{"error":"test-secret-do-not-return private-model-thought"}`)
			})
			session := newSession(t, provider)
			_, err := session.Next(context.Background(), nil)
			assertCode(t, err, "provider_failure")
			_, err = session.Next(context.Background(), nil)
			assertCode(t, err, "provider_response_invalid")
			if count.Load() != 1 {
				t.Fatalf("request retried or redirect followed: %d requests", count.Load())
			}
		})
	}
}

func TestMalformedResponsesAreRejected(t *testing.T) {
	validCall := `{"functionCall":{"id":"one","name":"read_logs","args":{"service":"checkout"}}}`
	final := `{"functionCall":{"name":"finish_investigation","args":{"diagnosis":"Pool exhausted.","evidence_ids":[]}}}`
	for name, body := range map[string]string{
		"invalid JSON":              `{`,
		"duplicate final member":    toolResponse(`[{"functionCall":{"name":"finish_investigation","args":{"diagnosis":"one","diagnosis":"two","evidence_ids":[]}}}]`),
		"duplicate response member": `{"candidates":[],"candidates":[{}]}`,
		"missing candidate":         `{}`,
		"multiple candidates":       `{"candidates":[{},{}]}`,
		"truncated generation":      strings.Replace(toolResponse(`[`+validCall+`]`), `"STOP"`, `"MAX_TOKENS"`, 1),
		"prose":                     toolResponse(`[{"text":"guess"}]`),
		"prose beside call":         toolResponse(`[{"text":"guess"},` + validCall + `]`),
		"thought only":              toolResponse(`[{"thought":true,"text":"private-model-thought"}]`),
		"missing arguments":         toolResponse(`[{"functionCall":{"name":"read_logs"}}]`),
		"unknown part":              toolResponse(`[{"inlineData":{"data":"unexpected"}}]`),
		"duplicate ID":              toolResponse(`[` + validCall + `,` + validCall + `]`),
		"multiple final answers":    toolResponse(`[` + final + `,` + final + `]`),
		"read and final":            toolResponse(`[` + validCall + `,` + final + `]`),
		"final then read":           toolResponse(`[` + final + `,` + validCall + `]`),
		"final extra field":         toolResponse(`[{"functionCall":{"name":"finish_investigation","args":{"diagnosis":"Pool.","evidence_ids":[],"extra":"wrong"}}}]`),
		"final null":                toolResponse(`[{"functionCall":{"name":"finish_investigation","args":null}}]`),
		"final missing citations":   toolResponse(`[{"functionCall":{"name":"finish_investigation","args":{"diagnosis":"Pool."}}}]`),
		"too many parts":            toolResponse(`[` + strings.Repeat(`{"thought":true,"text":"x"},`, 16) + validCall + `]`),
	} {
		t.Run(name, func(t *testing.T) {
			provider := newFixture(t, func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, body) })
			_, err := newSession(t, provider).Next(context.Background(), nil)
			assertCode(t, err, "provider_response_invalid")
		})
	}
}

func TestResponseAndHistoryCaps(t *testing.T) {
	t.Run("response", func(t *testing.T) {
		provider := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.WriteString(w, strings.Repeat("x", maxBodyBytes+1))
		})
		_, err := newSession(t, provider).Next(context.Background(), nil)
		assertCode(t, err, "provider_response_too_large")
	})
	t.Run("history", func(t *testing.T) {
		var count atomic.Int32
		provider := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
			count.Add(1)
			_, _ = io.WriteString(w, toolResponse(`[{"functionCall":{"id":"one","name":"read_logs","args":{"service":"checkout"}},"thoughtSignature":"`+strings.Repeat("s", 40*1024)+`"}]`))
		})
		session, err := provider.NewSession(strings.Repeat("p", 30*1024))
		if err != nil {
			t.Fatal(err)
		}
		turn, err := session.Next(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		_, err = session.Next(context.Background(), []agent.ToolResult{{ID: turn.Calls[0].ID, Name: "read_logs"}})
		assertCode(t, err, "provider_response_too_large")
		if count.Load() != 1 {
			t.Fatal("oversized request was sent")
		}
	})
}

func TestCancellationReachesHTTP(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	provider := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(started)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := newSession(t, provider).Next(ctx, nil); done <- err }()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("HTTP request never started")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancellation did not stop request")
	}
}

func TestMismatchedToolResultsDoNotContactProvider(t *testing.T) {
	var count atomic.Int32
	provider := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		count.Add(1)
		_, _ = io.WriteString(w, toolResponse(`[{"functionCall":{"id":"one","name":"read_logs","args":{"service":"checkout"}}}]`))
	})
	session := newSession(t, provider)
	if _, err := session.Next(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	_, err := session.Next(context.Background(), []agent.ToolResult{{ID: "wrong", Name: "read_logs"}})
	assertCode(t, err, "provider_response_invalid")
	if count.Load() != 1 {
		t.Fatal("mismatched tool result was sent")
	}
}

func TestClientTimeoutAndTransportErrorsAreSanitized(t *testing.T) {
	t.Run("timeout", func(t *testing.T) {
		provider := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			<-r.Context().Done()
		})
		provider.client.Timeout = 20 * time.Millisecond
		_, err := newSession(t, provider).Next(context.Background(), nil)
		assertCode(t, err, "provider_timeout")
	})
	t.Run("transport", func(t *testing.T) {
		provider, err := New(Config{APIKey: "test-secret", Model: "test", HTTPClient: &http.Client{Transport: transportFunc(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("test-secret-do-not-return private-model-thought")
		})}})
		if err != nil {
			t.Fatal(err)
		}
		_, err = newSession(t, provider).Next(context.Background(), nil)
		assertCode(t, err, "provider_failure")
	})
}
