package evaluation

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/neev-rao/distributed-agent-service/internal/agent"
	"github.com/neev-rao/distributed-agent-service/internal/appconfig"
	"github.com/neev-rao/distributed-agent-service/internal/fixture"
	"github.com/neev-rao/distributed-agent-service/internal/gemini"
)

type localTransport struct {
	target *url.URL
	base   http.RoundTripper
}

func (l localTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	copy := r.Clone(r.Context())
	location := *r.URL
	location.Scheme = l.target.Scheme
	location.Host = l.target.Host
	copy.URL = &location
	copy.Host = l.target.Host
	return l.base.RoundTrip(copy)
}

// This checks the actual runner -> agent -> Gemini HTTP -> fixture path, not
// model quality. The local model server returns explicitly scripted answers.
func TestRepeatedEvaluationUsesFreshProviderConversationsAndSelectedEvidence(t *testing.T) {
	var requests atomic.Int32
	var initial json.RawMessage
	catalog := fixture.EvaluationScenarios()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := int(requests.Add(1))
		var input struct {
			Contents []json.RawMessage `json:"contents"`
		}
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		if r.Header.Get("x-goog-api-key") != "test-key-not-a-credential" {
			t.Error("missing test authentication")
		}
		var calls []map[string]any
		if request%2 == 1 {
			if len(input.Contents) != 1 {
				t.Errorf("conversation reused across evaluations: %d messages", len(input.Contents))
				w.WriteHeader(400)
				return
			}
			if initial == nil {
				initial = append([]byte(nil), input.Contents[0]...)
			} else if !bytes.Equal(initial, input.Contents[0]) {
				t.Error("task prompt changed with evidence case")
			}
			for _, id := range catalog {
				if bytes.Contains(input.Contents[0], []byte(id)) {
					t.Error("scenario identity leaked into model prompt")
				}
			}
			calls = []map[string]any{
				{"id": "m", "name": "read_metrics", "args": map[string]string{"service": "checkout"}},
				{"id": "l", "name": "read_logs", "args": map[string]string{"service": "checkout"}},
				{"id": "r", "name": "read_runbook", "args": map[string]string{"name": "database-pool"}},
			}
		} else {
			if len(input.Contents) != 3 {
				t.Errorf("tool exchange missing or history leaked: %d messages", len(input.Contents))
				w.WriteHeader(400)
				return
			}
			scenario := catalog[(request/2-1)%len(catalog)]
			var observations struct {
				Parts []struct {
					Response struct {
						Name string `json:"name"`
						Data struct {
							Evidence []fixture.Evidence `json:"evidence"`
						} `json:"response"`
					} `json:"functionResponse"`
				} `json:"parts"`
			}
			if err := json.Unmarshal(input.Contents[2], &observations); err != nil {
				t.Error(err)
			}
			if len(observations.Parts) != 3 {
				t.Error("missing read-tool results")
			}
			for _, part := range observations.Parts {
				want, err := fixture.Read(r.Context(), scenario, part.Response.Name)
				if err != nil || !reflect.DeepEqual(part.Response.Data.Evidence, want) {
					t.Errorf("%s observations do not match %s", part.Response.Name, scenario)
				}
			}
			result := passingResult(scenario)
			calls = []map[string]any{{"id": "f", "name": "finish_investigation", "args": result}}
		}
		parts := make([]map[string]any, 0, len(calls))
		for _, call := range calls {
			parts = append(parts, map[string]any{"functionCall": call})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"candidates": []any{map[string]any{
			"content": map[string]any{"role": "model", "parts": parts}, "finishReason": "STOP"}}})
	}))
	defer server.Close()
	target, _ := url.Parse(server.URL)
	provider, err := gemini.New(gemini.Config{APIKey: "test-key-not-a-credential", Model: "test-model", HTTPClient: &http.Client{Transport: localTransport{target, http.DefaultTransport}}})
	if err != nil {
		t.Fatal(err)
	}
	executor, err := agent.New(agent.DefaultConfig(), provider)
	if err != nil {
		t.Fatal(err)
	}
	cfg := testConfig()
	cfg.Profile = appconfig.GeminiProfile(cfg.Model)
	report, err := Run(context.Background(), cfg, executor)
	if err != nil || !report.Passed || report.Completed != 8 || requests.Load() != 16 {
		t.Fatalf("report%+v requests%d err%v", report, requests.Load(), err)
	}
}
