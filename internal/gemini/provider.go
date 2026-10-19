// Package gemini adapts the bounded agent protocol to Gemini's GenerateContent
// REST API. It executes no tools and retries no provider requests.
package gemini

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/neev-rao/distributed-agent-service/internal/agent"
	"github.com/neev-rao/distributed-agent-service/internal/jobs"
)

const (
	maxBodyBytes = 64 * 1024
	maxCallParts = 7 // Six evidence tools and one final answer is the outer bound.
	endpoint     = "https://generativelanguage.googleapis.com/v1beta/models/"
)

var modelName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$`)

type Config struct {
	APIKey string
	Model  string
	// HTTPClient allows a controlled transport in tests. Its redirect policy is
	// always replaced, and its timeout cannot exceed the per-call ten seconds.
	HTTPClient *http.Client
}

type Provider struct {
	key, model string
	client     *http.Client
}

// New does not read environment variables or contact the provider. Configuration
// errors are returned before a worker can accept a job.
func New(config Config) (*Provider, error) {
	if strings.TrimSpace(config.APIKey) == "" || len(config.APIKey) > 512 || strings.ContainsAny(config.APIKey, "\r\n") {
		return nil, errors.New("GEMINI_API_KEY must be configured")
	}
	if !modelName.MatchString(config.Model) {
		return nil, errors.New("GEMINI_MODEL must be a model name without a path or query")
	}
	client := &http.Client{}
	if config.HTTPClient != nil {
		copy := *config.HTTPClient
		client = &copy
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if client.Timeout <= 0 || client.Timeout > 10*time.Second {
		client.Timeout = 10 * time.Second
	}
	return &Provider{key: config.APIKey, model: config.Model, client: client}, nil
}

type pendingCall struct {
	id, providerID, name string
}

// A session belongs to one investigation and is used sequentially by its runtime.
// Full model content is retained privately so opaque thought signatures are sent
// back in their original parts; model thoughts are never exposed as job results.
type session struct {
	provider *Provider
	history  []json.RawMessage
	pending  []pendingCall
	seenIDs  map[string]bool
	turn     int
	closed   bool
}

func (p *Provider) NewSession(prompt string) (agent.Session, error) {
	if prompt == "" || len(prompt) > maxBodyBytes/2 {
		return nil, failure("provider_response_invalid")
	}
	initial, _ := json.Marshal(map[string]any{"role": "user", "parts": []map[string]string{{"text": prompt}}})
	return &session{provider: p, history: []json.RawMessage{initial}, seenIDs: make(map[string]bool)}, nil
}

func failure(code string) error { return &agent.Error{Code: code} }

func (s *session) Next(ctx context.Context, results []agent.ToolResult) (turn agent.Turn, err error) {
	if s.closed {
		return turn, failure("provider_response_invalid")
	}
	// An error closes this session: a caller must never accidentally retry a
	// potentially billable request after an ambiguous transport failure.
	defer func() {
		if err != nil {
			s.closed = true
		}
	}()
	if err = s.appendResults(results); err != nil {
		return turn, err
	}
	declarations := make([]map[string]any, 0, 4)
	for _, tool := range agent.Tools() {
		// The runtime schemas use JSON Schema features such as
		// additionalProperties. Gemini's narrower Schema-valued "parameters"
		// field cannot represent those; use its full JSON Schema alternative.
		declarations = append(declarations, map[string]any{"name": tool.Name, "description": tool.Description, "parametersJsonSchema": tool.Parameters})
	}
	body, err := json.Marshal(map[string]any{
		"contents":         s.history,
		"tools":            []map[string]any{{"functionDeclarations": declarations}},
		"toolConfig":       map[string]any{"functionCallingConfig": map[string]string{"mode": "ANY"}},
		"generationConfig": map[string]int{"candidateCount": 1, "maxOutputTokens": 1024},
	})
	if err != nil || len(body) > maxBodyBytes {
		return turn, failure("provider_response_too_large")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+s.provider.model+":generateContent", bytes.NewReader(body))
	if err != nil {
		return turn, failure("provider_failure")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-goog-api-key", s.provider.key)
	response, err := s.provider.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return turn, ctx.Err()
		}
		var timeout net.Error
		if errors.As(err, &timeout) && timeout.Timeout() {
			return turn, failure("provider_timeout")
		}
		return turn, failure("provider_failure")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		// Do not include the response body, URL, or transport error: providers can
		// reflect credentials, prompts, and other private content in those fields.
		return turn, failure("provider_failure")
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, maxBodyBytes+1))
	if err != nil {
		if ctx.Err() != nil {
			return turn, ctx.Err()
		}
		var timeout net.Error
		if errors.As(err, &timeout) && timeout.Timeout() {
			return turn, failure("provider_timeout")
		}
		return turn, failure("provider_failure")
	}
	if len(raw) > maxBodyBytes {
		return turn, failure("provider_response_too_large")
	}
	return s.parse(raw)
}

func (s *session) appendResults(results []agent.ToolResult) error {
	if len(results) != len(s.pending) {
		return failure("provider_response_invalid")
	}
	if len(s.pending) == 0 {
		return nil
	}
	parts := make([]map[string]any, 0, len(results))
	for i, result := range results {
		pending := s.pending[i]
		if result.ID != pending.id || result.Name != pending.name {
			return failure("provider_response_invalid")
		}
		functionResponse := map[string]any{"name": result.Name, "response": map[string]any{"evidence": result.Evidence}}
		if pending.providerID != "" {
			functionResponse["id"] = pending.providerID
		}
		parts = append(parts, map[string]any{"functionResponse": functionResponse})
	}
	content, err := json.Marshal(map[string]any{"role": "user", "parts": parts})
	if err != nil || len(content) > maxBodyBytes {
		return failure("provider_response_too_large")
	}
	s.history = append(s.history, content)
	s.pending = nil
	return nil
}

type content struct {
	Role  string            `json:"role"`
	Parts []json.RawMessage `json:"parts"`
}

type responsePart struct {
	Text             string        `json:"text"`
	Thought          bool          `json:"thought"`
	ThoughtSignature string        `json:"thoughtSignature"`
	FunctionCall     *functionCall `json:"functionCall"`
}

type functionCall struct {
	ID   string          `json:"id"`
	Name string          `json:"name"`
	Args json.RawMessage `json:"args"`
}

func (s *session) parse(raw []byte) (agent.Turn, error) {
	var response struct {
		Candidates []struct {
			Content      json.RawMessage `json:"content"`
			FinishReason string          `json:"finishReason"`
		} `json:"candidates"`
		Usage struct {
			Input    int `json:"promptTokenCount"`
			Output   int `json:"candidatesTokenCount"`
			Thoughts int `json:"thoughtsTokenCount"`
		} `json:"usageMetadata"`
	}
	var turn agent.Turn
	if !utf8.Valid(raw) || uniqueJSON(raw) != nil || json.Unmarshal(raw, &response) != nil || len(response.Candidates) != 1 || response.Candidates[0].FinishReason != "STOP" || response.Usage.Input < 0 || response.Usage.Output < 0 || response.Usage.Thoughts < 0 || response.Usage.Output > 1_000_000 || response.Usage.Thoughts > 1_000_000 {
		return turn, failure("provider_response_invalid")
	}
	var candidate content
	if json.Unmarshal(response.Candidates[0].Content, &candidate) != nil || candidate.Role != "model" || len(candidate.Parts) == 0 || len(candidate.Parts) > 16 {
		return turn, failure("provider_response_invalid")
	}
	s.turn++
	for i, rawPart := range candidate.Parts {
		var part responsePart
		if strictDecode(rawPart, &part) != nil {
			return turn, failure("provider_response_invalid")
		}
		if part.Thought {
			if part.FunctionCall != nil {
				return turn, failure("provider_response_invalid")
			}
			continue
		}
		if part.FunctionCall == nil {
			// Signature-only parts are permitted; a prose answer is not our
			// structured investigation protocol, including prose beside tools.
			if part.Text != "" || part.ThoughtSignature == "" {
				return turn, failure("provider_response_invalid")
			}
			continue
		}
		if part.Text != "" || len(turn.Calls) >= maxCallParts || turn.Result != nil {
			return turn, failure("provider_response_invalid")
		}
		call := part.FunctionCall
		if !json.Valid(call.Args) || len(call.Name) == 0 || len(call.Name) > 128 || len(call.ID) > 128 {
			return turn, failure("provider_response_invalid")
		}
		if call.Name == "finish_investigation" {
			if len(turn.Calls) != 0 {
				return turn, failure("provider_response_invalid")
			}
			var result jobs.Result
			if strictDecode(call.Args, &result) != nil || result.Diagnosis == "" || result.EvidenceIDs == nil {
				return turn, failure("provider_response_invalid")
			}
			turn.Result = &result
			continue
		}
		id := call.ID
		if id == "" {
			id = fmt.Sprintf("gemini-%d-%d", s.turn, i)
		}
		if s.seenIDs[id] {
			return turn, failure("provider_response_invalid")
		}
		s.seenIDs[id] = true
		turn.Calls = append(turn.Calls, agent.ToolCall{ID: id, Name: call.Name, Arguments: call.Args})
		s.pending = append(s.pending, pendingCall{id: id, providerID: call.ID, name: call.Name})
	}
	if len(turn.Calls) == 0 && turn.Result == nil {
		return turn, failure("provider_response_invalid")
	}
	turn.InputTokens, turn.OutputTokens = response.Usage.Input, response.Usage.Output+response.Usage.Thoughts
	if turn.Result != nil {
		s.closed = true
	} else {
		// Retain the entire original content, not a reconstructed tool-call
		// subset. Gemini requires opaque signatures in their original parts.
		s.history = append(s.history, append(json.RawMessage(nil), response.Candidates[0].Content...))
	}
	return turn, nil
}

func strictDecode(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return errors.New("trailing JSON data")
	}
	return nil
}

// encoding/json accepts duplicate object members and silently keeps the last.
// Reject that ambiguity before extracting actions or final-answer fields.
func uniqueJSON(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	var value func(int) error
	value = func(depth int) error {
		if depth > 64 {
			return errors.New("JSON nesting limit exceeded")
		}
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		switch token {
		case json.Delim('{'):
			seen := make(map[string]bool)
			for decoder.More() {
				key, err := decoder.Token()
				name, ok := key.(string)
				if err != nil || !ok || seen[name] {
					return errors.New("invalid or duplicate JSON member")
				}
				seen[name] = true
				if err := value(depth + 1); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim('}') {
				return errors.New("invalid JSON object")
			}
		case json.Delim('['):
			for decoder.More() {
				if err := value(depth + 1); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim(']') {
				return errors.New("invalid JSON array")
			}
		case json.Delim('}'), json.Delim(']'):
			return errors.New("unexpected JSON delimiter")
		}
		return nil
	}
	if err := value(0); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return errors.New("trailing JSON data")
	}
	return nil
}
