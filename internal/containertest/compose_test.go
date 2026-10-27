//go:build containers

// Package containertest verifies the actual images and Compose lifecycle.
// Opt in with: go test -tags=containers -count=1 -timeout=10m ./internal/containertest
package containertest

import (
	"context"
	"crypto/rand"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/neev-rao/distributed-agent-service/internal/jobs"
)

type container struct {
	ID     string `json:"Id"`
	Image  string
	Path   string
	Config struct {
		User   string
		Env    []string
		Labels map[string]string
	}
	State struct {
		Running  bool
		ExitCode int
		Health   struct{ Status string }
	}
	HostConfig struct {
		ReadonlyRootfs bool
		Memory         int64
		NanoCPUs       int64
		CapDrop        []string
	}
	NetworkSettings struct {
		Ports map[string][]struct{ HostIP, HostPort string }
	}
}

type stack struct {
	t                                *testing.T
	root, project, image, temp, base string
	env                              []string
	compose                          []string
}

func (s *stack) command(args ...string) []byte {
	s.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Dir, cmd.Env = s.root, s.env
	output, err := cmd.CombinedOutput()
	if err != nil {
		s.t.Fatalf("docker command %s failed: %v\n%s", args[0], err, output)
	}
	return output
}

func (s *stack) composeCommand(args ...string) []byte {
	return s.command(append(append([]string(nil), s.compose...), args...)...)
}

func (s *stack) inspect(id string) container {
	s.t.Helper()
	var items []container
	if err := json.Unmarshal(s.command("inspect", id), &items); err != nil || len(items) != 1 {
		s.t.Fatalf("inspect: %v", err)
	}
	if items[0].Config.Labels["com.docker.compose.project"] != s.project {
		s.t.Fatal("refusing an unowned container")
	}
	return items[0]
}

func (s *stack) ids(service string) []string {
	return strings.Fields(string(s.composeCommand("ps", "--all", "--quiet", service)))
}

func newStack(t *testing.T) *stack {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = exec.LookPath("docker"); err != nil {
		t.Fatal("Docker CLI is required for explicitly selected container tests")
	}
	var suffix [6]byte
	if _, err = rand.Read(suffix[:]); err != nil {
		t.Fatal(err)
	}
	project := "das-verify-" + hex.EncodeToString(suffix[:])
	s := &stack{t: t, root: root, project: project, image: "distributed-agent-service:" + project, temp: t.TempDir()}
	// These values override inherited model keys, database URLs, image/port
	// overrides, and resource settings. Never load the user's .env or .env.agent.
	settings := map[string]string{
		"POSTGRES_USER": "agentservice", "POSTGRES_DB": "agentservice_test", "POSTGRES_PASSWORD": "local-development-only", "POSTGRES_PORT": "0",
		"CONTAINER_DATABASE_URL": "postgres://agentservice:local-development-only@postgres:5432/agentservice_test?sslmode=disable",
		"APP_API_PORT":           "0", "APP_IMAGE": s.image, "AGENT_EXECUTOR": "fixture", "GEMINI_MODEL": "", "GEMINI_API_KEY": "",
		"API_CPUS": "0.5", "API_MEMORY": "128m", "WORKER_CPUS": "1.0", "WORKER_MEMORY": "256m",
	}
	for _, item := range os.Environ() {
		key, _, _ := strings.Cut(item, "=")
		if _, override := settings[key]; !override {
			s.env = append(s.env, item)
		}
	}
	for key, value := range settings {
		s.env = append(s.env, key+"="+value)
	}
	envFile := filepath.Join(s.temp, "test.env")
	if err = os.WriteFile(envFile, nil, 0600); err != nil {
		t.Fatal(err)
	}
	override := filepath.Join(s.temp, "delay.yaml")
	if err = os.WriteFile(override, []byte("services:\n  worker:\n    command: [-health-listen=0.0.0.0:8081, -shutdown-grace=40s, -executor=fixture, -fixture-delay=3s]\n"), 0600); err != nil {
		t.Fatal(err)
	}
	s.compose = []string{"compose", "-p", project, "--env-file", envFile, "-f", filepath.Join(root, "compose.yaml"), "-f", filepath.Join(root, "compose.app.yaml"), "-f", override}
	if len(strings.Fields(string(s.command("ps", "-aq", "--filter", "label=com.docker.compose.project="+project)))) != 0 {
		t.Fatal("test project already exists")
	}
	t.Cleanup(func() {
		// Inspect every resource before permitting project-scoped deletion.
		for _, id := range strings.Fields(string(s.composeCommand("ps", "--all", "--quiet"))) {
			item := s.inspect(id)
			switch item.Config.Labels["com.docker.compose.service"] {
			case "api", "worker", "postgres":
			default:
				t.Error("unexpected project service; cleanup refused")
				return
			}
		}
		for _, kind := range []string{"volume", "network"} {
			for _, name := range strings.Fields(string(s.command(kind, "ls", "-q", "--filter", "label=com.docker.compose.project="+project))) {
				var resources []struct{ Labels map[string]string }
				if err := json.Unmarshal(s.command(kind, "inspect", name), &resources); err != nil || len(resources) != 1 || resources[0].Labels["com.docker.compose.project"] != project {
					t.Error("resource ownership mismatch; cleanup refused")
					return
				}
			}
		}
		if t.Failed() {
			t.Logf("owned project failure logs:\n%s", s.composeCommand("logs", "--no-color", "--tail", "50"))
		}
		s.composeCommand("down", "--volumes", "--timeout", "45")
		s.command("image", "rm", s.image)
	})
	t.Log("building and starting isolated Compose project with two workers")
	s.composeCommand("up", "--build", "-d", "--wait", "--wait-timeout", "90", "--scale", "worker=2")
	s.refreshAddress()
	return s
}

func (s *stack) refreshAddress() {
	s.t.Helper()
	api := s.inspect(s.ids("api")[0])
	ports := api.NetworkSettings.Ports["8080/tcp"]
	if len(ports) != 1 || ports[0].HostIP != "127.0.0.1" {
		s.t.Fatal("API is not restricted to loopback")
	}
	s.base = "http://127.0.0.1:" + ports[0].HostPort
}

func wait(t *testing.T, description string, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("timed out: " + description)
		case <-ticker.C:
		}
	}
}

func (s *stack) request(method, path, key string) (int, []byte) {
	s.t.Helper()
	var body *strings.Reader
	if method == "POST" {
		body = strings.NewReader(`{"scenario_id":"checkout-db-timeout"}`)
	} else {
		body = strings.NewReader("")
	}
	req, err := http.NewRequest(method, s.base+path, body)
	if err != nil {
		s.t.Fatal(err)
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 5 * time.Second}
	response, err := client.Do(req)
	if err != nil {
		return 0, nil
	}
	defer response.Body.Close()
	var raw json.RawMessage
	if path == "/livez" || path == "/readyz" {
		return response.StatusCode, nil
	}
	if err = json.NewDecoder(response.Body).Decode(&raw); err != nil {
		s.t.Fatal(err)
	}
	return response.StatusCode, raw
}

func (s *stack) submit(key string) string {
	s.t.Helper()
	code, raw := s.request("POST", "/jobs", key)
	var item struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(raw, &item); err != nil || code != 202 || item.ID == "" {
		s.t.Fatalf("submit %d %s %v", code, raw, err)
	}
	return item.ID
}

func (s *stack) job(id string) jobs.Job {
	s.t.Helper()
	code, raw := s.request("GET", "/jobs/"+id, "")
	var job jobs.Job
	if err := json.Unmarshal(raw, &job); err != nil || code != 200 {
		s.t.Fatalf("get %d %s %v", code, raw, err)
	}
	return job
}

func (s *stack) finished(id string) jobs.Job {
	s.t.Helper()
	var result jobs.Job
	wait(s.t, "terminal job", 25*time.Second, func() bool { result = s.job(id); return result.State == jobs.Succeeded || result.State == jobs.Failed })
	if result.State != jobs.Succeeded || result.Result == nil {
		s.t.Fatalf("job did not succeed: %+v", result)
	}
	return result
}

func (s *stack) owner(id string) string {
	s.t.Helper()
	for _, worker := range s.ids("worker") {
		for _, line := range strings.Split(string(s.command("logs", worker)), "\n") {
			var event struct {
				Message string `json:"msg"`
				JobID   string `json:"job_id"`
			}
			if json.Unmarshal([]byte(line), &event) == nil && event.Message == "job_claimed" && event.JobID == id {
				return worker
			}
		}
	}
	s.t.Fatal("job owner missing from worker logs")
	return ""
}

func TestComposeLifecycle(t *testing.T) {
	s := newStack(t)
	workers := s.ids("worker")
	if len(workers) != 2 {
		t.Fatal("expected two independently scaled workers")
	}
	api := s.inspect(s.ids("api")[0])
	for _, id := range append(workers, api.ID) {
		item := s.inspect(id)
		if item.Image != api.Image || item.Config.User != "65532:65532" || !item.HostConfig.ReadonlyRootfs || item.HostConfig.Memory <= 0 || item.HostConfig.NanoCPUs <= 0 || len(item.HostConfig.CapDrop) != 1 || item.HostConfig.CapDrop[0] != "ALL" {
			t.Fatalf("unexpected image/runtime configuration: %s", id)
		}
		for _, value := range item.Config.Env {
			if strings.HasPrefix(value, "GEMINI_API_KEY=") && (id == api.ID || value != "GEMINI_API_KEY=") {
				t.Fatal("provider key passed to API or fixture")
			}
		}
		if id != api.ID {
			for _, bindings := range item.NetworkSettings.Ports {
				if len(bindings) != 0 {
					t.Fatal("worker has a published port")
				}
			}
		}
	}
	certFile := filepath.Join(s.temp, "ca.pem")
	s.command("cp", api.ID+":/etc/ssl/certs/ca-certificates.crt", certFile)
	certs, err := os.ReadFile(certFile)
	if err != nil || !x509.NewCertPool().AppendCertsFromPEM(certs) {
		t.Fatal("runtime trust bundle is absent or invalid")
	}

	t.Log("verifying four execution slots, competing workers, and idempotent replay")
	var ids []string
	for i := 0; i < 4; i++ {
		ids = append(ids, s.submit(fmt.Sprintf("concurrent-%d", i)))
	}
	wait(t, "four jobs running", 2*time.Second, func() bool {
		for _, id := range ids {
			if s.job(id).State != jobs.Running {
				return false
			}
		}
		return true
	})
	owners := map[string]bool{}
	for _, id := range ids {
		s.finished(id)
		owners[s.owner(id)] = true
	}
	if len(owners) != 2 || s.submit("concurrent-0") != ids[0] {
		t.Fatal("worker distribution or keyed replay failed")
	}

	t.Log("verifying accepted queued work survives an API restart")
	s.composeCommand("stop", "worker")
	queued := s.submit("queued-restart")
	s.composeCommand("restart", "api")
	// Docker can assign a different ephemeral published port on restart.
	s.refreshAddress()
	wait(t, "API ready after restart", 20*time.Second, func() bool { code, _ := s.request("GET", "/readyz", ""); return code == 200 })
	if s.job(queued).State != jobs.Queued {
		t.Fatal("queued job was lost or unexpectedly executed")
	}
	s.composeCommand("up", "-d", "--no-build", "--wait", "--scale", "worker=2", "worker")
	// Compose may replace containers when reconciling the built image/config.
	workers = s.ids("worker")
	s.finished(queued)

	t.Log("verifying killed worker recovery under the existing ten-second lease")
	recovered := s.submit("killed-worker")
	wait(t, "running before kill", 2*time.Second, func() bool { return s.job(recovered).State == jobs.Running })
	owner := s.owner(recovered)
	s.command("kill", "--signal=KILL", owner)
	if job := s.finished(recovered); job.Attempt != 2 {
		t.Fatalf("same job did not recover on attempt two: %+v", job)
	}

	t.Log("verifying database outage changes readiness, preserves liveness, and recovers")
	s.composeCommand("stop", "postgres")
	for _, path := range []string{"/readyz", "/jobs/" + recovered} {
		code, _ := s.request("GET", path, "")
		if code != 503 {
			t.Fatalf("%s during database outage: %d", path, code)
		}
	}
	if code, _ := s.request("POST", "/jobs", "database-down"); code != 503 {
		t.Fatal("database outage submission did not fail")
	}
	if code, _ := s.request("GET", "/livez", ""); code != 200 {
		t.Fatal("database outage broke liveness")
	}
	wait(t, "Docker unhealthy readiness", 20*time.Second, func() bool { return s.inspect(api.ID).State.Health.Status == "unhealthy" })
	var survivor string
	for _, id := range workers {
		if id != owner {
			survivor = id
		}
	}
	if !s.inspect(survivor).State.Running {
		t.Fatal("worker exited on database outage")
	}
	s.command("exec", survivor, "/app/probe", "-url=http://127.0.0.1:8081/livez")
	s.composeCommand("start", "postgres")
	wait(t, "database readiness recovery", 25*time.Second, func() bool { code, _ := s.request("GET", "/readyz", ""); return code == 200 })
	if s.job(recovered).State != jobs.Succeeded {
		t.Fatal("stored result lost after database restart")
	}
	s.finished(s.submit("after-outage"))

	t.Log("verifying SIGTERM reaches the worker, drains active work, and stops new claims")
	first, second := s.submit("drain-first"), s.submit("drain-second")
	wait(t, "active drain jobs", 2*time.Second, func() bool { return s.job(first).State == jobs.Running && s.job(second).State == jobs.Running })
	remaining := s.submit("after-drain")
	s.command("kill", "--signal=TERM", survivor)
	// The runtime stays alive to finish active work, while readiness is false.
	s.command("exec", survivor, "/app/probe", "-url=http://127.0.0.1:8081/livez")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	probe := exec.CommandContext(ctx, "docker", "exec", survivor, "/app/probe", "-url=http://127.0.0.1:8081/readyz")
	probe.Env = s.env
	if err := probe.Run(); err == nil {
		t.Fatal("draining worker reported ready")
	}
	wait(t, "graceful worker exit", 10*time.Second, func() bool { return !s.inspect(survivor).State.Running })
	if s.inspect(survivor).State.ExitCode != 0 {
		t.Fatal("worker was forcibly killed or exited with failure")
	}
	s.finished(first)
	s.finished(second)
	if s.job(remaining).State != jobs.Queued {
		t.Fatal("draining worker claimed queued work")
	}

	t.Log("verifying Gemini pool startup in a fresh database without model requests")
	s.command("exec", s.ids("postgres")[0], "createdb", "-U", "agentservice", "agentservice_ai")
	settings := map[string]string{
		"CONTAINER_DATABASE_URL": "postgres://agentservice:local-development-only@postgres:5432/agentservice_ai?sslmode=disable",
		"AGENT_EXECUTOR":         "gemini", "GEMINI_MODEL": "test-model", "GEMINI_API_KEY": "test-placeholder-no-provider-calls",
	}
	for i, item := range s.env {
		key, _, _ := strings.Cut(item, "=")
		if value, ok := settings[key]; ok {
			s.env[i] = key + "=" + value
		}
	}
	override := s.compose[len(s.compose)-1]
	if err := os.WriteFile(override, []byte("services:\n  worker:\n    command: [-health-listen=0.0.0.0:8081, -shutdown-grace=40s, -executor=gemini]\n"), 0600); err != nil {
		t.Fatal(err)
	}
	s.composeCommand("up", "-d", "--no-build", "--wait", "--scale", "worker=1", "api", "worker")
	s.refreshAddress()
	for _, value := range s.inspect(s.ids("api")[0]).Config.Env {
		if strings.HasPrefix(value, "GEMINI_API_KEY=") {
			t.Fatal("Gemini API received provider key")
		}
	}
	if code, _ := s.request("GET", "/jobs/"+remaining, ""); code != 404 {
		t.Fatal("Gemini pool did not use its separate database")
	}
	count := strings.TrimSpace(string(s.command("exec", s.ids("postgres")[0], "psql", "-U", "agentservice", "-d", "agentservice_ai", "-Atc", "SELECT count(*) FROM jobs")))
	if count != "0" {
		t.Fatal("unexpected model work in Gemini startup check")
	}
	ctx2, stop := context.WithTimeout(context.Background(), 20*time.Second)
	defer stop()
	preflight := exec.CommandContext(ctx2, "docker", append(append([]string(nil), s.compose...), "run", "--rm", "--no-deps", "-e", "GEMINI_API_KEY=", "worker")...)
	preflight.Env, preflight.Dir = s.env, s.root
	output, err := preflight.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "Gemini configuration failed") {
		t.Fatalf("missing key did not fail before execution: %v %s", err, output)
	}
}
