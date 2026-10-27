//go:build kubernetes

// Package kubetest verifies the real local Kubernetes deployment. It never uses
// the default kubeconfig or user credentials and never submits Gemini work.
package kubetest

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/neev-rao/distributed-agent-service/internal/jobs"
)

const fakeProviderKey = "test-placeholder-never-call-provider"

type metadata struct{ Name, UID string }
type container struct {
	Name, Image string
	Env         []struct {
		Name      string
		ValueFrom struct{ SecretKeyRef struct{ Name, Key string } }
	}
}
type pod struct {
	Metadata metadata
	Spec     struct {
		Containers, InitContainers    []container
		TerminationGracePeriodSeconds int
	}
	Status struct {
		Conditions        []struct{ Type, Status string }
		ContainerStatuses []struct{ ImageID string }
	}
}

func (p pod) ready() bool {
	for _, c := range p.Status.Conditions {
		if c.Type == "Ready" && c.Status == "True" {
			return true
		}
	}
	return false
}

type cluster struct {
	t                        *testing.T
	root, name, state, image string
	env                      []string
}

func (c *cluster) command(env []string, binary string, args ...string) ([]byte, error) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Dir, cmd.Env = c.root, env
	output, err := cmd.CombinedOutput()
	if bytes.Contains(output, []byte(fakeProviderKey)) {
		c.t.Fatal("test provider key leaked to command output")
	}
	return output, err
}
func (c *cluster) must(binary string, args ...string) []byte {
	c.t.Helper()
	out, err := c.command(c.env, binary, args...)
	if err != nil {
		c.t.Fatalf("%s failed: %v\n%s", binary, err, out)
	}
	return out
}
func (c *cluster) script(name string, args ...string) []byte {
	return c.must("bash", append([]string{filepath.Join(c.root, "scripts", name)}, args...)...)
}
func (c *cluster) kube(args ...string) []byte {
	return c.must("kubectl", append([]string{"--kubeconfig", filepath.Join(c.state, "kubeconfig"), "--context", "kind-" + c.name}, args...)...)
}
func (c *cluster) pods(namespace, selector string) []pod {
	c.t.Helper()
	var list struct{ Items []pod }
	if err := json.Unmarshal(c.kube("get", "pods", "-n", namespace, "-l", selector, "-o", "json"), &list); err != nil {
		c.t.Fatal(err)
	}
	return list.Items
}
func replaceEnv(base []string, values map[string]string) []string {
	result := make([]string, 0, len(base)+len(values))
	for _, v := range base {
		key, _, _ := strings.Cut(v, "=")
		if _, ok := values[key]; !ok {
			result = append(result, v)
		}
	}
	for key, v := range values {
		result = append(result, key+"="+v)
	}
	return result
}

func newCluster(t *testing.T) *cluster {
	t.Helper()
	for _, tool := range []string{"docker", "kind", "kubectl", "bash"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Fatalf("Explicit Kubernetes tests require %s", tool)
		}
	}
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	var nonce [6]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	suffix := hex.EncodeToString(nonce[:])
	name := "das-k8s-verify-" + suffix
	temp := t.TempDir()
	dbFile, geminiFile, sentinel := filepath.Join(temp, "database.env"), filepath.Join(temp, "gemini.env"), filepath.Join(temp, "unrelated-kubeconfig")
	if err = os.WriteFile(dbFile, []byte("POSTGRES_PASSWORD=local-development-only\nDATABASE_URL=postgres://agentservice:local-development-only@postgres:5432/agentservice?sslmode=disable\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(geminiFile, []byte("GEMINI_MODEL=test-model\nGEMINI_API_KEY="+fakeProviderKey+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(sentinel, []byte("unrelated configuration must remain untouched"), 0600); err != nil {
		t.Fatal(err)
	}
	c := &cluster{t: t, root: root, name: name, state: filepath.Join(root, ".local", "kubernetes", name), image: "distributed-agent-service:k8s-verify-" + suffix}
	c.env = replaceEnv(os.Environ(), map[string]string{"CLUSTER_NAME": name, "NAMESPACE": "", "DATABASE_ENV_FILE": dbFile, "GEMINI_ENV_FILE": geminiFile, "APP_IMAGE": c.image, "SKIP_BUILD": "0", "KUBECONFIG": sentinel, "GEMINI_API_KEY": "", "GEMINI_MODEL": ""})
	t.Cleanup(func() {
		if t.Failed() {
			if output, err := c.command(c.env, "kubectl", "--kubeconfig", filepath.Join(c.state, "kubeconfig"), "--context", "kind-"+name, "get", "pods,pvc", "-A"); err == nil {
				t.Logf("test cluster status:\n%s", output)
			}
		}
		if _, err := os.Stat(filepath.Join(c.state, "owned")); err == nil {
			c.script("kind-destroy.sh", "--delete-data")
		}
		if _, err := c.command(c.env, "docker", "image", "inspect", c.image); err == nil {
			c.must("docker", "image", "rm", c.image)
		}
		data, err := os.ReadFile(sentinel)
		if err != nil || string(data) != "unrelated configuration must remain untouched" {
			t.Error("unrelated kubeconfig was changed")
		}
	})
	t.Log("creating a uniquely named cluster with a dedicated kubeconfig")
	c.script("kind-up.sh")
	t.Log("building/loading the image and deploying the fixture overlay")
	c.script("kind-deploy.sh", "fixture")
	return c
}

func (c *cluster) forward(namespace string) string {
	c.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, "kubectl", "--kubeconfig", filepath.Join(c.state, "kubeconfig"), "--context", "kind-"+c.name, "-n", namespace, "port-forward", "--address=127.0.0.1", "service/api", ":8080")
	cmd.Env = c.env
	cmd.Stderr = io.Discard
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		c.t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		c.t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	c.t.Cleanup(func() { cancel(); <-done })
	ready := make(chan int, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			var port int
			if n, _ := fmt.Sscanf(scanner.Text(), "Forwarding from 127.0.0.1:%d ->", &port); n == 1 {
				select {
				case ready <- port:
				default:
				}
			}
		}
		close(ready)
	}()
	select {
	case port, ok := <-ready:
		if !ok {
			c.t.Fatal("port-forward exited before listening")
		}
		return fmt.Sprintf("http://127.0.0.1:%d", port)
	case <-time.After(30 * time.Second):
		c.t.Fatal("port-forward did not start")
	}
	return ""
}

func request(t *testing.T, base, method, path, key string) (int, []byte) {
	t.Helper()
	body := ""
	if method == "POST" {
		body = `{"scenario_id":"checkout-db-timeout"}`
	}
	req, err := http.NewRequest(method, base+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	response, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		return 0, nil
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 128<<10))
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, data
}
func submit(t *testing.T, base, key string) string {
	t.Helper()
	code, raw := request(t, base, "POST", "/jobs", key)
	var item struct {
		ID string `json:"id"`
	}
	if json.Unmarshal(raw, &item) != nil || code != 202 || item.ID == "" {
		t.Fatalf("submit %d %s", code, raw)
	}
	return item.ID
}
func lookup(t *testing.T, base, id string) jobs.Job {
	t.Helper()
	code, raw := request(t, base, "GET", "/jobs/"+id, "")
	var job jobs.Job
	if code != 200 || json.Unmarshal(raw, &job) != nil {
		t.Fatalf("lookup %d %s", code, raw)
	}
	return job
}
func wait(t *testing.T, label string, duration time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(duration)
	defer deadline.Stop()
	ticker := time.NewTicker(150 * time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("timed out: " + label)
		case <-ticker.C:
		}
	}
}

func TestLocalKubernetesDeployment(t *testing.T) {
	c := newCluster(t)
	base := c.forward("agent-fixture")
	workers := c.pods("agent-fixture", "app=agent-worker")
	if len(workers) != 2 || !workers[0].ready() || !workers[1].ready() {
		t.Fatal("two workers not ready")
	}
	api := c.pods("agent-fixture", "app=agent-api")[0]
	if api.Spec.TerminationGracePeriodSeconds != 15 || workers[0].Spec.TerminationGracePeriodSeconds != 45 {
		t.Fatal("termination budgets do not cover application drain")
	}
	for _, p := range append(workers, api) {
		if len(p.Spec.InitContainers) != 1 || p.Spec.InitContainers[0].Name != "migrate" {
			t.Fatal("migration init container missing")
		}
		for _, container := range append(p.Spec.Containers, p.Spec.InitContainers...) {
			if container.Image != c.image {
				t.Fatal("inconsistent application image")
			}
			for _, env := range container.Env {
				if env.Name == "GEMINI_API_KEY" {
					t.Fatal("fixture deployment received a provider key")
				}
			}
		}
	}
	t.Log("verifying competing workers and four execution slots")
	c.kube("patch", "deployment", "worker", "-n", "agent-fixture", "--type=json", "-p", `[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"-fixture-delay=2s"}]`)
	c.kube("rollout", "status", "deployment/worker", "-n", "agent-fixture", "--timeout=180s")
	var ids []string
	for i := 0; i < 4; i++ {
		ids = append(ids, submit(t, base, fmt.Sprintf("job-%d", i)))
	}
	wait(t, "four running jobs", 2*time.Second, func() bool {
		for _, id := range ids {
			if lookup(t, base, id).State != jobs.Running {
				return false
			}
		}
		return true
	})
	wait(t, "completed investigations", 15*time.Second, func() bool {
		for _, id := range ids {
			if lookup(t, base, id).State != jobs.Succeeded {
				return false
			}
		}
		return true
	})
	owners := map[string]bool{}
	for _, p := range c.pods("agent-fixture", "app=agent-worker") {
		for _, line := range strings.Split(string(c.kube("logs", p.Metadata.Name, "-n", "agent-fixture", "-c", "worker")), "\n") {
			var event struct {
				Message string `json:"msg"`
				JobID   string `json:"job_id"`
			}
			if json.Unmarshal([]byte(line), &event) == nil && event.Message == "job_claimed" {
				for _, id := range ids {
					if event.JobID == id {
						owners[p.Metadata.Name] = true
					}
				}
			}
		}
	}
	if len(owners) != 2 || submit(t, base, "job-0") != ids[0] {
		t.Fatal("distribution or keyed replay failed")
	}
	result := lookup(t, base, ids[0]).Result
	t.Log("reapplying manifests and restarting the API without losing a result")
	c.kube("apply", "-f", filepath.Join(c.state, "agent-fixture.yaml"))
	c.kube("rollout", "status", "deployment/worker", "-n", "agent-fixture", "--timeout=180s")
	c.kube("rollout", "restart", "deployment/api", "-n", "agent-fixture")
	c.kube("rollout", "status", "deployment/api", "-n", "agent-fixture", "--timeout=180s")
	base = c.forward("agent-fixture")
	if got := lookup(t, base, ids[0]); got.Result == nil || got.Result.Diagnosis != result.Diagnosis {
		t.Fatal("result lost on API replacement")
	}
	before := c.pods("agent-fixture", "app=agent-postgres")[0]
	claimBefore := string(c.kube("get", "pvc", "data-postgres-0", "-n", "agent-fixture", "-o", "jsonpath={.metadata.uid}:{.spec.volumeName}"))
	t.Log("replacing the PostgreSQL Pod and checking the same persistent volume")
	c.kube("delete", "pod", "postgres-0", "-n", "agent-fixture", "--wait=true")
	wait(t, "replacement database Pod", 180*time.Second, func() bool {
		pods := c.pods("agent-fixture", "app=agent-postgres")
		return len(pods) == 1 && pods[0].Metadata.UID != before.Metadata.UID && pods[0].ready()
	})
	wait(t, "API storage reconnect", 30*time.Second, func() bool { code, _ := request(t, base, "GET", "/readyz", ""); return code == 200 })
	claimAfter := string(c.kube("get", "pvc", "data-postgres-0", "-n", "agent-fixture", "-o", "jsonpath={.metadata.uid}:{.spec.volumeName}"))
	if claimBefore != claimAfter || lookup(t, base, ids[0]).Result.Diagnosis != result.Diagnosis {
		t.Fatal("persistent storage or result changed")
	}
	c.env = replaceEnv(c.env, map[string]string{"SKIP_BUILD": "1"})
	t.Log("verifying an isolated idle Gemini pool without provider requests")
	c.script("kind-deploy.sh", "gemini")
	geminiBase := c.forward("agent-gemini")
	if code, _ := request(t, geminiBase, "GET", "/jobs/"+ids[0], ""); code != 404 {
		t.Fatal("Gemini namespace shares fixture records")
	}
	for _, p := range c.pods("agent-gemini", "app=agent-api") {
		for _, container := range append(p.Spec.Containers, p.Spec.InitContainers...) {
			for _, env := range container.Env {
				if env.Name == "GEMINI_API_KEY" {
					t.Fatal("API received Gemini key")
				}
			}
		}
	}
	count := strings.TrimSpace(string(c.kube("exec", "postgres-0", "-n", "agent-gemini", "--", "psql", "-U", "agentservice", "-d", "agentservice", "-Atc", "SELECT count(*) FROM jobs")))
	if count != "0" {
		t.Fatal("unexpected model work")
	}
	var geminiFile string
	for _, entry := range c.env {
		if value, ok := strings.CutPrefix(entry, "GEMINI_ENV_FILE="); ok {
			geminiFile = value
		}
	}
	if err := os.WriteFile(geminiFile, []byte("GEMINI_MODEL=other-model\nGEMINI_API_KEY="+fakeProviderKey+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	output, err := c.command(c.env, "bash", filepath.Join(c.root, "scripts", "kind-deploy.sh"), "gemini")
	if err == nil || !bytes.Contains(output, []byte("fresh namespace")) {
		t.Fatal("in-place profile change was not rejected")
	}
}
