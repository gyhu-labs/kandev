package backendapp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"sync"
	"testing"

	client "github.com/kandev/kandev/internal/agent/runtime/agentctl"
	"github.com/kandev/kandev/internal/agent/runtime/lifecycle"
	"github.com/kandev/kandev/internal/task/models"
)

const adapterScopeBaseCommit = "1111111111111111111111111111111111111111"

// adapterBareRootAgentctl serves a workspace root that is not a repository
// plus one registered repository scope.
type adapterBareRootAgentctl struct {
	mu       sync.Mutex
	requests []string
}

func (a *adapterBareRootAgentctl) snapshot() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.requests)
}

func (a *adapterBareRootAgentctl) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	a.requests = append(a.requests, r.URL.Path+"?"+r.URL.RawQuery)
	a.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.URL.Path == "/api/v1/git/status" && r.URL.Query().Get("repo") == "":
		_, _ = w.Write([]byte(`{"success":false,"status_state":"unavailable","error_code":"repository_unavailable"}`))
	case r.URL.Path == "/api/v1/git/status" && r.URL.Query().Get("repo") == "acme-widget-main":
		_, _ = w.Write([]byte(`{"success":true,"status_state":"ready","files_complete":true,"detail_state":"ready","base_commit":"` + adapterScopeBaseCommit + `"}`))
	case r.URL.Path == "/api/v1/git/turn-changes/scopes":
		_, _ = w.Write([]byte(`{"scopes":["acme-widget-main"]}`))
	default:
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"success":false,"error":"unexpected request"}`))
	}
}

func adapterWithBareRootExecution(t *testing.T, executorType string) (*lifecycleAdapter, *adapterBareRootAgentctl) {
	t.Helper()
	agent := &adapterBareRootAgentctl{}
	server := httptest.NewServer(agent)
	t.Cleanup(server.Close)
	u, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil {
		t.Fatal(err)
	}
	log := newTestLogger()
	agentClient := client.NewClient(u.Hostname(), port, log)
	t.Cleanup(agentClient.Close)
	mgr := lifecycle.NewManager(nil, nil, nil, nil, nil, nil, lifecycle.ExecutorFallbackDeny, t.TempDir(), log)
	if err := mgr.ExecutionStoreForTesting().Add(&lifecycle.AgentExecution{
		ID: "execution-scope", TaskID: "task-scope", SessionID: "session-scope", ExecutorType: executorType,
	}); err != nil {
		t.Fatalf("add execution: %v", err)
	}
	execution, ok := mgr.GetExecutionBySessionID("session-scope")
	if !ok {
		t.Fatal("execution was not registered")
	}
	execution.SetAgentCtlClientForTesting(agentClient)
	return &lifecycleAdapter{mgr: mgr}, agent
}

// Base-commit capture and the orchestrator's status snapshots read through
// lifecycleAdapter.GetGitStatusWithDetails. A plugin session gets its sole
// repository's status.
func TestLifecycleAdapterGitStatusWithDetailsReadsPluginRepositoryScope(t *testing.T) {
	adapter, agent := adapterWithBareRootExecution(t, string(models.ExecutorTypePluginRemote))

	status, err := adapter.GetGitStatusWithDetails(context.Background(), "session-scope")

	if err != nil || status == nil || status.BaseCommit != adapterScopeBaseCommit {
		t.Fatalf("GetGitStatusWithDetails() = %#v, %v; want base %s", status, err, adapterScopeBaseCommit)
	}
	if want := "/api/v1/git/status?fresh=true&details=wait&repo=acme-widget-main"; !slices.Contains(agent.snapshot(), want) {
		t.Fatalf("requests = %v, want %s", agent.snapshot(), want)
	}
}

// A k8s (class S/M) session with the same bare-root layout, e.g. started
// repo-less with one repository attached later, keeps the root-only read.
func TestLifecycleAdapterGitStatusWithDetailsKeepsRootReadForKubernetes(t *testing.T) {
	adapter, agent := adapterWithBareRootExecution(t, string(models.ExecutorTypeKubernetes))

	status, err := adapter.GetGitStatusWithDetails(context.Background(), "session-scope")

	if err == nil || status == nil || status.ErrorCode != "repository_unavailable" {
		t.Fatalf("GetGitStatusWithDetails() = %#v, %v; want the root repository_unavailable result", status, err)
	}
	if got, want := agent.snapshot(), []string{"/api/v1/git/status?fresh=true&details=wait"}; !slices.Equal(got, want) {
		t.Fatalf("requests = %v, want %v", got, want)
	}
}
