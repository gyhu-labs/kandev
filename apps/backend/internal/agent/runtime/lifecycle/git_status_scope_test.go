package lifecycle

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	agentctl "github.com/kandev/kandev/internal/agent/runtime/agentctl"
	"github.com/kandev/kandev/internal/task/models"
)

const bareRootScopeBaseCommit = "1111111111111111111111111111111111111111"

// bareRootAgentctl serves the layout where the workspace root is not a Git
// repository and agentctl registers one named repository scope. Plugin
// executors always look like this. A k8s/Docker/sprites/local session that
// started repo-less and had one repository attached later looks the same.
type bareRootAgentctl struct {
	mu       sync.Mutex
	requests []string
}

func (a *bareRootAgentctl) snapshot() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.requests)
}

func (a *bareRootAgentctl) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	a.requests = append(a.requests, r.URL.Path+"?"+r.URL.RawQuery)
	a.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.URL.Path == "/api/v1/git/status" && r.URL.Query().Get("repo") == "":
		_, _ = w.Write([]byte(`{"success":false,"status_state":"unavailable","error_code":"repository_unavailable"}`))
	case r.URL.Path == "/api/v1/git/status" && r.URL.Query().Get("repo") == "acme-widget-main":
		_, _ = w.Write([]byte(`{"success":true,"status_state":"ready","files_complete":true,"detail_state":"ready","base_commit":"` + bareRootScopeBaseCommit + `"}`))
	case r.URL.Path == "/api/v1/git/turn-changes/scopes":
		_, _ = w.Write([]byte(`{"scopes":["acme-widget-main"]}`))
	default:
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"success":false,"error":"unexpected request"}`))
	}
}

func newBareRootAgentctlClient(t *testing.T) (*agentctl.Client, *bareRootAgentctl) {
	t.Helper()
	agent := &bareRootAgentctl{}
	server := httptest.NewServer(agent)
	t.Cleanup(server.Close)
	u, err := url.Parse(server.URL)
	require.NoError(t, err)
	port, err := strconv.Atoi(u.Port())
	require.NoError(t, err)
	client := agentctl.NewClient(u.Hostname(), port, newTestLogger())
	t.Cleanup(client.Close)
	return client, agent
}

func TestExecutionGitStatusWithDetailsReadsSoleScopeOnlyForPluginExecutors(t *testing.T) {
	client, agent := newBareRootAgentctlClient(t)

	status, err := ExecutionGitStatusWithDetails(context.Background(),
		&AgentExecution{ExecutorType: string(models.ExecutorTypePluginRemote)}, client)

	require.NoError(t, err)
	require.Equal(t, bareRootScopeBaseCommit, status.BaseCommit)
	require.Equal(t, []string{
		"/api/v1/git/status?fresh=true&details=wait",
		"/api/v1/git/turn-changes/scopes?",
		"/api/v1/git/status?fresh=true&details=wait&repo=acme-widget-main",
	}, agent.snapshot())
}

// The same bare-root layout on any other executor keeps the root-only read
// and its repository_unavailable result.
func TestExecutionGitStatusWithDetailsKeepsRootReadOutsidePluginExecutors(t *testing.T) {
	for _, executorType := range []string{
		"",
		string(models.ExecutorTypeLocal),
		string(models.ExecutorTypeWorktree),
		string(models.ExecutorTypeLocalDocker),
		string(models.ExecutorTypeRemoteDocker),
		string(models.ExecutorTypeSprites),
		string(models.ExecutorTypeSSH),
		string(models.ExecutorTypeKubernetes),
		string(models.ExecutorTypeMockRemote),
	} {
		t.Run("executor="+executorType, func(t *testing.T) {
			client, agent := newBareRootAgentctlClient(t)

			status, err := ExecutionGitStatusWithDetails(context.Background(), &AgentExecution{ExecutorType: executorType}, client)

			require.Error(t, err)
			require.NotNil(t, status)
			require.Equal(t, "repository_unavailable", status.ErrorCode)
			require.Equal(t, []string{"/api/v1/git/status?fresh=true&details=wait"}, agent.snapshot())
		})
	}
	t.Run("nil execution", func(t *testing.T) {
		client, agent := newBareRootAgentctlClient(t)
		_, err := ExecutionGitStatusWithDetails(context.Background(), nil, client)
		require.Error(t, err)
		require.Equal(t, []string{"/api/v1/git/status?fresh=true&details=wait"}, agent.snapshot())
	})
}
