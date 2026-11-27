package lifecycle

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kandev/kandev/internal/agent/executor"
	"github.com/kandev/kandev/internal/agentruntime"
	"github.com/kandev/kandev/internal/task/models"
)

// turnChangeWorkspaceRepository mirrors the spec the task service builds for a
// workspace repository: RepositorySubpath is the sanitized repository name with
// no branch suffix (task/service/service_turns.go workspaceRepositorySpec).
func turnChangeWorkspaceRepository(name, baseBranch, checkoutBranch string) WorkspaceRepositorySpec {
	return WorkspaceRepositorySpec{
		TaskEnvironmentRepoID: "env-repo-" + name, TaskRepositoryID: "task-repo-" + name,
		RepositoryID: "repo-" + name, WorktreeID: "worktree-" + name, RepoName: name,
		BaseBranch: baseBranch, CheckoutBranch: checkoutBranch,
		RepositorySubpath: name, RepositorySubpathKnown: true,
	}
}

func legacyTurnChangeCheckout(name string) TurnChangeCheckout {
	return TurnChangeCheckout{
		ID: "env-repo-" + name, EnvironmentRepoID: "env-repo-" + name,
		TaskRepositoryID: "task-repo-" + name, RepositoryID: "repo-" + name,
		WorktreeID: "worktree-" + name, DisplayName: name,
		RepositorySubpath: name, RepositorySubpathKnown: true,
	}
}

// Plugin executors clone every repository, the primary one included, into a
// branch-scoped directory under a workspace root that is not itself a Git
// repository. agentctl registers that directory name as the repository scope,
// so the turn-change checkout must carry the same name or the start checkpoint
// fails with checkout_unavailable.
func TestTurnChangeCheckoutsUsePluginBranchScopedDirectories(t *testing.T) {
	repositories := []WorkspaceRepositorySpec{
		turnChangeWorkspaceRepository("acme-widget", "main", ""),
		turnChangeWorkspaceRepository("acme-gadget", "main", "feature/next"),
	}

	checkouts := turnChangeCheckoutsFromWorkspaceRepositories(repositories, string(models.ExecutorTypePluginRemote))

	// The directories agentctl materializes for the same plugin launch.
	projection, err := remoteWorkspaceProjectionFromLaunch(&LaunchRequest{
		ExecutorType: string(models.ExecutorTypePluginRemote),
		Repositories: []RepoLaunchSpec{
			{RepoName: "acme-widget", BaseBranch: "main", RepositoryURL: "https://git.example.invalid/acme/widget.git"},
			{RepoName: "acme-gadget", BaseBranch: "main", CheckoutBranch: "feature/next", RepositoryURL: "https://git.example.invalid/acme/gadget.git"},
		},
	}, true)
	require.NoError(t, err)
	require.Len(t, projection, 2)
	require.Len(t, checkouts, 2)
	for index, checkout := range checkouts {
		require.Equal(t, projection[index].Destination, checkout.RepositorySubpath, "checkout %d must name the materialized directory", index)
		require.True(t, checkout.RepositorySubpathKnown)
	}

	want := []TurnChangeCheckout{legacyTurnChangeCheckout("acme-widget"), legacyTurnChangeCheckout("acme-gadget")}
	want[0].RepositorySubpath = "acme-widget-main"
	want[1].RepositorySubpath = "acme-gadget-feature-next"
	require.Equal(t, want, checkouts, "only the subpath changes; identity and display name stay as recorded")
}

// Every other executor type keeps the recorded subpath. Class S/M (k8s) and
// Docker keep a single repository at the workspace root, which agentctl
// registers as the root scope, and local executors key by repository name.
func TestTurnChangeCheckoutsKeepRecordedSubpathOutsidePluginExecutors(t *testing.T) {
	repositories := []WorkspaceRepositorySpec{
		turnChangeWorkspaceRepository("acme-widget", "main", ""),
		turnChangeWorkspaceRepository("acme-gadget", "main", "feature/next"),
	}
	want := []TurnChangeCheckout{legacyTurnChangeCheckout("acme-widget"), legacyTurnChangeCheckout("acme-gadget")}
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
			require.Equal(t, want[:1], turnChangeCheckoutsFromWorkspaceRepositories(repositories[:1], executorType), "single repository")
			require.Equal(t, want, turnChangeCheckoutsFromWorkspaceRepositories(repositories, executorType), "multiple repositories")
		})
	}
}

// A repository with no branch has no safe branch-scoped directory. The checkout
// keeps its recorded subpath, so capture reports it unavailable exactly as before.
func TestTurnChangeCheckoutsKeepRecordedSubpathWithoutPluginDirectory(t *testing.T) {
	checkouts := turnChangeCheckoutsFromWorkspaceRepositories(
		[]WorkspaceRepositorySpec{turnChangeWorkspaceRepository("acme-widget", "", "")},
		string(models.ExecutorTypePluginRemote),
	)
	require.Equal(t, []TurnChangeCheckout{legacyTurnChangeCheckout("acme-widget")}, checkouts)
	require.Nil(t, turnChangeCheckoutsFromWorkspaceRepositories(nil, string(models.ExecutorTypePluginRemote)))
}

// Call site manager_turn_changes.go: the checkout manifest refreshed at prompt
// admission uses the session's executor type.
func TestLaunchTurnChangeCheckoutsUsePluginExecutorLayout(t *testing.T) {
	manager := newTestManager(t)
	provider := &mockWorkspaceInfoProvider{infos: map[string]*WorkspaceInfo{
		"session-plugin-checkouts": {
			TaskID: "task-plugin-checkouts", SessionID: "session-plugin-checkouts",
			TaskEnvironmentID: "environment-plugin-checkouts", ExecutorType: string(models.ExecutorTypePluginRemote),
			WorkspaceRepositories: []WorkspaceRepositorySpec{turnChangeWorkspaceRepository("acme-widget", "main", "")},
		},
	}}
	manager.SetWorkspaceInfoProvider(provider)
	execution := &AgentExecution{
		ID: "execution-plugin-checkouts", TaskID: "task-plugin-checkouts",
		SessionID: "session-plugin-checkouts", TaskEnvironmentID: "environment-plugin-checkouts",
	}

	manager.populateLaunchTurnChangeCheckouts(context.Background(), execution)

	require.Len(t, execution.TurnChangeCheckouts, 1)
	require.Equal(t, "acme-widget-main", execution.TurnChangeCheckouts[0].RepositorySubpath)
	require.True(t, execution.TurnChangeCheckouts[0].RepositorySubpathKnown)
}

// Call site manager_execution.go: a newly created execution starts with the
// same plugin layout before the first admission refresh.
func TestCreatedPluginExecutionTurnChangeCheckoutsUseBranchScopedDirectory(t *testing.T) {
	backend := &MockExecutor{name: executor.NameStandalone}
	manager, _ := newRuntimeSelectionManager(t, backend)
	request := &ExecutorCreateRequest{
		InstanceID: "execution-plugin-created", TaskID: "task-plugin-created",
		SessionID: "session-plugin-created", TaskEnvironmentID: "environment-plugin-created",
		ExecutorType: string(models.ExecutorTypePluginRemote), WorkspacePath: t.TempDir(),
	}
	info := &WorkspaceInfo{
		TaskID: request.TaskID, SessionID: request.SessionID, TaskEnvironmentID: request.TaskEnvironmentID,
		WorkspacePath: request.WorkspacePath, ExecutorType: string(models.ExecutorTypePluginRemote),
		WorkspaceRepositories: []WorkspaceRepositorySpec{turnChangeWorkspaceRepository("acme-widget", "main", "")},
	}
	runtimeInstance := &ExecutorInstance{
		InstanceID: request.InstanceID, TaskID: request.TaskID, SessionID: request.SessionID,
		RuntimeName: agentruntime.RuntimeStandalone, WorkspacePath: request.WorkspacePath,
	}

	execution := manager.initializeCreatedExecution(
		context.Background(), request.TaskID, info, backend, runtimeInstance,
		&executionCreatePreparation{request: request},
	)

	require.Len(t, execution.TurnChangeCheckouts, 1)
	require.Equal(t, "acme-widget-main", execution.TurnChangeCheckouts[0].RepositorySubpath)
}
