package lifecycle

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kandev/kandev/internal/agentruntime"
	"github.com/kandev/kandev/internal/task/models"
)

// recoveryWorkspaceInfoProvider serves the remote recovery snapshot and the
// session's checkout manifest, as the task service does after a restart.
type recoveryWorkspaceInfoProvider struct {
	task08RemoteSnapshotProvider
	infos map[string]*WorkspaceInfo
}

func (p *recoveryWorkspaceInfoProvider) GetWorkspaceInfoForSession(_ context.Context, _, sessionID string) (*WorkspaceInfo, error) {
	return p.infos[sessionID], nil
}

// A turn that is running while the backend restarts finishes on the execution
// that remote recovery rebuilds. Its terminal checkpoint is admitted with the
// execution's TurnChangeCheckouts (turnChangeAdmission), so the recovered
// execution must carry the checkouts a fresh launch computes, including the
// plugin branch-scoped directory, or the turn settles checkout_unavailable.
func TestRecoveredRemoteExecutionKeepsLaunchTurnChangeCheckouts(t *testing.T) {
	for _, tc := range []struct {
		runtime      agentruntime.Runtime
		executorType models.ExecutorType
		wantSubpath  string
	}{
		{agentruntime.RuntimeKubernetes, models.ExecutorTypeKubernetes, "acme-widget"},
		{agentruntime.RuntimePluginRemote, models.ExecutorTypePluginRemote, "acme-widget-main"},
	} {
		t.Run(string(tc.runtime), func(t *testing.T) {
			ctx := context.Background()
			repo := newSettingsSourceRecoveryRepository(t, "task", "session")
			mgr := newTestManager(t)
			registerRecoveryTestAgentProfile(t, mgr)
			mgr.SetExecutorRunningWriter(repo)
			e := createTestExecution("execution", "task", "session")
			e.RuntimeName, e.AgentProfileID = tc.runtime, recoveryTestAgentProfileID
			require.NoError(t, mgr.persistExecutorRunningResult(ctx, e))
			row, err := repo.GetExecutorRunningBySessionID(ctx, e.SessionID)
			require.NoError(t, err)
			snapshot := task08RemoteSnapshot(row)
			info := &WorkspaceInfo{
				TaskID: row.TaskID, SessionID: row.SessionID, TaskEnvironmentID: snapshot.TaskEnvironmentID,
				ExecutorType:          string(tc.executorType),
				WorkspaceRepositories: []WorkspaceRepositorySpec{turnChangeWorkspaceRepository("acme-widget", "main", "")},
			}
			mgr.workspaceInfoProvider = &recoveryWorkspaceInfoProvider{infos: map[string]*WorkspaceInfo{row.SessionID: info}}

			// What a fresh launch of the same session binds (manager_execution.go
			// initializeCreatedExecution and the admission refresh).
			launched := &AgentExecution{ID: row.AgentExecutionID, TaskID: row.TaskID, SessionID: row.SessionID,
				TaskEnvironmentID: snapshot.TaskEnvironmentID}
			mgr.populateLaunchTurnChangeCheckouts(ctx, launched)
			require.Len(t, launched.TurnChangeCheckouts, 1)
			require.Equal(t, tc.wantSubpath, launched.TurnChangeCheckouts[0].RepositorySubpath)

			ri := &ExecutorInstance{InstanceID: e.ID, TaskID: e.TaskID, SessionID: e.SessionID,
				RuntimeName: tc.runtime, AgentProfileID: recoveryTestAgentProfileID,
				Metadata: map[string]interface{}{MetadataKeyExecutorType: string(tc.executorType)}}
			recovered, err := mgr.prepareRecoveredRemoteExecution(ctx, row, snapshot, ri)
			require.NoError(t, err)
			require.Equal(t, launched.TurnChangeCheckouts, recovered.TurnChangeCheckouts,
				"recovered execution must carry the launch checkout manifest")
			admission := turnChangeAdmission(recovered, 1, "turn-in-flight")
			require.Equal(t, launched.TurnChangeCheckouts, admission.Checkouts)
		})
	}
}
