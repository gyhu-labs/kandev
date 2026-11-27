package lifecycle

import (
	"context"

	agentctl "github.com/kandev/kandev/internal/agent/runtime/agentctl"
	"github.com/kandev/kandev/internal/task/models"
)

// ExecutionGitStatusWithDetails reads the detailed Git status that session
// base commits and status snapshots come from. Plugin executors clone every
// repository into a branch-scoped directory under a workspace root that is not
// a repository, so for them the read falls back to the sole registered
// repository scope. Every other executor type reads the workspace root only,
// even when its root is bare and a single repository was attached later.
// ExecutorType is fixed when the execution is built; empty means unknown and
// keeps the root-only read.
func ExecutionGitStatusWithDetails(ctx context.Context, execution *AgentExecution, client *agentctl.Client) (*agentctl.GitStatusResult, error) {
	if execution != nil && execution.ExecutorType == string(models.ExecutorTypePluginRemote) {
		return client.GetPrimaryGitStatusWithDetails(ctx)
	}
	return client.GetGitStatusWithDetails(ctx)
}
