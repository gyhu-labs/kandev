package lifecycle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path"
	"strings"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"

	kubeexecutor "github.com/kandev/kandev/internal/agent/kubernetes"
	"github.com/kandev/kandev/internal/common/securityutil"
)

// kubernetesAgentStateRoot holds per-session agent conversation state on the
// workspace volume. The repository checkout owns the mount root, so the
// directory sits under the already Kandev-owned /.kandev/ prefix, which the
// link step adds to the checkout's .git/info/exclude.
const kubernetesAgentStateRoot = kubernetesWorkspacePath + "/.kandev/agent-state"

// kubernetesAgentStatePaths are HOME-relative directories that hold an
// agent's native conversation history. ACP session/load reads Claude's
// transcripts from ~/.claude/projects/<cwd>/<session>.jsonl, so that
// directory alone decides whether a replacement Pod can resume natively.
//
// Only conversation history moves to the PVC. HOME itself, auth.env and
// every credential file (for example ~/.claude/.credentials.json) stay on the
// kandev-auth emptyDir and die with the Pod.
var kubernetesAgentStatePaths = []string{".claude/projects"}

// kubernetesWorkspaceModeSurvivesPod reports whether the workspace volume
// outlives a Pod replacement. An emptyDir workspace dies with the Pod, so
// linking state into it would gain nothing.
func kubernetesWorkspaceModeSurvivesPod(mode string) bool {
	return mode == string(kubeexecutor.WorkspaceModeManagedPVC) || mode == string(kubeexecutor.WorkspaceModeExistingClaim)
}

func kubernetesRequestWorkspaceMode(req *ExecutorCreateRequest) string {
	if req == nil {
		return ""
	}
	mode := getMetadataString(req.Metadata, MetadataKeyKubernetesRuntimeWorkspaceMode)
	if mode == "" {
		mode = getMetadataString(req.Metadata, MetadataKeyKubernetesWorkspaceMode)
	}
	return mode
}

// kubernetesAgentStateDir is stable for a session, so a replacement Pod for
// the same session finds the directory its predecessor wrote.
func kubernetesAgentStateDir(req *ExecutorCreateRequest) string {
	sum := sha256.Sum256([]byte(req.SessionID))
	return path.Join(kubernetesAgentStateRoot, hex.EncodeToString(sum[:16]))
}

// kubernetesAgentStateCommand links each state path under home to the same
// path under stateDir. It is idempotent: a correct link is kept, a stale link
// is replaced, and a real directory (state written before the link existed)
// is moved onto the volume first.
func kubernetesAgentStateCommand(workspace, home, stateDir string, relatives []string) string {
	quoted := make([]string, 0, len(relatives))
	for _, relative := range relatives {
		quoted = append(quoted, shellQuote(relative))
	}
	return fmt.Sprintf(`set -eu
umask 077
workspace=%s
home=%s
state=%s
if git_dir=$(git -c safe.directory='*' -C "$workspace" rev-parse --absolute-git-dir 2>/dev/null); then
  exclude_file="$git_dir/info/exclude"
  mkdir -p "$git_dir/info"
  touch "$exclude_file"
  grep -Fqx '/.kandev/' "$exclude_file" || printf '%%s\n' '/.kandev/' >>"$exclude_file"
fi
mkdir -p "$state"
for relative in %s; do
  target="$state/$relative"
  link="$home/$relative"
  mkdir -p "$target" "$(dirname "$link")"
  if [ -L "$link" ]; then
    [ "$(readlink "$link")" != "$target" ] || continue
    rm -f "$link"
  elif [ -d "$link" ]; then
    cp -R "$link"/. "$target"/
    rm -rf "$link"
  elif [ -e "$link" ]; then
    echo "kandev: agent state path is not a directory" >&2
    exit 73
  fi
  ln -s "$target" "$link"
done`, shellQuote(workspace), shellQuote(home), shellQuote(stateDir), strings.Join(quoted, " "))
}

// linkKubernetesAgentState keeps the session's native conversation history
// on the workspace volume. It must run after the prepare script, because a
// fresh PVC has to be empty for the clone. A failure only costs native resume
// after a Pod loss, so it is logged and never fails the launch.
func (r *KubernetesExecutor) linkKubernetesAgentState(
	ctx context.Context,
	streams *kubeexecutor.StreamOperations,
	req *ExecutorCreateRequest,
	pod *corev1.Pod,
	container, workspaceMode string,
) {
	if req == nil || pod == nil || streams == nil || req.SessionID == "" || !kubernetesWorkspaceModeSurvivesPod(workspaceMode) {
		return
	}
	output := &kubernetesPrepareOutput{}
	err := streams.Exec(ctx, kubeexecutor.ExecRequest{
		Namespace: pod.Namespace, Pod: pod.Name, Container: container,
		Command: []string{"sh", "-c", kubernetesAgentStateCommand(
			kubernetesWorkspacePath, kubernetesSessionHome(req), kubernetesAgentStateDir(req), kubernetesAgentStatePaths,
		)},
		Stdout: output, Stderr: output,
	})
	if err != nil && r.logger != nil {
		r.logger.Warn("failed to keep agent conversation state on the workspace volume; native resume after a Pod loss will not find it",
			zap.String("session_id", req.SessionID), zap.String("pod", pod.Name),
			zap.String("output", securityutil.SanitizeCredentials(output.diagnostic())),
			zap.String("error", securityutil.SanitizeCredentials(err.Error())))
	}
}
