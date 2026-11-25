package lifecycle

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kubeexecutor "github.com/kandev/kandev/internal/agent/kubernetes"
)

func runAgentStateCommand(t *testing.T, workspace, home, state string) {
	t.Helper()
	command := kubernetesAgentStateCommand(workspace, home, state, kubernetesAgentStatePaths)
	output, err := exec.Command("sh", "-c", command).CombinedOutput()
	require.NoError(t, err, string(output))
}

func gitInWorkspace(t *testing.T, workspace string, args ...string) string {
	t.Helper()
	command := exec.Command("git", append([]string{"-C", workspace}, args...)...)
	command.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1")
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	return string(output)
}

// The command runs for real against a checkout and two HOMEs, standing in
// for the agent Pod and its replacement on the same PVC.
func TestKubernetesAgentStateSurvivesHomeReplacement(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	root := t.TempDir()
	workspace := filepath.Join(root, "workspace")
	require.NoError(t, os.MkdirAll(workspace, 0o755))
	gitInWorkspace(t, workspace, "init", "-q")
	require.NoError(t, os.WriteFile(filepath.Join(workspace, "README.md"), []byte("repo\n"), 0o644))
	gitInWorkspace(t, workspace, "add", "README.md")
	gitInWorkspace(t, workspace, "-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-q", "-m", "init")
	state := filepath.Join(workspace, ".kandev", "agent-state", "abc")

	// The first Pod already wrote a transcript and holds a credential file
	// before the link exists (state written by an older agentctl).
	firstHome := filepath.Join(root, "run-1", "home")
	transcript := filepath.Join(".claude", "projects", "-workspace", "11111111-2222-3333-4444-555555555555.jsonl")
	require.NoError(t, os.MkdirAll(filepath.Dir(filepath.Join(firstHome, transcript)), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(firstHome, transcript), []byte("{\"type\":\"user\"}\n"), 0o600))
	credentials := filepath.Join(firstHome, ".claude", ".credentials.json")
	require.NoError(t, os.WriteFile(credentials, []byte("{\"token\":\"secret\"}"), 0o600))

	runAgentStateCommand(t, workspace, firstHome, state)
	runAgentStateCommand(t, workspace, firstHome, state) // idempotent

	link := filepath.Join(firstHome, ".claude", "projects")
	target, err := os.Readlink(link)
	require.NoError(t, err, "projects must be a symlink onto the workspace volume")
	require.Equal(t, filepath.Join(state, ".claude", "projects"), target)
	moved, err := os.ReadFile(filepath.Join(state, transcript))
	require.NoError(t, err, "existing transcripts move onto the volume")
	require.Contains(t, string(moved), "user")
	require.FileExists(t, credentials, "credentials stay in the emptyDir HOME")
	require.NoFileExists(t, filepath.Join(state, ".claude", ".credentials.json"), "credentials never reach the PVC")
	info, err := os.Stat(state)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o700), info.Mode().Perm())

	exclude, err := os.ReadFile(filepath.Join(workspace, ".git", "info", "exclude"))
	require.NoError(t, err)
	require.Equal(t, 1, strings.Count(string(exclude), "/.kandev/\n"), "the exclude line is added once")
	require.Empty(t, strings.TrimSpace(gitInWorkspace(t, workspace, "status", "--porcelain", "--untracked-files=all")),
		"agent state must not show up in git status")

	// The replacement Pod starts with an empty HOME on a new emptyDir.
	secondHome := filepath.Join(root, "run-2", "home")
	runAgentStateCommand(t, workspace, secondHome, state)
	resumed, err := os.ReadFile(filepath.Join(secondHome, transcript))
	require.NoError(t, err, "the replacement HOME sees the transcript the lost Pod wrote")
	require.Equal(t, string(moved), string(resumed))
	require.NoFileExists(t, filepath.Join(secondHome, ".claude", ".credentials.json"))
}

func TestKubernetesAgentStateReplacesStaleLink(t *testing.T) {
	root := t.TempDir()
	workspace := filepath.Join(root, "workspace") // not a checkout: no exclude step
	home := filepath.Join(root, "home")
	state := filepath.Join(workspace, ".kandev", "agent-state", "abc")
	require.NoError(t, os.MkdirAll(filepath.Join(home, ".claude"), 0o700))
	require.NoError(t, os.Symlink(filepath.Join(root, "elsewhere"), filepath.Join(home, ".claude", "projects")))

	runAgentStateCommand(t, workspace, home, state)

	target, err := os.Readlink(filepath.Join(home, ".claude", "projects"))
	require.NoError(t, err)
	require.Equal(t, filepath.Join(state, ".claude", "projects"), target)
}

func TestKubernetesAgentStateRefusesNonDirectory(t *testing.T) {
	root := t.TempDir()
	home := filepath.Join(root, "home")
	require.NoError(t, os.MkdirAll(filepath.Join(home, ".claude"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(home, ".claude", "projects"), []byte("x"), 0o600))
	command := kubernetesAgentStateCommand(filepath.Join(root, "workspace"), home, filepath.Join(root, "state"), kubernetesAgentStatePaths)
	err := exec.Command("sh", "-c", command).Run()
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)
	require.Equal(t, 73, exitErr.ExitCode())
}

func TestKubernetesAgentStateDirIsStablePerSession(t *testing.T) {
	first := validKubernetesCreateRequest()
	second := validKubernetesCreateRequest()
	require.Equal(t, kubernetesAgentStateDir(first), kubernetesAgentStateDir(second))
	require.True(t, strings.HasPrefix(kubernetesAgentStateDir(first), "/workspace/.kandev/agent-state/"))
	second.SessionID = "session-2"
	require.NotEqual(t, kubernetesAgentStateDir(first), kubernetesAgentStateDir(second))
	require.NotContains(t, kubernetesAgentStateDir(first), first.SessionID, "the directory name does not expose the session id")
}

func agentStateExecs(execs *recordingKubernetesExec) []recordedKubernetesExec {
	var matched []recordedKubernetesExec
	for _, call := range execs.requests {
		if strings.Contains(strings.Join(call.request.Command, " "), kubernetesAgentStateRoot) {
			matched = append(matched, call)
		}
	}
	return matched
}

func TestKubernetesBootstrapLinksAgentStateAfterPrepareOnPVC(t *testing.T) {
	execs := &recordingKubernetesExec{}
	streams := kubeexecutor.NewStreamOperations(execs, nil)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "kandev-agents", Name: "agent-pod"}}
	executor := NewKubernetesExecutor(nil, newTestLogger())
	req := validKubernetesCreateRequest()
	req.Metadata[metadataKubernetesTaskOwned] = true
	profile := kubeexecutor.ProfileConfig{MainContainer: "kandev-agent"}
	profile.Workspace.Mode = kubeexecutor.WorkspaceModeManagedPVC

	require.NoError(t, executor.bootstrapPod(context.Background(), &kubernetesRuntimeClient{streams: streams},
		req, pod, profile, "nonce", []byte("agentctl")))

	prepareIndex, linkIndex, signalIndex := -1, -1, -1
	for index, recorded := range execs.requests {
		command := strings.Join(recorded.request.Command, " ")
		switch {
		case strings.Contains(command, "sh "+kubernetesPreparePath):
			prepareIndex = index
		case strings.Contains(command, kubernetesAgentStateRoot):
			linkIndex = index
		case strings.Contains(command, kubernetesStartPath) && recorded.request.Stdin == nil:
			signalIndex = index
		}
	}
	require.NotEqual(t, -1, linkIndex, "a PVC workspace links agent state")
	require.Less(t, prepareIndex, linkIndex, "a fresh PVC must stay empty until the clone")
	require.Less(t, linkIndex, signalIndex, "state is linked before the agent starts")
	link := agentStateExecs(execs)[0]
	require.Equal(t, "kandev-agent", link.request.Container)
	command := strings.Join(link.request.Command, " ")
	require.Contains(t, command, kubernetesSessionHome(req))
	require.Contains(t, command, kubernetesAgentStateDir(req))
	require.NotContains(t, command, "auth.env", "the link step never touches auth material")
	require.Empty(t, link.stdin)
}

func TestKubernetesBootstrapSkipsAgentStateOnEmptyDirWorkspace(t *testing.T) {
	execs := &recordingKubernetesExec{}
	executor := NewKubernetesExecutor(nil, newTestLogger())
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "kandev-agents", Name: "agent-pod"}}
	profile := kubeexecutor.ProfileConfig{MainContainer: "kandev-agent"}
	profile.Workspace.Mode = kubeexecutor.WorkspaceModeEmptyDir

	require.NoError(t, executor.bootstrapPod(context.Background(),
		&kubernetesRuntimeClient{streams: kubeexecutor.NewStreamOperations(execs, nil)},
		validKubernetesCreateRequest(), pod, profile, "nonce", []byte("agentctl")))

	require.Empty(t, agentStateExecs(execs))
}

func TestKubernetesSharedSessionLinksAgentStateAndToleratesFailure(t *testing.T) {
	execs := &recordingKubernetesExec{}
	execs.execFunc = func(_ context.Context, request kubeexecutor.ExecRequest) error {
		if strings.Contains(strings.Join(request.Command, " "), kubernetesAgentStateRoot) {
			_, _ = request.Stderr.Write([]byte("mkdir: permission denied"))
			return errors.New("command terminated with exit code 1")
		}
		return nil
	}
	executor := NewKubernetesExecutor(nil, newTestLogger())
	runtime := &kubernetesRuntimeClient{streams: kubeexecutor.NewStreamOperations(execs, nil)}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "kandev-agents", Name: "task-pod"}}
	req := validKubernetesCreateRequest()
	req.Metadata[metadataKubernetesTaskOwned] = true
	req.Metadata[MetadataKeyKubernetesRuntimeWorkspaceMode] = string(kubeexecutor.WorkspaceModeManagedPVC)

	require.NoError(t, executor.prepareSharedKubernetesCredentials(context.Background(), runtime, req, pod, "kandev-agent"),
		"a failed link costs native resume, never the launch")
	require.Len(t, agentStateExecs(execs), 1)
}

func TestKubernetesWorkspaceModeSurvivesPod(t *testing.T) {
	require.True(t, kubernetesWorkspaceModeSurvivesPod(string(kubeexecutor.WorkspaceModeManagedPVC)))
	require.True(t, kubernetesWorkspaceModeSurvivesPod(string(kubeexecutor.WorkspaceModeExistingClaim)))
	require.False(t, kubernetesWorkspaceModeSurvivesPod(string(kubeexecutor.WorkspaceModeEmptyDir)))
	require.False(t, kubernetesWorkspaceModeSurvivesPod(""))
}
