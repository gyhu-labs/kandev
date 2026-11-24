package lifecycle

import (
	"context"
	"io"
	"net"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	kubeexecutor "github.com/kandev/kandev/internal/agent/kubernetes"
)

// bridgingKubernetesExec answers `agentctl tcp-stdio` exec streams by
// bridging them to local test agentctl servers, and records every other exec
// through the shared recording fake.
type bridgingKubernetesExec struct {
	files *recordingKubernetesExec
	ports map[string]uint16

	mu      sync.Mutex
	bridges []kubeexecutor.ExecRequest
}

func (b *bridgingKubernetesExec) Exec(ctx context.Context, request kubeexecutor.ExecRequest) error {
	if len(request.Command) < 2 || request.Command[1] != kubeexecutor.TCPStdioSubcommand {
		return b.files.Exec(ctx, request)
	}
	b.mu.Lock()
	b.bridges = append(b.bridges, request)
	b.mu.Unlock()
	port := b.ports[request.Command[len(request.Command)-1]]
	upstream, err := (&net.Dialer{}).DialContext(ctx, "tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(int(port))))
	if err != nil {
		return err
	}
	defer func() { _ = upstream.Close() }()
	stop := context.AfterFunc(ctx, func() { _ = upstream.Close() })
	defer stop()
	go func() {
		_, _ = io.Copy(upstream, request.Stdin)
		_ = upstream.(*net.TCPConn).CloseWrite()
	}()
	_, err = io.Copy(request.Stdout, upstream)
	return err
}

func (b *bridgingKubernetesExec) bridgeRequests() []kubeexecutor.ExecRequest {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]kubeexecutor.ExecRequest(nil), b.bridges...)
}

func TestKubernetesCreateInstanceReachesAgentctlOverExecTransport(t *testing.T) {
	controlPort := startKubernetesAgentctlServer(t, true, 41001)
	instancePort := startKubernetesAgentctlServer(t, false, 0)
	resources := &fakeKubernetesResources{}
	files := &recordingKubernetesExec{}
	execs := &bridgingKubernetesExec{files: files, ports: map[string]uint16{
		strconv.Itoa(int(kubeexecutor.DefaultAgentctlPort)): controlPort,
		"41001": instancePort,
	}}
	portForwards := &recordingKubernetesForwarder{}
	var configured kubeexecutor.ExecutorConfig
	executor := NewKubernetesExecutor(nil, newTestLogger())
	executor.clientFactory = func(config kubeexecutor.ExecutorConfig) (*kubernetesRuntimeClient, error) {
		configured = config
		return &kubernetesRuntimeClient{
			resources: resources,
			streams: kubeexecutor.NewStreamOperations(
				execs, newKubernetesForwarder(config, execs, portForwards),
			),
		}, nil
	}
	executor.resolveBinary = func(context.Context, *ExecutorCreateRequest, kubeexecutor.Platform) ([]byte, error) {
		return []byte("agentctl-binary"), nil
	}
	req := validKubernetesCreateRequest()
	req.Metadata[MetadataKeyKubernetesTransport] = "exec"

	instance, err := executor.CreateInstance(context.Background(), req)

	require.NoError(t, err)
	require.NotNil(t, instance.Client)
	require.Equal(t, "handshake-token", instance.AuthToken)
	require.Equal(t, kubeexecutor.StreamTransportExec, configured.EffectiveTransport())
	require.Empty(t, portForwards.remotePorts(), "exec transport must not open pods/portforward")
	bridges := execs.bridgeRequests()
	require.NotEmpty(t, bridges)
	remotePorts := map[string]bool{}
	for _, bridge := range bridges {
		require.Equal(t, "kandev-agent", bridge.Container)
		require.Equal(t, kubernetesAgentctlPath, bridge.Command[0])
		remotePorts[bridge.Command[len(bridge.Command)-1]] = true
	}
	require.True(t, remotePorts[strconv.Itoa(int(kubeexecutor.DefaultAgentctlPort))], "control port bridged")
	require.True(t, remotePorts["41001"], "instance port bridged")
	require.NoError(t, instance.Client.Health(context.Background()))
	require.NotNil(t, executor.closeKubernetesSession(instance.InstanceID), "the exec forward belongs to the session")
}

func TestNewKubernetesForwarderDefaultsToPortForward(t *testing.T) {
	portForwarder := &recordingKubernetesForwarder{}
	selected := newKubernetesForwarder(kubeexecutor.ExecutorConfig{}, &recordingKubernetesExec{}, portForwarder)
	require.Same(t, portForwarder, selected)

	selected = newKubernetesForwarder(
		kubeexecutor.ExecutorConfig{Transport: kubeexecutor.StreamTransportExec}, &recordingKubernetesExec{}, portForwarder,
	)
	execForwarder, ok := selected.(*kubeexecutor.ExecPortForwarder)
	require.True(t, ok)
	require.Equal(t, kubernetesAgentctlPath, execForwarder.BridgePath)
}

func TestKubernetesBootstrapContainerFindsAgentctlContainer(t *testing.T) {
	pod := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{
		{Name: "docker-engine", Command: []string{"dockerd"}},
		{Name: "kandev-agent", Command: []string{"sh", "-c"}, Args: []string{kubernetesBootstrapCommand()}},
	}}}
	require.Equal(t, "kandev-agent", kubernetesBootstrapContainer(pod))
	require.Empty(t, kubernetesBootstrapContainer(&corev1.Pod{}))
	require.Empty(t, kubernetesBootstrapContainer(nil))
}

func TestKubernetesExecutorConfigFromMetadataReadsTransport(t *testing.T) {
	metadata := validKubernetesCreateRequest().Metadata
	config, err := kubernetesExecutorConfigFromMetadata(metadata)
	require.NoError(t, err)
	require.Equal(t, kubeexecutor.StreamTransportPortForward, config.EffectiveTransport())

	metadata[MetadataKeyKubernetesTransport] = "exec"
	config, err = kubernetesExecutorConfigFromMetadata(metadata)
	require.NoError(t, err)
	require.Equal(t, kubeexecutor.StreamTransportExec, config.EffectiveTransport())

	metadata[MetadataKeyKubernetesTransport] = "vsock"
	_, err = kubernetesExecutorConfigFromMetadata(metadata)
	require.ErrorContains(t, err, "config.transport")
}

func TestKubernetesTransportIsTrustedConnectionMetadata(t *testing.T) {
	require.True(t, isTrustedExecutorConfigKey(MetadataKeyKubernetesTransport))
	require.Contains(t, kubernetesConnectionMetadataKeys[:], MetadataKeyKubernetesTransport)
	require.True(t, persistentMetadataKeys[MetadataKeyKubernetesTransport])
}
