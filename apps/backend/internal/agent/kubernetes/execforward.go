package kubernetes

import (
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"sync"

	"k8s.io/apimachinery/pkg/util/validation"
)

// TCPStdioSubcommand is the agentctl subcommand that bridges its stdin and
// stdout to one TCP port on Pod loopback.
const TCPStdioSubcommand = "tcp-stdio"

// ExecPortForwarder implements PortForwarder over pods/exec. It listens on a
// process-local loopback port exactly like the port-forward transport, and
// carries every accepted connection over its own exec stream into
// `<BridgePath> tcp-stdio --port <remote port>` inside the target container.
// The bridge dials only 127.0.0.1, so agentctl stays bound to Pod loopback.
type ExecPortForwarder struct {
	Executor   PodExecutor
	BridgePath string
}

// Forward starts a loopback listener for the request. The returned session is
// ready as soon as the listener is bound; each connection opens its exec
// stream lazily, so an unreachable agentctl surfaces as a closed connection.
func (f *ExecPortForwarder) Forward(ctx context.Context, request PortForwardRequest) (PortForwardSession, error) {
	if f == nil || f.Executor == nil || f.BridgePath == "" {
		return nil, errors.New("exec port forwarder is not configured")
	}
	if len(validation.IsDNS1123Label(request.Container)) > 0 {
		return nil, fieldError("port_forward.container", "must name the agentctl container for exec transport")
	}
	if request.RemotePort == 0 {
		return nil, fieldError("port_forward.remote_port", "must be non-zero")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	localAddress := request.LocalAddress
	if localAddress == "" {
		localAddress = loopbackAddress
	}
	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(
		ctx, "tcp", net.JoinHostPort(localAddress, strconv.Itoa(int(request.LocalPort))),
	)
	if err != nil {
		return nil, err
	}
	tcpAddress, ok := listener.Addr().(*net.TCPAddr)
	if !ok || tcpAddress.Port <= 0 || tcpAddress.Port > 65535 {
		_ = listener.Close()
		return nil, errors.New("exec port forward did not allocate one local port")
	}
	sessionCtx, cancel := context.WithCancel(ctx)
	session := &execForwardSession{
		parent: ctx, listener: listener, localPort: uint16(tcpAddress.Port),
		ready: make(chan struct{}), done: make(chan error, 1), cancel: cancel,
		finished: make(chan struct{}), conns: make(map[net.Conn]struct{}),
	}
	close(session.ready)
	go session.serve(sessionCtx, func(connCtx context.Context, conn net.Conn) {
		f.bridge(connCtx, request, conn)
	})
	return session, nil
}

func (f *ExecPortForwarder) bridge(ctx context.Context, request PortForwardRequest, conn net.Conn) {
	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer func() { _ = conn.Close() }()
	_ = f.Executor.Exec(connCtx, ExecRequest{
		Namespace: request.Namespace, Pod: request.Pod, Container: request.Container,
		Command: ExecBridgeCommand(f.BridgePath, request.RemotePort),
		Stdin:   conn, Stdout: conn, Stderr: io.Discard,
	})
}

// ExecBridgeCommand returns the in-Pod command that bridges stdio to the
// given Pod-loopback port.
func ExecBridgeCommand(bridgePath string, remotePort uint16) []string {
	return []string{bridgePath, TCPStdioSubcommand, "--port", strconv.Itoa(int(remotePort))}
}

type execForwardSession struct {
	parent    context.Context
	listener  net.Listener
	localPort uint16
	ready     chan struct{}
	done      chan error
	cancel    context.CancelFunc
	finished  chan struct{}
	closeOnce sync.Once

	mu       sync.Mutex
	closed   bool
	conns    map[net.Conn]struct{}
	handlers sync.WaitGroup
}

func (s *execForwardSession) serve(ctx context.Context, handle func(context.Context, net.Conn)) {
	defer close(s.finished)
	stopListener := context.AfterFunc(ctx, func() { _ = s.listener.Close() })
	defer stopListener()
	var acceptErr error
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			acceptErr = err
			break
		}
		if !s.track(conn) {
			_ = conn.Close()
			continue
		}
		s.handlers.Add(1)
		go func() {
			defer s.handlers.Done()
			defer s.untrack(conn)
			handle(ctx, conn)
		}()
	}
	s.closeConnections()
	s.handlers.Wait()
	s.done <- s.finalError(acceptErr)
}

func (s *execForwardSession) finalError(acceptErr error) error {
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return nil
	}
	if err := s.parent.Err(); err != nil {
		return err
	}
	return acceptErr
}

func (s *execForwardSession) track(conn net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.conns[conn] = struct{}{}
	return true
}

func (s *execForwardSession) untrack(conn net.Conn) {
	s.mu.Lock()
	delete(s.conns, conn)
	s.mu.Unlock()
}

func (s *execForwardSession) closeConnections() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for conn := range s.conns {
		_ = conn.Close()
	}
}

func (s *execForwardSession) LocalPort() uint16      { return s.localPort }
func (s *execForwardSession) Ready() <-chan struct{} { return s.ready }
func (s *execForwardSession) Done() <-chan error     { return s.done }

// Close stops accepting, cancels every in-flight exec stream, and waits for
// the connection handlers to return.
func (s *execForwardSession) Close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		s.cancel()
		_ = s.listener.Close()
	})
	<-s.finished
	return nil
}
