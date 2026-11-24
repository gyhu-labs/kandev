package main

import (
	"bytes"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// startTCPStdioPeer accepts one connection and hands it to serve.
func startTCPStdioPeer(t *testing.T, serve func(net.Conn)) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		serve(conn)
	}()
	return listener.Addr().(*net.TCPAddr).Port
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestRunTCPStdioRoundTripsAndExitsOnStdinEOF(t *testing.T) {
	port := startTCPStdioPeer(t, func(conn net.Conn) {
		request, _ := io.ReadAll(conn)
		_, _ = conn.Write([]byte("echo:" + string(request)))
	})
	var stdout, stderr lockedBuffer

	code := runTCPStdio([]string{"--port", strconv.Itoa(port)}, strings.NewReader("ping"), &stdout, &stderr)

	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
	}
	if got := stdout.String(); got != "echo:ping" {
		t.Fatalf("stdout = %q, want %q", got, "echo:ping")
	}
}

func TestRunTCPStdioExitsWhenPeerCloses(t *testing.T) {
	port := startTCPStdioPeer(t, func(conn net.Conn) {
		_, _ = conn.Write([]byte("bye"))
	})
	stdin, stdinWriter := io.Pipe()
	defer func() { _ = stdinWriter.Close() }()
	var stdout, stderr lockedBuffer
	done := make(chan int, 1)

	go func() { done <- runTCPStdio([]string{"--port", strconv.Itoa(port)}, stdin, &stdout, &stderr) }()

	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("tcp-stdio did not exit after the TCP peer closed while stdin stayed open")
	}
	if got := stdout.String(); got != "bye" {
		t.Fatalf("stdout = %q, want %q", got, "bye")
	}
}

func TestBridgeTCPStdioStopsAfterDrainTimeout(t *testing.T) {
	release := make(chan struct{})
	port := startTCPStdioPeer(t, func(net.Conn) { <-release })
	defer close(release)
	conn, err := net.Dial("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)

	go func() { done <- bridgeTCPStdio(conn, strings.NewReader(""), io.Discard, 50*time.Millisecond) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("bridge error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("bridge did not stop after stdin EOF and the drain timeout")
	}
}

func TestRunTCPStdioReportsDialFailure(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	var stderr lockedBuffer

	code := runTCPStdio([]string{"--port", strconv.Itoa(port)}, strings.NewReader(""), io.Discard, &stderr)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "dial 127.0.0.1:"+strconv.Itoa(port)) {
		t.Fatalf("stderr = %q, want the loopback dial error", stderr.String())
	}
}

func TestParseTCPStdioArgsRejectsInvalidInput(t *testing.T) {
	for _, args := range [][]string{
		nil,
		{"--port", "0"},
		{"--port", "65536"},
		{"--port", "8765", "extra"},
		{"--host", "10.0.0.1", "--port", "8765"},
	} {
		if _, err := parseTCPStdioArgs(args, io.Discard); err == nil {
			t.Fatalf("parseTCPStdioArgs(%q) succeeded, want an error", args)
		}
	}
	port, err := parseTCPStdioArgs([]string{"--port", "8765"}, io.Discard)
	if err != nil || port != 8765 {
		t.Fatalf("parseTCPStdioArgs = %d, %v", port, err)
	}
}

func TestTCPStdioSubcommandName(t *testing.T) {
	if tcpStdioSubcommand != "tcp-stdio" {
		t.Fatalf("subcommand = %q; the Kubernetes exec transport invokes tcp-stdio", tcpStdioSubcommand)
	}
}
