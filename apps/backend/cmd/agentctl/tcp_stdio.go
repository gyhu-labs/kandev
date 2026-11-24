package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"
)

// tcpStdioSubcommand must match kubernetes.TCPStdioSubcommand. agentctl does
// not import the backend Kubernetes package to keep its dependency graph small.
const tcpStdioSubcommand = "tcp-stdio"

const (
	tcpStdioDialTimeout  = 10 * time.Second
	tcpStdioDrainTimeout = 10 * time.Second
)

// runTCPStdio bridges stdin and stdout to one TCP port on 127.0.0.1. The
// Kubernetes exec transport runs it once per forwarded connection, so the
// control endpoint stays bound to Pod loopback. It exits when the TCP peer
// closes, or shortly after stdin reaches EOF.
func runTCPStdio(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	port, err := parseTCPStdioArgs(args, stderr)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "agentctl %s: %v\n", tcpStdioSubcommand, err)
		return 2
	}
	address := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	conn, err := net.DialTimeout("tcp", address, tcpStdioDialTimeout)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "agentctl %s: dial %s: %v\n", tcpStdioSubcommand, address, err)
		return 1
	}
	if err := bridgeTCPStdio(conn, stdin, stdout, tcpStdioDrainTimeout); err != nil {
		_, _ = fmt.Fprintf(stderr, "agentctl %s: %v\n", tcpStdioSubcommand, err)
		return 1
	}
	return 0
}

func parseTCPStdioArgs(args []string, stderr io.Writer) (int, error) {
	flags := flag.NewFlagSet(tcpStdioSubcommand, flag.ContinueOnError)
	flags.SetOutput(stderr)
	port := flags.Int("port", 0, "TCP port on 127.0.0.1 to bridge")
	if err := flags.Parse(args); err != nil {
		return 0, err
	}
	if flags.NArg() > 0 {
		return 0, fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	if *port < 1 || *port > 65535 {
		return 0, errors.New("--port must be between 1 and 65535")
	}
	return *port, nil
}

// bridgeTCPStdio copies stdin to conn and conn to stdout. EOF on the TCP side
// ends the bridge. EOF on stdin half-closes the TCP write side and gives the
// peer drainTimeout to finish its response before the bridge ends.
func bridgeTCPStdio(conn net.Conn, stdin io.Reader, stdout io.Writer, drainTimeout time.Duration) error {
	defer func() { _ = conn.Close() }()
	upstreamDone := make(chan struct{})
	go func() {
		defer close(upstreamDone)
		_, _ = io.Copy(conn, stdin)
		if halfCloser, ok := conn.(interface{ CloseWrite() error }); ok {
			_ = halfCloser.CloseWrite()
		}
	}()
	downstreamDone := make(chan error, 1)
	go func() {
		_, err := io.Copy(stdout, conn)
		downstreamDone <- err
	}()
	select {
	case err := <-downstreamDone:
		return ignoreClosedConnError(err)
	case <-upstreamDone:
	}
	timer := time.NewTimer(drainTimeout)
	defer timer.Stop()
	select {
	case err := <-downstreamDone:
		return ignoreClosedConnError(err)
	case <-timer.C:
		return nil
	}
}

func ignoreClosedConnError(err error) error {
	if err == nil || errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}
