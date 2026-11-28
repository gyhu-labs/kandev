package executor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	client "github.com/kandev/kandev/internal/agent/runtime/agentctl"
	"github.com/kandev/kandev/internal/task/models"
)

// fastBaseCommitRetryDelay keeps retry tests in milliseconds; the default
// schedule is pinned by TestCaptureBaseCommitRetriesUntilDetailsAreReady.
const fastBaseCommitRetryDelay = time.Millisecond

// sequencedGitStatusManager answers each detailed status read with the next
// scripted reply and repeats the last one when the script runs out.
type sequencedGitStatusManager struct {
	*mockAgentManager
	replies []gitStatusReply
	onCall  func(call int)

	mu      sync.Mutex
	calls   int
	readsAt []time.Time
}

type gitStatusReply struct {
	status *client.GitStatusResult
	err    error
}

func (m *sequencedGitStatusManager) GetGitStatusWithDetails(context.Context, string) (*client.GitStatusResult, error) {
	m.mu.Lock()
	m.calls++
	call := m.calls
	m.readsAt = append(m.readsAt, time.Now())
	m.mu.Unlock()
	if m.onCall != nil {
		m.onCall(call)
	}
	reply := m.replies[len(m.replies)-1]
	if call <= len(m.replies) {
		reply = m.replies[call-1]
	}
	return reply.status, reply.err
}

func (m *sequencedGitStatusManager) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

// gaps returns the time between consecutive reads.
func (m *sequencedGitStatusManager) gaps() []time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	gaps := make([]time.Duration, 0, len(m.readsAt))
	for i := 1; i < len(m.readsAt); i++ {
		gaps = append(gaps, m.readsAt[i].Sub(m.readsAt[i-1]))
	}
	return gaps
}

// detailsNotReadyReply is what agentctl returns while the basic status is
// complete but the detail enrichment has not settled (api/git.go marks it
// details_unavailable), as the agentctl client surfaces it.
func detailsNotReadyReply() gitStatusReply {
	return gitStatusReply{
		status: &client.GitStatusResult{
			Success: false, StatusState: "ready", FilesComplete: true, DetailState: "unavailable",
			HeadCommit: "unrelated-head", ErrorCode: "details_unavailable",
		},
		err: errors.New("git status details unavailable: details_unavailable"),
	}
}

func readyStatusReply(base string) gitStatusReply {
	return gitStatusReply{status: &client.GitStatusResult{
		Success: true, StatusState: "ready", FilesComplete: true, DetailState: "ready",
		HeadCommit: "observed-head", BaseCommit: base,
	}}
}

// newBaseCaptureFixture returns an executor with the default backoff; tests
// that do not measure the schedule shorten it with baseCommitRetryDelay.
func newBaseCaptureFixture(t *testing.T, replies ...gitStatusReply) (*Executor, *sequencedGitStatusManager, *mockRepository) {
	t.Helper()
	repo := newMockRepository()
	repo.sessions["session-123"] = &models.TaskSession{ID: "session-123"}
	manager := &sequencedGitStatusManager{mockAgentManager: &mockAgentManager{}, replies: replies}
	return newTestExecutor(t, manager, repo), manager, repo
}

// Class L: the first detailed read races the fresh clone's enrichment and
// reports details_unavailable; the base commit is captured once it settles.
// Runs on the production schedule, so the first backoff is 500 ms.
func TestCaptureBaseCommitRetriesUntilDetailsAreReady(t *testing.T) {
	executor, manager, repo := newBaseCaptureFixture(t, detailsNotReadyReply(), readyStatusReply("merge-base"))

	executor.captureBaseCommit(context.Background(), "session-123")

	if got := manager.callCount(); got != 2 {
		t.Fatalf("detailed status calls = %d, want 2 (one retry after details_unavailable)", got)
	}
	if got := repo.sessions["session-123"].BaseCommitSHA; got != "merge-base" {
		t.Fatalf("saved base commit = %q, want the merge-base from the settled status", got)
	}
	if gap := manager.gaps()[0]; gap < 500*time.Millisecond {
		t.Fatalf("first backoff = %s, want the 500 ms production delay", gap)
	}
}

// The retry is bounded: details that never settle leave no baseline after
// baseCommitCaptureAttempts reads.
func TestCaptureBaseCommitStopsRetryingAfterBoundedAttempts(t *testing.T) {
	executor, manager, repo := newBaseCaptureFixture(t, detailsNotReadyReply())
	executor.baseCommitRetryDelay = fastBaseCommitRetryDelay

	executor.captureBaseCommit(context.Background(), "session-123")

	if got := manager.callCount(); got != 4 {
		t.Fatalf("detailed status calls = %d, want 4 bounded attempts", got)
	}
	if got := repo.sessions["session-123"].BaseCommitSHA; got != "" {
		t.Fatalf("saved base commit = %q, want no baseline from unavailable enrichment", got)
	}
}

// Each wait doubles the previous one: d, 2d, 4d between the four reads.
func TestCaptureBaseCommitBackoffDoublesBetweenReads(t *testing.T) {
	const first = 20 * time.Millisecond
	executor, manager, _ := newBaseCaptureFixture(t, detailsNotReadyReply())
	executor.baseCommitRetryDelay = first

	executor.captureBaseCommit(context.Background(), "session-123")

	gaps := manager.gaps()
	if len(gaps) != baseCommitCaptureAttempts-1 {
		t.Fatalf("reads = %d, want %d", len(gaps)+1, baseCommitCaptureAttempts)
	}
	for i, want := range []time.Duration{first, 2 * first, 4 * first} {
		if gaps[i] < want {
			t.Fatalf("backoff %d = %s, want at least %s (schedule %v)", i+1, gaps[i], want, gaps)
		}
	}
}

// A first read that succeeds (S, M, local, Docker) is not delayed, and hard
// errors are never retried.
func TestCaptureBaseCommitDoesNotRetryOtherOutcomes(t *testing.T) {
	for name, reply := range map[string]gitStatusReply{
		"ready":                  readyStatusReply("merge-base"),
		"transport error":        {err: errors.New("failed to execute request: connection refused")},
		"repository unavailable": {status: &client.GitStatusResult{Success: false, StatusState: "unavailable", DetailState: "unavailable", ErrorCode: "repository_unavailable"}, err: errors.New("git status details unavailable: repository_unavailable")},
		"torn down":              {},
	} {
		t.Run(name, func(t *testing.T) {
			executor, manager, _ := newBaseCaptureFixture(t, reply, readyStatusReply("late-base"))
			started := time.Now()

			executor.captureBaseCommit(context.Background(), "session-123")

			if got := manager.callCount(); got != 1 {
				t.Fatalf("detailed status calls = %d, want exactly one", got)
			}
			if elapsed := time.Since(started); elapsed > 200*time.Millisecond {
				t.Fatalf("capture took %s, want no retry delay", elapsed)
			}
		})
	}
}

// Cancellation during a read ends the capture without another read.
func TestCaptureBaseCommitRetryHonoursContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	executor, manager, repo := newBaseCaptureFixture(t, detailsNotReadyReply(), readyStatusReply("merge-base"))
	executor.baseCommitRetryDelay = fastBaseCommitRetryDelay
	manager.onCall = func(call int) {
		if call == 1 {
			cancel()
		}
	}

	executor.captureBaseCommit(ctx, "session-123")

	if got := manager.callCount(); got != 1 {
		t.Fatalf("detailed status calls = %d, want none after cancellation", got)
	}
	if got := repo.sessions["session-123"].BaseCommitSHA; got != "" {
		t.Fatalf("saved base commit = %q, want none after cancellation", got)
	}
}

// Cancellation while the backoff timer is pending ends the wait at once
// instead of sleeping out the delay.
func TestCaptureBaseCommitCancelDuringBackoffWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	executor, manager, repo := newBaseCaptureFixture(t, detailsNotReadyReply(), readyStatusReply("merge-base"))
	executor.baseCommitRetryDelay = time.Minute
	manager.onCall = func(call int) {
		if call == 1 {
			time.AfterFunc(20*time.Millisecond, cancel)
		}
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		executor.captureBaseCommit(ctx, "session-123")
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("capture kept waiting out the backoff after its context was cancelled")
	}

	if got := manager.callCount(); got != 1 {
		t.Fatalf("detailed status calls = %d, want none after cancellation", got)
	}
	if got := repo.sessions["session-123"].BaseCommitSHA; got != "" {
		t.Fatalf("saved base commit = %q, want none after cancellation", got)
	}
}
