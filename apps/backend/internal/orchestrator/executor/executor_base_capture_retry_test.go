package executor

import (
	"context"
	"errors"
	"testing"
	"time"

	client "github.com/kandev/kandev/internal/agent/runtime/agentctl"
	"github.com/kandev/kandev/internal/task/models"
)

// sequencedGitStatusManager answers each detailed status read with the next
// scripted reply and repeats the last one when the script runs out.
type sequencedGitStatusManager struct {
	*mockAgentManager
	replies []gitStatusReply
	calls   int
	onCall  func(call int)
}

type gitStatusReply struct {
	status *client.GitStatusResult
	err    error
}

func (m *sequencedGitStatusManager) GetGitStatusWithDetails(context.Context, string) (*client.GitStatusResult, error) {
	m.calls++
	if m.onCall != nil {
		m.onCall(m.calls)
	}
	reply := m.replies[len(m.replies)-1]
	if m.calls <= len(m.replies) {
		reply = m.replies[m.calls-1]
	}
	return reply.status, reply.err
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

func newBaseCaptureFixture(t *testing.T, replies ...gitStatusReply) (*Executor, *sequencedGitStatusManager, *mockRepository) {
	t.Helper()
	repo := newMockRepository()
	repo.sessions["session-123"] = &models.TaskSession{ID: "session-123"}
	manager := &sequencedGitStatusManager{mockAgentManager: &mockAgentManager{}, replies: replies}
	return newTestExecutor(t, manager, repo), manager, repo
}

// Class L: the first detailed read races the fresh clone's enrichment and
// reports details_unavailable; the base commit is captured once it settles.
func TestCaptureBaseCommitRetriesUntilDetailsAreReady(t *testing.T) {
	executor, manager, repo := newBaseCaptureFixture(t, detailsNotReadyReply(), readyStatusReply("merge-base"))

	executor.captureBaseCommit(context.Background(), "session-123")

	if manager.calls != 2 {
		t.Fatalf("detailed status calls = %d, want 2 (one retry after details_unavailable)", manager.calls)
	}
	if got := repo.sessions["session-123"].BaseCommitSHA; got != "merge-base" {
		t.Fatalf("saved base commit = %q, want the merge-base from the settled status", got)
	}
}

// The retry is bounded: details that never settle leave no baseline after
// baseCommitCaptureAttempts reads.
func TestCaptureBaseCommitStopsRetryingAfterBoundedAttempts(t *testing.T) {
	executor, manager, repo := newBaseCaptureFixture(t, detailsNotReadyReply())

	started := time.Now()
	executor.captureBaseCommit(context.Background(), "session-123")

	if manager.calls != 4 {
		t.Fatalf("detailed status calls = %d, want 4 bounded attempts", manager.calls)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("bounded retry took %s, want a small total budget", elapsed)
	}
	if got := repo.sessions["session-123"].BaseCommitSHA; got != "" {
		t.Fatalf("saved base commit = %q, want no baseline from unavailable enrichment", got)
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

			if manager.calls != 1 {
				t.Fatalf("detailed status calls = %d, want exactly one", manager.calls)
			}
			if elapsed := time.Since(started); elapsed > 200*time.Millisecond {
				t.Fatalf("capture took %s, want no retry delay", elapsed)
			}
		})
	}
}

// Cancellation during the backoff ends the capture without another read.
func TestCaptureBaseCommitRetryHonoursContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	executor, manager, repo := newBaseCaptureFixture(t, detailsNotReadyReply(), readyStatusReply("merge-base"))
	manager.onCall = func(call int) {
		if call == 1 {
			cancel()
		}
	}

	executor.captureBaseCommit(ctx, "session-123")

	if manager.calls != 1 {
		t.Fatalf("detailed status calls = %d, want none after cancellation", manager.calls)
	}
	if got := repo.sessions["session-123"].BaseCommitSHA; got != "" {
		t.Fatalf("saved base commit = %q, want none after cancellation", got)
	}
}
