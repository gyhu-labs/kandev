package changes

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/kandev/kandev/internal/common/turnchanges"
	"github.com/stretchr/testify/require"
)

// agentctlStartCheckpointTimeout mirrors turnCheckpointStartTimeout in
// internal/agentctl/server/process/turn_checkpoint_capture.go: the backend
// must not cut a start capture shorter than the executor allows itself.
const agentctlStartCheckpointTimeout = 10 * time.Second

type deadlineRecordingCaptureClient struct {
	*coordinatorStartCaptureClient
	mu        sync.Mutex
	remaining []time.Duration
	delay     time.Duration
}

func (c *deadlineRecordingCaptureClient) CaptureTurnCheckpoint(ctx context.Context, request turnchanges.CheckpointRequest) (*turnchanges.CheckpointResult, error) {
	if deadline, ok := ctx.Deadline(); ok {
		c.mu.Lock()
		c.remaining = append(c.remaining, time.Until(deadline))
		c.mu.Unlock()
	}
	if c.delay > 0 {
		select {
		case <-time.After(c.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return c.coordinatorStartCaptureClient.CaptureTurnCheckpoint(ctx, request)
}

// @covers AC-TASKS-TURN-CHANGES-003.1
func TestCoordinatorStartCaptureBudgetCoversExecutorCheckpointLimit(t *testing.T) {
	require.GreaterOrEqual(t, turnChangeAdmissionTimeout-turnChangeStartPersistenceReserve, agentctlStartCheckpointTimeout)

	admission := admissionWithStartCaptureCheckouts(1)
	store := newCoordinatorStore()
	policy := &coordinatorPolicy{result: turnchanges.CapturePolicy{
		SettingsUserID: "user-initiator", Enabled: true, ResolutionKind: turnchanges.PolicyAuthenticatedUser,
	}}
	coordinator := NewCoordinator(store, coordinatorTurnReader{}, policy, nil, nil)
	client := &deadlineRecordingCaptureClient{coordinatorStartCaptureClient: newCoordinatorStartCaptureClient(admission)}

	require.NoError(t, coordinator.Admit(context.Background(), admission, client))

	require.Len(t, client.remaining, 1)
	require.Greater(t, client.remaining[0], agentctlStartCheckpointTimeout-time.Second,
		"the default start capture budget must not be the old 4 s")
}

// A start capture slower than the old 4 s budget (git on a Kata guest's
// virtio-fs) now completes instead of ending capture_failed.
// @covers AC-TASKS-TURN-CHANGES-003.1
func TestCoordinatorStartCaptureSlowerThanFourSecondsSucceeds(t *testing.T) {
	if testing.Short() {
		t.Skip("waits 4.5 s of wall time")
	}
	admission := admissionWithStartCaptureCheckouts(1)
	store := newCoordinatorStore()
	policy := &coordinatorPolicy{result: turnchanges.CapturePolicy{
		SettingsUserID: "user-initiator", Enabled: true, ResolutionKind: turnchanges.PolicyAuthenticatedUser,
	}}
	coordinator := NewCoordinator(store, coordinatorTurnReader{}, policy, nil, nil)
	client := &deadlineRecordingCaptureClient{
		coordinatorStartCaptureClient: newCoordinatorStartCaptureClient(admission), delay: 4500 * time.Millisecond,
	}

	require.NoError(t, coordinator.Admit(context.Background(), admission, client))

	rows := store.repositoryRows[turnChangeSetID(admission.TurnID)]
	require.Len(t, rows, 1)
	require.Empty(t, rows[0].Reason, "the slow start capture must not be recorded as capture_failed")
	require.Equal(t, "commit-"+rows[0].CheckoutID, rows[0].StartCommitOID)
}
