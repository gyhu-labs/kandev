package changes

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kandev/kandev/internal/common/turnchanges"
	"github.com/kandev/kandev/internal/task/models"
)

// Plugin executors register only the branch-scoped directory as a repository
// scope; the workspace root is not a repository. The lifecycle builds the
// checkout subpath "acme-widget-main" for them
// (lifecycle.TestTurnChangeCheckoutsUsePluginBranchScopedDirectories); this
// pins that such a checkout is captured, and that the bare repository name
// recorded before the fix is not.
func TestCoordinatorMatchesSingleCheckoutToBranchScopedScopeWithoutRoot(t *testing.T) {
	for _, tc := range []struct {
		name        string
		subpath     string
		wantErr     string
		wantSubpath string
	}{
		{name: "branch-scoped subpath", subpath: "acme-widget-main", wantSubpath: "acme-widget-main"},
		{name: "bare repository name", subpath: "acme-widget", wantErr: `checkout "checkout-1" is absent from registered repository scopes`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newCoordinatorStore()
			policy := &coordinatorPolicy{result: turnchanges.CapturePolicy{SettingsUserID: "user-initiator", Enabled: true, ResolutionKind: turnchanges.PolicyAuthenticatedUser}}
			client := &coordinatorCheckpointClient{scopes: []string{"acme-widget-main"}}
			coordinator := NewCoordinator(store, coordinatorTurnReader{}, policy, nil, nil)
			admission := coordinatorAdmission()
			admission.Checkouts[0].DisplayName = "acme-widget"
			admission.Checkouts[0].RepositorySubpath = tc.subpath

			err := coordinator.Admit(context.Background(), admission, client)

			rows := store.repositoryRows[turnChangeSetID(admission.TurnID)]
			require.Len(t, rows, 1)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				require.Zero(t, client.startCalls)
				require.Equal(t, models.TurnChangeAvailabilityUnavailable, rows[0].Availability)
				require.Equal(t, models.TurnChangeReasonCheckoutUnavailable, rows[0].Reason)
				return
			}
			require.NoError(t, err)
			require.Equal(t, 1, client.startCalls)
			require.Equal(t, tc.wantSubpath, rows[0].RepositorySubpath)
			require.NotEqual(t, models.TurnChangeReasonCheckoutUnavailable, rows[0].Reason)
		})
	}
}
