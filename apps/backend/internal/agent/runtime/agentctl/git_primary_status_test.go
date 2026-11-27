package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
)

const primaryStatusReady = `{"success":true,"status_state":"ready","files_complete":true,"detail_state":"ready","base_commit":"1111111111111111111111111111111111111111"}`

type primaryStatusServer struct {
	rootStatus string
	scopes     []string
	scopesCode int

	mu       sync.Mutex
	requests []string
}

func (s *primaryStatusServer) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.requests)
}

func (s *primaryStatusServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.requests = append(s.requests, r.URL.Path+"?"+r.URL.RawQuery)
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.URL.Path == "/api/v1/git/status" && r.URL.Query().Get("repo") == "":
		_, _ = w.Write([]byte(s.rootStatus))
	case r.URL.Path == "/api/v1/git/status" && slices.Contains(s.scopes, r.URL.Query().Get("repo")):
		_, _ = w.Write([]byte(primaryStatusReady))
	case r.URL.Path == "/api/v1/git/turn-changes/scopes":
		if s.scopesCode != 0 {
			w.WriteHeader(s.scopesCode)
		}
		body, _ := json.Marshal(map[string][]string{"scopes": s.scopes})
		_, _ = w.Write(body)
	default:
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"success":false,"error":"unexpected request"}`))
	}
}

const rootNotRepository = `{"success":false,"status_state":"unavailable","error_code":"repository_unavailable"}`

func TestGetPrimaryGitStatusWithDetails(t *testing.T) {
	tests := []struct {
		name         string
		server       *primaryStatusServer
		wantBase     string
		wantErr      bool
		wantRequests []string
	}{
		{
			// Class S/M and local: the root is the repository; one request, as before.
			name:         "root repository",
			server:       &primaryStatusServer{rootStatus: primaryStatusReady, scopes: []string{""}},
			wantBase:     "1111111111111111111111111111111111111111",
			wantRequests: []string{"/api/v1/git/status?fresh=true&details=wait"},
		},
		{
			// Plugin executors: one repository in a branch-scoped directory.
			name:     "sole repository scope under a non-repository root",
			server:   &primaryStatusServer{rootStatus: rootNotRepository, scopes: []string{"acme-widget-main"}},
			wantBase: "1111111111111111111111111111111111111111",
			wantRequests: []string{
				"/api/v1/git/status?fresh=true&details=wait",
				"/api/v1/git/turn-changes/scopes?",
				"/api/v1/git/status?fresh=true&details=wait&repo=acme-widget-main",
			},
		},
		{
			name:    "several repository scopes stay ambiguous",
			server:  &primaryStatusServer{rootStatus: rootNotRepository, scopes: []string{"acme-gadget-main", "acme-widget-main"}},
			wantErr: true,
			wantRequests: []string{
				"/api/v1/git/status?fresh=true&details=wait",
				"/api/v1/git/turn-changes/scopes?",
			},
		},
		{
			name:    "scope lookup failure keeps the root result",
			server:  &primaryStatusServer{rootStatus: rootNotRepository, scopes: []string{"acme-widget-main"}, scopesCode: http.StatusInternalServerError},
			wantErr: true,
			wantRequests: []string{
				"/api/v1/git/status?fresh=true&details=wait",
				"/api/v1/git/turn-changes/scopes?",
			},
		},
		{
			// Any other root failure is not a layout question; no scope lookup.
			name:         "other root failure",
			server:       &primaryStatusServer{rootStatus: `{"success":false,"status_state":"unavailable","error_code":"git_status_failed"}`, scopes: []string{"acme-widget-main"}},
			wantErr:      true,
			wantRequests: []string{"/api/v1/git/status?fresh=true&details=wait"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(tt.server)
			t.Cleanup(server.Close)

			status, err := newHTTPOnlyClient(server.URL).GetPrimaryGitStatusWithDetails(context.Background())

			if (err != nil) != tt.wantErr {
				t.Fatalf("GetPrimaryGitStatusWithDetails() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && (status == nil || status.BaseCommit != tt.wantBase) {
				t.Fatalf("GetPrimaryGitStatusWithDetails() = %#v, want base %q", status, tt.wantBase)
			}
			if got := tt.server.snapshot(); !slices.Equal(got, tt.wantRequests) {
				t.Fatalf("requests = %v, want %v", got, tt.wantRequests)
			}
		})
	}
}

// GetGitStatusWithDetails itself stays root-only for every other caller.
func TestGetGitStatusWithDetailsStaysRootScoped(t *testing.T) {
	server := &primaryStatusServer{rootStatus: rootNotRepository, scopes: []string{"acme-widget-main"}}
	httpServer := httptest.NewServer(server)
	t.Cleanup(httpServer.Close)

	if _, err := newHTTPOnlyClient(httpServer.URL).GetGitStatusWithDetails(context.Background()); err == nil {
		t.Fatal("GetGitStatusWithDetails() error = nil, want repository_unavailable from the root")
	}
	if got, want := server.snapshot(), []string{"/api/v1/git/status?fresh=true&details=wait"}; !slices.Equal(got, want) {
		t.Fatalf("requests = %v, want %v", got, want)
	}
}
