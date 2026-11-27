package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"sync"
	"testing"

	ws "github.com/kandev/kandev/pkg/websocket"
)

const scopeTestBaseCommit = "1111111111111111111111111111111111111111"

// repositoryScopeAgentctl answers the agentctl Git endpoints the cumulative
// diff reads. rootIsRepository=false models a plugin executor workspace: the
// root is not a repository and the only repository lives in a branch-scoped
// directory that agentctl registers as a repository scope.
type repositoryScopeAgentctl struct {
	rootIsRepository bool
	scopes           []string

	mu       sync.Mutex
	requests []string
}

func (a *repositoryScopeAgentctl) record(r *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.requests = append(a.requests, r.URL.Path+"?"+r.URL.RawQuery)
}

func (a *repositoryScopeAgentctl) snapshot() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.requests)
}

func (a *repositoryScopeAgentctl) requested(path string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Contains(a.requests, path)
}

func (a *repositoryScopeAgentctl) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.record(r)
	w.Header().Set("Content-Type", "application/json")
	switch r.URL.Path {
	case "/api/v1/git/status":
		repo := r.URL.Query().Get("repo")
		if repo == "" && !a.rootIsRepository {
			_, _ = w.Write([]byte(`{"success":false,"status_state":"unavailable","error_code":"repository_unavailable","error":"workspace is not a Git repository"}`))
			return
		}
		if repo != "" && !slices.Contains(a.scopes, repo) {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"success":false,"error":"unknown repository"}`))
			return
		}
		_, _ = w.Write([]byte(`{"success":true,"status_state":"ready","files_complete":true,"detail_state":"ready","branch":"main","base_commit":"` + scopeTestBaseCommit + `","head_commit":"` + scopeTestBaseCommit + `"}`))
	case "/api/v1/git/turn-changes/scopes":
		body, _ := json.Marshal(map[string][]string{"scopes": a.scopes})
		_, _ = w.Write(body)
	case "/api/v1/git/cumulative-diff":
		_, _ = w.Write([]byte(`{"success":true,"base_commit":"` + r.URL.Query().Get("base") + `","head_commit":"2222222222222222222222222222222222222222","total_commits":1,"files":{}}`))
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func cumulativeDiffPayload(t *testing.T, h *GitHandlers) map[string]any {
	t.Helper()
	msg, err := ws.NewRequest("request-1", ws.ActionSessionCumulativeDiff, CumulativeDiffRequest{SessionID: "s"})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := h.wsCumulativeDiff(context.Background(), msg)
	if err != nil {
		t.Fatalf("wsCumulativeDiff: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(resp.Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	return payload
}

func cumulativeDiffBase(t *testing.T, payload map[string]any) string {
	t.Helper()
	diff, ok := payload["cumulative_diff"].(map[string]any)
	if !ok {
		t.Fatalf("cumulative_diff = %#v, want a diff", payload["cumulative_diff"])
	}
	base, _ := diff["base_commit"].(string)
	return base
}

// Plugin executor layout: without a stored session base commit the fallback
// must read the base from the only registered repository.
func TestWsCumulativeDiff_BaseFallbackUsesSoleRepositoryScope(t *testing.T) {
	agentctl := &repositoryScopeAgentctl{scopes: []string{"acme-widget-main"}}
	h, server := gitHandlerServer(t, agentctl.ServeHTTP)
	t.Cleanup(server.Close)

	payload := cumulativeDiffPayload(t, h)

	if got := cumulativeDiffBase(t, payload); got != scopeTestBaseCommit {
		t.Fatalf("cumulative diff base = %q, want %q", got, scopeTestBaseCommit)
	}
	if !agentctl.requested("/api/v1/git/status?fresh=true&details=wait&repo=acme-widget-main") {
		t.Fatalf("requests = %v, want a status read scoped to acme-widget-main", agentctl.snapshot())
	}
}

// Class S/M layout: the root is the repository, so the root status answers
// and no scope lookup happens.
func TestWsCumulativeDiff_BaseFallbackKeepsRootRepository(t *testing.T) {
	agentctl := &repositoryScopeAgentctl{rootIsRepository: true, scopes: []string{""}}
	h, server := gitHandlerServer(t, agentctl.ServeHTTP)
	t.Cleanup(server.Close)

	payload := cumulativeDiffPayload(t, h)

	if got := cumulativeDiffBase(t, payload); got != scopeTestBaseCommit {
		t.Fatalf("cumulative diff base = %q, want %q", got, scopeTestBaseCommit)
	}
	if agentctl.requested("/api/v1/git/turn-changes/scopes?") {
		t.Fatalf("requests = %v, want no scope lookup when the root is a repository", agentctl.snapshot())
	}
}

// Several repositories and no root repository: there is no single base to
// pick, so the response stays empty as before.
func TestWsCumulativeDiff_BaseFallbackSkipsAmbiguousRepositoryScopes(t *testing.T) {
	agentctl := &repositoryScopeAgentctl{scopes: []string{"acme-gadget-main", "acme-widget-main"}}
	h, server := gitHandlerServer(t, agentctl.ServeHTTP)
	t.Cleanup(server.Close)

	payload := cumulativeDiffPayload(t, h)

	if payload["cumulative_diff"] != nil {
		t.Fatalf("cumulative_diff = %#v, want nil without a single repository", payload["cumulative_diff"])
	}
	for _, scope := range agentctl.scopes {
		if agentctl.requested("/api/v1/git/status?fresh=true&details=wait&repo=" + scope) {
			t.Fatalf("requests = %v, want no scoped status read", agentctl.snapshot())
		}
	}
}
