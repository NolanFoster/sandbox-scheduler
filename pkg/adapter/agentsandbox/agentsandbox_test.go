package agentsandbox_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/NolanFoster/sandbox-scheduler/pkg/adapter"
	"github.com/NolanFoster/sandbox-scheduler/pkg/adapter/agentsandbox"
)

func build(t *testing.T, endpoint, token string, opts map[string]string) (fetch func() (int, error)) {
	t.Helper()
	src, err := adapter.New(agentsandbox.AdapterName, adapter.Config{
		ProviderID:  "civo",
		Endpoint:    endpoint,
		Credentials: map[string][]byte{agentsandbox.CredentialKey: []byte(token)},
		Options:     opts,
	})
	if err != nil {
		t.Fatalf("building adapter: %v", err)
	}
	return func() (int, error) {
		rep, err := src.Fetch(context.Background())
		return rep.WarmCapacity, err
	}
}

func TestReadsWarmCapacityFromTheGateway(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/warmpool" {
			t.Errorf("requested %s, want /warmpool", r.URL.Path)
		}
		w.Write([]byte(`{"name":"python-warm-pool","replicas":3,"readyReplicas":3,"template":"python"}`))
	}))
	defer srv.Close()

	got, err := build(t, srv.URL, "", nil)()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 3 {
		t.Fatalf("warm capacity %d, want 3", got)
	}
}

func TestSendsTheBearerToken(t *testing.T) {
	var seen string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Header.Get("Authorization")
		w.Write([]byte(`{"readyReplicas":1}`))
	}))
	defer srv.Close()

	if _, err := build(t, srv.URL, "s3cret", nil)(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if seen != "Bearer s3cret" {
		t.Fatalf("Authorization header %q, want Bearer s3cret", seen)
	}
}

func TestNamedWarmPoolIsPassedThrough(t *testing.T) {
	var query string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query = r.URL.RawQuery
		w.Write([]byte(`{"readyReplicas":2}`))
	}))
	defer srv.Close()

	if _, err := build(t, srv.URL, "", map[string]string{"warmPoolName": "gpu-pool"})(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(query, "name=gpu-pool") {
		t.Fatalf("query %q should select the named pool", query)
	}
}

func TestMissingWarmPoolIsZeroCapacityNotAFailure(t *testing.T) {
	// A cluster with no warm pool is reachable and can still cold-start.
	// Treating 404 as an error would mark it unreachable and drop it out of
	// contention entirely.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"error":"SandboxWarmPool not found"}`))
	}))
	defer srv.Close()

	got, err := build(t, srv.URL, "", nil)()
	if err != nil {
		t.Fatalf("404 should not be an error, got %v", err)
	}
	if got != 0 {
		t.Fatalf("warm capacity %d, want 0", got)
	}
}

func TestAuthFailureSaysWhatToFix(t *testing.T) {
	// Indistinguishable from "provider down" in a generic status-code error,
	// but it is the one an operator can actually act on.
	for _, code := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(code)
		}))
		_, err := build(t, srv.URL, "wrong", nil)()
		srv.Close()

		if err == nil {
			t.Fatalf("HTTP %d should be an error", code)
		}
		if !strings.Contains(err.Error(), "credentialsRef") {
			t.Fatalf("HTTP %d error should point at the credential, got %q", code, err)
		}
	}
}

func TestServerErrorIsReported(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("boom"))
	}))
	defer srv.Close()

	_, err := build(t, srv.URL, "", nil)()
	if err == nil || !strings.Contains(err.Error(), "500") {
		t.Fatalf("want an error naming the status, got %v", err)
	}
}

func TestResponseWithoutReadyReplicasIsAnError(t *testing.T) {
	// A 200 from something that is not this API. Reporting zero would look
	// like a healthy, empty pool and quietly divert traffic elsewhere forever.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"hello":"world"}`))
	}))
	defer srv.Close()

	_, err := build(t, srv.URL, "", nil)()
	if err == nil {
		t.Fatal("a response without readyReplicas must be an error, not zero capacity")
	}
	if !strings.Contains(err.Error(), "readyReplicas") {
		t.Fatalf("error should name the missing field, got %q", err)
	}
}

func TestUnparseableBodyIsAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>not json</html>`))
	}))
	defer srv.Close()

	if _, err := build(t, srv.URL, "", nil)(); err == nil {
		t.Fatal("unparseable JSON must be an error")
	}
}

func TestNegativeReadyReplicasIsClampedToZero(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"readyReplicas":-5}`))
	}))
	defer srv.Close()

	got, err := build(t, srv.URL, "", nil)()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 0 {
		t.Fatalf("warm capacity %d, want 0", got)
	}
}

func TestContextCancellationIsRespected(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()

	src, err := adapter.New(agentsandbox.AdapterName, adapter.Config{
		ProviderID: "civo", Endpoint: srv.URL,
	})
	if err != nil {
		t.Fatalf("building adapter: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := src.Fetch(ctx); err == nil {
		t.Fatal("a cancelled context must abort the fetch")
	}
}

func TestEndpointIsRequired(t *testing.T) {
	_, err := adapter.New(agentsandbox.AdapterName, adapter.Config{ProviderID: "civo"})
	if err == nil || !strings.Contains(err.Error(), "endpoint") {
		t.Fatalf("want an endpoint-required error, got %v", err)
	}
}

func TestTrailingSlashInEndpointDoesNotDoubleUp(t *testing.T) {
	var path string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		w.Write([]byte(`{"readyReplicas":1}`))
	}))
	defer srv.Close()

	if _, err := build(t, srv.URL+"///", "", nil)(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if path != "/warmpool" {
		t.Fatalf("path %q, want /warmpool", path)
	}
}
