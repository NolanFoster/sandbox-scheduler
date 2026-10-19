package agentsandbox_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NolanFoster/sandbox-scheduler/pkg/adapter"
	"github.com/NolanFoster/sandbox-scheduler/pkg/adapter/agentsandbox"
	"github.com/NolanFoster/sandbox-scheduler/pkg/provider/adaptertest"
)

func build(t *testing.T, endpoint, token string, opts map[string]string) (fetch func() (int, error)) {
	t.Helper()
	p, err := adapter.New(agentsandbox.AdapterName, adapter.Config{
		ProviderID:  "civo",
		Endpoint:    endpoint,
		Credentials: map[string][]byte{agentsandbox.CredentialKey: []byte(token)},
		Options:     opts,
	})
	if err != nil {
		t.Fatalf("building adapter: %v", err)
	}
	return func() (int, error) {
		rep, err := p.Source.Fetch(context.Background())
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

	p, err := adapter.New(agentsandbox.AdapterName, adapter.Config{
		ProviderID: "civo", Endpoint: srv.URL,
	})
	if err != nil {
		t.Fatalf("building adapter: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.Source.Fetch(ctx); err == nil {
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

func TestBinderConformance(t *testing.T) {
	adaptertest.TestBinder(t, func(t *testing.T) adaptertest.Harness {
		type storedClaim struct {
			name      string
			uid       string
			key       string
			owner     string
			specHash  string
			expiresAt string
			deleting  bool
		}
		var (
			mu     sync.Mutex
			byName = map[string]*storedClaim{}
			byKey  = map[string]string{}
			seq    int
			fault  adaptertest.Fault
			calls  atomic.Int64
		)

		writeClaim := func(w http.ResponseWriter, status int, c *storedClaim) {
			delTS := ""
			if c.deleting {
				delTS = time.Now().UTC().Format(time.RFC3339)
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{
					"name":              c.name,
					"uid":               c.uid,
					"DeletionTimestamp": delTS,
					"deletionTimestamp": delTS,
					"labels": map[string]string{
						agentsandbox.LabelOwner: c.owner,
					},
					"annotations": map[string]string{
						agentsandbox.AnnotationKey:       c.key,
						agentsandbox.AnnotationSpecHash:  c.specHash,
						agentsandbox.AnnotationExpiresAt: c.expiresAt,
					},
				},
			})
		}

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			mu.Lock()
			defer mu.Unlock()

			switch {
			case r.Method == http.MethodPost && r.URL.Path == "/sandboxes":
				f := fault
				fault = adaptertest.FaultNone
				switch f {
				case adaptertest.FaultNoCapacity:
					w.WriteHeader(http.StatusConflict)
					_ = json.NewEncoder(w).Encode(map[string]any{"error": "pool exhausted", "reason": "NoCapacity"})
					return
				case adaptertest.FaultBusy:
					w.Header().Set("Retry-After", "1")
					w.WriteHeader(http.StatusTooManyRequests)
					_ = json.NewEncoder(w).Encode(map[string]any{"error": "rate limited", "reason": "Busy", "retryAfterMs": 500})
					return
				case adaptertest.FaultUnavailable:
					w.WriteHeader(http.StatusServiceUnavailable)
					_ = json.NewEncoder(w).Encode(map[string]any{"error": "apiserver down", "reason": "Unavailable"})
					return
				case adaptertest.FaultRejected:
					w.WriteHeader(http.StatusUnprocessableEntity)
					_ = json.NewEncoder(w).Encode(map[string]any{"error": "unknown template"})
					return
				}

				var body struct {
					Name       string `json:"name"`
					Key        string `json:"key"`
					Owner      string `json:"owner"`
					SpecHash   string `json:"specHash"`
					TTLSeconds int64  `json:"ttlSeconds"`
				}
				_ = json.NewDecoder(r.Body).Decode(&body)
				if existing, ok := byName[body.Name]; ok {
					if existing.deleting {
						w.WriteHeader(http.StatusTooManyRequests)
						_ = json.NewEncoder(w).Encode(map[string]any{"error": "claim deleting", "reason": "KeyBusy", "retryAfterMs": 200})
						return
					}
					if existing.specHash != body.SpecHash || existing.owner != body.Owner || existing.key != body.Key {
						w.WriteHeader(http.StatusConflict)
						_ = json.NewEncoder(w).Encode(map[string]any{"error": "spec mismatch", "reason": "KeyConflict"})
						return
					}
					writeClaim(w, http.StatusOK, existing)
					return
				}

				seq++
				exp := time.Now().UTC().Add(time.Duration(body.TTLSeconds) * time.Second).Format(time.RFC3339)
				c := &storedClaim{
					name:      body.Name,
					uid:       fmt.Sprintf("uid-%d", seq),
					key:       body.Key,
					owner:     body.Owner,
					specHash:  body.SpecHash,
					expiresAt: exp,
				}
				byName[body.Name] = c
				byKey[body.Key] = body.Name

				if f == adaptertest.FaultDropAfterSend {
					w.WriteHeader(http.StatusGatewayTimeout)
					_ = json.NewEncoder(w).Encode(map[string]any{"error": "lost response after create"})
					return
				}
				writeClaim(w, http.StatusCreated, c)

			case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/sandboxes/"):
				name := strings.TrimPrefix(r.URL.Path, "/sandboxes/")
				c, ok := byName[name]
				if !ok {
					w.WriteHeader(http.StatusNotFound)
					_ = json.NewEncoder(w).Encode(map[string]any{"error": "not found"})
					return
				}
				writeClaim(w, http.StatusOK, c)

			case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/sandboxes/"):
				name := strings.TrimPrefix(r.URL.Path, "/sandboxes/")
				c, ok := byName[name]
				if !ok {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				if wantUID := r.URL.Query().Get("uid"); wantUID != "" && wantUID != c.uid {
					w.WriteHeader(http.StatusConflict)
					return
				}
				delete(byName, name)
				delete(byKey, c.key)
				w.WriteHeader(http.StatusOK)
			}
		}))
		t.Cleanup(srv.Close)

		prov, err := adapter.New(agentsandbox.AdapterName, adapter.Config{
			ProviderID:  "civo",
			Endpoint:    srv.URL,
			Credentials: map[string][]byte{agentsandbox.CredentialKey: []byte("tok")},
		})
		if err != nil {
			t.Fatalf("adapter.New: %v", err)
		}

		return adaptertest.Harness{
			Provider: prov,
			InjectFault: func(f adaptertest.Fault) {
				mu.Lock()
				fault = f
				mu.Unlock()
			},
			MarkDeleting: func(key string, deleting bool) {
				mu.Lock()
				if name, ok := byKey[key]; ok {
					if c, exists := byName[name]; exists {
						c.deleting = deleting
					}
				}
				mu.Unlock()
			},
			CallCount: func() int {
				return int(calls.Load())
			},
		}
	})
}
