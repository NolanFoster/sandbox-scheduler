package e2b_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NolanFoster/sandbox-scheduler/pkg/adapter"
	"github.com/NolanFoster/sandbox-scheduler/pkg/adapter/e2b"
	"github.com/NolanFoster/sandbox-scheduler/pkg/provider"
	"github.com/NolanFoster/sandbox-scheduler/pkg/provider/adaptertest"
)

type fakeE2BServer struct {
	mu        sync.Mutex
	sandboxes map[string]map[string]any
	byKey     map[string]string
	fault     adaptertest.Fault
	calls     atomic.Int64
	seq       int
}

func newFakeE2BServer(t *testing.T) (*fakeE2BServer, *httptest.Server) {
	t.Helper()
	f := &fakeE2BServer{
		sandboxes: make(map[string]map[string]any),
		byKey:     make(map[string]string),
	}
	srv := httptest.NewServer(http.HandlerFunc(f.serveHTTP))
	t.Cleanup(srv.Close)
	return f, srv
}

func toListedSandbox(sb map[string]any) map[string]any {
	listed := make(map[string]any, len(sb))
	for k, v := range sb {
		if k == "domain" || k == "envdAccessToken" {
			continue
		}
		listed[k] = v
	}
	return listed
}

func (f *fakeE2BServer) serveHTTP(w http.ResponseWriter, r *http.Request) {
	f.calls.Add(1)

	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/v2/sandboxes":
		f.mu.Lock()
		defer f.mu.Unlock()

		for param := range r.URL.Query() {
			switch param {
			case "metadata", "state", "order", "startedAfter", "template", "nextToken", "limit":
			default:
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": "unsupported query parameter: " + param})
				return
			}
		}

		limit := 0
		if rawLimit := r.URL.Query().Get("limit"); rawLimit != "" {
			limit, _ = strconv.Atoi(rawLimit)
			if limit > e2b.MaxPageSize {
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": "limit exceeds maximum of 100"})
				return
			}
		}

		var filterMeta url.Values
		if rawMeta := r.URL.Query().Get("metadata"); rawMeta != "" {
			filterMeta, _ = url.ParseQuery(rawMeta)
		}

		// When querying a single key by metadata, do a direct O(1) lookup by key.
		if singleKey := filterMeta.Get(e2b.MetaKey); singleKey != "" {
			var out []map[string]any
			if id, ok := f.byKey[singleKey]; ok {
				if sb, exists := f.sandboxes[id]; exists {
					meta, _ := sb["metadata"].(map[string]any)
					wantOwner := filterMeta.Get(e2b.MetaOwner)
					gotOwner, _ := meta[e2b.MetaOwner].(string)
					if wantOwner == "" || gotOwner == "" || wantOwner == gotOwner {
						out = append(out, toListedSandbox(sb))
					}
				}
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(out)
			return
		}

		var matched []map[string]any
		wantOwner := filterMeta.Get(e2b.MetaOwner)
		for _, sb := range f.sandboxes {
			meta, _ := sb["metadata"].(map[string]any)
			if wantOwner != "" {
				gotOwner, _ := meta[e2b.MetaOwner].(string)
				if gotOwner != wantOwner {
					continue
				}
			}
			matched = append(matched, toListedSandbox(sb))
		}

		offset := 0
		if rawToken := r.URL.Query().Get("nextToken"); rawToken != "" {
			offset, _ = strconv.Atoi(rawToken)
		}
		if offset > len(matched) {
			offset = len(matched)
		}
		page := matched[offset:]
		nextToken := ""
		if limit > 0 && len(page) > limit {
			page = page[:limit]
			nextToken = strconv.Itoa(offset + limit)
		}
		if nextToken != "" {
			w.Header().Set("X-Next-Token", nextToken)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(page)

	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/sandboxes/") && !strings.HasSuffix(r.URL.Path, "/timeout"):
		id := strings.TrimPrefix(r.URL.Path, "/sandboxes/")
		f.mu.Lock()
		sb, ok := f.sandboxes[id]
		if !ok {
			f.mu.Unlock()
			w.WriteHeader(http.StatusNotFound)
			return
		}
		copySB := make(map[string]any, len(sb))
		for k, v := range sb {
			copySB[k] = v
		}
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(copySB)

	case r.Method == http.MethodPost && r.URL.Path == "/v2/sandboxes":
		var body struct {
			TemplateID string            `json:"templateID"`
			Timeout    int64             `json:"timeout"`
			Metadata   map[string]string `json:"metadata"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)

		f.mu.Lock()
		fault := f.fault
		f.fault = adaptertest.FaultNone
		f.mu.Unlock()

		switch fault {
		case adaptertest.FaultNoCapacity:
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": "quota exceeded", "reason": "NoCapacity"})
			return
		case adaptertest.FaultBusy:
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": "rate limited", "reason": "Busy"})
			return
		case adaptertest.FaultUnavailable:
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": "upstream down", "reason": "Unavailable"})
			return
		case adaptertest.FaultRejected:
			w.WriteHeader(http.StatusUnprocessableEntity)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": "unknown template", "reason": "Rejected"})
			return
		}

		f.mu.Lock()
		f.seq++
		id := fmt.Sprintf("e2b-sb-%d", f.seq)
		ver := fmt.Sprintf("v-%d", f.seq)
		metaAny := make(map[string]any, len(body.Metadata))
		for k, v := range body.Metadata {
			metaAny[k] = v
		}
		ttl := time.Duration(body.Timeout) * time.Second
		if ttl <= 0 {
			ttl = time.Hour
		}
		endAt := time.Now().UTC().Add(ttl).Format(time.RFC3339)
		sb := map[string]any{
			"sandboxID":       id,
			"templateID":      body.TemplateID,
			"clientID":        "cid-1",
			"startedAt":       time.Now().UTC().Format(time.RFC3339),
			"endAt":           endAt,
			"state":           "running",
			"version":         ver,
			"domain":          "e2b.app",
			"envdAccessToken": "envd-tok",
			"metadata":        metaAny,
		}
		f.sandboxes[id] = sb
		if key := body.Metadata[e2b.MetaKey]; key != "" {
			f.byKey[key] = id
		}
		f.mu.Unlock()

		if fault == adaptertest.FaultDropAfterSend {
			if hj, ok := w.(http.Hijacker); ok {
				conn, _, err := hj.Hijack()
				if err == nil {
					_ = conn.Close()
					return
				}
			}
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(sb)

	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/sandboxes/") && strings.HasSuffix(r.URL.Path, "/timeout"):
		id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/sandboxes/"), "/timeout")
		var body struct {
			Timeout int64 `json:"timeout"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.mu.Lock()
		sb, ok := f.sandboxes[id]
		if !ok {
			f.mu.Unlock()
			w.WriteHeader(http.StatusNotFound)
			return
		}
		sb["endAt"] = time.Now().UTC().Add(time.Duration(body.Timeout) * time.Second).Format(time.RFC3339)
		f.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)

	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/sandboxes/"):
		id := strings.TrimPrefix(r.URL.Path, "/sandboxes/")
		f.mu.Lock()
		sb, ok := f.sandboxes[id]
		if !ok {
			f.mu.Unlock()
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if meta, ok := sb["metadata"].(map[string]any); ok {
			if key, _ := meta[e2b.MetaKey].(string); key != "" && f.byKey[key] == id {
				delete(f.byKey, key)
			}
		}
		delete(f.sandboxes, id)
		f.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)

	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func TestE2BBinderConformance(t *testing.T) {
	adaptertest.TestBinder(t, func(t *testing.T) adaptertest.Harness {
		f, srv := newFakeE2BServer(t)
		prov, err := adapter.New(e2b.AdapterName, adapter.Config{
			ProviderID:  "e2b",
			Endpoint:    srv.URL,
			Credentials: map[string][]byte{"token": []byte("e2b-test-key")},
			Templates:   map[string]string{"python-3.12": "e2b-python-312", "node-22": "e2b-node-22"},
		})
		if err != nil {
			t.Fatalf("adapter.New: %v", err)
		}
		return adaptertest.Harness{
			Provider: prov,
			InjectFault: func(fault adaptertest.Fault) {
				f.mu.Lock()
				f.fault = fault
				f.mu.Unlock()
			},
			MarkDeleting: func(key string, deleting bool) {
				f.mu.Lock()
				defer f.mu.Unlock()
				if id, ok := f.byKey[key]; ok {
					if sb, exists := f.sandboxes[id]; exists {
						if deleting {
							sb["state"] = "killing"
							sb["deleting"] = true
						} else {
							sb["state"] = "running"
							delete(sb, "deleting")
						}
					}
				}
			},
			CallCount: func() int {
				return int(f.calls.Load())
			},
		}
	})
}

func TestE2BServerSideCreateTimeoutAndErrorCodeClassification(t *testing.T) {
	if e2b.DefaultEndpoint != "https://api.e2b.app" {
		t.Fatalf("DefaultEndpoint = %q, want https://api.e2b.app", e2b.DefaultEndpoint)
	}

	f, srv := newFakeE2BServer(t)
	prov, err := adapter.New(e2b.AdapterName, adapter.Config{
		ProviderID:  "e2b",
		Endpoint:    srv.URL,
		Credentials: map[string][]byte{"apiKey": []byte("e2b-test-key")},
	})
	if err != nil {
		t.Fatalf("adapter.New: %v", err)
	}
	if prov.Lister != nil {
		t.Fatalf("expected Provider.Lister == nil because E2B enforces TTL natively via timeout/endAt")
	}
	if prov.BatchLookup != nil {
		t.Fatalf("expected Provider.BatchLookup == nil because E2B /v2/sandboxes does not support multi-key IN queries")
	}

	ctx := context.Background()
	rep, err := prov.Source.Fetch(ctx)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if !rep.Elastic {
		t.Fatalf("expected Elastic=true on E2B report, got %+v", rep)
	}

	// Claim with a 3s context deadline and 10m TTL: initial create uses a 3s
	// server-side timeout, then extends to 10m via POST /sandboxes/{id}/timeout.
	claimCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	h, err := prov.Binder.Claim(claimCtx, provider.ClaimRequest{
		Owner:    "ss-owner-1",
		Key:      "deadline-key",
		SpecHash: "h1",
		Template: "base",
		TTL:      10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("Claim with deadline: %v", err)
	}
	if time.Until(h.ExpiresAt) < 9*time.Minute {
		t.Fatalf("expected promoted ExpiresAt ~10m in future, got %v", h.ExpiresAt)
	}
	wantEnvdURL := "https://49983-" + h.ID + ".e2b.app"
	if h.Connect["domain"] != "e2b.app" || h.Connect["envdUrl"] != wantEnvdURL || h.Connect["envdAccessToken"] != "envd-tok" {
		t.Fatalf("expected Claim handle to include domain, envdUrl, and envdAccessToken, got %+v", h.Connect)
	}

	// Lookup must hydrate domain and envdAccessToken via GET /sandboxes/{id}
	// even though GET /v2/sandboxes (ListedSandbox) omits both fields.
	lookedUp, err := prov.Binder.Lookup(ctx, provider.ClaimRequest{
		Owner:    "ss-owner-1",
		Key:      "deadline-key",
		SpecHash: "h1",
	})
	if err != nil {
		t.Fatalf("Lookup(deadline-key): %v", err)
	}
	if lookedUp.Connect["domain"] != "e2b.app" || lookedUp.Connect["envdUrl"] != wantEnvdURL || lookedUp.Connect["envdAccessToken"] != "envd-tok" {
		t.Fatalf("expected Lookup handle to hydrate domain, envdUrl, and envdAccessToken via GET /sandboxes/{id}, got %+v", lookedUp.Connect)
	}

	// Seed an orphaned late-started sandbox (startedAt > MetaCreateDeadline,
	// never promoted to MetaExpiresAt): Lookup must delete it and return ErrNotFound.
	now := time.Now().UTC()
	f.mu.Lock()
	f.sandboxes["e2b-orphan-1"] = map[string]any{
		"sandboxID":  "e2b-orphan-1",
		"templateID": "base",
		"startedAt":  now.Add(5 * time.Second).Format(time.RFC3339),
		"endAt":      now.Add(8 * time.Second).Format(time.RFC3339),
		"state":      "running",
		"metadata": map[string]any{
			e2b.MetaOwner:          "ss-owner-1",
			e2b.MetaKey:            "orphan-key",
			e2b.MetaSpecHash:       "h1",
			e2b.MetaExpiresAt:      now.Add(10 * time.Minute).Format(time.RFC3339),
			e2b.MetaCreateDeadline: now.Format(time.RFC3339Nano),
		},
	}
	f.byKey["orphan-key"] = "e2b-orphan-1"
	f.mu.Unlock()

	_, lookErr := prov.Binder.Lookup(ctx, provider.ClaimRequest{
		Owner:    "ss-owner-1",
		Key:      "orphan-key",
		SpecHash: "h1",
	})
	if lookErr != provider.ErrNotFound {
		t.Fatalf("Lookup on late-started orphan = %v, want ErrNotFound", lookErr)
	}
	f.mu.Lock()
	_, stillExists := f.sandboxes["e2b-orphan-1"]
	f.mu.Unlock()
	if stillExists {
		t.Fatalf("expected late-started orphan sandbox to be deleted by Lookup")
	}

	// Passing a non-empty Version to Release must be rejected because E2B's
	// DELETE /sandboxes/{id} endpoint has no server-side version precondition.
	relErr := prov.Binder.Release(ctx, provider.Handle{ID: h.ID, Version: "v-1"})
	if provider.KindOf(relErr) != provider.KindRejected {
		t.Fatalf("expected KindRejected when passing non-empty Version to E2B Release, got %v (%v)", provider.KindOf(relErr), relErr)
	}

	// Sub-second positive TTL rounds up to 1s rather than defaulting to 24h.
	subSecHandle, err := prov.Binder.Claim(ctx, provider.ClaimRequest{
		Owner:    "ss-owner-1",
		Key:      "subsec-key",
		SpecHash: "h1",
		Template: "base",
		TTL:      500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Claim with 500ms TTL: %v", err)
	}
	if rem := time.Until(subSecHandle.ExpiresAt); rem <= 0 || rem > 5*time.Second {
		t.Fatalf("expected 500ms TTL to round up to ~1s, got remaining %v (ExpiresAt=%v)", rem, subSecHandle.ExpiresAt)
	}

	// Verify real E2B error_code payloads ("sandbox_capacity_unavailable" and
	// "sandbox_no_compatible_node") on HTTP 503 classify as KindNoCapacity.
	for _, errCode := range []string{"sandbox_capacity_unavailable", "sandbox_no_compatible_node"} {
		errCode := errCode
		errSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`[]`))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code":       503,
				"error_code": errCode,
				"message":    "no capacity in region",
			})
		}))
		defer errSrv.Close()

		errProv, err := adapter.New(e2b.AdapterName, adapter.Config{
			ProviderID:  "e2b-cap",
			Endpoint:    errSrv.URL,
			Credentials: map[string][]byte{"apiKey": []byte("k")},
		})
		if err != nil {
			t.Fatalf("adapter.New(e2b-cap): %v", err)
		}
		_, capErr := errProv.Binder.Claim(ctx, provider.ClaimRequest{
			Owner:    "ss-owner-1",
			Key:      "cap-key-" + errCode,
			SpecHash: "h1",
			Template: "base",
			TTL:      time.Minute,
		})
		if provider.KindOf(capErr) != provider.KindNoCapacity {
			t.Fatalf("expected KindNoCapacity for E2B error_code=%s, got %v (%v)", errCode, provider.KindOf(capErr), capErr)
		}
	}
}

func TestE2BAuthModeAndTemplateValidation(t *testing.T) {
	// Invalid authMode is rejected at construction time.
	if _, err := adapter.New(e2b.AdapterName, adapter.Config{
		ProviderID: "e2b",
		Options:    map[string]string{"authMode": "basic"},
	}); err == nil {
		t.Fatal("expected error for invalid authMode")
	}

	// Bearer authMode without teamID is rejected at construction time.
	if _, err := adapter.New(e2b.AdapterName, adapter.Config{
		ProviderID: "e2b",
		Options:    map[string]string{"authMode": "bearer"},
	}); err == nil {
		t.Fatal("expected error for bearer authMode without teamID")
	}

	// Empty template mapping ref is rejected at construction time.
	if _, err := adapter.New(e2b.AdapterName, adapter.Config{
		ProviderID: "e2b",
		Templates:  map[string]string{"python": "   "},
	}); err == nil {
		t.Fatal("expected error for empty template mapping reference")
	}

	var gotAPIKey, gotAuthHeader, gotTeamID string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAPIKey = r.Header.Get("X-API-Key")
		gotAuthHeader = r.Header.Get("Authorization")
		gotTeamID = r.Header.Get("X-Team-ID")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[]`))
	}))
	defer srv.Close()

	// Default authMode (x-api-key) sets X-API-Key and omits Authorization.
	provAPIKey, err := adapter.New(e2b.AdapterName, adapter.Config{
		ProviderID:  "e2b-key",
		Endpoint:    srv.URL,
		Credentials: map[string][]byte{"apiKey": []byte("k-123")},
		Templates:   map[string]string{"python-3.12": "base-py312"},
	})
	if err != nil {
		t.Fatalf("adapter.New(x-api-key): %v", err)
	}
	if _, err := provAPIKey.Source.Fetch(context.Background()); err != nil {
		t.Fatalf("Fetch(x-api-key): %v", err)
	}
	if gotAPIKey != "k-123" || gotAuthHeader != "" || gotTeamID != "" {
		t.Fatalf("x-api-key mode sent X-API-Key=%q Authorization=%q X-Team-ID=%q", gotAPIKey, gotAuthHeader, gotTeamID)
	}

	// Unmapped template when Templates is non-empty returns KindRejected without hitting the server.
	_, claimErr := provAPIKey.Binder.Claim(context.Background(), provider.ClaimRequest{
		Owner:    "ss-owner",
		Key:      "unmapped-key",
		SpecHash: "h1",
		Template: "node-22",
		TTL:      time.Minute,
	})
	if provider.KindOf(claimErr) != provider.KindRejected {
		t.Fatalf("expected KindRejected for unmapped template, got %v (%v)", provider.KindOf(claimErr), claimErr)
	}

	// Bearer authMode sets Authorization: Bearer and X-Team-ID, and omits X-API-Key.
	provBearer, err := adapter.New(e2b.AdapterName, adapter.Config{
		ProviderID:  "e2b-bearer",
		Endpoint:    srv.URL,
		Credentials: map[string][]byte{"token": []byte("tok-456")},
		Options:     map[string]string{"authMode": "bearer", "teamID": "team-789"},
	})
	if err != nil {
		t.Fatalf("adapter.New(bearer): %v", err)
	}
	if _, err := provBearer.Source.Fetch(context.Background()); err != nil {
		t.Fatalf("Fetch(bearer): %v", err)
	}
	if gotAuthHeader != "Bearer tok-456" || gotTeamID != "team-789" || gotAPIKey != "" {
		t.Fatalf("bearer mode sent Authorization=%q X-Team-ID=%q X-API-Key=%q", gotAuthHeader, gotTeamID, gotAPIKey)
	}
}
