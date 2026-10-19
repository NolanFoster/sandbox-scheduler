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

func (f *fakeE2BServer) serveHTTP(w http.ResponseWriter, r *http.Request) {
	f.calls.Add(1)

	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/sandboxes":
		f.mu.Lock()
		defer f.mu.Unlock()

		var filterMeta url.Values
		if rawMeta := r.URL.Query().Get("metadata"); rawMeta != "" {
			filterMeta, _ = url.ParseQuery(rawMeta)
		}
		var filterKeys map[string]bool
		if rawKeys := r.URL.Query().Get("keys"); rawKeys != "" {
			filterKeys = make(map[string]bool)
			for _, k := range strings.Split(rawKeys, ",") {
				if k != "" {
					filterKeys[k] = true
				}
			}
		}

		// When querying a single key by metadata, do a direct O(1) lookup by key.
		if singleKey := filterMeta.Get(e2b.MetaKey); singleKey != "" && filterKeys == nil {
			var out []map[string]any
			if id, ok := f.byKey[singleKey]; ok {
				if sb, exists := f.sandboxes[id]; exists {
					meta, _ := sb["metadata"].(map[string]any)
					wantOwner := filterMeta.Get(e2b.MetaOwner)
					gotOwner, _ := meta[e2b.MetaOwner].(string)
					if wantOwner == "" || gotOwner == "" || wantOwner == gotOwner {
						out = append(out, sb)
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
			if filterKeys != nil {
				gotKey, _ := meta[e2b.MetaKey].(string)
				if !filterKeys[gotKey] {
					continue
				}
			}
			matched = append(matched, sb)
		}

		limit := 0
		if rawLimit := r.URL.Query().Get("limit"); rawLimit != "" {
			limit, _ = strconv.Atoi(rawLimit)
		}
		offset := 0
		if rawToken := r.URL.Query().Get("pageToken"); rawToken != "" {
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
			w.Header().Set("X-Next-Page-Token", nextToken)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(page)

	case r.Method == http.MethodPost && r.URL.Path == "/sandboxes":
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

	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/sandboxes/"):
		id := strings.TrimPrefix(r.URL.Path, "/sandboxes/")
		wantVersion := r.URL.Query().Get("version")
		if wantVersion == "" {
			wantVersion = r.Header.Get("If-Match")
		}
		f.mu.Lock()
		sb, ok := f.sandboxes[id]
		if !ok {
			f.mu.Unlock()
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if wantVersion != "" && sb["version"] != wantVersion {
			f.mu.Unlock()
			w.WriteHeader(http.StatusPreconditionFailed)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": "version precondition failed"})
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

func TestE2BFetchAndListOwnedPagination(t *testing.T) {
	_, srv := newFakeE2BServer(t)
	prov, err := adapter.New(e2b.AdapterName, adapter.Config{
		ProviderID:  "e2b",
		Endpoint:    srv.URL,
		Credentials: map[string][]byte{"apiKey": []byte("e2b-test-key")},
	})
	if err != nil {
		t.Fatalf("adapter.New: %v", err)
	}

	ctx := context.Background()
	rep, err := prov.Source.Fetch(ctx)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if !rep.Elastic {
		t.Fatalf("expected Elastic=true on E2B report, got %+v", rep)
	}

	for i := 0; i < 5; i++ {
		_, err := prov.Binder.Claim(ctx, provider.ClaimRequest{
			Owner:    "ss-owner-1",
			Key:      fmt.Sprintf("list-key-%d", i),
			SpecHash: "h1",
			Template: "base",
			TTL:      10 * time.Minute,
		})
		if err != nil {
			t.Fatalf("Claim %d: %v", i, err)
		}
	}

	var all []provider.Handle
	pageToken := ""
	for {
		hs, next, err := prov.Lister.ListOwned(ctx, "ss-owner-1", pageToken, 2)
		if err != nil {
			t.Fatalf("ListOwned: %v", err)
		}
		all = append(all, hs...)
		if next == "" {
			break
		}
		pageToken = next
	}
	if len(all) != 5 {
		t.Fatalf("ListOwned returned %d handles across pages, want 5", len(all))
	}
	for _, h := range all {
		if h.ID == "" || h.Version == "" || h.ExpiresAt.IsZero() {
			t.Fatalf("incomplete handle from ListOwned: %+v", h)
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

	// Empty template mapping ref is rejected at construction time.
	if _, err := adapter.New(e2b.AdapterName, adapter.Config{
		ProviderID: "e2b",
		Templates:  map[string]string{"python": "   "},
	}); err == nil {
		t.Fatal("expected error for empty template mapping reference")
	}

	var gotAPIKey, gotAuthHeader string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAPIKey = r.Header.Get("X-API-Key")
		gotAuthHeader = r.Header.Get("Authorization")
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
	if gotAPIKey != "k-123" || gotAuthHeader != "" {
		t.Fatalf("x-api-key mode sent X-API-Key=%q Authorization=%q", gotAPIKey, gotAuthHeader)
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

	// Bearer authMode sets Authorization: Bearer and omits X-API-Key.
	provBearer, err := adapter.New(e2b.AdapterName, adapter.Config{
		ProviderID:  "e2b-bearer",
		Endpoint:    srv.URL,
		Credentials: map[string][]byte{"token": []byte("tok-456")},
		Options:     map[string]string{"authMode": "bearer"},
	})
	if err != nil {
		t.Fatalf("adapter.New(bearer): %v", err)
	}
	if _, err := provBearer.Source.Fetch(context.Background()); err != nil {
		t.Fatalf("Fetch(bearer): %v", err)
	}
	if gotAuthHeader != "Bearer tok-456" || gotAPIKey != "" {
		t.Fatalf("bearer mode sent Authorization=%q X-API-Key=%q", gotAuthHeader, gotAPIKey)
	}
}

