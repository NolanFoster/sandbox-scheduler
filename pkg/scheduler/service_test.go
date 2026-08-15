package scheduler_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/NolanFoster/sandbox-scheduler/api/v1alpha1"
	"github.com/NolanFoster/sandbox-scheduler/pkg/registry"
	"github.com/NolanFoster/sandbox-scheduler/pkg/scheduler"
)

func newService(t *testing.T, reports map[string]registry.Report, policies ...v1alpha1.SandboxPlacementPolicy) http.Handler {
	t.Helper()
	return newServiceWithConfig(t, reports, nil, policies...)
}

// newServiceWithConfig separates what a provider reports (capacity) from what
// the operator declares (attributes, cost) — the split that keeps a provider
// from asserting facts about its own isolation.
func newServiceWithConfig(t *testing.T, reports map[string]registry.Report,
	configs map[string]registry.ProviderConfig, policies ...v1alpha1.SandboxPlacementPolicy) http.Handler {
	t.Helper()
	reg := registry.New(registry.Options{})
	for id, rep := range reports {
		reg.Report(id, rep)
	}
	for id, cfg := range configs {
		reg.SetConfig(id, cfg)
	}
	pl := &scheduler.PolicyList{}
	pl.Set(policies)
	svc := &scheduler.Service{
		Registry: reg,
		Policies: pl,
		Endpoints: func(p string) string {
			return "https://" + p + ".example.com"
		},
	}
	return svc.Handler()
}

func post(t *testing.T, h http.Handler, body string) (*httptest.ResponseRecorder, scheduler.Response, scheduler.ErrorResponse) {
	t.Helper()
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(http.MethodPost, "/schedule", nil)
	} else {
		r = httptest.NewRequest(http.MethodPost, "/schedule", strings.NewReader(body))
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	var ok scheduler.Response
	var bad scheduler.ErrorResponse
	raw := w.Body.Bytes()
	_ = json.Unmarshal(raw, &ok)
	_ = json.Unmarshal(raw, &bad)
	return w, ok, bad
}

func TestSchedulePicksTheCheapWarmProvider(t *testing.T) {
	h := newService(t, map[string]registry.Report{
		"civo": {WarmCapacity: 3},
		"gke":  {WarmCapacity: 8},
	})
	w, resp, _ := post(t, h, `{"name":"sb-1"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status %d, body %s", w.Code, w.Body)
	}
	if resp.Provider == "" {
		t.Fatal("a decision must name a provider")
	}
	if resp.Explanation == "" {
		t.Fatal("every decision must carry its explanation")
	}
	if resp.Endpoint != "https://"+resp.Provider+".example.com" {
		t.Fatalf("endpoint %q should be resolved for the caller", resp.Endpoint)
	}
}

func TestScheduleWithNoBodyStillWorks(t *testing.T) {
	// The simplest possible call: "place something, anywhere sensible".
	h := newService(t, map[string]registry.Report{"civo": {WarmCapacity: 1}})
	w, resp, _ := post(t, h, "")
	if w.Code != http.StatusOK {
		t.Fatalf("status %d, body %s", w.Code, w.Body)
	}
	if resp.Provider != "civo" {
		t.Fatalf("provider %q, want civo", resp.Provider)
	}
}

func TestNoProvidersConfiguredIsDistinctFromUnschedulable(t *testing.T) {
	// An operator problem, not a policy one — and the fix is completely
	// different, so the two must not share a status code.
	h := newService(t, nil)
	w, _, errResp := post(t, h, `{}`)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503", w.Code)
	}
	if !strings.Contains(errResp.Error, "SandboxProvider") {
		t.Fatalf("error should say what to create, got %q", errResp.Error)
	}
}

func TestUnschedulableReturns409WithPerProviderReasons(t *testing.T) {
	// "unschedulable" alone is the most common complaint about schedulers.
	policy := v1alpha1.SandboxPlacementPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "gpu"},
		Spec: v1alpha1.SandboxPlacementPolicySpec{
			Filters:  []string{"RequiredAttributes"},
			Requires: map[string]string{"gpu": "true"},
		},
	}
	h := newService(t, map[string]registry.Report{
		"civo": {WarmCapacity: 3},
		"gke":  {WarmCapacity: 3},
	}, policy)

	w, _, errResp := post(t, h, `{}`)
	if w.Code != http.StatusConflict {
		t.Fatalf("status %d, want 409", w.Code)
	}
	for _, want := range []string{"civo", "gke", "gpu"} {
		if !strings.Contains(errResp.Explanation+errResp.Error, want) {
			t.Fatalf("response should mention %q, got %s / %s", want, errResp.Error, errResp.Explanation)
		}
	}
}

func TestPolicyIsSelectedByLabels(t *testing.T) {
	strict := v1alpha1.SandboxPlacementPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "isolated"},
		Spec: v1alpha1.SandboxPlacementPolicySpec{
			Priority: 10,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"tier": "untrusted"}},
			Filters:  []string{"RequiredAttributes"},
			Requires: map[string]string{"runtime": "gvisor"},
		},
	}
	// Reported by the provider, NOT declared by the operator. This must not
	// satisfy the policy's runtime requirement.
	h := newService(t, map[string]registry.Report{
		"civo": {WarmCapacity: 3, Attributes: map[string]string{"runtime": "gvisor"}},
	}, strict)

	// Labels do not match: the default policy applies and placement succeeds.
	w, resp, _ := post(t, h, `{"labels":{"tier":"normal"}}`)
	if w.Code != http.StatusOK || resp.Policy != "default" {
		t.Fatalf("want the default policy, got %d / %q", w.Code, resp.Policy)
	}

	// Labels match: the strict policy applies and placement fails, because the
	// only provider merely *claims* gvisor rather than being declared as such.
	w, _, _ = post(t, h, `{"labels":{"tier":"untrusted"}}`)
	if w.Code != http.StatusConflict {
		t.Fatalf("status %d, want 409 — self-reported attributes must not satisfy a requirement", w.Code)
	}

	// Declared by the operator: now it satisfies the same policy.
	h = newServiceWithConfig(t,
		map[string]registry.Report{"civo": {WarmCapacity: 3}},
		map[string]registry.ProviderConfig{"civo": {Attributes: map[string]string{"runtime": "gvisor"}}},
		strict)
	w, resp, _ = post(t, h, `{"labels":{"tier":"untrusted"}}`)
	if w.Code != http.StatusOK || resp.Policy != "isolated" {
		t.Fatalf("a declared attribute should satisfy the policy: %d / %q", w.Code, resp.Policy)
	}
}

func TestInvalidPolicyReturns422(t *testing.T) {
	broken := v1alpha1.SandboxPlacementPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "broken"},
		Spec: v1alpha1.SandboxPlacementPolicySpec{
			Requires: map[string]string{"gpu": "true"}, // no enforcing filter
		},
	}
	h := newService(t, map[string]registry.Report{"civo": {WarmCapacity: 1}}, broken)
	w, _, errResp := post(t, h, `{}`)
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status %d, want 422", w.Code)
	}
	if !strings.Contains(errResp.Error, "broken") {
		t.Fatalf("error should name the offending policy, got %q", errResp.Error)
	}
}

func TestMalformedJSONIsRejected(t *testing.T) {
	h := newService(t, map[string]registry.Report{"civo": {WarmCapacity: 1}})
	w, _, errResp := post(t, h, `{not json`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status %d, want 400", w.Code)
	}
	if errResp.Error == "" {
		t.Fatal("a 400 should say what was wrong")
	}
}

func TestPreferProviderIsHonouredWhenViable(t *testing.T) {
	h := newService(t, map[string]registry.Report{
		"civo": {WarmCapacity: 3},
		"gke":  {WarmCapacity: 3},
	})
	_, resp, _ := post(t, h, `{"preferProvider":"gke"}`)
	if resp.Provider != "gke" {
		t.Fatalf("provider %q; affinity should win an otherwise even contest:\n%s",
			resp.Provider, resp.Explanation)
	}
}

func TestPreferProviderDoesNotPinToAColdOne(t *testing.T) {
	h := newService(t, map[string]registry.Report{
		"civo": {WarmCapacity: 5},
		"gke":  {WarmCapacity: 0},
	})
	_, resp, _ := post(t, h, `{"preferProvider":"gke"}`)
	if resp.Provider != "civo" {
		t.Fatalf("provider %q; a session must move rather than wait on a cold provider:\n%s",
			resp.Provider, resp.Explanation)
	}
}

func TestGetProvidersExposesWhatTheSchedulerBelieves(t *testing.T) {
	// The first thing to check when a placement looks wrong.
	h := newService(t, map[string]registry.Report{"civo": {WarmCapacity: 2}})
	r := httptest.NewRequest(http.MethodGet, "/providers", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status %d", w.Code)
	}
	var out []scheduler.ProviderStatus
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("unparseable body: %v", err)
	}
	if len(out) != 1 || out[0].Provider != "civo" || out[0].WarmCapacity != 2 {
		t.Fatalf("unexpected body: %+v", out)
	}
	if !out[0].Reachable {
		t.Fatal("a freshly reported provider should be reachable")
	}
}

func TestHealthzIsUnauthenticatedAndCheap(t *testing.T) {
	h := newService(t, nil)
	r := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status %d", w.Code)
	}
}

func TestGetOnScheduleIsNotAllowed(t *testing.T) {
	h := newService(t, map[string]registry.Report{"civo": {WarmCapacity: 1}})
	r := httptest.NewRequest(http.MethodGet, "/schedule", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code == http.StatusOK {
		t.Fatal("GET /schedule should not return a decision")
	}
}

func TestPolicyListIsSafeToReplaceConcurrently(t *testing.T) {
	// Run with -race. The controller replaces this while requests read it.
	pl := &scheduler.PolicyList{}
	done := make(chan struct{})
	go func() {
		for i := 0; i < 200; i++ {
			pl.Set([]v1alpha1.SandboxPlacementPolicy{*v1alpha1.DefaultPolicy()})
		}
		close(done)
	}()
	for i := 0; i < 200; i++ {
		_ = pl.Policies()
	}
	<-done
}

func TestPolicyListReturnsACopy(t *testing.T) {
	pl := &scheduler.PolicyList{}
	pl.Set([]v1alpha1.SandboxPlacementPolicy{*v1alpha1.DefaultPolicy()})
	got := pl.Policies()
	got[0].Name = "tampered"
	if pl.Policies()[0].Name == "tampered" {
		t.Fatal("mutating the returned slice changed the source")
	}
}

// --- authentication --------------------------------------------------------

func newAuthedService(t *testing.T, token string) http.Handler {
	t.Helper()
	reg := registry.New(registry.Options{})
	reg.Report("civo", registry.Report{WarmCapacity: 1})
	pl := &scheduler.PolicyList{}
	svc := &scheduler.Service{Registry: reg, Policies: pl, Token: token}
	return svc.Handler()
}

func do(h http.Handler, method, path, auth string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader("{}"))
	if auth != "" {
		r.Header.Set("Authorization", auth)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestProtectedRoutesRequireTheToken(t *testing.T) {
	// GET /providers discloses provider names, endpoints and live capacity —
	// reconnaissance for anyone deciding where to aim.
	h := newAuthedService(t, "s3cret")
	for _, tc := range []struct{ method, path string }{
		{http.MethodPost, "/schedule"},
		{http.MethodGet, "/providers"},
	} {
		if got := do(h, tc.method, tc.path, "").Code; got != http.StatusUnauthorized {
			t.Fatalf("%s %s without a token returned %d, want 401", tc.method, tc.path, got)
		}
		if got := do(h, tc.method, tc.path, "Bearer wrong").Code; got != http.StatusUnauthorized {
			t.Fatalf("%s %s with a wrong token returned %d, want 401", tc.method, tc.path, got)
		}
		if got := do(h, tc.method, tc.path, "Bearer s3cret").Code; got == http.StatusUnauthorized {
			t.Fatalf("%s %s with the right token was rejected", tc.method, tc.path)
		}
	}
}

func TestHealthzStaysPublic(t *testing.T) {
	// kubelet probes carry no credential, and it discloses nothing.
	h := newAuthedService(t, "s3cret")
	if got := do(h, http.MethodGet, "/healthz", "").Code; got != http.StatusOK {
		t.Fatalf("healthz returned %d without a token, want 200", got)
	}
}

func TestNoTokenConfiguredLeavesTheApiOpen(t *testing.T) {
	// The cluster-internal default. Documented as only defensible when nothing
	// outside the cluster can reach it.
	h := newAuthedService(t, "")
	if got := do(h, http.MethodGet, "/providers", "").Code; got != http.StatusOK {
		t.Fatalf("unauthenticated service returned %d, want 200", got)
	}
}

func TestAuthSchemeIsCaseInsensitiveButTokenIsNot(t *testing.T) {
	h := newAuthedService(t, "s3cret")
	if got := do(h, http.MethodGet, "/providers", "bearer s3cret").Code; got == http.StatusUnauthorized {
		t.Fatal("the scheme is case-insensitive per RFC 7235")
	}
	if got := do(h, http.MethodGet, "/providers", "Bearer S3CRET").Code; got != http.StatusUnauthorized {
		t.Fatalf("token comparison must be case-sensitive, got %d", got)
	}
}

func TestMalformedAuthorizationHeadersAreRejected(t *testing.T) {
	h := newAuthedService(t, "s3cret")
	for _, header := range []string{"s3cret", "Bearer", "Bearer ", "Basic s3cret", "Bearers3cret"} {
		if got := do(h, http.MethodGet, "/providers", header).Code; got != http.StatusUnauthorized {
			t.Fatalf("header %q returned %d, want 401", header, got)
		}
	}
}

// Fleet-wide callers need every provider's address from one request. Keeping a
// second copy of that mapping elsewhere is how a cluster ends up invisible
// while still running workloads.
func TestProvidersReportEndpoints(t *testing.T) {
	h := newService(t, map[string]registry.Report{
		"civo": {WarmCapacity: 3},
		"gke":  {WarmCapacity: 1},
	})

	w := do(h, http.MethodGet, "/providers", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status %d, want 200: %s", w.Code, w.Body.String())
	}
	var got []scheduler.ProviderStatus
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decoding: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d providers, want 2", len(got))
	}
	for _, ps := range got {
		want := "https://" + ps.Provider + ".example.com"
		if ps.Endpoint != want {
			t.Fatalf("provider %q endpoint %q, want %q", ps.Provider, ps.Endpoint, want)
		}
	}
}

// Without a lookup the field is absent rather than blank, so a caller can tell
// "not configured" from "configured as empty" instead of fanning out to "".
func TestProvidersOmitEndpointWhenUnset(t *testing.T) {
	reg := registry.New(registry.Options{})
	reg.Report("civo", registry.Report{WarmCapacity: 1})
	pl := &scheduler.PolicyList{}
	pl.Set(nil)
	svc := &scheduler.Service{Registry: reg, Policies: pl}

	w := do(svc.Handler(), http.MethodGet, "/providers", "")
	if strings.Contains(w.Body.String(), "endpoint") {
		t.Fatalf("endpoint should be omitted entirely:\n%s", w.Body.String())
	}
}
