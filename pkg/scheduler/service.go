// Package scheduler serves placement decisions and sandbox claims over HTTP.
//
// Why HTTP and not a CRD write per decision: a sandbox start is a 90–150ms
// budget, and `create CR → watch → reconcile → read status` spends most of it
// in etcd. The declarative API describes *configuration* — which providers
// exist, what the policy is — and this endpoint answers *questions* against
// that configuration from memory, optionally executing the provider claim on
// the caller's behalf. Decisions are recorded to status asynchronously, so the
// audit trail is durable without the write being on the path.
package scheduler

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/NolanFoster/sandbox-scheduler/api/v1alpha1"
	"github.com/NolanFoster/sandbox-scheduler/pkg/binder"
	"github.com/NolanFoster/sandbox-scheduler/pkg/framework"
	"github.com/NolanFoster/sandbox-scheduler/pkg/metrics"
	"github.com/NolanFoster/sandbox-scheduler/pkg/plugins"
	"github.com/NolanFoster/sandbox-scheduler/pkg/registry"
)

// maxClaimBodyBytes caps POST /api/v1alpha1/sandboxes request bodies at 64 KiB.
const maxClaimBodyBytes = 64 << 10

// PolicySource supplies the policies currently in the cluster.
type PolicySource interface {
	Policies() []v1alpha1.SandboxPlacementPolicy
}

// PolicyList is a trivial PolicySource, safe for concurrent use.
type PolicyList struct {
	mu       sync.RWMutex
	policies []v1alpha1.SandboxPlacementPolicy
}

// Set replaces the policy list.
func (p *PolicyList) Set(policies []v1alpha1.SandboxPlacementPolicy) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.policies = append([]v1alpha1.SandboxPlacementPolicy(nil), policies...)
}

// Policies implements PolicySource.
func (p *PolicyList) Policies() []v1alpha1.SandboxPlacementPolicy {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return append([]v1alpha1.SandboxPlacementPolicy(nil), p.policies...)
}

// Request is the body of POST /schedule and POST /api/v1alpha1/schedule.
type Request struct {
	// Name identifies the sandbox being placed, for logs and traces.
	Name string `json:"name,omitempty"`
	// Labels select which SandboxPlacementPolicy governs this request.
	Labels map[string]string `json:"labels,omitempty"`
	// Requires are hard attribute requirements from the caller. The governing
	// policy's own requirements are merged over these and win on conflict.
	Requires map[string]string `json:"requires,omitempty"`
	// PreferProvider asks to return to a provider, typically where a
	// hibernated session's data already is. A preference, never a pin.
	PreferProvider string `json:"preferProvider,omitempty"`
	// Template optionally filters candidates to providers that declare this
	// logical template name.
	Template string `json:"template,omitempty"`
	// WarmOnly requires a warm sandbox from a pre-warmed pool.
	WarmOnly bool `json:"warmOnly,omitempty"`
}

// Response is the body of a successful placement decision.
type Response struct {
	Provider string `json:"provider"`
	// Endpoint is the provider's address, so the caller does not need its own
	// copy of the provider registry to act on the decision.
	Endpoint string `json:"endpoint,omitempty"`
	Score    int64  `json:"score"`
	Policy   string `json:"policy"`
	Reason   string `json:"reason,omitempty"`
	// Explanation is the full per-candidate reasoning. Always present: a
	// decision that cannot be explained after the fact cannot be operated.
	Explanation string `json:"explanation"`
}

// ClaimRequest is the JSON body of POST /api/v1alpha1/sandboxes.
type ClaimRequest struct {
	Template       string            `json:"template,omitempty"`
	Key            string            `json:"key"`
	Retry          bool              `json:"retry,omitempty"`
	Labels         map[string]string `json:"labels,omitempty"`
	Requires       map[string]string `json:"requires,omitempty"`
	PreferProvider string            `json:"preferProvider,omitempty"`
	PinProvider    string            `json:"pinProvider,omitempty"`
	WarmOnly       bool              `json:"warmOnly,omitempty"`
	TTL            string            `json:"ttl,omitempty"`
	TTLSeconds     *int64            `json:"ttlSeconds,omitempty"`
	Timeout        string            `json:"timeout,omitempty"`
	TimeoutSeconds *int64            `json:"timeoutSeconds,omitempty"`
}

// ClaimResponse is the JSON body of a successful POST /api/v1alpha1/sandboxes.
type ClaimResponse struct {
	ID          string            `json:"id"`
	Provider    string            `json:"provider"`
	Endpoint    string            `json:"endpoint,omitempty"`
	Connect     map[string]string `json:"connect,omitempty"`
	ExpiresAt   *time.Time        `json:"expiresAt,omitempty"`
	Replay      bool              `json:"replay"`
	Warm        bool              `json:"warm"`
	Policy      string            `json:"policy"`
	Score       int64             `json:"score"`
	Explanation string            `json:"explanation,omitempty"`
}

// ErrorResponse is returned for anything that is not a decision or claim.
type ErrorResponse struct {
	Error string `json:"error"`
	// Reason is a stable machine-readable error code on claim routes.
	Reason string `json:"reason,omitempty"`
	// Providers lists the provider names touched or implicated before failing.
	Providers []string `json:"providers,omitempty"`
	// RetryAfterMs is the recommended client retry delay in milliseconds.
	RetryAfterMs int64 `json:"retryAfterMs,omitempty"`
	// Explanation carries per-provider rejection reasons when placement failed,
	// so "unschedulable" is never the whole answer.
	Explanation string `json:"explanation,omitempty"`
}

// EndpointLookup resolves a provider name to its endpoint.
type EndpointLookup func(provider string) string

// Service answers placement questions and executes sandbox claims.
type Service struct {
	Registry *registry.Registry
	Policies PolicySource
	// Endpoints is optional; when set, decisions carry the provider's address.
	Endpoints EndpointLookup

	// Binder executes provider claims when ClaimsEnabled is true.
	Binder *binder.Binder
	// ClaimsEnabled gates POST /api/v1alpha1/sandboxes and DELETE
	// /api/v1alpha1/sandboxes/{provider}/{id}. When false, those routes return
	// 501 Not Implemented so placement-only deployments have zero claim surface.
	ClaimsEnabled bool

	// Token, when set, is required as a bearer credential on every route
	// except /healthz (and claim routes when ClaimToken is set separately).
	//
	// Empty means unauthenticated, which is only defensible for a Service
	// reachable solely from inside the cluster. GET /providers discloses the
	// fleet: provider names, gateway endpoints and live capacity. That is
	// reconnaissance for anyone deciding where to aim, so anything exposing
	// this beyond the cluster must set a token.
	Token string

	// ClaimToken, when set, is required on POST /api/v1alpha1/sandboxes and
	// DELETE /api/v1alpha1/sandboxes/{provider}/{id}, separating permission to
	// allocate billable sandboxes from permission to query placement. When
	// empty, claim routes fall back to Token.
	ClaimToken string

	// Active, when set, reports whether this replica may place or claim.
	// Placement reserves capacity in this process's memory, which is only
	// exact while one replica makes every decision, so a standby refuses
	// rather than placing against reservations it cannot see. Nil means always
	// active.
	Active func() bool
}

// Handler returns the HTTP handler.
func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /schedule", withDeprecation(s.requireReadAuth(s.handleSchedule)))
	mux.HandleFunc("GET /providers", withDeprecation(s.requireReadAuth(s.handleProviders)))
	mux.HandleFunc("POST /api/v1alpha1/schedule", s.requireReadAuth(s.handleSchedule))
	mux.HandleFunc("GET /api/v1alpha1/providers", s.requireReadAuth(s.handleProviders))
	mux.HandleFunc("POST /api/v1alpha1/sandboxes", s.requireClaimAuth(s.handleClaim))
	mux.HandleFunc("DELETE /api/v1alpha1/sandboxes/{provider}/{id}", s.requireClaimAuth(s.handleRelease))
	return s.withHealthz(mux)
}

func withDeprecation(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Deprecation", "true")
		w.Header().Set("Sunset", "v0.3.0")
		next(w, r)
	}
}

// withHealthz leaves /healthz public for kubelet probes, which carry no
// credential. It discloses nothing: a fixed string, no fleet state.
func (s *Service) withHealthz(next http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.Handle("/", next)
	return mux
}

func (s *Service) requireReadAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.Token != "" && !authorizedWithToken(r, s.Token) {
			writeJSON(w, http.StatusUnauthorized, ErrorResponse{
				Error: "missing or invalid bearer token",
			})
			return
		}
		next(w, r)
	}
}

func (s *Service) requireClaimAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tok := s.ClaimToken
		if tok == "" {
			tok = s.Token
		}
		if tok != "" && !authorizedWithToken(r, tok) {
			writeJSON(w, http.StatusUnauthorized, ErrorResponse{
				Error: "missing or invalid bearer token",
			})
			return
		}
		next(w, r)
	}
}

func authorizedWithToken(r *http.Request, expected string) bool {
	header := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if len(header) <= len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return false
	}
	// Constant time: a token check that returns faster on an early mismatch
	// leaks the token a byte at a time to anyone patient enough to measure.
	return subtle.ConstantTimeCompare([]byte(header[len(prefix):]), []byte(expected)) == 1
}

func (s *Service) handleSchedule(w http.ResponseWriter, r *http.Request) {
	// Timed around the decision only. HTTP overhead is the caller's transport
	// problem; what this defends is the claim that placement itself reads local
	// memory and stays out of the API server.
	start := time.Now()

	if s.Active != nil && !s.Active() {
		// Normally unreachable: the placement Service only selects the
		// leader. This covers a request that arrives during a handover, or
		// one sent to a pod directly.
		metrics.RecordFailure("standby", nil, "", 0)
		writeStandby(w)
		return
	}

	var req Request
	if r.ContentLength != 0 {
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "invalid JSON body: " + err.Error()})
			return
		}
	}

	candidates := s.Registry.Snapshot()
	if len(candidates) == 0 {
		// Distinct from "nothing satisfied the request": there is nothing
		// configured at all, which is an operator problem, not a policy one.
		metrics.RecordFailure("no_providers", nil, "", 0)
		writeJSON(w, http.StatusServiceUnavailable, ErrorResponse{
			Error:  "no providers are configured; create a SandboxProvider",
			Reason: binder.ReasonNoProviders,
		})
		return
	}

	policy, err := v1alpha1.SelectPolicy(s.Policies.Policies(), req.Labels)
	if err != nil {
		metrics.RecordFailure("policy_selection_error", nil, "", 0)
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
		return
	}
	if policy == nil {
		// A cluster with providers but no policy still schedules sensibly
		// rather than refusing.
		policy = v1alpha1.DefaultPolicy()
	}

	profile, err := policy.BuildProfile(candidates)
	if err != nil {
		// The policy controller marks these invalid on the object; this is the
		// belt to that braces, for a policy edited between reconciles.
		metrics.RecordFailure("invalid_policy", nil, policy.Name, 0)
		writeJSON(w, http.StatusUnprocessableEntity, ErrorResponse{
			Error:  "policy " + policy.Name + " is not usable: " + err.Error(),
			Reason: binder.ReasonInvalidPolicy,
		})
		return
	}
	if req.Template != "" && !hasFilter(profile.Filters, "TemplateAvailable") {
		profile.Filters = append(profile.Filters, plugins.TemplateAvailable{})
	}
	if req.WarmOnly && !hasFilter(profile.Filters, "WarmOnly") {
		profile.Filters = append(profile.Filters, plugins.WarmOnly{})
	}
	schedReq := policy.BuildRequest(req.Name, req.Requires, req.PreferProvider)
	schedReq.Template = req.Template
	schedReq.WarmOnly = req.WarmOnly

	var decision *framework.Decision
	for {
		decision, err = profile.Schedule(r.Context(), schedReq, candidates)
		if err != nil {
			var noCand *framework.ErrNoCandidates
			if errors.As(err, &noCand) {
				metrics.RecordFailure("no_candidates", noCand.Results, policy.Name, time.Since(start))
				writeJSON(w, http.StatusConflict, ErrorResponse{
					Error:       err.Error(),
					Reason:      binder.ReasonNoCapacity,
					Explanation: explainResults(noCand.Results),
				})
				return
			}
			metrics.RecordFailure("error", nil, policy.Name, time.Since(start))
			writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
			return
		}

		// Claim the warm sandbox this placement will take, so the requests
		// behind it in a burst see what is left rather than what the last
		// poll saw. A cold placement takes nothing from the pool and reserves
		// nothing.
		if !warmIn(candidates, decision.Provider) || s.Registry.TryReserveWarm(decision.Provider) {
			break
		}
		// A concurrent placement took the last of this provider's warm
		// capacity, so the decision counted on a sandbox that is gone. Mark it
		// empty in this request's copy and decide again: every other
		// provider's warm count is still checked when it is claimed, and each
		// pass empties one more provider, so this ends within one pass per
		// warm provider, on a sandbox that was free or on a cold placement no
		// warm one could have beaten.
		for i := range candidates {
			if candidates[i].Provider == decision.Provider {
				candidates[i].WarmCapacity = 0
			}
		}
	}

	if !warmIn(candidates, decision.Provider) {
		// Counted so that the cold placements behind this one in a burst go
		// to whichever equally good provider is less loaded.
		s.Registry.RecordColdPlacement(decision.Provider)
	}

	metrics.RecordDecision(policy.Name, decision, candidates, time.Since(start))

	resp := Response{
		Provider:    decision.Provider,
		Score:       decision.Score,
		Policy:      policy.Name,
		Explanation: decision.Explain(),
	}
	if s.Endpoints != nil {
		resp.Endpoint = s.Endpoints(decision.Provider)
	}
	for _, res := range decision.Results {
		if res.Provider == decision.Provider && len(res.Details) > 0 {
			resp.Reason = res.Details[0].Scorer
			break
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Service) handleClaim(w http.ResponseWriter, r *http.Request) {
	if !s.ClaimsEnabled || s.Binder == nil {
		writeJSON(w, http.StatusNotImplemented, ErrorResponse{
			Error:     "sandbox claims are disabled; start the scheduler with --enable-claims",
			Reason:    binder.ReasonClaimsDisabled,
			Providers: []string{},
		})
		return
	}

	if s.Active != nil && !s.Active() {
		metrics.RecordFailure("standby", nil, "", 0)
		writeStandby(w)
		return
	}

	var raw ClaimRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxClaimBodyBytes)).Decode(&raw); err != nil {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{
			Error:     "invalid JSON body: " + err.Error(),
			Reason:    binder.ReasonInvalidRequest,
			Providers: []string{},
		})
		return
	}

	const maxSecondsField = int64(time.Hour * 24 * 365 / time.Second)
	var ttl time.Duration
	if raw.TTLSeconds != nil {
		if *raw.TTLSeconds <= 0 || *raw.TTLSeconds > maxSecondsField {
			writeJSON(w, http.StatusBadRequest, ErrorResponse{
				Error:     "invalid ttlSeconds: must be positive",
				Reason:    binder.ReasonInvalidRequest,
				Providers: []string{},
			})
			return
		}
		ttl = time.Duration(*raw.TTLSeconds) * time.Second
	} else if raw.TTL != "" {
		d, err := time.ParseDuration(raw.TTL)
		if err != nil || d <= 0 {
			writeJSON(w, http.StatusBadRequest, ErrorResponse{
				Error:     "invalid ttl duration: " + raw.TTL,
				Reason:    binder.ReasonInvalidRequest,
				Providers: []string{},
			})
			return
		}
		ttl = d
	}

	var timeout time.Duration
	if raw.TimeoutSeconds != nil {
		if *raw.TimeoutSeconds <= 0 || *raw.TimeoutSeconds > maxSecondsField {
			writeJSON(w, http.StatusBadRequest, ErrorResponse{
				Error:     "invalid timeoutSeconds: must be positive",
				Reason:    binder.ReasonInvalidRequest,
				Providers: []string{},
			})
			return
		}
		timeout = time.Duration(*raw.TimeoutSeconds) * time.Second
	} else if raw.Timeout != "" {
		d, err := time.ParseDuration(raw.Timeout)
		if err != nil || d <= 0 {
			writeJSON(w, http.StatusBadRequest, ErrorResponse{
				Error:     "invalid timeout duration: " + raw.Timeout,
				Reason:    binder.ReasonInvalidRequest,
				Providers: []string{},
			})
			return
		}
		timeout = d
	}

	var policies []v1alpha1.SandboxPlacementPolicy
	if s.Policies != nil {
		policies = s.Policies.Policies()
	}
	policy, err := v1alpha1.SelectPolicy(policies, raw.Labels)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{
			Error:     err.Error(),
			Providers: []string{},
		})
		return
	}
	if policy == nil {
		policy = v1alpha1.DefaultPolicy()
	}

	res, berr := s.Binder.Bind(r.Context(), policy, binder.Request{
		Template:       raw.Template,
		Key:            raw.Key,
		Retry:          raw.Retry,
		Labels:         raw.Labels,
		Requires:       raw.Requires,
		PreferProvider: raw.PreferProvider,
		PinProvider:    raw.PinProvider,
		WarmOnly:       raw.WarmOnly,
		TTL:            ttl,
		Timeout:        timeout,
	})
	if berr != nil {
		writeBinderError(w, berr)
		return
	}

	endpoint := res.Handle.Endpoint
	if endpoint == "" && s.Endpoints != nil {
		endpoint = s.Endpoints(res.Handle.Provider)
	}
	var expiresAt *time.Time
	if !res.Handle.ExpiresAt.IsZero() {
		t := res.Handle.ExpiresAt.UTC()
		expiresAt = &t
	}
	status := http.StatusCreated
	if res.Replay {
		status = http.StatusOK
	}
	writeJSON(w, status, ClaimResponse{
		ID:          res.Handle.ID,
		Provider:    res.Handle.Provider,
		Endpoint:    endpoint,
		Connect:     res.Handle.Connect,
		ExpiresAt:   expiresAt,
		Replay:      res.Replay,
		Warm:        res.Warm,
		Policy:      res.Policy,
		Score:       res.Score,
		Explanation: res.Explanation,
	})
}

func (s *Service) handleRelease(w http.ResponseWriter, r *http.Request) {
	if !s.ClaimsEnabled || s.Binder == nil {
		writeJSON(w, http.StatusNotImplemented, ErrorResponse{
			Error:     "sandbox claims are disabled; start the scheduler with --enable-claims",
			Reason:    binder.ReasonClaimsDisabled,
			Providers: []string{},
		})
		return
	}

	if s.Active != nil && !s.Active() {
		writeStandby(w)
		return
	}

	providerID := r.PathValue("provider")
	sandboxID := r.PathValue("id")
	if providerID == "" || sandboxID == "" {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{
			Error:     "provider and sandbox id are required",
			Reason:    binder.ReasonInvalidRequest,
			Providers: []string{},
		})
		return
	}

	if berr := s.Binder.Release(r.Context(), providerID, sandboxID); berr != nil {
		writeBinderError(w, berr)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func writeStandby(w http.ResponseWriter) {
	retryWait := time.Second + time.Duration(rand.Int64N(int64(3*time.Second)))
	setRetryAfterHeader(w, retryWait)
	writeJSON(w, http.StatusServiceUnavailable, ErrorResponse{
		Error:        "this replica is a standby; placement is served by the elected leader",
		Reason:       binder.ReasonStandby,
		Providers:    []string{},
		RetryAfterMs: retryWait.Milliseconds(),
	})
}

func writeBinderError(w http.ResponseWriter, berr *binder.Error) {
	providers := berr.Providers
	if providers == nil {
		providers = []string{}
	}
	var retryMs int64
	if berr.RetryAfter > 0 {
		setRetryAfterHeader(w, berr.RetryAfter)
		retryMs = berr.RetryAfter.Milliseconds()
	}
	writeJSON(w, berr.Status, ErrorResponse{
		Error:        berr.Message,
		Reason:       berr.Reason,
		Providers:    providers,
		RetryAfterMs: retryMs,
		Explanation:  berr.Explanation,
	})
}

func setRetryAfterHeader(w http.ResponseWriter, d time.Duration) {
	secs := int((d + time.Second - 1) / time.Second)
	if secs < 1 {
		secs = 1
	}
	w.Header().Set("Retry-After", strconv.Itoa(secs))
}

// ProviderStatus is one entry of GET /providers and GET /api/v1alpha1/providers.
type ProviderStatus struct {
	Provider string `json:"provider"`

	// Endpoint is the provider's address. Callers that need to reach every
	// cluster in the fleet — to list what is running on each, say — would
	// otherwise have to keep their own copy of this mapping, which then drifts
	// from the SandboxProvider objects that actually define it. One
	// unremarked-on difference and a whole cluster's workloads become
	// invisible while still running.
	//
	// Omitted when no endpoint lookup is configured.
	Endpoint string `json:"endpoint,omitempty"`

	Reachable    bool `json:"reachable"`
	Stale        bool `json:"stale"`
	WarmCapacity int  `json:"warmCapacity"`
	// Reserved is how many placements since the last poll are being
	// subtracted from WarmCapacity when scheduling.
	Reserved int `json:"reserved"`
	// InFlightCold is how many cold starts were placed here within the cold
	// start window; ties between cold providers go to the less loaded.
	InFlightCold int               `json:"inFlightCold"`
	Templates    map[string]string `json:"templates,omitempty"`
	AgeSeconds   int64             `json:"ageSeconds"`
	LastError    string            `json:"lastError,omitempty"`
}

// handleProviders exposes what the scheduler currently believes, which is the
// first thing to check when a placement looks wrong.
func (s *Service) handleProviders(w http.ResponseWriter, _ *http.Request) {
	statuses := s.Registry.Statuses()
	out := make([]ProviderStatus, 0, len(statuses))
	for _, st := range statuses {
		ps := ProviderStatus{
			Provider:     st.Provider,
			Reachable:    st.Reachable,
			Stale:        st.Stale,
			WarmCapacity: st.Report.WarmCapacity,
			Reserved:     st.Reserved,
			InFlightCold: st.InFlightCold,
			Templates:    st.Config.Templates,
			AgeSeconds:   int64(st.Age / time.Second),
		}
		if s.Endpoints != nil {
			ps.Endpoint = s.Endpoints(st.Provider)
		}
		if st.LastError != nil {
			ps.LastError = st.LastError.Error()
		}
		out = append(out, ps)
	}
	writeJSON(w, http.StatusOK, out)
}

// warmIn reports whether a provider showed warm capacity in candidates.
func warmIn(candidates []framework.Candidate, provider string) bool {
	for _, c := range candidates {
		if c.Provider == provider {
			return c.WarmCapacity > 0
		}
	}
	return false
}

func hasFilter(filters []framework.Filter, name string) bool {
	for _, f := range filters {
		if f.Name() == name {
			return true
		}
	}
	return false
}

func explainResults(results []framework.CandidateResult) string {
	return framework.ExplainResults(results)
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
