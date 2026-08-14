// Package scheduler serves placement decisions over HTTP.
//
// Why HTTP and not a CRD write per decision: a sandbox start is a 90–150ms
// budget, and `create CR → watch → reconcile → read status` spends most of it
// in etcd. The declarative API describes *configuration* — which providers
// exist, what the policy is — and this endpoint answers *questions* against
// that configuration from memory. Decisions are recorded to status
// asynchronously, so the audit trail is durable without the write being on the
// path.
//
// The handler does no I/O of its own: it reads a capacity snapshot and a cached
// policy list, both maintained by the controller.
package scheduler

import (
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/NolanFoster/sandbox-scheduler/api/v1alpha1"
	"github.com/NolanFoster/sandbox-scheduler/pkg/framework"
	"github.com/NolanFoster/sandbox-scheduler/pkg/registry"
)

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

// Request is the body of POST /schedule.
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
}

// Response is the body of a successful decision.
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

// ErrorResponse is returned for anything that is not a decision.
type ErrorResponse struct {
	Error string `json:"error"`
	// Explanation carries per-provider rejection reasons when placement failed,
	// so "unschedulable" is never the whole answer.
	Explanation string `json:"explanation,omitempty"`
}

// EndpointLookup resolves a provider name to its endpoint.
type EndpointLookup func(provider string) string

// Service answers placement questions.
type Service struct {
	Registry *registry.Registry
	Policies PolicySource
	// Endpoints is optional; when set, decisions carry the provider's address.
	Endpoints EndpointLookup
}

// Handler returns the HTTP handler.
func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /schedule", s.handleSchedule)
	mux.HandleFunc("GET /providers", s.handleProviders)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return mux
}

func (s *Service) handleSchedule(w http.ResponseWriter, r *http.Request) {
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
		writeJSON(w, http.StatusServiceUnavailable, ErrorResponse{
			Error: "no providers are configured; create a SandboxProvider",
		})
		return
	}

	policy, err := v1alpha1.SelectPolicy(s.Policies.Policies(), req.Labels)
	if err != nil {
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
		writeJSON(w, http.StatusUnprocessableEntity, ErrorResponse{
			Error: "policy " + policy.Name + " is not usable: " + err.Error(),
		})
		return
	}

	decision, err := profile.Schedule(r.Context(),
		policy.BuildRequest(req.Name, req.Requires, req.PreferProvider), candidates)
	if err != nil {
		var noCand *framework.ErrNoCandidates
		if errors.As(err, &noCand) {
			writeJSON(w, http.StatusConflict, ErrorResponse{
				Error:       err.Error(),
				Explanation: explainResults(noCand.Results),
			})
			return
		}
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
		return
	}

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

// ProviderStatus is one entry of GET /providers.
type ProviderStatus struct {
	Provider     string `json:"provider"`
	Reachable    bool   `json:"reachable"`
	Stale        bool   `json:"stale"`
	WarmCapacity int    `json:"warmCapacity"`
	AgeSeconds   int64  `json:"ageSeconds"`
	LastError    string `json:"lastError,omitempty"`
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
			AgeSeconds:   int64(st.Age / time.Second),
		}
		if st.LastError != nil {
			ps.LastError = st.LastError.Error()
		}
		out = append(out, ps)
	}
	writeJSON(w, http.StatusOK, out)
}

func explainResults(results []framework.CandidateResult) string {
	return framework.ExplainResults(results)
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
