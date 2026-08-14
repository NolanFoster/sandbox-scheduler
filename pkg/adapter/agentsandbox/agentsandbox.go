// Package agentsandbox adapts a Kubernetes cluster running the upstream
// agent-sandbox controller.
//
// It reads warm-pool depth through a gateway's `/warmpool` endpoint rather than
// through the Kubernetes API. That is a deliberate choice: talking to the API
// server would need a kubeconfig per cluster, RBAC in each, and network reach
// to every control plane. A gateway is already how these clusters expose
// sandboxes to the outside world, so the scheduler needs no privilege it does
// not already have as a client.
//
// The response shape is upstream's:
//
//	{"name":"python-warm-pool","replicas":3,"readyReplicas":3,...}
package agentsandbox

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/NolanFoster/sandbox-scheduler/pkg/adapter"
	"github.com/NolanFoster/sandbox-scheduler/pkg/registry"
)

// AdapterName is the value used in SandboxProvider.spec.adapter.
const AdapterName = "agent-sandbox"

// CredentialKey is the Secret key holding the gateway bearer token.
const CredentialKey = "token"

func init() {
	adapter.Register(AdapterName, func(cfg adapter.Config) (registry.Source, error) {
		if cfg.Endpoint == "" {
			return nil, fmt.Errorf("adapter %s: endpoint is required", AdapterName)
		}
		return &Source{
			id:       cfg.ProviderID,
			endpoint: strings.TrimRight(cfg.Endpoint, "/"),
			token:    cfg.Credential(CredentialKey),
			pool:     cfg.Options["warmPoolName"],
			client:   &http.Client{Timeout: 30 * time.Second},
		}, nil
	})
}

// Source polls one agent-sandbox gateway.
type Source struct {
	id       string
	endpoint string
	token    string
	pool     string
	client   *http.Client
}

// ProviderID implements registry.Source.
func (s *Source) ProviderID() string { return s.id }

type warmPoolResponse struct {
	Name          string `json:"name"`
	Replicas      *int   `json:"replicas"`
	ReadyReplicas *int   `json:"readyReplicas"`
	Template      string `json:"template"`
}

// Fetch reads warm-pool depth from the gateway.
func (s *Source) Fetch(ctx context.Context) (registry.Report, error) {
	url := s.endpoint + "/warmpool"
	if s.pool != "" {
		url += "?name=" + s.pool
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return registry.Report{}, err
	}
	if s.token != "" {
		req.Header.Set("Authorization", "Bearer "+s.token)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return registry.Report{}, fmt.Errorf("%s: %w", s.id, err)
	}
	defer resp.Body.Close()

	// Bound the read: a provider returning an unbounded body must not be able
	// to exhaust the scheduler's memory, and a capacity response is tiny.
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return registry.Report{}, fmt.Errorf("%s: reading response: %w", s.id, err)
	}

	switch {
	case resp.StatusCode == http.StatusNotFound:
		// The cluster is reachable and answering; it just has no warm pool.
		// That is zero warm capacity, not a failure — cold-start capacity may
		// well exist, and treating it as an error would make the provider look
		// unreachable and drop it out of contention entirely.
		return registry.Report{WarmCapacity: 0}, nil
	case resp.StatusCode == http.StatusUnauthorized, resp.StatusCode == http.StatusForbidden:
		// Called out separately because it is the failure an operator can
		// actually fix, and it is indistinguishable from "provider down" in a
		// generic status-code error.
		return registry.Report{}, fmt.Errorf(
			"%s: gateway rejected the credential (HTTP %d) — check the Secret referenced by credentialsRef",
			s.id, resp.StatusCode)
	case resp.StatusCode < 200 || resp.StatusCode >= 300:
		return registry.Report{}, fmt.Errorf("%s: gateway returned HTTP %d: %s",
			s.id, resp.StatusCode, truncate(string(body), 200))
	}

	var wp warmPoolResponse
	if err := json.Unmarshal(body, &wp); err != nil {
		return registry.Report{}, fmt.Errorf("%s: gateway returned unparseable JSON: %w", s.id, err)
	}
	if wp.ReadyReplicas == nil {
		// A 200 without the field means we are talking to something that is not
		// this API. Reporting zero would look like a healthy, empty pool and
		// quietly divert traffic elsewhere forever.
		return registry.Report{}, fmt.Errorf(
			"%s: gateway response has no readyReplicas field; is %s an agent-sandbox gateway?",
			s.id, s.endpoint)
	}

	ready := *wp.ReadyReplicas
	if ready < 0 {
		ready = 0
	}
	return registry.Report{
		WarmCapacity: ready,
		Attributes: map[string]string{
			// Observed, not authoritative: recorded in status for humans, never
			// used for placement. See SandboxProviderSpec.Attributes.
			"warmPoolName": wp.Name,
			"template":     wp.Template,
		},
	}, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
