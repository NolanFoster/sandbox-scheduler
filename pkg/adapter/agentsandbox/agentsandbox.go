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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/NolanFoster/sandbox-scheduler/pkg/adapter"
	"github.com/NolanFoster/sandbox-scheduler/pkg/provider"
	"github.com/NolanFoster/sandbox-scheduler/pkg/registry"
)

// AdapterName is the value used in SandboxProvider.spec.adapter.
const AdapterName = "agent-sandbox"

// CredentialKey is the Secret key holding the gateway bearer token.
const CredentialKey = "token"

// Metadata keys stamped on SandboxClaim objects via the gateway.
const (
	LabelOwner          = "placement.agents.x-k8s.io/owner"
	AnnotationKey       = "placement.agents.x-k8s.io/key"
	AnnotationSpecHash  = "placement.agents.x-k8s.io/spec-hash"
	AnnotationExpiresAt = "placement.agents.x-k8s.io/expires-at"
)

func init() {
	adapter.Register(AdapterName, func(cfg adapter.Config) (provider.Provider, error) {
		if cfg.Endpoint == "" {
			return provider.Provider{}, fmt.Errorf("adapter %s: endpoint is required", AdapterName)
		}
		endpoint := strings.TrimRight(cfg.Endpoint, "/")
		token := cfg.Credential(CredentialKey)
		templates := make(map[string]string, len(cfg.Templates))
		for k, v := range cfg.Templates {
			templates[k] = v
		}
		httpClient := &http.Client{Timeout: 30 * time.Second}
		return provider.Provider{
			Source: &Source{
				id:       cfg.ProviderID,
				endpoint: endpoint,
				token:    token,
				pool:     cfg.Options["warmPoolName"],
				client:   httpClient,
			},
			Binder: &Binder{
				id:        cfg.ProviderID,
				endpoint:  endpoint,
				token:     token,
				templates: templates,
				client:    httpClient,
			},
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
	u := s.endpoint + "/warmpool"
	if s.pool != "" {
		u += "?name=" + url.QueryEscape(s.pool)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
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

// Binder executes Claim, Lookup, and Release against an agent-sandbox gateway.
type Binder struct {
	id        string
	endpoint  string
	token     string
	templates map[string]string
	client    *http.Client
}

type gatewayClaimRequest struct {
	Name           string            `json:"name"`
	Key            string            `json:"key"`
	Owner          string            `json:"owner"`
	SpecHash       string            `json:"specHash"`
	Template       string            `json:"template,omitempty"`
	Labels         map[string]string `json:"labels,omitempty"`
	TTLSeconds     int64             `json:"ttlSeconds,omitempty"`
	ShutdownPolicy string            `json:"shutdownPolicy"`
}

type gatewayClaimObject struct {
	Metadata struct {
		Name              string            `json:"name"`
		UID               string            `json:"uid"`
		ResourceVersion   string            `json:"resourceVersion"`
		DeletionTimestamp string            `json:"deletionTimestamp"`
		Labels            map[string]string `json:"labels"`
		Annotations       map[string]string `json:"annotations"`
	} `json:"metadata"`
	Error        string `json:"error"`
	Reason       string `json:"reason"`
	RetryAfterMs int64  `json:"retryAfterMs"`
}

// SandboxName returns the deterministic DNS-1035 SandboxClaim name for (owner, key).
// Always includes owner in the digest so two scheduler installations sharing a
// gateway never collide on the same key.
func SandboxName(owner, key string) string {
	sum := sha256.Sum256([]byte(owner + "/" + key))
	return "sc-" + hex.EncodeToString(sum[:10])
}

func (b *Binder) resolveTemplate(logical string) string {
	if logical == "" {
		return ""
	}
	if mapped, ok := b.templates[logical]; ok && mapped != "" {
		return mapped
	}
	return logical
}

// Claim creates or replays an idempotent SandboxClaim via the gateway.
func (b *Binder) Claim(ctx context.Context, req provider.ClaimRequest) (provider.Handle, error) {
	name := SandboxName(req.Owner, req.Key)
	ttlSec := int64(req.TTL / time.Second)
	if ttlSec <= 0 {
		ttlSec = int64((24 * time.Hour) / time.Second)
	}
	payload := gatewayClaimRequest{
		Name:           name,
		Key:            req.Key,
		Owner:          req.Owner,
		SpecHash:       req.SpecHash,
		Template:       b.resolveTemplate(req.Template),
		Labels:         req.Labels,
		TTLSeconds:     ttlSec,
		ShutdownPolicy: "Delete",
	}
	rawBody, err := json.Marshal(payload)
	if err != nil {
		return provider.Handle{}, provider.Rejected(err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, b.endpoint+"/sandboxes", bytes.NewReader(rawBody))
	if err != nil {
		return provider.Handle{}, provider.Rejected(err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if b.token != "" {
		httpReq.Header.Set("Authorization", "Bearer "+b.token)
	}

	resp, err := b.client.Do(httpReq)
	if err != nil {
		return provider.Handle{}, fmt.Errorf("%s: claim request failed: %w", b.id, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return provider.Handle{}, fmt.Errorf("%s: reading claim response: %w", b.id, err)
	}

	if resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusOK {
		var obj gatewayClaimObject
		if err := json.Unmarshal(body, &obj); err != nil {
			return provider.Handle{}, fmt.Errorf("%s: unparseable claim response: %w", b.id, err)
		}
		return b.inspectClaimObject(obj, name, req)
	}

	if resp.StatusCode == http.StatusConflict {
		var obj gatewayClaimObject
		_ = json.Unmarshal(body, &obj)
		switch obj.Reason {
		case "NoCapacity":
			return provider.Handle{}, provider.NoCapacity(fmt.Errorf("%s: %s", b.id, nonEmpty(obj.Error, "no capacity")))
		case "KeyConflict", "SpecMismatch":
			return provider.Handle{}, provider.KeyConflict(fmt.Errorf("%s: %s", b.id, nonEmpty(obj.Error, "spec mismatch for key "+req.Key)))
		}
		// Fallback for a plain Kubernetes 409 AlreadyExists from an older gateway:
		// inspect the existing claim via Lookup to verify spec-hash or deleting state.
		return b.Lookup(ctx, req)
	}

	return provider.Handle{}, b.classifyHTTPError(resp, body)
}

// Lookup inspects a single SandboxClaim by deterministic name without listing.
func (b *Binder) Lookup(ctx context.Context, req provider.ClaimRequest) (provider.Handle, error) {
	name := SandboxName(req.Owner, req.Key)
	u := b.endpoint + "/sandboxes/" + url.PathEscape(name)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return provider.Handle{}, provider.Rejected(err)
	}
	if b.token != "" {
		httpReq.Header.Set("Authorization", "Bearer "+b.token)
	}

	resp, err := b.client.Do(httpReq)
	if err != nil {
		return provider.Handle{}, fmt.Errorf("%s: lookup failed: %w", b.id, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return provider.Handle{}, fmt.Errorf("%s: reading lookup response: %w", b.id, err)
	}

	if resp.StatusCode == http.StatusNotFound {
		return provider.Handle{}, provider.ErrNotFound
	}
	if resp.StatusCode == http.StatusOK {
		var obj gatewayClaimObject
		if err := json.Unmarshal(body, &obj); err != nil {
			return provider.Handle{}, fmt.Errorf("%s: unparseable lookup response: %w", b.id, err)
		}
		return b.inspectClaimObject(obj, name, req)
	}

	return provider.Handle{}, b.classifyHTTPError(resp, body)
}

// Release deletes a SandboxClaim by ID, honoring Handle.Version (UID) when set.
func (b *Binder) Release(ctx context.Context, h provider.Handle) error {
	if h.ID == "" {
		return provider.Rejected(errors.New("sandbox ID is required"))
	}
	u := b.endpoint + "/sandboxes/" + url.PathEscape(h.ID)
	if h.Version != "" {
		u += "?uid=" + url.QueryEscape(h.Version)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodDelete, u, nil)
	if err != nil {
		return provider.Rejected(err)
	}
	if b.token != "" {
		httpReq.Header.Set("Authorization", "Bearer "+b.token)
	}

	resp, err := b.client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("%s: release failed: %w", b.id, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("%s: reading release response: %w", b.id, err)
	}

	switch {
	case resp.StatusCode == http.StatusNotFound:
		return nil
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return nil
	case resp.StatusCode == http.StatusConflict:
		// Precondition (UID) failed: the original sandbox was already replaced;
		// treat as already gone so the replacement is untouched.
		return nil
	default:
		return b.classifyHTTPError(resp, body)
	}
}

func (b *Binder) inspectClaimObject(obj gatewayClaimObject, fallbackName string, req provider.ClaimRequest) (provider.Handle, error) {
	if obj.Metadata.DeletionTimestamp != "" {
		return provider.Handle{}, provider.KeyBusy(500*time.Millisecond,
			fmt.Errorf("%s: sandbox claim %q for key %q is terminating", b.id, fallbackName, req.Key))
	}
	if existingOwner := obj.Metadata.Labels[LabelOwner]; existingOwner != "" && req.Owner != "" && existingOwner != req.Owner {
		return provider.Handle{}, provider.KeyConflict(
			fmt.Errorf("%s: sandbox claim %q is owned by %q, not %q", b.id, fallbackName, existingOwner, req.Owner))
	}
	if existingKey := obj.Metadata.Annotations[AnnotationKey]; existingKey != "" && existingKey != req.Key {
		return provider.Handle{}, provider.KeyConflict(
			fmt.Errorf("%s: sandbox claim %q belongs to key %q, not %q", b.id, fallbackName, existingKey, req.Key))
	}
	if existingHash := obj.Metadata.Annotations[AnnotationSpecHash]; existingHash != "" && req.SpecHash != "" && existingHash != req.SpecHash {
		return provider.Handle{}, provider.KeyConflict(
			fmt.Errorf("%s: key %q already exists with spec hash %s (want %s)", b.id, req.Key, existingHash, req.SpecHash))
	}

	id := obj.Metadata.Name
	if id == "" {
		id = fallbackName
	}
	var expiresAt time.Time
	if rawExp := obj.Metadata.Annotations[AnnotationExpiresAt]; rawExp != "" {
		if t, err := time.Parse(time.RFC3339, rawExp); err == nil {
			if !t.After(time.Now()) {
				return provider.Handle{}, provider.KeyBusy(500*time.Millisecond,
					fmt.Errorf("%s: sandbox claim %q for key %q expired at %s", b.id, fallbackName, req.Key, rawExp))
			}
			expiresAt = t
		}
	}
	if expiresAt.IsZero() && req.TTL > 0 {
		expiresAt = time.Now().UTC().Add(req.TTL)
	}
	return provider.Handle{
		Provider:  b.id,
		ID:        id,
		Version:   obj.Metadata.UID,
		Endpoint:  b.endpoint,
		ExpiresAt: expiresAt,
	}, nil
}

func (b *Binder) classifyHTTPError(resp *http.Response, body []byte) error {
	var parsed gatewayClaimObject
	_ = json.Unmarshal(body, &parsed)
	msg := nonEmpty(parsed.Error, truncate(strings.TrimSpace(string(body)), 200))
	if msg == "" {
		msg = http.StatusText(resp.StatusCode)
	}

	retryAfter := parseRetryAfter(resp.Header.Get("Retry-After"), parsed.RetryAfterMs)

	switch resp.StatusCode {
	case http.StatusUnauthorized, http.StatusForbidden:
		return provider.Rejected(fmt.Errorf(
			"%s: gateway rejected the credential (HTTP %d) — check the Secret referenced by credentialsRef",
			b.id, resp.StatusCode))
	case http.StatusBadRequest, http.StatusUnprocessableEntity:
		return provider.Rejected(fmt.Errorf("%s: gateway rejected request (HTTP %d): %s", b.id, resp.StatusCode, msg))
	case http.StatusConflict:
		if parsed.Reason == "NoCapacity" {
			return provider.NoCapacity(fmt.Errorf("%s: %s", b.id, msg))
		}
		return provider.KeyConflict(fmt.Errorf("%s: %s", b.id, msg))
	case http.StatusTooManyRequests:
		if parsed.Reason == "KeyBusy" {
			return provider.KeyBusy(retryAfter, fmt.Errorf("%s: %s", b.id, msg))
		}
		return provider.Busy(retryAfter, fmt.Errorf("%s: %s", b.id, msg))
	case http.StatusBadGateway, http.StatusServiceUnavailable:
		if parsed.Reason == "Unavailable" {
			return provider.Unavailable(fmt.Errorf("%s: gateway unavailable (HTTP %d): %s", b.id, resp.StatusCode, msg))
		}
		return fmt.Errorf("%s: ambiguous gateway HTTP %d: %s", b.id, resp.StatusCode, msg)
	default:
		return fmt.Errorf("%s: gateway returned HTTP %d: %s", b.id, resp.StatusCode, msg)
	}
}

func parseRetryAfter(header string, bodyMs int64) time.Duration {
	if bodyMs > 0 {
		return time.Duration(bodyMs) * time.Millisecond
	}
	if header != "" {
		if secs, err := strconv.Atoi(strings.TrimSpace(header)); err == nil && secs > 0 {
			return time.Duration(secs) * time.Second
		}
	}
	return 500 * time.Millisecond
}

func nonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
