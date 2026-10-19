// Package e2b adapts the E2B hosted sandbox API (https://e2b.dev) to the
// sandbox-scheduler provider contract.
//
// E2B is an elastic, non-Kubernetes provider: sandboxes are created on demand
// via `POST /sandboxes`, queried by metadata via `GET /sandboxes?metadata=...`,
// and terminated via `DELETE /sandboxes/{sandboxID}`. Because E2B's create
// endpoint is not natively idempotent on a caller key, Claim queries
// `GET /sandboxes` by `(owner, key)` metadata before creating a new sandbox so
// replays and spec mismatches never allocate duplicate sandboxes.
package e2b

import (
	"bytes"
	"context"
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
)

// AdapterName is the value used in SandboxProvider.spec.adapter.
const AdapterName = "e2b"

// DefaultEndpoint is the public E2B control-plane API base URL.
const DefaultEndpoint = "https://api.e2b.dev"

// DefaultTemplate is the default E2B sandbox template when none is mapped.
const DefaultTemplate = "base"

// Credential keys checked in the referenced Secret.
const (
	CredentialKeyToken  = "token"
	CredentialKeyAPIKey = "apiKey"

	AuthModeAPIKey = "x-api-key"
	AuthModeBearer = "bearer"
)

// Metadata keys stamped on E2B sandboxes.
const (
	MetaOwner     = "placement.agents.x-k8s.io/owner"
	MetaKey       = "placement.agents.x-k8s.io/key"
	MetaSpecHash  = "placement.agents.x-k8s.io/spec-hash"
	MetaExpiresAt = "placement.agents.x-k8s.io/expires-at"
)

func init() {
	adapter.Register(AdapterName, func(cfg adapter.Config) (provider.Provider, error) {
		endpoint := strings.TrimRight(cfg.Endpoint, "/")
		if endpoint == "" {
			endpoint = DefaultEndpoint
		}
		token := cfg.Credential(CredentialKeyToken)
		if token == "" {
			token = cfg.Credential(CredentialKeyAPIKey)
		}
		authMode := strings.ToLower(strings.TrimSpace(cfg.Options["authMode"]))
		switch authMode {
		case "", AuthModeAPIKey:
			authMode = AuthModeAPIKey
		case AuthModeBearer:
		default:
			return provider.Provider{}, fmt.Errorf("%s: invalid e2b authMode %q (want %q or %q)",
				cfg.ProviderID, cfg.Options["authMode"], AuthModeAPIKey, AuthModeBearer)
		}

		templates := make(map[string]string, len(cfg.Templates))
		for k, v := range cfg.Templates {
			if strings.TrimSpace(v) == "" {
				return provider.Provider{}, fmt.Errorf("%s: template %q has an empty e2b template reference", cfg.ProviderID, k)
			}
			templates[k] = v
		}
		defaultTpl := cfg.Options["defaultTemplate"]
		if defaultTpl == "" {
			defaultTpl = DefaultTemplate
		}

		// Dedicated HTTP/2-capable transport sized for concurrent claims (5.7).
		transport := &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			MaxIdleConns:          512,
			MaxIdleConnsPerHost:   512,
			MaxConnsPerHost:       512,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			ForceAttemptHTTP2:     true,
		}
		httpClient := &http.Client{
			Transport: transport,
			Timeout:   30 * time.Second,
		}

		c := &Client{
			id:              cfg.ProviderID,
			endpoint:        endpoint,
			token:           token,
			authMode:        authMode,
			templates:       templates,
			defaultTemplate: defaultTpl,
			client:          httpClient,
		}
		return provider.Provider{
			Source:      c,
			Binder:      c,
			BatchLookup: c,
		}, nil
	})
}

// Client implements provider.Source, provider.Binder, and
// provider.BatchLookuper against an E2B-compatible HTTP API. E2B enforces
// sandbox TTL natively via the create request's `timeout` field (`endAt`), so
// Provider.Lister is left nil.
type Client struct {
	id              string
	endpoint        string
	token           string
	authMode        string
	templates       map[string]string
	defaultTemplate string
	client          *http.Client
}

// ProviderID implements provider.Source.
func (c *Client) ProviderID() string { return c.id }

// Fetch implements provider.Source. It probes the E2B sandboxes endpoint to
// verify reachability and API key validity, reporting Elastic: true so the
// scheduler scores this provider using ElasticWarmScore.
func (c *Client) Fetch(ctx context.Context) (provider.Report, error) {
	req, err := c.newRequest(ctx, http.MethodGet, c.endpoint+"/v2/sandboxes?state=running&limit=1", nil)
	if err != nil {
		return provider.Report{}, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return provider.Report{}, fmt.Errorf("%s: polling e2b: %w", c.id, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))

	if resp.StatusCode != http.StatusOK {
		return provider.Report{}, fmt.Errorf("%s: e2b poll returned HTTP %d", c.id, resp.StatusCode)
	}
	return provider.Report{
		Elastic: true,
		Attributes: map[string]string{
			"adapter": AdapterName,
		},
	}, nil
}

type createSandboxRequest struct {
	TemplateID string            `json:"templateID"`
	Timeout    int64             `json:"timeout"`
	Metadata   map[string]string `json:"metadata"`
}

type sandboxObject struct {
	SandboxID  string            `json:"sandboxID"`
	TemplateID string            `json:"templateID,omitempty"`
	ClientID   string            `json:"clientID,omitempty"`
	StartedAt  string            `json:"startedAt,omitempty"`
	EndAt      string            `json:"endAt,omitempty"`
	State      string            `json:"state,omitempty"`
	Deleting   bool              `json:"deleting,omitempty"`
	Version    string            `json:"version,omitempty"`
	Domain     string            `json:"domain,omitempty"`
	EnvdToken  string            `json:"envdAccessToken,omitempty"`
	Metadata   map[string]string `json:"metadata,omitempty"`
	Error      string            `json:"error,omitempty"`
	ErrorCode  string            `json:"error_code,omitempty"`
	Message    string            `json:"message,omitempty"`
	Reason     string            `json:"reason,omitempty"`
}

func (c *Client) resolveTemplate(logical string) (string, error) {
	if mapped, ok := c.templates[logical]; ok && mapped != "" {
		return mapped, nil
	}
	if len(c.templates) > 0 && logical != "" {
		return "", provider.Rejected(fmt.Errorf("%s: unknown template %q", c.id, logical))
	}
	if logical != "" {
		return logical, nil
	}
	if mapped, ok := c.templates["default"]; ok && mapped != "" {
		return mapped, nil
	}
	if c.defaultTemplate != "" {
		return c.defaultTemplate, nil
	}
	return DefaultTemplate, nil
}

// Claim implements provider.Binder.
func (c *Client) Claim(ctx context.Context, req provider.ClaimRequest) (provider.Handle, error) {
	templateID, err := c.resolveTemplate(req.Template)
	if err != nil {
		return provider.Handle{}, err
	}

	// Check for an existing sandbox under (Owner, Key) first so replays and
	// spec mismatches never allocate a second sandbox on E2B.
	existing, err := c.Lookup(ctx, req)
	if err == nil {
		return existing, nil
	}
	if !errors.Is(err, provider.ErrNotFound) {
		return provider.Handle{}, err
	}

	ttlSec := int64(req.TTL / time.Second)
	if ttlSec <= 0 {
		ttlSec = int64((24 * time.Hour) / time.Second)
	}
	expiresAt := time.Now().UTC().Add(time.Duration(ttlSec) * time.Second).Format(time.RFC3339)

	meta := make(map[string]string, len(req.Labels)+4)
	for k, v := range req.Labels {
		meta[k] = v
	}
	meta[MetaOwner] = req.Owner
	meta[MetaKey] = req.Key
	meta[MetaSpecHash] = req.SpecHash
	meta[MetaExpiresAt] = expiresAt

	payload := createSandboxRequest{
		TemplateID: templateID,
		Timeout:    ttlSec,
		Metadata:   meta,
	}
	rawBody, err := json.Marshal(payload)
	if err != nil {
		return provider.Handle{}, provider.Rejected(err)
	}

	httpReq, err := c.newRequest(ctx, http.MethodPost, c.endpoint+"/sandboxes", bytes.NewReader(rawBody))
	if err != nil {
		return provider.Handle{}, provider.Rejected(err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if req.Owner != "" || req.Key != "" {
		httpReq.Header.Set("Idempotency-Key", req.Owner+"/"+req.Key)
	}

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return provider.Handle{}, fmt.Errorf("%s: e2b claim request failed: %w", c.id, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return provider.Handle{}, fmt.Errorf("%s: reading e2b claim response: %w", c.id, err)
	}

	if resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusOK {
		var sb sandboxObject
		if err := json.Unmarshal(body, &sb); err != nil {
			return provider.Handle{}, fmt.Errorf("%s: unparseable e2b claim response: %w", c.id, err)
		}
		return c.inspectSandbox(sb, req)
	}

	if resp.StatusCode == http.StatusConflict {
		var sb sandboxObject
		_ = json.Unmarshal(body, &sb)
		if isNoCapacityError(sb) {
			return provider.Handle{}, provider.NoCapacity(fmt.Errorf("%s: %s", c.id, nonEmpty(sb.Error, sb.Message, "no capacity")))
		}
		switch sb.Reason {
		case "KeyConflict", "SpecMismatch":
			return provider.Handle{}, provider.KeyConflict(fmt.Errorf("%s: %s", c.id, nonEmpty(sb.Error, sb.Message, "spec mismatch for key "+req.Key)))
		}
		return c.Lookup(ctx, req)
	}

	return provider.Handle{}, c.classifyHTTPError(resp, body)
}

// Lookup implements provider.Binder. It queries E2B by `(owner, key)` metadata
// in a single HTTP request so lookup cost never scales with total sandbox count.
func (c *Client) Lookup(ctx context.Context, req provider.ClaimRequest) (provider.Handle, error) {
	q := url.Values{}
	metaQuery := url.Values{}
	if req.Owner != "" {
		metaQuery.Set(MetaOwner, req.Owner)
	}
	metaQuery.Set(MetaKey, req.Key)
	q.Set("metadata", metaQuery.Encode())
	q.Set("limit", "1")

	u := c.endpoint + "/v2/sandboxes?" + q.Encode()
	httpReq, err := c.newRequest(ctx, http.MethodGet, u, nil)
	if err != nil {
		return provider.Handle{}, provider.Rejected(err)
	}

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return provider.Handle{}, fmt.Errorf("%s: e2b lookup failed: %w", c.id, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return provider.Handle{}, fmt.Errorf("%s: reading e2b lookup response: %w", c.id, err)
	}

	if resp.StatusCode == http.StatusNotFound {
		return provider.Handle{}, provider.ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return provider.Handle{}, c.classifyHTTPError(resp, body)
	}

	list, err := decodeSandboxList(body)
	if err != nil {
		return provider.Handle{}, fmt.Errorf("%s: unparseable e2b lookup response: %w", c.id, err)
	}
	for _, sb := range list {
		if sb.Metadata[MetaKey] != req.Key {
			continue
		}
		if req.Owner != "" && sb.Metadata[MetaOwner] != "" && sb.Metadata[MetaOwner] != req.Owner {
			continue
		}
		return c.inspectSandbox(sb, req)
	}
	return provider.Handle{}, provider.ErrNotFound
}

// LookupMany implements provider.BatchLookuper. It resolves a batch of keys for
// the same owner via `GET /v2/sandboxes`, following `X-Next-Token` pagination
// if the owner has more than MaxPageSize sandboxes and some requested keys have
// not yet been seen.
func (c *Client) LookupMany(ctx context.Context, reqs []provider.ClaimRequest) []provider.LookupResult {
	out := make([]provider.LookupResult, len(reqs))
	if len(reqs) == 0 {
		return out
	}

	owner := reqs[0].Owner
	keys := make([]string, 0, len(reqs))
	wanted := make(map[string]struct{}, len(reqs))
	for _, r := range reqs {
		keys = append(keys, r.Key)
		wanted[r.Key] = struct{}{}
	}

	byKey := make(map[string]sandboxObject, len(reqs))
	var nextToken string
	for page := 0; page < 100; page++ {
		q := url.Values{}
		if owner != "" {
			metaQuery := url.Values{}
			metaQuery.Set(MetaOwner, owner)
			q.Set("metadata", metaQuery.Encode())
		}
		q.Set("keys", strings.Join(keys, ","))
		q.Set("limit", strconv.Itoa(MaxPageSize))
		if nextToken != "" {
			q.Set("nextToken", nextToken)
		}

		body, headerNext, err := c.doGetSandboxes(ctx, "/v2/sandboxes?"+q.Encode())
		if err != nil {
			for i := range out {
				out[i] = provider.LookupResult{Err: err}
			}
			return out
		}

		list, tok, err := decodePagedSandboxList(body, headerNext)
		if err != nil {
			for i := range out {
				out[i] = provider.LookupResult{Err: fmt.Errorf("%s: unparseable e2b batch lookup: %w", c.id, err)}
			}
			return out
		}

		for _, sb := range list {
			k := sb.Metadata[MetaKey]
			if k == "" {
				continue
			}
			if owner != "" && sb.Metadata[MetaOwner] != "" && sb.Metadata[MetaOwner] != owner {
				continue
			}
			if _, ok := wanted[k]; ok {
				byKey[k] = sb
			}
		}

		if len(byKey) >= len(wanted) || tok == "" || tok == nextToken {
			break
		}
		nextToken = tok
	}

	for i, req := range reqs {
		sb, ok := byKey[req.Key]
		if !ok {
			out[i] = provider.LookupResult{Err: provider.ErrNotFound}
			continue
		}
		h, err := c.inspectSandbox(sb, req)
		out[i] = provider.LookupResult{Handle: h, Err: err}
	}
	return out
}

// MaxPageSize is the maximum page size accepted by E2B's GET /v2/sandboxes
// endpoint.
const MaxPageSize = 100

func (c *Client) doGetSandboxes(ctx context.Context, pathAndQuery string) ([]byte, string, error) {
	httpReq, err := c.newRequest(ctx, http.MethodGet, c.endpoint+pathAndQuery, nil)
	if err != nil {
		return nil, "", provider.Rejected(err)
	}

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return nil, "", fmt.Errorf("%s: e2b list sandboxes failed: %w", c.id, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, "", fmt.Errorf("%s: reading e2b list sandboxes response: %w", c.id, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, "", c.classifyHTTPError(resp, body)
	}

	headerNext := nonEmpty(resp.Header.Get("X-Next-Token"), resp.Header.Get("X-Next-Page-Token"))
	return body, headerNext, nil
}

// Release implements provider.Binder. Because E2B's `DELETE /sandboxes/{id}`
// endpoint does not accept a server-side version precondition (`If-Match` or
// `?version=`), `toHandle` leaves `Handle.Version` empty and `Release` rejects
// non-empty `h.Version` rather than performing an unguarded delete.
func (c *Client) Release(ctx context.Context, h provider.Handle) error {
	if h.ID == "" {
		return provider.Rejected(errors.New("sandbox ID is required"))
	}
	if h.Version != "" {
		return provider.Rejected(fmt.Errorf("%s: e2b delete does not support version preconditions", c.id))
	}
	u := c.endpoint + "/sandboxes/" + url.PathEscape(h.ID)
	httpReq, err := c.newRequest(ctx, http.MethodDelete, u, nil)
	if err != nil {
		return provider.Rejected(err)
	}

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("%s: e2b release failed: %w", c.id, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("%s: reading e2b release response: %w", c.id, err)
	}

	switch {
	case resp.StatusCode == http.StatusNotFound:
		return nil
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return nil
	default:
		return c.classifyHTTPError(resp, body)
	}
}

func (c *Client) newRequest(ctx context.Context, method, u string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return nil, err
	}
	if c.token != "" {
		if c.authMode == AuthModeBearer {
			req.Header.Set("Authorization", "Bearer "+c.token)
		} else {
			req.Header.Set("X-API-Key", c.token)
		}
	}
	if deadline, ok := ctx.Deadline(); ok {
		if rem := time.Until(deadline); rem > 0 {
			ms := rem.Milliseconds()
			if ms < 1 {
				ms = 1
			}
			req.Header.Set("X-Request-Timeout-Ms", strconv.FormatInt(ms, 10))
		}
	}
	return req, nil
}

func (c *Client) inspectSandbox(sb sandboxObject, req provider.ClaimRequest) (provider.Handle, error) {
	if sb.Deleting || strings.EqualFold(sb.State, "killing") {
		return provider.Handle{}, provider.KeyBusy(500*time.Millisecond,
			fmt.Errorf("%s: e2b sandbox %q for key %q is terminating", c.id, sb.SandboxID, req.Key))
	}
	if existingOwner := sb.Metadata[MetaOwner]; existingOwner != "" && req.Owner != "" && existingOwner != req.Owner {
		return provider.Handle{}, provider.KeyConflict(
			fmt.Errorf("%s: e2b sandbox %q is owned by %q, not %q", c.id, sb.SandboxID, existingOwner, req.Owner))
	}
	if existingKey := sb.Metadata[MetaKey]; existingKey != "" && req.Key != "" && existingKey != req.Key {
		return provider.Handle{}, provider.KeyConflict(
			fmt.Errorf("%s: e2b sandbox %q belongs to key %q, not %q", c.id, sb.SandboxID, existingKey, req.Key))
	}
	if existingHash := sb.Metadata[MetaSpecHash]; existingHash != "" && req.SpecHash != "" && existingHash != req.SpecHash {
		return provider.Handle{}, provider.KeyConflict(
			fmt.Errorf("%s: key %q already exists with spec hash %s (want %s)", c.id, req.Key, existingHash, req.SpecHash))
	}

	h := c.toHandle(sb)
	if !h.ExpiresAt.IsZero() && !time.Now().Before(h.ExpiresAt) {
		return provider.Handle{}, provider.KeyBusy(500*time.Millisecond,
			fmt.Errorf("%s: e2b sandbox %q for key %q has expired and is awaiting cleanup", c.id, h.ID, req.Key))
	}
	return h, nil
}

func (c *Client) toHandle(sb sandboxObject) provider.Handle {
	var expiresAt time.Time
	if raw := sb.EndAt; raw != "" {
		if t, err := time.Parse(time.RFC3339, raw); err == nil {
			expiresAt = t
		}
	}
	if expiresAt.IsZero() {
		if raw := sb.Metadata[MetaExpiresAt]; raw != "" {
			if t, err := time.Parse(time.RFC3339, raw); err == nil {
				expiresAt = t
			}
		}
	}
	endpoint := c.endpoint
	connect := map[string]string{
		"sandboxID": sb.SandboxID,
	}
	if sb.ClientID != "" {
		connect["clientID"] = sb.ClientID
	}
	if sb.Domain != "" {
		connect["domain"] = sb.Domain
	}
	if sb.EnvdToken != "" {
		connect["envdAccessToken"] = sb.EnvdToken
	}
	return provider.Handle{
		Provider:  c.id,
		ID:        sb.SandboxID,
		Endpoint:  endpoint,
		Connect:   connect,
		ExpiresAt: expiresAt,
	}
}

func (c *Client) classifyHTTPError(resp *http.Response, body []byte) error {
	var parsed sandboxObject
	_ = json.Unmarshal(body, &parsed)
	msg := nonEmpty(parsed.Error, parsed.Message, strings.TrimSpace(string(body)), http.StatusText(resp.StatusCode))
	baseErr := fmt.Errorf("%s: HTTP %d: %s", c.id, resp.StatusCode, msg)

	retryAfter := parseRetryAfter(resp.Header.Get("Retry-After"))

	switch resp.StatusCode {
	case http.StatusUnauthorized, http.StatusForbidden, http.StatusBadRequest, http.StatusUnprocessableEntity:
		return provider.Rejected(baseErr)
	case http.StatusConflict:
		if isNoCapacityError(parsed) {
			return provider.NoCapacity(baseErr)
		}
		if parsed.Reason == "KeyConflict" || parsed.Reason == "SpecMismatch" {
			return provider.KeyConflict(baseErr)
		}
		return baseErr
	case http.StatusTooManyRequests:
		if parsed.Reason == "KeyBusy" {
			return provider.KeyBusy(retryAfter, baseErr)
		}
		return provider.Busy(retryAfter, baseErr)
	case http.StatusServiceUnavailable, http.StatusBadGateway:
		if isNoCapacityError(parsed) {
			return provider.NoCapacity(baseErr)
		}
		if isUnavailableError(parsed) {
			return provider.Unavailable(baseErr)
		}
		return baseErr
	default:
		return baseErr
	}
}

func isNoCapacityError(sb sandboxObject) bool {
	if sb.Reason == "NoCapacity" {
		return true
	}
	switch strings.ToLower(strings.TrimSpace(sb.ErrorCode)) {
	case "sandbox_capacity_unavailable", "insufficient_capacity", "no_capacity", "quota_exceeded":
		return true
	default:
		return false
	}
}

func isUnavailableError(sb sandboxObject) bool {
	if sb.Reason == "Unavailable" {
		return true
	}
	switch strings.ToLower(strings.TrimSpace(sb.ErrorCode)) {
	case "service_unavailable", "unavailable":
		return true
	default:
		return false
	}
}

func decodeSandboxList(body []byte) ([]sandboxObject, error) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return nil, nil
	}
	if trimmed[0] == '[' {
		var list []sandboxObject
		if err := json.Unmarshal(trimmed, &list); err != nil {
			return nil, err
		}
		return list, nil
	}
	var wrapper struct {
		Sandboxes []sandboxObject `json:"sandboxes"`
		Items     []sandboxObject `json:"items"`
	}
	if err := json.Unmarshal(trimmed, &wrapper); err != nil {
		return nil, err
	}
	if len(wrapper.Sandboxes) > 0 {
		return wrapper.Sandboxes, nil
	}
	return wrapper.Items, nil
}

func decodePagedSandboxList(body []byte, headerNext string) ([]sandboxObject, string, error) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return nil, headerNext, nil
	}
	if trimmed[0] == '[' {
		var list []sandboxObject
		if err := json.Unmarshal(trimmed, &list); err != nil {
			return nil, "", err
		}
		return list, headerNext, nil
	}
	var wrapper struct {
		Sandboxes     []sandboxObject `json:"sandboxes"`
		Items         []sandboxObject `json:"items"`
		NextToken     string          `json:"nextToken"`
		NextPageToken string          `json:"nextPageToken"`
	}
	if err := json.Unmarshal(trimmed, &wrapper); err != nil {
		return nil, "", err
	}
	next := nonEmpty(wrapper.NextToken, wrapper.NextPageToken, headerNext)
	if len(wrapper.Sandboxes) > 0 {
		return wrapper.Sandboxes, next, nil
	}
	return wrapper.Items, next, nil
}

func parseRetryAfter(raw string) time.Duration {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 500 * time.Millisecond
	}
	if secs, err := strconv.Atoi(raw); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	return 500 * time.Millisecond
}

func nonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
