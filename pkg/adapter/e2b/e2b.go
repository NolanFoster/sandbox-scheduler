// Package e2b adapts the E2B hosted sandbox API (https://e2b.dev) to the
// sandbox-scheduler provider contract.
//
// E2B is an elastic, non-Kubernetes provider: sandboxes are created on demand
// via `POST /v2/sandboxes`, queried by metadata via `GET /v2/sandboxes?metadata=...`,
// extended via `POST /sandboxes/{sandboxID}/timeout`, and terminated via
// `DELETE /sandboxes/{sandboxID}`.
package e2b

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
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

// DefaultEndpoint is the public E2B control-plane API base URL from E2B's
// OpenAPI specification (https://github.com/e2b-dev/E2B/blob/main/spec/openapi.yml).
const DefaultEndpoint = "https://api.e2b.app"

// DefaultTemplate is the default E2B sandbox template when none is mapped.
const DefaultTemplate = "base"

// Credential keys checked in the referenced Secret.
const (
	CredentialKeyToken  = "token"
	CredentialKeyAPIKey = "apiKey"
	CredentialKeyTeamID = "teamID"

	AuthModeAPIKey = "x-api-key"
	AuthModeBearer = "bearer"
)

// Metadata keys stamped on E2B sandboxes.
const (
	MetaOwner          = "placement.agents.x-k8s.io/owner"
	MetaKey            = "placement.agents.x-k8s.io/key"
	MetaSpecHash       = "placement.agents.x-k8s.io/spec-hash"
	MetaExpiresAt      = "placement.agents.x-k8s.io/expires-at"
	MetaCreateDeadline = "placement.agents.x-k8s.io/create-deadline"
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
		teamID := strings.TrimSpace(cfg.Options[CredentialKeyTeamID])
		if teamID == "" {
			teamID = strings.TrimSpace(cfg.Credential(CredentialKeyTeamID))
		}
		authMode := strings.ToLower(strings.TrimSpace(cfg.Options["authMode"]))
		switch authMode {
		case "", AuthModeAPIKey:
			authMode = AuthModeAPIKey
		case AuthModeBearer:
			if teamID == "" {
				return provider.Provider{}, fmt.Errorf("%s: e2b bearer authMode requires %q in options or credentials",
					cfg.ProviderID, CredentialKeyTeamID)
			}
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
		}

		c := &Client{
			id:              cfg.ProviderID,
			endpoint:        endpoint,
			token:           token,
			teamID:          teamID,
			authMode:        authMode,
			templates:       templates,
			defaultTemplate: defaultTpl,
			client:          httpClient,
		}
		return provider.Provider{
			Source: c,
			Binder: c,
		}, nil
	})
}

// Client implements provider.Source and provider.Binder against an
// E2B-compatible HTTP API. E2B enforces sandbox TTL natively via `timeout` /
// `endAt`, so Provider.Lister is left nil; E2B's `/v2/sandboxes` endpoint
// filters metadata by exact key/value equality rather than multi-key `IN` sets,
// so Provider.BatchLookup is also left nil.
type Client struct {
	id              string
	endpoint        string
	token           string
	teamID          string
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
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
	}
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

type sandboxTimeoutRequest struct {
	Timeout int64 `json:"timeout"`
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
//
// Because E2B's `POST /v2/sandboxes` endpoint has no server-side HTTP request
// timeout header, Claim bounds unconfirmed creates using E2B's server-side
// sandbox `timeout` field: when `ctx` carries a deadline, the sandbox is
// initially created with `timeout` equal to the remaining attempt budget (and
// `MetaCreateDeadline` stamped in metadata), then immediately extended to the
// remaining duration until `expiresAt` via `POST /sandboxes/{sandboxID}/timeout`
// once the create response arrives before the deadline. If the leader loses its
// lease mid-create, any unconfirmed sandbox either expires on E2B within the
// attempt timeout window or (if E2B booted it after `MetaCreateDeadline`) is
// rejected and deleted by Lookup.
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

	ttlSec := int64((24 * time.Hour) / time.Second)
	if req.TTL > 0 {
		ttlSec = int64(math.Ceil(req.TTL.Seconds()))
		if ttlSec < 1 {
			ttlSec = 1
		}
	}
	now := time.Now().UTC()
	expiresAt := now.Add(time.Duration(ttlSec) * time.Second)

	initialTimeoutSec := ttlSec
	var createDeadline time.Time
	if deadline, ok := ctx.Deadline(); ok {
		rem := deadline.Sub(now)
		if rem <= 0 {
			return provider.Handle{}, context.DeadlineExceeded
		}
		sec := int64(math.Ceil(rem.Seconds()))
		if sec < 1 {
			sec = 1
		}
		if sec < initialTimeoutSec {
			initialTimeoutSec = sec
		}
		createDeadline = deadline.UTC()
	}

	meta := make(map[string]string, len(req.Labels)+5)
	for k, v := range req.Labels {
		meta[k] = v
	}
	meta[MetaOwner] = req.Owner
	meta[MetaKey] = req.Key
	meta[MetaSpecHash] = req.SpecHash
	meta[MetaExpiresAt] = expiresAt.Format(time.RFC3339)
	if !createDeadline.IsZero() {
		meta[MetaCreateDeadline] = createDeadline.Format(time.RFC3339Nano)
	}

	payload := createSandboxRequest{
		TemplateID: templateID,
		Timeout:    initialTimeoutSec,
		Metadata:   meta,
	}
	rawBody, err := json.Marshal(payload)
	if err != nil {
		return provider.Handle{}, provider.Rejected(err)
	}

	httpReq, err := c.newRequest(ctx, http.MethodPost, c.endpoint+"/v2/sandboxes", bytes.NewReader(rawBody))
	if err != nil {
		return provider.Handle{}, provider.Rejected(err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

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
		if len(sb.Metadata) == 0 {
			sb.Metadata = meta
		}
		if !createDeadline.IsZero() && time.Now().UTC().After(createDeadline) {
			c.deleteSandboxAsync(sb.SandboxID)
			return provider.Handle{}, context.DeadlineExceeded
		}
		if initialTimeoutSec < ttlSec && sb.SandboxID != "" {
			remSec := int64(math.Ceil(time.Until(expiresAt).Seconds()))
			if remSec <= 0 {
				c.deleteSandboxAsync(sb.SandboxID)
				return provider.Handle{}, context.DeadlineExceeded
			}
			if err := c.setSandboxTimeout(ctx, sb.SandboxID, remSec); err != nil {
				c.deleteSandboxAsync(sb.SandboxID)
				return provider.Handle{}, err
			}
			sb.EndAt = expiresAt.Format(time.RFC3339)
		}
		return c.inspectSandbox(ctx, sb, req)
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

func (c *Client) setSandboxTimeout(ctx context.Context, sandboxID string, timeoutSec int64) error {
	if timeoutSec < 1 {
		timeoutSec = 1
	}
	rawBody, err := json.Marshal(sandboxTimeoutRequest{Timeout: timeoutSec})
	if err != nil {
		return provider.Rejected(err)
	}
	u := c.endpoint + "/sandboxes/" + url.PathEscape(sandboxID) + "/timeout"
	httpReq, err := c.newRequest(ctx, http.MethodPost, u, bytes.NewReader(rawBody))
	if err != nil {
		return provider.Rejected(err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("%s: setting e2b sandbox timeout: %w", c.id, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("%s: reading e2b sandbox timeout response: %w", c.id, err)
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	return c.classifyHTTPError(resp, body)
}

func (c *Client) deleteSandboxAsync(sandboxID string) {
	if sandboxID == "" {
		return
	}
	go func() {
		cleanCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = c.Release(cleanCtx, provider.Handle{Provider: c.id, ID: sandboxID})
	}()
}

// MaxPageSize is the maximum page size accepted by E2B's GET /v2/sandboxes
// endpoint.
const MaxPageSize = 100

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
	q.Set("limit", "10")

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
		h, err := c.inspectSandbox(ctx, sb, req)
		if errors.Is(err, provider.ErrNotFound) {
			continue
		}
		return h, err
	}
	return provider.Handle{}, provider.ErrNotFound
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
	if c.teamID != "" {
		req.Header.Set("X-Team-ID", c.teamID)
	}
	return req, nil
}

func (c *Client) getSandboxDetail(ctx context.Context, sandboxID string) (sandboxObject, error) {
	u := c.endpoint + "/sandboxes/" + url.PathEscape(sandboxID)
	httpReq, err := c.newRequest(ctx, http.MethodGet, u, nil)
	if err != nil {
		return sandboxObject{}, provider.Rejected(err)
	}
	resp, err := c.client.Do(httpReq)
	if err != nil {
		return sandboxObject{}, fmt.Errorf("%s: e2b get sandbox detail failed: %w", c.id, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return sandboxObject{}, fmt.Errorf("%s: reading e2b sandbox detail response: %w", c.id, err)
	}
	if resp.StatusCode == http.StatusNotFound {
		return sandboxObject{}, provider.ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return sandboxObject{}, c.classifyHTTPError(resp, body)
	}
	var detail sandboxObject
	if err := json.Unmarshal(body, &detail); err != nil {
		return sandboxObject{}, fmt.Errorf("%s: unparseable e2b sandbox detail response: %w", c.id, err)
	}
	return detail, nil
}

func (c *Client) inspectSandbox(ctx context.Context, sb sandboxObject, req provider.ClaimRequest) (provider.Handle, error) {
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

	// If this sandbox was created with a short initial create timeout (before
	// POST /sandboxes/{id}/timeout promoted its TTL to MetaExpiresAt), verify
	// that E2B started the sandbox within MetaCreateDeadline. If E2B finished
	// starting it after MetaCreateDeadline (an orphaned slow create after
	// attempt timeout / lease loss), delete it and treat as not found;
	// otherwise promote its timeout to the remaining TTL.
	if rawDeadline := sb.Metadata[MetaCreateDeadline]; rawDeadline != "" {
		createDeadline, dErr := time.Parse(time.RFC3339Nano, rawDeadline)
		targetExpires, eErr := time.Parse(time.RFC3339, sb.Metadata[MetaExpiresAt])
		currentEnd, cErr := time.Parse(time.RFC3339, sb.EndAt)
		if dErr == nil && eErr == nil && cErr == nil && targetExpires.Sub(currentEnd) > 2*time.Second {
			if startedAt, sErr := time.Parse(time.RFC3339Nano, sb.StartedAt); sErr == nil {
				// Truncate createDeadline to seconds if startedAt has second resolution.
				cmpDeadline := createDeadline
				if startedAt.Nanosecond() == 0 {
					cmpDeadline = createDeadline.Truncate(time.Second)
				}
				if startedAt.After(cmpDeadline) {
					_ = c.Release(ctx, provider.Handle{Provider: c.id, ID: sb.SandboxID})
					return provider.Handle{}, provider.ErrNotFound
				}
			}
			remSec := int64(math.Ceil(time.Until(targetExpires).Seconds()))
			if remSec <= 0 {
				return provider.Handle{}, provider.KeyBusy(500*time.Millisecond,
					fmt.Errorf("%s: e2b sandbox %q for key %q has expired and is awaiting cleanup", c.id, sb.SandboxID, req.Key))
			}
			if err := c.setSandboxTimeout(ctx, sb.SandboxID, remSec); err != nil {
				return provider.Handle{}, err
			}
			sb.EndAt = targetExpires.Format(time.RFC3339)
		}
	}

	// E2B's GET /v2/sandboxes list items (ListedSandbox) omit `domain` and
	// `envdAccessToken`; fetch GET /sandboxes/{sandboxID} (SandboxDetail) to
	// hydrate connection credentials when recovering via Lookup.
	if (sb.Domain == "" || sb.EnvdToken == "") && sb.SandboxID != "" {
		detail, err := c.getSandboxDetail(ctx, sb.SandboxID)
		if err != nil {
			return provider.Handle{}, err
		}
		if detail.Domain != "" {
			sb.Domain = detail.Domain
		}
		if detail.EnvdToken != "" {
			sb.EnvdToken = detail.EnvdToken
		}
		if detail.ClientID != "" {
			sb.ClientID = detail.ClientID
		}
		if detail.State != "" {
			sb.State = detail.State
		}
		if detail.Deleting {
			sb.Deleting = true
		}
		if detail.EndAt != "" {
			sb.EndAt = detail.EndAt
		}
		if sb.Deleting || strings.EqualFold(sb.State, "killing") {
			return provider.Handle{}, provider.KeyBusy(500*time.Millisecond,
				fmt.Errorf("%s: e2b sandbox %q for key %q is terminating", c.id, sb.SandboxID, req.Key))
		}
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
	case "sandbox_capacity_unavailable", "sandbox_no_compatible_node", "insufficient_capacity", "no_capacity", "quota_exceeded":
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
