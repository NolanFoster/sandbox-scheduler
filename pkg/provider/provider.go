// Package provider is the contract between the scheduler and a provider
// adapter.
//
// It imports only the Go standard library on purpose. Out-of-tree adapters
// depend on this package directly, and keeping it a leaf prevents dependency
// bloat and import cycles between pkg/adapter, pkg/registry, and pkg/binder.
//
// Capabilities are explicit struct fields on Provider rather than optional
// interfaces discovered by type assertion: a decorator (metrics, tracing,
// retries) wrapping a Source would silently hide an asserted Binder.
//
// Experimental: this API may change incompatibly until a non-Kubernetes
// adapter has implemented it (see docs/design/pluggable-claims.md, phase 6).
package provider

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
)

// DefaultAllowedLabelDomain is the label domain permitted for caller-supplied
// sandbox labels when no override is configured. Matches upstream
// agent-sandbox's default allowlisted domain.
const DefaultAllowedLabelDomain = "sandbox.users.io"

// ErrNotFound is returned by Lookup when no sandbox exists for a key. It is
// the only Lookup result that permits a new claim elsewhere.
var ErrNotFound = errors.New("provider: no sandbox for key")

// Report is what a provider says about itself at a point in time.
//
// Deliberately narrow: capacity, whether capacity is elastic, and facts that
// are informational only. Self-reported attributes are never matched on by
// placement filters, because a provider must not be able to influence its own
// selection by asserting isolation properties it does not have.
type Report struct {
	// WarmCapacity is the number of pre-warmed sandboxes ready to claim.
	// The one self-reported value placement does trust — a provider
	// overstating it only hurts itself, by attracting claims it cannot serve.
	WarmCapacity int

	// Elastic reports that this provider provisions on demand without a fixed
	// warm pool (for example, a hosted microVM API). Elastic providers score
	// a configurable baseline on WarmCapacity and record cold placements so
	// load spreads across them.
	Elastic bool

	// Attributes are facts the provider reports about itself. Surfaced in
	// status for humans; never matched on by filters.
	Attributes map[string]string
}

// Source fetches a Report for one provider.
//
// Implementations are called off the decision path by pkg/registry, so a slow
// implementation costs freshness, never placement latency.
type Source interface {
	// ProviderID is the stable identifier recorded on placed sandboxes.
	ProviderID() string
	// Fetch returns current capacity. It must respect ctx cancellation; the
	// registry imposes its own timeout regardless.
	Fetch(ctx context.Context) (Report, error)
}

// Provider bundles the capabilities an adapter offers for one provider.
type Provider struct {
	// Source is required: every provider reports capacity (or elastic status).
	Source Source
	// Binder is optional; nil means a decide-only provider.
	Binder Binder
	// Lister is optional; nil means the provider relies on native TTL expiry.
	Lister OwnedLister
	// BatchLookup is optional; nil means retry Lookups are issued one key at a
	// time.
	BatchLookup BatchLookuper
}

// BatchLookuper resolves many keys in one provider call (for a hosted API,
// typically one owner-labelled list filtered by a set of keys). Optional; it
// lets the binder coalesce a burst of retry Lookups so one Lookup rate-limit
// token covers the batch. Results are per request, in order, with the same
// semantics as Binder.Lookup.
type BatchLookuper interface {
	LookupMany(ctx context.Context, reqs []ClaimRequest) []LookupResult
}

// LookupResult is one element of a BatchLookuper.LookupMany response.
type LookupResult struct {
	Handle Handle
	Err    error
}

// Binder creates, looks up, and releases sandboxes on a provider.
//
// Prerequisite: an implementation must be able to find a sandbox by key
// without listing every sandbox on the provider (via deterministic naming,
// provider metadata, or indexed tags). The retry Lookup rule depends on this
// bound.
type Binder interface {
	// Claim provisions a sandbox for req. It must be idempotent on req.Key:
	// repeating it with the same SpecHash returns the same sandbox; a
	// different SpecHash returns KindKeyConflict; a sandbox still being
	// deleted returns KindKeyBusy.
	Claim(ctx context.Context, req ClaimRequest) (Handle, error)

	// Lookup returns the sandbox previously claimed with req.Key, applying the
	// same replay rules as Claim: a different req.SpecHash is KindKeyConflict,
	// a sandbox still being deleted is KindKeyBusy, and one not yet bound is
	// waited for within ctx. ErrNotFound (matched with errors.Is) means the
	// provider definitively has none; anything else means it cannot tell.
	//
	// Lookup must be a consistent read: a stale "not found" could cause the
	// binder to create a second sandbox under the same key elsewhere.
	Lookup(ctx context.Context, req ClaimRequest) (Handle, error)

	// Release deletes a sandbox. It must work from Provider and ID alone when
	// Version is empty, honour Version as a deletion precondition when set,
	// and return nil when the sandbox is already gone.
	Release(ctx context.Context, h Handle) error
}

// OwnedLister lists sandboxes created by a given scheduler owner identity.
// Used by the reaper for providers without native TTL expiry.
type OwnedLister interface {
	// ListOwned is paged; pageToken "" starts, and a returned "" ends.
	ListOwned(ctx context.Context, owner, pageToken string, limit int) (hs []Handle, next string, err error)
}

// ClaimRequest is the validated input to Binder.Claim and Binder.Lookup.
type ClaimRequest struct {
	// Template is the provider-neutral template name.
	Template string
	// Key is the caller's idempotency key.
	Key string
	// SpecHash is set by the core: a deterministic hash of Template, Labels,
	// and the TTL bucket in whole seconds.
	SpecHash string
	// Owner identifies the scheduler instance that created the sandbox.
	Owner string
	// Labels are caller-supplied labels within the allowlisted label domain;
	// nil and empty are equivalent.
	Labels map[string]string
	// TTL is the bounded lifetime of the sandbox, set by the core before
	// Validate is called.
	TTL time.Duration
}

// Handle describes a claimed sandbox.
type Handle struct {
	Provider string
	ID       string
	// Version is an opaque precondition (such as a UID or resourceVersion)
	// populated by ListOwned and enforced by Release when non-empty, so a
	// background reap cannot delete a newly created replacement that reused
	// the same ID. Empty on direct client deletes.
	Version   string
	Endpoint  string
	Connect   map[string]string
	ExpiresAt time.Time
}

var (
	// keyPattern permits alphanumeric start/end with internal '.', '_', ':',
	// or '-' up to 128 characters.
	keyPattern = regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9._:-]{0,126}[A-Za-z0-9])?$`)

	// labelNamePattern matches a Kubernetes qualified name segment (1..63
	// chars, alphanumeric start/end, internal '-', '_', '.').
	labelNamePattern = regexp.MustCompile(`^[A-Za-z0-9](?:[-A-Za-z0-9_.]{0,61}[A-Za-z0-9])?$`)

	// labelValuePattern matches a Kubernetes label value (0..63 chars).
	labelValuePattern = regexp.MustCompile(`^(?:[A-Za-z0-9](?:[-A-Za-z0-9_.]{0,61}[A-Za-z0-9])?)?$`)
)

// Validate checks the request as the core hands it to an adapter: key syntax,
// label keys within DefaultAllowedLabelDomain, label values, and
// 0 < TTL <= maxTTL. The core applies the TTL default first; adapters may
// assume a validated request.
func (r ClaimRequest) Validate(maxTTL time.Duration) error {
	return r.ValidateWithDomain(maxTTL, DefaultAllowedLabelDomain)
}

// ValidateWithDomain is Validate with an explicit allowlisted label domain.
func (r ClaimRequest) ValidateWithDomain(maxTTL time.Duration, allowedDomain string) error {
	if r.Key == "" {
		return errors.New("claim key is required")
	}
	if len(r.Key) > 128 || !keyPattern.MatchString(r.Key) {
		return fmt.Errorf("invalid claim key %q: must be 1..128 characters of [A-Za-z0-9._:-] starting and ending with an alphanumeric character", r.Key)
	}
	if r.TTL <= 0 {
		return fmt.Errorf("ttl must be positive, got %v", r.TTL)
	}
	if maxTTL > 0 && r.TTL > maxTTL {
		return fmt.Errorf("ttl %v exceeds maximum %v", r.TTL, maxTTL)
	}
	if allowedDomain == "" {
		allowedDomain = DefaultAllowedLabelDomain
	}
	if len(r.Labels) > 64 {
		return fmt.Errorf("too many labels (%d > 64)", len(r.Labels))
	}
	for k, v := range r.Labels {
		if err := validateLabel(k, v, allowedDomain); err != nil {
			return err
		}
	}
	return nil
}

func validateLabel(key, val, allowedDomain string) error {
	prefix, name, ok := strings.Cut(key, "/")
	if !ok || prefix == "" || name == "" {
		return fmt.Errorf("label key %q must have domain prefix %q", key, allowedDomain)
	}
	if len(prefix) > 253 {
		return fmt.Errorf("label key %q prefix exceeds 253 characters", key)
	}
	if prefix != allowedDomain && !strings.HasSuffix(prefix, "."+allowedDomain) {
		return fmt.Errorf("label key %q is outside allowlisted domain %q", key, allowedDomain)
	}
	// Verify every DNS label in a subdomain prefix is non-empty and valid.
	for _, part := range strings.Split(prefix, ".") {
		if part == "" || len(part) > 63 || !labelNamePattern.MatchString(part) {
			return fmt.Errorf("label key %q has invalid domain segment %q", key, part)
		}
	}
	if len(name) > 63 || !labelNamePattern.MatchString(name) {
		return fmt.Errorf("label key %q has invalid name segment %q", key, name)
	}
	if len(val) > 63 || !labelValuePattern.MatchString(val) {
		return fmt.Errorf("label %q has invalid value %q", key, val)
	}
	return nil
}

// SpecHashOf computes the deterministic spec hash over template, caller
// labels, and TTL bucketed to whole seconds. Nil and empty label maps produce
// the same hash.
func SpecHashOf(template string, labels map[string]string, ttl time.Duration) string {
	h := sha256.New()
	writeBytes := func(b []byte) {
		_ = binary.Write(h, binary.BigEndian, uint64(len(b)))
		h.Write(b)
	}
	writeBytes([]byte(template))
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	_ = binary.Write(h, binary.BigEndian, uint64(len(keys)))
	for _, k := range keys {
		writeBytes([]byte(k))
		writeBytes([]byte(labels[k]))
	}
	ttlSeconds := int64(ttl.Round(time.Second) / time.Second)
	_ = binary.Write(h, binary.BigEndian, ttlSeconds)
	sum := h.Sum(nil)
	return hex.EncodeToString(sum[:16])
}

// OwnerID returns the deterministic, DNS-1035-safe owner label value for a
// scheduler installation identified by release fullname, namespace, and
// cluster ID.
func OwnerID(fullname, namespace, clusterID string) string {
	h := sha256.New()
	for _, s := range []string{fullname, namespace, clusterID} {
		_ = binary.Write(h, binary.BigEndian, uint64(len(s)))
		h.Write([]byte(s))
	}
	sum := h.Sum(nil)
	return "ss-" + hex.EncodeToString(sum[:8])
}

// Kind classifies a claim or lookup failure. The zero value is
// KindUnknownOutcome — the safe default that never spills to another provider
// and so never creates a duplicate sandbox.
type Kind int

const (
	// KindUnknownOutcome means the request may have taken effect on the
	// provider (timeout, connection reset, unclassified error, or panic).
	// The binder pins to this provider and never spills.
	KindUnknownOutcome Kind = iota
	// KindNoCapacity means the provider confirmed it cannot serve this claim
	// without creating anything. Spills to the next candidate.
	KindNoCapacity
	// KindUnavailable means the request failed provably before taking effect
	// (connection refused, DNS failure, TLS handshake error). Spills to the
	// next candidate and counts against claim health.
	KindUnavailable
	// KindRejected means a permanent configuration or credential error for
	// this provider (unknown template, 401/403). Spills to the next candidate
	// and surfaces on the provider's Claiming condition.
	KindRejected
	// KindBusy means the provider rate-limited us (HTTP 429) before creating
	// anything. Honours RetryAfter and spills to the next candidate.
	KindBusy
	// KindKeyConflict means the idempotency key already exists with a
	// different spec. Terminal: no retry and no spill.
	KindKeyConflict
	// KindKeyBusy means the key's previous sandbox is still being deleted.
	// Pins to this provider and retries after RetryAfter; never spills.
	KindKeyBusy
)

// String returns the snake_case metrics and wire label for k.
func (k Kind) String() string {
	switch k {
	case KindUnknownOutcome:
		return "unknown_outcome"
	case KindNoCapacity:
		return "no_capacity"
	case KindUnavailable:
		return "unavailable"
	case KindRejected:
		return "rejected"
	case KindBusy:
		return "busy"
	case KindKeyConflict:
		return "key_conflict"
	case KindKeyBusy:
		return "key_busy"
	default:
		return "unknown_outcome"
	}
}

// ClaimError pairs a Kind and optional RetryAfter delay with an underlying
// error. Adapters should construct values via NoCapacity, Unavailable,
// Rejected, Busy, KeyConflict, and KeyBusy rather than building struct
// literals directly.
type ClaimError struct {
	Kind       Kind
	RetryAfter time.Duration
	Err        error
}

func (e *ClaimError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Err == nil {
		return "provider claim error (" + e.Kind.String() + ")"
	}
	return e.Kind.String() + ": " + e.Err.Error()
}

func (e *ClaimError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// NoCapacity classifies err as KindNoCapacity.
func NoCapacity(err error) error {
	return &ClaimError{Kind: KindNoCapacity, Err: err}
}

// Unavailable classifies err as KindUnavailable.
func Unavailable(err error) error {
	return &ClaimError{Kind: KindUnavailable, Err: err}
}

// Rejected classifies err as KindRejected.
func Rejected(err error) error {
	return &ClaimError{Kind: KindRejected, Err: err}
}

// Busy classifies err as KindBusy with an optional retry delay.
func Busy(after time.Duration, err error) error {
	if after < 0 {
		after = 0
	}
	return &ClaimError{Kind: KindBusy, RetryAfter: after, Err: err}
}

// KeyConflict classifies err as KindKeyConflict.
func KeyConflict(err error) error {
	return &ClaimError{Kind: KindKeyConflict, Err: err}
}

// KeyBusy classifies err as KindKeyBusy with an optional retry delay.
func KeyBusy(after time.Duration, err error) error {
	if after < 0 {
		after = 0
	}
	return &ClaimError{Kind: KindKeyBusy, RetryAfter: after, Err: err}
}

// KindOf classifies a non-nil error. Anything unclassified — including a
// ClaimError value rather than pointer, or an out-of-range Kind — returns
// KindUnknownOutcome. Callers must check err == nil first: KindOf(nil) is not
// a success signal.
func KindOf(err error) Kind {
	if err == nil {
		return KindUnknownOutcome
	}
	var ce *ClaimError
	if !errors.As(err, &ce) || ce == nil {
		return KindUnknownOutcome
	}
	switch ce.Kind {
	case KindNoCapacity, KindUnavailable, KindRejected, KindBusy, KindKeyConflict, KindKeyBusy:
		return ce.Kind
	default:
		return KindUnknownOutcome
	}
}

// RetryAfterOf returns the RetryAfter duration carried by err, or 0 if none.
func RetryAfterOf(err error) time.Duration {
	if err == nil {
		return 0
	}
	var ce *ClaimError
	if !errors.As(err, &ce) || ce == nil || ce.RetryAfter <= 0 {
		return 0
	}
	return ce.RetryAfter
}

// KindFromProto maps a proto/adapter/v1alpha1 Kind enum value to Kind.
// KIND_UNSPECIFIED (0) and any unrecognised enum number from a newer sidecar
// map to KindUnknownOutcome, never to a spilling kind.
func KindFromProto(code int32) Kind {
	switch code {
	case 0, 1:
		return KindUnknownOutcome
	case 2:
		return KindNoCapacity
	case 3:
		return KindUnavailable
	case 4:
		return KindRejected
	case 5:
		return KindBusy
	case 6:
		return KindKeyConflict
	case 7:
		return KindKeyBusy
	default:
		return KindUnknownOutcome
	}
}

// ToProtoKind maps Kind to its proto/adapter/v1alpha1 Kind enum number.
func ToProtoKind(k Kind) int32 {
	switch k {
	case KindUnknownOutcome:
		return 1
	case KindNoCapacity:
		return 2
	case KindUnavailable:
		return 3
	case KindRejected:
		return 4
	case KindBusy:
		return 5
	case KindKeyConflict:
		return 6
	case KindKeyBusy:
		return 7
	default:
		return 1
	}
}
