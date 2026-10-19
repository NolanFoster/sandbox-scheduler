# Pluggable claims: placing *and* provisioning sandboxes beyond Kubernetes

Status: **v7, Phases 0–7 implemented** after six review rounds (OSS
compatibility, scalability, Kubernetes practices, Go practices, and automated
PR review). Section 12 records what changed in each revision and why.

## 1. Summary

Today the scheduler answers "where should this sandbox go?" (`POST /schedule`)
and the caller creates the sandbox. This plan adds an optional second step: the
scheduler makes the claim on the chosen provider and, if that provider refuses,
tries the next one, through a pluggable interface that a Kubernetes cluster and
a hosted API (E2B, Daytona, Modal, Fly) can both implement.

Decide-only stays the default and stays unchanged. Claiming is opt-in per
deployment (`--enable-claims`) and per provider (`spec.claims.enabled`).

## 2. Goals and non-goals

Goals

1. One provider-neutral API to obtain a sandbox from any configured provider.
2. Spill to the next-best provider when a claim is refused, with no duplicate
   sandboxes, including across scheduler failover and client retries.
3. Providers plug in through a small interface, defined once as protobuf and
   implemented in-process (Go) or out-of-process (gRPC sidecar).
4. Keep today's properties: decisions are in-memory and sub-millisecond; the
   scheduler stores no per-sandbox state beyond claims currently in flight;
   self-reported provider facts never drive placement.

Non-goals

- Data plane (exec, files, ports). Handles pass provider-native connection
  details through untouched.
- Suspend/resume. On agent-sandbox it is `Sandbox.spec.operatingMode` in the
  owning cluster and needs no placement. On Substrate, resume *is* a scheduling
  decision (worker selection inside its control plane); resuming an actor in a
  different cluster is out of scope here, not unnecessary.
- Lifecycle tracking or a placement database.
- The marginal-cost scorer (sunk-cost capacity vs per-second billing). Separate
  plan; no billing field is added until it lands.

## 3. Current state (main @ 21db540)

- `pkg/adapter`: name-keyed registry; `Factory func(Config) (provider.Provider,
  error)`. One adapter, `agent-sandbox`, reading `GET <endpoint>/warmpool` from
  an **operator-owned gateway** (not an upstream component).
- `pkg/registry`: polling off the decision path; lock-free `Snapshot()`;
  atomic `TryReserveWarm`; `ReleaseReservation`; `RecordColdPlacement` for
  power-of-two-choices; per-provider ledgers published copy-on-write via
  `atomic.Pointer`.
- `pkg/scheduler`: `POST /schedule`, `GET /providers`, and versioned
  `/api/v1alpha1/{schedule,providers,sandboxes}`; bearer token (`Token` and
  optional separate `ClaimToken`); leader-only serving via pod label.
- API group `placement.agents.x-k8s.io/v1alpha1` (see 10.1).
- Module pins k8s.io/* v0.36 and controller-runtime v0.24.1; upstream
  `sigs.k8s.io/agent-sandbox` **v1.0.7** (the release this plan targets)
  requires k8s.io v0.37.1 and controller-runtime v0.25.1.

**Existing issue found in review, fixed in phase 0:** the chart's ClusterRole
previously granted cluster-wide `get/list/watch` on Secrets. Phase 0 replaces
that cluster-wide rule with an unconditional namespaced `Role` in the release
namespace paired with the manager's `cache.Options.ByObject` namespace
restriction.

## 4. Ecosystem alignment

| Upstream work | Relationship |
|---|---|
| agent-sandbox v1beta1 (`SandboxClaim`, `SandboxWarmPool`) | The `agent-sandbox` binder creates `SandboxClaim`s with `warmPoolRef`; reuses the Go SDK's deterministic `ClaimName` + `AdoptExisting` convention. |
| KEP-1434 multi-cluster fleet (implementable) | 1434 decides **which cluster holds which warm pools** (minutes); this project decides **which provider serves each request** (milliseconds), across clusters *and* hosted APIs. A `fleet` capacity adapter reading 1434 capacity reports becomes the **recommended** agent-sandbox capacity source; it ships as its own module/sidecar, since it needs object-store credentials and a GCS/S3 client. Positioned as the cross-provider router above 1434, not an alternative. |
| SIG-Multicluster ClusterProfile (KEP-4322) | Preferred source of cluster access for the kubeapi transport (5.11). Raw kubeconfig Secrets are a fallback. |
| Substrate | Named as a target provider (capacity = idle-worker headroom from `ate-api-server`; claim = create actor). Its out-of-process `objectstore.v1` sidecar is precedent for 5.10. |

The custom gateway `POST /claims` contract from v1 is dropped; operators with a
gateway implement the gRPC adapter protocol (5.10) instead.

## 5. Design

### 5.1 Adapter contract (`pkg/provider`, new leaf package)

A new package with **standard-library imports only**, so out-of-tree adapters
depend on a small surface and later changes cannot create import cycles
(`pkg/adapter` imports `pkg/registry` today). `pkg/registry` keeps
`type Source = provider.Source` and `type Report = provider.Report` for
compatibility.

Capabilities are explicit fields, not discovered by type assertion: a decorator
(metrics, retries) wrapping a `Source` would silently hide an asserted
`Binder`.

```go
// Package provider is the contract between the scheduler and a provider
// adapter.
//
// Experimental: this API may change incompatibly until a non-Kubernetes
// adapter has implemented it (see docs/design/pluggable-claims.md, phase 6).
package provider

// ErrNotFound is returned by Lookup when no sandbox exists for a key. It is
// the only Lookup result that permits a new claim elsewhere (5.6).
var ErrNotFound = errors.New("provider: no sandbox for key")

type Provider struct {
    Source      Source        // required: capacity
    Binder      Binder        // nil: decide-only provider
    Lister      OwnedLister   // nil: relies on provider-native expiry
    BatchLookup BatchLookuper // nil: Lookups are issued one key at a time
}

// BatchLookuper resolves many keys in one provider call (for a hosted API,
// typically one owner-labelled list filtered by a set of keys). Optional; it
// lets the core coalesce a burst of retry Lookups (5.6). Results are per
// request, in order, with the same semantics as Binder.Lookup.
type BatchLookuper interface {
    LookupMany(ctx context.Context, reqs []ClaimRequest) []LookupResult
}

type LookupResult struct {
    Handle Handle
    Err    error
}

type Binder interface {
    // Claim must be idempotent on req.Key: repeating it returns the same
    // sandbox. Implementations that cannot guarantee this must not be Binders.
    Claim(ctx context.Context, req ClaimRequest) (Handle, error)
    // Lookup returns the sandbox previously claimed with req.Key, applying the
    // same replay rules as Claim (5.11): a different req.SpecHash is
    // KeyConflict, a sandbox still being deleted is KeyBusy, one not yet
    // bound is waited for. ErrNotFound (matched with errors.Is) means the
    // provider definitively has none; anything else means it cannot tell.
    // It is a consistent read: a cached "not found" could create a duplicate.
    // Waiting for binding stays within ctx: a slow bind returns an
    // inconclusive error at the deadline rather than blocking the probe.
    Lookup(ctx context.Context, req ClaimRequest) (Handle, error)
    // Release must work from Provider and ID alone when Version is empty,
    // honour Version as a deletion precondition when set, and return nil when
    // the sandbox is already gone.
    Release(ctx context.Context, h Handle) error
}

type OwnedLister interface {
    // ListOwned is paged; pageToken "" starts, a returned "" ends.
    ListOwned(ctx context.Context, owner, pageToken string, limit int) (hs []Handle, next string, err error)
}

type ClaimRequest struct {
    Template string            // provider-neutral name (5.3)
    Key      string            // idempotency key
    SpecHash string            // set by the core: hash of template, labels, TTL bucket
    Owner    string            // scheduler instance identity (5.9)
    Labels   map[string]string // caller labels; nil and empty are equivalent
    TTL      time.Duration     // set by the core before Validate (5.9)
}

// Validate checks the request as the core hands it to an adapter: key regex,
// label domains, and 0 < TTL <= maxTTL. The core applies the TTL default
// first; adapters may assume a validated request.
func (r ClaimRequest) Validate(maxTTL time.Duration) error

type Handle struct {
    Provider  string
    ID        string
    // Version is an opaque precondition (such as a UID or resourceVersion)
    // populated by ListOwned and enforced by Release when non-empty, so a
    // background reap cannot delete a newly created replacement that reused
    // the same ID. Empty on direct client deletes.
    Version   string
    Endpoint  string
    Connect   map[string]string // provider-native, documented per adapter
    ExpiresAt time.Time         // zero = unknown
}
```

**Prerequisite for implementing `Binder`:** the adapter must be able to find a
sandbox by key without listing everything (a deterministic name, provider
metadata or tags). The Lookup rule (5.6) depends on it; `adaptertest` checks it
by asserting `Lookup` issues a bounded number of provider calls regardless of
how many sandboxes exist.

`Factory` becomes `func(Config) (provider.Provider, error)`, changed now while
one in-tree adapter exists. `pkg/binder` resolves a provider's `Provider`,
limiter and settings through a copy-on-write `atomic.Pointer[map[string]*target]`
published on every add/remove/config change, the same pattern as the existing
ledgers, so the request path takes no lock. No JSON tags on these types; wire
types live in `pkg/scheduler`. The same messages are defined in
`proto/adapter/v1alpha1` from phase 1.

### 5.2 Error taxonomy

```go
type Kind int

const (
    KindUnknownOutcome Kind = iota // zero value: the safe default
    KindNoCapacity
    KindUnavailable
    KindRejected
    KindBusy        // provider rate-limited us; honour RetryAfter; spills
    KindKeyConflict // key already used with a different spec; terminal
    KindKeyBusy     // key's previous sandbox still being deleted; pins
)

type ClaimError struct {
    Kind       Kind
    RetryAfter time.Duration
    Err        error
}

func (e *ClaimError) Error() string
func (e *ClaimError) Unwrap() error

func NoCapacity(err error) error  // constructors so authors never hand-build
func Unavailable(err error) error
func Rejected(err error) error
func Busy(after time.Duration, err error) error
func KeyConflict(err error) error
func KeyBusy(after time.Duration, err error) error

// KindOf classifies a non-nil error; anything unclassified, including a
// ClaimError value rather than pointer, is KindUnknownOutcome. Callers check
// err == nil first: KindOf(nil) is not a success signal.
func KindOf(err error) Kind
func (k Kind) String() string // metrics label
```

Unclassified errors, a missing `Kind`, and panics in `Claim` are all
`UnknownOutcome`, the one kind that never spills and so never duplicates.

| Kind | Meaning | Binder loop | Final HTTP |
|---|---|---|---|
| `UnknownOutcome` | Sent, outcome unknown; or unclassified | Pin to this provider: `Lookup`, then retry same key within budget; never spill | 504 `{providers}` |
| `KeyConflict` | Same key exists with a different spec | Stop: no retry, no spill (either would create a second sandbox under the key) | 409 `SpecMismatch` |
| `NoCapacity` | Provider confirmed it cannot serve | Mark `(provider, template)` refused **globally** for `--refusal-ttl` (1s); next candidate | 409 with reasons |
| `Busy` | Provider rate limit (e.g. 429) | Cooldown for `RetryAfter`; next candidate | 429 + `Retry-After` |
| `KeyBusy` | The key's previous sandbox is still being deleted | Pin: retry this provider after `RetryAfter` within the deadline; never spill (the key's sandbox still exists here) | 429 + `Retry-After`, `providers` set |
| `Unavailable` | Failed provably before taking effect | Release reservation; health signal (5.8); next candidate | 503 |
| `Rejected` | Permanent for this provider (unknown template, auth) | Release reservation; config-error metric + `Claiming` condition; next candidate | 422 if all rejected |

agent-sandbox never refuses for capacity: an empty warm pool cold-starts the
claim (`sandboxclaim_controller.go`, "Cold path"). For that adapter, warm vs
cold is decided by the existing reservation before the claim, and a cold start
is a success. Requests may set `"warmOnly": true`, in which case a provider
whose reservation fails is skipped without a claim. Cold claims on clusters are
rate-bounded too (`spec.claims.maxRate`, 5.4), since a burst of cold claims
would otherwise queue far behind a pool's refill rate.

### 5.3 Templates by reference

```go
// +listType=map
// +listMapKey=name
// +kubebuilder:validation:MaxItems=256
Templates []TemplateMapping `json:"templates,omitempty"`

type TemplateMapping struct {
    // +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
    // +kubebuilder:validation:MaxLength=63
    Name string `json:"name"`
    // Provider-native reference: a SandboxWarmPool name for agent-sandbox
    // (cold-only use is a pool with replicas: 0), a template ID elsewhere.
    // +kubebuilder:validation:MinLength=1
    // +kubebuilder:validation:MaxLength=253
    Ref string `json:"ref"`
}
```

- Mappings stay on the provider (they are inherently per provider; a separate
  CRD adds a cross-object reference for little gain).
- New filter `TemplateAvailable`, added to the profile only when a template is
  requested, so `/schedule` is unchanged.

### 5.4 Rate bounds and elastic providers

`provider.Report` gains `Elastic bool`. Elastic providers:

- score `spec.claims.elasticWarmScore` (default **20 of 100**), not saturation:
  at saturation an elastic provider (5×100+3×0+3×100 = 800) beat every cold
  cluster (≤ 600) at any price and absorbed all overflow at per-use rates; at
  20 it scores 400 + 3×cost and loses to a cold cluster priced below about two
  thirds of the most expensive provider;
- record cold placements (they are cold starts), so spreading between elastic
  providers works.

`spec.claims.maxRate` (claims/s, token bucket) applies to **every** claimable
provider: cold claims on clusters and all claims on elastic providers. Defaults:
**100/s for elastic providers, 50/s for cold claims on a cluster** (below one
warm pool's measured refill of 70–85/s); warm claims on clusters are bounded by
reservations, not by `maxRate`. A provider over its rate is skipped for that
request without spending a spill attempt.

### 5.5 HTTP API

Versioned separately from the CRD: `/api/v1alpha1/...`. The unversioned
`/schedule` and `/providers` are aliased under it and kept until v1beta1, with
removal announced one minor release ahead.

```
POST /api/v1alpha1/sandboxes
  { "template", "key", "retry", "labels", "requires", "preferProvider",
    "pinProvider", "warmOnly", "ttlSeconds", "timeoutSeconds" }
  201 created | 200 idempotent replay (same key, same spec)
  409 no capacity | 409 SpecMismatch / ConstraintExcluded
  422 all rejected | 429 busy (+Retry-After, jittered) | 504 unknown {providers}

DELETE /api/v1alpha1/sandboxes/{provider}/{id}
  204 | 404 (gone; success for clients)
```

- `key` required; claims without idempotency are rejected (400).
- `retry: true` marks a client retry of a request whose outcome it did not
  learn (timeout, connection reset); the client SDK sets it on every retry.
  `pinProvider` is set from a 504's `providers` field when it names exactly
  one.
- Error bodies always carry `providers` (an array, possibly empty) and a
  machine-readable `reason`, so SDKs see one response shape. The two 409
  key-conflict cases are distinguished: `reason: SpecMismatch` (the key exists
  with a different spec) versus `reason: ConstraintExcluded` (the key's sandbox
  exists on a provider this request's filters exclude).
- A request that joins an in-flight claim for the same key (5.6) inherits that
  claim's deadline; its own `timeoutSeconds` is ignored.
- Separate token `PLACEMENT_CLAIM_TOKEN` (existing-Secret pattern,
  constant-time compare).
- Idempotency window = lifetime of the provider object. Documented: a replay
  after the sandbox expired creates a new one.

### 5.6 Binder loop (`pkg/binder`)

**Concurrent same-key requests.** Binding is leader-only, so the leader
de-duplicates in-flight keys with per-key singleflight coordination: a second
request for a key already being claimed joins the first one's result instead of
deciding independently (two first attempts could otherwise land on different
providers, where per-provider idempotency cannot see each other).

- A joiner whose spec-hash differs gets 409 `SpecMismatch` immediately.
- A joiner otherwise receives the first request's outcome, decided under the
  first request's filters and deadline; its own `timeoutSeconds` does not extend
  it. The shared result is then checked against the joiner's own filters
  (`requires`, `warmOnly`, `pinProvider`, `template`): if it fails them, the
  joiner gets 409 `ConstraintExcluded`, never a sandbox its constraints exclude.
  A successful joiner gets 200 (replay), not 201.
- Joiners `select` on the flight's completion channel and their own context, so
  a joiner whose client leaves stops waiting without affecting the flight. The
  shared function recovers its own panics (converting them to `UnknownOutcome`).
- At most `--max-joiners-per-key` (default 8) joiners per key; beyond that,
  429. Joiners count toward the global in-flight cap.
- If the first request's client disconnects, spilling stops and joiners receive
  its outcome; they retry, and the Lookup rule finds the sandbox.

This makes "at most one sandbox per key" hold for concurrent requests on one
leader.

**Lookup rule (no duplicates across retries and failover, without server
state).** A request may create a new sandbox only if it is a first attempt, or
if **every claim-capable provider** returns `ErrNotFound`. The probe set is
**unfiltered**: the provider holding the key is often the one that just flaked,
is now stale or unreachable, and would be dropped by a filter, after which the
rest would all say "not found" and a duplicate would be created. Constraints are
enforced on the *result* instead: a sandbox found on a provider the request's
filters exclude returns 409 `ConstraintExcluded` (never 200, never a new claim),
so a retry still cannot obtain a sandbox its constraints exclude. `Lookup`
compares the spec-hash, so a retry with a different spec gets `SpecMismatch`.

```
if pinProvider != "":
    Lookup(pinProvider, req)
    found → 200 if it passes the request's filters, else 409;
    ErrNotFound → claim only on pinProvider;
    KeyConflict → 409; KeyBusy or Busy → 429 {providers}; other → 504 {providers}
elif retry:
    reserve one Lookup token on every non-batched claim-capable provider, all or nothing:
        reserve on each bucket (rate.Limiter.ReserveN); if the longest wait
        fits before reqDeadline, wait it out and proceed; otherwise cancel
        every reservation (spending nothing) and return 429 {providers} with
        Retry-After = that longest wait, jittered
    probe Lookup on every claim-capable provider in parallel (per-attempt
    timeout), coalesced per provider when BatchLookup is implemented (where
    the coalescer reserves one Lookup token per batch on flush)
    any found          → 200 if it passes the request's filters, else 409
    any KeyConflict    → 409
    all ErrNotFound    → continue as a first attempt
    any Busy / KeyBusy → 429 {providers} with Retry-After (not 504, so a rate
                         limit on Lookups cannot loop into more Lookups)
    one inconclusive   → pin to it if it passes the filters, else 504 {providers}
    more than one      → 504 {providers}
```

Because every retry needs a Lookup on every provider, failover recovery would
be capped by the smallest Lookup rate in the fleet (20/s on an elastic
provider: ~3.4 min for 4,096 retries) if tokens were reserved per retry. Three
things lift that cap:

- **All-or-nothing reservation for non-batched providers**: a retry that cannot
  get every token spends none, so a slow bucket does not drain the fast ones.
- **Waiting within the deadline** instead of failing fast, so retries queue
  smoothly rather than bouncing as 429s.
- **Coalescing with per-batch token reservation**: retry Lookups for a provider
  implementing `BatchLookuper` arriving within a 50ms window are sent as one
  `LookupMany` call, and **one Lookup token is reserved per coalesced batch on
  flush** (not per individual retry). With batching, an elastic provider at 20
  batches/s and batches of up to 100 keys resolves 2,000 keys/s, so 4,096
  retries clear in ~2s.

An inconclusive result from a provider the filters exclude is still
inconclusive, never "not found". The common consequence: while a provider's
credentials are broken (401/403 → `Rejected`, inconclusive), retries of keys
that might be there return 504 or pin and fail with 422; they never create a
duplicate, and the provider's `Claiming` condition shows the credentials reason.

**Lease handover.** If the leader loses its lease, the process exits at once
(controller-runtime skips graceful shutdown on lease loss), and a claim it had
already sent may still be running on its remote provider. Two mechanisms bound
that window:
1. Adapters set the remote server-side request timeout (e.g. Kubernetes create
   `metav1.CreateOptions` / request `timeout` parameter) to the attempt context's
   remaining deadline, so the remote API server itself aborts any create that
   exceeds `spec.claims.timeout`.
2. A new leader **holds off claim, Lookup, and Release traffic** after acquiring
   the lease for **`max(spec.claims.timeout) + RemoteTimeoutSkewGrace (500ms)`,
   capped by `--claim-timeout`**, returning 503 `HoldOff` with a **jittered**
   `Retry-After` (hold-off remaining + random 0–3s) so the retry wave does not
   return in lockstep.

Graceful shutdowns release the lease only after in-flight attempts finish
(5.7). The old leader's `placement.agents.x-k8s.io/active` pod label is left
behind on lease loss; the next start clears it before reporting Ready
(`internal/leader`).

**Claim loop.**

```
reqDeadline := min(timeoutSeconds, --claim-timeout)
excluded := {}; spills := 0
loop while spills < --max-spill (3) and before reqDeadline:
  if client gone: stop spilling (an in-flight attempt still completes)
  decision := Schedule(candidates − excluded − refused(provider, template) − over maxRate)
  if none:
      if every remaining exclusion is "slots full": wait for any slot (5.7) once
      elif every remaining exclusion is "over maxRate":
          return 429, Retry-After = earliest token-bucket refill
      else return per collected outcomes
  if !TryAcquire(slot of decision.Provider):          // non-blocking
      excluded += provider (locally, slots full); continue  // costs no spill
  release := slot.Release; defer release()             // every return path
  reserve warm/cold (existing ledger)                  // after the slot
  h, err := Claim(attempt ctx, req)                    // 5.7
  if err == nil: return 201 h
  switch KindOf(err) { ... per 5.2; spill kinds do spills++ ... }
```

In Go the per-attempt body is its own function so `defer` releases the slot at
the end of each attempt, not of the request.

- Skipping a full provider can trade a warm start there for a cold or elastic
  start elsewhere. That is deliberate: 512 slots already exceed a cluster's
  ~300 claims/s, so a full provider is genuinely saturated. If measurements
  show warm→cold downgrades dominating, a short wait (≤ the provider's p50)
  only when the re-decision downgrades warm to cold is the follow-up.
- Non-blocking slot acquisition: a full provider is re-decided away from
  immediately, so a burst spreads across providers at once instead of queueing
  on the top-scored one and then moving in waves. A request blocks only when
  every viable candidate's slots are full, and then for at most
  `--claim-queue-timeout` (default 1s), once, before returning 429.
- Slot before reservation: refusals that never reach a provider do not drain its
  ledger.
- `registry.ReleaseReservation(provider)` undoes a reservation for `Busy`,
  `KeyBusy`, `KeyConflict`, `Unavailable`, and `Rejected`. It decrements the
  ledger's state word **only if** it removed a count from a bucket or the carry
  (by CAS); if a poll already cleared the reservation, it does nothing,
  preserving the ledger's never-below-zero invariant.
- A provider-confirmed `NoCapacity` refusal is recorded **globally** for
  `--refusal-ttl` **scoped to `(provider, template)`**, so a shortage in one
  template pool (e.g. `gpu`) costs one refused call per provider per TTL without
  blocking unrelated templates (e.g. `python`) on the same provider.
- `UnknownOutcome` retries use full-jitter exponential backoff (100 ms base,
  2 s cap) and a per-provider retry budget (≤ 10% of claims, token bucket).

### 5.7 Concurrency, timeouts, transports, shutdown

- Each attempt runs on `context.WithTimeout(context.WithoutCancel(reqCtx),
  perAttempt)`: a client disconnect stops spilling but never abandons a claim
  mid-flight (the key makes the result recoverable via replay).
- Per-attempt timeout default **3s** (`spec.claims.timeout`, CEL-bounded
  1s–5m). `--claim-timeout` (request deadline cap) default **15s**.
- `spec.claims.maxInFlight` default **512**, bounds 1–4096. Sizing: upstream
  measures claim latency of p90 ≤ 0.2s for a warm burst drained from a ready
  pool and sustained p90 0.68–1.03s with refill, with a 300-claim burst at p50
  ≈ 1.27s (agent-sandbox `docs/performance-tuning.md`). ~300 claims/s per
  cluster at ~1.0s needs ≈ 300 × 1.0 × 1.5 ≈ 450 in flight; 512 covers it. An
  adaptive (AIMD) limit is a later option.
- **Limiter changes**: a provider's limiter is replaced, not resized, when
  `maxInFlight` changes; each attempt holds a pointer to the limiter it acquired
  from and releases into that one, so in-flight attempts drain from the old
  limiter while new ones use the new.
- **Global cap** `--claim-max-inflight` (default 4096) counts requests holding a
  slot **and** requests waiting for one; beyond it the endpoint returns 429
  immediately, so a 100K burst parks at most 4096 goroutines.
- **Transports**: client-go QPS/burst sized as *rate*, not concurrency:
  `--remote-qps` ≥ peak claim rate × API calls per claim (create + bound watch
  setup ≈ 2), default 1000, burst 2000 (client-go defaults of 5/10 would cap
  claims at ~3/s). Hosted APIs get their own `http.Transport` with
  `MaxIdleConnsPerHost = MaxConnsPerHost = maxInFlight` and HTTP/2 where
  offered.
- **Binding detection (agent-sandbox)**: one watch per **in-flight** claim with
  a `metadata.name` field selector, **started from the `resourceVersion` the
  create returned** (so a bind that lands before the watch opens is not missed;
  410 Gone falls back to a GET), closed on bind or timeout; bounded by
  `maxInFlight`; watches opened by a `Lookup` waiting for binding count against
  the same `maxInFlight` and stream budget. No informer over all owned claims, so
  memory is per in-flight claim, not per live sandbox, and leader failover
  relists nothing.
- **Lookups** have their own per-provider token bucket (`spec.claims.lookupRate`,
  burst equal to one second of rate), separate from claims, and are always
  consistent reads (no `resourceVersion=0`). Default 200/s for Kubernetes
  providers and 20/s for elastic (hosted) providers, whose own API limits are
  often lower.
- **Shutdown**: detached attempts are tracked by a counter plus a done channel;
  the closing flag and the counter increment are under one mutex, so no attempt
  starts after shutdown begins and none races the channel close. Shutdown
  `select`s on the done channel against the claim deadline, since
  `WaitGroup.Wait` has no timeout.

### 5.8 Health from claims

Claim health is tracked separately from poll health: a windowed error ratio
(≥ 50% `Unavailable` over ≥ 20 attempts) demotes the provider's claim path.
Recovery is through **half-open probes**: a `Lookup` of a random, never-used key
(cheap and side-effect free; `ErrNotFound` counts as healthy; 401/403 count as
unhealthy and set the `Claiming` reason to a credentials problem), with
exponential backoff, not the next poll, so a healthy `/warmpool` cannot flap a
broken claim path every 10s. Readiness never depends on claim outcomes.

### 5.9 Ownership, expiry and cleanup

- **Owner** = short hash of (release fullname, namespace, cluster ID); the
  readable name goes in an annotation. Cluster ID is the `kube-system` namespace
  UID, read with a ClusterRole rule `get namespaces, resourceNames:
  [kube-system]`, or set explicitly with `--cluster-id` for installs that cannot
  be granted it. Two installs with the default release name in different
  clusters cannot reap each other.
- Owner and key are **labels on the provider object only** (selectors);
  `placement.agents.x-k8s.io/spec-hash` (template, labels, TTL bucket) is an
  annotation. Caller labels that reach pods must use upstream's allowlisted
  domain (default `sandbox.users.io`) and are validated up front.
- **Expiry is always bounded**: the core sets `TTL` to `ttlSeconds` or
  `--claim-max-ttl` (default 24h), capped by the latter, before `Validate`.
  agent-sandbox claims always set `lifecycle.shutdownTime = now + TTL`,
  `shutdownPolicy: Delete` (upstream defaults to `Retain`, which leaves claims
  forever) and `ttlSecondsAfterFinished`.
- **Reaper** only for providers without native expiry: leader-only runnable;
  paged `ListOwned` (500/page, server-side selector, metadata-only on
  Kubernetes); per-object deletes with `Handle.Version` (UID/resourceVersion)
  preconditions so a background reap never deletes a newly created replacement
  that reused the same ID, never DeleteCollection; skips objects already
  deleting; token-bucket rate limit and `--reap-max-per-cycle` circuit breaker;
  server-side `DryRun: All` mode with an Event. No finalizers.
- **Events**: emitted only on reaper dry-run and on `Claiming` condition
  transitions, never per claim. Events on the cluster-scoped `SandboxProvider`
  land in `default`, so the ClusterRole gains `events.k8s.io events: create,
  patch`.

### 5.10 Out-of-process adapters

- `proto/adapter/v1alpha1`: `GetInfo` (capabilities: `can_claim`,
  `can_batch_lookup`, `can_list_owned`, `is_elastic`, `supported_kinds`),
  `Fetch`, `Claim`, `Lookup`, `LookupMany`, `Release`, `ListOwned`, with `Kind`
  as a proto enum following buf lint style: `KIND_UNSPECIFIED = 0`, handled
  exactly like `KIND_UNKNOWN_OUTCOME`, then the named kinds. Enum numbers are
  append-only and never reused; the host maps `UNSPECIFIED` **and any
  unrecognised number** (a newer sidecar talking to an older host) to
  `KindUnknownOutcome`. `Handle` carries `version` (field 3) so out-of-process
  reaper deletes preserve UID/resourceVersion preconditions.
- `spec.adapter: grpc`, `spec.endpoint: unix:///var/run/adapters/<name>.sock`.
- Pod hardening so each sidecar holds only its own credentials:
  `automountServiceAccountToken: false` with a projected token for the
  scheduler container only; sidecar Secrets outside the namespace the scheduler
  can read; socket in an `emptyDir`, mode 0660, distinct `runAsUser` per
  sidecar; the chart's restricted `securityContext` copied.
- `adaptertest` is importable and can run against a socket, so third parties
  can certify adapters.

### 5.11 agent-sandbox binder

- **Access**, in order of preference:
  1. **ClusterProfile**: `spec.clusterProfileRef{namespace, name}`, confined to
     an allowlist of namespaces (`--cluster-profile-namespaces`, default the
     release namespace), read with a namespaced Role.
  2. Workload-identity federation.
  3. **Kubeconfig Secret** (fallback). Parsed with `clientcmd.Load` and rejected
     if it uses `exec`, `auth-provider`, `tokenFile` or client-certificate file
     paths.
- **Options** are typed (`spec.kubeapi.namespace`), not annotations.
- **Claim**: `SandboxClaim` named `sc-` + first 20 hex of
  sha256(owner + "/" + key) (starts with a letter, ≤ 63, DNS-1035-safe for the
  derived Sandbox/Service names), `warmPoolRef` from the template mapping,
  lifecycle per 5.9, owner/key labels, spec-hash annotation.
- **Replay** on `AlreadyExists`, and **Lookup**, apply one shared rule: live
  claim with matching spec-hash → the handle (waiting for binding if not yet
  bound); different hash → `KeyConflict` (409); claim being deleted, or past
  its `shutdownTime` and about to be → `KeyBusy` with a short `RetryAfter`,
  pinned to this provider.

## 6. Phases

Each phase is one PR. `--enable-claims` stays off by default until after
phase 6.

0. **Secret scope fix**: namespace-scoped Secret cache (`cache.Options.ByObject`)
   and an unconditional namespaced Role replacing the cluster-wide Secret rule.
1. **Contract**: `pkg/provider` (types, `ErrNotFound`, `Kind`, constructors,
   `KindOf`, `Validate`), `Factory` returns `Provider`, registry aliases,
   `proto/adapter/v1alpha1`, `adaptertest` conformance suite, fake binder.
2. **API fields**: `templates` list-map, `claims{enabled, maxInFlight, timeout,
   maxRate, lookupRate, elasticWarmScore}`, `clusterProfileRef`,
   `kubeapi{namespace}`; `Claiming` condition + `status.claims{lastSuccessTime,
   lastFailureReason}`; printer columns; status writes on the 15s loop via merge
   patch; `TemplateAvailable` and `WarmOnly` filters; owner/cluster-ID RBAC and
   `--cluster-id`.
3. **Binder loop and endpoints**: `pkg/binder`, `/api/v1alpha1/sandboxes`,
   Lookup rule, non-blocking slots, global cap, template-scoped refusal TTL,
   rate bounds, retry budget, `ReleaseReservation`, health from claims, metrics
   (`claim_attempts_total{provider,kind}`, `claim_duration_seconds{provider}`,
   `claim_spills_total`, `claim_inflight{provider}`), shutdown sizing.
4. **Dependency bump** to upstream v1.0.7's k8s/controller-runtime versions.
5. **agent-sandbox binder** (5.11) + envtest with upstream CRDs + a real-cluster
   run.
6. **First hosted adapter (`pkg/adapter/e2b`)**: elastic `Source`, `Binder`,
   and `BatchLookuper` over the E2B HTTP API (`apiKey`/`token` Secret
   credentials, `authMode: x-api-key|bearer` option, native `timeout`/`endAt`
   TTL enforcement, server-side metadata filtering on
   `placement.agents.x-k8s.io/{owner,key,spec-hash,expires-at}`, dedicated
   HTTP/2 transport sized to 512 connections per host, and
   `adaptertest.TestBinder` conformance). **Interface stability gate.**
7. **Reaper (`pkg/reaper`)**: leader-only periodic reaper (`--reap-interval=30s`,
   `--reap-max-per-cycle=200`, `--reap-rate=10`, `--reap-dry-run=false`) paging
   `ListOwned` (`500`/page) and releasing expired handles with `Handle.Version`
   preconditions, `sandbox_scheduler_reap_total{provider,outcome}` metrics, and
   `events.k8s.io/v1` Events on `Claiming` condition transitions and reaper
   dry-run cycles.
8. **gRPC adapter host**, chart sidecar packaging, `-plugins` image variant;
   `fleet` (KEP-1434) capacity adapter as its own module.

## 7. Testing

- Binder loop: table tests over every `Kind`, including `KeyConflict` (no
  retry, no spill), `KeyBusy` (pins, never spills) versus `Busy` (spills),
  template-scoped `NoCapacity` refusals, and inconclusive `Lookup` on retry;
  unfiltered retry probes and coalesced `LookupMany` batches; singleflight
  coalescing, joiner filter enforcement, and joiner cap; non-blocking slot
  saturation; post-election hold-off including `RemoteTimeoutSkewGrace`.
- `ReleaseReservation` after a poll already cleared the reservation leaves the
  ledger unchanged.
- `adaptertest.TestBinder(t *testing.T, newHarness func(*testing.T) Harness)`
  exercising idempotent replay, `KeyConflict`, `KeyBusy`, `NoCapacity`, `Busy`,
  `Unavailable`, `Rejected`, `UnknownOutcome` (including `KIND_UNSPECIFIED` and
  unrecognised proto enum numbers), O(1) `Lookup` call bound, `LookupMany`
  consistency, and `Release` precondition enforcement.
- `FuzzValidateKey`, `FuzzLabels`.

## 8. Compatibility and migration

- `/schedule` and `/providers` unchanged (also served under `/api/v1alpha1`).
- CRD changes additive within v1alpha1.
- Phase 0 narrows RBAC; installs that point `credentialsRef` at another
  namespace already fail today (reads are confined to `--secret-namespace`).

## 9. Scalability

- Decision path unchanged: memory-only, ~20µs/decision.
- Claim throughput per provider is λ = in-flight / latency. With 512 slots and
  upstream's measured latencies, headroom is ~400–2,500 claims/s per provider.
- Failover: on a crash, ~15s until the lease expires plus the hold-off (3.5s
  with defaults) gives ~18.5s of 503s; graceful handovers skip the lease wait.
- Retry recovery after failover: with `BatchLookup` reserving one token per
  coalesced batch (up to 100 keys), 4,096 retries clear a 20 batches/s elastic
  provider in ~2s.
- Leader memory: ~50KB per in-flight claim; global cap of 4096 bounds this at
  ~200MB. Nothing scales with the number of live sandboxes.

## 10. Open questions

1. **API group.** `placement.agents.x-k8s.io` vs `placement.sandbox-scheduler.io`.
2. `Lookup` fan-out on retries without `pinProvider`: acceptable with batching,
   or should the SDK always persist the provider from a 504?
3. Per-tenant quotas before enabling claims anywhere shared, or is the separate
   claim token enough for now?

## 11. Review findings not adopted

- *Separate Go module for the adapter contract now* (OSS): deferred until the
  contract is declared stable at phase 6.
- *A `TransformFunc`-stripped informer over all owned claims* (scalability):
  rejected because it still scales with live sandboxes and relists on failover.
- *Mapping a deleting claim to `Rejected`*: chose `KeyBusy` (pinned) to the
  provider instead, since spilling would create a second sandbox under the same
  key while the first still exists.

## 12. Changes

### v6 → v7 (round 6: PR #5 review + Phases 0–7 implementation)

| v6 | v7 | Raised by |
|---|---|---|
| Retry pseudocode reserved 1 Lookup token per retry before coalescing, still capping batched providers at 20 retries/s | Non-batched providers reserve all-or-nothing per retry; batched providers (`BatchLookuper`) reserve 1 Lookup token per coalesced batch inside `lookupCoalescer.flush` | Codex review (P1) |
| Lease handover hold-off used `max(spec.claims.timeout)` without server-side timeout enforcement or skew margin | Adapters bound remote server-side request timeouts to the attempt deadline; hold-off adds `RemoteTimeoutSkewGrace` (500ms) and covers `Bind` and `Release` | Codex review (P1) |
| `proto/adapter/v1alpha1` lacked batch lookup RPC and capability bit | Added `rpc LookupMany(LookupManyRequest) returns (LookupManyResponse)` and `bool can_batch_lookup = 3` in `GetInfoResponse` | Codex review (P1) |
| `NoCapacity` refusal cache was keyed only by `provider`, suppressing all templates when one pool was empty | Refusal cache is keyed by `(provider, template)` (`refusalKey`) so an empty `gpu` pool does not suppress `python` on the same provider | Codex review (P2) |
| `provider.Handle` and proto `Handle` had no deletion precondition field for reaper safety | Added `Version string` (`string version = 3` in proto `Handle`), populated by `ListOwned` and enforced by `Release` when non-empty | Codex review (P2), Go review |
| Hosted E2B adapter and reaper flags/metrics specified only in prose | Implemented `pkg/adapter/e2b` (`apiKey`/`token`, `authMode: x-api-key\|bearer`, metadata filter `Lookup`/`LookupMany`, native `timeout` TTL) and `pkg/reaper` (`--reap-interval=30s`, `--reap-max-per-cycle=200`, `--reap-rate=10`, `--reap-dry-run=false`, `sandbox_scheduler_reap_total{provider,outcome}`) | OSS review |

### v5 → v6 (round 5)

| v5 | v6 | Raised by |
|---|---|---|
| Unfiltered retry probe made failover recovery wait on the smallest Lookup bucket (20/s elastic: ~3.4 min for 4,096 retries), and failed probes drained other buckets | All-or-nothing token reservation; wait within the deadline; per-provider Lookup coalescing via optional `BatchLookup` (≈2s for 4,096); section 9 restated | Scalability (major) |
| Hold-off fixed at the default per-attempt timeout, fixed `Retry-After` | Largest provider timeout, capped by `--claim-timeout`; jittered `Retry-After`; stated as a heuristic | Go, Kubernetes, Scalability |
| One 409 body for spec mismatch and constraint exclusion | `reason: SpecMismatch` vs `ConstraintExcluded` | Go |
| Joiner `timeoutSeconds` silently ignored | Documented in 5.5 | Kubernetes |
| "Lookup by key without listing" implied | Stated as a `Binder` prerequisite; asserted in `adaptertest` | OSS |
| Unknown proto enum test implicit | Explicit contract case; must not map to a spilling kind | Go |
| Crash failover 503 duration unstated | ~18s stated in section 9 | Scalability |
