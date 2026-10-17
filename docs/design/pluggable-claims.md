# Pluggable claims: placing *and* provisioning sandboxes beyond Kubernetes

Status: **draft v5, revised after four review rounds** (OSS compatibility,
scalability, Kubernetes practices, Go practices). Nothing here is implemented.
Section 12 records what changed in each revision and why.

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

## 3. Current state (main @ 2bbbd7d)

- `pkg/adapter`: name-keyed registry; `Factory func(Config) (registry.Source,
  error)`. One adapter, `agent-sandbox`, reading `GET <endpoint>/warmpool` from
  an **operator-owned gateway** (not an upstream component).
- `pkg/registry`: polling off the decision path; lock-free `Snapshot()`;
  atomic `TryReserveWarm`; `RecordColdPlacement` for power-of-two-choices;
  per-provider ledgers published copy-on-write via `atomic.Pointer`.
- `pkg/scheduler`: `POST /schedule`, `GET /providers`; bearer token; leader-only
  serving via pod label.
- API group `placement.agents.x-k8s.io/v1alpha1` (see 10.1).
- Module pins k8s.io/* v0.36 and controller-runtime v0.24.1; upstream
  `sigs.k8s.io/agent-sandbox` **v1.0.7** (the release this plan targets)
  requires k8s.io v0.37.1 and controller-runtime v0.25.1.

**Existing issue found in review, fixed first (phase 0):** the chart's
ClusterRole grants cluster-wide `get/list/watch` on Secrets and the manager sets
no cache restriction, so the reconciler's cached Secret reads start a
cluster-wide Secret informer: every Secret in the cluster is held in memory by
a process that serves HTTP, although only `--secret-namespace` is ever read.

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
    Source Source      // required: capacity
    Binder Binder      // nil: decide-only provider
    Lister OwnedLister // nil: relies on provider-native expiry
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
    // Release must work from Provider and ID alone, and returns nil when the
    // sandbox is already gone.
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
    Endpoint  string
    Connect   map[string]string // provider-native, documented per adapter
    ExpiresAt time.Time         // zero = unknown
}
```

`Factory` becomes `func(Config) (provider.Provider, error)`, changed now while
one in-tree adapter exists. `pkg/binder` resolves a provider's `Provider`,
limiter and settings through a copy-on-write `atomic.Pointer[map[string]*target]`
published by the registry on every add/remove/config change, the same pattern
as the existing ledgers, so the request path takes no lock. No JSON tags on
these types; wire types live in `pkg/scheduler`. The same messages are defined
in `proto/adapter/v1alpha1` from phase 1.

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
| `KeyConflict` | Same key exists with a different spec | Stop: no retry, no spill (either would create a second sandbox under the key) | 409 `KeyConflict` |
| `NoCapacity` | Provider confirmed it cannot serve | Mark provider refused **globally** for `--refusal-ttl` (1s); next candidate | 409 with reasons |
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
  409 no capacity | 409 KeyConflict (same key, different spec)
  422 all rejected | 429 busy (+Retry-After, jittered) | 504 unknown {providers}

DELETE /api/v1alpha1/sandboxes/{provider}/{id}
  204 | 404 (gone; success for clients)
```

- `key` required; claims without idempotency are rejected (400).
- `retry: true` marks a client retry of a request whose outcome it did not
  learn (timeout, connection reset); the client SDK sets it on every retry.
  `pinProvider` is set from a 504's `providers` field when it names exactly
  one.
- Error bodies always carry `providers` (an array, possibly empty) so SDKs see
  one response shape.
- Separate token `PLACEMENT_CLAIM_TOKEN` (existing-Secret pattern,
  constant-time compare).
- Idempotency window = lifetime of the provider object. Documented: a replay
  after the sandbox expired creates a new one.

### 5.6 Binder loop (`pkg/binder`)

**Concurrent same-key requests.** Binding is leader-only, so the leader
de-duplicates in-flight keys with `singleflight.Group` keyed by key: a second
request for a key already being claimed joins the first one's result instead of
deciding independently (two first attempts could otherwise land on different
providers, where per-provider idempotency cannot see each other).

- A joiner whose spec-hash differs gets 409 `KeyConflict` immediately.
- A joiner otherwise receives the first request's outcome, decided under the
  first request's filters and deadline; its own `timeoutSeconds` does not extend
  it. The shared result is then checked against the joiner's own filters
  (`requires`, `warmOnly`): if it fails them, the joiner gets 409
  `KeyConflict`, never a sandbox its constraints exclude. A successful joiner
  gets 200 (replay), not 201.
- Joiners wait with `DoChan` and `select` on their own context, so a joiner whose
  client leaves stops waiting without affecting the flight. The shared function
  recovers its own panics (converting them to `UnknownOutcome`), since a panic
  under `DoChan` would otherwise crash the process.
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
filters exclude returns 409 `KeyConflict` (never 200, never a new claim), so a
retry still cannot obtain a sandbox its constraints exclude. `Lookup` compares
the spec-hash, so a retry with a different spec also gets `KeyConflict`.

```
if pinProvider != "":
    Lookup(pinProvider, req)
    found → 200 if it passes the request's filters, else 409;
    ErrNotFound → claim only on pinProvider;
    KeyConflict → 409; KeyBusy or Busy → 429 {providers}; other → 504 {providers}
elif retry:
    probe Lookup on every claim-capable provider in parallel (bounded,
    per-attempt timeout, separate per-provider Lookup token bucket)
    any found          → 200 if it passes the request's filters, else 409
    any KeyConflict    → 409
    all ErrNotFound    → continue as a first attempt
    any Busy / KeyBusy → 429 {providers} with Retry-After (not 504, so a rate
                         limit on Lookups cannot loop into more Lookups)
    one inconclusive   → pin to it if it passes the filters, else 504 {providers}
    more than one      → 504 {providers}
```

An inconclusive result from a provider the filters exclude is still
inconclusive, never "not found". The common consequence: while a provider's
credentials are broken (401/403 → `Rejected`, inconclusive), retries of keys
that might be there return 504 or pin and fail with 422; they never create a
duplicate, and the provider's `Claiming` condition shows the credentials reason.

**Lease handover.** If the leader loses its lease, the process exits at once
(controller-runtime skips graceful shutdown on lease loss), and a claim it had
already sent may still land on its provider. A new leader therefore **holds off
claim and Lookup traffic for one per-attempt timeout** (default 3s) after
acquiring the lease, returning 503 with `Retry-After`; any claim the old process
sent completes or fails within that time, so the new leader's Lookups see it.
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
  decision := Schedule(candidates − excluded − globally refused − over maxRate)
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
  `Unavailable`, `Rejected` and slot failures. It decrements the ledger's state
  word **only if** it removed a count from a bucket or the carry (by CAS); if a
  poll already cleared the reservation, it does nothing, preserving the
  ledger's never-below-zero invariant.
- A provider-confirmed refusal is recorded **globally** for `--refusal-ttl`, so
  a fleet-wide shortage costs one refused call per provider per TTL, not one per
  request per provider.
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
  the same `maxInFlight` and stream budget. No informer over all owned claims, so memory is per in-flight
  claim, not per live sandbox, and leader failover relists nothing. The
  apiserver has no field index for a custom resource's `metadata.name`, so each
  claim event is filtered against every open watch in the namespace: at 512
  watches and ~1K events/s that is ~0.5M cheap checks/s, tolerable. Watches and
  creates share one transport per provider; the apiserver's HTTP/2
  streams-per-connection limit must exceed `maxInFlight` plus headroom, or
  client-go is configured to open more connections. An example FlowSchema
  covering `create`, `get` and `watch` on `sandboxclaims` for the scheduler's
  remote identity is shipped (upstream's APF overlay covers only its own
  controller).
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
  HTTP shutdown timeout and `ctrl.Options.GracefulShutdownTimeout` = claim
  timeout + 5s; the chart sets `terminationGracePeriodSeconds` ≥ preStop +
  claim timeout + 10s and a native `preStop.sleep` of 5s (the image has no
  shell).

### 5.8 Health from claims

Claim health is tracked separately from poll health: a windowed error ratio
(≥ 50% `Unavailable` over ≥ 20 attempts) demotes the provider's claim path.
Recovery is through **half-open probes**: a `Lookup` of a random, never-used key
(cheap and side-effect free; `ErrNotFound` counts as healthy; 401/403 count as
unhealthy and set the `Claiming` reason to a credentials problem), with exponential
backoff, not the next poll, so a healthy `/warmpool` cannot flap a broken claim
path every 10s. Readiness never depends on claim outcomes.

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
  Kubernetes); per-object deletes with UID/resourceVersion preconditions, never
  DeleteCollection; skips objects already deleting; token-bucket rate limit and
  `--reap-max-per-cycle` circuit breaker; server-side `DryRun: All` mode with an
  Event. No finalizers.
- **Events**: emitted only on reaper dry-run and on `Claiming` condition
  transitions, never per claim. Events on the cluster-scoped `SandboxProvider`
  land in `default`, so the ClusterRole gains `events.k8s.io events: create,
  patch`.

### 5.10 Out-of-process adapters

- `proto/adapter/v1alpha1`: `GetInfo` (capabilities: claim, lookup, list,
  elastic), `Fetch`, `Claim`, `Lookup`, `Release`, `ListOwned`, with `Kind` as a
  proto enum following buf lint style: `KIND_UNSPECIFIED = 0`, handled exactly
  like `KIND_UNKNOWN_OUTCOME`, then the named kinds. Enum numbers are
  append-only and never reused; the host maps `UNSPECIFIED` **and any
  unrecognised number** (a newer sidecar talking to an older host) to
  `KindUnknownOutcome`. `GetInfo` reports which kinds the adapter can return
  (notably `KEY_BUSY`, `KEY_CONFLICT`), so an older sidecar is handled
  predictably. Published as its own Go
  module once stable; deprecation policy stated before v1.
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
     release namespace), read with a namespaced Role. If the ClusterProfile CRD
     is not installed (checked via discovery), the provider reports
     `Claiming=False, reason ClusterProfileUnavailable`. Its access providers
     run credential plugins (e.g. `gke-gcloud-auth-plugin`, `kubelogin`). The
     documented default is mounting them from a volume the operator supplies;
     an optional `-plugins` image variant redistributes them only with a NOTICE
     file, per-plugin license checks and CVE tracking. Either way they are
     configured by the operator in the deployment, never by a Secret.
  2. Workload-identity federation.
  3. **Kubeconfig Secret** (fallback). Parsed with `clientcmd.Load` and rejected
     if it uses `exec`, `auth-provider`, `tokenFile` or client-certificate file
     paths: these would let anyone who can write the Secret run binaries or read
     the scheduler's own token. The exec ban applies to Secret-supplied
     kubeconfigs only. The client is rebuilt when the Secret's resourceVersion
     changes, closing the old transport.
  Remote RBAC: namespaced Role, `sandboxclaims: create,get,list,watch,delete`.
  RBAC cannot restrict `delete` by label, so the owner label and UID
  preconditions are the only guard against touching other tenants' claims;
  the documented setup is a **namespace dedicated to the scheduler** on each
  remote cluster.
- **Options** are typed (`spec.kubeapi.namespace`), not annotations.
- **Claim**: `SandboxClaim` named `sc-` + first 20 hex of
  sha256(owner + "/" + key) (starts with a letter, ≤ 63, DNS-1035-safe for the
  derived Sandbox/Service names), `warmPoolRef` from the template mapping,
  lifecycle per 5.9, owner/key labels, spec-hash annotation.
- **Replay** on `AlreadyExists`, and **Lookup**, apply one shared rule: live
  claim with matching spec-hash → the handle (waiting for binding if not yet
  bound); different hash → `KeyConflict` (409); claim being deleted, or past
  its `shutdownTime` and about to be → `KeyBusy` with a short `RetryAfter`,
  pinned to this provider, so the key is not reused elsewhere while its sandbox
  still exists.
- **Relation to the upstream SDK's `AdoptExisting`**: the naming convention is
  shared but the replay semantics differ deliberately: a terminating claim is
  `KeyBusy` (wait and retry) where the SDK fails, and a spec-hash over
  template, labels and TTL bucket replaces the SDK's warm-pool comparison.
  The two cannot collide: scheduler claim names are hashed from owner and key.
- **Lookup** = one consistent GET of the deterministic claim name (no list).
  `ErrNotFound` only on 404; 401/403 are `Rejected` (inconclusive for the
  Lookup rule), never "not found".
- **Bound** when `status.sandbox.name` is set (there is no Bound condition);
  `Connect` carries `X-Sandbox-ID` (the Sandbox name) and
  `X-Sandbox-Namespace`.
- **Dependency**: importing upstream's generated clientset
  (`clients/k8s/extensions` in the root module `sigs.k8s.io/agent-sandbox`)
  upgrades this repo to k8s.io v0.37 / controller-runtime v0.25. Done as its own
  PR pinned to v1.0.7 (confirming the clientset path in that tag), with a
  version matrix in the README.

## 6. Phases

Each phase is one PR. `--enable-claims` stays off by default until after
phase 6.

0. **Secret scope fix** (independent, ship now): namespace-scoped Secret cache
   (`cache.Options.ByObject`) and an unconditional namespaced Role (not gated on
   `leaderElection` like the existing leader-election Role) replacing the
   cluster-wide Secret rule.
1. **Contract**: `pkg/provider` (types, `ErrNotFound`, `Kind`, constructors,
   `KindOf`, `Validate`), `Factory` returns `Provider`, registry aliases,
   `proto/adapter/v1alpha1`, `adaptertest` skeleton, fake binder. No behaviour
   change.
2. **API fields**: `templates` list-map, `claims{enabled, maxInFlight, timeout,
   maxRate, elasticWarmScore}`, `clusterProfileRef`, `kubeapi{namespace}`;
   `Claiming` condition + `status.claims{lastSuccessTime, lastFailureReason}`;
   printer columns; status writes on the existing 15s loop via merge patch;
   `TemplateAvailable` filter; owner/cluster-ID RBAC and `--cluster-id`.
3. **Binder loop and endpoints**: `pkg/binder`, `/api/v1alpha1/sandboxes`,
   Lookup rule, non-blocking slots, global cap, refusal TTL, rate bounds, retry
   budget, `ReleaseReservation`, health from claims, metrics
   (`claim_attempts_total{provider,kind}`, `claim_duration_seconds{provider}`,
   `claim_spills_total`, `claim_inflight{provider}`), shutdown sizing.
4. **Dependency bump** to upstream v1.0.7's k8s/controller-runtime versions.
5. **agent-sandbox binder** (5.11) + envtest with upstream CRDs + a real-cluster
   run.
6. **First hosted adapter**, out-of-tree module or sidecar so vendor
   dependencies stay out of the core. Daytona's Apache-2.0 Go SDK (pin the
   SDK-only module, whose path has moved between `daytonaio/daytona/libs/sdk-go`
   and `daytona/clients/sdk-go`, and verify its LICENSE; the server is reported
   AGPL) or a hand-written E2B REST client. **Interface stability gate.**
7. **Reaper.**
8. **gRPC adapter host**, chart sidecar packaging, `-plugins` image variant;
   `fleet` (KEP-1434) capacity adapter as its own module.

## 7. Testing

- Binder loop: table tests over every `Kind`, including `KeyConflict` (no
  retry, no spill), `KeyBusy` (pins, never spills) versus `Busy` (spills), and
  inconclusive `Lookup` on retry (pins, never claims elsewhere); a retry whose
  holding provider is unreachable still finds the sandbox (unfiltered probe);
  a retry whose `requires` excludes the holding provider gets 409, never a new
  claim; a retry with a different template gets 409; concurrent first attempts
  with one key join one claim (singleflight), a joiner failing its own filters
  gets 409, joiners beyond the per-key cap get 429, and a panic in the shared
  function is recovered; requests during the post-election hold-off get 503;
  slot released on every return path; mixed outcomes; deadlines and backoff under `testing/synctest`;
  concurrent same-key claims under `-race` asserting at most one sandbox; a
  burst against one full provider spreads to others without spending spills;
  all-full returns 429, never 409.
- `ReleaseReservation` after a poll already cleared the reservation leaves the
  ledger unchanged (property test over interleavings).
- `adaptertest.TestBinder(t *testing.T, newHarness func(*testing.T) Harness)`
  modelled on `testing/fstest`; the harness injects faults the suite cannot
  reach generically (no capacity, rate limit, refuse-before-send,
  drop-after-send, auth, key conflict, deleting key), and checks that `Lookup`
  applies the same replay rules as `Claim`. Over gRPC it also checks that
  `KIND_UNSPECIFIED` and an unrecognised enum number map to
  `KindUnknownOutcome`.
- `FuzzValidateKey`, `FuzzLabels`.
- agent-sandbox: envtest with upstream CRDs; replay with matching and
  mismatched spec-hash; replay of a deleting claim; claims on an empty pool
  (cold success); watch-per-claim cleanup on timeout.
- Benchmarks: decision path must not regress; binder loop at 10K in-flight
  fake claims (memory per claim).
- License check in CI for any adapter dependency.

## 8. Compatibility and migration

- `/schedule` and `/providers` unchanged (also served under `/api/v1alpha1`).
- CRD changes additive within v1alpha1; `spec.claims.enabled: true` with the
  flag off surfaces `Claiming=False, reason DisabledByFlag`.
- Phase 0 narrows RBAC; installs that point `credentialsRef` at another
  namespace already fail today (reads are confined to `--secret-namespace`).

## 9. Scalability

- Decision path unchanged: memory-only, ~20µs/decision.
- Claim throughput per provider is λ = in-flight / latency. With 512 slots and
  upstream's measured latencies (agent-sandbox `docs/performance-tuning.md`:
  warm burst p90 ≤ 0.2s, 300-claim burst p50 ≈ 1.27s, sustained p90
  0.68–1.03s), headroom is ~400–2,500 claims/s per provider; the cluster
  (≈300/s warm, 70–85/s per pool refill) is the limit, not the scheduler. At
  the same table's burst p90 (≈3.2s), 512 slots sustain ~160/s per provider;
  operators expecting long bursts near 300/s raise `maxInFlight` toward 1024.
- Retry storms after failover: up to 4,096 retries × P providers of consistent
  Lookup GETs, spread by client jitter and capped by the per-provider Lookup
  bucket; Lookup rate limits return 429, not 504, so they cannot loop.
- Leader memory: ~50KB per in-flight claim, including its bind watch; the
  global cap of 4096 bounds this at ~200MB. Nothing scales with the number of
  live sandboxes.
- Bursts spread immediately: a full provider is skipped without waiting, so
  fleet throughput is the sum of providers, not one at a time.
- Shortage amplification is bounded: one refused call per provider per
  `--refusal-ttl`, at most `--max-spill` attempts per request, retries within a
  10% budget, cold claims rate-bounded.
- Leader-only binding is acceptable for throughput; for availability, the Lookup
  rule makes failover safe (a client retry after a reset finds the original
  sandbox instead of creating a second). The ~15s failover gap remains.
  Active-active later: reservations as per-replica hints, per-provider limits
  divided by replica count, keys routed by consistent hash or recovered by
  `Lookup`.

## 10. Open questions

1. **API group.** `placement.agents.x-k8s.io` sits in upstream agent-sandbox's
   namespace and implies endorsement; upstream could add a `placement` subgroup.
   Options: seek sponsorship from agent-sandbox/SIG-Apps, or rename now (e.g.
   `placement.sandbox-scheduler.io`) while v1alpha1 makes it cheap. **Owner
   decision needed before phase 2.**
2. `Lookup` fan-out on retries without `pinProvider` costs one GET per
   claim-capable provider: acceptable, or should the SDK always persist the
   provider from a 504?
3. Per-tenant quotas before enabling claims anywhere shared, or is the separate
   claim token enough for now?

## 11. Review findings not adopted

- *Separate Go module for the adapter contract now* (OSS): deferred until the
  contract is declared stable at phase 6; a v0 single module avoids release
  overhead while it still changes. The package is marked experimental instead.
- *A `TransformFunc`-stripped informer over all owned claims* (scalability,
  alternative to per-claim watches): rejected because it still scales with live
  sandboxes and relists on failover; per-claim watches bounded by `maxInFlight`
  keep state proportional to work in flight.
- *Mapping a deleting claim to `Rejected`* (acceptable to the Go review if
  documented): chose `KeyBusy` (pinned) to the provider instead (Kubernetes review),
  since spilling would create a second sandbox under the same key while the
  first still exists.

## 12. Changes

### v4 → v5 (round 4)

| v4 | v5 | Raised by |
|---|---|---|
| Retry Lookup probed only filter-passing candidates, so a flaked (now unreachable) holder was skipped and a duplicate created | Unfiltered probe of every claim-capable provider; constraints enforced on the result (found on an excluded provider → 409); inconclusive on an excluded provider stays inconclusive | Go (major), reconciling the Kubernetes round-3 finding |
| Singleflight joiner semantics unspecified | Joiner gets the first's outcome and deadline; result checked against the joiner's filters (409 if excluded); `DoChan` + own ctx; panic recovered in the shared function; 200 replay; per-key joiner cap → 429; joiners count toward the global cap | Go, Scalability |
| Lease-loss duplicate window documented only | New leader holds off claims/Lookups for one per-attempt timeout; stale `active` label noted | Go, Kubernetes |
| Lookup could block on binding indefinitely; its watches unbounded | Lookup stays within ctx; its watches count against maxInFlight and the stream budget | Go, Kubernetes |
| Lookup bucket burst and hosted defaults unspecified | Burst = 1s of rate; 200/s Kubernetes, 20/s elastic | Scalability |
| Broken-credential retries undocumented | Documented: 504/422, never a duplicate; `Claiming` credentials reason | Kubernetes |
| Shared remote namespace | Dedicated namespace recommended (RBAC cannot restrict delete by label) | Kubernetes |
| Difference from upstream `AdoptExisting` implicit | Stated in 5.11 | OSS |
| Proto enum evolution unspecified | Append-only numbers; unknown → UnknownOutcome; `GetInfo` reports supported kinds; contract test | OSS |

### v3 → v4 (round 3)

| v3 | v4 | Raised by |
|---|---|---|
| One `KindBusy` for rate limits (spill) and deleting keys (pin) | Separate `KindKeyBusy` that pins; `Busy` spills | Go (major) |
| Concurrent first attempts with one key could land on different providers | Leader `singleflight` per key; joiner with a different spec gets 409 | Go (major) |
| Retry Lookup probed all providers and ignored the spec | Probe only filter-passing candidates; Lookup takes the request and applies Claim's replay rules (KeyConflict, KeyBusy, wait for bind) | Kubernetes (major) |
| Lease-loss duplicate window undocumented | Documented and bounded | Go |
| Busy Lookups → 504 | 429 with Retry-After; separate Lookup token bucket; consistent reads | Scalability |
| All candidates over rate → "per outcomes" | 429 with refill-time Retry-After | Scalability |
| Watch start point unspecified | From create's resourceVersion; 410 → GET; stream limits; FlowSchema covers get/watch | Kubernetes, Scalability |
| Slot release after the switch | `defer` per attempt | Go |
| Shutdown flag and counter unsynchronised | Same mutex | Go |
| `ErrNotFound` compared directly | `errors.Is` | Go |
| Half-open probe ignored 401/403 | Unhealthy + credentials reason | Kubernetes |
| 504 `{provider}` vs `{providers}` | Always a `providers` array | OSS |
| Proto zero value `UNKNOWN_OUTCOME` | `KIND_UNSPECIFIED = 0` handled as unknown (buf style) | OSS |
| `-plugins` image as a default | Volume mount is the default; image variant needs NOTICE + license + CVE tracking | OSS |
| Warm→cold trade-off and burst p90 unstated | Stated in 5.6 and section 9 | Scalability, OSS |

### v2 → v3 (round 2)

| v2 | v3 | Raised by |
|---|---|---|
| No kind for "same key, different spec" | `KindKeyConflict`: terminal, 409, no retry, no spill | Go (major) |
| Lookup trigger and failure rule undefined | `retry` request field; new claim only if all probed Lookups return `ErrNotFound`; inconclusive pins or 504 | Go (major) |
| Blocking slot wait on the chosen provider | Non-blocking `TryAcquire`; re-decide without spending a spill; block once only when all are full | Scalability (major) |
| Shared informer over all owned claims | Watch per in-flight claim, bounded by `maxInFlight`; no relist on failover | Scalability (major) |
| Deleting/expired claim → `Rejected` | `Busy`, pinned, short Retry-After | Kubernetes, Go |
| maxInFlight 256 | 512, cited from upstream measurements | Scalability, OSS |
| QPS ≥ maxInFlight | QPS sized as rate × calls per claim | Scalability |
| `maxRate` only for elastic, no default | All claimable providers; defaults 100/s elastic, 50/s cold | Scalability |
| Global cap counted claims only | Counts waiters too; 429 beyond it | Scalability |
| `ReleaseReservation` unconditional | Decrements only if a count was actually removed | Scalability |
| WaitGroup wait without timeout | Counter + done channel with deadline; `GracefulShutdownTimeout` sized | Go, Kubernetes |
| `Validate()` couldn't see max TTL | Core defaults TTL; `Validate(maxTTL)` | Go |
| `ErrNotFound`, `Report` alias, `KindOf(nil)` unspecified | Declared and documented | Go |
| Binder lookup of providers unspecified | Copy-on-write target map | Go |
| Half-open probe unspecified | Lookup of a never-used key | Scalability |
| Owner cluster UID needed RBAC | ClusterRole `get namespaces/kube-system` or `--cluster-id` | Kubernetes |
| `clusterProfileRef` underspecified | Namespace allowlist, RBAC, CRD-absent reason, plugin binaries | Kubernetes, OSS |
| Events RBAC namespaced only | ClusterRole `events.k8s.io` create/patch | Kubernetes |
| Ref without MinLength; Secret Role conditional | MinLength=1; unconditional Role | Kubernetes |
| Upstream pinned by commit; Daytona path; §9 uncited | v1.0.7; module path caveat; cited | OSS |
| Fleet adapter an open question | Recommended capacity source, own module | OSS |

### v1 → v2 (round 1)

| v1 | v2 | Raised by |
|---|---|---|
| `sandboxTemplateRef` on SandboxClaim | `warmPoolRef` (the real v1beta1 field) | OSS, Kubernetes |
| TTL via lifecycle, default policy | Always bounded `shutdownTime`, `shutdownPolicy: Delete` | OSS, Kubernetes |
| Replay on AlreadyExists | Spec-hash; `KeyConflict`; deleting/expired handled | Kubernetes |
| Optional interfaces by type assertion | `Provider{Source, Binder, Lister}` struct | Go |
| Types in `pkg/adapter` | Stdlib-only leaf `pkg/provider` | Go |
| Unclassified errors undefined | Zero `Kind` = `UnknownOutcome`; constructors; panics recovered | Go |
| Client disconnect cancels the attempt | Detached attempt; disconnect stops spill only | Go, Kubernetes |
| Full bulkhead = NoCapacity (409) | Busy, then 429 | Scalability |
| Reserve, then acquire slot; keep all reservations | Slot first; release reservations unless NoCapacity/UnknownOutcome | Scalability |
| Per-request spill on refusal | Global refusal TTL, spill cap, retry budget, jitter | Scalability |
| client-go defaults | Explicit QPS/burst; FlowSchema | Scalability, Kubernetes |
| Elastic = saturated warm | Score 20/100 + rate bound | Scalability |
| N consecutive failures | Windowed ratio + half-open probes | Scalability |
| `ListOwned` unpaged | Paged; reaper safety rules | Scalability, Kubernetes |
| Owner = release name | Hash incl. cluster ID | Kubernetes |
| Kubeconfig Secrets only | ClusterProfile first; exec/file auth rejected; rotation | OSS, Kubernetes |
| Templates as a map | List-map with validation | Kubernetes |
| `billing` added early | Dropped until used | Kubernetes |
| Private gateway `POST /claims` | Dropped; gRPC adapter protocol instead | OSS |
| No mention of KEP-1434 / ClusterProfile | Ecosystem section; fleet adapter | OSS |
| gRPC proto at phase 7 | Proto from phase 1 | OSS |
| `/v1alpha1/` tied to CRD | `/api/v1alpha1/`, deprecation policy | OSS, Kubernetes |
| — | Phase 0: Secret cache scope fix | Kubernetes |
| — | Dependency-bump phase for upstream clientset | OSS |
