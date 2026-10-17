# Pluggable claims: placing *and* provisioning sandboxes beyond Kubernetes

Status: **draft v2, revised after review** (OSS compatibility, scalability,
Kubernetes practices, Go practices). Nothing here is implemented. Section 12
records what changed from v1 and why.

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
   scheduler stores no per-sandbox state; self-reported provider facts never
   drive placement.

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
  atomic `TryReserveWarm`; `RecordColdPlacement` for power-of-two-choices.
- `pkg/scheduler`: `POST /schedule`, `GET /providers`; bearer token; leader-only
  serving via pod label.
- API group `placement.agents.x-k8s.io/v1alpha1` (see 10.1).
- Module pins k8s.io/* v0.36 and controller-runtime v0.24.1; upstream
  agent-sandbox (`sigs.k8s.io/agent-sandbox` @ e0b002e) requires v0.37.1 and
  v0.25.1.

**Existing issue found in review, fixed first (phase 0):** the chart's
ClusterRole grants cluster-wide `get/list/watch` on Secrets and the manager sets
no cache restriction, so the reconciler's cached Secret reads start a
cluster-wide Secret informer: every Secret in the cluster is held in memory by
a process that serves HTTP, although only `--secret-namespace` is ever read.

## 4. Ecosystem alignment

| Upstream work | Relationship |
|---|---|
| agent-sandbox v1beta1 (`SandboxClaim`, `SandboxWarmPool`) | The `agent-sandbox` binder creates `SandboxClaim`s with `warmPoolRef`; reuses the Go SDK's deterministic `ClaimName` + `AdoptExisting` convention. |
| KEP-1434 multi-cluster fleet (implementable) | 1434 decides **which cluster holds which warm pools** (minutes); this project decides **which provider serves each request** (milliseconds), across clusters *and* hosted APIs. A `fleet` capacity adapter reads 1434 capacity reports instead of a private gateway endpoint. Positioned as the cross-provider router above 1434, not an alternative. |
| SIG-Multicluster ClusterProfile (KEP-4322) | Preferred source of cluster access for the kubeapi transport (`spec.clusterProfileRef`, access providers / credential plugins). Raw kubeconfig Secrets are a fallback. |
| Substrate | Named as a target provider (capacity = idle-worker headroom from `ate-api-server`; claim = create actor). Its out-of-process `objectstore.v1` sidecar is precedent for 5.10. |

The custom gateway `POST /claims` contract from v1 is dropped as a first-class
transport; operators with a gateway can implement the gRPC adapter protocol
(5.10) instead of a second bespoke HTTP contract.

## 5. Design

### 5.1 Adapter contract (`pkg/provider`, new leaf package)

A new package with **standard-library imports only**, so out-of-tree adapters
depend on a small surface and later changes cannot create import cycles
(`pkg/adapter` imports `pkg/registry` today). `pkg/registry` keeps
`type Source = provider.Source` for compatibility.

Capabilities are explicit fields, not discovered by type assertion: a decorator
(metrics, retries) wrapping a `Source` would silently hide an asserted
`Binder`.

```go
// Package provider is the contract between the scheduler and a provider
// adapter.
//
// Experimental: this API may change incompatibly until a non-Kubernetes
// adapter has implemented it (see docs/design/pluggable-claims.md, phase 5).
package provider

type Provider struct {
    Source Source      // required: capacity
    Binder Binder      // nil: decide-only provider
    Lister OwnedLister // nil: relies on provider-native expiry
}

type Binder interface {
    // Claim must be idempotent on req.Key: repeating it returns the same
    // sandbox. Implementations that cannot guarantee this must not be Binders.
    Claim(ctx context.Context, req ClaimRequest) (Handle, error)
    // Lookup returns the sandbox previously claimed with key, or ErrNotFound.
    // It is what makes failover and client retries safe without server state.
    Lookup(ctx context.Context, key string) (Handle, error)
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
    Key      string            // idempotency key, validated by Validate
    Owner    string            // scheduler instance identity (5.9)
    Labels   map[string]string // caller labels; nil and empty are equivalent
    TTL      time.Duration     // always > 0 after Validate (5.9)
}

func (r ClaimRequest) Validate() error // key regex, label domains, TTL bounds

type Handle struct {
    Provider  string
    ID        string
    Endpoint  string
    Connect   map[string]string // provider-native, documented per adapter
    ExpiresAt time.Time         // zero = unknown
}
```

`Factory` becomes `func(Config) (provider.Provider, error)`, changed now while
one in-tree adapter exists. No JSON tags on these types; wire types live in
`pkg/scheduler`. The same messages are defined in `proto/adapter/v1alpha1`
from phase 1 so in-process and gRPC adapters share one contract.

### 5.2 Error taxonomy

```go
type Kind int

const (
    KindUnknownOutcome Kind = iota // zero value: the safe default
    KindNoCapacity
    KindUnavailable
    KindRejected
    KindBusy        // provider rate-limited us; honour RetryAfter
)

type ClaimError struct {
    Kind       Kind
    RetryAfter time.Duration
    Err        error
}

func NoCapacity(err error) error  // constructors so authors never hand-build
func Unavailable(err error) error
func Rejected(err error) error
func Busy(after time.Duration, err error) error
func KindOf(err error) Kind       // errors.As; anything unclassified → KindUnknownOutcome
```

**Unclassified errors, a missing `Kind`, and panics in `Claim` are all
`UnknownOutcome`**, the one kind that never spills and so never duplicates.

| Kind | Meaning | Binder loop | Final HTTP |
|---|---|---|---|
| `UnknownOutcome` | Sent, outcome unknown; or unclassified | `Lookup`, then retry same provider + key within budget; never spill | 504 `{provider}` |
| `NoCapacity` | Provider confirmed it cannot serve | Mark provider refused **globally** for `--refusal-ttl` (1s); next candidate | 409 with reasons |
| `Busy` | Provider rate limit (e.g. 429) | Cooldown for `RetryAfter`; next candidate | 429 + `Retry-After` |
| `Unavailable` | Failed provably before taking effect | Release reservation; health signal (5.8); next candidate | 503 |
| `Rejected` | Permanent for this provider (unknown template, auth) | Release reservation; config-error metric + `Claiming` condition; next candidate | 422 if all rejected |

agent-sandbox never refuses for capacity: an empty warm pool cold-starts the
claim (`sandboxclaim_controller.go`, "Cold path"). For that adapter, warm vs
cold is decided by the existing reservation before the claim, and a cold start
is a success. Requests may set `"warmOnly": true`, in which case a provider
whose reservation fails is skipped without a claim.

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
    // +kubebuilder:validation:MaxLength=253
    Ref string `json:"ref"`
}
```

- Mappings stay on the provider (they are inherently per provider; a separate
  CRD adds a cross-object reference for little gain).
- New filter `TemplateAvailable`, added to the profile only when a template is
  requested, so `/schedule` is unchanged.

### 5.4 Elastic providers

`provider.Report` gains `Elastic bool`. Elastic providers:

- score `spec.claims.elasticWarmScore` (default **20 of 100**), not saturation:
  at saturation an elastic provider (5×100+3×0+3×100 = 800) beat every cold
  cluster (≤ 600) at any price and absorbed all overflow at per-use rates;
- are rate-bounded by `spec.claims.maxRate` (token bucket), so they take a
  bounded share of a burst;
- record cold placements (they are cold starts), so spreading between elastic
  providers works.

### 5.5 HTTP API

Versioned separately from the CRD: `/api/v1alpha1/...`. The unversioned
`/schedule` and `/providers` are aliased under it and kept until v1beta1, with
removal announced one minor release ahead.

```
POST /api/v1alpha1/sandboxes
  { "template", "key", "labels", "requires", "preferProvider", "pinProvider",
    "warmOnly", "ttlSeconds", "timeoutSeconds" }
  201 created | 200 idempotent replay (same key, same spec)
  409 no capacity | 409 KeyConflict (same key, different spec)
  422 all rejected | 429 busy (+Retry-After, jittered) | 504 unknown {provider}

DELETE /api/v1alpha1/sandboxes/{provider}/{id}
  204 | 404 (gone; success for clients)
```

- `key` required; claims without idempotency are rejected (400).
- Separate token `PLACEMENT_CLAIM_TOKEN` (existing-Secret pattern,
  constant-time compare).
- Idempotency window = lifetime of the provider object. Documented: a replay
  after the sandbox expired creates a new one.

### 5.6 Binder loop (`pkg/binder`)

```
reqDeadline := min(timeoutSeconds, --claim-timeout)
if retry or pinProvider: h := Lookup(key) on pinned/all claim-capable
                         candidates (parallel, bounded); found → 200
for attempt := 0; attempt < --max-spill (3); attempt++:
  if client gone: stop spilling (an in-flight attempt still completes)
  decision := Schedule(candidates − excluded − globally refused)
  if none: return per collected outcomes
  slot := acquire provider slot, waiting until reqDeadline     // bulkhead
           timeout → excluded += provider (busy, not empty)
  reserve warm/cold (existing ledger)                           // after the slot
  h, err := Claim(attempt ctx, req)                             // 5.7
  switch KindOf(err) { ... per 5.2 ... }
  release slot
if every exclusion was busy: 429 with jittered Retry-After
```

- Slot before reservation: refusals that never reach a provider no longer
  drain its ledger.
- New `registry.ReleaseReservation(provider)` (CAS decrement of the ledger
  state and bucket) for `Busy`, `Unavailable`, `Rejected` and bulkhead
  timeouts; reservations are kept only for `NoCapacity` and `UnknownOutcome`.
- A provider-confirmed refusal is recorded **globally** for `--refusal-ttl`, so
  a fleet-wide shortage costs one refused call per provider per TTL, not one per
  request per provider.
- Retries (`UnknownOutcome`) use full-jitter exponential backoff (100 ms base,
  2 s cap) and a per-provider retry budget (≤ 10% of claims, token bucket).

### 5.7 Concurrency, timeouts, transports, shutdown

- Each attempt runs on `context.WithTimeout(context.WithoutCancel(reqCtx),
  perAttempt)`: a client disconnect stops spilling but never abandons a claim
  mid-flight (the key makes the result recoverable via replay).
- Detached attempts are tracked in a `sync.WaitGroup`; shutdown and
  leadership loss wait up to the claim deadline.
- Per-attempt timeout default **3s** (`spec.claims.timeout`, CEL-bounded
  1s–5m).
- `spec.claims.maxInFlight` default sized from measured latency: upstream
  measures claim p90 of 0.2s (warm burst) to ~1.0s (sustained with refill), so
  ~300 claims/s per cluster needs ≈ 300 × 1.0 × 1.5 ≈ **450** in flight at the
  slow end. Default 256, bounds 1–4096; an adaptive (AIMD) limit is a later
  option.
- Global cap `--claim-max-inflight` (default 2048) → 429.
- Per-provider transports: client-go QPS/burst set ≥ maxInFlight
  (`--remote-qps`, `--remote-burst`; defaults 5/10 would cap claims at ~5/s);
  hosted APIs get their own `http.Transport` with `MaxIdleConnsPerHost =
  MaxConnsPerHost = maxInFlight` and HTTP/2 where offered.
- Kubernetes: binding detected via one shared, owner-label-filtered informer
  per remote namespace on the leader, not a GET or watch per claim; an example
  FlowSchema for the scheduler's remote identity is shipped (upstream's APF
  overlay covers only its own controller).
- Shutdown: HTTP shutdown timeout = claim timeout + 5s; chart sets
  `terminationGracePeriodSeconds` ≥ preStop + claim timeout + 10s and a native
  `preStop.sleep` of 5s (the image has no shell).

### 5.8 Health from claims

Claim health is tracked separately from poll health: a windowed error ratio
(≥ 50% `Unavailable` over ≥ 20 attempts) demotes the provider's claim path;
recovery is through half-open probes with exponential backoff, not the next
poll, so a healthy `/warmpool` cannot flap a broken claim path every 10s.
Readiness never depends on claim outcomes.

### 5.9 Ownership, expiry and cleanup

- **Owner** = short hash of (release fullname, namespace, `kube-system`
  namespace UID); the readable name goes in an annotation. Two installs with the
  default release name in different clusters cannot reap each other.
- Owner and key are **labels on the provider object only** (selectors);
  `placement.agents.x-k8s.io/spec-hash` (template, labels, TTL bucket) is an
  annotation. Caller labels that reach pods must use upstream's allowlisted
  domain (default `sandbox.users.io`) and are validated up front.
- **Expiry is always bounded**: `TTL` defaults to and is capped by
  `--claim-max-ttl`. agent-sandbox claims always set `lifecycle.shutdownTime =
  now + TTL`, `shutdownPolicy: Delete` (upstream defaults to `Retain`, which
  leaves claims forever) and `ttlSecondsAfterFinished`.
- **Reaper** only for providers without native expiry: leader-only runnable;
  paged `ListOwned` (500/page, server-side selector, metadata-only on
  Kubernetes); per-object deletes with UID/resourceVersion preconditions, never
  DeleteCollection; skips objects already deleting; token-bucket rate limit and
  `--reap-max-per-cycle` circuit breaker; server-side `DryRun: All` mode with an
  Event. No finalizers.

### 5.10 Out-of-process adapters

- `proto/adapter/v1alpha1`: `GetInfo` (capabilities: claim, lookup, list,
  elastic), `Fetch`, `Claim`, `Lookup`, `Release`, `ListOwned`; published as its
  own Go module once stable; deprecation policy stated before v1.
- `spec.adapter: grpc`, `spec.endpoint: unix:///var/run/adapters/<name>.sock`.
- Pod hardening so each sidecar holds only its own credentials:
  `automountServiceAccountToken: false` with a projected token for the
  scheduler container only; sidecar Secrets outside the namespace the scheduler
  can read; socket in an `emptyDir`, mode 0660, distinct `runAsUser` per
  sidecar; the chart's restricted `securityContext` copied.
- `adaptertest` is importable and can run against a socket, so third parties
  can certify adapters.

### 5.11 agent-sandbox binder

- **Access**, in order of preference: ClusterProfile access providers →
  workload-identity federation → kubeconfig Secret. Kubeconfigs are parsed with
  `clientcmd.Load` and rejected if they use `exec`, `auth-provider`,
  `tokenFile` or client-certificate file paths. The client is rebuilt when the
  Secret's resourceVersion changes, closing the old transport. Remote RBAC:
  namespaced Role, `sandboxclaims: create,get,list,watch,delete`.
- **Options** are typed (`spec.kubeapi.namespace`), not annotations.
- **Claim**: `SandboxClaim` named `sc-` + first 20 hex of
  sha256(owner + "/" + key) (starts with a letter, ≤ 63, DNS-1035-safe for the
  derived Sandbox/Service names), `warmPoolRef` from the template mapping,
  lifecycle per 5.9, owner/key labels, spec-hash annotation.
- **Replay** on `AlreadyExists`: live claim with matching spec-hash → 200;
  different hash → 409 `KeyConflict`; deleting or expired → `Rejected`.
- **Bound** when `status.sandbox.name` is set (there is no Bound condition);
  `Connect` carries `X-Sandbox-ID` (the Sandbox name) and
  `X-Sandbox-Namespace`.
- **Dependency**: importing upstream's generated clientset pulls the root module
  `sigs.k8s.io/agent-sandbox` and upgrades this repo to k8s.io v0.37 /
  controller-runtime v0.25. Done as its own PR pinned to a tagged upstream
  release, with a version matrix in the README.

## 6. Phases

Each phase is one PR. `--enable-claims` stays off by default until after
phase 5.

0. **Secret scope fix** (independent, ship now): namespace-scoped Secret cache
   (`cache.Options.ByObject`) and a namespaced Role instead of the cluster-wide
   Secret rule.
1. **Contract**: `pkg/provider` (types, `Kind`, constructors, `KindOf`,
   `Validate`), `Factory` returns `Provider`, `proto/adapter/v1alpha1`,
   `adaptertest` skeleton, fake binder. No behaviour change.
2. **API fields**: `templates` list-map, `claims{enabled, maxInFlight, timeout,
   maxRate, elasticWarmScore}`, `clusterProfileRef`, `kubeapi{namespace}`;
   `Claiming` condition + `status.claims`; printer columns; status writes on the
   existing 15s loop via merge patch; `TemplateAvailable` filter.
3. **Binder loop and endpoints**: `pkg/binder`, `/api/v1alpha1/sandboxes`,
   bulkheads, refusal TTL, retry budget, `ReleaseReservation`, health from
   claims, metrics (`claim_attempts_total{provider,kind}`,
   `claim_duration_seconds{provider}`, `claim_spills_total`), shutdown sizing.
4. **Dependency bump** to upstream's k8s/controller-runtime versions.
5. **agent-sandbox binder** (5.11) + envtest with upstream CRDs + a real-cluster
   run.
6. **First hosted adapter** (Daytona has an Apache-2.0 Go SDK; E2B needs a
   hand-written REST client), out-of-tree module or sidecar so vendor
   dependencies stay out of the core. **Interface stability gate.**
7. **Reaper.**
8. **gRPC adapter host** and chart sidecar packaging; `fleet` (KEP-1434)
   capacity adapter.

## 7. Testing

- Binder loop: table tests over every `Kind` and mixed outcomes; deadlines and
  backoff under `testing/synctest`; concurrent same-key claims under `-race`
  asserting at most one sandbox; bulkhead saturation returns 429, never 409.
- `adaptertest.TestBinder(t *testing.T, newHarness func(*testing.T) Harness)`
  modelled on `testing/fstest`; the harness injects faults the suite cannot
  reach generically (no capacity, refuse-before-send, drop-after-send, auth).
- `FuzzValidateKey`, `FuzzLabels`.
- agent-sandbox: envtest with upstream CRDs; replay with matching and
  mismatched spec-hash; claims on an empty pool (cold success).
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
- Claim throughput per provider is λ = in-flight / latency. With upstream's
  measured p90 of 0.2–1.0s, 256 slots give ~250–1,250 claims/s of headroom; the
  cluster (≈300/s warm, 70–85/s per pool refill) is the limit, not the
  scheduler.
- Leader memory: ~50KB per in-flight claim; 10K in flight ≈ 0.5GB.
- Shortage amplification is bounded: one refused call per provider per
  `--refusal-ttl`, at most `--max-spill` attempts per request, retries within a
  10% budget.
- Leader-only binding is acceptable for throughput; for availability, `Lookup`
  makes failover safe (a client retry after a reset finds the original sandbox
  instead of creating a second). Active-active later: reservations as
  per-replica hints, per-provider limits divided by replica count, keys routed
  by consistent hash or recovered by `Lookup`.

## 10. Open questions

1. **API group.** `placement.agents.x-k8s.io` sits in upstream agent-sandbox's
   namespace and implies endorsement; upstream could add a `placement` subgroup.
   Options: seek sponsorship from agent-sandbox/SIG-Apps, or rename now (e.g.
   `placement.sandbox-scheduler.io`) while v1alpha1 makes it cheap. **Owner
   decision needed before phase 2.**
2. `Lookup` fan-out on retries with no `pinProvider`: bounded to claim-capable
   candidates in parallel; acceptable cost, or require clients to send
   `pinProvider` from the 504?
3. Per-tenant quotas before enabling claims anywhere shared, or is the separate
   claim token enough for now?
4. Should the `fleet` (KEP-1434) adapter replace the gateway `/warmpool`
   adapter as the recommended agent-sandbox capacity source?

## 11. Review findings not adopted

- *Separate Go module for the adapter contract now* (OSS): deferred until the
  contract is declared stable at phase 6; a v0 single module avoids release
  overhead while it still changes. The package is marked experimental instead.
- *Waiting for a bulkhead slot until the request deadline* (scalability) is
  adopted, but capped by `--claim-queue-timeout` (default 1s) so a saturated
  provider cannot hold request goroutines for the full claim timeout.

## 12. Changes from v1

| v1 | v2 | Raised by |
|---|---|---|
| `sandboxTemplateRef` on SandboxClaim | `warmPoolRef` (the real v1beta1 field) | OSS, Kubernetes |
| TTL via lifecycle, default policy | Always bounded `shutdownTime`, `shutdownPolicy: Delete` | OSS, Kubernetes |
| Replay on AlreadyExists | Spec-hash; `KeyConflict`; deleting/expired → Rejected | Kubernetes |
| Optional interfaces by type assertion | `Provider{Source, Binder, Lister}` struct | Go |
| Types in `pkg/adapter` | Stdlib-only leaf `pkg/provider` | Go |
| Unclassified errors undefined | Zero `Kind` = `UnknownOutcome`; constructors; panics recovered | Go |
| Client disconnect cancels the attempt | Detached attempt; disconnect stops spill only | Go, Kubernetes |
| Full bulkhead = NoCapacity (409) | Busy: bounded wait, then 429 | Scalability |
| Reserve, then acquire slot; keep all reservations | Slot first; release reservations unless NoCapacity/UnknownOutcome | Scalability |
| Per-request spill on refusal | Global refusal TTL, spill cap, retry budget, jitter | Scalability |
| maxInFlight 64, timeout 10s, ~400/s claim | 256 / 3s, restated from measured p90 | Scalability |
| client-go defaults | QPS/burst ≥ maxInFlight; shared informer; FlowSchema | Scalability, Kubernetes |
| Elastic = saturated warm | Score 20/100 + rate bound | Scalability |
| N consecutive failures | Windowed ratio + half-open probes | Scalability |
| `ListOwned` unpaged | Paged; reaper safety rules | Scalability, Kubernetes |
| Owner = release name | Hash incl. cluster UID | Kubernetes |
| Kubeconfig Secrets only | ClusterProfile first; exec/file auth rejected; rotation | OSS, Kubernetes |
| Templates as a map | List-map with validation | Kubernetes |
| `billing` added early | Dropped until used | Kubernetes |
| Private gateway `POST /claims` | Dropped; gRPC adapter protocol instead | OSS |
| No mention of KEP-1434 / ClusterProfile | Ecosystem section; fleet adapter | OSS |
| gRPC proto at phase 7 | Proto from phase 1 | OSS |
| `/v1alpha1/` tied to CRD | `/api/v1alpha1/`, deprecation policy | OSS, Kubernetes |
| — | Phase 0: Secret cache scope fix | Kubernetes |
| — | Phase 4: k8s/controller-runtime bump for upstream clientset | OSS |
