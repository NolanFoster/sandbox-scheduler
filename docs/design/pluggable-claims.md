# Pluggable claims: placing *and* provisioning sandboxes beyond Kubernetes

Status: **draft v3, revised after two review rounds** (OSS compatibility,
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
    // Lookup returns the sandbox previously claimed with key, ErrNotFound if
    // there is none, or another error if it cannot tell.
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
    Key      string            // idempotency key
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
    KindBusy        // rate-limited or transiently unable; honour RetryAfter
    KindKeyConflict // key already used with a different spec; terminal
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
| `UnknownOutcome` | Sent, outcome unknown; or unclassified | Pin to this provider: `Lookup`, then retry same key within budget; never spill | 504 `{provider}` |
| `KeyConflict` | Same key exists with a different spec | Stop: no retry, no spill (either would create a second sandbox under the key) | 409 `KeyConflict` |
| `NoCapacity` | Provider confirmed it cannot serve | Mark provider refused **globally** for `--refusal-ttl` (1s); next candidate | 409 with reasons |
| `Busy` | Provider rate limit, or the key's previous sandbox is still being deleted | Cooldown for `RetryAfter`; next candidate, except a deleting key (pin, retry after) | 429 + `Retry-After` |
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
  422 all rejected | 429 busy (+Retry-After, jittered) | 504 unknown {provider}

DELETE /api/v1alpha1/sandboxes/{provider}/{id}
  204 | 404 (gone; success for clients)
```

- `key` required; claims without idempotency are rejected (400).
- `retry: true` marks a client retry of a request whose outcome it did not
  learn (timeout, connection reset); the client SDK sets it on every retry.
  `pinProvider` is set from a 504's `provider` field when the client has one.
- Separate token `PLACEMENT_CLAIM_TOKEN` (existing-Secret pattern,
  constant-time compare).
- Idempotency window = lifetime of the provider object. Documented: a replay
  after the sandbox expired creates a new one.

### 5.6 Binder loop (`pkg/binder`)

**Lookup rule (no duplicates without server state).** A request may create a
new sandbox only if it is a first attempt, or if every claim-capable candidate
it probes returns `ErrNotFound`:

```
if pinProvider != "":
    h, err := Lookup(pinProvider, key)
    found → 200; ErrNotFound → claim only on pinProvider; other err → 504 {pinProvider}
elif retry:
    probe Lookup on all claim-capable candidates in parallel (bounded, per-attempt timeout)
    any found        → 200 that handle
    all ErrNotFound  → continue as a first attempt
    any inconclusive → pin to that provider (claim there with the same key);
                       if more than one is inconclusive → 504 {providers}
```

**Claim loop.**

```
reqDeadline := min(timeoutSeconds, --claim-timeout)
excluded := {}; spills := 0
loop while spills < --max-spill (3) and before reqDeadline:
  if client gone: stop spilling (an in-flight attempt still completes)
  decision := Schedule(candidates − excluded − globally refused − over maxRate)
  if none:
      if every remaining exclusion is "slots full": wait for any slot (5.7) once
      else return per collected outcomes
  if !TryAcquire(slot of decision.Provider):          // non-blocking
      excluded += provider (locally, slots full); continue  // costs no spill
  reserve warm/cold (existing ledger)                  // after the slot
  h, err := Claim(attempt ctx, req)                    // 5.7
  if err == nil: return 201 h
  switch KindOf(err) { ... per 5.2; spill kinds do spills++ ... }
  release slot
```

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
  a `metadata.name` field selector, opened after create and closed on bind or
  timeout, falling back to a single GET; bounded by `maxInFlight`. No informer
  over all owned claims, so memory is per in-flight claim, not per live sandbox,
  and leader failover relists nothing. An example FlowSchema for the
  scheduler's remote identity is shipped (upstream's APF overlay covers only its
  own controller).
- **Shutdown**: detached attempts are tracked by a counter plus a done channel
  (`wg.Go` is not called once shutdown begins; shutdown `select`s on the done
  channel against the claim deadline, since `WaitGroup.Wait` has no timeout).
  HTTP shutdown timeout and `ctrl.Options.GracefulShutdownTimeout` = claim
  timeout + 5s; the chart sets `terminationGracePeriodSeconds` ≥ preStop +
  claim timeout + 10s and a native `preStop.sleep` of 5s (the image has no
  shell).

### 5.8 Health from claims

Claim health is tracked separately from poll health: a windowed error ratio
(≥ 50% `Unavailable` over ≥ 20 attempts) demotes the provider's claim path.
Recovery is through **half-open probes**: a `Lookup` of a random, never-used key
(cheap and side-effect free; `ErrNotFound` counts as healthy), with exponential
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
  proto enum whose zero value is `UNKNOWN_OUTCOME`; published as its own Go
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
     run credential plugins (e.g. `gke-gcloud-auth-plugin`, `kubelogin`); these
     ship in an `-plugins` image variant or are mounted from a volume, and are
     configured by the operator in the deployment, never by a Secret.
  2. Workload-identity federation.
  3. **Kubeconfig Secret** (fallback). Parsed with `clientcmd.Load` and rejected
     if it uses `exec`, `auth-provider`, `tokenFile` or client-certificate file
     paths: these would let anyone who can write the Secret run binaries or read
     the scheduler's own token. The exec ban applies to Secret-supplied
     kubeconfigs only. The client is rebuilt when the Secret's resourceVersion
     changes, closing the old transport.
  Remote RBAC: namespaced Role, `sandboxclaims: create,get,list,watch,delete`.
- **Options** are typed (`spec.kubeapi.namespace`), not annotations.
- **Claim**: `SandboxClaim` named `sc-` + first 20 hex of
  sha256(owner + "/" + key) (starts with a letter, ≤ 63, DNS-1035-safe for the
  derived Sandbox/Service names), `warmPoolRef` from the template mapping,
  lifecycle per 5.9, owner/key labels, spec-hash annotation.
- **Replay** on `AlreadyExists`: live claim with matching spec-hash → 200;
  different hash → `KeyConflict` (409); claim being deleted, or past its
  `shutdownTime` and about to be → `Busy` with a short `RetryAfter`, pinned to
  this provider, so the key is not reused elsewhere while its sandbox still
  exists.
- **Lookup** = GET of the deterministic claim name (cheap; no list).
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
  retry, no spill) and inconclusive `Lookup` on retry (pins, never claims
  elsewhere); mixed outcomes; deadlines and backoff under `testing/synctest`;
  concurrent same-key claims under `-race` asserting at most one sandbox; a
  burst against one full provider spreads to others without spending spills;
  all-full returns 429, never 409.
- `ReleaseReservation` after a poll already cleared the reservation leaves the
  ledger unchanged (property test over interleavings).
- `adaptertest.TestBinder(t *testing.T, newHarness func(*testing.T) Harness)`
  modelled on `testing/fstest`; the harness injects faults the suite cannot
  reach generically (no capacity, refuse-before-send, drop-after-send, auth,
  key conflict, deleting key).
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
  (≈300/s warm, 70–85/s per pool refill) is the limit, not the scheduler.
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
  documented): chose `Busy` pinned to the provider instead (Kubernetes review),
  since spilling would create a second sandbox under the same key while the
  first still exists.

## 12. Changes

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
