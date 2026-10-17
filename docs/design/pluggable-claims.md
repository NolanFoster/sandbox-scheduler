# Pluggable claims: placing *and* provisioning sandboxes beyond Kubernetes

Status: **draft for review**. Nothing here is implemented.

## 1. Summary

Today the scheduler answers "where should this sandbox go?" (`POST /schedule`)
and the caller creates the sandbox. This plan adds an optional second step: the
scheduler makes the claim on the chosen provider and, if that provider refuses,
tries the next one, through a pluggable `Binder` interface that a Kubernetes
cluster and a hosted API (E2B, Daytona, Modal, Fly) can both implement.

Decide-only stays the default and stays unchanged. Claiming is opt-in per
deployment and per provider.

## 2. Goals and non-goals

Goals

1. One provider-neutral API to obtain a sandbox from any configured provider.
2. Automatic spill to the next-best provider when a claim is refused, with no
   duplicate sandboxes.
3. Providers plug in through a small, stable interface, first in-process (Go),
   later out-of-process (gRPC), without changing the core.
4. Keep the properties that exist today: placement decisions are in-memory and
   sub-millisecond; the scheduler stores no per-sandbox state; a provider's
   self-reported facts never drive placement.

Non-goals (this plan)

- Data plane: exec, file transfer, port access. Handles carry provider-native
  connection details through untouched.
- Suspend/resume. On agent-sandbox this is `Sandbox.spec.operatingMode` in the
  owning cluster and needs no placement.
- Lifecycle tracking or a placement database. The scheduler keeps no record of
  sandboxes it created.
- The marginal-cost scorer (sunk-cost capacity vs per-second billing). It is
  needed for cost-led multicloud placement but is a separate plan; this plan
  only adds the capacity fields it will need (section 5.4).

## 3. Current state (main @ 2bbbd7d)

- `pkg/adapter`: name-keyed registry (`Register(name, Factory)`), where
  `Factory func(Config) (registry.Source, error)` and `registry.Source` is
  `ProviderID() string` + `Fetch(ctx) (Report, error)`. One adapter exists:
  `agent-sandbox`, which reads `GET <endpoint>/warmpool` from an
  **operator-owned gateway** (not an upstream component) with a bearer token.
- `pkg/registry`: polls sources off the decision path; `Snapshot()` is a
  lock-free read; `TryReserveWarm(provider)` atomically reserves one warm
  sandbox; `RecordColdPlacement` feeds power-of-two-choices tie-breaking.
- `pkg/framework` / `pkg/plugins`: filter/score profile, pure functions.
- `pkg/scheduler`: `POST /schedule` decides, reserves (re-deciding when a
  reservation loses a race), responds with provider + endpoint. `GET
  /providers`. Bearer-token auth. Served by the elected leader only.
- API: `SandboxProvider` (cluster-scoped; adapter, endpoint, credentialsRef,
  attributes, costPerHour, refreshInterval, disabled) and
  `SandboxPlacementPolicy`, group `placement.agents.x-k8s.io/v1alpha1`.

## 4. Architecture

```
            POST /schedule (unchanged)            POST /v1alpha1/sandboxes (new)
                     │                                       │
                     ▼                                       ▼
   registry.Snapshot ─► framework.Profile ─► reserve ─► [binder loop] ─► adapter.Binder.Claim
                                                         │  error kind decides:
                                                         │  next candidate / same provider / fail
                                                         ▼
                                                      Handle{provider, id, endpoint, connect}
```

The decision path is shared and unchanged. The binder loop is new code in a new
package (`pkg/binder`) and is the only place that performs provider I/O on the
request path.

## 5. Design

### 5.1 Adapter interfaces (`pkg/adapter`)

`Source` (capacity) stays mandatory. Claiming is a set of **optional**
interfaces the same value may implement, discovered by type assertion (the
`http.Flusher` pattern), so an adapter can ship capacity-only first:

```go
// Binder claims and releases sandboxes on one provider.
//
// Claim must be idempotent on req.Key: a second call with the same key returns
// the same sandbox (or an equivalent "already exists" Handle), never a second
// one. An adapter that cannot guarantee this must not implement Binder.
type Binder interface {
    Claim(ctx context.Context, req ClaimRequest) (Handle, error)
    Release(ctx context.Context, h Handle) error
}

// OwnedLister lets the reaper find sandboxes this scheduler created. Optional;
// required only for providers without a native TTL.
type OwnedLister interface {
    ListOwned(ctx context.Context, owner string) ([]Handle, error)
}
```

Types are plain data so they map 1:1 onto protobuf later (section 5.10):

```go
type ClaimRequest struct {
    Template string            // provider-neutral name; mapped per provider (5.3)
    Key      string            // idempotency key, [a-z0-9-]{1,40}, from the caller
    Owner    string            // scheduler instance identity, for the reaper
    Labels   map[string]string // caller labels, validated + prefixed
    TTL      time.Duration     // 0 = provider default; upper bound on lifetime
}

type Handle struct {
    Provider  string            // SandboxProvider name
    ID        string            // provider-native identifier
    Endpoint  string            // where to connect, if the provider has one
    Connect   map[string]string // provider-native connection details (opaque)
    ExpiresAt *time.Time        // when the provider will reclaim it, if known
}
```

### 5.2 Error taxonomy

The binder loop is generic only if every adapter maps its failures onto a fixed
set of kinds. Adapters return `*adapter.ClaimError{Kind, Err}`; the core uses
`errors.As` and never inspects provider-specific errors.

| Kind | Meaning | Binder loop action | HTTP if final |
|---|---|---|---|
| `NoCapacity` | Pool empty, quota or rate limit | Mark provider empty in this request's candidate copy; next candidate | 409 with per-provider reasons |
| `Unavailable` | Failed **before** the request could have taken effect (dial, 5xx without side effect, timeout before send) | Next candidate; feed a passive health signal (5.8) | 503 |
| `Rejected` | Permanent for this request on this provider (unknown template, auth) | Next candidate; surface as config error in metrics and provider status | 422 if all rejected |
| `UnknownOutcome` | Timed out or connection lost **after** sending | Retry the **same provider with the same key** up to `--claim-retries` within the deadline; never spill | 504 with `provider` set, so a client retry can stay on it |

Rule of thumb for adapter authors: if you cannot prove the provider did not act,
it is `UnknownOutcome`.

### 5.3 Templates by reference

Callers name a template; each provider maps names to its native form. New
optional spec field:

```yaml
spec:
  templates:
    python-gvisor: python-gvisor          # agent-sandbox: SandboxTemplate name
    node-gvisor: node-gvisor
# on an E2B provider:
#   python-gvisor: 3k9x2...               # E2B template ID
```

- New filter plugin `TemplateAvailable`: drops candidates whose `templates`
  lacks the requested name. Added to the default profile only when a template
  is requested, so `/schedule` behaviour is unchanged.
- Template names follow DNS-1123 label rules; values are adapter-validated.
- Spec passthrough (callers sending a full sandbox spec) is explicitly out of
  scope: it forces the core to model every provider's spec format.

### 5.4 Capacity for elastic providers

Hosted APIs have no warm count; they accept or rate-limit. `registry.Report`
gains:

```go
Elastic bool // capacity is effectively unbounded; WarmCapacity is ignored
```

- Elastic providers appear in snapshots with `Elastic=true`; `WarmCapacity`
  scores them as a fixed configurable value (default: the saturation point,
  since a hosted API "has warm capacity" in the sense that matters — its start
  latency), and `TryReserveWarm` always succeeds.
- `SandboxProviderSpec.billing: Provisioned | PerUse` (default `Provisioned`)
  is added now, unused by scoring until the marginal-cost plan lands, so the
  CRD change happens once.

### 5.5 API

New endpoints, versioned in the path (the existing ones stay unversioned for
compatibility, and are aliased under `/v1alpha1/` too):

```
POST   /v1alpha1/sandboxes
  { "template": "python-gvisor", "key": "run-42-step-7",
    "labels": {...}, "requires": {...}, "preferProvider": "civo",
    "ttlSeconds": 3600, "timeoutSeconds": 30 }
  201 { "provider": "civo", "id": "...", "endpoint": "...", "connect": {...},
        "expiresAt": "...", "attempts": [ {provider, outcome, ms}, ... ],
        "explanation": "..." }
  200 same body when the key already existed on that provider (idempotent replay)

DELETE /v1alpha1/sandboxes/{provider}/{id}
  204 | 404 (already gone, treated as success by clients) | 502/503
```

- `key` is **required**. A request without one is rejected (400): claiming
  without idempotency is the one way this feature creates leaks.
- Cross-request idempotency without server state: on `UnknownOutcome` the 504
  body names the provider; the client SDK retries with `pinProvider` set, which
  turns the request into "claim on this provider or fail". This keeps the
  scheduler stateless at the cost of a client contract (open question 10.1).
- Auth: the same bearer token scheme, but a **separate token** for claim
  endpoints (`PLACEMENT_CLAIM_TOKEN`), since claiming spends money and
  `/schedule` does not. Per-tenant authorization is out of scope.

### 5.6 Binder loop (`pkg/binder`)

```
deadline := min(request timeout, --claim-timeout)
excluded := {}
loop:
  decision := profile.Schedule(candidates minus excluded)      // pure, in-memory
  if none: return 409/422/503 per collected outcomes
  reserve warm (existing TryReserveWarm / RecordColdPlacement)
  acquire per-provider in-flight slot (5.7) or treat as NoCapacity
  h, err := binder.Claim(ctx with per-attempt timeout, req)
  switch kind(err):
    nil:            return 201 h
    NoCapacity:     zero provider's warm in local copy; excluded += provider
    Unavailable:    excluded += provider; health.observe(provider, fail)
    Rejected:       excluded += provider; record config error
    UnknownOutcome: retry same provider, same key, until retries/deadline;
                    then return 504 {provider}
```

- Bounded: at most one attempt per provider plus `--claim-retries` on the
  provider with an unknown outcome; the deadline bounds wall time.
- A reservation taken for a refused claim is **kept** until the next poll (the
  existing conservative rule: under-count rather than over-place).
- Decisions remain explainable: the response lists every attempt and outcome.

### 5.7 Concurrency, limits, timeouts

- Per-provider in-flight semaphore (`spec.claims.maxInFlight`, default 64): a
  slow provider cannot absorb all request goroutines; a full semaphore counts
  as `NoCapacity` for that request (bulkhead).
- Global in-flight cap (`--claim-max-inflight`, default 1024); beyond it the
  endpoint sheds load with 429 + `Retry-After`.
- Per-attempt timeout per provider (`spec.claims.timeout`, default 10s);
  request deadline from `timeoutSeconds` capped by `--claim-timeout`.
- `http.Server` gains `WriteTimeout`/`IdleTimeout` sized above the claim
  deadline (today only `ReadHeaderTimeout` is set).
- All provider I/O uses the request context; client disconnect cancels the
  attempt (which an adapter must then report as `UnknownOutcome` if it had
  already sent).

### 5.8 Health from claims

A claim outcome is a fresher signal than a 10s poll. `registry` gains
`ObserveClaim(provider, ok bool)`; N consecutive `Unavailable` outcomes mark the
provider unreachable until the next successful poll or claim. This is a
demotion (Reachability scorer), consistent with the existing "stale demotes,
never removes" rule.

### 5.9 Ownership and cleanup

- Every claimed sandbox carries `placement.agents.x-k8s.io/owner=<owner>` and
  `.../key=<key>` (as labels, annotations or provider tags).
- Prefer provider-native expiry: agent-sandbox `SandboxClaim.spec.lifecycle`
  (shutdown time + policy); hosted APIs' timeout parameters.
- For providers without native expiry, a leader-only reaper uses `OwnedLister`
  to delete owned sandboxes past `ExpiresAt`. The reaper never deletes anything
  without the owner label, and has a dry-run mode.
- `Owner` defaults to the Helm release fullname, so two scheduler installs never
  reap each other's sandboxes.

### 5.10 Out-of-process adapters (later phase)

- A `grpc` adapter that speaks a small protobuf service mirroring 5.1/5.2
  (`Fetch`, `Claim`, `Release`, `ListOwned`) to a sidecar over a Unix socket;
  `spec.adapter: grpc`, `spec.endpoint: unix:///var/run/adapters/e2b.sock`.
- Each sidecar holds only its own provider's credentials — the main reason to
  do this, since claiming otherwise concentrates write credentials for every
  provider in one process.
- Versioned proto package (`adapter.v1`), capability negotiation via a
  `GetInfo` RPC (supports_claim, supports_list_owned, elastic).

### 5.11 agent-sandbox Binder

The existing adapter reads an operator-owned gateway, not the Kubernetes API.
Two transports, chosen per provider by an option:

- **gateway** (default): requires the gateway to expose
  `POST /claims {template, key, labels, ttlSeconds}` and `DELETE /claims/{id}`.
  The gateway is the operator's code; this plan specifies the contract and
  ships a reference handler, not a gateway.
- **kubeapi**: credentials Secret holds a kubeconfig; the adapter creates a
  `SandboxClaim` (`extensions.agents.x-k8s.io/v1beta1`) named
  `sc-<hash(key)>` in a configured namespace with `sandboxTemplateRef` from the
  template mapping and `lifecycle` from `TTL`. `AlreadyExists` on create =
  idempotent replay (fetch and return). Uses a typed client from upstream's
  generated clientset, not dynamic unstructured objects.

`Claim` returns when the claim is **accepted** (created and bound to a
sandbox), not when the sandbox is Ready; `Connect` carries the router headers
(`X-Sandbox-ID`, `X-Sandbox-Namespace`). Waiting for readiness is the caller's
job, against the provider, as today.

## 6. Phases

Each phase is one PR, independently shippable, with `--enable-claims` (default
false) gating everything user-visible until phase 4.

1. **Interfaces and taxonomy.** `Binder`, `OwnedLister`, `ClaimRequest`,
   `Handle`, `ClaimError` + kinds in `pkg/adapter`; a fake binder for tests;
   docs for adapter authors. No behaviour change.
2. **API fields.** `spec.templates`, `spec.claims{enabled,maxInFlight,timeout}`,
   `spec.billing`; `Report.Elastic`; `TemplateAvailable` filter; CRD regen.
   Additive only.
3. **Binder loop and endpoints.** `pkg/binder`, `POST/DELETE /v1alpha1/sandboxes`,
   limits, metrics (`claim_attempts_total{provider,outcome}`,
   `claim_duration_seconds{provider}`, `claim_spills_total`), health from
   claims. Leader-only, like `/schedule`.
4. **agent-sandbox Binder** (kubeapi transport first; gateway contract
   documented). Flip `--enable-claims` default only after a real-cluster test.
5. **First hosted adapter** (E2B or Daytona): capacity (`Elastic`) + `Binder` +
   native TTL. Revise 5.1/5.2 if it does not fit — before declaring the
   interface stable.
6. **Reaper** for providers without native expiry.
7. **gRPC adapter protocol** and sidecar packaging in the chart.

Phase 5 is the validation gate: the interface is not called stable until a
non-Kubernetes provider implements it.

## 7. Testing

- Unit: binder loop table tests over every error kind and combination, with a
  fake clock and fake binders; idempotent replay; deadline expiry mid-loop;
  semaphore exhaustion.
- Contract test suite in `pkg/adapter/adaptertest` that every adapter runs:
  idempotency on key, error-kind classification for injected faults, Release of
  an unknown ID returns success-equivalent.
- agent-sandbox kubeapi: envtest with upstream CRDs installed.
- Race detector on the binder under concurrent claims.
- Benchmark: decision path latency must not regress (`BenchmarkSchedule`).

## 8. Compatibility and migration

- `/schedule` and `/providers` unchanged; all CRD changes additive within
  `v1alpha1`; unknown adapters still reported, not rejected.
- `--enable-claims=false` default; claim endpoints 404 when disabled.
- RBAC: kubeapi transport needs no new in-cluster RBAC (it uses remote
  kubeconfigs from Secrets already readable via `credentialsRef`).

## 9. Scalability notes

- Decision path untouched: still memory-only.
- Claim throughput is bounded by provider latency × in-flight limits, not CPU;
  with 64 in flight per provider and ~150 ms warm claims that is ~400 claims/s
  per provider, above upstream's measured ~300 warm claims/s per cluster.
- No per-sandbox state in the scheduler; the reaper lists owned sandboxes per
  provider, paged, on the leader only.
- Leader-only binding is a phase 3 simplification. Because providers arbitrate
  claims, the binding endpoint could later run active-active, with reservations
  as a per-replica hint; deferred until measured need.

## 10. Open questions

1. Cross-request idempotency: is `pinProvider` on client retry acceptable, or
   should adapters implement `Lookup(key)` so the scheduler can probe all
   binders on a replayed key (N calls, no client contract)?
2. Should `Rejected` spill at all, or fail fast so config errors are loud?
3. Should `Handle.Connect` be typed per adapter (a versioned schema) rather
   than an opaque map?
4. Should templates be their own cluster-scoped CRD (`SandboxProfile`) instead
   of a map on each provider, to avoid repeating mappings?
5. Is a separate claim token enough, or does claiming need per-tenant quotas
   before it is enabled anywhere shared?
