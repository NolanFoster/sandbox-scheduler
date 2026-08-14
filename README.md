# sandbox-scheduler

Placement for agent sandboxes across multiple clusters and providers.

> **Status: early.** The scheduling framework and its built-in policies work and
> are tested. The CRDs, controller and provider adapters are not written yet.
> Interfaces will change.

## What this is

[Agent Sandbox](https://github.com/kubernetes-sigs/agent-sandbox) gives you
isolated, stateful sandboxes in *one* cluster, and `sandbox-router` proxies
traffic to them. Neither answers a question that shows up as soon as you have
more than one place to run things:

**Which cluster — or which provider — should this sandbox run on?**

`sandbox-scheduler` answers that. It slots in beside the existing pieces:

```
  sandbox-router      data plane     which pod, within a cluster   (upstream)
  sandbox-scheduler   control plane  which cluster or provider     (this)
```

## Why it exists

This was extracted from a working two-cluster deployment: a cheap Civo cluster
carrying the default load and a GKE cluster absorbing overflow, with sessions
hibernating to object storage so they can wake on either. The placement logic
started as an `if` statement and stopped being one about an hour later.

The generalisation is not hypothetical. The obvious next providers — E2B, Modal,
Fly — are not Kubernetes clusters at all, which is why
[Karmada](https://karmada.io/) and Open Cluster Management do not solve this
despite solving the cluster-to-cluster version well. Their policy model is worth
borrowing; their scheduling unit is not.

## Design

### Filter, score, bind

Straight from the Kubernetes scheduler framework, deliberately. Multi-cluster
placement has been expressed in this vocabulary before, and reviewers who know
one system know the others. Filters are hard constraints, scorers are weighted
preferences, binding is the provider call:

```go
profile := plugins.DefaultProfile(candidates)
decision, err := profile.Schedule(ctx, &framework.Request{
    Requires: map[string]string{"runtime": "gvisor"},
}, candidates)
```

A new policy is a plugin and a weight, not a change to the scheduler.

### Scheduling reads a cache, never the providers

A scheduling decision is a pure function over a capacity snapshot. Probing
providers at decision time would put a network round trip *per provider* into
every sandbox start — and sandbox start is a 90–150ms game, so a 1.5s timeout
budget is not a budget at all.

Capacity arrives asynchronously; the pipeline only reads it. This is how the
Kubernetes scheduler achieves sub-millisecond decisions, and it is the single
most important property here.

`pkg/registry` is the other half of that: it polls providers on its own
schedule, and `Snapshot()` returns from memory under a read lock. Two rules keep
it honest — **a failed refresh never erases what we knew** (it ages, because a
blip that zeroed capacity would silently redirect a fleet), and **a stale
provider never leaves the snapshot** (it is reported unreachable with its last
known capacity, so policy decides; dropping it makes "why wasn't civo
considered?" unanswerable).

### Every decision explains itself

A scheduler that says `gke` and nothing else is unoperable — you cannot tell a
misconfigured filter from a genuinely full cluster. Filters record why they
rejected a candidate and scorers record what they contributed, always:

```
placed on "civo" (score 830)
  civo         score 830   WarmCapacity=60*5 Cost=75*3 Reachability=100*3 Affinity=0*1
  gke          score 700   WarmCapacity=100*5 Cost=0*3 Reachability=100*3 Affinity=0*1
  modal        filtered: RequiredAttributes: requires runtime=gvisor, provider has runtime=firecracker
```

`ErrNoCandidates` carries the same per-provider reasons, because an opaque
"unschedulable" is the most common complaint levelled at schedulers.

### Built-in policy

| Plugin | Kind | Behaviour |
| --- | --- | --- |
| `RequiredAttributes` | filter | Exact match on declared provider facts (`gpu`, `runtime`, `region`). Exact, not fuzzy — this is how untrusted workloads stay off providers without isolation. |
| `Reachable` | filter | Excludes providers with no recent report. Not in the default profile; see below. |
| `WarmCapacity` | scorer | Prefers pre-warmed sandboxes. **Saturates** — past ~5 warm the marginal value to *this* request is nil, and scoring linearly would let an over-provisioned provider dominate a dimension that stopped mattering. |
| `Cost` | scorer | Relative to the most expensive candidate, so a per-second hosted API and a self-hosted cluster's amortised node cost are comparable without a shared unit. |
| `Reachability` | scorer | Demotes silent providers instead of excluding them. |
| `Affinity` | scorer | Returns a woken session to where its data is warm — a tiebreak, never a pin. |

The default profile demotes unreachable providers rather than filtering them: a
provider can miss heartbeats while still accepting claims perfectly well, and
placement should not fail for want of a heartbeat. Operators who prefer
precision over availability add the `Reachable` filter.

Affinity being a preference rather than a constraint is the same trade in the
other direction: a session should return to its data when that is sensible, and
move rather than wait when it is not.

## Roadmap

- [x] Scheduling framework: filter, score, bind, with explanations
- [x] Built-in filters and scorers
- [x] Capacity registry: refresh off the decision path, scheduler reads memory
- [ ] `SandboxProvider` / `SandboxPlacementPolicy` CRDs
- [ ] Controller: watch CRDs, maintain the registry, serve decisions
- [ ] Provider adapters: Kubernetes (agent-sandbox), then non-Kubernetes
- [ ] KEP proposing this to the agent-sandbox SIG as an extension

The intended home is upstream. `SandboxClaim`, `SandboxTemplate` and
`SandboxWarmPool` already live in agent-sandbox's `extensions/` rather than its
core API, and placement is the same shape of thing: an optional layer over the
core `Sandbox` API. This repo is where the design gets proven before it is
proposed.

## Contributing

Early enough that the interfaces are still moving, so issues discussing the
design are more useful than PRs implementing it.

```bash
go test ./...
```

Apache 2.0, matching upstream.
