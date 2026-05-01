# Postgres backend parity gaps

Last updated: 2026-05-01 (PR-C4b)

Cairn-rs's `fabric-postgres` feature is a work-in-progress backend. This
document tracks:

1. **Bucket-C trait methods** that currently return
   `EngineError::Unavailable { op }` on Postgres (5 methods).
2. **Full-app-mode limitation** — why
   `CAIRN_FABRIC_BACKEND=postgres` cannot boot the complete service
   aggregate yet.
3. **Operator guide** — what works, what fails, and how to choose a
   backend.

The Valkey backend is the complete reference implementation. Every
cairn feature that ships on Valkey is presumed to work on Valkey; this
document only enumerates deltas relative to that baseline.

## Bucket C — trait methods returning `Unavailable` on Postgres

Each method returns
`Err(FabricError::Engine(Box::new(EngineError::Unavailable { op: "<method>" })))`
rather than panicking. Callers can branch on this variant to degrade
gracefully or surface a classified error to operators.

| Method | Gap reason | Upstream | Workaround |
|---|---|---|---|
| `list_incoming_edges` | FF 0.13's `EngineBackend` trait has no point-query for an execution's incoming edges; SDK-only today. | [FF#477](https://github.com/avifenesh/FlowFabric/issues/477) | Use Valkey backend. Cairn's internal schedulers always own the `FlowId` at the call site, so an alternative path exists once the trait surfaces the primitive. |
| `register_worker` | FF 0.13's `EngineBackend` trait has no worker-registry primitive. | [FF#473](https://github.com/avifenesh/FlowFabric/issues/473) | Use Valkey backend. Cairn-app's worker paths are Valkey-gated today; PG-only deployments wait on FF#473. |
| `heartbeat_worker` | Same as `register_worker`. | FF#473 | Use Valkey backend. |
| `mark_worker_dead` | Same as `register_worker`. | FF#473 | Use Valkey backend. |
| `list_expired_leases` | FF handles lease reclaim server-side via each backend's scanner; no operator-facing trait read exposed. | FF#473 (bundles with worker-registry) | Use Valkey backend for operator dashboards; PG deployments lose only the "expired leases" surface on the dashboards, not reclaim correctness. |

Integration tests in
`crates/cairn-fabric/tests/postgres_control_plane_live.rs` assert the
typed `Unavailable` response for each method. A regression that
replaces the typed error with a panic or a wrong `op` literal trips
CI.

## Full-app-mode limitation (PR-C4c scope)

Today `FabricServices::start(BackendKind::Postgres)` returns
`FabricError::Config` with an actionable error pointing at
[cairn-rs#602](https://github.com/avifenesh/cairn-rs/issues/602).

### Why

The ~30 cairn service constructors (`run_service`, `task_service`,
`session_service`, `scheduler_service`, `quota_service`,
`budget_service`, …) hold `Arc<FabricRuntime>` **concretely**.
`FabricRuntime` is the Valkey runtime; it carries a `ferriskey::Client`
and Valkey-specific handles. Swapping in a `PostgresFabricRuntime`
there is a type error, not a config flip.

### What unblocks it

Refactor service constructors to take
`Arc<dyn ControlPlaneBackend>` + `Arc<dyn Engine>` (or a new
backend-agnostic `Arc<dyn FabricRuntimeHandle>` superset). Tracked at
**[cairn-rs#602](https://github.com/avifenesh/cairn-rs/issues/602)**
(PR-C4c).

### What works today on PG

`PostgresFabricRuntime::start(config)` boots fine for
**control-plane-only consumers** — callers that instantiate
`PostgresControlPlane` directly and only need the trait surface.
Integration tests drive this path live against a Postgres container.

## Operator guide

### Choosing a backend

| Deployment shape | Recommended backend |
|---|---|
| Full cairn-app in `--mode team` (runs, tasks, sessions, approvals) | **Valkey** |
| Full cairn-app in `--mode local` | **Valkey** |
| Control-plane-only integration (third-party tool calling `PostgresControlPlane` for flow / edge / execution reads + lifecycle primitives) | **Postgres** works for 32/37 trait methods; 5 bucket-C methods return `Unavailable` |
| Worker pool (`cairn-app` as worker) | **Valkey** until FF#473 lands |

### What works on `fabric-postgres` today

- 27 bucket-B trait methods — run/task/flow lifecycle, dependency
  staging, eligibility evaluation, lease renewal, cancel/complete/fail
  paths, budget + quota primitives, HMAC rotation, claim issuance.
- 3 bucket-A trait methods — execution tag get/set, flow tag set.
- Direct `PostgresControlPlane` construction for control-plane-only
  callers.

### What fails / degrades on `fabric-postgres`

- **Full app mode boot** — `FabricServices::start(BackendKind::Postgres)`
  returns `FabricError::Config` with pointer to PR-C4c. See
  "Full-app-mode limitation" above.
- **Worker registry** — 4 bucket-C methods return `Unavailable`. No
  regression in cairn-app today because worker paths are
  Valkey-gated, but tenants wanting PG-only deployments must wait for
  FF#473.
- **`list_incoming_edges`** — returns `Unavailable`. Scheduler-internal
  callers own the `FlowId` at the site, so composing on top of
  `describe_flow` is a viable alternative once FF#477 lands.
- **`list_expired_leases`** — returns `Unavailable`. Reclaim itself
  works (FF's server-side scanner handles it per-backend); only the
  operator dashboard's "currently expired" read degrades.

### Error shape for callers

All `Unavailable` responses surface as:

```rust
Err(FabricError::Engine(Box::new(EngineError::Unavailable {
    op: "<method_name>",
})))
```

Match on the typed variant to branch:

```rust
match cp.list_incoming_edges(&eid).await {
    Ok(edges) => { /* Valkey path */ }
    Err(FabricError::Engine(e)) => match *e {
        EngineError::Unavailable { op } => {
            // PG fallback — choose: degrade the read, or surface a
            // 503 with the op name in the detail field.
        }
        other => return Err(FabricError::Engine(Box::new(other))),
    },
    Err(other) => return Err(other),
}
```

## Cross-references

- [cairn-rs#346](https://github.com/avifenesh/cairn-rs/issues/346) — Postgres opt-in feature (closed by PR-C4b).
- [cairn-rs#347](https://github.com/avifenesh/cairn-rs/issues/347) — backend-agnosticism meta (closed by PR-C4b).
- [cairn-rs#602](https://github.com/avifenesh/cairn-rs/issues/602) — service-constructor refactor (PR-C4c).
- [FF#473](https://github.com/avifenesh/FlowFabric/issues/473) — worker-registry parity upstream.
- [FF#477](https://github.com/avifenesh/FlowFabric/issues/477) — `list_incoming_edges` trait surfacing upstream.
- [`docs/design/ff-migration/pr-c4a-classification.md`](ff-migration/pr-c4a-classification.md) — per-method bucket A/B/C classification.
