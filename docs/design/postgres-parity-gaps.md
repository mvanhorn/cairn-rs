# Postgres backend parity gaps

Last updated: 2026-05-01 (PR-C4c)

Cairn-rs's `fabric-postgres` feature is a work-in-progress backend. This
document tracks:

1. **Bucket-C trait methods** that currently return
   `EngineError::Unavailable { op }` on Postgres (5 methods).
2. **Service-layer parity** — the Valkey-specific operations that
   still surface `Unavailable` on PG even though the full service
   aggregate now boots (PR-C4c).
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

## Service-layer parity (PR-C4c)

`FabricServices::start(BackendKind::Postgres)` now returns `Ok(_)` —
the full service aggregate boots against a `PostgresFabricRuntime`.
cairn-rs#602 is closed.

### What changed

Service constructors (`FabricRunService::new`,
`FabricTaskService::new`, `FabricSessionService::new`,
`FabricQuotaService::new`, `FabricSchedulerService::new`,
`SignalBridge::new`) accept `Arc<dyn FabricRuntimeHandle>` instead of
`Arc<FabricRuntime>`. Both the Valkey `FabricRuntime` and
`PostgresFabricRuntime` impl the trait — the aggregate's construction
chain is shared between the two backends in `build_services`.

The `FabricServices` struct exposes two runtime slots:

- `pub runtime: Arc<dyn FabricRuntimeHandle>` — backend-agnostic,
  always populated.
- `pub valkey_runtime: Option<Arc<FabricRuntime>>` — concrete Valkey
  runtime. `Some(_)` on Valkey boots, `None` on PG boots. Callers
  that need `ferriskey::Client`, `ff_observability::Metrics`, or the
  instance-tag backfill branch gate on the `Option`.

### Surfaces that still return `Unavailable` on PG

Two cairn-side services wrap primitives FF's `EngineBackend` trait
does not yet surface natively:

- **`FabricSchedulerService::claim_for_worker`** — FF 0.13's
  `ff_scheduler::Scheduler::new` takes `ferriskey::Client`. The PG
  runtime's `valkey_client()` returns `None`, so the inner
  `Scheduler` is skipped and `claim_for_worker` returns
  `EngineError::Unavailable { op: "scheduler_claim_for_worker (Postgres backend has no ff-scheduler)" }`.
  Cairn-app's worker loop is gated at the app layer on a Valkey
  backend, so this branch is unreachable on today's PG
  full-app-mode deploys.
- **`SignalBridge::deliver_*_signal`** — cairn still dispatches
  `ff_deliver_signal` via a raw `ferriskey::Client::fcall`. The
  `FabricRuntimeHandle::fcall` trait method returns
  `EngineError::Unavailable { op: "fcall (Postgres backend has no Lua surface)" }`
  on PG. Signal-delivery surfaces (approval resolution, tool-result,
  child-completed) are Valkey-gated at the app layer.

Both limitations will retire when FF surfaces the equivalent
primitives on `EngineBackend`.

### What works end-to-end on PG

- `FabricServices::start` boot.
- `FabricRunService` / `FabricTaskService` / `FabricSessionService` /
  `FabricQuotaService` — every method that routes through
  `ControlPlaneBackend` + `Engine` (27 bucket-B + 3 bucket-A trait
  methods). That's the entire run / task / flow lifecycle,
  dependency staging, eligibility evaluation, lease renewal,
  cancel/complete/fail paths, budget + quota primitives, HMAC
  rotation, claim issuance.
- `FabricRotationService::rotate_waitpoint_hmac` — via
  `ControlPlaneBackend`.

## Operator guide

### Choosing a backend

| Deployment shape | Recommended backend |
|---|---|
| Full cairn-app in `--mode team` (runs, tasks, sessions, approvals) — HTTP CRUD surfaces only | **Valkey** or **Postgres** (PR-C4c: full aggregate boots on both) |
| Full cairn-app in `--mode team` with worker pool (`cairn-app` claim loop) | **Valkey** until FF#473 lands — scheduler's `claim_for_worker` needs a `ferriskey::Client` today |
| Full cairn-app in `--mode team` with signal delivery (approvals, tool-result, child-completed) | **Valkey** until FF surfaces a trait-level `deliver_signal` cairn can consume |
| Full cairn-app in `--mode local` | **Valkey** (local-mode shares the signal-delivery path) |
| Control-plane-only integration (third-party tool calling `PostgresControlPlane` for flow / edge / execution reads + lifecycle primitives) | **Postgres** works for 32/37 trait methods; 5 bucket-C methods return `Unavailable` |

### What works on `fabric-postgres` today

- **`FabricServices::start(BackendKind::Postgres)` boots the full
  service aggregate** (PR-C4c). Run / task / session / quota /
  rotation surfaces all route through the same `ControlPlaneBackend`
  + `Engine` traits on both backends.
- 27 bucket-B trait methods — run/task/flow lifecycle, dependency
  staging, eligibility evaluation, lease renewal, cancel/complete/fail
  paths, budget + quota primitives, HMAC rotation, claim issuance.
- 3 bucket-A trait methods — execution tag get/set, flow tag set.
- Direct `PostgresControlPlane` construction for control-plane-only
  callers.

### What fails / degrades on `fabric-postgres`

- **Scheduler's `claim_for_worker`** — returns `Unavailable` on PG
  because `ff_scheduler::Scheduler` is `ferriskey::Client`-bound.
  Cairn-app's worker loop is app-layer-gated to Valkey, so PG
  full-app-mode deploys that serve only HTTP CRUD are unaffected.
- **`SignalBridge::deliver_*_signal`** — returns `Unavailable` on PG
  because `ff_deliver_signal` is a Valkey Lua FCALL. Signal-delivery
  surfaces (approvals, tool-result, child-completed) are
  app-layer-gated to Valkey.
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
- **`/metrics`'s `ff_observability` block** — rendered only on Valkey.
  The PG backend has no registry today; cairn-side metrics still
  surface.
- **`CAIRN_BACKFILL_INSTANCE_TAG=1` backfill** — Valkey-only (walks
  `ff:exec:*:tags`). The env var is ignored with a log warning on PG.

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
- [cairn-rs#602](https://github.com/avifenesh/cairn-rs/issues/602) — service-constructor refactor (closed by PR-C4c).
- [FF#473](https://github.com/avifenesh/FlowFabric/issues/473) — worker-registry parity upstream.
- [FF#477](https://github.com/avifenesh/FlowFabric/issues/477) — `list_incoming_edges` trait surfacing upstream.
- [`docs/design/ff-migration/pr-c4a-classification.md`](ff-migration/pr-c4a-classification.md) — per-method bucket A/B/C classification.
