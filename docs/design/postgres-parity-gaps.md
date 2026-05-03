# Postgres backend parity gaps

Last updated: 2026-05-03 (FF 0.14 adoption)

Cairn-rs's `fabric-postgres` feature is a work-in-progress backend. This
document tracks:

1. **Trait-level parity** — every method on cairn's `Engine` +
   `ControlPlaneBackend` now routes through an `EngineBackend` trait
   body on Postgres; bucket-C is **closed**.
2. **Service-layer parity** — two cairn-side surfaces
   (`FabricSchedulerService::claim_for_worker`,
   `SignalBridge::deliver_*_signal`) still surface `Unavailable` on
   PG because they depend on `ferriskey::Client` / Lua-FCALL primitives
   FF has not yet exposed on `EngineBackend`.
3. **Operator guide** — what works, what fails, and how to choose a
   backend.

The Valkey backend is the complete reference implementation. Every
cairn feature that ships on Valkey is presumed to work on Valkey; this
document only enumerates deltas relative to that baseline.

## Trait-level parity — closed by FF 0.14

FF 0.14 landed the two upstream asks that held bucket-C open:

- **FF#473** (RFC-025 worker-registry parity): 5 new `EngineBackend`
  trait methods — `register_worker`, `heartbeat_worker`,
  `mark_worker_dead`, `list_expired_leases`, `list_workers` — with
  bodies on every in-tree backend (Valkey, Postgres, SQLite).
- **FF#477** (`list_incoming_edges` on Postgres): surfaced as two
  composable trait primitives — `resolve_execution_flow_id` +
  `list_edges(flow_id, direction)` — with Postgres-native bodies.

Cairn-rs adopted both in the FF 0.13 → 0.14 bump. The
`PostgresControlPlane` impl now delegates verbatim for all 37/37
trait methods; no `Unavailable` returns remain at the trait layer.

Integration tests in
`crates/cairn-fabric/tests/postgres_control_plane_live.rs` assert the
delegation end-to-end on a live Postgres container. The PG
register_worker / idempotent-refresh tests are currently `#[ignore]`
pending an FF 0.14 upstream fix: `ff_backend_postgres::register_worker`
uses `RETURNING (xmax = 0)` in a `query_scalar`, which PG 16 rejects
with `0A000: cannot retrieve a system column in this context`
(`execTuples.c:tts_virtual_getsysattr`). The one-line upstream fix is
either `RETURNING xmax` + client-side comparison or `RETURNING (xmax
= 0)::boolean`. The Valkey register_worker path is fully covered in
`crates/cairn-fabric/tests/integration/test_control_plane.rs` so the
cairn adapter + trait-delegation shape is regression-safe in the
meantime.

## Service-layer parity — two surfaces still Valkey-only

`FabricServices::start(BackendKind::Postgres)` returns `Ok(_)` and the
full service aggregate boots against a `PostgresFabricRuntime`
(cairn-rs#602, landed in PR-C4c).

Two cairn-side services wrap primitives FF's `EngineBackend` trait
does not yet surface natively:

- **`FabricSchedulerService::claim_for_worker`** — FF 0.14's
  `ff_scheduler::Scheduler::new` still takes `ferriskey::Client`. The
  PG runtime's `valkey_client()` returns `None`, so the inner
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
  `FabricQuotaService` / `FabricWorkerService` — every method that
  routes through `ControlPlaneBackend` + `Engine`. That's the entire
  run / task / flow lifecycle, dependency staging, eligibility
  evaluation, lease renewal, cancel/complete/fail paths, budget +
  quota primitives, HMAC rotation, claim issuance, and the
  RFC-025 worker-pool primitives.
- `FabricRotationService::rotate_waitpoint_hmac` — via
  `ControlPlaneBackend`.

## Operator guide

### Choosing a backend

| Deployment shape | Recommended backend |
|---|---|
| Full cairn-app in `--mode team` (runs, tasks, sessions, approvals) — HTTP CRUD surfaces only | **Valkey** or **Postgres** (PR-C4c: full aggregate boots on both) |
| Full cairn-app in `--mode team` with worker pool (`cairn-app` claim loop) | **Valkey** — scheduler's `claim_for_worker` needs a `ferriskey::Client` today |
| Full cairn-app in `--mode team` with signal delivery (approvals, tool-result, child-completed) | **Valkey** until FF surfaces a trait-level `deliver_signal` cairn can consume |
| Full cairn-app in `--mode local` | **Valkey** (local-mode shares the signal-delivery path) |
| Control-plane-only integration (third-party tool calling `PostgresControlPlane` for flow / edge / execution reads + lifecycle primitives + worker registry) | **Postgres** — all 37 trait methods have real bodies |

### What works on `fabric-postgres` today

- **`FabricServices::start(BackendKind::Postgres)` boots the full
  service aggregate** (PR-C4c).
- **All 37 `Engine` + `ControlPlaneBackend` trait methods** — run /
  task / flow lifecycle, dependency staging, eligibility evaluation,
  lease renewal, cancel / complete / fail paths, budget + quota
  primitives, HMAC rotation, claim issuance, execution + flow tag
  reads/writes, RFC-025 worker-pool primitives
  (`register_worker` / `heartbeat_worker` / `mark_worker_dead` /
  `list_workers` / `list_expired_leases`), and `list_incoming_edges`
  via the `resolve_execution_flow_id` + `list_edges` composition.
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
- **`/metrics`'s `ff_observability` block** — rendered only on Valkey.
  The PG backend has no registry today; cairn-side metrics still
  surface.
- **`CAIRN_BACKFILL_INSTANCE_TAG=1` backfill** — Valkey-only (walks
  `ff:exec:*:tags`). The env var is ignored with a log warning on PG.

### Error shape for callers

`Unavailable` responses from the two remaining service-layer surfaces
surface as:

```rust
Err(FabricError::Engine(Box::new(EngineError::Unavailable {
    op: "<method_name>",
})))
```

Match on the typed variant to branch.

## Cross-references

- [cairn-rs#346](https://github.com/avifenesh/cairn-rs/issues/346) — Postgres opt-in feature (closed by PR-C4b).
- [cairn-rs#347](https://github.com/avifenesh/cairn-rs/issues/347) — backend-agnosticism meta (closed by PR-C4b).
- [cairn-rs#602](https://github.com/avifenesh/cairn-rs/issues/602) — service-constructor refactor (closed by PR-C4c).
- [FF#473](https://github.com/avifenesh/FlowFabric/issues/473) — worker-registry parity upstream (closed by FF 0.14).
- [FF#477](https://github.com/avifenesh/FlowFabric/issues/477) — `list_incoming_edges` trait surfacing upstream (closed by FF 0.14).
- [FF 0.14 consumer migration guide](https://github.com/avifenesh/FlowFabric/blob/main/docs/CONSUMER_MIGRATION_0.14_worker_registry.md).
- [`docs/design/ff-migration/pr-c4a-classification.md`](ff-migration/pr-c4a-classification.md) — per-method bucket A/B/C classification (historical).
