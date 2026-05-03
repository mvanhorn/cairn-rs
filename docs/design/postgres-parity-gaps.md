# Postgres backend parity gaps

Last updated: 2026-05-03 (FF 0.14.1 — SignalBridge backend-agnostic)

Cairn-rs's `fabric-postgres` feature is a work-in-progress backend. This
document tracks:

1. **Trait-level parity** — every method on cairn's `Engine` +
   `ControlPlaneBackend` now routes through an `EngineBackend` trait
   body on Postgres; bucket-C is **closed**.
2. **Service-layer parity** — one cairn-side surface
   (`FabricSchedulerService::claim_for_worker`) still surfaces
   `Unavailable` on PG because it depends on `ff_scheduler::Scheduler`,
   which FF has not yet made backend-agnostic ([FF#511](https://github.com/avifenesh/FlowFabric/issues/511)).
   `SignalBridge::deliver_*_signal` now routes through
   `EngineBackend::deliver_signal` and works on both backends.
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
delegation end-to-end on a live Postgres container. All 33 PG
control-plane tests — including `pg_register_heartbeat_mark_dead_roundtrip`
and `pg_register_worker_is_idempotent_on_same_instance` — pass on
FF 0.14.1 against PG 16 (FlowFabric PR #509 replaced the broken
`RETURNING (xmax = 0)` clause; tracked as cairn-rs #508, closed).

## Service-layer parity — one surface still Valkey-only

`FabricServices::start(BackendKind::Postgres)` returns `Ok(_)` and the
full service aggregate boots against a `PostgresFabricRuntime`
(cairn-rs#602, landed in PR-C4c).

One cairn-side service still wraps a primitive FF's `EngineBackend`
trait does not yet surface natively:

- **`FabricSchedulerService::claim_for_worker`** — FF 0.14.1's
  `ff_scheduler::Scheduler::new` still takes `ferriskey::Client`. The
  PG runtime's `valkey_client()` returns `None`, so the inner
  `Scheduler` is skipped and `claim_for_worker` returns
  `EngineError::Unavailable { op: "scheduler_claim_for_worker (Postgres backend has no ff-scheduler)" }`.
  Cairn-app's worker loop is gated at the app layer on a Valkey
  backend, so this branch is unreachable on today's PG
  full-app-mode deploys. Tracked at [FF#511](https://github.com/avifenesh/FlowFabric/issues/511)
  — FF upstream ask for a backend-agnostic `Scheduler` constructor.

`SignalBridge` now routes through `EngineBackend::deliver_signal`
verbatim — the raw `ff_deliver_signal` Lua FCALL is gone. Signal
delivery (approval resolution, tool-result, child-completed) works
on both Valkey and Postgres via the trait method's bodied impls.

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
| Full cairn-app in `--mode team` with worker pool (`cairn-app` claim loop) | **Valkey** — scheduler's `claim_for_worker` needs a `ferriskey::Client` today ([FF#511](https://github.com/avifenesh/FlowFabric/issues/511)) |
| Full cairn-app in `--mode team` with signal delivery (approvals, tool-result, child-completed) | **Valkey** or **Postgres** — routes through `EngineBackend::deliver_signal` |
| Full cairn-app in `--mode local` | **Valkey** or **Postgres** — same signal-delivery trait path |
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
  Tracked at [FF#511](https://github.com/avifenesh/FlowFabric/issues/511).
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
- [FF#508](https://github.com/avifenesh/FlowFabric/issues/508) — `register_worker` PG 16 `RETURNING (xmax = 0)` fix (closed by FF 0.14.1).
- [FF#511](https://github.com/avifenesh/FlowFabric/issues/511) — backend-agnostic `Scheduler` constructor (open; last service-layer PG parity gap).
- [FF 0.14 consumer migration guide](https://github.com/avifenesh/FlowFabric/blob/main/docs/CONSUMER_MIGRATION_0.14_worker_registry.md).
- [`docs/design/ff-migration/pr-c4a-classification.md`](ff-migration/pr-c4a-classification.md) — per-method bucket A/B/C classification (historical).
