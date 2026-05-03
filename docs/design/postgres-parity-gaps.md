# Postgres backend parity gaps

Last updated: 2026-05-03 (FF 0.15 — all service-layer gaps closed)

Cairn-rs's `fabric-postgres` feature is a work-in-progress backend. This
document tracks:

1. **Trait-level parity** — every method on cairn's `Engine` +
   `ControlPlaneBackend` routes through an `EngineBackend` trait
   body on Postgres; bucket-C is **closed**.
2. **Service-layer parity** — every cairn-side service constructor
   (runs, tasks, sessions, quotas, scheduler, signal delivery) now
   routes through the `EngineBackend` trait on both backends.
   `FabricSchedulerService::claim_for_worker` no longer surfaces
   `Unavailable` on PG after FF 0.15 ([FF#511](https://github.com/avifenesh/FlowFabric/issues/511),
   closed).
3. **Operator guide** — what works, what degrades, and how to choose a
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
delegation end-to-end on a live Postgres container.

### FF 0.15 additions

FF 0.15 added four new `EngineBackend` trait methods in the admission
/ budget family:

- `release_admission`
- `read_quota_policy_limits`
- `block_execution_for_admission`
- `read_budget_usage_and_limits`

Coverage on in-tree backends:

| Method | Valkey | Postgres | SQLite |
|---|---|---|---|
| `release_admission` | yes | yes | yes |
| `read_quota_policy_limits` | yes | yes | yes |
| `block_execution_for_admission` | yes | `Unavailable` | `Unavailable` |
| `read_budget_usage_and_limits` | yes | `Unavailable` | `Unavailable` |

Cairn's service layer does not yet invoke these primitives directly
(admission / budget flows route through `check_admission_and_record`
on the existing trait surface). The methods are available for future
cairn-side use — admission release on grant expiry, limit reads for
the UI quota-admin page, and so on.

## Service-layer parity — closed by FF 0.15

`FabricServices::start(BackendKind::Postgres)` returns `Ok(_)` and the
full service aggregate boots against a `PostgresFabricRuntime`
(cairn-rs#602, landed in PR-C4c).

FF 0.15 closed [FF#511](https://github.com/avifenesh/FlowFabric/issues/511):
`ff_scheduler::Scheduler` is now backend-agnostic via the new
`Scheduler::new_with_backend(Weak<dyn EngineBackend>, PartitionConfig)`
constructor. Cairn's `FabricSchedulerService::new` threads the
runtime's `backend()` handle (weakly downgraded) into the scheduler
regardless of backend, so `claim_for_worker` no longer gates on a
Valkey-only `ferriskey::Client`.

On Postgres the scheduler's partition-scanner path (`ZRANGEBYSCORE` +
`exec_core` `HGET`) has no trait primitive yet, so `claim_for_worker`
degrades to `Ok(None)` rather than returning a typed `Unavailable`.
PG deployments that want real worker claims should use the PG-native
claim path (`PostgresScheduler`) instead. Cairn-app's worker loop is
app-layer-gated to Valkey today, so the degrade path is not hit in
production.

`SignalBridge` routes through `EngineBackend::deliver_signal` verbatim
on both backends.

### What works end-to-end on PG

- `FabricServices::start` boot.
- `FabricRunService` / `FabricTaskService` / `FabricSessionService` /
  `FabricQuotaService` / `FabricWorkerService` — every method that
  routes through `ControlPlaneBackend` + `Engine`. That's the entire
  run / task / flow lifecycle, dependency staging, eligibility
  evaluation, lease renewal, cancel/complete/fail paths, budget +
  quota primitives, HMAC rotation, claim issuance, and the
  RFC-025 worker-pool primitives.
- `FabricSchedulerService::claim_for_worker` — constructs on PG;
  returns `Ok(None)` on PG rather than `Unavailable` (FF 0.15). Real
  claims on PG require `PostgresScheduler`.
- `FabricRotationService::rotate_waitpoint_hmac` — via
  `ControlPlaneBackend`.
- `SignalBridge::deliver_*_signal` — via `EngineBackend::deliver_signal`.

## Operator guide

### Choosing a backend

| Deployment shape | Recommended backend |
|---|---|
| Full cairn-app in `--mode team` (runs, tasks, sessions, approvals) — HTTP CRUD surfaces only | **Valkey** or **Postgres** (PR-C4c: full aggregate boots on both) |
| Full cairn-app in `--mode team` with worker pool (`cairn-app` claim loop) | **Valkey** — scheduler's scanner path is Valkey-specialised; PG `claim_for_worker` degrades to `Ok(None)` (FF 0.15). PG deployments that need real claims should use `PostgresScheduler` |
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
- **`FabricSchedulerService::claim_for_worker` constructs** (FF 0.15).
  Returns `Ok(None)` on PG because FF kept the partition scanner
  Valkey-specialised; no more `Unavailable` on the construct-or-call
  path.
- Direct `PostgresControlPlane` construction for control-plane-only
  callers.

### What degrades on `fabric-postgres`

- **Scheduler's `claim_for_worker`** — returns `Ok(None)` on PG
  rather than a real grant, because the scanner path is still
  Valkey-specialised in FF 0.15. PG deployments that need real claims
  should use `PostgresScheduler`. Cairn-app's worker loop is
  app-layer-gated to Valkey, so PG full-app-mode deploys that serve
  only HTTP CRUD are unaffected.
- **`/metrics`'s `ff_observability` block** — rendered only on Valkey.
  The PG backend has no registry today; cairn-side metrics still
  surface.
- **`CAIRN_BACKFILL_INSTANCE_TAG=1` backfill** — Valkey-only (walks
  `ff:exec:*:tags`). The env var is ignored with a log warning on PG.

### Error shape for callers

`Unavailable` from service-layer surfaces surfaces as:

```rust
Err(FabricError::Engine(Box::new(EngineError::Unavailable {
    op: "<method_name>",
})))
```

Match on the typed variant to branch. After FF 0.15 this is reserved
for future service-layer additions — all current cairn-fabric service
methods return either real results or typed domain errors on both
backends.

## Cross-references

- [cairn-rs#346](https://github.com/avifenesh/cairn-rs/issues/346) — Postgres opt-in feature (closed by PR-C4b).
- [cairn-rs#347](https://github.com/avifenesh/cairn-rs/issues/347) — backend-agnosticism meta (closed by PR-C4b).
- [cairn-rs#602](https://github.com/avifenesh/cairn-rs/issues/602) — service-constructor refactor (closed by PR-C4c).
- [FF#473](https://github.com/avifenesh/FlowFabric/issues/473) — worker-registry parity upstream (closed by FF 0.14).
- [FF#477](https://github.com/avifenesh/FlowFabric/issues/477) — `list_incoming_edges` trait surfacing upstream (closed by FF 0.14).
- [FF#508](https://github.com/avifenesh/FlowFabric/issues/508) — `register_worker` PG 16 `RETURNING (xmax = 0)` fix (closed by FF 0.14.1).
- [FF#511](https://github.com/avifenesh/FlowFabric/issues/511) — backend-agnostic `Scheduler` constructor (closed by FF 0.15).
- [FF 0.15 consumer migration guide](https://github.com/avifenesh/FlowFabric/blob/main/docs/CONSUMER_MIGRATION_0.15_scheduler_agnostic.md).
- [FF 0.14 consumer migration guide](https://github.com/avifenesh/FlowFabric/blob/main/docs/CONSUMER_MIGRATION_0.14_worker_registry.md).
- [`docs/design/ff-migration/pr-c4a-classification.md`](ff-migration/pr-c4a-classification.md) — per-method bucket A/B/C classification (historical).
