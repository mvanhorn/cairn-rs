# RFC-025 Provider Boundary Research: provider_bindings vs provider_connections

**Date:** 2026-04-28  
**Context:** RFC-025 Phase 3 decision gate — projected vs ephemeral classification

## Research Findings

### 1. What is `provider_bindings`?

**Type:** Configuration record with operational state.

`ProviderBindingRecord` (cairn-domain/src/providers.rs): Contains:
- `provider_binding_id`, `project: ProjectKey`, `provider_connection_id`, `provider_model_id`
- `operation_kind` (enum: Brain, Worker, Embedder)
- `settings: ProviderBindingSettings` (metadata)
- `active: bool` (runtime state)
- `created_at: u64` (timestamp)

**Assessment:** Answer **(a)** with operator-controlled state. The record is:
- A static configuration mapping (project → connection → model)
- Scoped by ProjectKey (multi-tenant boundary)
- Operator-visible via `POST /v1/providers/bindings` (line cairn-app/src/openapi_spec.rs:1048+)
- Backed by event: `ProviderBindingCreated` (cairn-domain/src/events.rs:205-209)
- Mutated via events: `ProviderBindingStateChanged` (line 206)
- Must survive restart for operator UX (no reconnect needed after restart)

**File citations:**
- Trait: cairn-runtime/src/provider_bindings.rs:11-41
- Record: cairn-domain/src/providers.rs (ProviderBindingRecord)
- Events: cairn-domain/src/events.rs:205-209, 206, 207

---

### 2. What is `provider_connections`?

**Type:** HTTP/gRPC endpoint registration + metadata.

`ProviderConnectionRecord` (cairn-domain/src/providers.rs): Contains:
- `provider_connection_id`, `tenant_id`
- `provider_family, adapter_type` (e.g., "openai", "bedrock")
- `supported_models: Vec<String>`
- `status: ProviderConnectionStatus` (Active | Disabled)
- `created_at: u64` (timestamp)

**Assessment:** Answer **(c)** — persistent metadata record, NOT the HTTP client itself. The record is:
- A tenant-level configuration resource (who owns which endpoint)
- Operator-visible and referenceable after restart
- Backed by event: `ProviderConnectionRegistered` (cairn-domain/src/events.rs:209-212)
- Mutable via events: `ProviderConnectionDeleted` (line 213, with note "projection row is removed")
- Requires persistence: hard-delete removes the row so `provider_connection_id` can be re-created (line 49, cairn-runtime/src/provider_connections.rs)

**Critically:** `provider_connections` record ≠ HTTP client instance. The service holds *metadata about the endpoint*, not the live reqwest::Client.

**File citations:**
- Trait: cairn-runtime/src/provider_connections.rs:17-50
- Record: cairn-domain/src/providers.rs (ProviderConnectionRecord)
- Events: cairn-domain/src/events.rs:209-213

---

### 3. Provider Lifecycle Events

**Events in scope:**

| Event | Domain | Emitter | Consumer |
|-------|--------|---------|----------|
| `ProviderConnectionRegistered` | Tenant registration | `createProviderConnection` handler | → ProviderConnectionService |
| `ProviderConnectionDeleted` | Tenant retraction | `deleteProviderConnection` handler | → ProviderConnectionService |
| `ProviderBindingCreated` | Project deployment | `createProviderBinding` handler | → ProviderBindingService |
| `ProviderBindingStateChanged` | Binding activation | `activateProviderBinding` / `deactivateProviderBinding` handlers | → ProviderBindingService |
| `ProviderHealthChecked` | Per-binding health | `recordHealthCheck` handler | → ProviderHealthService (ephemeral) |
| `ProviderPoolCreated` | Pool allocation | `createProviderPool` handler | → ProviderConnectionPoolService (ephemeral) |
| `ProviderPoolConnectionAdded/Removed` | Pool mutation | `addConnection` / `removeConnection` handlers | → ProviderConnectionPoolService (ephemeral) |

**Key distinction:** `ProviderConnectionRegistered` and `ProviderBindingCreated` have **persistent semantics** — they must survive restarts. `ProviderHealthChecked` and pool events are **ephemeral markers** — they inform in-memory state but aren't relied upon for boot recovery.

**File citations:**
- Event enum: cairn-domain/src/events.rs:156, 205-220
- Event types: cairn-domain/src/events.rs (struct defs for each)

---

### 4. Boot Replay & Persistence

**Status quo:** No explicit `replay_providers` function exists in cairn-app/src/state.rs.

**Implication:** Provider state is NOT currently rebuilt via O(N) event-log replay on boot (unlike evals, triggers, graph). This is because:
- `provider_connections` records are populated only by `ProviderConnectionRegistered` events (appended durably to the log)
- `provider_bindings` records are populated only by `ProviderBindingCreated` events (appended durably to the log)
- The SyncProjection mechanism (already in place per RFC-025 discussion) reconstructs these on startup from the log atomically

**Actual boot flow:**
1. EventLog opens (pg/sqlite/in-memory)
2. SyncProjection fires inside the same transaction as event append
3. On boot, tables are scanned (O(1) if already projected; O(N) if projection is stubbed)

**Current stubs:** Grep for `log_stub` in cairn-store/src/pg/projections.rs and sqlite/projections.rs confirms provider events are likely stubbed today (no-op projections). Phase 3 must fill these.

**What gets rebuilt:** Any field in `ProviderConnectionRecord` and `ProviderBindingRecord` that has a `created_at` or status field persists **because the events encode those fields** (see `ProviderConnectionRegistered` struct at events.rs line 2000+).

**File citations:**
- Boot flow: cairn-app/src/main.rs:1478-1480 (calls replay_*; provider NOT in that list)
- State.rs: cairn-app/src/state.rs (no `replay_providers` defined)
- Event log atomicity: cairn-store/src/pg/event_log.rs:77-86

---

### 5. Operator-Visible Surface

**API endpoints (from OpenAPI spec cairn-app/src/openapi_spec.rs):**

- `POST /v1/providers/connections` → createProviderConnection (line 1048+)
- `GET /v1/providers/connections` → listProviderConnections
- `GET /v1/providers/connections/{id}` → getProviderConnection
- `DELETE /v1/providers/connections/{id}` → deleteProviderConnection

- `POST /v1/providers/bindings` → createProviderBinding
- `GET /v1/providers/bindings` → listProviderBindings (project-scoped)
- `PATCH /v1/providers/bindings/{id}/activate` → activateProviderBinding
- `PATCH /v1/providers/bindings/{id}/deactivate` → deactivateProviderBinding

**Handlers:** cairn-app/src/handlers/providers.rs (lines 1-50+)

**Critical:** Both endpoints are **reachable after restart**. They read from `state.runtime.provider_connections` and `state.runtime.provider_bindings`, which must be populated by the SyncProjection. If the projections are stubbed, reads return empty (silent failure risk). This is the core issue RFC-025 Phase 3 addresses.

**File citations:**
- OpenAPI: cairn-app/src/openapi_spec.rs:1048+, 1056+
- Handlers: cairn-app/src/handlers/providers.rs:632-750 (sample read paths)

---

### 6. Restart Semantics

**Question:** Do previously-registered provider connections survive a restart?

**Answer:** YES. They must, because:

1. **Event durability:** `ProviderConnectionRegistered` and `ProviderBindingCreated` are appended to the event log (Postgres/SQLite/InMemory backend).
2. **SyncProjection:** On boot, the projection layer replays the log and reconstructs the tables.
3. **No manual action needed:** Operator does not need to re-register connections after restart.

**Recovery flow:**
1. Cairn restarts
2. EventLog opens the backed store
3. For each event variant marked `Projected` in the registry, SyncProjection fires and populates the read-model table
4. Operator queries `GET /v1/providers/connections` → reads from the projected table → sees all previously-registered connections

**Caveat:** This only works if the projection is **not stubbed**. Today they likely are (hence the Phase 3 work).

**File citations:**
- Event durability: cairn-domain/src/events.rs (ProviderConnectionRegistered, ProviderBindingCreated)
- Atomicity: cairn-store/src/pg/event_log.rs:77-86
- Recovery contract: RFC-025 line 97

---

### 7. Downstream Hot-Path Consumers

**Search results:**

| Consumer | Path | Read rate |
|----------|------|-----------|
| Provider binding lookup (LLM dispatch) | cairn-orchestrator/src (not found) | Unknown |
| Provider connection lookup | cairn-app/src/handlers/providers.rs:633 | Operator UI (low rate) |
| Provider registry initialization | cairn-runtime/src/provider_registry.rs | Once per boot |
| Health check dispatch | cairn-runtime/src/provider_health.rs | Periodic (health check interval) |

**Result:** No evidence of >1000/sec hot-path reads found in grep. Provider state is read by:
1. Operator dashboard (CRUD interface) — low rate, bursty
2. Boot initialization (provider_registry) — once per process
3. Health check loops — periodic, not per-LLM-call

**Implication:** Even if reads are projection-backed (not in-memory), latency is acceptable for these call patterns. The *write* path (event append) is what matters for boot speed.

**File citations:**
- Handlers: cairn-app/src/handlers/providers.rs:633
- Registry init: cairn-runtime/src/provider_registry.rs (reads binding config at startup)

---

## Recommendation

**Classification: Bindings PROJECTED, Connections PROJECTED, Pools & Health EPHEMERAL** (Option A + clarification)

### Rationale

1. **Bindings must be projected** (ProviderBindingRecord):
   - Operator configures which model uses which provider per project
   - Must survive restart (operator doesn't re-configure on each restart)
   - Has persistent fields (binding_id, project, created_at, active state)
   - Event-backed (ProviderBindingCreated, ProviderBindingStateChanged)

2. **Connections must be projected** (ProviderConnectionRecord):
   - Tenant-level resource registration (which HTTP endpoint the tenant owns)
   - Must survive restart (delete is hard-delete for re-creation, not transient)
   - Has persistent fields (connection_id, tenant_id, created_at, status)
   - Event-backed (ProviderConnectionRegistered, ProviderConnectionDeleted)

3. **Pools & Health are ephemeral**:
   - Pools track current request count in live HTTP connections — not persistable
   - Health tracks in-flight probe state and last-check timestamp — rebuilt on next health cycle
   - No hard delete semantics; no operator visibility on restart
   - Rebuilt from bindings + connection records at boot

### Tradeoff

**Accepted cost:** Phase 3 must fill projections for `ProviderConnectionRegistered`, `ProviderConnectionDeleted`, `ProviderBindingCreated`, `ProviderBindingStateChanged` in pg/sqlite (in-memory is already correct). ~200 LOC per backend, ~3 schema columns.

**Benefit:** Operator sees stable provider config after restart. No silent reads. Boot cost is O(1) (projection table scan, not event-log replay).

---

## Next Action for Phase 3

**Code change:** `RFC-025 Phase 3 implementation` (existing PR scope in RFC-025 line 196-206)

1. **crates/cairn-store/src/pg/projections.rs** + `crates/cairn-store/src/sqlite/projections.rs`:
   - Replace `log_stub` for `ProviderConnectionRegistered`, `ProviderConnectionDeleted`, `ProviderBindingCreated`, `ProviderBindingStateChanged`
   - Add read-model table columns: for connections (`provider_connection_id`, `tenant_id`, `provider_family`, `adapter_type`, `status`, `created_at`); for bindings (`provider_binding_id`, `project`, `provider_connection_id`, `provider_model_id`, `operation_kind`, `active`, `created_at`)
   - **LOC estimate:** 150-200 per backend

2. **crates/cairn-store/tests/projection_parity.rs** (Phase 0 deliverable):
   - Assert byte-equality of ProviderConnectionRecord and ProviderBindingRecord across in-memory / sqlite / pg after emitting events
   - **LOC estimate:** 40-60

3. **Registry annotation** (Phase 0 registry macro):
   - Mark `ProviderBindingCreated`, `ProviderBindingStateChanged`, `ProviderConnectionRegistered`, `ProviderConnectionDeleted` as `#[projection(status = "projected", table = "provider_connections")]` (equivalent names)
   - Mark `ProviderPoolCreated`, `ProviderPoolConnectionAdded/Removed`, `ProviderHealthChecked` as `#[projection(status = "ephemeral")]`

4. **Documentation** (crates/cairn-store/README.md + docs/design/runtime-services.md):
   - Table: provider_bindings (Projected), provider_connections (Projected), provider_pools (Ephemeral), provider_health (Ephemeral)
   - Rationale: configs survive restart, pools do not

**Total LOC:** ~400-500 (fills stubs + parity test + registry tags)

---

