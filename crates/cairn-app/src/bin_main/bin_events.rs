//! Event replay and append handlers (RFC 002).

#[allow(unused_imports)]
use crate::*;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::Json;
use cairn_store::{EventLog, EventPosition};
use serde::{Deserialize, Serialize};

// ── Event replay handler (RFC 002) ────────────────────────────────────────────

#[derive(Deserialize)]
pub(crate) struct EventReplayQuery {
    /// Return events strictly after this log position.
    after: Option<u64>,
    #[serde(default = "default_event_limit")]
    limit: usize,
}

pub(crate) fn default_event_limit() -> usize {
    100
}

#[derive(Serialize)]
pub(crate) struct StoredEventSummary {
    position: u64,
    stored_at: u64,
    event_type: String,
}

// Run tool invocations handler → bin_handlers.rs

/// `GET /v1/events` — cursor-based replay of the global event log (RFC 002).
///
/// Clients use `?after=<position>&limit=<n>` to page forward. Returns at most
/// `limit` events (default 100, max 500) strictly after the given position.
/// When Postgres is configured, replays from the durable Postgres log.
pub(crate) async fn list_events_handler(
    State(state): State<AppState>,
    Query(q): Query<EventReplayQuery>,
) -> impl axum::response::IntoResponse {
    let limit = q.limit.min(500);
    let after = q.after.map(EventPosition);
    // Use durable event log for replay when available (Postgres > SQLite > InMemory).
    let read_result = if let Some(pg) = &state.pg {
        pg.event_log.read_stream(after, limit).await
    } else if let Some(sq) = &state.sqlite {
        sq.event_log.read_stream(after, limit).await
    } else {
        state.runtime.store.read_stream(after, limit).await
    };
    match read_result {
        Ok(events) => {
            let summaries: Vec<StoredEventSummary> = events
                .into_iter()
                .map(|e| StoredEventSummary {
                    position: e.position.0,
                    stored_at: e.stored_at,
                    event_type: event_type_name(&e.envelope.payload).to_owned(),
                })
                .collect();
            Ok(Json(summaries))
        }
        Err(e) => Err(internal_error(e.to_string())),
    }
}

// event_type_name is the canonical copy from the lib crate (helpers.rs).
// Re-exported here so binary modules can use it via `use crate::*`.
pub(crate) use cairn_app::event_type_name;

// ── Event append handler (RFC 002) ────────────────────────────────────────────

/// Per-envelope result returned by `POST /v1/events/append`.
#[derive(Serialize)]
pub(crate) struct AppendResult {
    event_id: String,
    position: u64,
    /// `true` = event was newly appended; `false` = idempotent duplicate
    /// (causation_id already existed — existing position is returned).
    appended: bool,
}

/// `POST /v1/events/append` — write path for the event log (RFC 002).
///
/// Accepts a JSON array of `EventEnvelope<RuntimeEvent>` objects. Each
/// envelope is processed for idempotency:
///
/// - If the envelope carries a `causation_id` **and** an event with that
///   causation ID already exists in the log, the existing position is
///   returned without re-appending.
/// - Otherwise the event is appended and its assigned position is returned.
///
/// Appended events are broadcast immediately to all SSE subscribers.
///
/// Returns an array of `AppendResult` in the same order as the input.
pub(crate) async fn append_events_handler(
    State(state): State<AppState>,
    axum::extract::Extension(principal): axum::extract::Extension<cairn_api::auth::AuthPrincipal>,
    Json(envelopes): Json<Vec<cairn_domain::EventEnvelope<cairn_domain::RuntimeEvent>>>,
) -> impl axum::response::IntoResponse {
    if envelopes.is_empty() {
        return Ok((StatusCode::OK, Json(Vec::<AppendResult>::new())));
    }

    // T6c-C4: raw event-log writes are a privileged operation — an
    // operator could otherwise forge envelopes owned by any tenant,
    // bypass state-machine invariants, or inject synthetic audit
    // trails. Admins write freely; non-admins must own every envelope
    // they submit (match on `ownership.tenant_id`). System principals
    // (internal callers) also pass.
    let is_admin = cairn_app::extractors::is_admin_principal(&principal);
    if !is_admin {
        let caller_tenant = principal.tenant().map(|t| t.tenant_id.clone());
        let Some(caller_tenant) = caller_tenant else {
            return Err(forbidden("missing tenant scope for event append"));
        };
        for envelope in &envelopes {
            let ownership_tenant = match &envelope.ownership {
                cairn_domain::tenancy::OwnershipKey::System => None,
                cairn_domain::tenancy::OwnershipKey::Tenant(k) => Some(&k.tenant_id),
                cairn_domain::tenancy::OwnershipKey::Workspace(k) => Some(&k.tenant_id),
                cairn_domain::tenancy::OwnershipKey::Project(k) => Some(&k.tenant_id),
            };
            match ownership_tenant {
                Some(t) if *t == caller_tenant => {}
                _ => {
                    return Err(forbidden(format!(
                        "event {} carries ownership outside caller tenant",
                        envelope.event_id.as_str(),
                    )));
                }
            }

            let payload_project = envelope.payload.project();
            if payload_project.tenant_id != caller_tenant {
                return Err(forbidden(format!(
                    "event {} payload project outside caller tenant",
                    envelope.event_id.as_str(),
                )));
            }

            let ownership_matches_payload = match &envelope.ownership {
                cairn_domain::tenancy::OwnershipKey::System => false,
                cairn_domain::tenancy::OwnershipKey::Tenant(k) => {
                    k.tenant_id == payload_project.tenant_id
                }
                cairn_domain::tenancy::OwnershipKey::Workspace(k) => {
                    k.tenant_id == payload_project.tenant_id
                        && k.workspace_id == payload_project.workspace_id
                }
                cairn_domain::tenancy::OwnershipKey::Project(k) => {
                    k.tenant_id == payload_project.tenant_id
                        && k.workspace_id == payload_project.workspace_id
                        && k.project_id == payload_project.project_id
                }
            };
            if !ownership_matches_payload {
                return Err(forbidden(format!(
                    "event {} ownership does not match payload project scope",
                    envelope.event_id.as_str(),
                )));
            }
        }
    }

    let mut results: Vec<AppendResult> = Vec::with_capacity(envelopes.len());

    for envelope in envelopes {
        let event_id = envelope.event_id.as_str().to_owned();

        // ── Notification hook ──────────────────────────────────────────────────
        // Inspect each event and push a notification for operator-relevant ones.
        {
            use cairn_domain::lifecycle::RunState;
            use cairn_domain::RuntimeEvent as E;
            use std::time::SystemTime;
            let now_ms = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            let notif_id = format!("notif-{}", &event_id[..event_id.len().min(16)]);

            let maybe_notif: Option<Notification> = match &envelope.payload {
                E::ApprovalRequested(e) => Some(Notification {
                    id: notif_id,
                    notif_type: NotifType::ApprovalRequested,
                    message: format!(
                        "Approval requested for {}",
                        e.run_id.as_ref().map(|r| r.as_str()).unwrap_or("a task"),
                    ),
                    entity_id: Some(e.approval_id.as_str().to_owned()),
                    href: "approvals".to_owned(),
                    read: false,
                    created_at: now_ms,
                }),
                E::ApprovalResolved(e) => Some(Notification {
                    id: notif_id,
                    notif_type: NotifType::ApprovalResolved,
                    message: format!(
                        "Approval {} — decision: {:?}",
                        e.approval_id.as_str(),
                        e.decision,
                    ),
                    entity_id: Some(e.approval_id.as_str().to_owned()),
                    href: "approvals".to_owned(),
                    read: false,
                    created_at: now_ms,
                }),
                E::RunStateChanged(e) => match &e.transition.to {
                    RunState::Completed => Some(Notification {
                        id: notif_id,
                        notif_type: NotifType::RunCompleted,
                        message: format!("Run {} completed", e.run_id.as_str()),
                        entity_id: Some(e.run_id.as_str().to_owned()),
                        href: format!("run/{}", e.run_id.as_str()),
                        read: false,
                        created_at: now_ms,
                    }),
                    RunState::Failed => Some(Notification {
                        id: notif_id,
                        notif_type: NotifType::RunFailed,
                        message: format!(
                            "Run {} failed{}",
                            e.run_id.as_str(),
                            e.failure_class
                                .as_ref()
                                .map(|f| format!(" ({f:?})"))
                                .unwrap_or_default(),
                        ),
                        entity_id: Some(e.run_id.as_str().to_owned()),
                        href: format!("run/{}", e.run_id.as_str()),
                        read: false,
                        created_at: now_ms,
                    }),
                    _ => None,
                },
                E::TaskStateChanged(e) => {
                    use cairn_domain::lifecycle::TaskState;
                    match &e.transition.to {
                        TaskState::DeadLettered | TaskState::RetryableFailed => {
                            Some(Notification {
                                id: notif_id,
                                notif_type: NotifType::TaskStuck,
                                message: format!(
                                    "Task {} is stuck ({:?})",
                                    e.task_id.as_str(),
                                    e.transition.to,
                                ),
                                entity_id: Some(e.task_id.as_str().to_owned()),
                                href: "tasks".to_owned(),
                                read: false,
                                created_at: now_ms,
                            })
                        }
                        _ => None,
                    }
                }
                _ => None,
            };

            if let Some(n) = maybe_notif {
                if let Ok(mut buf) = state.notifications.write() {
                    buf.push(n);
                }
            }
        }
        // ── End notification hook ──────────────────────────────────────────────

        // Idempotency check: if causation_id is set and already in the log,
        // return the existing position instead of appending.
        if let Some(ref cid) = envelope.causation_id {
            // Check InMemory first (fastest path); Pg check follows when configured.
            let existing = state.runtime.store.find_by_causation_id(cid.as_str()).await;
            match existing {
                Ok(Some(pos)) => {
                    results.push(AppendResult {
                        event_id,
                        position: pos.0,
                        appended: false,
                    });
                    continue;
                }
                Ok(None) => {} // not found — fall through to append
                Err(e) => return Err(internal_error(e.to_string())),
            }
        }

        // Append the single event.
        // Dual-write: persist to durable backend first, then update InMemory
        // so projections and SSE broadcasts stay current.
        if let Some(ref pg) = state.pg {
            if let Err(e) = pg.event_log.append(std::slice::from_ref(&envelope)).await {
                return Err(internal_error(format!("postgres append: {e}")));
            }
        } else if let Some(ref sq) = state.sqlite {
            if let Err(e) = sq.event_log.append(std::slice::from_ref(&envelope)).await {
                return Err(internal_error(format!("sqlite append: {e}")));
            }
        }
        // Always write to InMemory: updates projections + broadcasts to SSE subscribers.
        // Use `from_ref` so `envelope` stays borrowable for the
        // service-layer sync below without needing a clone.
        match state
            .runtime
            .store
            .append(std::slice::from_ref(&envelope))
            .await
        {
            Ok(positions) => {
                results.push(AppendResult {
                    event_id,
                    position: positions[0].0,
                    appended: true,
                });
            }
            Err(e) => return Err(internal_error(e.to_string())),
        }

        // ── Service-layer sync (split-brain guard) ─────────────────────────
        //
        // `/v1/events/append` is a projection-level write seam. It updates
        // the event log and the cairn-store projections but does NOT, by
        // default, populate FF state (the FabricTaskService / RunService /
        // SessionService durably own their state in Valkey via FCALL, not
        // in the cairn-store projection). For creation events that have a
        // service counterpart — `TaskCreated`, `RunCreated`, `SessionCreated`
        // — projection-only writes drift from service state: a subsequent
        // `POST /v1/tasks/:id/claim` hits FF, FF has no record of the task,
        // and the claim returns 404 even though `GET /v1/tasks` shows the
        // task in the projection.
        //
        // To keep the two views in sync we best-effort invoke the service
        // `submit` / `start` / `create` method after the log append
        // succeeds. The service-layer methods are idempotent on their
        // primary key — if FF already has the execution (normal replay
        // case), the call is a no-op and the bridge does not emit a
        // duplicate event. If FF does not have the execution (the
        // smoke_worker / backdoor / projection-repair case), the service
        // populates FF and the bridge emits its own canonical creation
        // event which the projection upserts without corruption.
        //
        // The sync is best-effort: the log write is the source of truth, so
        // a service-layer failure is logged but does not 5xx the response.
        // Callers who want a hard guarantee must use the service-layer
        // HTTP endpoints (`POST /v1/sessions`, `POST /v1/runs`,
        // `POST /v1/tasks`) — events/append is explicitly documented as a
        // projection-repair seam.
        sync_service_for_creation_event(&state, &envelope.payload).await;
    }

    Ok((StatusCode::CREATED, Json(results)))
}

/// Best-effort re-drive service-layer state from a freshly-appended
/// creation event. See the long-form comment at the call site for why
/// this is a best-effort, idempotent sync rather than a hard write
/// through the service.
async fn sync_service_for_creation_event(state: &AppState, payload: &cairn_domain::RuntimeEvent) {
    use cairn_domain::RuntimeEvent as E;
    match payload {
        E::SessionCreated(e) => {
            if let Err(err) = state
                .runtime
                .sessions
                .create(&e.project, e.session_id.clone())
                .await
            {
                // `AlreadyExists` on the service side is the expected
                // idempotent-replay outcome; everything else is a real
                // sync gap an operator should know about.
                if !is_already_exists(&err) {
                    tracing::warn!(
                        target: "cairn_app::events_append_sync",
                        session_id = %e.session_id,
                        err = %err,
                        "events/append SessionCreated service sync failed (best-effort)",
                    );
                }
            }
        }
        E::RunCreated(e) => {
            if let Err(err) = state
                .runtime
                .runs
                .start(
                    &e.project,
                    &e.session_id,
                    e.run_id.clone(),
                    e.parent_run_id.clone(),
                )
                .await
            {
                if !is_already_exists(&err) {
                    tracing::warn!(
                        target: "cairn_app::events_append_sync",
                        run_id = %e.run_id,
                        err = %err,
                        "events/append RunCreated service sync failed (best-effort)",
                    );
                }
            }
        }
        E::TaskCreated(e) => {
            // The TaskService trait requires `Option<&SessionId>`; pass
            // whatever the event carried and let the adapter derive from
            // `parent_run_id → run.session_id` when absent.
            let session_id = e.session_id.as_ref();
            if let Err(err) = state
                .runtime
                .tasks
                .submit(
                    &e.project,
                    session_id,
                    e.task_id.clone(),
                    e.parent_run_id.clone(),
                    e.parent_task_id.clone(),
                    0,
                )
                .await
            {
                if !is_already_exists(&err) {
                    tracing::warn!(
                        target: "cairn_app::events_append_sync",
                        task_id = %e.task_id,
                        err = %err,
                        "events/append TaskCreated service sync failed (best-effort)",
                    );
                }
            }
        }
        _ => {}
    }
}

/// Classify a `RuntimeError` as a benign duplicate-create outcome that
/// the best-effort sync should swallow without logging. The goal of the
/// sync is to make FF state catch up when missing, so replay of the
/// same creation event is the expected no-op path, not an incident.
///
/// Scoped narrowly on purpose:
/// * We do NOT match `RuntimeError::Conflict { .. }` blindly. The
///   Fabric adapter also maps FF claim-contention races to
///   `Conflict { entity: "execution", id: <code> }` via
///   `fabric_err_to_runtime::is_claim_contention`, so a bare
///   `Conflict` match would hide real contention bugs from the log.
///   The Conflict variant we care about here — "row already exists"
///   from an in-memory service impl — carries a message text with
///   `"already exists"` / `"duplicate"`, so the string match below
///   covers it without the over-broad variant match.
/// * String matching is tight: only `"already exists"`,
///   `"already_exists"`, and `"duplicate"` — NOT just `"exists"`, so
///   orthogonal errors like `"parent session does not exist"` still
///   surface as real sync gaps (per SEC-007 — do not over-match on
///   internal strings).
fn is_already_exists(err: &cairn_runtime::error::RuntimeError) -> bool {
    let s = err.to_string().to_ascii_lowercase();
    s.contains("already exists") || s.contains("already_exists") || s.contains("duplicate")
}
