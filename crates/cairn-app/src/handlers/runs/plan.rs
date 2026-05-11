//! Plan-review handlers + DTOs (RFC 018).
//!
//! Covers:
//! - `POST /v1/runs/:plan_run_id/approve`
//! - `POST /v1/runs/:plan_run_id/reject`
//! - `POST /v1/runs/:plan_run_id/revise`

use std::sync::Arc;

use axum::{
    extract::{Extension, Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};

use cairn_api::auth::AuthPrincipal;
use cairn_domain::RunId;

use crate::current_event_head;
use crate::errors::{
    run_not_found_response, runtime_error_response, validation_error_response, AppApiError,
};
use crate::extractors::TenantScope;
use crate::helpers::load_run_visible_to_tenant;
use crate::publish_runtime_frames_since;
use crate::state::AppState;
use cairn_store::EventLog;

// ── Plan review request DTOs (RFC 018) — #427 ────────────────────────────────
//
// Before #427 these three endpoints accepted `serde_json::Value` and
// plucked fields by name. Consequences logged in the audit: unknown
// fields silently accepted (typos in UI code went through audit as
// "plan approved with no comment"), wrong types silently ignored,
// OpenAPI could not describe the shape, no input size cap.
//
// Typed structs with `deny_unknown_fields` turn typos into 422s and
// let utoipa derive a ToSchema so the OpenAPI spec is accurate.

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ApprovePlanRequest {
    #[serde(default)]
    pub(crate) reviewer_comments: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RejectPlanRequest {
    /// Optional operator-provided reason. When absent the handler
    /// substitutes "rejected by operator" (pre-#427 behaviour).
    #[serde(default)]
    pub(crate) reason: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RevisePlanRequest {
    /// Required — a revise without reviewer_comments is a no-op from
    /// the plan-author's perspective.
    pub(crate) reviewer_comments: String,
}

/// POST /v1/runs/:plan_run_id/approve
pub(crate) async fn approve_plan_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Extension(principal): Extension<AuthPrincipal>,
    Path(plan_run_id): Path<String>,
    // PR #555 review (Copilot): surface `deny_unknown_fields` / wrong-
    // type rejections through the canonical `Error` envelope rather
    // than axum's default body. Matches the OpenAPI 422 contract.
    body: Result<Json<ApprovePlanRequest>, axum::extract::rejection::JsonRejection>,
) -> impl IntoResponse {
    use cairn_domain::events::PlanApproved;
    use cairn_runtime::make_envelope;

    let Json(body) = match body {
        Ok(b) => b,
        Err(err) => return crate::errors::json_rejection_response(err),
    };
    let run_id = RunId::new(&plan_run_id);

    // T6a-C2: tenant scope check.
    let run = match load_run_visible_to_tenant(state.as_ref(), &tenant_scope, &run_id).await {
        Ok(Some(r)) => r,
        Ok(None) => {
            return run_not_found_response();
        }
        Err(response) => return response,
    };

    // #427: reviewer_comments is now a typed Option<String>. Normalise
    // empty strings to None so audit rows carry `null` rather than `""`.
    //
    // Copilot review: this IS a semantic change from the pre-typing
    // behaviour (not a preservation). The old `Value::as_str()` would
    // have returned `Some("")` for `""`, and `.map(str::to_owned)`
    // produced `Some("".to_owned())`. We intentionally collapse empty
    // to None now because an empty `reviewer_comments` audit row is
    // operator noise, not a signal worth preserving.
    let reviewer_comments = body.reviewer_comments.filter(|s| !s.is_empty());
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    // T6a-H7: use the authenticated principal as the actor rather than a
    // hardcoded "operator" literal. Pre-fix, every plan approval was
    // attributed to a fake user — breaking audit integrity in team
    // deployments where multiple operators review plans.
    let evt = make_envelope(cairn_domain::RuntimeEvent::PlanApproved(PlanApproved {
        project: run.project.clone(),
        plan_run_id: run_id,
        approved_by: cairn_domain::OperatorId::new(crate::handlers::admin::audit_actor_id(
            &principal,
        )),
        reviewer_comments,
        approved_at: now_ms,
    }));

    if let Err(e) = state.runtime.store.append(&[evt]).await {
        return AppApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "store_error",
            e.to_string(),
        )
        .into_response();
    }

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "plan_run_id": plan_run_id,
            "status": "approved",
            "next_step": "create_execute_run",
        })),
    )
        .into_response()
}

/// POST /v1/runs/:plan_run_id/reject
pub(crate) async fn reject_plan_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Extension(principal): Extension<AuthPrincipal>,
    Path(plan_run_id): Path<String>,
    // PR #555 review (Copilot): canonical Error envelope on rejections.
    body: Result<Json<RejectPlanRequest>, axum::extract::rejection::JsonRejection>,
) -> impl IntoResponse {
    use cairn_domain::events::PlanRejected;
    use cairn_runtime::make_envelope;

    let Json(body) = match body {
        Ok(b) => b,
        Err(err) => return crate::errors::json_rejection_response(err),
    };
    let run_id = RunId::new(&plan_run_id);

    // T6a-C2: tenant scope check.
    let run = match load_run_visible_to_tenant(state.as_ref(), &tenant_scope, &run_id).await {
        Ok(Some(r)) => r,
        Ok(None) => {
            return run_not_found_response();
        }
        Err(response) => return response,
    };

    // #427: reason is now an Option<String>; default to "rejected by
    // operator" when the caller omits it OR sends an empty string.
    //
    // Copilot review: this IS a semantic change from the pre-typing
    // path. The old `.as_str().unwrap_or("rejected by operator")`
    // returned the literal `""` when the client sent an empty
    // string — it only defaulted when the field was absent. We
    // intentionally collapse empty-to-default here because an empty
    // `reason` audit row is operator noise, not a meaningful signal.
    let reason = body
        .reason
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "rejected by operator".to_owned());
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    // T6a-H7: audit with the real principal, not a hardcoded "operator".
    let evt = make_envelope(cairn_domain::RuntimeEvent::PlanRejected(PlanRejected {
        project: run.project.clone(),
        plan_run_id: run_id,
        rejected_by: cairn_domain::OperatorId::new(crate::handlers::admin::audit_actor_id(
            &principal,
        )),
        reason,
        rejected_at: now_ms,
    }));

    if let Err(e) = state.runtime.store.append(&[evt]).await {
        return AppApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "store_error",
            e.to_string(),
        )
        .into_response();
    }

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "plan_run_id": plan_run_id,
            "status": "rejected",
        })),
    )
        .into_response()
}

/// POST /v1/runs/:plan_run_id/revise
pub(crate) async fn revise_plan_handler(
    State(state): State<Arc<AppState>>,
    tenant_scope: TenantScope,
    Path(plan_run_id): Path<String>,
    // PR #555 review (Copilot): canonical Error envelope on rejections.
    body: Result<Json<RevisePlanRequest>, axum::extract::rejection::JsonRejection>,
) -> impl IntoResponse {
    use cairn_domain::events::PlanRevisionRequested;
    use cairn_runtime::make_envelope;

    let Json(body) = match body {
        Ok(b) => b,
        Err(err) => return crate::errors::json_rejection_response(err),
    };
    let original_run_id = RunId::new(&plan_run_id);

    // T6a-C2: tenant scope check.
    let original_run =
        match load_run_visible_to_tenant(state.as_ref(), &tenant_scope, &original_run_id).await {
            Ok(Some(r)) => r,
            Ok(None) => {
                return run_not_found_response();
            }
            Err(response) => return response,
        };

    // #427: reviewer_comments is now a typed String. An empty string
    // is still a client error (a revise without comments is the same
    // as not reviewing).
    let reviewer_comments = body.reviewer_comments;
    if reviewer_comments.is_empty() {
        return validation_error_response("reviewer_comments is required for revise");
    }

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    // Create a new Plan-mode run for the revision.
    //
    // Copilot review (PR #567): ms-resolution uniqueness collides under
    // rapid retries in the same millisecond (operator double-click,
    // retry after 502). UUID v4 is collision-proof; keep the
    // `run_..._rev` suffix so operators can still filter revision runs
    // in the UI by id pattern. The `now_ms` value is still used below
    // for the event timestamp.
    let new_run_id = RunId::new(format!("run_{}_rev", uuid::Uuid::new_v4()));
    let before = current_event_head(&state).await;
    match state
        .runtime
        .runs
        .start(
            &original_run.project,
            &original_run.session_id,
            new_run_id.clone(),
            Some(original_run_id.clone()),
        )
        .await
    {
        Ok(_) => {}
        Err(err) => return runtime_error_response(err),
    }

    // Emit PlanRevisionRequested event.
    let evt = make_envelope(cairn_domain::RuntimeEvent::PlanRevisionRequested(
        PlanRevisionRequested {
            project: original_run.project.clone(),
            original_plan_run_id: original_run_id,
            new_plan_run_id: new_run_id.clone(),
            reviewer_comments,
            requested_at: now_ms,
        },
    ));

    if let Err(e) = state.runtime.store.append(&[evt]).await {
        return AppApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "store_error",
            e.to_string(),
        )
        .into_response();
    }

    publish_runtime_frames_since(&state, before).await;

    (
        StatusCode::CREATED,
        Json(serde_json::json!({
            "plan_run_id": plan_run_id,
            "new_plan_run_id": new_run_id.as_str(),
            "status": "revision_requested",
        })),
    )
        .into_response()
}
