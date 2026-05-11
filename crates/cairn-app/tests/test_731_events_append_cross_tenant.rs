//! #731 — `/v1/events/append` cross-tenant authorization.
//!
//! Codex's PR #731 closed a service-write disclosure path on
//! `POST /v1/events/append`: pre-fix, a non-admin caller could
//! submit an envelope they own (`OwnershipKey::Tenant(t1)`) whose
//! payload's `project` referenced a different tenant (`t2`),
//! driving the post-append `sync_service_for_creation_event` to
//! create a session/run/task in `t2`'s service state.
//!
//! Post-fix the handler enforces three rules for non-admin callers:
//!
//! 1. Envelope `ownership.tenant_id` must equal caller's tenant
//!    (existing).
//! 2. Envelope payload `project.tenant_id` must equal caller's
//!    tenant (new).
//! 3. Envelope `ownership` must align with the payload `project`
//!    at the granularity of `Tenant`, `Workspace`, or `Project`
//!    (new).
//!
//! This file pins the three rules with explicit assertions:
//! cross-tenant payload with own-tenant ownership → 403,
//! ownership/payload scope mismatch → 403, legitimate same-tenant
//! same-scope payload → 200 (positive control). Pre-fix, the first
//! two would return 200 + silently write into the victim tenant.

mod support;

use serde_json::json;
use support::live_fabric::LiveHarness;

/// Mint a non-admin operator token scoped to a tenant id. Mirrors
/// `test_733_defaults_tenant_isolation::mint_operator_token` and
/// `test_tenant_role_promote_revoke::mint_operator_token`.
async fn mint_operator_token(h: &LiveHarness, operator_id: &str, tenant_id: &str) -> String {
    let r = h
        .client()
        .post(format!("{}/v1/auth/tokens", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "operator_id": operator_id,
            "tenant_id": tenant_id,
            "name": format!("test-731-{operator_id}"),
        }))
        .send()
        .await
        .expect("POST /v1/auth/tokens");
    assert_eq!(
        r.status().as_u16(),
        201,
        "operator-token mint: {}",
        r.text().await.unwrap_or_default()
    );
    let body: serde_json::Value = r.json().await.unwrap();
    body["token"].as_str().unwrap().to_owned()
}

/// Build a SessionCreated envelope with caller-controlled
/// `ownership` (envelope-level) and `payload.project`.
fn session_created_envelope(
    event_id: &str,
    ownership_tenant: &str,
    ownership_workspace: &str,
    ownership_project: &str,
    payload_tenant: &str,
    payload_workspace: &str,
    payload_project: &str,
    session_id: &str,
) -> serde_json::Value {
    json!([{
        "event_id": event_id,
        "source": { "source_type": "runtime" },
        "ownership": {
            "scope": "project",
            "tenant_id": ownership_tenant,
            "workspace_id": ownership_workspace,
            "project_id": ownership_project,
        },
        "causation_id": null,
        "correlation_id": null,
        "payload": {
            "event": "session_created",
            "project": {
                "tenant_id": payload_tenant,
                "workspace_id": payload_workspace,
                "project_id": payload_project,
            },
            "session_id": session_id,
        },
    }])
}

/// Rule 2: payload tenant must match caller tenant.
///
/// Caller has tenant `B`, submits envelope they own (ownership:
/// `Tenant(B)`) but payload's `project.tenant_id = A`. Pre-fix,
/// the handler accepted (200) and `sync_service_for_creation_event`
/// would have created a SessionRecord in tenant A's projection.
/// Post-fix: 403 with message naming the payload-project mismatch.
#[tokio::test]
async fn cross_tenant_payload_rejected_for_non_admin() {
    let h = LiveHarness::setup().await;
    let tenant_a = format!("tenant-a-{}", uuid::Uuid::new_v4());
    let tenant_b = format!("tenant-b-{}", uuid::Uuid::new_v4());
    let op_b = format!("op_b_{}", uuid::Uuid::new_v4());
    let token_b = mint_operator_token(&h, &op_b, &tenant_b).await;

    // Operator B owns the envelope (ownership = Tenant B's
    // project) but payload targets tenant A.
    let envelope = session_created_envelope(
        &format!("evt_731_xtenant_{}", uuid::Uuid::new_v4()),
        &tenant_b,
        "ws_b",
        "proj_b",
        &tenant_a,
        "ws_a",
        "proj_a",
        "sess_731_xtenant",
    );

    let r = h
        .client()
        .post(format!("{}/v1/events/append", h.base_url))
        .bearer_auth(&token_b)
        .json(&envelope)
        .send()
        .await
        .expect("POST /v1/events/append");

    assert_eq!(
        r.status().as_u16(),
        403,
        "cross-tenant payload must 403, got {}: {}",
        r.status(),
        r.text().await.unwrap_or_default(),
    );
}

/// Rule 3: ownership scope must align with payload project scope.
///
/// Caller has tenant `B`, submits envelope with
/// `ownership = Workspace(B/ws_x)` but payload targets a different
/// workspace (`B/ws_y/proj_y`). Tenant matches at the top level
/// but workspace-scoped ownership should not authorize a write
/// into a different workspace.
#[tokio::test]
async fn workspace_scope_mismatch_rejected_for_non_admin() {
    let h = LiveHarness::setup().await;
    let tenant_b = format!("tenant-b-{}", uuid::Uuid::new_v4());
    let op_b = format!("op_b_{}", uuid::Uuid::new_v4());
    let token_b = mint_operator_token(&h, &op_b, &tenant_b).await;

    // Same tenant, but workspace-scoped ownership says ws_x while
    // payload targets ws_y.
    let envelope = json!([{
        "event_id": format!("evt_731_wsmis_{}", uuid::Uuid::new_v4()),
        "source": { "source_type": "runtime" },
        "ownership": {
            "scope": "workspace",
            "tenant_id": tenant_b,
            "workspace_id": "ws_x",
        },
        "causation_id": null,
        "correlation_id": null,
        "payload": {
            "event": "session_created",
            "project": {
                "tenant_id": tenant_b,
                "workspace_id": "ws_y",
                "project_id": "proj_y",
            },
            "session_id": "sess_731_wsmis",
        },
    }]);

    let r = h
        .client()
        .post(format!("{}/v1/events/append", h.base_url))
        .bearer_auth(&token_b)
        .json(&envelope)
        .send()
        .await
        .expect("POST /v1/events/append");

    assert_eq!(
        r.status().as_u16(),
        403,
        "ownership/payload workspace mismatch must 403, got {}: {}",
        r.status(),
        r.text().await.unwrap_or_default(),
    );
}

/// Positive control: legitimate same-tenant same-scope envelope
/// from a non-admin caller still succeeds. Without this, the gate
/// could be inadvertently strict and block all non-admin appends.
///
/// Note: `sync_service_for_creation_event` runs best-effort after
/// the append — failure does not flip the response status — so the
/// 200 here proves the authz gate accepted the envelope, not that
/// the downstream sync wrote a row. Functional verification of the
/// downstream sync is covered by
/// `test_events_append_service_sync.rs::test_events_append_task_created_populates_service`
/// (which uses an admin token, exercising the bypass path).
#[tokio::test]
async fn same_tenant_same_scope_succeeds_for_non_admin() {
    let h = LiveHarness::setup().await;
    let tenant_b = format!("tenant-b-{}", uuid::Uuid::new_v4());
    let op_b = format!("op_b_{}", uuid::Uuid::new_v4());
    let token_b = mint_operator_token(&h, &op_b, &tenant_b).await;

    let envelope = session_created_envelope(
        &format!("evt_731_legit_{}", uuid::Uuid::new_v4()),
        &tenant_b,
        "ws_b",
        "proj_b",
        &tenant_b,
        "ws_b",
        "proj_b",
        "sess_731_legit",
    );

    let r = h
        .client()
        .post(format!("{}/v1/events/append", h.base_url))
        .bearer_auth(&token_b)
        .json(&envelope)
        .send()
        .await
        .expect("POST /v1/events/append");

    let status = r.status().as_u16();
    assert!(
        status == 200 || status == 201,
        "legitimate same-tenant non-admin append must succeed (200/201), got {}: {}",
        status,
        r.text().await.unwrap_or_default(),
    );
}

/// Admin-bypass: admin can submit cross-tenant envelopes (the gate
/// only fires on `is_admin == false`). Pin this so a future
/// "tighten admin too" change is intentional, not accidental.
#[tokio::test]
async fn admin_bypass_allows_cross_tenant_for_dev_admin() {
    let h = LiveHarness::setup().await;
    let tenant_a = format!("tenant-a-{}", uuid::Uuid::new_v4());
    let tenant_b = format!("tenant-b-{}", uuid::Uuid::new_v4());

    // Admin sends a cross-tenant envelope (ownership: Tenant A,
    // payload: Tenant B). The gate's `is_admin` short-circuit lets
    // this pass; this is intentional for migrations and operator
    // tooling.
    let envelope = session_created_envelope(
        &format!("evt_731_admin_{}", uuid::Uuid::new_v4()),
        &tenant_a,
        "ws_a",
        "proj_a",
        &tenant_b,
        "ws_b",
        "proj_b",
        "sess_731_admin",
    );

    let r = h
        .client()
        .post(format!("{}/v1/events/append", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&envelope)
        .send()
        .await
        .expect("POST /v1/events/append");

    let status = r.status().as_u16();
    assert!(
        status == 200 || status == 201,
        "admin must still bypass tenant alignment checks (200/201), got {}: {}",
        status,
        r.text().await.unwrap_or_default(),
    );
}
