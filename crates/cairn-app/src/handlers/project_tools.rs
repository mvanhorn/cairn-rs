//! `GET /v1/projects/:project/tools` — per-project tool inventory
//! (closes [#799](https://github.com/avifenesh/cairn-rs/issues/799)).
//!
//! Immediate caller: RFC 031 role editor's tool-allowlist autocomplete.
//! Generalises to any operator surface that needs "what tools does this
//! project see right now" (webhook action config, eval scorecards,
//! plugin-health panels).
//!
//! ## Semantics
//!
//! - Every tool in the `BuiltinToolRegistry` (Core + Registered + Deferred)
//!   is included with `source = "builtin"` and the registry's `tier`.
//! - Every tool exposed by a plugin that is currently **enabled** for this
//!   project (RFC 015 project enablement) is included with
//!   `source = "plugin:<plugin_id>"` and `tier = "registered"`. Plugin tools
//!   are always Registered-tier by construction — they are advertised via
//!   the plugin-handshake tool list and are neither Core (not shipped with
//!   cairn-rs) nor Deferred (if the plugin is enabled the operator has
//!   opted in).
//! - Plugin tools respect the enablement's `tool_allowlist`: when a
//!   non-empty allowlist is present, only listed tool ids surface.
//! - Results sorted alphabetically by `id` so the UI can diff / stable-sort.
//! - No pagination — bounded small set per project (matches
//!   `/v1/plugins/:id/tools`).
//! - No admin guard; any authenticated operator whose tenant scope covers
//!   the target tenant can read.
//! - `parameters_schema` is denormalised onto each item so the editor
//!   doesn't round-trip a second GET for every hover preview.

use std::sync::Arc;

use axum::{
    extract::{Extension, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;

use cairn_api::auth::AuthPrincipal;

use crate::extractors::enforce_project_tenant;
use crate::marketplace_routes::project_key_from_path;
use crate::AppState;

/// Wire shape of one row in the response `items[]`.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct ProjectToolItem {
    pub(crate) id: String,
    /// `"builtin"` or `"plugin:<plugin_id>"`.
    pub(crate) source: String,
    /// `"core"` / `"registered"` / `"deferred"`. Plugin-sourced tools
    /// always report `"registered"`.
    pub(crate) tier: &'static str,
    pub(crate) description: String,
    pub(crate) parameters_schema: serde_json::Value,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ProjectToolsResponse {
    pub(crate) items: Vec<ProjectToolItem>,
    pub(crate) total: usize,
    pub(crate) has_more: bool,
}

fn tier_str(tier: cairn_tools::builtins::ToolTier) -> &'static str {
    match tier {
        cairn_tools::builtins::ToolTier::Core => "core",
        cairn_tools::builtins::ToolTier::Registered => "registered",
        cairn_tools::builtins::ToolTier::Deferred => "deferred",
    }
}

pub(crate) async fn list_project_tools_handler(
    State(state): State<Arc<AppState>>,
    Extension(principal): Extension<AuthPrincipal>,
    Path(project_raw): Path<String>,
) -> Response {
    let project = match project_key_from_path(&project_raw) {
        Ok(p) => p,
        Err(message) => {
            return crate::errors::AppApiError::new(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                message,
            )
            .into_response();
        }
    };
    if !enforce_project_tenant(&principal, &project) {
        return crate::errors::tenant_scope_mismatch_error().into_response();
    }

    let mut items: Vec<ProjectToolItem> = Vec::new();

    // ── Built-ins ──
    //
    // `state.tool_registry` is `None` on test fixtures that never wire a
    // registry (cairn-app integration tests that don't exercise tool
    // dispatch). Treat missing registry as "no built-ins surfaced"
    // rather than failing the call — plugin tools still land below.
    if let Some(registry) = &state.tool_registry {
        for desc in registry.list_all() {
            items.push(ProjectToolItem {
                id: desc.name,
                source: "builtin".to_owned(),
                tier: tier_str(desc.tier),
                description: desc.description,
                parameters_schema: desc.parameters_schema,
            });
        }
    }

    // ── Plugin-provided tools ──
    //
    // Join `marketplace.enablements_for_project(project)` with
    // `plugin_host.get_tools(id)`. Apply the enablement's
    // `tool_allowlist` when present. Drop plugins whose host
    // handshake never succeeded (get_tools returns Err).
    // Extract and drop the marketplace lock before taking the plugin-host
    // lock. `enablements_for_project` returns `Vec<&PluginEnablement>`, so
    // we must clone the fields we keep — the refs die with the guard.
    // Plugin tools per-project are O(<20) today; the allocation is
    // immaterial and the short lock window lets other marketplace
    // readers proceed while we wait on the host lock.
    let enablements: Vec<(String, Option<std::collections::HashSet<String>>)> = {
        let marketplace = state.marketplace.lock().unwrap_or_else(|e| e.into_inner());
        let out = marketplace
            .enablements_for_project(&project)
            .iter()
            .map(|e| {
                (
                    e.plugin_id.clone(),
                    e.tool_allowlist
                        .as_ref()
                        .map(|list| list.iter().cloned().collect()),
                )
            })
            .collect();
        drop(marketplace);
        out
    };

    if !enablements.is_empty() {
        if let Ok(host) = state.plugin_host.lock() {
            for (plugin_id, allowlist) in enablements {
                let Ok(plugin_tools) = host.get_tools(&plugin_id) else {
                    // Plugin registered but not yet handshaken / host
                    // process dead. Silently skip — the UI renders
                    // what's actually callable today.
                    continue;
                };
                for t in plugin_tools {
                    if let Some(a) = &allowlist {
                        if !a.contains(&t.name) {
                            continue;
                        }
                    }
                    items.push(ProjectToolItem {
                        id: t.name,
                        source: format!("plugin:{plugin_id}"),
                        tier: "registered",
                        description: t.description,
                        parameters_schema: t.parameters_schema,
                    });
                }
            }
        }
    }

    items.sort_by(|a, b| a.id.cmp(&b.id));

    let total = items.len();
    let body = ProjectToolsResponse {
        items,
        total,
        has_more: false,
    };
    (StatusCode::OK, Json(body)).into_response()
}
