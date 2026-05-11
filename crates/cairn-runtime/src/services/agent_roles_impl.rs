//! RFC 031: `AgentRoleService` — owns the define / retract / resolve
//! paths for operator-defined agent roles.
//!
//! Per RFC 031 §D14 there is no cross-node warm cache. `resolve` is
//! a direct read against the projection on every call; on pg/sqlite
//! the projection is already memory-resident (event-replay seeds
//! `InMemoryStore` at boot per CLAUDE.md RFC-025 Phase 4), so the
//! read cost is a HashMap get with a fallback to `default_roles()`.
//!
//! The per-run `list` memoisation that §D14 layer 2 describes lives
//! on `OrchestrationContext::agent_role_list_cache` (landed
//! alongside the service in this same PR-A); the service returns
//! `Vec<ResolvedRole>` on every call and relies on the caller to
//! memoise if they want to.

use std::sync::Arc;

use async_trait::async_trait;
use cairn_domain::agent_roles::{default_roles, AgentRole};
use cairn_domain::events::{AgentRoleDefined, AgentRoleRetracted};
use cairn_domain::{OperatorId, ProjectKey, RuntimeEvent};
use cairn_store::projections::{AgentRoleReadModel, AgentRoleRecord};
use cairn_store::EventLog;
use serde::Serialize;

use super::event_helpers::make_envelope;
use crate::error::RuntimeError;

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// RFC 031 §Runtime Resolution Delta: envelope returned by `list`.
///
/// Carries the role alongside its provenance — whether it came from
/// the compile-time `default_roles()` or from the projection, and
/// (for projection rows) who defined it and when. HTTP handlers in
/// PR-B wrap `ResolvedRole` into the GET-list response envelope.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ResolvedRole {
    pub role: AgentRole,
    pub source: RoleSource,
    /// `Some("reviewer")` when `source == CustomShadow`; `None`
    /// otherwise.
    pub shadows_builtin: Option<String>,
    /// `None` for `source == Builtin`; `Some(ms)` for rows in the
    /// projection.
    pub defined_at: Option<u64>,
    /// `None` for `source == Builtin`; `Some(op-id)` for rows in
    /// the projection.
    pub defined_by: Option<OperatorId>,
}

/// RFC 031 §D8: every role shipped through `list` carries a source
/// tag so the UI can render the three cases distinctly.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RoleSource {
    /// A compile-time role from `default_roles()` with no active
    /// project-scoped shadow.
    Builtin,
    /// A project-scoped operator-defined role whose id does NOT
    /// match any built-in.
    Custom,
    /// A project-scoped operator-defined role whose id shadows a
    /// built-in.
    CustomShadow,
}

/// RFC 031 §Scope: filter argument on `list`. `All` returns the
/// merged view (built-ins + custom + shadows); the named filters
/// narrow by `source` tag.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceFilter {
    All,
    Builtin,
    Custom,
    /// Shorthand for `Custom | CustomShadow` — the full operator-
    /// written set regardless of whether it shadows a built-in. Used
    /// by the audit UI's "custom roles" tab.
    AnyCustom,
    CustomShadow,
}

/// RFC 031 `AgentRoleService` contract.
///
/// `define` / `retract` emit the corresponding `RuntimeEvent`
/// variant and return the resolved view.
/// `resolve` returns the active role for a `(project, role_id)`
/// pair — never fails, always returns a role (generic fallback per
/// §D7).
/// `list` returns the merged view filtered by `source`.
#[async_trait]
pub trait AgentRoleService: Send + Sync {
    async fn define(
        &self,
        project: &ProjectKey,
        role: AgentRole,
        actor: OperatorId,
    ) -> Result<ResolvedRole, RuntimeError>;

    async fn retract(
        &self,
        project: &ProjectKey,
        role_id: &str,
        actor: OperatorId,
    ) -> Result<(), RuntimeError>;

    async fn resolve(&self, project: &ProjectKey, role_id: &str)
        -> Result<AgentRole, RuntimeError>;

    async fn list(
        &self,
        project: &ProjectKey,
        filter: SourceFilter,
    ) -> Result<Vec<ResolvedRole>, RuntimeError>;
}

/// Implementation of `AgentRoleService` backed by the `cairn-store`
/// event log and the `project_agent_roles` projection.
///
/// `S` must implement both [`EventLog`] (for define / retract
/// appends) and [`AgentRoleReadModel`] (for resolve / list reads).
/// `InMemoryStore` satisfies both in PR-B; pg/sqlite parity is the
/// PR-B2 follow-up.
///
/// Behaviour:
/// * `define` appends `AgentRoleDefined` and returns the
///   freshly-projected row view. The projection is written
///   synchronously by the store's `apply_projection`, so the event
///   append and the row insert land in the same `.append` call.
/// * `retract` appends `AgentRoleRetracted`; the projection row
///   stays in place with `retracted_at = Some(_)`.
/// * `resolve` reads the active projection row; falls back to
///   `default_roles()` when no row exists for `(project, role_id)`;
///   falls back to the generic role verbatim per §D7 when the
///   requested id is unknown.
/// * `list` merges the project's active custom rows (which may
///   shadow built-ins) with the remaining built-ins, then applies
///   the `SourceFilter`.
pub struct AgentRoleServiceImpl<S> {
    store: Arc<S>,
}

impl<S> AgentRoleServiceImpl<S> {
    pub fn new(store: Arc<S>) -> Self {
        Self { store }
    }
}

/// Pick the resolved built-in by id if it exists, else the generic
/// role verbatim (§D7). Single `default_roles()` call: the primary
/// lookup short-circuits when the requested id matches; only on
/// fallthrough do we scan the same vec for `"generic"`.
fn builtin_fallback(role_id: &str) -> AgentRole {
    let roles = default_roles();
    if let Some(role) = roles.iter().find(|r| r.role_id == role_id) {
        return role.clone();
    }
    roles
        .into_iter()
        .find(|r| r.role_id == "generic")
        .expect("generic role must exist per #775")
}

fn is_builtin_id(role_id: &str) -> bool {
    default_roles().iter().any(|r| r.role_id == role_id)
}

/// Convert a projection row into the service-layer envelope.
fn record_to_resolved(row: AgentRoleRecord) -> ResolvedRole {
    let source = if row.shadows_builtin.is_some() {
        RoleSource::CustomShadow
    } else {
        RoleSource::Custom
    };
    ResolvedRole {
        role: row.role,
        source,
        shadows_builtin: row.shadows_builtin,
        defined_at: Some(row.defined_at),
        defined_by: Some(row.defined_by),
    }
}

#[async_trait]
impl<S> AgentRoleService for AgentRoleServiceImpl<S>
where
    S: EventLog + AgentRoleReadModel + 'static,
{
    async fn define(
        &self,
        project: &ProjectKey,
        role: AgentRole,
        actor: OperatorId,
    ) -> Result<ResolvedRole, RuntimeError> {
        let shadows_builtin = if is_builtin_id(&role.role_id) {
            Some(role.role_id.clone())
        } else {
            None
        };
        let at_ms = now_ms();
        let role_id = role.role_id.clone();
        let event = make_envelope(RuntimeEvent::AgentRoleDefined(AgentRoleDefined {
            project: project.clone(),
            role,
            shadows_builtin,
            defined_by: actor,
            at_ms,
        }));
        self.store.append(&[event]).await?;

        // Read back the projection row we just wrote. `apply_projection`
        // ran synchronously during `append`, so the row is present
        // and active. `get_active` returning `None` here is an
        // invariant violation; surface as `RuntimeError::Internal`.
        let row = self
            .store
            .get_active(project, &role_id)
            .await
            .map_err(|e| RuntimeError::Internal(e.to_string()))?
            .ok_or_else(|| {
                RuntimeError::Internal(format!(
                    "AgentRoleDefined({role_id}) appended but projection row not found"
                ))
            })?;
        Ok(record_to_resolved(row))
    }

    async fn retract(
        &self,
        project: &ProjectKey,
        role_id: &str,
        actor: OperatorId,
    ) -> Result<(), RuntimeError> {
        let event = make_envelope(RuntimeEvent::AgentRoleRetracted(AgentRoleRetracted {
            project: project.clone(),
            role_id: role_id.to_owned(),
            retracted_by: actor,
            at_ms: now_ms(),
        }));
        self.store.append(&[event]).await?;
        Ok(())
    }

    async fn resolve(
        &self,
        project: &ProjectKey,
        role_id: &str,
    ) -> Result<AgentRole, RuntimeError> {
        // §D14 §D7: projection first → built-in → generic verbatim.
        if let Some(row) = self
            .store
            .get_active(project, role_id)
            .await
            .map_err(|e| RuntimeError::Internal(e.to_string()))?
        {
            return Ok(row.role);
        }
        Ok(builtin_fallback(role_id))
    }

    async fn list(
        &self,
        project: &ProjectKey,
        filter: SourceFilter,
    ) -> Result<Vec<ResolvedRole>, RuntimeError> {
        let custom_rows = self
            .store
            .list_active(project)
            .await
            .map_err(|e| RuntimeError::Internal(e.to_string()))?;

        // Which built-in ids are currently shadowed?
        let shadowed: std::collections::HashSet<String> = custom_rows
            .iter()
            .filter_map(|r| r.shadows_builtin.clone())
            .collect();

        // Start with custom rows (custom + custom_shadow) — they win
        // on any id they shadow.
        let mut merged: Vec<ResolvedRole> =
            custom_rows.into_iter().map(record_to_resolved).collect();

        // Append built-ins that are NOT shadowed by an active custom row.
        for role in default_roles() {
            if !shadowed.contains(&role.role_id) {
                merged.push(ResolvedRole {
                    role,
                    source: RoleSource::Builtin,
                    shadows_builtin: None,
                    defined_at: None,
                    defined_by: None,
                });
            }
        }

        merged.sort_by(|a, b| a.role.role_id.cmp(&b.role.role_id));

        let filtered: Vec<ResolvedRole> = merged
            .into_iter()
            .filter(|r| match filter {
                SourceFilter::All => true,
                SourceFilter::Builtin => matches!(r.source, RoleSource::Builtin),
                SourceFilter::Custom => matches!(r.source, RoleSource::Custom),
                SourceFilter::AnyCustom => {
                    matches!(r.source, RoleSource::Custom | RoleSource::CustomShadow)
                }
                SourceFilter::CustomShadow => matches!(r.source, RoleSource::CustomShadow),
            })
            .collect();
        Ok(filtered)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_domain::agent_roles::AgentRoleTier;
    use cairn_domain::{ProjectId, TenantId, WorkspaceId};
    use cairn_store::InMemoryStore;

    fn project() -> ProjectKey {
        ProjectKey {
            tenant_id: TenantId::new("t"),
            workspace_id: WorkspaceId::new("w"),
            project_id: ProjectId::new("p"),
        }
    }

    fn svc() -> AgentRoleServiceImpl<InMemoryStore> {
        AgentRoleServiceImpl::new(Arc::new(InMemoryStore::new()))
    }

    #[tokio::test]
    async fn define_custom_role_emits_event_and_returns_custom_source() {
        let service = svc();
        let role = AgentRole::new("pr-reviewer", "PR Reviewer", AgentRoleTier::Standard);
        let resolved = service
            .define(&project(), role.clone(), OperatorId::new("op-a"))
            .await
            .expect("define");
        assert_eq!(resolved.source, RoleSource::Custom);
        assert!(resolved.shadows_builtin.is_none());
        assert_eq!(resolved.defined_by, Some(OperatorId::new("op-a")));
    }

    #[tokio::test]
    async fn define_with_builtin_id_reports_custom_shadow_source() {
        let service = svc();
        // Shadow the `reviewer` built-in.
        let role = AgentRole::new("reviewer", "Custom Reviewer", AgentRoleTier::Standard);
        let resolved = service
            .define(&project(), role, OperatorId::new("op-a"))
            .await
            .unwrap();
        assert_eq!(resolved.source, RoleSource::CustomShadow);
        assert_eq!(resolved.shadows_builtin.as_deref(), Some("reviewer"));
    }

    #[tokio::test]
    async fn resolve_returns_builtin_by_id_when_projection_empty() {
        // PR-A behaviour: resolve always returns the built-in because
        // the projection reader is PR-B work. Test pins this.
        let service = svc();
        let role = service.resolve(&project(), "reviewer").await.unwrap();
        assert_eq!(role.role_id, "reviewer");
    }

    #[tokio::test]
    async fn resolve_unknown_id_returns_generic_verbatim() {
        // §D7: unknown role id falls through to the generic role
        // verbatim — returned role's id is "generic", not the
        // requested id.
        let service = svc();
        let role = service.resolve(&project(), "nonexistent").await.unwrap();
        assert_eq!(role.role_id, "generic");
    }

    #[tokio::test]
    async fn list_all_returns_six_builtins_alphabetical() {
        let service = svc();
        let items = service.list(&project(), SourceFilter::All).await.unwrap();
        // #806 added `status-checker` to the original five built-ins.
        assert_eq!(items.len(), 6);
        let ids: Vec<&str> = items.iter().map(|i| i.role.role_id.as_str()).collect();
        // Alphabetical ordering by role_id.
        assert_eq!(
            ids,
            vec![
                "executor",
                "generic",
                "orchestrator",
                "researcher",
                "reviewer",
                "status-checker",
            ]
        );
        assert!(items
            .iter()
            .all(|r| matches!(r.source, RoleSource::Builtin)));
    }

    #[tokio::test]
    async fn list_custom_filter_returns_empty_when_only_builtins() {
        let service = svc();
        let items = service
            .list(&project(), SourceFilter::Custom)
            .await
            .unwrap();
        assert!(items.is_empty());
    }

    #[tokio::test]
    async fn retract_appends_retracted_event() {
        let service = svc();
        service
            .retract(&project(), "pr-reviewer", OperatorId::new("op-a"))
            .await
            .expect("retract");
    }

    // ── PR-B: projection-aware behaviour ──────────────────────────

    #[tokio::test]
    async fn define_then_resolve_reads_custom_role() {
        let service = svc();
        let role = AgentRole::new("pr-reviewer", "PR Reviewer", AgentRoleTier::Standard)
            .with_description("Reviews PRs.");
        service
            .define(&project(), role.clone(), OperatorId::new("op-a"))
            .await
            .unwrap();
        let resolved = service.resolve(&project(), "pr-reviewer").await.unwrap();
        assert_eq!(resolved.role_id, "pr-reviewer");
        assert_eq!(resolved.description, "Reviews PRs.");
    }

    #[tokio::test]
    async fn define_then_retract_falls_back_to_generic() {
        let service = svc();
        let role = AgentRole::new("pr-reviewer", "PR Reviewer", AgentRoleTier::Standard);
        service
            .define(&project(), role, OperatorId::new("op-a"))
            .await
            .unwrap();
        service
            .retract(&project(), "pr-reviewer", OperatorId::new("op-b"))
            .await
            .unwrap();
        // §D7: unknown (now-retracted) id resolves to generic verbatim.
        let resolved = service.resolve(&project(), "pr-reviewer").await.unwrap();
        assert_eq!(resolved.role_id, "generic");
    }

    #[tokio::test]
    async fn define_builtin_shadow_overrides_in_list_and_resolve() {
        // Shadowing the built-in `reviewer`: custom row wins.
        let service = svc();
        let shadow = AgentRole::new("reviewer", "Lane Reviewer", AgentRoleTier::Standard)
            .with_description("Lane-specific review.");
        service
            .define(&project(), shadow, OperatorId::new("op-a"))
            .await
            .unwrap();
        // `resolve("reviewer")` returns the shadow.
        let resolved = service.resolve(&project(), "reviewer").await.unwrap();
        assert_eq!(resolved.display_name, "Lane Reviewer");
        assert_eq!(resolved.description, "Lane-specific review.");

        // `list` merges: 6 roles total (5 untouched built-ins + the shadow).
        // The default_roles() set is [executor, generic, orchestrator,
        // researcher, reviewer, status-checker]; shadowing reviewer replaces
        // it with the custom_shadow row but the count stays 6.
        let items = service.list(&project(), SourceFilter::All).await.unwrap();
        assert_eq!(items.len(), 6);
        let reviewer = items.iter().find(|r| r.role.role_id == "reviewer").unwrap();
        assert_eq!(reviewer.source, RoleSource::CustomShadow);
        assert_eq!(reviewer.shadows_builtin.as_deref(), Some("reviewer"));
    }

    #[tokio::test]
    async fn define_retract_redefine_restores_active_row() {
        // §D6: re-POST after retract returns 201 / upserts in place;
        // the projection clears `retracted_at` back to None.
        let service = svc();
        let role = AgentRole::new("pr-reviewer", "V1", AgentRoleTier::Standard);
        service
            .define(&project(), role, OperatorId::new("op-a"))
            .await
            .unwrap();
        service
            .retract(&project(), "pr-reviewer", OperatorId::new("op-b"))
            .await
            .unwrap();
        // After retract, resolve falls back (§D7 → generic for
        // non-builtin id).
        assert_eq!(
            service
                .resolve(&project(), "pr-reviewer")
                .await
                .unwrap()
                .role_id,
            "generic"
        );
        // Redefine — new display_name; projection re-activates.
        let role_v2 = AgentRole::new("pr-reviewer", "V2", AgentRoleTier::Standard);
        service
            .define(&project(), role_v2, OperatorId::new("op-c"))
            .await
            .unwrap();
        let active = service.resolve(&project(), "pr-reviewer").await.unwrap();
        assert_eq!(active.display_name, "V2");
    }

    #[tokio::test]
    async fn list_custom_filter_returns_only_custom_rows() {
        let service = svc();
        service
            .define(
                &project(),
                AgentRole::new("alpha", "Alpha", AgentRoleTier::Standard),
                OperatorId::new("op-a"),
            )
            .await
            .unwrap();
        service
            .define(
                &project(),
                AgentRole::new("reviewer", "Shadow", AgentRoleTier::Standard),
                OperatorId::new("op-a"),
            )
            .await
            .unwrap();

        let custom_only = service
            .list(&project(), SourceFilter::Custom)
            .await
            .unwrap();
        assert_eq!(custom_only.len(), 1);
        assert_eq!(custom_only[0].role.role_id, "alpha");
        assert_eq!(custom_only[0].source, RoleSource::Custom);

        let shadows_only = service
            .list(&project(), SourceFilter::CustomShadow)
            .await
            .unwrap();
        assert_eq!(shadows_only.len(), 1);
        assert_eq!(shadows_only[0].role.role_id, "reviewer");

        let any_custom = service
            .list(&project(), SourceFilter::AnyCustom)
            .await
            .unwrap();
        assert_eq!(any_custom.len(), 2);
    }

    #[tokio::test]
    async fn list_scope_isolation_across_projects() {
        // Roles defined in project A don't leak into project B.
        let service = svc();
        let proj_a = project();
        let proj_b = ProjectKey {
            tenant_id: TenantId::new("t"),
            workspace_id: WorkspaceId::new("w"),
            project_id: ProjectId::new("p_other"),
        };
        service
            .define(
                &proj_a,
                AgentRole::new("a-only", "A", AgentRoleTier::Standard),
                OperatorId::new("op-a"),
            )
            .await
            .unwrap();
        let b_items = service.list(&proj_b, SourceFilter::Custom).await.unwrap();
        assert!(
            b_items.is_empty(),
            "project B must not see project A's custom roles"
        );
    }
}
