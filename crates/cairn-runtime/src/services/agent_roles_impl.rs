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

/// PR-A skeleton implementation of `AgentRoleService`.
///
/// **Behaviour**: `define` / `retract` append the corresponding
/// event to the event log (wire shape per §Event-Sourcing Delta).
/// `resolve` / `list` read from `default_roles()` only — the
/// `project_agent_roles` projection reader lands in PR-B alongside
/// the HTTP handlers that write rows. PR-A ensures the event
/// payloads are structurally sound and the service trait is wired
/// into `RuntimeServices`; PR-B upgrades `resolve` / `list` to
/// consult the projection first, with `default_roles()` as the
/// fallback per §D14.
///
/// This means PR-A, merged alone, has zero observable behaviour
/// change on the orchestrator: every `resolve(project, id)` call
/// falls through to `default_roles()`, which is exactly what the
/// pre-RFC code did directly.
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

#[async_trait]
impl<S> AgentRoleService for AgentRoleServiceImpl<S>
where
    S: EventLog + 'static,
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
        let event = make_envelope(RuntimeEvent::AgentRoleDefined(AgentRoleDefined {
            project: project.clone(),
            role: role.clone(),
            shadows_builtin: shadows_builtin.clone(),
            defined_by: actor.clone(),
            at_ms,
        }));
        self.store.append(&[event]).await?;

        // PR-A service does not yet maintain the
        // `project_agent_roles` projection read model — that writer
        // lands in PR-B alongside the HTTP handler. Return a
        // synthesised `ResolvedRole` so PR-B's handler gets a view
        // that matches the eventual projection row shape without
        // needing to change return types when the projection
        // writer ships.
        let source = match shadows_builtin.as_deref() {
            Some(_) => RoleSource::CustomShadow,
            None => RoleSource::Custom,
        };
        Ok(ResolvedRole {
            role,
            source,
            shadows_builtin,
            defined_at: Some(at_ms),
            defined_by: Some(actor),
        })
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
        _project: &ProjectKey,
        role_id: &str,
    ) -> Result<AgentRole, RuntimeError> {
        // PR-A: projection read is a follow-up. Today `resolve`
        // always falls through to `default_roles()`; PR-B adds the
        // project-scoped lookup ahead of this fallback.
        Ok(builtin_fallback(role_id))
    }

    async fn list(
        &self,
        _project: &ProjectKey,
        filter: SourceFilter,
    ) -> Result<Vec<ResolvedRole>, RuntimeError> {
        // PR-A: merged view is built-ins only. Custom-role rows are
        // pulled in by PR-B when the projection reader ships; the
        // ordering (alphabetical by role_id) and the envelope shape
        // stay stable.
        let mut items: Vec<ResolvedRole> = default_roles()
            .into_iter()
            .map(|role| ResolvedRole {
                role,
                source: RoleSource::Builtin,
                shadows_builtin: None,
                defined_at: None,
                defined_by: None,
            })
            .collect();
        items.sort_by(|a, b| a.role.role_id.cmp(&b.role.role_id));

        let filtered: Vec<ResolvedRole> = items
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
        // Retract is event-only in PR-A; the projection writer lands
        // with the handler in PR-B. The service must not error on a
        // retract for an id it doesn't recognise either — the HTTP
        // layer translates absence into a 404 before calling the
        // service (PR-B handler responsibility).
        service
            .retract(&project(), "pr-reviewer", OperatorId::new("op-a"))
            .await
            .expect("retract");
    }
}
