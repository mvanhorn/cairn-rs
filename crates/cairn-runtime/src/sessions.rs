//! Session service boundary per RFC 005.
//!
//! Sessions are long-lived conversational or operational contexts.
//! Session state is derived from run outcomes plus explicit close/archive.

use async_trait::async_trait;
use cairn_domain::{ProjectKey, SessionId};
use cairn_store::projections::SessionRecord;

use crate::error::RuntimeError;

/// Session service boundary.
///
/// Per RFC 005:
/// - sessions start as `open`
/// - session state is derived from run outcomes
/// - sessions can be explicitly archived
///
/// ## Tenant-isolation (issue #439)
///
/// Every id-keyed method takes a required `project: &ProjectKey`. The
/// service layer enforces the scope — callers must provide the expected
/// project, and the service returns `None` / `SessionNotFound` for any
/// id that exists in a different project. This closes the #185-shape
/// landmine that `get`/`archive` had while they took `SessionId` alone:
/// every handler had to remember to pre-check the tenant after the
/// fetch, and the SQ/EQ + assistant + admin paths did not. Pushing the
/// scope check into the service makes the safe usage the default.
///
/// Admin tokens that need to look up sessions across tenants now use
/// the two-step `list_admin_session_project` → `get(project, id)`
/// pattern (see `crates/cairn-app/src/handlers/sessions.rs`).
#[async_trait]
pub trait SessionService: Send + Sync {
    /// Create a new session in a project.
    async fn create(
        &self,
        project: &ProjectKey,
        session_id: SessionId,
    ) -> Result<SessionRecord, RuntimeError>;

    /// Get a session by ID within a project scope.
    ///
    /// Returns `None` for ids that do not exist **or** that exist in a
    /// different project — callers cannot distinguish the two.
    async fn get(
        &self,
        project: &ProjectKey,
        session_id: &SessionId,
    ) -> Result<Option<SessionRecord>, RuntimeError>;

    /// Admin-only cross-tenant lookup.
    ///
    /// Resolves a session record by id **without** enforcing a project
    /// scope. Returns `None` for unknown ids.
    ///
    /// Split off from `get` in issue #439: tenant-isolation requires
    /// `get` to be scope-checked at the service layer, but admin
    /// handlers (e.g. `GET /v1/sessions/:id` with an admin token)
    /// legitimately need to inspect sessions across tenants and do
    /// not know the owning project ahead of time. Routing those
    /// handlers through this method makes the cross-tenant intent
    /// explicit — reviewers see `lookup_any_admin` in a diff and know
    /// it is admin-only by contract, rather than the historical
    /// footgun where `get(&session_id)` transparently leaked
    /// cross-tenant rows to handlers that forgot to post-check.
    ///
    /// Call sites MUST guard this with an admin-token check (the
    /// `AdminRoleGuard` or `TenantScope::is_admin` gate in
    /// cairn-app). Non-admin handlers MUST use `get(&project, …)`
    /// and accept a `None` for ids outside their scope.
    async fn lookup_any_admin(
        &self,
        session_id: &SessionId,
    ) -> Result<Option<SessionRecord>, RuntimeError>;

    /// List sessions for a project.
    async fn list(
        &self,
        project: &ProjectKey,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<SessionRecord>, RuntimeError>;

    /// Archive a session (terminal).
    ///
    /// Scope-checked: a caller supplying a `project` that does not own
    /// `session_id` receives a `RuntimeError::NotFound { entity:
    /// "session", .. }`, indistinguishable from an unknown id.
    async fn archive(
        &self,
        project: &ProjectKey,
        session_id: &SessionId,
    ) -> Result<SessionRecord, RuntimeError>;
}

#[cfg(test)]
mod tests {
    use cairn_domain::SessionId;

    #[test]
    fn session_id_is_stable() {
        let id = SessionId::new("sess_123");
        assert_eq!(id.as_str(), "sess_123");
    }
}
