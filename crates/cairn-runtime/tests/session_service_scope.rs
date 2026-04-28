//! Tenant-isolation invariant for the `SessionService` trait (issue #439).
//!
//! The `get` and `archive` methods gained a required `&ProjectKey`. The
//! trait contract promises that a mismatched project behaves as "session
//! not found" — mirroring the #185 defence-in-depth pattern for F65
//! projections in #438. This file locks the contract with a tiny
//! in-memory stub so the check lives in cairn-runtime itself, not in
//! cairn-app where it depends on the full fabric wiring.
//!
//! The real scope-check behaviour for the production adapter
//! (`FabricSessionServiceAdapter`) is exercised by the fabric
//! integration suite in cairn-app — any regression there will also
//! break those tests. This one is a cheap shape-level safety net
//! owned by the crate that ships the trait.

use async_trait::async_trait;
use cairn_domain::{ProjectKey, SessionId, SessionState};
use cairn_runtime::error::RuntimeError;
use cairn_runtime::sessions::SessionService;
use cairn_store::projections::SessionRecord;
use std::collections::HashMap;
use std::sync::Mutex;

/// Tiny in-memory stub — stores rows by session_id, tags them with the
/// project they were "created under", and performs the same scope
/// semantics as the real adapter (mismatched project → None).
#[derive(Default)]
struct StubSessions {
    rows: Mutex<HashMap<String, SessionRecord>>,
}

impl StubSessions {
    fn insert(&self, project: ProjectKey, session_id: SessionId) {
        let record = SessionRecord {
            session_id: session_id.clone(),
            project,
            state: SessionState::Open,
            version: 1,
            created_at: 0,
            updated_at: 0,
            goal_title: None,
            issue_budget: None,
            max_attempts: 1,
            attempts_used: 0,
        };
        self.rows
            .lock()
            .expect("rows lock")
            .insert(session_id.as_str().to_owned(), record);
    }
}

#[async_trait]
impl SessionService for StubSessions {
    async fn create(
        &self,
        _project: &ProjectKey,
        _session_id: SessionId,
    ) -> Result<SessionRecord, RuntimeError> {
        Err(RuntimeError::Internal("read-only stub".into()))
    }

    async fn get(
        &self,
        project: &ProjectKey,
        session_id: &SessionId,
    ) -> Result<Option<SessionRecord>, RuntimeError> {
        let rows = self.rows.lock().expect("rows lock");
        let Some(record) = rows.get(session_id.as_str()) else {
            return Ok(None);
        };
        // Issue #439: mismatched project → None, indistinguishable
        // from unknown id on purpose.
        if record.project != *project {
            return Ok(None);
        }
        Ok(Some(record.clone()))
    }

    async fn lookup_any_admin(
        &self,
        session_id: &SessionId,
    ) -> Result<Option<SessionRecord>, RuntimeError> {
        let rows = self.rows.lock().expect("rows lock");
        Ok(rows.get(session_id.as_str()).cloned())
    }

    async fn list(
        &self,
        project: &ProjectKey,
        _limit: usize,
        _offset: usize,
    ) -> Result<Vec<SessionRecord>, RuntimeError> {
        let rows = self.rows.lock().expect("rows lock");
        Ok(rows
            .values()
            .filter(|r| r.project == *project)
            .cloned()
            .collect())
    }

    async fn archive(
        &self,
        project: &ProjectKey,
        session_id: &SessionId,
    ) -> Result<SessionRecord, RuntimeError> {
        let mut rows = self.rows.lock().expect("rows lock");
        let Some(record) = rows.get_mut(session_id.as_str()) else {
            return Err(RuntimeError::NotFound {
                entity: "session",
                id: session_id.as_str().to_owned(),
            });
        };
        if record.project != *project {
            return Err(RuntimeError::NotFound {
                entity: "session",
                id: session_id.as_str().to_owned(),
            });
        }
        record.state = SessionState::Archived;
        Ok(record.clone())
    }
}

fn project_a() -> ProjectKey {
    ProjectKey::new("tenant_a", "ws_a", "proj_a")
}

fn project_b() -> ProjectKey {
    ProjectKey::new("tenant_b", "ws_b", "proj_b")
}

#[tokio::test]
async fn get_returns_none_when_project_mismatches() {
    let svc = StubSessions::default();
    svc.insert(project_a(), SessionId::new("sess_1"));

    // tenant-A sees its own session.
    let hit = svc
        .get(&project_a(), &SessionId::new("sess_1"))
        .await
        .expect("get");
    assert!(hit.is_some(), "tenant-A must see own session");

    // tenant-B cannot, even with the correct id.
    let miss = svc
        .get(&project_b(), &SessionId::new("sess_1"))
        .await
        .expect("get cross-tenant");
    assert!(
        miss.is_none(),
        "cross-tenant get must yield None (issue #439)"
    );
}

#[tokio::test]
async fn archive_returns_not_found_when_project_mismatches() {
    let svc = StubSessions::default();
    svc.insert(project_a(), SessionId::new("sess_a1"));

    let err = svc
        .archive(&project_b(), &SessionId::new("sess_a1"))
        .await
        .expect_err("cross-tenant archive must fail");
    match err {
        RuntimeError::NotFound { entity, id } => {
            assert_eq!(entity, "session");
            assert_eq!(id, "sess_a1");
        }
        other => panic!("expected NotFound, got {other:?}"),
    }

    // tenant-A's archive still works.
    let archived = svc
        .archive(&project_a(), &SessionId::new("sess_a1"))
        .await
        .expect("own-tenant archive");
    assert_eq!(archived.state, SessionState::Archived);
}

#[tokio::test]
async fn lookup_any_admin_ignores_project_on_purpose() {
    // The admin-only lookup preserves the pre-#439 unchecked shape so
    // cross-tenant admin endpoints stay viable. The distinction from
    // `get` is that the method name makes the intent explicit — a
    // reviewer sees `lookup_any_admin` in a diff and knows the
    // caller MUST guard with an admin-role gate.
    let svc = StubSessions::default();
    svc.insert(project_a(), SessionId::new("sess_admin"));

    let hit = svc
        .lookup_any_admin(&SessionId::new("sess_admin"))
        .await
        .expect("lookup_any_admin");
    assert!(hit.is_some(), "admin lookup must see any session");
    assert_eq!(hit.unwrap().project, project_a());
}
