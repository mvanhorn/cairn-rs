//! Concrete project service implementation.
//!
//! Manages project lifecycle by emitting `ProjectCreated` events
//! and reading back via the `ProjectReadModel` projection.

use std::sync::Arc;

use async_trait::async_trait;
use cairn_domain::*;
use cairn_store::projections::ProjectReadModel;
use cairn_store::EventLog;

use super::event_helpers::make_envelope;
use crate::error::RuntimeError;
use crate::projects::ProjectService;

pub struct ProjectServiceImpl<S> {
    store: Arc<S>,
}

impl<S> ProjectServiceImpl<S> {
    pub fn new(store: Arc<S>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl<S> ProjectService for ProjectServiceImpl<S>
where
    S: EventLog + ProjectReadModel + 'static,
{
    async fn create(
        &self,
        project: ProjectKey,
        name: String,
    ) -> Result<ProjectRecord, RuntimeError> {
        // Check for existing project.
        if ProjectReadModel::get_project(self.store.as_ref(), &project)
            .await?
            .is_some()
        {
            return Err(RuntimeError::Conflict {
                entity: "project",
                id: project.project_id.to_string(),
            });
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        // RFC 030 PR-G: emit `ProjectCreated` + the two bootstrap
        // provider bindings atomically. Both families default to
        // `cairn-default`; the `is_bootstrap = true` flag
        // distinguishes this emission from operator-driven
        // re-configurations (which always set it `false`).
        //
        // Emitting all three in a single `append` keeps the project's
        // first snapshot consistent — a concurrent reader never sees a
        // project without its provider slots populated. Idempotent
        // backfill for pre-RFC-030 projects runs at boot
        // (see `cairn_app::bootstrap`).
        // System-actor sentinel. Matches the RFC 029 pattern of marking
        // system-emitted lifecycle events with a constant operator id so
        // audit queries can distinguish bootstrap emissions from
        // operator-driven configures (which carry the authenticated
        // operator's id).
        let bootstrap_actor = "system:bootstrap";
        let events = vec![
            make_envelope(RuntimeEvent::ProjectCreated(ProjectCreated {
                project: project.clone(),
                name,
                created_at: now,
            })),
            make_envelope(RuntimeEvent::KnowledgeProviderConfigured(
                KnowledgeProviderConfigured {
                    project: project.clone(),
                    // RFC 030 §Decisions D2: the stable provider-ref for
                    // the in-process default is the literal
                    // `"cairn-default"`. Hardcoded rather than imported
                    // from `cairn-memory` because runtime is
                    // upstream of memory in the dep graph.
                    provider_ref: ProviderRef::new("cairn-default"),
                    configured_by: OperatorId::new(bootstrap_actor),
                    is_bootstrap: true,
                    at_ms: now,
                },
            )),
            make_envelope(RuntimeEvent::MemoryProviderConfigured(
                MemoryProviderConfigured {
                    project: project.clone(),
                    // RFC 030 §Decisions D2: the stable provider-ref for
                    // the in-process default is the literal
                    // `"cairn-default"`. Hardcoded rather than imported
                    // from `cairn-memory` because runtime is
                    // upstream of memory in the dep graph.
                    provider_ref: ProviderRef::new("cairn-default"),
                    configured_by: OperatorId::new(bootstrap_actor),
                    is_bootstrap: true,
                    at_ms: now,
                },
            )),
        ];

        self.store.append(&events).await?;

        ProjectReadModel::get_project(self.store.as_ref(), &project)
            .await?
            .ok_or_else(|| RuntimeError::Internal("project not found after create".into()))
    }

    async fn get(&self, project: &ProjectKey) -> Result<Option<ProjectRecord>, RuntimeError> {
        Ok(ProjectReadModel::get_project(self.store.as_ref(), project).await?)
    }

    async fn list_by_workspace(
        &self,
        tenant_id: &TenantId,
        workspace_id: &WorkspaceId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ProjectRecord>, RuntimeError> {
        Ok(self
            .store
            .list_by_workspace(tenant_id, workspace_id, limit, offset)
            .await?)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use cairn_domain::*;
    use cairn_store::InMemoryStore;

    use crate::projects::ProjectService;

    use super::ProjectServiceImpl;

    fn test_project() -> ProjectKey {
        ProjectKey::new("tenant_acme", "ws_main", "project_alpha")
    }

    #[tokio::test]
    async fn create_persists_and_returns_project() {
        let store = Arc::new(InMemoryStore::new());
        let svc = ProjectServiceImpl::new(store.clone());
        let project = test_project();

        let record = svc
            .create(project.clone(), "Alpha Project".to_owned())
            .await
            .unwrap();

        assert_eq!(record.project_id, project.project_id);
        assert_eq!(record.workspace_id, project.workspace_id);
        assert_eq!(record.tenant_id, project.tenant_id);
        assert_eq!(record.name, "Alpha Project");
    }

    #[tokio::test]
    async fn create_emits_dual_family_bootstrap_bindings() {
        // RFC 030 PR-G: `ProjectCreated` must atomically emit the
        // knowledge + memory bootstrap bindings, both pointing at
        // `cairn-default` with `is_bootstrap = true`.
        use cairn_store::EventLog;
        let store = Arc::new(InMemoryStore::new());
        let svc = ProjectServiceImpl::new(store.clone());
        let project = test_project();
        svc.create(project.clone(), "Alpha".to_owned())
            .await
            .unwrap();

        let events = EventLog::read_stream(&*store, None, usize::MAX)
            .await
            .unwrap();
        let for_project: Vec<_> = events
            .iter()
            .filter(|e| {
                matches!(
                &e.envelope.payload,
                RuntimeEvent::ProjectCreated(p) if p.project == project)
                    || matches!(
                    &e.envelope.payload,
                    RuntimeEvent::KnowledgeProviderConfigured(k) if k.project == project)
                    || matches!(
                    &e.envelope.payload,
                    RuntimeEvent::MemoryProviderConfigured(m) if m.project == project)
            })
            .collect();
        assert_eq!(
            for_project.len(),
            3,
            "expected 3 events: ProjectCreated + KnowledgeProviderConfigured + MemoryProviderConfigured"
        );

        let mut saw_project_created = false;
        let mut saw_knowledge_bootstrap = false;
        let mut saw_memory_bootstrap = false;
        for stored in &for_project {
            match &stored.envelope.payload {
                RuntimeEvent::ProjectCreated(_) => saw_project_created = true,
                RuntimeEvent::KnowledgeProviderConfigured(e) => {
                    assert_eq!(e.provider_ref.as_str(), "cairn-default");
                    assert!(
                        e.is_bootstrap,
                        "bootstrap emission must set is_bootstrap = true"
                    );
                    saw_knowledge_bootstrap = true;
                }
                RuntimeEvent::MemoryProviderConfigured(e) => {
                    assert_eq!(e.provider_ref.as_str(), "cairn-default");
                    assert!(
                        e.is_bootstrap,
                        "bootstrap emission must set is_bootstrap = true"
                    );
                    saw_memory_bootstrap = true;
                }
                _ => {}
            }
        }
        assert!(saw_project_created);
        assert!(saw_knowledge_bootstrap);
        assert!(saw_memory_bootstrap);
    }

    #[tokio::test]
    async fn create_duplicate_returns_conflict() {
        let store = Arc::new(InMemoryStore::new());
        let svc = ProjectServiceImpl::new(store.clone());
        let project = test_project();

        svc.create(project.clone(), "Alpha".to_owned())
            .await
            .unwrap();

        let result = svc.create(project, "Alpha 2".to_owned()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn get_returns_created_project() {
        let store = Arc::new(InMemoryStore::new());
        let svc = ProjectServiceImpl::new(store);
        let project = test_project();

        svc.create(project.clone(), "Alpha".to_owned())
            .await
            .unwrap();

        let found = svc.get(&project).await.unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().name, "Alpha");
    }

    #[tokio::test]
    async fn get_nonexistent_returns_none() {
        let store = Arc::new(InMemoryStore::new());
        let svc = ProjectServiceImpl::new(store);

        let result = svc.get(&test_project()).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn list_by_workspace_filters_correctly() {
        let store = Arc::new(InMemoryStore::new());
        let svc = ProjectServiceImpl::new(store);

        svc.create(ProjectKey::new("t1", "ws1", "p_a"), "A".to_owned())
            .await
            .unwrap();

        svc.create(ProjectKey::new("t1", "ws1", "p_b"), "B".to_owned())
            .await
            .unwrap();

        svc.create(ProjectKey::new("t1", "ws2", "p_c"), "C".to_owned())
            .await
            .unwrap();

        let results = svc
            .list_by_workspace(&TenantId::new("t1"), &WorkspaceId::new("ws1"), 10, 0)
            .await
            .unwrap();
        assert_eq!(results.len(), 2);

        let other_results = svc
            .list_by_workspace(&TenantId::new("t1"), &WorkspaceId::new("ws2"), 10, 0)
            .await
            .unwrap();
        assert_eq!(other_results.len(), 1);
    }
}
