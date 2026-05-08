//! RFC 029: `KnowledgeProviderService` — the service that owns the
//! `ConfigureKnowledgeProvider` command path. Appends
//! `KnowledgeProviderConfigured` on the event log; downstream projections
//! upsert the current-configuration row on `project_knowledge_providers`.
//!
//! Query-time events (`KnowledgeProviderUnavailable`,
//! `KnowledgeProviderCapabilityChanged`, and the `KnowledgeIngest*`
//! family) are emitted by the retrieval / ingest dispatch layer
//! (`cairn_memory::multi_provider`) when it observes the condition —
//! they're not driven by a command surface.

use std::sync::Arc;

use async_trait::async_trait;
use cairn_domain::commands::ConfigureKnowledgeProvider;
use cairn_domain::events::KnowledgeProviderConfigured;
use cairn_domain::{OperatorId, ProjectKey, ProviderRef, RuntimeEvent};
use cairn_store::EventLog;

use super::event_helpers::make_envelope;
use crate::error::RuntimeError;

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[async_trait]
pub trait KnowledgeProviderService: Send + Sync {
    async fn configure(
        &self,
        project: &ProjectKey,
        provider_ref: &ProviderRef,
        actor: &OperatorId,
    ) -> Result<(), RuntimeError>;
}

pub struct KnowledgeProviderServiceImpl<S> {
    store: Arc<S>,
}

impl<S> KnowledgeProviderServiceImpl<S> {
    pub fn new(store: Arc<S>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl<S> KnowledgeProviderService for KnowledgeProviderServiceImpl<S>
where
    S: EventLog + 'static,
{
    async fn configure(
        &self,
        project: &ProjectKey,
        provider_ref: &ProviderRef,
        actor: &OperatorId,
    ) -> Result<(), RuntimeError> {
        let event = make_envelope(RuntimeEvent::KnowledgeProviderConfigured(
            KnowledgeProviderConfigured {
                project: project.clone(),
                provider_ref: provider_ref.clone(),
                configured_by: actor.clone(),
                at_ms: now_ms(),
            },
        ));
        self.store.append(&[event]).await?;
        Ok(())
    }
}

/// Handle a raw `ConfigureKnowledgeProvider` command envelope. Used by
/// the `PUT /v1/projects/:id/knowledge-provider` HTTP handler — the
/// actor-from-auth-token resolution happens at the app layer; this
/// helper takes the already-decoded command and runs it.
pub async fn handle_configure_knowledge_provider<S>(
    service: &KnowledgeProviderServiceImpl<S>,
    cmd: &ConfigureKnowledgeProvider,
) -> Result<(), RuntimeError>
where
    S: EventLog + 'static,
{
    service
        .configure(&cmd.project, &cmd.provider_ref, &cmd.actor)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_store::{EventLog, InMemoryStore};

    fn proj() -> ProjectKey {
        ProjectKey::new("t", "w", "p")
    }

    #[tokio::test]
    async fn configure_emits_configured_event() {
        let store = Arc::new(InMemoryStore::new());
        let svc = KnowledgeProviderServiceImpl::new(store.clone());
        svc.configure(
            &proj(),
            &ProviderRef::new("plugin:mem0"),
            &OperatorId::new("op_1"),
        )
        .await
        .unwrap();

        let events = store.read_stream(None, 100).await.unwrap();
        assert_eq!(events.len(), 1);
        match &events[0].envelope.payload {
            RuntimeEvent::KnowledgeProviderConfigured(e) => {
                assert_eq!(e.project, proj());
                assert_eq!(e.provider_ref.as_str(), "plugin:mem0");
                assert_eq!(e.configured_by.as_str(), "op_1");
            }
            other => panic!("expected KnowledgeProviderConfigured, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn configure_twice_emits_two_events() {
        // Each call appends — projection upserts the row, so the audit
        // log retains the full configuration history.
        let store = Arc::new(InMemoryStore::new());
        let svc = KnowledgeProviderServiceImpl::new(store.clone());
        svc.configure(
            &proj(),
            &ProviderRef::new("cairn-default"),
            &OperatorId::new("op_1"),
        )
        .await
        .unwrap();
        svc.configure(
            &proj(),
            &ProviderRef::new("plugin:bedrock-kb"),
            &OperatorId::new("op_2"),
        )
        .await
        .unwrap();
        let events = store.read_stream(None, 100).await.unwrap();
        assert_eq!(events.len(), 2);
    }

    #[tokio::test]
    async fn handle_configure_knowledge_provider_bridges_command() {
        let store = Arc::new(InMemoryStore::new());
        let svc = KnowledgeProviderServiceImpl::new(store.clone());
        let cmd = ConfigureKnowledgeProvider {
            project: proj(),
            provider_ref: ProviderRef::new("plugin:mem0"),
            actor: OperatorId::new("op_3"),
        };
        handle_configure_knowledge_provider(&svc, &cmd)
            .await
            .unwrap();
        assert_eq!(store.read_stream(None, 100).await.unwrap().len(), 1);
    }
}
