//! RFC 030: `MemoryProviderService` — owns the `ConfigureMemoryProvider`
//! command path. Mirror of [`super::knowledge_provider_impl`] but routes
//! the memory-family event.
//!
//! Query-time events (`MemoryProviderUnavailable`,
//! `MemoryProviderCapabilityChanged`, and the `MemoryIngest*` family) are
//! emitted by the dispatch layer (`cairn_memory::multi_provider_memory`)
//! when it observes the condition — they're not driven by a command
//! surface.

use std::sync::Arc;

use async_trait::async_trait;
use cairn_domain::commands::ConfigureMemoryProvider;
use cairn_domain::events::MemoryProviderConfigured;
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
pub trait MemoryProviderService: Send + Sync {
    async fn configure(
        &self,
        project: &ProjectKey,
        provider_ref: &ProviderRef,
        actor: &OperatorId,
    ) -> Result<(), RuntimeError>;
}

pub struct MemoryProviderServiceImpl<S> {
    store: Arc<S>,
}

impl<S> MemoryProviderServiceImpl<S> {
    pub fn new(store: Arc<S>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl<S> MemoryProviderService for MemoryProviderServiceImpl<S>
where
    S: EventLog + 'static,
{
    async fn configure(
        &self,
        project: &ProjectKey,
        provider_ref: &ProviderRef,
        actor: &OperatorId,
    ) -> Result<(), RuntimeError> {
        let event = make_envelope(RuntimeEvent::MemoryProviderConfigured(
            MemoryProviderConfigured {
                project: project.clone(),
                provider_ref: provider_ref.clone(),
                configured_by: actor.clone(),
                // Operator-driven re-configuration is never a bootstrap
                // binding; only `ProjectCreated` + the V019 backfill sweep
                // (PR-G) set `is_bootstrap = true`.
                is_bootstrap: false,
                at_ms: now_ms(),
            },
        ));
        self.store.append(&[event]).await?;
        Ok(())
    }
}

/// Handle a raw `ConfigureMemoryProvider` command envelope. Used by the
/// `PUT /v1/projects/:id/memory-provider` HTTP handler — the
/// actor-from-auth-token resolution happens at the app layer.
pub async fn handle_configure_memory_provider<S>(
    service: &MemoryProviderServiceImpl<S>,
    cmd: &ConfigureMemoryProvider,
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
    use cairn_store::InMemoryStore;

    fn proj() -> ProjectKey {
        ProjectKey::new("t", "w", "p")
    }

    #[tokio::test]
    async fn configure_appends_memory_provider_configured_event() {
        let store = Arc::new(InMemoryStore::new());
        let svc = MemoryProviderServiceImpl::new(store.clone());
        svc.configure(
            &proj(),
            &ProviderRef::new("plugin:mem0"),
            &OperatorId::new("op"),
        )
        .await
        .unwrap();

        let events = cairn_store::EventLog::read_stream(&*store, None, usize::MAX)
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        match &events[0].envelope.payload {
            RuntimeEvent::MemoryProviderConfigured(e) => {
                assert_eq!(e.provider_ref.as_str(), "plugin:mem0");
                assert_eq!(e.configured_by.as_str(), "op");
                assert!(!e.is_bootstrap, "operator path must not set bootstrap");
            }
            other => panic!("expected MemoryProviderConfigured, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn handle_command_dispatches_to_configure() {
        let store = Arc::new(InMemoryStore::new());
        let svc = MemoryProviderServiceImpl::new(store.clone());
        let cmd = ConfigureMemoryProvider {
            project: proj(),
            provider_ref: ProviderRef::new("cairn-default"),
            actor: OperatorId::new("op"),
        };
        handle_configure_memory_provider(&svc, &cmd).await.unwrap();

        let events = cairn_store::EventLog::read_stream(&*store, None, usize::MAX)
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
    }
}
