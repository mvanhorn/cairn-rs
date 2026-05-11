//! RFC 029: concrete `KnowledgePluginDispatcher` backed by
//! `StdioPluginHost`.
//!
//! The dispatcher translates in-process `KnowledgeQueryParams` /
//! `KnowledgeIngestParams` / `KnowledgeIngestStatusParams` into
//! JSON-RPC 2.0 requests, sends them through the shared plugin host's
//! stdio transport, and returns the decoded wire response.
//!
//! ## Threading model
//!
//! `StdioPluginHost::send_request` takes `&mut self` and the
//! underlying stdio transport is synchronous. `cairn-memory`'s
//! `KnowledgePluginDispatcher` trait is `async fn(&self, …)`, so this
//! adapter:
//!
//! 1. Holds an `Arc<std::sync::Mutex<StdioPluginHost>>` — the same
//!    `Arc` the rest of the app shares (`AppState.plugin_host`).
//! 2. Uses `tokio::task::spawn_blocking` to acquire the mutex + run
//!    the synchronous send/recv on the blocking thread pool, so the
//!    request-handling async runtime is never blocked.
//!
//! **Known limitation (tracked as a follow-up):** the mutex is held
//! for the entire round-trip, which serialises every knowledge.*
//! call globally — not just per plugin. Concurrency against
//! different plugins is therefore artificially limited. The right
//! fix is a per-plugin lock in `StdioPluginHost` plus an async
//! transport variant; that's a separate refactor because every
//! existing plugin-host consumer (tools, eval-score, marketplace)
//! assumes the current sync API. In the meantime, the blocking
//! pool (`spawn_blocking`) absorbs the wait so the async runtime
//! never stalls — the cost is latency under fan-out, not
//! correctness.
//!
//! ## Error mapping
//!
//! - Transport-level failures (plugin exited, pipe closed, decode
//!   error) → `KnowledgePluginError::Unavailable`. The dispatch layer
//!   surfaces these as `RetrievalError::ProviderUnavailable` so the
//!   runtime emits `KnowledgeProviderUnavailable` for operator audit.
//! - JSON-RPC error responses (plugin returned an `error` body) →
//!   `KnowledgePluginError::PluginError` with the plugin's message.
//! - Mutex poisoning, missing plugin, wrong state → `Unavailable`
//!   (operator cause, not a transient transport failure).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use cairn_memory::multi_provider::{KnowledgePluginDispatcher, KnowledgePluginError};
use cairn_plugin_proto::knowledge::{
    KnowledgeIngestAck, KnowledgeIngestParams, KnowledgeIngestStatusParams,
    KnowledgeIngestStatusResult, KnowledgeQueryParams, KnowledgeQueryResult,
};
use cairn_plugin_proto::wire::{methods, JsonRpcRequest, JsonRpcResponse};
use tracing::warn;

use crate::plugin_host::{PluginHostError, StdioPluginHost};
use crate::transport::TransportError;

/// `StdioPluginHost`-backed dispatcher. Clone-cheap (Arc under the hood).
#[derive(Clone)]
pub struct StdioKnowledgeDispatcher {
    host: Arc<Mutex<StdioPluginHost>>,
    request_counter: Arc<AtomicU64>,
}

impl StdioKnowledgeDispatcher {
    /// Construct a dispatcher with a **fresh** request counter. Use
    /// this when you're sure no other dispatcher instance shares the
    /// plugin host — otherwise `new_with_counter` is the safer choice.
    pub fn new(host: Arc<Mutex<StdioPluginHost>>) -> Self {
        Self::new_with_counter(host, Arc::new(AtomicU64::new(0)))
    }

    /// Construct a dispatcher sharing the request counter with other
    /// dispatcher instances. Every site that creates a separate
    /// `StdioKnowledgeDispatcher` against the same `plugin_host`
    /// should thread the same counter through so JSON-RPC request ids
    /// stay globally unique per plugin process — avoiding id collision
    /// if the transport ever becomes asynchronous (concurrent in-
    /// flight requests to the same plugin).
    pub fn new_with_counter(host: Arc<Mutex<StdioPluginHost>>, counter: Arc<AtomicU64>) -> Self {
        Self {
            host,
            request_counter: counter,
        }
    }

    fn next_request_id(&self) -> String {
        let n = self.request_counter.fetch_add(1, Ordering::Relaxed);
        format!("knowledge-{n}")
    }

    /// Blocking send + decode. Runs inside `spawn_blocking` so the
    /// async caller never holds the std Mutex across an await.
    fn send_blocking(
        host: Arc<Mutex<StdioPluginHost>>,
        plugin_id: String,
        method: &str,
        request: JsonRpcRequest,
    ) -> Result<JsonRpcResponse, KnowledgePluginError> {
        let mut guard = host.lock().map_err(|e| {
            // Poison is a hard error; caller treats it as "plugin
            // subsystem is wedged, need restart". Surface as
            // Unavailable rather than Internal so the operator gets
            // a clear cause in the event log.
            warn!(plugin = %plugin_id, method, "plugin host mutex poisoned");
            KnowledgePluginError::Unavailable(format!("plugin host mutex poisoned: {e}"))
        })?;
        match guard.send_request(&plugin_id, &request) {
            Ok(resp) => Ok(resp),
            Err(PluginHostError::NotFound(_)) => {
                warn!(plugin = %plugin_id, method, "plugin not registered on host");
                Err(KnowledgePluginError::Unavailable(format!(
                    "plugin:{plugin_id} is not registered on this host"
                )))
            }
            Err(PluginHostError::InvalidState { state, .. }) => {
                warn!(
                    plugin = %plugin_id,
                    method,
                    state = ?state,
                    "plugin not ready for request",
                );
                Err(KnowledgePluginError::Unavailable(format!(
                    "plugin:{plugin_id} is not ready (state: {state:?})"
                )))
            }
            Err(PluginHostError::Transport(transport_err)) => {
                // Split transport errors by kind:
                //
                // - `InvalidResponse` typically wraps a JSON-RPC `{error: …}`
                //   body the plugin sent back — that's a protocol-level
                //   rejection, not a transport failure, and should
                //   surface as `PluginError` so the runtime does not
                //   emit `KnowledgeProviderUnavailable` (which would
                //   falsely mark the provider as down for operator UI).
                // - `SpawnFailed` / `WriteFailed` / `ReadFailed` /
                //   `ProcessExited` are all genuine transport issues;
                //   those stay as `Unavailable`.
                warn!(
                    plugin = %plugin_id,
                    method,
                    error = %transport_err,
                    "plugin transport error",
                );
                match transport_err {
                    TransportError::InvalidResponse(msg) => Err(KnowledgePluginError::PluginError(
                        format!("invalid response: {msg}"),
                    )),
                    other => Err(KnowledgePluginError::Unavailable(format!(
                        "transport error: {other}"
                    ))),
                }
            }
            Err(e) => {
                warn!(plugin = %plugin_id, method, error = %e, "plugin host error");
                Err(KnowledgePluginError::PluginError(e.to_string()))
            }
        }
    }

    async fn roundtrip<R>(
        &self,
        plugin_id: &str,
        method: &'static str,
        params: serde_json::Value,
    ) -> Result<R, KnowledgePluginError>
    where
        R: serde::de::DeserializeOwned + Send + 'static,
    {
        let request = JsonRpcRequest::new(self.next_request_id(), method, params);
        let host = Arc::clone(&self.host);
        let plugin_id_owned = plugin_id.to_owned();
        let method_owned = method;
        let response = tokio::task::spawn_blocking(move || {
            Self::send_blocking(host, plugin_id_owned, method_owned, request)
        })
        .await
        .map_err(|e| {
            // spawn_blocking JoinError: worker panicked or was
            // cancelled. Treat as Unavailable — the plugin may still
            // be alive, but we can't trust the result.
            KnowledgePluginError::Unavailable(format!("blocking task failed: {e}"))
        })??;

        // The stdio transport only returns a `JsonRpcResponse` on
        // success (plugin returned `{result: ...}`). Error bodies are
        // surfaced by the transport as `TransportError::JsonRpcError`
        // and come through the `Err` branch above — by the time we
        // reach here, `response.result` is the typed payload.
        serde_json::from_value(response.result).map_err(|e| {
            KnowledgePluginError::PluginError(format!(
                "plugin:{plugin_id} returned a response {method} could not decode: {e}"
            ))
        })
    }
}

#[async_trait]
impl KnowledgePluginDispatcher for StdioKnowledgeDispatcher {
    async fn query(
        &self,
        plugin_id: &str,
        params: KnowledgeQueryParams,
    ) -> Result<KnowledgeQueryResult, KnowledgePluginError> {
        let value = serde_json::to_value(&params).map_err(|e| {
            KnowledgePluginError::Internal(format!("serialize KnowledgeQueryParams: {e}"))
        })?;
        self.roundtrip(plugin_id, methods::KNOWLEDGE_QUERY, value)
            .await
    }

    async fn ingest(
        &self,
        plugin_id: &str,
        params: KnowledgeIngestParams,
    ) -> Result<KnowledgeIngestAck, KnowledgePluginError> {
        let value = serde_json::to_value(&params).map_err(|e| {
            KnowledgePluginError::Internal(format!("serialize KnowledgeIngestParams: {e}"))
        })?;
        self.roundtrip(plugin_id, methods::KNOWLEDGE_INGEST, value)
            .await
    }

    async fn ingest_status(
        &self,
        plugin_id: &str,
        params: KnowledgeIngestStatusParams,
    ) -> Result<KnowledgeIngestStatusResult, KnowledgePluginError> {
        let value = serde_json::to_value(&params).map_err(|e| {
            KnowledgePluginError::Internal(format!("serialize KnowledgeIngestStatusParams: {e}"))
        })?;
        self.roundtrip(plugin_id, methods::KNOWLEDGE_INGEST_STATUS, value)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_domain::{KnowledgeDocumentId, ProjectKey};
    use cairn_plugin_proto::knowledge::{KnowledgeIngestStatus, RetrievalModeWire};

    /// Fresh dispatcher pointing at an empty host. Every knowledge.*
    /// request routes to `plugin_id` that is not registered — so we
    /// see the `NotFound → Unavailable` path.
    fn unregistered_dispatcher() -> StdioKnowledgeDispatcher {
        StdioKnowledgeDispatcher::new(Arc::new(Mutex::new(StdioPluginHost::new())))
    }

    #[tokio::test]
    async fn query_against_unregistered_plugin_surfaces_unavailable() {
        let d = unregistered_dispatcher();
        let params = KnowledgeQueryParams {
            project: ProjectKey::new("t", "w", "p"),
            query_text: "hello".into(),
            mode: RetrievalModeWire::Hybrid,
            limit: 5,
            metadata_filters: vec![],
        };
        let err = d
            .query("missing_plugin", params)
            .await
            .expect_err("should error");
        match err {
            KnowledgePluginError::Unavailable(msg) => {
                assert!(msg.contains("missing_plugin"), "msg: {msg}");
                assert!(msg.contains("not registered"), "msg: {msg}");
            }
            other => panic!("expected Unavailable, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn ingest_against_unregistered_plugin_surfaces_unavailable() {
        let d = unregistered_dispatcher();
        let params = KnowledgeIngestParams {
            document_id: KnowledgeDocumentId::new("d"),
            source_id: cairn_domain::SourceId::new("s"),
            source_type: cairn_plugin_proto::knowledge::SourceTypeWire::Markdown,
            project: ProjectKey::new("t", "w", "p"),
            content: "payload".into(),
            import_id: None,
            corpus_id: None,
            tags: vec![],
        };
        let err = d.ingest("x", params).await.expect_err("should error");
        assert!(matches!(err, KnowledgePluginError::Unavailable(_)));
    }

    #[tokio::test]
    async fn ingest_status_against_unregistered_plugin_surfaces_unavailable() {
        let d = unregistered_dispatcher();
        let params = KnowledgeIngestStatusParams {
            document_id: KnowledgeDocumentId::new("d"),
        };
        let err = d
            .ingest_status("y", params)
            .await
            .expect_err("should error");
        assert!(matches!(err, KnowledgePluginError::Unavailable(_)));
        // Status enum coverage: make sure the wire variants compile.
        let _ = KnowledgeIngestStatus::Completed;
    }

    #[tokio::test]
    async fn request_ids_are_unique_per_dispatcher() {
        // Covers the monotonic counter: two consecutive calls land
        // two distinct ids on the wire. Uses a synthetic access to
        // `next_request_id` since we don't have a real plugin to
        // intercept the request with.
        let d = unregistered_dispatcher();
        let a = d.next_request_id();
        let b = d.next_request_id();
        assert_ne!(a, b);
        assert!(a.starts_with("knowledge-"));
        assert!(b.starts_with("knowledge-"));
    }

    #[tokio::test]
    async fn clones_share_the_counter() {
        // Dispatcher is cloneable; every clone must share the same
        // Arc<AtomicU64> so JSON-RPC ids stay unique across the
        // agent-memory and deep-search paths that share a host.
        let d1 = unregistered_dispatcher();
        let d2 = d1.clone();
        let a = d1.next_request_id();
        let b = d2.next_request_id();
        // `a = knowledge-0`, `b = knowledge-1` — if the counter
        // weren't shared, d2 would also start at 0 and produce
        // `knowledge-0`, colliding with d1.
        assert_ne!(a, b);
    }

    #[tokio::test]
    async fn new_with_counter_shares_across_separate_dispatchers() {
        // Use case: two dispatchers against the same host
        // (hypothetical — today we ensure there's one dispatcher
        // app-wide and clone it). With a shared counter, request
        // ids stay globally unique regardless of construction site.
        use std::sync::atomic::AtomicU64;
        let counter = Arc::new(AtomicU64::new(0));
        let host = Arc::new(Mutex::new(StdioPluginHost::new()));
        let d1 = StdioKnowledgeDispatcher::new_with_counter(host.clone(), counter.clone());
        let d2 = StdioKnowledgeDispatcher::new_with_counter(host, counter);
        let a = d1.next_request_id();
        let b = d2.next_request_id();
        assert_ne!(a, b);
    }
}
