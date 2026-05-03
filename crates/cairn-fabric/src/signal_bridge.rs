use std::sync::Arc;

use flowfabric::core::engine_backend::EngineBackend;
use flowfabric::core::partition::execution_partition;
use flowfabric::core::types::{ExecutionId, SignalId, TimestampMs, WaitpointId, WaitpointToken};
use flowfabric::sdk::task::{Signal, SignalOutcome};

use crate::error::FabricError;
use crate::helpers::sanitize_signal_component;
use crate::runtime_handle::FabricRuntimeHandle;

/// Read the HMAC waitpoint token for `waitpoint_id` via FF's
/// `EngineBackend::read_waitpoint_token` trait method. Cairn never
/// caches the token — FF owns it from mint (`ff_suspend_execution`)
/// to reveal.
///
/// Returns `Err(Validation)` ONLY when the field is missing or empty — i.e.
/// the waitpoint hash has never been written, or was deleted. FF does NOT
/// clear `waitpoint_token` on close (audit retention): a closed waitpoint
/// still has its token, so this helper returns `Ok(token)` for it and the
/// downstream `ff_deliver_signal` reply surfaces `waitpoint_closed` at the
/// state boundary where it belongs. That separation matters — mixing
/// "waitpoint never existed" with "waitpoint is closed" at the auth layer
/// would re-create the exact oracle FF's Lua took pains to eliminate.
///
/// # FF 0.14 wrappers NOT adopted (intentional)
///
/// FF 0.14 ships two optional consumer surfaces over this trait method:
///
/// * `ff_sdk::FlowFabricAdminClient::read_waitpoint_token` — HTTP-
///   fronted wrapper. Not adopted because cairn holds the
///   `Arc<dyn EngineBackend>` in-process and the HTTP detour would
///   add a network hop for zero benefit.
/// * `ff_sdk::signal_bridge::verify_and_deliver` — packages
///   "read token → constant-time compare → forward via
///   `FlowFabricWorker::deliver_signal`" for consumers that don't
///   already own signal-bridge logic. Not adopted because cairn's
///   `SignalBridge` is richer (multi-signal-type dispatch, lane-id
///   cache, cairn-specific error enum, FCALL-direct delivery path)
///   and already calls this primitive. Adopting FF's composite would
///   drop cairn-specific behaviour and add a router hop.
///
/// Pre-PR-C2 this was a direct `ferriskey::Client::hget` against
/// `{exec}:waitpoint:<wp>`; PR-C2 routes it through the backend trait
/// so pg/sqlite backends answer the same shape without wire-layer
/// Valkey coupling.
pub(crate) async fn read_waitpoint_token(
    backend: &dyn EngineBackend,
    partition_config: &flowfabric::core::partition::PartitionConfig,
    execution_id: &ExecutionId,
    waitpoint_id: &WaitpointId,
) -> Result<WaitpointToken, FabricError> {
    let partition = execution_partition(execution_id, partition_config);
    let token_opt = backend
        .read_waitpoint_token(partition.into(), waitpoint_id)
        .await
        .map_err(|e| FabricError::Engine(Box::new(e)))?;
    match token_opt {
        Some(s) if !s.is_empty() => Ok(WaitpointToken::new(s)),
        _ => Err(FabricError::Validation {
            reason: format!("waitpoint {waitpoint_id} is not active (missing token)"),
        }),
    }
}

pub struct SignalBridge {
    /// Backend-agnostic runtime handle. Used for `partition_config()`,
    /// `signal_dedup_ttl_ms()`, and `backend()` — the trait-routed
    /// `EngineBackend::deliver_signal` reaches both Valkey and
    /// Postgres without the Lua-only FCALL path.
    runtime: Arc<dyn FabricRuntimeHandle>,
}

impl SignalBridge {
    pub fn new(runtime: Arc<dyn FabricRuntimeHandle>) -> Self {
        Self { runtime }
    }

    pub async fn deliver_approval_signal(
        &self,
        execution_id: &ExecutionId,
        waitpoint_id: &WaitpointId,
        approved: bool,
        approval_id: &str,
        details: Option<String>,
    ) -> Result<SignalOutcome, FabricError> {
        let safe_id = sanitize_signal_component(approval_id);
        let signal_name = if approved {
            format!("approval_granted:{safe_id}")
        } else {
            format!("approval_rejected:{safe_id}")
        };

        let payload = details.map(|d| {
            serde_json::json!({
                "approved": approved,
                "details": d,
            })
            .to_string()
            .into_bytes()
        });

        let waitpoint_token = read_waitpoint_token(
            self.runtime.backend().as_ref(),
            self.runtime.partition_config(),
            execution_id,
            waitpoint_id,
        )
        .await?;

        let signal = Signal {
            signal_name,
            signal_category: "approval".into(),
            payload,
            source_type: crate::constants::SOURCE_TYPE_APPROVAL_OPERATOR.into(),
            source_identity: "cairn".into(),
            idempotency_key: Some(format!("approval:{safe_id}")),
            waitpoint_token,
        };

        self.deliver_signal(execution_id, waitpoint_id, signal)
            .await
    }

    pub async fn deliver_child_completed_signal(
        &self,
        parent_execution_id: &ExecutionId,
        parent_waitpoint_id: &WaitpointId,
        child_task_id: &str,
        success: bool,
    ) -> Result<SignalOutcome, FabricError> {
        let payload = serde_json::json!({
            "child_task_id": child_task_id,
            "success": success,
        })
        .to_string()
        .into_bytes();

        let safe_id = sanitize_signal_component(child_task_id);
        let waitpoint_token = read_waitpoint_token(
            self.runtime.backend().as_ref(),
            self.runtime.partition_config(),
            parent_execution_id,
            parent_waitpoint_id,
        )
        .await?;

        let signal = Signal {
            signal_name: format!("child_completed:{safe_id}"),
            signal_category: "subagent".into(),
            payload: Some(payload),
            source_type: crate::constants::SOURCE_TYPE_RUNTIME.into(),
            source_identity: "cairn".into(),
            idempotency_key: Some(format!("child_completed:{safe_id}")),
            waitpoint_token,
        };

        self.deliver_signal(parent_execution_id, parent_waitpoint_id, signal)
            .await
    }

    pub async fn deliver_tool_result_signal(
        &self,
        execution_id: &ExecutionId,
        waitpoint_id: &WaitpointId,
        invocation_id: &str,
        result_payload: Option<Vec<u8>>,
    ) -> Result<SignalOutcome, FabricError> {
        let safe_id = sanitize_signal_component(invocation_id);
        let waitpoint_token = read_waitpoint_token(
            self.runtime.backend().as_ref(),
            self.runtime.partition_config(),
            execution_id,
            waitpoint_id,
        )
        .await?;

        let signal = Signal {
            signal_name: format!("tool_result:{safe_id}"),
            signal_category: "tool".into(),
            payload: result_payload,
            source_type: crate::constants::SOURCE_TYPE_RUNTIME.into(),
            source_identity: "cairn".into(),
            idempotency_key: Some(format!("tool_result:{safe_id}")),
            waitpoint_token,
        };

        self.deliver_signal(execution_id, waitpoint_id, signal)
            .await
    }

    async fn deliver_signal(
        &self,
        execution_id: &ExecutionId,
        waitpoint_id: &WaitpointId,
        signal: Signal,
    ) -> Result<SignalOutcome, FabricError> {
        // RFC-025 / FF 0.14 adoption: route signal delivery through the
        // backend-agnostic `EngineBackend::deliver_signal` trait method
        // instead of the Valkey-only `ff_deliver_signal` Lua FCALL.
        // The PG backend's bodied `deliver_signal` performs the same
        // suspend-table read + waitpoint-token verification + signal-
        // stream append inside a SERIALIZABLE transaction, so cairn
        // gets identical semantics on either backend.
        //
        // Historical note: `load_lane_id` and `build_deliver_signal`
        // were required when we hand-built Lua KEYS/ARGV; the trait
        // method takes a typed `DeliverSignalArgs` so the lane cache
        // and the key-layout helper are no longer needed on this hot
        // path. The helper is retained for the lane-id-bearing
        // approval-signal call in `FabricRunService` which still goes
        // through `ControlPlaneBackend::deliver_approval_signal` until
        // that surface merges with this one.
        // Capture one wall-clock sample and reuse it for `created_at`
        // + `now` so the args represent a single dispatch moment —
        // matches FF's own `deliver_approval_signal_impl` pattern on
        // the Valkey backend. Gemini PR #630 review.
        let now = TimestampMs::now();

        // `payload_encoding` stays `None` at this layer.
        // `deliver_approval_signal` + `deliver_child_completed_signal`
        // build JSON bodies and could truthfully declare `"json"`, but
        // `deliver_tool_result_signal` takes arbitrary caller bytes —
        // there is no shape-honest single default. FF's own
        // `deliver_approval_signal_impl` on Valkey also passes
        // `None`; we match that. Gemini PR #630 review.
        let args = flowfabric::core::contracts::DeliverSignalArgs {
            execution_id: execution_id.clone(),
            waitpoint_id: waitpoint_id.clone(),
            signal_id: SignalId::new(),
            signal_name: signal.signal_name,
            signal_category: signal.signal_category,
            source_type: signal.source_type,
            source_identity: signal.source_identity,
            payload: signal.payload,
            payload_encoding: None,
            correlation_id: None,
            idempotency_key: signal.idempotency_key,
            target_scope: "waitpoint".to_owned(),
            created_at: Some(now),
            dedup_ttl_ms: Some(self.runtime.signal_dedup_ttl_ms()),
            resume_delay_ms: None,
            max_signals_per_execution: Some(
                crate::constants::DEFAULT_MAX_SIGNALS_PER_EXECUTION_U64,
            ),
            signal_maxlen: Some(crate::constants::DEFAULT_SIGNAL_MAXLEN_U64),
            waitpoint_token: signal.waitpoint_token,
            now,
        };

        let result = self
            .runtime
            .backend()
            .deliver_signal(args)
            .await
            .map_err(|e| FabricError::Engine(Box::new(e)))?;

        Ok(match result {
            flowfabric::core::contracts::DeliverSignalResult::Accepted { signal_id, effect } => {
                if effect == "resume_condition_satisfied" {
                    SignalOutcome::TriggeredResume { signal_id }
                } else {
                    SignalOutcome::Accepted { signal_id, effect }
                }
            }
            flowfabric::core::contracts::DeliverSignalResult::Duplicate { existing_signal_id } => {
                SignalOutcome::Duplicate {
                    existing_signal_id: existing_signal_id.to_string(),
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signal_carries_waitpoint_token_field() {
        // Pin the contract: after FF v2 #4, every Signal MUST carry a
        // waitpoint_token. If ff-sdk removes or renames the field, this
        // test fails to compile — which is the point.
        let token = WaitpointToken::new("kid_1:deadbeef");
        let signal = Signal {
            signal_name: "approval_granted:foo".into(),
            signal_category: "approval".into(),
            payload: None,
            source_type: crate::constants::SOURCE_TYPE_APPROVAL_OPERATOR.into(),
            source_identity: "cairn".into(),
            idempotency_key: Some("approval:foo".into()),
            waitpoint_token: token.clone(),
        };
        assert_eq!(signal.waitpoint_token.as_str(), "kid_1:deadbeef");
        // Debug MUST redact; it's ok to rely on the ff-core guarantee, but
        // pin it here because a regression would leak HMAC material into logs.
        let dbg = format!("{:?}", signal.waitpoint_token);
        assert!(dbg.contains("REDACTED"), "token Debug must redact: {dbg}");
        assert!(!dbg.contains("deadbeef"), "token hex must not leak: {dbg}");
    }

    #[test]
    fn signal_debug_redacts_token_transitively() {
        // Derive(Debug) on Signal delegates to WaitpointToken::Debug, which
        // redacts. Pin this so a future ff-sdk custom Debug impl that used
        // token.as_str() would leak HMAC material into every tracing!(?signal).
        let token = WaitpointToken::new("kid_3:deadbeefcafe");
        let signal = Signal {
            signal_name: "tool_result:x".into(),
            signal_category: "tool".into(),
            payload: None,
            source_type: "cairn_runtime".into(),
            source_identity: "cairn".into(),
            idempotency_key: None,
            waitpoint_token: token,
        };
        let dbg = format!("{signal:?}");
        assert!(
            !dbg.contains("deadbeef"),
            "Signal Debug leaked token material: {dbg}"
        );
        assert!(
            dbg.contains("REDACTED"),
            "Signal Debug should surface redaction marker: {dbg}"
        );
    }

    #[test]
    fn waitpoint_token_display_redacts() {
        // Defensive: if we ever log a token accidentally via Display (e.g. in
        // an error message), the redaction must hold.
        let token = WaitpointToken::new("kid_2:cafebabe0011");
        let disp = format!("{token}");
        assert!(disp.contains("REDACTED"), "Display must redact: {disp}");
        assert!(
            !disp.contains("cafebabe"),
            "token hex must not leak: {disp}"
        );
    }

    #[test]
    fn approval_signal_name_granted() {
        let id = "appr_1";
        let name = format!("approval_granted:{id}");
        assert_eq!(name, "approval_granted:appr_1");
    }

    #[test]
    fn approval_signal_name_rejected() {
        let id = "appr_2";
        let name = format!("approval_rejected:{id}");
        assert_eq!(name, "approval_rejected:appr_2");
    }

    #[test]
    fn child_completed_signal_name_format() {
        let name = format!("child_completed:{}", "task_abc");
        assert_eq!(name, "child_completed:task_abc");
    }

    #[test]
    fn tool_result_signal_name_format() {
        let name = format!("tool_result:{}", "inv_xyz");
        assert_eq!(name, "tool_result:inv_xyz");
    }

    #[test]
    fn idempotency_key_format_approval() {
        let key = format!("approval:{}", "appr_1");
        assert_eq!(key, "approval:appr_1");
    }

    #[test]
    fn idempotency_key_format_child() {
        let key = format!("child_completed:{}", "task_1");
        assert_eq!(key, "child_completed:task_1");
    }

    #[test]
    fn idempotency_key_format_tool() {
        let key = format!("tool_result:{}", "inv_1");
        assert_eq!(key, "tool_result:inv_1");
    }

    #[test]
    fn approval_payload_json_structure() {
        let payload = serde_json::json!({
            "approved": true,
            "details": "looks good",
        });
        let obj = payload.as_object().unwrap();
        assert_eq!(obj.get("approved").unwrap(), true);
        assert_eq!(obj.get("details").unwrap(), "looks good");
    }

    #[test]
    fn child_completed_payload_json_structure() {
        let payload = serde_json::json!({
            "child_task_id": "task_1",
            "success": false,
        });
        let obj = payload.as_object().unwrap();
        assert_eq!(obj.get("child_task_id").unwrap(), "task_1");
        assert_eq!(obj.get("success").unwrap(), false);
    }
}
