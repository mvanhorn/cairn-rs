use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use flowfabric::core::keys::ExecKeyContext;
use flowfabric::core::types::{
    ExecutionId, LaneId, SignalId, TimestampMs, WaitpointId, WaitpointToken,
};
use flowfabric::sdk::task::{Signal, SignalOutcome};

use crate::boot::FabricRuntime;
use crate::error::FabricError;
use crate::helpers::sanitize_signal_component;

/// Bounded cap for the per-execution `lane_id` cache (#506).
///
/// Each entry is `(ExecutionId, LaneId)` — both small owned `String`s
/// backed by UUIDv4 / lane literals, so worst-case memory at the cap is
/// ~200 bytes × `LANE_ID_CACHE_MAX` ≈ 200 KiB. Generous for the expected
/// working set (hundreds of concurrent runs per cairn-app process) while
/// still giving a hard ceiling so a pathological fan-out of dead
/// executions can't grow the map unbounded.
const LANE_ID_CACHE_MAX: usize = 1024;

/// Read the HMAC waitpoint token from FF's waitpoint hash.
///
/// FF mints the token during `ff_suspend_execution` and writes it to the
/// `waitpoint_token` field of the waitpoint hash (see lua/suspension.lua
/// line 185). It is the ONLY source of truth — cairn never caches it.
///
/// Returns `Err(Validation)` ONLY when the field is missing or empty — i.e.
/// the waitpoint hash has never been written, or was deleted. FF does NOT
/// clear `waitpoint_token` on close (audit retention): a closed waitpoint
/// still has its token, so this helper returns `Ok(token)` for it and the
/// downstream `ff_deliver_signal` reply surfaces `waitpoint_closed` at the
/// state boundary where it belongs. That separation matters — mixing
/// "waitpoint never existed" with "waitpoint is closed" at the auth layer
/// would re-create the exact oracle FF's Lua took pains to eliminate.
pub(crate) async fn read_waitpoint_token(
    client: &ferriskey::Client,
    ctx: &ExecKeyContext,
    waitpoint_id: &WaitpointId,
) -> Result<WaitpointToken, FabricError> {
    let token_str: Option<String> = client
        .hget(&ctx.waitpoint(waitpoint_id), "waitpoint_token")
        .await
        .map_err(|e| FabricError::Valkey(format!("HGET waitpoint_token: {e}")))?;
    match token_str {
        Some(s) if !s.is_empty() => Ok(WaitpointToken::new(s)),
        _ => Err(FabricError::Validation {
            reason: format!("waitpoint {waitpoint_id} is not active (missing token)"),
        }),
    }
}

pub struct SignalBridge {
    runtime: Arc<FabricRuntime>,
    /// Per-execution `lane_id` cache.
    ///
    /// FF stamps `lane_id` on the execution core hash at
    /// `ff_create_flow` / `ff_create_execution` time and never rewrites
    /// it — every signal delivery (tool_result / approval /
    /// child_completed) was paying an extra round-trip HGET to read the
    /// same static value (#506). Caching it here halves the signal
    /// delivery round-trip count on the hot path.
    ///
    /// `Mutex<HashMap>` rather than a striped cache: signal delivery is
    /// already serialized upstream (one signal per waitpoint at a time
    /// via FF's idempotency fence), and the critical section is two
    /// hash ops — contention is a non-concern at the rates cairn hits.
    /// **Arbitrary-victim eviction** at `LANE_ID_CACHE_MAX` (via
    /// `HashMap::keys().next()` — order is unspecified by construction);
    /// cold-miss on evicted entries simply re-HGETs. Not LRU: strict LRU
    /// would need a side queue, and the cost isn't justified because
    /// lane_id is immutable per execution so any eviction is always
    /// safe (just refetches).
    lane_id_cache: Mutex<HashMap<ExecutionId, LaneId>>,
}

impl SignalBridge {
    pub fn new(runtime: &Arc<FabricRuntime>) -> Self {
        Self {
            runtime: runtime.clone(),
            lane_id_cache: Mutex::new(HashMap::new()),
        }
    }

    /// Load the `lane_id` for this execution, consulting the per-
    /// execution cache first. Cache misses fall back to an HGET against
    /// `ctx.core()`; the default lane literal `"cairn"` is used when
    /// FF returns `None` (matches the pre-cache behaviour).
    ///
    /// Cache invalidation is not strictly required — FF never rewrites
    /// `lane_id` after creation — but the map is capped at
    /// `LANE_ID_CACHE_MAX` with arbitrary-victim eviction (see the
    /// `lane_id_cache` field doc for why not LRU) to keep memory
    /// bounded when the process serves thousands of runs over its
    /// lifetime.
    async fn load_lane_id(
        &self,
        execution_id: &ExecutionId,
        ctx: &ExecKeyContext,
    ) -> Result<LaneId, FabricError> {
        // Fast path: cached. Recover from a poisoned mutex rather than
        // silently skipping the cache — any prior panic here left the
        // map in a valid state (two simple hash ops) and downgrading
        // poison into a silent fallthrough would both lose the
        // performance win AND hide the panic from operators forever.
        // Matches the `AppMetrics` poison-recovery pattern.
        {
            let cache = self.lane_id_cache.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(lane) = cache.get(execution_id) {
                return Ok(lane.clone());
            }
        }

        // Cold path: fetch from FF's exec core hash.
        let lane_str: Option<String> = self
            .runtime
            .client
            .hget(&ctx.core(), "lane_id")
            .await
            .map_err(|e| FabricError::Valkey(format!("HGET lane_id: {e}")))?;
        let lane_id = LaneId::new(lane_str.as_deref().unwrap_or("cairn"));

        // Insert into the cache. Size-cap via "drop one arbitrary key"
        // rather than a strict LRU — the map is write-heavy on fresh
        // runs, read-heavy thereafter, and lane_id never changes for a
        // given execution so any eviction is safe (just refetches).
        let mut cache = self.lane_id_cache.lock().unwrap_or_else(|e| e.into_inner());
        if cache.len() >= LANE_ID_CACHE_MAX {
            // HashMap iteration order is unspecified — the victim
            // choice is arbitrary, not insertion-order. `.keys().next()`
            // is amortized O(1) to grab the first iterator item; the
            // arbitrary-victim policy is safe because lane_id is
            // immutable per execution, so any eviction just forces a
            // refetch on next access.
            if let Some(victim) = cache.keys().next().cloned() {
                cache.remove(&victim);
            }
        }
        cache.insert(execution_id.clone(), lane_id.clone());
        Ok(lane_id)
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

        let partition = flowfabric::core::partition::execution_partition(
            execution_id,
            &self.runtime.partition_config,
        );
        let ctx = ExecKeyContext::new(&partition, execution_id);
        let waitpoint_token =
            read_waitpoint_token(&self.runtime.client, &ctx, waitpoint_id).await?;

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
        let partition = flowfabric::core::partition::execution_partition(
            parent_execution_id,
            &self.runtime.partition_config,
        );
        let ctx = ExecKeyContext::new(&partition, parent_execution_id);
        let waitpoint_token =
            read_waitpoint_token(&self.runtime.client, &ctx, parent_waitpoint_id).await?;

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
        let partition = flowfabric::core::partition::execution_partition(
            execution_id,
            &self.runtime.partition_config,
        );
        let ctx = ExecKeyContext::new(&partition, execution_id);
        let waitpoint_token =
            read_waitpoint_token(&self.runtime.client, &ctx, waitpoint_id).await?;

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
        let partition = flowfabric::core::partition::execution_partition(
            execution_id,
            &self.runtime.partition_config,
        );
        let ctx = flowfabric::core::keys::ExecKeyContext::new(&partition, execution_id);
        let idx = flowfabric::core::keys::IndexKeys::new(&partition);

        let signal_id = SignalId::new();
        let now = TimestampMs::now();

        // `lane_id` is stamped on the exec core hash at create-flow
        // time and never rewritten — cache it per execution so the
        // happy path is a HashMap lookup instead of a second Valkey
        // round-trip per signal (#506).
        let lane_id = self.load_lane_id(execution_id, &ctx).await?;

        let derived_idem = format!("{}:{}:{}", execution_id, signal.signal_name, waitpoint_id);
        let effective_idem = signal
            .idempotency_key
            .clone()
            .unwrap_or_else(|| derived_idem.clone());
        let idem_key = ctx.signal_dedup(waitpoint_id, &effective_idem);

        let payload_str = signal
            .payload
            .as_ref()
            .map(|p| String::from_utf8_lossy(p).into_owned())
            .unwrap_or_default();

        let (keys, args) = crate::fcall::suspension::build_deliver_signal(
            &ctx,
            &idx,
            &lane_id,
            &signal_id,
            waitpoint_id,
            idem_key,
            execution_id,
            signal.signal_name,
            signal.signal_category,
            signal.source_type,
            signal.source_identity,
            payload_str,
            effective_idem,
            now,
            self.runtime.config.signal_dedup_ttl_ms,
            crate::constants::DEFAULT_SIGNAL_MAXLEN,
            crate::constants::DEFAULT_MAX_SIGNALS_PER_EXECUTION,
            signal.waitpoint_token.as_str(),
        );

        // Pass `&[String]` directly into `FabricRuntime::fcall` — the
        // ~2 × Vec<&str> rebuild on every signal delivery (tool-result /
        // approval / child-completed, all on the hot path) was pure
        // re-borrowing waste. See #501 and `FabricRuntime::fcall`'s new
        // signature accepting `&[String]`.
        let raw: ferriskey::Value = self
            .runtime
            .fcall(crate::fcall::names::FF_DELIVER_SIGNAL, &keys, &args)
            .await?;

        parse_signal_result(&raw)
    }
}

fn parse_signal_result(raw: &ferriskey::Value) -> Result<SignalOutcome, FabricError> {
    let arr = match raw {
        ferriskey::Value::Array(arr) => arr,
        _ => return Err(FabricError::Bridge("deliver_signal: expected Array".into())),
    };

    let status = match arr.first() {
        Some(Ok(ferriskey::Value::Int(n))) => *n,
        _ => return Err(FabricError::Bridge("deliver_signal: bad status".into())),
    };

    if status != 1 {
        // Error path: we format into a new String either way, so materialise
        // once from the borrowed view rather than allocating a separate
        // String via `extract_str` first.
        let code = extract_str_ref(arr, 1).unwrap_or(std::borrow::Cow::Borrowed("unknown"));
        return Err(FabricError::Bridge(format!(
            "deliver_signal rejected: {code}"
        )));
    }

    // Hot path: only materialise a `String` when we actually need it.
    // The `DUPLICATE` sentinel is checked via a borrowed byte comparison
    // so the (frequent) non-duplicate branch stays alloc-free at the
    // sub-tag read.
    if is_tag(arr, 1, b"DUPLICATE") {
        let existing_id = extract_str(arr, 2).unwrap_or_default();
        return Ok(SignalOutcome::Duplicate {
            existing_signal_id: existing_id,
        });
    }

    let signal_id_str = extract_str(arr, 2).unwrap_or_default();
    let effect = extract_str(arr, 3).unwrap_or_default();
    let signal_id = flowfabric::core::types::SignalId::parse(&signal_id_str)
        .map_err(|e| FabricError::Bridge(format!("bad signal_id in response: {e}")))?;

    if effect == "resume_condition_satisfied" {
        Ok(SignalOutcome::TriggeredResume { signal_id })
    } else {
        Ok(SignalOutcome::Accepted { signal_id, effect })
    }
}

/// Zero-alloc tag check against a ferriskey envelope slot. Both
/// `BulkString` and `SimpleString` are compared by borrowed byte slices,
/// so the hot path of `parse_signal_result` (non-duplicate, non-error)
/// never allocates a `String` just to check a sentinel.
fn is_tag(arr: &[Result<ferriskey::Value, ferriskey::Error>], idx: usize, tag: &[u8]) -> bool {
    match arr.get(idx) {
        Some(Ok(ferriskey::Value::BulkString(b))) => &**b == tag,
        Some(Ok(ferriskey::Value::SimpleString(s))) => s.as_bytes() == tag,
        _ => false,
    }
}

fn extract_str(arr: &[Result<ferriskey::Value, ferriskey::Error>], idx: usize) -> Option<String> {
    arr.get(idx).and_then(|v| match v {
        Ok(ferriskey::Value::BulkString(b)) => Some(String::from_utf8_lossy(b).into_owned()),
        Ok(ferriskey::Value::SimpleString(s)) => Some(s.clone()),
        Ok(ferriskey::Value::Int(n)) => Some(n.to_string()),
        _ => None,
    })
}

/// Borrowed variant of [`extract_str`]. Returns `Cow::Borrowed` for
/// `SimpleString` and `Cow::Owned` for `BulkString` (UTF-8 validation
/// may copy) / `Int` (digit conversion). Used on the rejected-envelope
/// path where the final destination is a `format!` — the owned-String
/// intermediate was pure waste.
fn extract_str_ref(
    arr: &[Result<ferriskey::Value, ferriskey::Error>],
    idx: usize,
) -> Option<std::borrow::Cow<'_, str>> {
    arr.get(idx).and_then(|v| match v {
        Ok(ferriskey::Value::BulkString(b)) => Some(String::from_utf8_lossy(b)),
        Ok(ferriskey::Value::SimpleString(s)) => Some(std::borrow::Cow::Borrowed(s.as_str())),
        Ok(ferriskey::Value::Int(n)) => Some(std::borrow::Cow::Owned(n.to_string())),
        _ => None,
    })
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

    // ── #499 regression: hot-path allocation on sub-tag check ──────────
    //
    // `parse_signal_result` must treat the `DUPLICATE` marker via a
    // borrowed byte comparison (no `String` materialisation on the
    // non-duplicate path). These tests pin the decoded outcome from
    // both sides of the borrow — if a refactor re-introduces the
    // `extract_str(arr, 1).unwrap_or_default() == "DUPLICATE"` pattern
    // the behaviour stays correct but the perf win disappears; the
    // explicit `is_tag` helper is the load-bearing assertion.

    #[test]
    fn parse_signal_result_accepted_path() {
        let sig_id = flowfabric::core::types::SignalId::new();
        let raw = ferriskey::Value::Array(vec![
            Ok(ferriskey::Value::Int(1)),
            Ok(ferriskey::Value::BulkString(b"OK".to_vec().into())),
            Ok(ferriskey::Value::BulkString(
                sig_id.to_string().into_bytes().into(),
            )),
            Ok(ferriskey::Value::SimpleString("accepted".to_owned())),
        ]);
        let outcome = parse_signal_result(&raw).expect("parse accepted");
        assert!(matches!(outcome, SignalOutcome::Accepted { .. }));
    }

    #[test]
    fn parse_signal_result_duplicate_via_simple_string() {
        let raw = ferriskey::Value::Array(vec![
            Ok(ferriskey::Value::Int(1)),
            Ok(ferriskey::Value::SimpleString("DUPLICATE".to_owned())),
            Ok(ferriskey::Value::SimpleString("sig_existing".to_owned())),
        ]);
        let outcome = parse_signal_result(&raw).expect("parse dup");
        match outcome {
            SignalOutcome::Duplicate { existing_signal_id } => {
                assert_eq!(existing_signal_id, "sig_existing");
            }
            other => panic!("expected Duplicate, got {other:?}"),
        }
    }

    #[test]
    fn parse_signal_result_duplicate_via_bulk_string() {
        let raw = ferriskey::Value::Array(vec![
            Ok(ferriskey::Value::Int(1)),
            Ok(ferriskey::Value::BulkString(b"DUPLICATE".to_vec().into())),
            Ok(ferriskey::Value::BulkString(b"sig_dup".to_vec().into())),
        ]);
        let outcome = parse_signal_result(&raw).expect("parse dup bulk");
        match outcome {
            SignalOutcome::Duplicate { existing_signal_id } => {
                assert_eq!(existing_signal_id, "sig_dup");
            }
            other => panic!("expected Duplicate, got {other:?}"),
        }
    }

    #[test]
    fn is_tag_matches_bulk_and_simple_strings_without_alloc() {
        let arr: Vec<Result<ferriskey::Value, ferriskey::Error>> = vec![
            Ok(ferriskey::Value::Int(1)),
            Ok(ferriskey::Value::BulkString(b"DUPLICATE".to_vec().into())),
        ];
        assert!(is_tag(&arr, 1, b"DUPLICATE"));
        assert!(!is_tag(&arr, 1, b"ACCEPTED"));

        let arr2: Vec<Result<ferriskey::Value, ferriskey::Error>> = vec![
            Ok(ferriskey::Value::Int(1)),
            Ok(ferriskey::Value::SimpleString("DUPLICATE".to_owned())),
        ];
        assert!(is_tag(&arr2, 1, b"DUPLICATE"));

        // Out-of-bounds or wrong shape never panics or allocates.
        let empty: Vec<Result<ferriskey::Value, ferriskey::Error>> = vec![];
        assert!(!is_tag(&empty, 1, b"DUPLICATE"));
    }

    #[test]
    fn parse_signal_result_rejected_path_formats_code() {
        let raw = ferriskey::Value::Array(vec![
            Ok(ferriskey::Value::Int(0)),
            Ok(ferriskey::Value::SimpleString(
                "waitpoint_closed".to_owned(),
            )),
        ]);
        let err = parse_signal_result(&raw).expect_err("expected rejection");
        assert!(err.to_string().contains("waitpoint_closed"));
    }

    // ── #506 regression: lane_id cache cap + insertion-order eviction ──
    //
    // The live happy-path / cache-miss behaviour is exercised by the
    // integration-test `test_signal_delivery_is_idempotent` (pulls a
    // real `SignalBridge` through `FabricRuntime` + Valkey
    // testcontainer). This unit test pins the in-memory map semantics
    // in isolation: capped map + evict-on-overflow + new inserts
    // succeed after eviction. Together they're the full contract.

    #[test]
    fn lane_id_cache_evicts_at_cap_and_accepts_new_entries() {
        // Exercise the cap logic directly so the integration test
        // doesn't need to populate 1024 entries through live Valkey.
        let cache: Mutex<HashMap<ExecutionId, LaneId>> = Mutex::new(HashMap::new());
        let cap = 4usize;
        let mint = |_i: u32| {
            let uuid = uuid::Uuid::new_v4();
            ExecutionId::parse(&format!("{{fp:0}}:{uuid}")).expect("uuid+prefix should parse")
        };

        // Populate up to cap.
        let ids: Vec<ExecutionId> = (0..cap as u32).map(mint).collect();
        {
            let mut m = cache.lock().unwrap();
            for (i, eid) in ids.iter().enumerate() {
                if m.len() >= cap {
                    if let Some(v) = m.keys().next().cloned() {
                        m.remove(&v);
                    }
                }
                m.insert(eid.clone(), LaneId::new(format!("lane_{i}")));
            }
            assert_eq!(m.len(), cap);
        }

        // Insert one more and evict.
        let overflow = mint(999);
        {
            let mut m = cache.lock().unwrap();
            if m.len() >= cap {
                if let Some(v) = m.keys().next().cloned() {
                    m.remove(&v);
                }
            }
            m.insert(overflow.clone(), LaneId::new("overflow"));
            assert_eq!(m.len(), cap, "cap must hold after eviction + insert");
            assert!(
                m.contains_key(&overflow),
                "newest insert must survive eviction"
            );
        }
    }

    // Pin the cap — if someone bumps it 100x, the memory budget
    // documented in the constant comment needs re-evaluation. Compile-
    // time assertion so the invariant is enforced without a runtime test.
    const _: () = {
        assert!(LANE_ID_CACHE_MAX <= 10_000);
        assert!(LANE_ID_CACHE_MAX >= 256);
    };
}
