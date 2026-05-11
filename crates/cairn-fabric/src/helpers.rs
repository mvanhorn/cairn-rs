#[cfg(feature = "fabric-valkey")]
use std::borrow::Cow;

use cairn_domain::ProjectKey;

#[cfg(feature = "fabric-valkey")]
use crate::error::FabricError;

/// Current wall-clock time in milliseconds since UNIX_EPOCH. On clock skew
/// (system clock set before 1970), logs a warning and returns 0 so the
/// caller at least sees an obviously-wrong timestamp rather than silently
/// continuing with `Duration::default()`.
pub fn now_ms() -> u64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => d.as_millis() as u64,
        Err(e) => {
            tracing::warn!(error = %e, "system clock is before UNIX_EPOCH — returning 0");
            0
        }
    }
}

#[cfg(feature = "fabric-valkey")]
pub fn check_fcall_success(raw: &ferriskey::Value, function_name: &str) -> Result<(), FabricError> {
    let arr = match raw {
        ferriskey::Value::Array(arr) => arr,
        _ => return Ok(()),
    };
    let status = match arr.first() {
        Some(Ok(ferriskey::Value::Int(n))) => *n,
        _ => return Ok(()),
    };
    if status == 1 {
        return Ok(());
    }
    // Only allocate on the rejected path. `fcall_error_code_ref` returns a
    // borrowed `&str` for `SimpleString` (no alloc) and a `Cow::Owned` only
    // for `BulkString` (one unavoidable UTF-8-validation copy). The
    // `format!` below materialises the final message either way, but we
    // no longer pay a separate owned-String allocation when a caller
    // pre-dispatches on the typed code (see `fcall_error_code_ref`).
    let code = fcall_error_code_ref(raw).unwrap_or(Cow::Borrowed("unknown"));
    Err(FabricError::Internal(format!(
        "{function_name} rejected: {code}"
    )))
}

/// Extract the Lua error code string from a rejected fcall envelope
/// (`{Int(status_code), BulkString(error_code), ...}`). Returns `None` when
/// the envelope is OK or malformed.
///
/// **Prefer [`fcall_error_code_ref`]** — it returns a borrowed `Cow<'_, str>`
/// (zero alloc for `SimpleString`) and lets typed-code dispatch avoid the
/// owned-String round-trip entirely. This owned-String variant is retained
/// only for backwards-compat with callers that cannot thread a lifetime
/// through; new code should use the `_ref` form.
///
/// Callers use this to dispatch on FF's typed error codes (e.g.
/// `use_claim_resumed_execution`) without going through the string-formatted
/// [`FabricError::Internal`] message. Keep the caller pattern:
///
/// ```ignore
/// if let Some(code) = fcall_error_code_ref(&raw) {
///     if code == "use_claim_resumed_execution" { /* dispatch */ }
/// }
/// check_fcall_success(&raw, FF_…)?;
/// ```
#[cfg(feature = "fabric-valkey")]
pub fn fcall_error_code(raw: &ferriskey::Value) -> Option<String> {
    fcall_error_code_ref(raw).map(Cow::into_owned)
}

/// Zero-alloc variant of [`fcall_error_code`]: returns `Cow::Borrowed` for
/// the `SimpleString` envelope shape (FF's most common on typed-error paths
/// — `lease_expired`, `stale_lease`, `waitpoint_closed`, etc.) and
/// `Cow::Owned` only for `BulkString` (which requires a UTF-8 validation
/// copy). The success path stays alloc-free because the function
/// short-circuits on `status == 1` before touching the error slot.
#[cfg(feature = "fabric-valkey")]
pub fn fcall_error_code_ref(raw: &ferriskey::Value) -> Option<Cow<'_, str>> {
    let arr = match raw {
        ferriskey::Value::Array(arr) => arr,
        _ => return None,
    };
    let status = match arr.first() {
        Some(Ok(ferriskey::Value::Int(n))) => *n,
        _ => return None,
    };
    if status == 1 {
        return None;
    }
    match arr.get(1) {
        // `String::from_utf8_lossy` returns `Cow<'_, str>` — Borrowed when
        // the bytes are valid UTF-8 (FF always writes ASCII codes here),
        // Owned only on the cold error-recovery path.
        Some(Ok(ferriskey::Value::BulkString(b))) => Some(String::from_utf8_lossy(b)),
        Some(Ok(ferriskey::Value::SimpleString(s))) => Some(Cow::Borrowed(s.as_str())),
        _ => None,
    }
}

pub fn parse_public_state(s: &str) -> flowfabric::core::state::PublicState {
    match s {
        "waiting" => flowfabric::core::state::PublicState::Waiting,
        "delayed" => flowfabric::core::state::PublicState::Delayed,
        "rate_limited" => flowfabric::core::state::PublicState::RateLimited,
        "waiting_children" => flowfabric::core::state::PublicState::WaitingChildren,
        "active" => flowfabric::core::state::PublicState::Active,
        "suspended" => flowfabric::core::state::PublicState::Suspended,
        "completed" => flowfabric::core::state::PublicState::Completed,
        "failed" => flowfabric::core::state::PublicState::Failed,
        "cancelled" => flowfabric::core::state::PublicState::Cancelled,
        "expired" => flowfabric::core::state::PublicState::Expired,
        "skipped" => flowfabric::core::state::PublicState::Skipped,
        // FF 0.9 (RFC-014 Stage 2) addition — transient state between
        // Suspended and Active, serialized as "resumable". Without
        // this arm the snapshot-decode path silently falls through to
        // `Waiting` and misclassifies a mid-resume run as queued.
        "resumable" => flowfabric::core::state::PublicState::Resumable,
        _ => flowfabric::core::state::PublicState::Waiting,
    }
}

pub fn try_parse_project_key(s: &str) -> Option<ProjectKey> {
    let parts: Vec<&str> = s.splitn(3, '/').collect();
    match parts.as_slice() {
        [t, w, p] if !t.is_empty() && !w.is_empty() && !p.is_empty() => {
            Some(ProjectKey::new(*t, *w, *p))
        }
        _ => None,
    }
}

/// Look up `key` in the HGETALL-shaped map, returning the borrowed value
/// iff it exists and is non-empty.
///
/// Returns `Option<&str>` (was `Option<String>`, which cloned on every
/// call — see issue #509). Callers that genuinely need to own the string
/// can write `.map(str::to_owned)` at the call site; nothing in-tree
/// does today.
pub fn read_hgetall_field<'a>(
    fields: &'a std::collections::HashMap<String, String>,
    key: &str,
) -> Option<&'a str> {
    fields
        .get(key)
        .map(String::as_str)
        .filter(|v| !v.is_empty())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailOutcome {
    RetryScheduled,
    TerminalFailed,
}

#[cfg(feature = "fabric-valkey")]
pub fn is_already_satisfied(raw: &ferriskey::Value) -> bool {
    if let ferriskey::Value::Array(arr) = raw {
        if let Some(Ok(ferriskey::Value::BulkString(b))) = arr.get(1) {
            return &**b == b"ALREADY_SATISFIED";
        }
        if let Some(Ok(ferriskey::Value::SimpleString(s))) = arr.get(1) {
            return s == "ALREADY_SATISFIED";
        }
    }
    false
}

#[cfg(feature = "fabric-valkey")]
pub fn parse_fail_outcome(raw: &ferriskey::Value) -> FailOutcome {
    if let ferriskey::Value::Array(arr) = raw {
        if let Some(Ok(ferriskey::Value::BulkString(b))) = arr.get(2) {
            if &**b == b"retry_scheduled" {
                return FailOutcome::RetryScheduled;
            }
        }
        if let Some(Ok(ferriskey::Value::SimpleString(s))) = arr.get(2) {
            if s == "retry_scheduled" {
                return FailOutcome::RetryScheduled;
            }
        }
    }
    FailOutcome::TerminalFailed
}

pub fn sanitize_signal_component(s: &str) -> String {
    s.replace(':', "_")
}

/// Extract a `String` out of a ferriskey `Value` in bulk or simple
/// string form. Returns `None` for other shapes.
#[cfg(feature = "fabric-valkey")]
pub fn value_to_string(v: &ferriskey::Value) -> Option<String> {
    match v {
        ferriskey::Value::BulkString(b) => Some(String::from_utf8_lossy(b).into_owned()),
        ferriskey::Value::SimpleString(s) => Some(s.clone()),
        ferriskey::Value::VerbatimString { text, .. } => Some(text.clone()),
        _ => None,
    }
}

/// Flatten a collection of string-like `Value`s into a `Vec<String>`.
/// Handles both `Value::Array(Vec<Result<Value, _>>)` (LRANGE, HKEYS…)
/// and `Value::Set(Vec<Value>)` (SMEMBERS, SINTER…). Errored or
/// non-string entries are skipped silently — missing/garbled members
/// shouldn't crash the read.
///
/// Allocates N+1 `String`s and — on the BulkString arm — performs a
/// lossy UTF-8 decode that copies even when the bytes are already valid
/// UTF-8. For bounded responses (e.g. SMEMBERS of a project-scoped set,
/// typically <100 elements) this is fine. For unbounded reads or for
/// callers that can work with `&str` directly, prefer
/// [`parse_string_array_borrowed`] — it yields a
/// `Cow<'_, str>` per element, borrowing on the valid-UTF-8 hot path
/// (#514).
#[cfg(feature = "fabric-valkey")]
pub fn parse_string_array(raw: &ferriskey::Value) -> Vec<String> {
    match raw {
        ferriskey::Value::Array(items) => items
            .iter()
            .filter_map(|r| r.as_ref().ok())
            .filter_map(value_to_string)
            .collect(),
        ferriskey::Value::Set(items) => items.iter().filter_map(value_to_string).collect(),
        _ => Vec::new(),
    }
}

/// Streaming / borrow-preferring variant of [`parse_string_array`].
/// Yields `Cow<'_, str>` per element:
/// - `Cow::Borrowed` when the underlying value is a valid-UTF-8
///   BulkString, or a SimpleString / VerbatimString (both already
///   owned `String` inside the `Value`) — no allocation.
/// - `Cow::Owned` when the BulkString carries non-UTF-8 bytes and
///   we fall back to `from_utf8_lossy` (same fallback semantics as
///   [`parse_string_array`], to keep behavior identical between the
///   two entry points).
///
/// Use this when the response is unbounded, when the caller wants to
/// filter/`take_while`/`any` without materializing the full list, or
/// when downstream code prefers a typed container other than
/// `Vec<String>` (e.g. `.map(MyId::from).collect()`).
///
/// Returns `impl Iterator` (static dispatch) rather than
/// `Box<dyn Iterator>` — the whole point of this helper is to avoid
/// per-call allocation on the hot path, and the boxed form would have
/// added a heap allocation at every call site, undermining the intent
/// (see review on #561).
#[cfg(feature = "fabric-valkey")]
pub fn parse_string_array_borrowed(
    raw: &ferriskey::Value,
) -> impl Iterator<Item = std::borrow::Cow<'_, str>> + '_ {
    let array_items = match raw {
        ferriskey::Value::Array(items) => Some(
            items
                .iter()
                .filter_map(|r| r.as_ref().ok())
                .filter_map(value_to_cow_str),
        ),
        _ => None,
    };

    let set_items = match raw {
        ferriskey::Value::Set(items) => Some(items.iter().filter_map(value_to_cow_str)),
        _ => None,
    };

    array_items
        .into_iter()
        .flatten()
        .chain(set_items.into_iter().flatten())
}

/// Borrow-preferring sibling of [`value_to_string`]. Returns a
/// [`std::borrow::Cow`] that borrows from `v` on the hot path and only
/// allocates on the lossy-UTF-8 fallback.
#[cfg(feature = "fabric-valkey")]
fn value_to_cow_str(v: &ferriskey::Value) -> Option<std::borrow::Cow<'_, str>> {
    use std::borrow::Cow;
    match v {
        ferriskey::Value::BulkString(b) => Some(match std::str::from_utf8(b) {
            Ok(s) => Cow::Borrowed(s),
            Err(_) => Cow::Owned(String::from_utf8_lossy(b).into_owned()),
        }),
        ferriskey::Value::SimpleString(s) => Some(Cow::Borrowed(s.as_str())),
        ferriskey::Value::VerbatimString { text, .. } => Some(Cow::Borrowed(text.as_str())),
        _ => None,
    }
}

/// Extract the `new_graph_revision` from the
/// `ff_stage_dependency_edge` OK envelope
/// `[1, "OK", "<edge_id>", "<new_graph_revision>"]`. FF's `ok(...)`
/// helper (flowfabric.lua) wraps `(status=1, "OK", ...caller_args)`,
/// so index 3 carries the second caller-supplied value. Returns
/// `None` on malformed shape.
#[cfg(feature = "fabric-valkey")]
pub fn parse_stage_result_revision(raw: &ferriskey::Value) -> Option<u64> {
    let ferriskey::Value::Array(arr) = raw else {
        return None;
    };
    let rev_value = arr.get(3)?.as_ref().ok()?;
    value_to_string(rev_value).and_then(|s| s.parse().ok())
}

/// Extract the eligibility state string from the
/// `ff_evaluate_flow_eligibility` OK envelope `[1, "OK", "<state>"]`.
/// Returns `None` on malformed shape.
#[cfg(feature = "fabric-valkey")]
pub fn parse_eligibility_result(raw: &ferriskey::Value) -> Option<String> {
    let ferriskey::Value::Array(arr) = raw else {
        return None;
    };
    let state_value = arr.get(2)?.as_ref().ok()?;
    value_to_string(state_value)
}

#[cfg(feature = "fabric-valkey")]
pub fn is_duplicate_result(raw: &ferriskey::Value) -> bool {
    if let ferriskey::Value::Array(arr) = raw {
        if let Some(Ok(ferriskey::Value::BulkString(b))) = arr.get(1) {
            return &**b == b"DUPLICATE";
        }
        if let Some(Ok(ferriskey::Value::SimpleString(s))) = arr.get(1) {
            return s == "DUPLICATE";
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_public_state_all_variants() {
        assert_eq!(
            parse_public_state("waiting"),
            flowfabric::core::state::PublicState::Waiting
        );
        assert_eq!(
            parse_public_state("active"),
            flowfabric::core::state::PublicState::Active
        );
        assert_eq!(
            parse_public_state("completed"),
            flowfabric::core::state::PublicState::Completed
        );
        assert_eq!(
            parse_public_state("failed"),
            flowfabric::core::state::PublicState::Failed
        );
        assert_eq!(
            parse_public_state("cancelled"),
            flowfabric::core::state::PublicState::Cancelled
        );
        assert_eq!(
            parse_public_state("suspended"),
            flowfabric::core::state::PublicState::Suspended
        );
        assert_eq!(
            parse_public_state("expired"),
            flowfabric::core::state::PublicState::Expired
        );
        assert_eq!(
            parse_public_state("skipped"),
            flowfabric::core::state::PublicState::Skipped
        );
        assert_eq!(
            parse_public_state("resumable"),
            flowfabric::core::state::PublicState::Resumable
        );
        assert_eq!(
            parse_public_state("delayed"),
            flowfabric::core::state::PublicState::Delayed
        );
        assert_eq!(
            parse_public_state("rate_limited"),
            flowfabric::core::state::PublicState::RateLimited
        );
        assert_eq!(
            parse_public_state("waiting_children"),
            flowfabric::core::state::PublicState::WaitingChildren
        );
        assert_eq!(
            parse_public_state("garbage"),
            flowfabric::core::state::PublicState::Waiting
        );
    }

    #[test]
    fn try_parse_project_key_valid() {
        let pk = try_parse_project_key("t/w/p").unwrap();
        assert_eq!(pk.tenant_id.as_str(), "t");
        assert_eq!(pk.workspace_id.as_str(), "w");
        assert_eq!(pk.project_id.as_str(), "p");
    }

    #[test]
    fn try_parse_project_key_with_slashes() {
        let pk = try_parse_project_key("t/w/p/extra").unwrap();
        assert_eq!(pk.project_id.as_str(), "p/extra");
    }

    #[test]
    fn try_parse_project_key_invalid_returns_none() {
        assert!(try_parse_project_key("bad").is_none());
    }

    #[test]
    fn try_parse_project_key_empty_returns_none() {
        assert!(try_parse_project_key("").is_none());
    }

    #[test]
    fn try_parse_project_key_empty_parts_returns_none() {
        assert!(try_parse_project_key("t//p").is_none());
        assert!(try_parse_project_key("/w/p").is_none());
    }

    // ── ferriskey-Value parser tests ────────────────────────────────
    //
    // Everything in `valkey_value_tests` exercises helpers that take
    // `&ferriskey::Value` — only meaningful when the `fabric-valkey`
    // feature is on. Under `--no-default-features` the helpers
    // themselves compile out, so the tests compile out too.
}

#[cfg(all(test, feature = "fabric-valkey"))]
mod valkey_value_tests {
    use super::*;

    #[test]
    fn is_duplicate_detects_duplicate_simple_string() {
        let raw = ferriskey::Value::Array(vec![
            Ok(ferriskey::Value::Int(1)),
            Ok(ferriskey::Value::SimpleString("DUPLICATE".to_owned())),
        ]);
        assert!(is_duplicate_result(&raw));
    }

    #[test]
    fn is_duplicate_detects_duplicate_bulk_string() {
        let raw = ferriskey::Value::Array(vec![
            Ok(ferriskey::Value::Int(1)),
            Ok(ferriskey::Value::BulkString(b"DUPLICATE".to_vec().into())),
        ]);
        assert!(is_duplicate_result(&raw));
    }

    #[test]
    fn is_duplicate_returns_false_for_ok() {
        let raw = ferriskey::Value::Array(vec![
            Ok(ferriskey::Value::Int(1)),
            Ok(ferriskey::Value::SimpleString("OK".to_owned())),
        ]);
        assert!(!is_duplicate_result(&raw));
    }

    #[test]
    fn is_duplicate_returns_false_for_non_array() {
        let raw = ferriskey::Value::SimpleString("not an array".to_owned());
        assert!(!is_duplicate_result(&raw));
    }

    #[test]
    fn is_duplicate_returns_false_for_empty_array() {
        let raw = ferriskey::Value::Array(vec![]);
        assert!(!is_duplicate_result(&raw));
    }

    #[test]
    fn check_fcall_success_ok() {
        let raw = ferriskey::Value::Array(vec![
            Ok(ferriskey::Value::Int(1)),
            Ok(ferriskey::Value::SimpleString("OK".to_owned())),
        ]);
        assert!(check_fcall_success(&raw, "test").is_ok());
    }

    #[test]
    fn check_fcall_success_error_returns_err() {
        let raw = ferriskey::Value::Array(vec![
            Ok(ferriskey::Value::Int(0)),
            Ok(ferriskey::Value::SimpleString("lease_expired".to_owned())),
        ]);
        let err = check_fcall_success(&raw, "ff_complete_execution");
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("lease_expired"));
    }

    #[test]
    fn check_fcall_success_error_bulk_string() {
        let raw = ferriskey::Value::Array(vec![
            Ok(ferriskey::Value::Int(0)),
            Ok(ferriskey::Value::BulkString(b"stale_lease".to_vec().into())),
        ]);
        let err = check_fcall_success(&raw, "ff_cancel");
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("stale_lease"));
    }

    #[test]
    fn check_fcall_success_non_array_passes() {
        let raw = ferriskey::Value::SimpleString("OK".to_owned());
        assert!(check_fcall_success(&raw, "test").is_ok());
    }

    #[test]
    fn check_fcall_success_duplicate_is_ok() {
        let raw = ferriskey::Value::Array(vec![
            Ok(ferriskey::Value::Int(1)),
            Ok(ferriskey::Value::SimpleString("DUPLICATE".to_owned())),
        ]);
        assert!(check_fcall_success(&raw, "test").is_ok());
    }

    #[test]
    fn parse_fail_outcome_retry_scheduled_simple_string() {
        let raw = ferriskey::Value::Array(vec![
            Ok(ferriskey::Value::Int(1)),
            Ok(ferriskey::Value::SimpleString("OK".to_owned())),
            Ok(ferriskey::Value::SimpleString("retry_scheduled".to_owned())),
            Ok(ferriskey::Value::SimpleString("1234567890".to_owned())),
        ]);
        assert_eq!(parse_fail_outcome(&raw), FailOutcome::RetryScheduled);
    }

    #[test]
    fn parse_fail_outcome_retry_scheduled_bulk_string() {
        let raw = ferriskey::Value::Array(vec![
            Ok(ferriskey::Value::Int(1)),
            Ok(ferriskey::Value::SimpleString("OK".to_owned())),
            Ok(ferriskey::Value::BulkString(
                b"retry_scheduled".to_vec().into(),
            )),
        ]);
        assert_eq!(parse_fail_outcome(&raw), FailOutcome::RetryScheduled);
    }

    #[test]
    fn parse_fail_outcome_terminal_failed() {
        let raw = ferriskey::Value::Array(vec![
            Ok(ferriskey::Value::Int(1)),
            Ok(ferriskey::Value::SimpleString("OK".to_owned())),
            Ok(ferriskey::Value::SimpleString("terminal_failed".to_owned())),
        ]);
        assert_eq!(parse_fail_outcome(&raw), FailOutcome::TerminalFailed);
    }

    #[test]
    fn parse_fail_outcome_non_array_defaults_terminal() {
        let raw = ferriskey::Value::SimpleString("OK".to_owned());
        assert_eq!(parse_fail_outcome(&raw), FailOutcome::TerminalFailed);
    }

    #[test]
    fn parse_fail_outcome_short_array_defaults_terminal() {
        let raw = ferriskey::Value::Array(vec![
            Ok(ferriskey::Value::Int(1)),
            Ok(ferriskey::Value::SimpleString("OK".to_owned())),
        ]);
        assert_eq!(parse_fail_outcome(&raw), FailOutcome::TerminalFailed);
    }

    #[test]
    fn check_fcall_success_already_satisfied_is_ok() {
        let raw = ferriskey::Value::Array(vec![
            Ok(ferriskey::Value::Int(1)),
            Ok(ferriskey::Value::SimpleString(
                "ALREADY_SATISFIED".to_owned(),
            )),
        ]);
        assert!(check_fcall_success(&raw, "test").is_ok());
    }

    #[test]
    fn is_already_satisfied_simple_string() {
        let raw = ferriskey::Value::Array(vec![
            Ok(ferriskey::Value::Int(1)),
            Ok(ferriskey::Value::SimpleString(
                "ALREADY_SATISFIED".to_owned(),
            )),
        ]);
        assert!(is_already_satisfied(&raw));
    }

    #[test]
    fn is_already_satisfied_bulk_string() {
        let raw = ferriskey::Value::Array(vec![
            Ok(ferriskey::Value::Int(1)),
            Ok(ferriskey::Value::BulkString(
                b"ALREADY_SATISFIED".to_vec().into(),
            )),
        ]);
        assert!(is_already_satisfied(&raw));
    }

    #[test]
    fn is_already_satisfied_false_for_ok() {
        let raw = ferriskey::Value::Array(vec![
            Ok(ferriskey::Value::Int(1)),
            Ok(ferriskey::Value::SimpleString("OK".to_owned())),
        ]);
        assert!(!is_already_satisfied(&raw));
    }

    #[test]
    fn is_already_satisfied_false_for_non_array() {
        let raw = ferriskey::Value::SimpleString("OK".to_owned());
        assert!(!is_already_satisfied(&raw));
    }

    // ── #500 regression: fcall_error_code_ref returns borrowed on SimpleString ──
    //
    // The zero-alloc variant must return `Cow::Borrowed` when the error
    // slot is a `SimpleString` (FF's normal encoding for short typed
    // codes: `lease_expired`, `stale_lease`, `waitpoint_closed`, …) so
    // that callers which only dispatch on typed codes pay zero
    // allocation on the rejected path.

    #[test]
    fn fcall_error_code_ref_returns_borrowed_for_simple_string() {
        let raw = ferriskey::Value::Array(vec![
            Ok(ferriskey::Value::Int(0)),
            Ok(ferriskey::Value::SimpleString("lease_expired".to_owned())),
        ]);
        let code = fcall_error_code_ref(&raw).expect("typed code present");
        assert_eq!(code, "lease_expired");
        assert!(
            matches!(code, Cow::Borrowed(_)),
            "SimpleString must stay borrowed (no alloc) on the rejected path"
        );
    }

    #[test]
    fn fcall_error_code_ref_returns_cow_for_bulk_string() {
        let raw = ferriskey::Value::Array(vec![
            Ok(ferriskey::Value::Int(0)),
            Ok(ferriskey::Value::BulkString(b"stale_lease".to_vec().into())),
        ]);
        let code = fcall_error_code_ref(&raw).expect("typed code present");
        assert_eq!(code, "stale_lease");
        // For valid-UTF-8 BulkString, `from_utf8_lossy` returns Borrowed.
        // This is FF's invariant (all typed codes are ASCII), so assert
        // it — a refactor that loses this zero-copy would silently
        // regress.
        assert!(matches!(code, Cow::Borrowed(_)));
    }

    #[test]
    fn fcall_error_code_ref_returns_none_on_success() {
        let raw = ferriskey::Value::Array(vec![
            Ok(ferriskey::Value::Int(1)),
            Ok(ferriskey::Value::SimpleString("OK".to_owned())),
        ]);
        assert!(fcall_error_code_ref(&raw).is_none());
    }

    #[test]
    fn fcall_error_code_ref_returns_none_on_non_array() {
        let raw = ferriskey::Value::SimpleString("OK".to_owned());
        assert!(fcall_error_code_ref(&raw).is_none());
    }

    #[test]
    fn fcall_error_code_owned_shim_still_works() {
        // Back-compat: the owned-String wrapper delegates to
        // fcall_error_code_ref and materialises. Callers that still
        // expect String keep working.
        let raw = ferriskey::Value::Array(vec![
            Ok(ferriskey::Value::Int(0)),
            Ok(ferriskey::Value::SimpleString(
                "use_claim_resumed_execution".to_owned(),
            )),
        ]);
        assert_eq!(
            fcall_error_code(&raw).as_deref(),
            Some("use_claim_resumed_execution"),
        );
    }

    // ── read_hgetall_field (#509) ───────────────────────────────────────

    #[test]
    fn read_hgetall_field_returns_borrowed_populated_value() {
        let mut fields = std::collections::HashMap::new();
        fields.insert("state".to_owned(), "active".to_owned());
        let got = read_hgetall_field(&fields, "state");
        assert_eq!(got, Some("active"));
        // The returned &str must borrow from `fields`, not be a clone.
        let expected_ptr = fields.get("state").unwrap().as_ptr();
        assert_eq!(
            got.unwrap().as_ptr(),
            expected_ptr,
            "must borrow, not clone"
        );
    }

    #[test]
    fn read_hgetall_field_filters_empty_value() {
        let mut fields = std::collections::HashMap::new();
        fields.insert("state".to_owned(), String::new());
        assert_eq!(read_hgetall_field(&fields, "state"), None);
    }

    #[test]
    fn read_hgetall_field_missing_key_returns_none() {
        let fields = std::collections::HashMap::<String, String>::new();
        assert_eq!(read_hgetall_field(&fields, "state"), None);
    }

    // ── parse_string_array_borrowed (#514) ──────────────────────────────

    #[test]
    fn parse_string_array_borrowed_yields_borrowed_on_valid_utf8() {
        use std::borrow::Cow;
        let raw = ferriskey::Value::Array(vec![
            Ok(ferriskey::Value::BulkString(b"alpha".to_vec().into())),
            Ok(ferriskey::Value::BulkString(b"beta".to_vec().into())),
            Ok(ferriskey::Value::SimpleString("gamma".into())),
        ]);
        let items: Vec<Cow<'_, str>> = parse_string_array_borrowed(&raw).collect();
        assert_eq!(items.len(), 3);
        for item in &items {
            assert!(
                matches!(item, Cow::Borrowed(_)),
                "valid-UTF-8 entries must be borrowed, got Cow::Owned for {item:?}",
            );
        }
        assert_eq!(items[0].as_ref(), "alpha");
        assert_eq!(items[1].as_ref(), "beta");
        assert_eq!(items[2].as_ref(), "gamma");
    }

    #[test]
    fn parse_string_array_borrowed_falls_back_to_owned_on_invalid_utf8() {
        use std::borrow::Cow;
        // Invalid UTF-8 (lone continuation byte): must round-trip via
        // from_utf8_lossy into an owned Cow — same semantics as the
        // legacy owning variant, so behaviour is identical on the
        // garbled-bytes path.
        let raw = ferriskey::Value::Array(vec![
            Ok(ferriskey::Value::BulkString(b"ok".to_vec().into())),
            Ok(ferriskey::Value::BulkString(vec![0xFFu8].into())),
        ]);
        let items: Vec<Cow<'_, str>> = parse_string_array_borrowed(&raw).collect();
        assert_eq!(items.len(), 2);
        assert!(matches!(items[0], Cow::Borrowed(_)));
        assert!(matches!(items[1], Cow::Owned(_)));
    }

    #[test]
    fn parse_string_array_borrowed_equivalent_to_owning_variant() {
        // Parity assertion: the two entry points must agree on the
        // ordered string contents for the typical bounded case.
        let raw = ferriskey::Value::Array(vec![
            Ok(ferriskey::Value::BulkString(b"a".to_vec().into())),
            Ok(ferriskey::Value::SimpleString("b".into())),
            Ok(ferriskey::Value::BulkString(b"c".to_vec().into())),
        ]);
        let borrowed: Vec<String> = parse_string_array_borrowed(&raw)
            .map(|c| c.into_owned())
            .collect();
        let owning: Vec<String> = parse_string_array(&raw);
        assert_eq!(borrowed, owning);
    }

    #[test]
    fn parse_string_array_borrowed_supports_set_shape() {
        use std::borrow::Cow;
        let raw = ferriskey::Value::Set(vec![
            ferriskey::Value::BulkString(b"x".to_vec().into()),
            ferriskey::Value::BulkString(b"y".to_vec().into()),
        ]);
        let items: Vec<Cow<'_, str>> = parse_string_array_borrowed(&raw).collect();
        assert_eq!(items.len(), 2);
        for item in &items {
            assert!(matches!(item, Cow::Borrowed(_)));
        }
    }

    #[test]
    fn parse_string_array_borrowed_empty_on_other_shapes() {
        let raw = ferriskey::Value::Int(42);
        let items: Vec<_> = parse_string_array_borrowed(&raw).collect();
        assert!(items.is_empty());
    }
}
