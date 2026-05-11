//! Idempotency-Key cache for expensive POST endpoints (#433).
//!
//! Closes audit finding #433 (low-severity, medium-effort). **Wired on
//! `POST /v1/runs/:id/orchestrate` only** in this PR — orchestrate is
//! the single most expensive endpoint (LLM tokens, provider cost,
//! breaker-state writes) and the one the audit explicitly targets.
//! `POST /v1/runs` and `POST /v1/tool-invocations` are future follow-up
//! wiring: both use a custom `ProjectJson<T>` extractor that consumes
//! the body before a middleware can hash it, so adding them requires
//! threading raw bytes through that extractor — out of scope for this
//! PR. The `IdempotencyEndpoint` enum is intentionally narrow to what
//! is wired today; see PR #555 review thread for the follow-up issue.
//!
//! An in-process cache keyed by (tenant_id, endpoint, Idempotency-Key)
//! records the first response (status + body bytes) and replays it
//! on key match.
//!
//! ### Semantics (loosely follows Stripe's Idempotency-Key contract)
//!
//! - Header name: `Idempotency-Key` (case-insensitive per HTTP).
//! - Scope: per (tenant, endpoint) — the same key on a different route
//!   or a different tenant is a fresh request.
//! - TTL: 5 minutes. Long enough to cover a 30-second gateway timeout +
//!   retry budget, short enough to bound memory.
//! - Body-hash guard: the first request's body hash is stored alongside
//!   the cached response. A retry with the SAME key but a DIFFERENT body
//!   returns **409 Conflict** rather than silently replaying the first
//!   response — clients re-using a key across distinct requests is a
//!   programming error worth surfacing (again, per Stripe's model).
//! - Concurrent submits: a `Mutex<HashMap>` is sufficient — orchestrate
//!   is not a hot-path (dozens per minute, not thousands); the lock is
//!   only held while inserting a `Pending` placeholder. While the first
//!   request is in-flight, concurrent retries see `Pending` and get a
//!   409 Conflict-style `idempotency_in_progress` 409 instead of racing
//!   both to completion.
//! - Eviction: amortized — on each insert, if the map size exceeds
//!   `MAX_ENTRIES`, expired entries are swept out in one O(N) pass.
//!
//! ### Out of scope (deferred)
//!
//! - Cross-process coordination. Single-node only; in multi-node team
//!   mode, requests routed to different nodes can still duplicate. A
//!   future FF-backed shared cache would close this gap — out of scope
//!   for the API-polish fix.
//! - Durability across restarts. The cache is in-process; a restart
//!   resets the cache. Acceptable because idempotency is a 5-minute
//!   window and a crash-induced restart effectively invalidates all
//!   in-flight retries anyway.

// Clippy's `result_large_err` fires on helpers that return `Result<_,
// axum::http::Response<Body>>` — but the axum idiom IS to surface a
// ready-to-send response on the error arm, and every other handler in
// cairn-app follows that same pattern. Boxing here would regress
// ergonomics at the only call site that matters. Allow at module
// scope to match the convention documented at the function bodies.
#![allow(clippy::result_large_err)]

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::errors::AppApiError;

/// Header clients use to mark a retry-safe POST.
pub const IDEMPOTENCY_HEADER: &str = "idempotency-key";

/// Cache entry TTL. Covers typical gateway-timeout retry windows
/// (30-60s) with generous margin, bounded so stale entries are reaped.
const TTL: Duration = Duration::from_secs(300);

/// Upper bound on cached entries. At ~2KB per cached response this is
/// ~10MB resident — comfortably under the cairn-app process budget.
const MAX_ENTRIES: usize = 5_000;

/// Endpoints that honor Idempotency-Key. Kept as a typed enum (rather
/// than a free-form string) so a typo in a handler doesn't silently
/// open a new cache namespace. Currently single-variant — `CreateRun`
/// and `CreateToolInvocation` are future wiring (see module doc).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum IdempotencyEndpoint {
    /// `POST /v1/runs/:id/orchestrate` — kick off the loop.
    Orchestrate,
}

impl IdempotencyEndpoint {
    fn label(self) -> &'static str {
        match self {
            Self::Orchestrate => "POST /v1/runs/:id/orchestrate",
        }
    }
}

#[derive(Clone)]
enum Slot {
    /// First request is in flight; retries see this and get 409.
    Pending { body_hash: u64, inserted: Instant },
    /// Response materialised; retries with matching body hash replay.
    Ready {
        body_hash: u64,
        status: StatusCode,
        body: Bytes,
        inserted: Instant,
    },
}

impl Slot {
    fn inserted(&self) -> Instant {
        match self {
            Self::Pending { inserted, .. } | Self::Ready { inserted, .. } => *inserted,
        }
    }

    fn body_hash(&self) -> u64 {
        match self {
            Self::Pending { body_hash, .. } | Self::Ready { body_hash, .. } => *body_hash,
        }
    }
}

/// Composite cache key: (tenant, endpoint, client-supplied key). Keeping
/// tenant in the key prevents one tenant's key from matching another's.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Key {
    tenant: String,
    endpoint: IdempotencyEndpoint,
    key: String,
}

/// Thread-safe idempotency cache. Held in `AppState` as
/// `Arc<IdempotencyCache>`.
pub struct IdempotencyCache {
    inner: Mutex<HashMap<Key, Slot>>,
    /// Monotonic "last eviction sweep" timestamp. Drives the
    /// amortised retain pass in `maybe_evict`.
    last_evict: Mutex<Instant>,
}

impl Default for IdempotencyCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Minimum interval between the O(N) eviction sweeps on `try_claim`.
/// Gemini review flagged the prior unconditional retain-when-full as
/// degenerate under sustained load: once the cap filled with
/// non-expired entries every subsequent request paid an O(N) scan AND
/// the cap was not actually enforced.
///
/// New behaviour: sweep at most once every `EVICT_INTERVAL`, and if
/// the sweep still leaves the map at the cap, drop the
/// oldest-inserted entry to force forward progress. 30s matches TTL/10
/// — tight enough that a burst of expiring keys clears within a few
/// sweeps, loose enough that the amortised cost is negligible.
const EVICT_INTERVAL: Duration = Duration::from_secs(30);

impl IdempotencyCache {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            last_evict: Mutex::new(Instant::now()),
        }
    }

    /// Amortised eviction + hard-cap enforcement. Runs at most once
    /// per `EVICT_INTERVAL`. Guarantees the map never exceeds
    /// `MAX_ENTRIES` after the call returns — if the sweep alone
    /// didn't free space (every entry is still fresh), the single
    /// oldest-inserted entry is dropped. Holding-the-lock cost:
    /// O(N) worst case, but only during the sweep, not on every call.
    fn maybe_evict(&self, guard: &mut std::sync::MutexGuard<'_, HashMap<Key, Slot>>) {
        if guard.len() < MAX_ENTRIES {
            return;
        }
        let mut last = self.last_evict.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        if now.duration_since(*last) < EVICT_INTERVAL && guard.len() == MAX_ENTRIES {
            // Skip the O(N) sweep — it ran recently. We still enforce
            // the hard cap below by dropping one entry.
        } else {
            guard.retain(|_, slot| now.duration_since(slot.inserted()) < TTL);
            *last = now;
        }
        // Hard cap: even after a sweep, if we're still at the cap
        // (all entries are fresh), drop the oldest-inserted entry so
        // the incoming request has a slot. This is strictly better
        // than returning an error — idempotency is a best-effort
        // cache, and losing the oldest entry just means its original
        // retry window ended early (5-min TTL is already the
        // advertised guarantee floor).
        while guard.len() >= MAX_ENTRIES {
            if let Some(oldest_key) = guard
                .iter()
                .min_by_key(|(_, slot)| slot.inserted())
                .map(|(k, _)| k.clone())
            {
                guard.remove(&oldest_key);
            } else {
                break;
            }
        }
    }

    /// Outcome of attempting to claim a slot for a (tenant, endpoint,
    /// key) triple.
    fn try_claim(&self, key: Key, body_hash: u64) -> ClaimOutcome {
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        self.maybe_evict(&mut guard);

        let now = Instant::now();
        match guard.get(&key) {
            Some(slot) if now.duration_since(slot.inserted()) >= TTL => {
                // Expired — drop and re-claim.
                guard.remove(&key);
                guard.insert(
                    key,
                    Slot::Pending {
                        body_hash,
                        inserted: now,
                    },
                );
                ClaimOutcome::Claimed
            }
            Some(Slot::Pending {
                body_hash: existing,
                ..
            }) => {
                if *existing == body_hash {
                    ClaimOutcome::InProgress
                } else {
                    ClaimOutcome::KeyReuseConflict
                }
            }
            Some(Slot::Ready {
                body_hash: existing,
                status,
                body,
                ..
            }) => {
                if *existing == body_hash {
                    ClaimOutcome::Replay {
                        status: *status,
                        body: body.clone(),
                    }
                } else {
                    ClaimOutcome::KeyReuseConflict
                }
            }
            None => {
                guard.insert(
                    key,
                    Slot::Pending {
                        body_hash,
                        inserted: now,
                    },
                );
                ClaimOutcome::Claimed
            }
        }
    }

    /// Record the response for a claimed slot so future retries replay
    /// it. No-op if the slot was evicted in the meantime (rare, but
    /// correct under eviction pressure — the caller already committed
    /// the side-effect, and a future retry will just re-run).
    fn publish(&self, key: Key, body_hash: u64, status: StatusCode, body: Bytes) {
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(slot) = guard.get_mut(&key) {
            // Only publish if we're still the Pending owner with the
            // same body hash. A mismatch means some other flow raced
            // past us (shouldn't happen because try_claim blocks, but
            // be defensive).
            if slot.body_hash() == body_hash {
                *slot = Slot::Ready {
                    body_hash,
                    status,
                    body,
                    inserted: Instant::now(),
                };
            }
        }
    }

    /// Drop a claim on a failure path so a caller can retry immediately
    /// without waiting for TTL. Called from the handler's error arms.
    fn release_claim(&self, key: &Key, body_hash: u64) {
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(Slot::Pending {
            body_hash: existing,
            ..
        }) = guard.get(key)
        {
            if *existing == body_hash {
                guard.remove(key);
            }
        }
    }
}

enum ClaimOutcome {
    /// This request wins the slot; handler should run and then call
    /// `publish` with the response.
    Claimed,
    /// A concurrent request with the same body is in flight.
    InProgress,
    /// A prior request materialised; replay its response.
    Replay { status: StatusCode, body: Bytes },
    /// Same key, different body — a programming error on the client.
    KeyReuseConflict,
}

/// Parsed header, or a 400 response if the caller sent a malformed key.
///
/// `None` (no header) is NOT a 400 — Idempotency-Key is optional.
pub(crate) fn extract_key(headers: &HeaderMap) -> Result<Option<String>, Response> {
    let Some(raw) = headers.get(IDEMPOTENCY_HEADER) else {
        return Ok(None);
    };
    let value = match raw.to_str() {
        Ok(v) => v.trim(),
        Err(_) => {
            return Err(AppApiError::new(
                StatusCode::BAD_REQUEST,
                "invalid_idempotency_key",
                "Idempotency-Key header must be ASCII",
            )
            .into_response());
        }
    };
    if value.is_empty() {
        return Err(AppApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_idempotency_key",
            "Idempotency-Key header must not be empty",
        )
        .into_response());
    }
    // Reasonable upper bound; prevents unbounded cache keys.
    if value.len() > 255 {
        return Err(AppApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_idempotency_key",
            "Idempotency-Key header must be 255 chars or fewer",
        )
        .into_response());
    }
    Ok(Some(value.to_owned()))
}

/// Hash request body bytes for the body-reuse guard. Using
/// DefaultHasher is fine — we only need collision resistance at the
/// per-tenant/per-endpoint level, not cryptographic guarantees.
pub(crate) fn hash_body(bytes: &[u8]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut h);
    h.finish()
}

/// Guard that a handler holds while running a claimed request. On drop
/// without `.publish()`, the claim is released so a retry can proceed.
pub(crate) struct IdempotencyGuard<'a> {
    cache: &'a IdempotencyCache,
    key: Key,
    body_hash: u64,
    published: bool,
}

impl<'a> IdempotencyGuard<'a> {
    pub(crate) fn publish(mut self, status: StatusCode, body: Bytes) {
        self.cache
            .publish(self.key.clone(), self.body_hash, status, body);
        self.published = true;
    }
}

impl Drop for IdempotencyGuard<'_> {
    fn drop(&mut self) {
        if !self.published {
            self.cache.release_claim(&self.key, self.body_hash);
        }
    }
}

/// Entry-point used by handlers. Returns either:
/// - `Ok(Some(guard))` — handler should run, then call `guard.publish()`
///   with the final response bytes. If the handler returns without
///   publishing (panic, early return), the guard's Drop releases the
///   claim so retries don't deadlock for 5 minutes.
/// - `Ok(None)` — no Idempotency-Key header; handler runs without
///   caching (original behaviour, preserved for backwards compat).
/// - `Err(response)` — either a 400 (malformed header), 409 (body-reuse
///   conflict), 409 (concurrent submit in progress), or 200/201 replay
///   of a prior response. Handler must short-circuit and return this.
pub(crate) fn claim<'a>(
    cache: &'a IdempotencyCache,
    headers: &HeaderMap,
    body_bytes: &[u8],
    tenant_id: &str,
    endpoint: IdempotencyEndpoint,
) -> Result<Option<IdempotencyGuard<'a>>, Response> {
    let Some(header_key) = extract_key(headers)? else {
        return Ok(None);
    };
    let key = Key {
        tenant: tenant_id.to_owned(),
        endpoint,
        key: header_key,
    };
    let body_hash = hash_body(body_bytes);
    match cache.try_claim(key.clone(), body_hash) {
        ClaimOutcome::Claimed => Ok(Some(IdempotencyGuard {
            cache,
            key,
            body_hash,
            published: false,
        })),
        ClaimOutcome::Replay { status, body } => {
            // Replay the prior response verbatim. Content-type is
            // always application/json for these endpoints.
            let mut resp = Response::new(axum::body::Body::from(body));
            *resp.status_mut() = status;
            resp.headers_mut().insert(
                axum::http::header::CONTENT_TYPE,
                axum::http::HeaderValue::from_static("application/json"),
            );
            resp.headers_mut().insert(
                "idempotent-replayed",
                axum::http::HeaderValue::from_static("true"),
            );
            Err(resp)
        }
        // Copilot review: go through `AppApiError` so the canonical
        // `{status_code, code, message, request_id}` envelope is used
        // consistently with every other error site in cairn-app. Prior
        // hand-rolled JSON bodies reintroduced exactly the drift
        // `errors.rs` was written to prevent.
        ClaimOutcome::InProgress => Err(AppApiError::new(
            StatusCode::CONFLICT,
            "idempotency_in_progress",
            format!(
                "A request with this Idempotency-Key is still being processed by {}",
                endpoint.label()
            ),
        )
        .into_response()),
        ClaimOutcome::KeyReuseConflict => Err(AppApiError::new(
            StatusCode::CONFLICT,
            "idempotency_key_reuse",
            format!(
                "Idempotency-Key previously used with a different request body on {}. \
                 Generate a new key per distinct request.",
                endpoint.label()
            ),
        )
        .into_response()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn make_headers(key: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(IDEMPOTENCY_HEADER, HeaderValue::from_str(key).unwrap());
        h
    }

    #[test]
    fn extract_missing_header_is_none() {
        let h = HeaderMap::new();
        assert!(matches!(extract_key(&h), Ok(None)));
    }

    #[test]
    fn extract_valid_key_returns_it() {
        let h = make_headers("abc-123");
        assert_eq!(extract_key(&h).unwrap(), Some("abc-123".to_owned()));
    }

    #[test]
    fn extract_empty_key_is_400() {
        let h = make_headers("   ");
        assert!(extract_key(&h).is_err());
    }

    #[test]
    fn extract_overlong_key_is_400() {
        let long = "x".repeat(256);
        let h = make_headers(&long);
        assert!(extract_key(&h).is_err());
    }

    #[test]
    fn first_claim_succeeds_second_replays() {
        let cache = IdempotencyCache::new();
        let headers = make_headers("k1");
        let body = b"{\"foo\":1}";

        // First claim: Ok(Some(guard)).
        let guard = claim(
            &cache,
            &headers,
            body,
            "tenant-a",
            IdempotencyEndpoint::Orchestrate,
        )
        .unwrap()
        .unwrap();
        guard.publish(StatusCode::ACCEPTED, Bytes::from_static(b"{\"ok\":true}"));

        // Second claim with same body: Err(replay).
        let result = claim(
            &cache,
            &headers,
            body,
            "tenant-a",
            IdempotencyEndpoint::Orchestrate,
        );
        assert!(result.is_err(), "expected replay response");
    }

    #[test]
    fn same_key_different_body_is_conflict() {
        let cache = IdempotencyCache::new();
        let headers = make_headers("k2");

        let guard = claim(
            &cache,
            &headers,
            b"{\"foo\":1}",
            "tenant-a",
            IdempotencyEndpoint::Orchestrate,
        )
        .unwrap()
        .unwrap();
        guard.publish(StatusCode::ACCEPTED, Bytes::from_static(b"{}"));

        let result = claim(
            &cache,
            &headers,
            b"{\"foo\":2}", // different body
            "tenant-a",
            IdempotencyEndpoint::Orchestrate,
        );
        assert!(result.is_err(), "body-reuse must 409");
    }

    #[test]
    fn different_tenants_get_separate_slots() {
        let cache = IdempotencyCache::new();
        let headers = make_headers("shared-key");
        let body = b"{}";

        let g1 = claim(
            &cache,
            &headers,
            body,
            "tenant-a",
            IdempotencyEndpoint::Orchestrate,
        )
        .unwrap()
        .unwrap();
        g1.publish(StatusCode::ACCEPTED, Bytes::from_static(b"{}"));

        // Tenant B — same key, same body, but different tenant must be
        // treated as a fresh request.
        let g2 = claim(
            &cache,
            &headers,
            body,
            "tenant-b",
            IdempotencyEndpoint::Orchestrate,
        )
        .unwrap();
        assert!(g2.is_some(), "tenant B must get a fresh claim");
    }

    // `different_endpoints_get_separate_slots` removed along with the
    // `CreateRun` / `CreateToolInvocation` variants that were unwired
    // (PR #555 review, Cursor bugbot). Re-add when the follow-up PR
    // wires those endpoints.

    #[test]
    fn concurrent_pending_claim_is_409() {
        let cache = IdempotencyCache::new();
        let headers = make_headers("k3");
        let body = b"{}";

        let _g1 = claim(
            &cache,
            &headers,
            body,
            "tenant-a",
            IdempotencyEndpoint::Orchestrate,
        )
        .unwrap()
        .unwrap();
        // g1 held (not dropped, not published) — second claim sees Pending.

        let r2 = claim(
            &cache,
            &headers,
            body,
            "tenant-a",
            IdempotencyEndpoint::Orchestrate,
        );
        assert!(r2.is_err(), "concurrent same-body must 409");
    }

    #[test]
    fn dropped_guard_releases_claim() {
        let cache = IdempotencyCache::new();
        let headers = make_headers("k4");
        let body = b"{}";

        {
            let _g = claim(
                &cache,
                &headers,
                body,
                "tenant-a",
                IdempotencyEndpoint::Orchestrate,
            )
            .unwrap()
            .unwrap();
            // guard dropped without publish — should release the claim.
        }

        // Next claim with same key should succeed (not 409).
        let g2 = claim(
            &cache,
            &headers,
            body,
            "tenant-a",
            IdempotencyEndpoint::Orchestrate,
        )
        .unwrap();
        assert!(g2.is_some(), "dropped claim must free the slot");
    }

    #[test]
    fn no_header_is_no_caching() {
        let cache = IdempotencyCache::new();
        let headers = HeaderMap::new();
        let r = claim(
            &cache,
            &headers,
            b"{}",
            "tenant-a",
            IdempotencyEndpoint::Orchestrate,
        )
        .unwrap();
        assert!(r.is_none(), "no header must bypass the cache entirely");
    }
}
