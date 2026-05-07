//! HTTP middleware: authentication, rate limiting, observability.

use std::{
    net::SocketAddr,
    sync::{atomic::Ordering, Arc},
    time::Instant,
};

use axum::{
    body::{to_bytes, Body},
    extract::{ConnectInfo, MatchedPath, Request, State},
    http::{header, HeaderValue, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use cairn_api::auth::Authenticator;
use cairn_api::auth::{AuthPrincipal, ServiceTokenAuthenticator};
use cairn_domain::{
    OperatorId, ProjectKey, PromptReleaseId, TenantId, WorkspaceId, WorkspaceKey, WorkspaceRole,
};
use cairn_runtime::set_current_trace_id;
#[cfg(feature = "metrics-core")]
use cairn_runtime::TenantService;
use cairn_runtime::WorkspaceService;
use cairn_store::projections::{
    OperatorTenantRoleReadModel, PromptReleaseReadModel, WorkspaceMembershipReadModel,
};
use percent_encoding::percent_decode_str;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::errors::{
    forbidden_api_error, now_ms, runtime_error_response, store_error_response, AppApiError,
};
use crate::state::{AppState, RateLimitBucket};
use crate::tokens::RequestLogEntry;
use crate::CreateRunRequest;

// ── Auth middleware ─────────────────────────────────────────────────────────

pub(crate) async fn auth_middleware(
    State(state): State<Arc<AppState>>,
    mut request: Request,
    next: Next,
) -> Response {
    if auth_exempt_path_method(request.uri().path(), request.method()) {
        return next.run(request).await;
    }

    let Some(token) = bearer_token(&request) else {
        return unauthorized_response();
    };

    let authenticator = ServiceTokenAuthenticator::new(state.service_tokens.clone());
    let Ok(principal) = authenticator.authenticate(&token) else {
        return unauthorized_response();
    };

    if let Some(tenant) = principal.tenant() {
        request.extensions_mut().insert(tenant.tenant_id.clone());
    }

    if let Err(response) = attach_workspace_role(&state, &principal, &mut request).await {
        return response;
    }

    if let Err(response) = attach_tenant_role(&state, &principal, &mut request).await {
        return response;
    }

    request.extensions_mut().insert(principal);

    next.run(request).await
}

// ── Rate limiting ───────────────────────────────────────────────────────────

/// Per-token rate limit: 1 000 requests per 60-second window.
pub(crate) const RL_TOKEN_LIMIT: u32 = 1_000;
/// Per-IP rate limit (when no bearer token): 100 requests per 60-second window.
pub(crate) const RL_IP_LIMIT: u32 = 100;
/// Sliding-window duration in milliseconds.
pub(crate) const RL_WINDOW_MS: u64 = 60_000;

pub(crate) async fn rate_limit_middleware(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Response {
    let rate_limits = state.rate_limits.clone();
    // Skip health/readiness probes — these must never be rate-limited.
    if matches!(
        request.uri().path(),
        "/health" | "/ready" | "/metrics" | "/version"
    ) {
        return next.run(request).await;
    }

    // ── Loopback exemption (closes #649) ────────────────────────────────
    //
    // Requests from a *direct* loopback caller bypass the rate limiter.
    // "Direct" means two things must both hold:
    //   (a) the TCP peer IP is loopback (127.0.0.0/8 or ::1), AND
    //   (b) the request carries no forwarding headers — no
    //       `X-Forwarded-For` and no `Forwarded` (RFC 7239).
    //
    // Motivation: our own CI job runs cairn-app and Playwright on the
    // same GitHub Actions runner, so every test request originates
    // from 127.0.0.1 over a direct socket and shares the single
    // `dev-admin-token` bucket (1000 req/min). A 122-spec × 2-worker
    // suite trips that limit deterministically — the limiter is
    // correctly doing its job, but it is the wrong layer.
    //
    // Security — why (b) is mandatory (Gemini r1 high-severity catch):
    //   A naive check of "is the resolved client IP loopback?" that
    //   trusted `X-Forwarded-For` over the TCP peer would let any
    //   external attacker forge `X-Forwarded-For: 127.0.0.1` and
    //   walk straight past the limiter. Instead we ignore forwarding
    //   headers for exemption purposes: their *presence* is the
    //   signal that a proxy handled this request, at which point
    //   the limiter is exactly the layer that should be doing its
    //   job. An attacker cannot synthesize the absence of a header
    //   through a proxy they don't control.
    //
    // Security — why this does not widen attack surface:
    //   1. Production deployments always terminate a reverse proxy
    //      in front of cairn-app. The proxy sets `X-Forwarded-For`
    //      (and/or `Forwarded`) on every request, so condition (b)
    //      is always false for external traffic and the limiter
    //      continues to apply.
    //   2. The direct-peer path only matters for callers on the
    //      same host — in a proxied deployment that means
    //      colocated processes already inside the trust boundary.
    //   3. Auth is unchanged. A loopback caller without a valid
    //      `CAIRN_ADMIN_TOKEN` still gets rejected by
    //      `auth_middleware` upstream.
    //
    // Caveat, called out explicitly: a deployment that exposes
    // cairn-app on loopback of a shared (multi-tenant) host without
    // a reverse proxy would lose rate-limit protection on that path.
    // That is not a supported production topology — `docs/ops/`
    // mandates reverse-proxy fronting — but an operator on a shared
    // VM should know the limiter no longer gates their peers.
    if is_direct_loopback_request(&request) {
        return next.run(request).await;
    }

    let now = now_ms();

    // Derive rate-limit key + per-key limit.
    // Token-authenticated requests get a higher allowance (1 000/min).
    // Unauthenticated requests are keyed by IP (100/min).
    //
    // Closes #490: bearer tokens are hashed (SHA-256, hex) before they
    // land in the rate-limit map. The map only lives in memory, but a
    // heap dump / core file / swap page written to disk would otherwise
    // carry the raw bearer bytes for up to 2× `RL_WINDOW_MS` (2 min)
    // after the last matching request. Hashing is a defense-in-depth
    // change — SHA-256 is collision-resistant (finding a collision is
    // computationally infeasible at this cardinality), preserves
    // observable behaviour (same token → same bucket key), and carries
    // zero plaintext. IPs are already safe to use verbatim; only the
    // token arm hashes.
    let (key, limit) = if let Some(token) = bearer_token(&request) {
        (
            format!("tok:{}", hash_rate_limit_token(&token)),
            RL_TOKEN_LIMIT,
        )
    } else if let Some(ip) = request_rate_limit_key(&request) {
        (format!("ip:{ip}"), RL_IP_LIMIT)
    } else {
        // No identifiable key — let the request through.
        return next.run(request).await;
    };

    let (remaining, reset_secs, exceeded) = {
        let mut buckets = match rate_limits.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };

        // T6b-C6: amortized time-based eviction. We cache the last sweep
        // timestamp on the state and only sweep once per window, so an
        // attacker keeping the map at the threshold can't trigger a
        // full O(N) scan on every request. A HashSet of "rotate bearer
        // strings" still grows up to 10k entries in the worst case
        // before the minute-boundary sweep — bounded and recoverable.
        let last_sweep = state.rate_limit_last_sweep_ms.load(Ordering::Relaxed);
        if now.saturating_sub(last_sweep) >= RL_WINDOW_MS {
            let evict_before = now.saturating_sub(RL_WINDOW_MS * 2);
            buckets.retain(|_, b| b.window_started_ms >= evict_before);
            state.rate_limit_last_sweep_ms.store(now, Ordering::Relaxed);
        }

        let bucket = buckets.entry(key).or_insert(RateLimitBucket {
            count: 0,
            window_started_ms: now,
        });

        // Reset the window if it has elapsed.
        if now.saturating_sub(bucket.window_started_ms) >= RL_WINDOW_MS {
            *bucket = RateLimitBucket {
                count: 0,
                window_started_ms: now,
            };
        }

        let reset_secs = (RL_WINDOW_MS - now.saturating_sub(bucket.window_started_ms))
            .max(1)
            .div_ceil(1000);

        if bucket.count >= limit {
            (0u32, reset_secs, true)
        } else {
            bucket.count += 1;
            (limit.saturating_sub(bucket.count), reset_secs, false)
        }
    };

    if exceeded {
        let mut response = AppApiError::new(
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limited",
            "rate limit exceeded",
        )
        .into_response();
        let headers = response.headers_mut();
        if let Ok(v) = HeaderValue::from_str(&reset_secs.to_string()) {
            headers.insert(header::RETRY_AFTER, v);
        }
        headers.insert("x-ratelimit-limit", HeaderValue::from(limit));
        headers.insert("x-ratelimit-remaining", HeaderValue::from(0u32));
        headers.insert("x-ratelimit-reset", HeaderValue::from(reset_secs));
        return response;
    }

    let mut response = next.run(request).await;

    // Attach rate-limit headers to every successful response.
    let headers = response.headers_mut();
    headers.insert("x-ratelimit-limit", HeaderValue::from(limit));
    headers.insert("x-ratelimit-remaining", HeaderValue::from(remaining));
    headers.insert("x-ratelimit-reset", HeaderValue::from(reset_secs));

    response
}

// ── Request ID / tracing ────────────────────────────────────────────────────

/// RFC 011 extension types for tracing context in request extensions.
///
/// `RequestId.0` IS consumed — see [`observability_middleware`] further
/// down this file, which reads the id off the request extensions via
/// `.get::<RequestId>()` and threads it into the audit log record. The
/// `#[allow(dead_code)]` that previously lived on the inner field was
/// stale; removed (#485).
#[derive(Clone, Debug)]
pub(crate) struct RequestId(pub(crate) String);
#[derive(Clone, Debug)]
pub(crate) struct TraceId(String);

impl TraceId {
    pub(crate) fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

/// `SpanId.0` is a per-request short hex identifier surfaced on the
/// `x-span-id` response header and reserved for correlation inside
/// handler-level tracing spans. No handler reads it off the extensions
/// map today, but the value is already computed and inserted by the
/// request-id middleware — exposing `as_str` lets a future tracing
/// subscriber correlate without reworking the middleware (#485).
#[derive(Clone, Debug)]
pub(crate) struct SpanId(String);

impl SpanId {
    #[allow(dead_code)] // reserved for handler-level tracing spans; see struct docstring
    pub(crate) fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

pub(crate) async fn request_id_middleware(mut request: Request, next: Next) -> Response {
    // Accept an incoming X-Trace-Id or generate a new one (RFC 011).
    let trace_id = request
        .headers()
        .get("x-trace-id")
        .and_then(|v| v.to_str().ok())
        .map(String::from)
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    let request_id = Uuid::new_v4().to_string();
    // Span ID: first 8 hex chars of the request UUID (no extra dep needed).
    let span_id = request_id
        .chars()
        .filter(|c| c.is_ascii_hexdigit())
        .take(8)
        .collect::<String>();

    // Propagate trace context to extensions so handlers can read it.
    request.extensions_mut().insert(TraceId(trace_id.clone()));
    request.extensions_mut().insert(SpanId(span_id.clone()));
    request
        .extensions_mut()
        .insert(RequestId(request_id.clone()));

    // Set thread-local so make_envelope() attaches trace_id to events.
    set_current_trace_id(&trace_id);

    let mut response = next.run(request).await;

    // Clear after handler completes.
    set_current_trace_id("");

    if let Ok(value) = HeaderValue::from_str(&request_id) {
        response
            .headers_mut()
            .insert(header::HeaderName::from_static("x-request-id"), value);
    }
    if let Ok(value) = HeaderValue::from_str(&trace_id) {
        response
            .headers_mut()
            .insert(header::HeaderName::from_static("x-trace-id"), value);
    }
    if let Ok(value) = HeaderValue::from_str(&span_id) {
        response
            .headers_mut()
            .insert(header::HeaderName::from_static("x-span-id"), value);
    }
    response
}

// ── Readiness gate (RFC 020 §"Startup order") ───────────────────────────────

/// Conservative retry hint (seconds) returned in the 503 body while the
/// readiness graph is still flipping branches. The real startup budget is
/// typically under 10s on dev hardware; cluster orchestrators use this as
/// a floor for re-probe scheduling.
const READINESS_RETRY_AFTER_SECONDS: u64 = 10;

/// Paths that must respond even while the readiness graph is incomplete.
/// - `/health` and `/healthz` stay up immediately (liveness, k8s probes).
/// - `/health/ready` is the readiness probe itself; it reports 503 with
///   the progress body during startup. If we blocked it here, clients
///   couldn't observe progress.
/// - Static SPA assets + client-side routes are served so the React
///   LoginPage can render while cairn-app finishes recovering.
fn readiness_exempt(path: &str, method: &axum::http::Method) -> bool {
    let normalized = path.to_ascii_lowercase();
    let normalized = normalized.as_str();

    // Mirrors the documentation / infra endpoints in `auth_exempt_path_method`
    // that don't depend on event-sourced state. These stay reachable during
    // recovery so operators can inspect the API spec or onboarding template
    // catalog while waiting for readiness to flip.
    if matches!(
        normalized,
        "/health"
            | "/healthz"
            | "/health/ready"
            | "/ready"
            | "/metrics"
            | "/version"
            | "/openapi.json"
            | "/docs"
            | "/v1/docs"
            | "/v1/onboarding/templates"
    ) {
        return true;
    }

    // SPA static assets + index fallback — mirrors `auth_exempt_path_method`
    // so the login UI can render before the readiness graph completes.
    if method != axum::http::Method::GET {
        return false;
    }
    if matches!(
        normalized,
        "/" | "/index.html"
            | "/favicon.svg"
            | "/favicon.ico"
            | "/robots.txt"
            | "/.well-known/agent.json"
    ) || normalized.starts_with("/assets/")
    {
        return true;
    }
    // SPA client-side routes (anything not under `/v1/`) fall through to
    // the embedded frontend so React Router can navigate while cairn
    // recovers. The LoginPage itself just renders a form; it only calls
    // `/v1/*` after the user submits a token, at which point readiness
    // will have flipped.
    !normalized.starts_with("/v1/")
}

/// RFC 020 readiness gate. Returns 503 with a compact JSON body for any
/// non-exempt route while `state.readiness.is_ready()` is `false`.
///
/// Non-exempt means "anything that could touch event-sourced state" —
/// the startup graph must finish before a client can write events or
/// read projections that may still be warming.
pub(crate) async fn readiness_middleware(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Response {
    if state.readiness.is_ready() || readiness_exempt(request.uri().path(), request.method()) {
        return next.run(request).await;
    }

    let mut response = (
        StatusCode::SERVICE_UNAVAILABLE,
        axum::Json(serde_json::json!({
            "status": "recovering",
            "retry_after_seconds": READINESS_RETRY_AFTER_SECONDS,
        })),
    )
        .into_response();
    if let Ok(v) = HeaderValue::from_str(&READINESS_RETRY_AFTER_SECONDS.to_string()) {
        response.headers_mut().insert(header::RETRY_AFTER, v);
    }
    response
}

// ── Auth helpers ────────────────────────────────────────────────────────────

#[allow(dead_code)] // retained for test coverage; production uses the method variant
pub(crate) fn auth_exempt_path(path: &str) -> bool {
    auth_exempt_path_method(path, &axum::http::Method::GET)
}

/// Same as [`auth_exempt_path`] but aware of HTTP method — the SPA
/// fallback exemption only covers GET, so a POST to an unknown
/// non-/v1/ path still goes through auth and 404s.
pub(crate) fn auth_exempt_path_method(path: &str, method: &axum::http::Method) -> bool {
    // T6b-C1: strict allowlist. Pre-fix, `!path.starts_with("/v1/")` made
    // ANY non-/v1/ path public, regardless of method — so a POST to
    // /internal/* or /debug/* would bypass auth. Normalize case +
    // require method=GET for the SPA-fallback exemption.
    let normalized = path.to_ascii_lowercase();
    let normalized = normalized.as_str();

    // Public infra endpoints.
    // NOTE: `/v1/stream` is NOT on this list — the SSE handler MUST
    // validate its `?token=` query param through the same
    // ServiceTokenAuthenticator and filter events by tenant (T6b-C7).
    if matches!(
        normalized,
        "/health"
            | "/healthz"
            | "/ready"
            | "/metrics"
            | "/version"
            | "/v1/onboarding/templates"
            | "/openapi.json"
            | "/docs"
            | "/v1/docs"
    ) {
        return true;
    }
    // Integration webhook receivers verify their own HMAC — bearer auth
    // doesn't apply here. Each integration's handler MUST fail-closed
    // when the signature check fails.
    if normalized.starts_with("/v1/webhooks/") {
        return true;
    }

    // Embedded React UI assets + SPA client-side routes. The SPA has
    // its own LoginPage that collects the bearer token client-side
    // before making API calls. We only exempt GET — POST/PATCH/DELETE
    // to any unknown path must still go through auth.
    if method != axum::http::Method::GET {
        return false;
    }
    if matches!(
        normalized,
        "/" | "/index.html"
            | "/favicon.svg"
            | "/favicon.ico"
            | "/robots.txt"
            | "/.well-known/agent.json"
    ) || normalized.starts_with("/assets/")
    {
        return true;
    }
    // SPA client-side routes: any GET that is NOT under /v1/ (and
    // isn't in the explicit list above) falls through to
    // `serve_frontend`, which returns index.html so React Router can
    // handle navigation. This is the only remaining wildcard, and it
    // is method-gated to GET.
    !normalized.starts_with("/v1/")
}

/// T6b-C2: redact credential-looking query params before the query
/// string is stored in telemetry. Keys matched case-insensitively
/// against a denylist after percent-decoding (so `?%74oken=secret`
/// — a URL-encoded `token=` — is also caught).
pub(crate) fn scrub_credentials_in_query(query: &str) -> String {
    const REDACTED_KEYS: &[&str] = &["token", "api_key", "apikey", "password", "secret", "bearer"];

    // Parse each pair ONCE through the urlencoded decoder, compare the
    // decoded key against the denylist, and rebuild the scrubbed query
    // (urlencoded-decoded — the point is ops visibility, not byte-exact
    // round-trip).
    let pairs: Vec<(String, String)> = url::form_urlencoded::parse(query.as_bytes())
        .map(|(k, v)| {
            let k = k.into_owned();
            let v_scrubbed = if REDACTED_KEYS.iter().any(|r| r.eq_ignore_ascii_case(&k)) {
                "REDACTED".to_owned()
            } else {
                v.into_owned()
            };
            (k, v_scrubbed)
        })
        .collect();
    url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs(&pairs)
        .finish()
}

pub(crate) fn request_rate_limit_key(request: &Request) -> Option<String> {
    request
        .headers()
        .get("x-forwarded-for")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

/// Returns `true` iff the request originated from a direct loopback
/// socket and carries no reverse-proxy forwarding headers.
///
/// This is the exemption predicate for the rate limiter (closes #649).
/// Both conditions must hold:
///   1. The TCP peer (`ConnectInfo<SocketAddr>`) is loopback
///      (127.0.0.0/8 or ::1) per [`IpAddr::is_loopback`].
///   2. Neither `X-Forwarded-For` nor `Forwarded` (RFC 7239) is
///      present on the request.
///
/// Condition 2 is the security gate (Gemini r1 high-severity catch).
/// If we exempted on peer alone we'd be fine, but if we ever let
/// `X-Forwarded-For: 127.0.0.1` classify a request as loopback, any
/// external attacker who could reach a proxy that doesn't strip
/// inbound `X-Forwarded-For` would trivially bypass rate limiting.
/// Treating the *presence* of any forwarding header as "this came
/// through a proxy, apply the limiter" keeps the exemption confined
/// to the CI + local-dev topology we care about and removes the
/// header-trust surface entirely. An attacker cannot synthesise the
/// absence of a header through infrastructure they don't control.
fn is_direct_loopback_request(request: &Request) -> bool {
    // Any forwarding header — even an empty one, even a nonsense
    // value — disqualifies the request from the exemption. We do
    // not parse or validate the header; its mere presence is the
    // "this was proxied" signal.
    if request.headers().contains_key("x-forwarded-for")
        || request.headers().contains_key("forwarded")
    {
        return false;
    }

    request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .is_some_and(|ConnectInfo(addr)| addr.ip().is_loopback())
}

/// Returns a lowercase hex SHA-256 digest of `token`. Closes #490: the
/// rate-limit map is keyed by this digest so the raw bearer never
/// appears in the map (or in a heap dump / core file / swap page).
/// SHA-256 is collision-resistant — finding a collision is
/// computationally infeasible at this cardinality — and deterministic,
/// so the rate-limit window still keys on identity.
pub(crate) fn hash_rate_limit_token(token: &str) -> String {
    let digest = Sha256::digest(token.as_bytes());
    let mut out = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

pub(crate) fn bearer_token(request: &Request) -> Option<String> {
    // 1. Standard `Authorization: Bearer <token>` header.
    if let Some(header) = request.headers().get(axum::http::header::AUTHORIZATION) {
        if let Ok(value) = header.to_str() {
            if let Some(token) = value.strip_prefix("Bearer ") {
                let trimmed = token.trim();
                if !trimmed.is_empty() {
                    return Some(trimmed.to_owned());
                }
            }
        }
    }
    // 2. Query-param fallback: `?token=<token>` on GET requests only.
    //    The original motivation is SSE EventSource and WebSocket
    //    upgrade (both GET-only, both unable to set custom headers from
    //    browser code). The gate is intentionally `method == GET` and
    //    not a path allow-list — every mutation verb (POST/PUT/PATCH/
    //    DELETE) is already rejected, and a single-predicate rule is
    //    easier to audit than a growing path catalog. If ops telemetry
    //    ever shows non-SSE/WS GETs accepting query tokens in practice,
    //    a tighter path allow-list (e.g. only `/v1/stream`, `/v1/ws`,
    //    `/v1/streams/runtime`) is the obvious next tightening.
    //
    //    #491: pre-fix the fallback fired on ANY method — a
    //    `POST /v1/admin/rotate-token?token=<bearer>` succeeded with
    //    only the query param. That turned every channel that leaks a
    //    URL (browser history, Referer header, upstream proxy /
    //    load-balancer access log, CSRF via image-tag or form-submit)
    //    into a full-privilege mutation vector. Bearer-in-query is a
    //    fundamentally leakier channel than a header; GET-only closes
    //    the mutation surface while keeping SSE + WS functional.
    //
    //    T6b-H2: percent-decode properly (so `%2B` becomes `+`),
    //    actively reject duplicate `token` keys (query-param smuggling),
    //    and reject whitespace-only values.
    if request.method() != axum::http::Method::GET {
        return None;
    }
    if let Some(query) = request.uri().query() {
        let mut found: Option<String> = None;
        for (k, v) in url::form_urlencoded::parse(query.as_bytes()) {
            if k.as_ref() == "token" {
                if found.is_some() {
                    // Duplicate `?token=a&token=b` — refuse the request
                    // rather than first-wins or last-wins. Query-param
                    // smuggling where a proxy reorders duplicates is a
                    // real-world attack vector (Copilot review on PR #548).
                    return None;
                }
                let v = v.trim();
                if v.is_empty() {
                    return None;
                }
                found = Some(v.to_owned());
            }
        }
        return found;
    }
    None
}

pub(crate) fn principal_member_id(principal: &AuthPrincipal) -> Option<&str> {
    match principal {
        AuthPrincipal::Operator { operator_id, .. } => Some(operator_id.as_str()),
        AuthPrincipal::ServiceAccount { name, .. } => Some(name.as_str()),
        AuthPrincipal::System => None,
    }
}

// ── Workspace role inference ────────────────────────────────────────────────

pub(crate) async fn lookup_workspace_role(
    state: &AppState,
    principal: &AuthPrincipal,
    workspace_key: &WorkspaceKey,
) -> Result<Option<WorkspaceRole>, Response> {
    let Some(member_id) = principal_member_id(principal) else {
        return Ok(None);
    };

    WorkspaceMembershipReadModel::get_member(state.runtime.store.as_ref(), workspace_key, member_id)
        .await
        .map(|membership| membership.map(|membership| membership.role))
        .map_err(store_error_response)
}

/// Classify a failure returned by `axum::body::to_bytes` into the
/// matching HTTP error envelope.
///
/// - 413 `payload_too_large` when the failure is the length-limit
///   path (`http_body_util::LengthLimitError` surfaces as the wrapped
///   source). Keeps the error parallel to what the axum extractors
///   return for `JsonRejection::BytesRejection`.
/// - 400 `bad_request` for every other path (io failure, transport
///   disconnect, framing error).
///
/// `axum::Error` is opaque — it wraps the inner `http_body_util`
/// error as a source but we don't depend on `http-body-util`
/// directly. We classify via a stable substring on the `Debug` /
/// `Display` output of the wrapped chain; the 400 fallback means a
/// future wording change silently degrades to 400 rather than
/// mis-classifying. Test: `classify_body_read_error_*` below.
fn classify_body_read_error(err: &axum::Error) -> AppApiError {
    let is_length_limit = format!("{err:?}").contains("LengthLimitError")
        || err.to_string().contains("length limit exceeded");
    if is_length_limit {
        AppApiError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "payload_too_large",
            "request body exceeds the 10 MiB limit",
        )
    } else {
        AppApiError::new(
            StatusCode::BAD_REQUEST,
            "bad_request",
            "invalid request body",
        )
    }
}

pub(crate) async fn infer_workspace_role_for_request(
    state: &AppState,
    principal: &AuthPrincipal,
    request: &mut Request,
) -> Result<Option<WorkspaceRole>, Response> {
    let path = request.uri().path().to_owned();
    let method = request.method().clone();

    if method == axum::http::Method::POST && path == "/v1/runs" {
        let owned_request = std::mem::replace(request, Request::new(Body::empty()));
        let (parts, body) = owned_request.into_parts();
        // Honour the underlying rejection's native status — a body
        // that trips the 10 MiB length cap returns 413 Payload Too
        // Large, while a transport/io failure surfaces as 400 Bad
        // Request. Mapping everything to 422 previously masked "you
        // exceeded the cap" with "your body is unparseable". Copilot
        // review on #564.
        let bytes = to_bytes(body, 10 * 1024 * 1024)
            .await
            .map_err(|err| classify_body_read_error(&err).into_response())?;
        *request = Request::from_parts(parts, Body::from(bytes.clone()));

        let Ok(payload) = serde_json::from_slice::<CreateRunRequest>(&bytes) else {
            return Ok(None);
        };
        let workspace_key = WorkspaceKey::new(payload.tenant_id, payload.workspace_id);
        return lookup_workspace_role(state, principal, &workspace_key).await;
    }

    let segments: Vec<&str> = path.trim_matches('/').split('/').collect();
    if method == axum::http::Method::POST
        && segments.len() == 5
        && segments[0] == "v1"
        && segments[1] == "admin"
        && segments[2] == "workspaces"
        && segments[4] == "members"
    {
        let workspace = state
            .runtime
            .workspaces
            .get(&WorkspaceId::new(segments[3]))
            .await
            .map_err(runtime_error_response)?
            .ok_or_else(|| {
                AppApiError::new(StatusCode::NOT_FOUND, "not_found", "workspace not found")
                    .into_response()
            })?;
        let workspace_key = WorkspaceKey::new(workspace.tenant_id, workspace.workspace_id);
        return lookup_workspace_role(state, principal, &workspace_key).await;
    }

    if method == axum::http::Method::POST
        && segments.len() == 5
        && segments[0] == "v1"
        && segments[1] == "prompts"
        && segments[2] == "releases"
        && segments[4] == "activate"
    {
        let release = PromptReleaseReadModel::get(
            state.runtime.store.as_ref(),
            &PromptReleaseId::new(segments[3]),
        )
        .await
        .map_err(store_error_response)?;
        if let Some(release) = release {
            return lookup_workspace_role(state, principal, &release.project.workspace_key()).await;
        }
    }

    Ok(None)
}

pub(crate) async fn attach_workspace_role(
    state: &AppState,
    principal: &AuthPrincipal,
    request: &mut Request,
) -> Result<(), Response> {
    if let Some(role) = infer_workspace_role_for_request(state, principal, request).await? {
        request.extensions_mut().insert(role);
    }
    Ok(())
}

/// RFC 026 PR-A0: attach the operator's `TenantRole` on the target
/// tenant to `request.extensions` for any path the admin surface may
/// authorize against.
///
/// The target tenant id is extracted from two path shapes:
///
///   * `/v1/admin/tenants/:tenant_id/...` — every tenant-scoped admin
///     handler (workspaces, snapshots, credentials, operator-profiles,
///     ...).
///   * `/v1/admin/operators/:operator_id/tenant-roles/:tenant_id/...` —
///     the promote + revoke endpoints introduced by PR-A0.
///
/// When both shapes fail to match, no extension is attached — the
/// `TenantAdminGuard` extractor falls back to god-token / workspace-role
/// evaluation, preserving behaviour on non-admin paths. System and
/// admin-service-account principals never need a TenantRole extension
/// because `is_admin_principal` short-circuits the guard before the
/// lookup runs.
pub(crate) async fn attach_tenant_role(
    state: &AppState,
    principal: &AuthPrincipal,
    request: &mut Request,
) -> Result<(), Response> {
    let Some(operator_id) = operator_id_for_tenant_role(principal) else {
        return Ok(());
    };
    let Some(tenant) = extract_target_tenant_id(request.uri().path()) else {
        return Ok(());
    };

    // Attach the request's TARGET tenant id under a dedicated newtype
    // so `TenantAdminGuard` can echo it into the `tenant_role_missing`
    // body without clobbering the authenticated-principal's home
    // tenant (still at `TenantId` via auth_middleware). The UI
    // `<AdminGate>` then renders "no role on tenant T'" where T' is
    // the request's target, not the caller's home. Gemini PR #609.
    request
        .extensions_mut()
        .insert(crate::extractors::TargetTenantId(tenant.clone()));

    let record =
        OperatorTenantRoleReadModel::get(state.runtime.store.as_ref(), &tenant, &operator_id)
            .await
            .map_err(store_error_response)?;

    // An active grant (revoked_at_ms is None) is the only shape that
    // should expose the role to `TenantAdminGuard`. Revoked rows stay
    // in the table for audit but must not escalate.
    if let Some(role_record) = record {
        if role_record.is_active() {
            request.extensions_mut().insert(role_record.role);
        }
    }
    Ok(())
}

/// Return the operator id for principals that can hold tenant roles.
/// Returns `None` for System + admin service-account: those bypass the
/// tenant-role lookup via `is_admin_principal`, and the admin SA's
/// `name = "admin"` is not a valid OperatorId.
fn operator_id_for_tenant_role(principal: &AuthPrincipal) -> Option<OperatorId> {
    match principal {
        AuthPrincipal::Operator { operator_id, .. } => Some(operator_id.clone()),
        AuthPrincipal::ServiceAccount { .. } | AuthPrincipal::System => None,
    }
}

/// Parse the request path for a tenant id segment.
///
/// Shapes matched (in order):
///
///   * `/v1/admin/tenants/:tenant_id/...`
///   * `/v1/admin/operators/:operator_id/tenant-roles/:tenant_id/...`
///
/// Returns `None` for paths that do not carry a target tenant.
/// Cross-tenant admin routes that don't scope to a single tenant (e.g.
/// `GET /v1/admin/tenants` to list every tenant) return `None`; those
/// routes stay gated by `AdminRoleGuard` (god-token) until the admin-UI
/// series migrates them individually.
fn extract_target_tenant_id(path: &str) -> Option<TenantId> {
    let segments: Vec<&str> = path.trim_matches('/').split('/').collect();
    // `/v1/admin/tenants/:tenant_id/...` — require at least 4 segments
    // so `/v1/admin/tenants` (list) does not match.
    if segments.len() >= 4
        && segments[0] == "v1"
        && segments[1] == "admin"
        && segments[2] == "tenants"
        && !segments[3].is_empty()
    {
        return decode_tenant_segment(segments[3]);
    }
    // `/v1/admin/operators/:operator_id/tenant-roles/:tenant_id[/...]`
    if segments.len() >= 6
        && segments[0] == "v1"
        && segments[1] == "admin"
        && segments[2] == "operators"
        && segments[4] == "tenant-roles"
        && !segments[5].is_empty()
    {
        return decode_tenant_segment(segments[5]);
    }
    None
}

fn decode_tenant_segment(segment: &str) -> Option<TenantId> {
    let decoded = percent_decode_str(segment).decode_utf8().ok()?;
    // Defense-in-depth: reject not just `/` (the documented bypass)
    // but the full set of path / string-injection vectors a TenantId
    // should never legitimately contain. Each branch corresponds to
    // a separate attack class:
    //
    //   * `/` — encoded path separator; the documented bypass
    //     vector (`/v1/admin/tenants/victim%2Fother`).
    //   * `\` — Windows / proxy path separator; some intermediate
    //     proxies and downstream components treat `\` as a separator.
    //   * `\0` — string truncation in C-based libraries / drivers
    //     (sqlx binds tolerate it but logging / metrics labels may
    //     truncate at the null byte).
    //   * `.` and `..` — directory components if the tenant id is
    //     ever used as a path element (snapshot/restore paths,
    //     filesystem-backed audit logs).
    //
    // None of these are exploitable on the current code paths —
    // the existing routes consume `TenantId` as opaque strings —
    // but the cost of these checks is one branch each, and they
    // prevent foot-guns when future routes add filesystem or
    // OS-call interactions. Per Gemini PR #724 review.
    if decoded.is_empty()
        || decoded.contains('/')
        || decoded.contains('\\')
        || decoded.contains('\0')
        || decoded == "."
        || decoded == ".."
    {
        return None;
    }
    Some(TenantId::new(decoded.into_owned()))
}

pub(crate) async fn ensure_workspace_role_for_project(
    state: &AppState,
    principal: &AuthPrincipal,
    project: &ProjectKey,
    minimum_role: WorkspaceRole,
) -> Result<(), Response> {
    let Some(role) = lookup_workspace_role(state, principal, &project.workspace_key()).await?
    else {
        return Ok(());
    };
    if !role.has_at_least(minimum_role) {
        return Err(forbidden_api_error("insufficient workspace role").into_response());
    }
    Ok(())
}

// ── Observability ───────────────────────────────────────────────────────────

pub(crate) async fn observability_middleware(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Response {
    let method = request.method().as_str().to_owned();
    let path = request
        .extensions()
        .get::<MatchedPath>()
        .map(MatchedPath::as_str)
        .unwrap_or_else(|| request.uri().path())
        .to_owned();
    // T6b-C2: scrub credential-ish query params before they hit the
    // request-log ring buffer. The SSE EventSource path uses
    // `?token=<bearer>` because browsers can't set custom headers on
    // SSE connections — the raw bearer would otherwise be visible via
    // `GET /v1/admin/logs` and durably flushed to disk on shutdown.
    let query = request.uri().query().map(scrub_credentials_in_query);
    let request_id = request
        .extensions()
        .get::<RequestId>()
        .map(|r| r.0.clone())
        .unwrap_or_default();
    let start_time_unix_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let start = Instant::now();
    let response = next.run(request).await;
    let latency_ms = start.elapsed().as_millis() as u64;
    let status = response.status().as_u16();

    state
        .metrics
        .record_request(&method, &path, status, latency_ms);

    // Write structured log entry to the request log ring buffer.
    let level = if status >= 500 {
        "error"
    } else if status >= 400 {
        "warn"
    } else {
        "info"
    };
    let timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let message = format!("{method} {path} -> {status} ({latency_ms}ms)");
    if let Ok(mut log) = state.request_log.write() {
        log.push(RequestLogEntry {
            timestamp,
            level,
            message,
            request_id,
            method: method.clone(),
            path: path.clone(),
            query,
            status,
            latency_ms,
            start_time_unix_ns,
        });
    }

    refresh_activity_metrics(state.as_ref()).await;

    response
}

/// Cheap per-request refresh of the global active-runs / active-tasks
/// counts. Called by the observability middleware on every request to
/// keep the headline gauges fresh without scrape-interval lag.
pub(crate) async fn refresh_activity_metrics(state: &AppState) {
    let active_runs = state.runtime.store.count_active_runs().await;
    let active_tasks = state.runtime.store.count_active_tasks().await;
    state
        .metrics
        .set_active_counts(active_runs as usize, active_tasks as usize);
}

/// Heavyweight refresh called only from the `/metrics` handler path.
/// Enumerates up to 200 tenants and fans out three async store reads
/// per tenant — cheap enough for a once-per-scrape call (operators
/// typically scrape at 15-60 s), expensive enough that it would kill
/// per-request latency if routed through the middleware.
///
/// The existing non-tenant counters (`active_runs_total`,
/// `active_tasks_total`) are refreshed on every request via
/// [`refresh_activity_metrics`]; this function layers tenant
/// breakdowns + projection lag on top of that.
#[cfg(feature = "metrics-core")]
pub(crate) async fn refresh_scrape_metrics(state: &AppState) {
    // Projection lag: for the in-memory store this is structurally 0
    // (projections are applied synchronously inside `append`). The
    // gauge ships anyway so Postgres / SQLite backends have a ready
    // series the moment async projections land.
    state.metrics.set_projection_lag(0);

    // Tenant-scoped queue depths. Clear the active-tenant set each
    // pass so tenants that disappear (deleted, or past the 200-row
    // window) drop out of the metric instead of lingering as phantom
    // series.
    let tenants = match state.runtime.tenants.list(200, 0).await {
        Ok(t) => t,
        Err(err) => {
            tracing::warn!(error = %err, "refresh_scrape_metrics: tenant list failed");
            return;
        }
    };

    let active_ids: Vec<String> = tenants
        .iter()
        .map(|t| t.tenant_id.as_str().to_owned())
        .collect();
    state.metrics.retain_tenant_queue_depth(&active_ids);

    for t in &tenants {
        let runs = state
            .runtime
            .store
            .count_active_runs_for_tenant(&t.tenant_id)
            .await;
        let tasks = state
            .runtime
            .store
            .count_active_tasks_for_tenant(&t.tenant_id)
            .await;
        let pending = state
            .runtime
            .store
            .count_pending_approvals_for_tenant(&t.tenant_id)
            .await;
        state
            .metrics
            .set_tenant_queue_depth(t.tenant_id.as_str(), runs, tasks, pending);
    }
}

// ── Private helpers ─────────────────────────────────────────────────────────

fn unauthorized_response() -> Response {
    AppApiError::new(StatusCode::UNAUTHORIZED, "unauthorized", "unauthorized").into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Request;

    // ── auth_exempt_path ───────────────────────────────────────────────

    #[test]
    fn exempt_infra_endpoints() {
        for path in &[
            "/health",
            "/healthz",
            "/ready",
            "/metrics",
            "/version",
            "/openapi.json",
            "/docs",
        ] {
            assert!(auth_exempt_path(path), "{path} should be auth-exempt");
        }
    }

    #[test]
    fn exempt_public_api_endpoints() {
        assert!(auth_exempt_path("/v1/onboarding/templates"));
        assert!(auth_exempt_path("/v1/docs"));
        // T6b-C7: /v1/stream is no longer blanket-exempt; the SSE
        // handler validates its own ?token= query param.
        assert!(!auth_exempt_path("/v1/stream"));
    }

    #[test]
    fn spa_fallback_only_exempts_get() {
        use axum::http::Method;
        // GET on a SPA-fallback path is exempt (serves index.html).
        assert!(auth_exempt_path_method("/settings", &Method::GET));
        // POST / PATCH / DELETE on the same path must still hit auth.
        assert!(!auth_exempt_path_method("/settings", &Method::POST));
        assert!(!auth_exempt_path_method("/settings", &Method::PATCH));
        assert!(!auth_exempt_path_method("/settings", &Method::DELETE));
    }

    #[test]
    fn case_insensitive_exempt_matching() {
        // /V1/RUNS should behave the same as /v1/runs — i.e. NOT
        // exempt (it's under /v1/, case-insensitive).
        assert!(!auth_exempt_path("/V1/runs"));
        assert!(!auth_exempt_path("/V1/Stream"));
    }

    #[test]
    fn exempt_webhook_paths() {
        assert!(auth_exempt_path("/v1/webhooks/github"));
        assert!(auth_exempt_path("/v1/webhooks/slack"));
        assert!(auth_exempt_path("/v1/webhooks/any-integration"));
    }

    #[test]
    fn exempt_static_ui_paths() {
        assert!(auth_exempt_path("/"));
        assert!(auth_exempt_path("/index.html"));
        assert!(auth_exempt_path("/favicon.svg"));
        assert!(auth_exempt_path("/assets/index-abc123.js"));
        assert!(auth_exempt_path("/assets/style.css"));
    }

    #[test]
    fn exempt_spa_fallback_non_v1() {
        // Any path that does NOT start with /v1/ is SPA fallback.
        assert!(auth_exempt_path("/settings"));
        assert!(auth_exempt_path("/runs/abc"));
        assert!(auth_exempt_path("/dashboard"));
    }

    #[test]
    fn not_exempt_v1_api_routes() {
        assert!(!auth_exempt_path("/v1/runs"));
        assert!(!auth_exempt_path("/v1/prompts"));
        assert!(!auth_exempt_path("/v1/admin/settings"));
        assert!(!auth_exempt_path("/v1/agents"));
        assert!(!auth_exempt_path("/v1/sessions"));
    }

    // ── bearer_token ───────────────────────────────────────────────────

    fn make_request(uri: &str, auth_header: Option<&str>) -> Request<Body> {
        let mut builder = Request::builder().uri(uri);
        if let Some(header) = auth_header {
            builder = builder.header("Authorization", header);
        }
        builder.body(Body::empty()).unwrap()
    }

    fn make_request_method(method: &str, uri: &str, auth_header: Option<&str>) -> Request<Body> {
        let mut builder = Request::builder().method(method).uri(uri);
        if let Some(header) = auth_header {
            builder = builder.header("Authorization", header);
        }
        builder.body(Body::empty()).unwrap()
    }

    #[test]
    fn bearer_from_auth_header() {
        let req = make_request("/v1/runs", Some("Bearer my-secret-token"));
        assert_eq!(bearer_token(&req), Some("my-secret-token".to_owned()));
    }

    #[test]
    fn bearer_from_query_param() {
        let req = make_request("/v1/stream?token=sse-token-123", None);
        assert_eq!(bearer_token(&req), Some("sse-token-123".to_owned()));
    }

    #[test]
    fn bearer_from_query_with_other_params() {
        let req = make_request("/v1/stream?follow=true&token=t1&limit=10", None);
        assert_eq!(bearer_token(&req), Some("t1".to_owned()));
    }

    #[test]
    fn bearer_header_takes_priority_over_query() {
        let req = make_request("/v1/stream?token=query-tok", Some("Bearer header-tok"));
        assert_eq!(bearer_token(&req), Some("header-tok".to_owned()));
    }

    #[test]
    fn bearer_none_when_missing() {
        let req = make_request("/v1/runs", None);
        assert_eq!(bearer_token(&req), None);
    }

    #[test]
    fn bearer_none_for_non_bearer_auth() {
        let req = make_request("/v1/runs", Some("Basic dXNlcjpwYXNz"));
        assert_eq!(bearer_token(&req), None);
    }

    #[test]
    fn bearer_none_for_empty_query_token() {
        let req = make_request("/v1/stream?token=", None);
        assert_eq!(bearer_token(&req), None);
    }

    #[test]
    fn tenant_target_is_percent_decoded() {
        let path = "/v1/admin/tenants/%76ictim/snapshot";
        let tenant = extract_target_tenant_id(path).expect("tenant should decode");
        assert_eq!(tenant.as_str(), "victim");
    }

    #[test]
    fn tenant_target_rejects_encoded_slash() {
        let path = "/v1/admin/tenants/victim%2Fother/snapshot";
        assert_eq!(extract_target_tenant_id(path), None);
    }

    /// Defense-in-depth checks added per Gemini PR #724 review.
    /// One row per attack class; each entry must be rejected even
    /// though the current routes don't expose an exploitation path.
    #[test]
    fn tenant_target_rejects_dangerous_sequences() {
        for path in &[
            // Encoded backslash (Windows / proxy separator)
            "/v1/admin/tenants/victim%5Cother/snapshot",
            // Encoded null byte (C-string truncation)
            "/v1/admin/tenants/victim%00other/snapshot",
            // Encoded directory components (path traversal)
            "/v1/admin/tenants/%2e/snapshot",
            "/v1/admin/tenants/%2e%2e/snapshot",
            // Same checks on the operators / tenant-roles route shape
            "/v1/admin/operators/op-1/tenant-roles/victim%5Cother",
            "/v1/admin/operators/op-1/tenant-roles/%2e%2e",
        ] {
            assert_eq!(extract_target_tenant_id(path), None, "should reject {path}");
        }
    }

    // #491: `?token=` query-string fallback must be GET-only.

    #[test]
    fn query_token_accepted_on_get() {
        let req = make_request_method("GET", "/v1/stream?token=t1", None);
        assert_eq!(bearer_token(&req), Some("t1".to_owned()));
    }

    #[test]
    fn query_token_refused_on_post() {
        let req = make_request_method("POST", "/v1/runs?token=t1", None);
        assert_eq!(
            bearer_token(&req),
            None,
            "query-param token fallback must not fire on POST",
        );
    }

    #[test]
    fn query_token_refused_on_put() {
        let req = make_request_method("PUT", "/v1/settings?token=t1", None);
        assert_eq!(bearer_token(&req), None);
    }

    #[test]
    fn query_token_refused_on_patch() {
        let req = make_request_method("PATCH", "/v1/settings?token=t1", None);
        assert_eq!(bearer_token(&req), None);
    }

    #[test]
    fn query_token_refused_on_delete() {
        let req = make_request_method("DELETE", "/v1/runs/foo?token=t1", None);
        assert_eq!(bearer_token(&req), None);
    }

    /// The method gate must NOT suppress a header-provided bearer on
    /// non-GET methods — that would lock out every API consumer.
    #[test]
    fn header_bearer_still_accepted_on_post() {
        let req = make_request_method("POST", "/v1/runs", Some("Bearer header-tok"));
        assert_eq!(bearer_token(&req), Some("header-tok".to_owned()));
    }

    /// #491 + Copilot r3: `?token=a&token=b` must be rejected outright
    /// (no first-wins or last-wins). A proxy that reorders duplicate
    /// query params is a real-world smuggling vector, and the code
    /// comment above the implementation commits to "duplicates are
    /// rejected" — this test binds that contract.
    #[test]
    fn query_token_duplicates_rejected() {
        let req = make_request_method("GET", "/v1/stream?token=a&token=b", None);
        assert_eq!(
            bearer_token(&req),
            None,
            "duplicate ?token= params must be rejected (query-param smuggling)",
        );
    }

    /// Triple-duplicate sanity check — any count > 1 is a rejection.
    #[test]
    fn query_token_triple_duplicates_rejected() {
        let req = make_request_method("GET", "/v1/stream?token=a&other=x&token=b&token=c", None);
        assert_eq!(bearer_token(&req), None);
    }

    // ── request_rate_limit_key ─────────────────────────────────────────

    #[test]
    fn rate_limit_key_from_forwarded_for() {
        let req = Request::builder()
            .uri("/v1/runs")
            .header("x-forwarded-for", "10.0.0.1")
            .body(Body::empty())
            .unwrap();
        assert_eq!(request_rate_limit_key(&req), Some("10.0.0.1".to_owned()));
    }

    #[test]
    fn rate_limit_key_none_without_header() {
        let req = Request::builder()
            .uri("/v1/runs")
            .body(Body::empty())
            .unwrap();
        assert_eq!(request_rate_limit_key(&req), None);
    }

    #[test]
    fn rate_limit_key_none_for_empty_header() {
        let req = Request::builder()
            .uri("/v1/runs")
            .header("x-forwarded-for", "  ")
            .body(Body::empty())
            .unwrap();
        assert_eq!(request_rate_limit_key(&req), None);
    }

    // ── principal_member_id ────────────────────────────────────────────

    use cairn_domain::ids::{OperatorId, TenantId};
    use cairn_domain::tenancy::TenantKey;

    fn test_tenant() -> TenantKey {
        TenantKey {
            tenant_id: TenantId::new("t"),
        }
    }

    #[test]
    fn member_id_for_operator() {
        let principal = AuthPrincipal::Operator {
            operator_id: OperatorId::new("op-1"),
            tenant: test_tenant(),
        };
        assert_eq!(principal_member_id(&principal), Some("op-1"));
    }

    #[test]
    fn member_id_for_service_account() {
        let principal = AuthPrincipal::ServiceAccount {
            name: "sa-ci".into(),
            tenant: test_tenant(),
        };
        assert_eq!(principal_member_id(&principal), Some("sa-ci"));
    }

    #[test]
    fn member_id_none_for_system() {
        assert_eq!(principal_member_id(&AuthPrincipal::System), None);
    }

    // ── #490: rate-limit key hashing ───────────────────────────────────────

    /// Two distinct bearer tokens must hash to distinct rate-limit keys.
    /// Regression guard: if SHA-256 were ever swapped for a cheaper hash
    /// with collisions at this cardinality the rate-limit windows would
    /// merge and we'd silently pool traffic from unrelated tokens.
    #[test]
    fn distinct_tokens_produce_distinct_rate_limit_hashes() {
        let h1 = hash_rate_limit_token("sk-alpha-abcdef0123456789");
        let h2 = hash_rate_limit_token("sk-beta-fedcba9876543210");
        assert_ne!(h1, h2);
        // Hex-encoded SHA-256 is always 64 chars of [0-9a-f].
        assert_eq!(h1.len(), 64);
        assert_eq!(h2.len(), 64);
        assert!(h1.chars().all(|c| c.is_ascii_hexdigit()));
        assert!(h2.chars().all(|c| c.is_ascii_hexdigit()));
    }

    /// Identical bearer tokens must hash to the same rate-limit key
    /// across calls, or the sliding-window bucket never matches.
    #[test]
    fn same_token_produces_same_rate_limit_hash() {
        let token = "sk-stable-0123456789abcdef";
        assert_eq!(
            hash_rate_limit_token(token),
            hash_rate_limit_token(token),
            "hash must be deterministic",
        );
    }

    /// The raw bearer token must NEVER appear in the rate-limit key —
    /// that's the whole point of #490. A `contains` substring check is
    /// the cheapest way to spot a regression (someone re-introducing
    /// `format!("tok:{token}")` without hashing).
    #[test]
    fn hash_rate_limit_token_does_not_leak_plaintext() {
        let token = "sk-leakproof-abcdef0123456789";
        let hashed = hash_rate_limit_token(token);
        assert!(
            !hashed.contains(token),
            "hashed key {hashed} must not contain raw token {token}",
        );
        // Also sanity-check a common substring that is NOT a hex char
        // to rule out accidental printable-ASCII leakage.
        assert!(
            !hashed.contains("sk-"),
            "hashed key {hashed} must not contain the 'sk-' prefix",
        );
    }

    /// Empty tokens hash deterministically (SHA-256 of "") — an empty
    /// token never reaches this function in practice because
    /// `bearer_token` filters empties first, but the hasher itself
    /// should still be defined.
    #[test]
    fn hash_rate_limit_token_handles_empty() {
        // SHA-256 of the empty string (well-known vector).
        assert_eq!(
            hash_rate_limit_token(""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        );
    }

    // ── #649: loopback exemption ──────────────────────────────────────────
    //
    // The rate-limit middleware bypasses bucket accounting when
    // `is_direct_loopback_request` returns true — i.e. when the TCP
    // peer is 127.0.0.0/8 or ::1 AND the request carries no
    // `X-Forwarded-For` / `Forwarded` header. These tests pin every
    // fork of that predicate, including the spoofing scenarios that
    // Gemini r1 flagged as high severity.

    fn connect_info_request(peer: &str) -> Request<Body> {
        let addr: SocketAddr = peer.parse().expect("valid socket addr");
        let mut req = Request::builder()
            .uri("/v1/runs")
            .body(Body::empty())
            .unwrap();
        req.extensions_mut().insert(ConnectInfo(addr));
        req
    }

    #[test]
    fn rate_limit_bypasses_ipv4_loopback() {
        // Peer = 127.0.0.1, no X-Forwarded-For → direct-loopback →
        // middleware short-circuits before touching the bucket map,
        // so no amount of repeat traffic can trip the 1 000/min
        // limit.
        let req = connect_info_request("127.0.0.1:54321");
        assert!(is_direct_loopback_request(&req));
    }

    #[test]
    fn rate_limit_bypasses_ipv6_loopback() {
        // IPv6 ::1 is the v6 loopback. Same bypass contract as v4.
        let req = connect_info_request("[::1]:54321");
        assert!(is_direct_loopback_request(&req));
    }

    #[test]
    fn rate_limit_respects_x_forwarded_for_over_loopback() {
        // Production topology: a reverse proxy terminates TLS on the
        // same host as cairn-app. The TCP peer is loopback AND the
        // proxy sets `X-Forwarded-For`. The exemption MUST NOT fire
        // — otherwise a proxied deployment would silently disable the
        // rate limiter.
        let addr: SocketAddr = "127.0.0.1:54321".parse().unwrap();
        let mut req = Request::builder()
            .uri("/v1/runs")
            .header("x-forwarded-for", "8.8.8.8")
            .body(Body::empty())
            .unwrap();
        req.extensions_mut().insert(ConnectInfo(addr));
        assert!(
            !is_direct_loopback_request(&req),
            "presence of X-Forwarded-For must disqualify the exemption \
             even when the TCP peer is 127.0.0.1",
        );
    }

    #[test]
    fn rate_limit_still_applies_to_rfc1918() {
        // Peer inside RFC 1918 private space (10.0.0.0/8) is NOT
        // loopback — the exemption must not leak to LAN traffic.
        let req = connect_info_request("10.0.0.1:54321");
        assert!(!is_direct_loopback_request(&req));
    }

    /// Gemini r1 HIGH: an external attacker must NOT be able to
    /// bypass the limiter by forging `X-Forwarded-For: 127.0.0.1`.
    /// The exemption ignores the header entirely for classification
    /// purposes — only the TCP peer matters, and the presence of
    /// *any* forwarding header disqualifies the request.
    #[test]
    fn rate_limit_spoofed_x_forwarded_for_loopback_does_not_bypass() {
        let addr: SocketAddr = "203.0.113.42:40000".parse().unwrap(); // TEST-NET-3
        let mut req = Request::builder()
            .uri("/v1/runs")
            .header("x-forwarded-for", "127.0.0.1")
            .body(Body::empty())
            .unwrap();
        req.extensions_mut().insert(ConnectInfo(addr));
        assert!(
            !is_direct_loopback_request(&req),
            "forged X-Forwarded-For: 127.0.0.1 from a public peer must \
             never bypass the rate limiter",
        );
    }

    /// The alternate RFC 7239 `Forwarded:` header must also
    /// disqualify the exemption — a proxy that speaks Forwarded
    /// instead of X-Forwarded-For is still a proxy.
    #[test]
    fn rate_limit_forwarded_header_also_disqualifies() {
        let addr: SocketAddr = "127.0.0.1:54321".parse().unwrap();
        let mut req = Request::builder()
            .uri("/v1/runs")
            .header("forwarded", "for=8.8.8.8")
            .body(Body::empty())
            .unwrap();
        req.extensions_mut().insert(ConnectInfo(addr));
        assert!(!is_direct_loopback_request(&req));
    }

    /// A request with no `ConnectInfo` extension falls through — no
    /// bypass, no panic. Applies to any axum `serve()` that wasn't
    /// wired with `into_make_service_with_connect_info` (test
    /// harnesses, or a misconfigured future server).
    #[test]
    fn rate_limit_no_connect_info_does_not_bypass() {
        let req = Request::builder()
            .uri("/v1/runs")
            .body(Body::empty())
            .unwrap();
        assert!(!is_direct_loopback_request(&req));
    }

    // ── classify_body_read_error (audit #483 follow-up, #564 review) ───────

    /// A 10 MiB cap exceeded by an oversized body must map to 413
    /// Payload Too Large with the `payload_too_large` code — matches
    /// the status the axum `JsonRejection::BytesRejection` path
    /// surfaces in `json_rejection_response`, so callers reading the
    /// response envelope see a stable code across both entry paths.
    #[tokio::test]
    async fn classify_body_read_error_length_limit_is_413() {
        // Build a body that exceeds the 1-byte cap so `to_bytes`
        // returns `axum::Error` wrapping `LengthLimitError`. Using
        // the real `to_bytes` rather than a synthetic `axum::Error`
        // keeps the test honest about the actual wrapped shape.
        let body = Body::from("aa");
        let err = to_bytes(body, 1)
            .await
            .expect_err("to_bytes over the cap should fail");
        let envelope = classify_body_read_error(&err);
        assert_eq!(envelope.status, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(envelope.error.status_code, 413);
        assert_eq!(envelope.error.code, "payload_too_large");
    }

    /// A failure that does NOT match the length-limit substring falls
    /// back to 400 `bad_request`. The fallback branch is exercised
    /// directly through the `err.to_string()` / `Debug` substring
    /// check — if a future `http-body-util` wording change breaks
    /// detection, we degrade to 400 (wrong but recoverable) rather
    /// than mis-classifying as 413.
    #[test]
    fn classify_body_read_error_unknown_shape_is_400() {
        // Fake a non-length-limit `axum::Error` by wrapping a
        // custom `std::io::Error` that has no "LengthLimitError" or
        // "length limit exceeded" in its formatted output.
        let io = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "peer reset");
        let err = axum::Error::new(io);
        let envelope = classify_body_read_error(&err);
        assert_eq!(envelope.status, StatusCode::BAD_REQUEST);
        assert_eq!(envelope.error.status_code, 400);
        assert_eq!(envelope.error.code, "bad_request");
    }
}
