//! Notification-channel target validation (#235, #451, #452).
//!
//! `POST /v1/admin/operators/:id/notifications` accepts arbitrary strings as
//! `channel.target`. Operators have been saving typos (`not-a-url`) and the
//! server happily 201'd — then the webhook dispatcher silently discarded
//! every delivery, which surfaces days later as "my alerts never fire".
//!
//! This module enforces a structured shape check at the HTTP boundary so
//! those typos round-trip as a 422 with a human-readable reason, not a
//! latent delivery failure.
//!
//! Rules:
//!
//! * `kind = webhook` or `kind = slack` — `target` must parse as an absolute
//!   URL. Scheme must be `https://` (or `http://` with the
//!   `CAIRN_ALLOW_INSECURE_WEBHOOKS` / Local-mode opt-in that already existed
//!   for #235).
//!
//!   **Independently of scheme**, the target host MUST NOT resolve to a
//!   loopback, link-local, private (RFC 1918 / ULA), carrier-grade NAT,
//!   unspecified, or multicast IP range. Cloud metadata services live on
//!   link-local (`169.254.169.254` for AWS / GCP / Azure) — unrestricted
//!   outbound webhooks to link-local would turn any operator with the
//!   `notifications` feature into a one-step IAM-credential exfiltrator
//!   (#451). The same set of blocked ranges also closes the http-only
//!   loopback gap (#452) — `https://localhost` is rejected now too.
//!
//!   Self-hosted dev boxes that intentionally POST to a local collector
//!   (ngrok-less webhook receivers, local Prometheus Alertmanager, etc.)
//!   opt in via `CAIRN_ALLOW_INTERNAL_WEBHOOKS=1`. This ONLY unlocks the
//!   SSRF block list; it does NOT change scheme requirements (http-vs-https
//!   is still gated by `CAIRN_ALLOW_INSECURE_WEBHOOKS`).
//!
//!   Any other scheme (`ftp://`, `file://`, `javascript:…`, etc.) is
//!   rejected.
//! * `kind = email` — basic RFC 5321 shape check: non-empty local part, a
//!   single `@`, and a non-empty domain with a dot. No deep DNS / MX
//!   validation (that's a delivery-time concern).
//! * Other kinds (`pagerduty`, `telegram`, …) are accepted without URL
//!   validation — the UI sends chat IDs / tokens that aren't URLs. If
//!   scheme checks are wanted for those later, add them per-kind here.
//!
//! Returns `Ok(())` on success or `Err(String)` with a message that is safe
//! to show the operator verbatim in the 422 body.
//!
//! ## Defense in depth at dispatch time
//!
//! Validation at registration is not sufficient on its own: a domain that
//! resolves to `93.184.216.34` today might resolve to `169.254.169.254`
//! tomorrow (DNS rebinding). Callers that actually issue outbound HTTP for
//! a webhook MUST additionally call [`enforce_outbound_webhook_target`]
//! right before the request. That function re-runs the same resolve +
//! block-list check against the URL currently being dispatched and fails
//! the delivery if the resolved IP set intersects any blocked range.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::OnceLock;

use cairn_api::bootstrap::{BootstrapConfig, DeploymentMode};
use cairn_domain::notification_prefs::NotificationChannel;

/// Env var that opts a team-mode deployment into accepting `http://localhost`
/// webhook targets. Production clusters should leave this unset so the
/// handler insists on `https://`.
pub const ALLOW_INSECURE_WEBHOOKS_ENV: &str = "CAIRN_ALLOW_INSECURE_WEBHOOKS";

/// Env var that opts in to accepting webhook targets whose resolved IP
/// falls inside loopback / link-local / private / CGNAT / multicast
/// ranges. Off by default so a production deployment is SSRF-safe on the
/// webhook path (#451, #452). Self-hosted operators running their own
/// local webhook sink (e.g. Alertmanager bound to 127.0.0.1) set this to
/// `1` to unblock their stack.
///
/// This is orthogonal to [`ALLOW_INSECURE_WEBHOOKS_ENV`] — the two opt-ins
/// address different concerns:
///
/// * `CAIRN_ALLOW_INSECURE_WEBHOOKS`: "I accept traffic over plain http."
/// * `CAIRN_ALLOW_INTERNAL_WEBHOOKS`: "I accept traffic to internal /
///   link-local destinations."
///
/// A team hitting a local http Alertmanager needs BOTH.
pub const ALLOW_INTERNAL_WEBHOOKS_ENV: &str = "CAIRN_ALLOW_INTERNAL_WEBHOOKS";

/// Cached result of `CAIRN_ALLOW_INSECURE_WEBHOOKS`. The env var is read at
/// most once per process; subsequent calls to [`insecure_webhooks_allowed`]
/// hit this cache instead of the libc `getenv` lock. Env vars are process-
/// level static for the lifetime of the server, so caching is safe.
static INSECURE_WEBHOOKS_ENV_CACHE: OnceLock<bool> = OnceLock::new();

/// Same caching pattern for `CAIRN_ALLOW_INTERNAL_WEBHOOKS`.
static INTERNAL_WEBHOOKS_ENV_CACHE: OnceLock<bool> = OnceLock::new();

fn env_bool(val: Result<String, std::env::VarError>) -> bool {
    match val {
        Ok(v) => matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => false,
    }
}

fn insecure_webhooks_env_set() -> bool {
    *INSECURE_WEBHOOKS_ENV_CACHE
        .get_or_init(|| env_bool(std::env::var(ALLOW_INSECURE_WEBHOOKS_ENV)))
}

fn internal_webhooks_env_set() -> bool {
    *INTERNAL_WEBHOOKS_ENV_CACHE
        .get_or_init(|| env_bool(std::env::var(ALLOW_INTERNAL_WEBHOOKS_ENV)))
}

/// Decide whether `http://localhost` webhook targets should be accepted for
/// this deployment.
///
/// Allowed when either:
///  - `DeploymentMode::Local` (single-operator dev box — cairn already
///    accepts `http://` admin traffic here), or
///  - `CAIRN_ALLOW_INSECURE_WEBHOOKS=1` is set in the server's env
///    (explicit operator opt-in for team-mode dev stacks).
///
/// Any other truthy value (`true`, `yes`, `on`) also counts so operators
/// don't get tripped up by shell quoting.
pub fn insecure_webhooks_allowed(config: &BootstrapConfig) -> bool {
    if matches!(config.mode, DeploymentMode::Local) {
        return true;
    }
    insecure_webhooks_env_set()
}

/// Decide whether webhooks may target loopback / link-local / private IPs.
///
/// Allowed when either:
///  - `DeploymentMode::Local` — a single-operator dev box is expected to
///    POST to its own 127.0.0.1 collectors; a production deployment is
///    not.
///  - `CAIRN_ALLOW_INTERNAL_WEBHOOKS=1` — explicit team-mode opt-in for
///    teams whose webhook sinks live inside their own network (self-hosted
///    Alertmanager, internal Slack bridge, etc.).
pub fn internal_webhooks_allowed(config: &BootstrapConfig) -> bool {
    if matches!(config.mode, DeploymentMode::Local) {
        return true;
    }
    internal_webhooks_env_set()
}

/// Per-call policy for channel validation. Construct via
/// [`WebhookValidationPolicy::from_config`] in production; tests build one
/// literally.
#[derive(Clone, Copy, Debug)]
pub struct WebhookValidationPolicy {
    /// Accept `http://` scheme (with loopback-host restriction retained).
    pub allow_insecure_scheme: bool,
    /// Accept loopback / link-local / private destination IPs.
    pub allow_internal_destinations: bool,
}

impl WebhookValidationPolicy {
    pub fn from_config(config: &BootstrapConfig) -> Self {
        // Narrow back-compat: `CAIRN_ALLOW_INSECURE_WEBHOOKS=1` alone
        // unlocks http-scheme targets AND the `http://localhost`
        // loopback destination for that scheme specifically. It does
        // NOT unlock internal https targets (169.254.x, RFC 1918, etc.)
        // — that would re-open #451 for any team-mode deployment that
        // uses the dev flag.
        //
        // Reviewers correctly flagged (#533 review round) that ORing
        // `allow_internal_destinations` with `allow_insecure_scheme` in
        // the previous revision let an operator with the dev flag set
        // post `https://169.254.169.254/…` and hit IMDS. The two
        // opt-ins are now truly orthogonal at policy construction —
        // `validate_url_target` alone handles the "http scheme →
        // loopback only" invariant without bleeding into the https
        // path's SSRF gate.
        Self {
            allow_insecure_scheme: insecure_webhooks_allowed(config),
            allow_internal_destinations: internal_webhooks_allowed(config),
        }
    }

    /// Production defaults — strictest policy. Every opt-in is OFF.
    pub fn strict() -> Self {
        Self {
            allow_insecure_scheme: false,
            allow_internal_destinations: false,
        }
    }
}

/// Validate a single notification channel. See module docs for the rule set.
pub async fn validate_channel(
    channel: &NotificationChannel,
    policy: WebhookValidationPolicy,
) -> Result<(), String> {
    let target = channel.target.trim();
    if target.is_empty() {
        return Err(format!(
            "channel target must not be empty (kind = {})",
            channel.kind
        ));
    }
    match channel.kind.as_str() {
        "webhook" | "slack" => validate_url_target(&channel.kind, target, policy).await,
        "email" => validate_email_target(target),
        // Unknown / future kinds round-trip unchanged — the delivery layer
        // is responsible for rejecting targets it can't dispatch.
        _ => Ok(()),
    }
}

/// Validate every channel in a request. Returns the first failure (callers
/// surface it as a 422). Index is included in the error so the UI can point
/// at the offending row.
pub async fn validate_channels(
    channels: &[NotificationChannel],
    policy: WebhookValidationPolicy,
) -> Result<(), String> {
    for (idx, ch) in channels.iter().enumerate() {
        if let Err(msg) = validate_channel(ch, policy).await {
            return Err(format!("channels[{idx}]: {msg}"));
        }
    }
    Ok(())
}

/// Re-check an outbound webhook URL right before dispatch. Returns the
/// same `Err(String)` shape as [`validate_channel`] and is intended to be
/// called by the HTTP delivery layer immediately before issuing the
/// request. Catches the narrow but real "resolved IP has rotated to an
/// internal address since registration" case (DNS rebinding).
///
/// `policy` should reflect the server's runtime config at dispatch time —
/// not the registration-time policy — so that operators who later set
/// `CAIRN_ALLOW_INTERNAL_WEBHOOKS=1` don't have stale registrations
/// silently fail.
pub async fn enforce_outbound_webhook_target(
    url: &str,
    policy: WebhookValidationPolicy,
) -> Result<(), String> {
    validate_url_target("webhook", url.trim(), policy).await
}

async fn validate_url_target(
    kind: &str,
    target: &str,
    policy: WebhookValidationPolicy,
) -> Result<(), String> {
    let parsed = url::Url::parse(target).map_err(|e| {
        format!(
            "{kind} target is not a valid URL: {e} (expected https://… with \
             an absolute host, e.g. https://hooks.example.com/path)"
        )
    })?;

    // First: scheme gate. `http` requires the insecure opt-in; anything
    // other than http/https is always rejected.
    let is_http = match parsed.scheme() {
        "https" => {
            require_host(&parsed, kind)?;
            false
        }
        "http" => {
            if !policy.allow_insecure_scheme {
                return Err(format!(
                    "{kind} target must use https:// (got http://). Set \
                     CAIRN_ALLOW_INSECURE_WEBHOOKS=1 to allow http://localhost \
                     in dev."
                ));
            }
            require_host(&parsed, kind)?;
            true
        }
        other => {
            return Err(format!(
                "{kind} target has unsupported scheme '{other}:'; only https:// \
                 (or http://localhost in dev) is allowed"
            ));
        }
    };

    // Second: SSRF / destination gate. Three cases, independent per
    // scheme (the reviewer feedback correction — the previous revision
    // let `CAIRN_ALLOW_INSECURE_WEBHOOKS=1` re-open #451 for https
    // targets, by making the scheme opt-in imply the internal-dest
    // opt-in. It no longer does).
    //
    //   http:    require loopback host (pre-#451 invariant). The
    //            `allow_insecure_scheme` opt-in is ABOUT enabling this
    //            one case. We do NOT additionally require
    //            `allow_internal_destinations` here — those two flags
    //            came as one bundle for http before #451 and
    //            separating them would silently break dev setups.
    //            The SSRF block list doesn't need to fire on top:
    //            loopback-only is already strictly narrower than the
    //            block list.
    //
    //   https + !allow_internal_destinations (default production):
    //            SSRF block list applies — the #451 / #452 fix proper.
    //            `https://169.254.169.254/…` is refused even when the
    //            dev scheme flag is on.
    //
    //   https + allow_internal_destinations (explicit operator opt-in
    //   for self-hosted stacks with a local https sink):
    //            SSRF block list skipped. Operator has accepted the
    //            risk.
    if is_http {
        if !is_loopback_host(&parsed) {
            return Err(format!(
                "{kind} target http:// is only permitted for loopback \
                 hosts (localhost, 127.0.0.1, ::1); got {host}",
                host = parsed.host_str().unwrap_or("<none>")
            ));
        }
    } else if !policy.allow_internal_destinations {
        check_host_not_blocked(&parsed, kind).await?;
    }

    Ok(())
}

fn is_loopback_host(parsed: &url::Url) -> bool {
    match parsed.host() {
        Some(url::Host::Domain(d)) => d.eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        None => false,
    }
}

/// Resolve the URL's host and refuse if any resolved IP (or the host
/// itself when it's an IP literal) falls into a blocked range.
///
/// Returns `Err` with an operator-safe message on failure. The message
/// names the blocked IP so the operator can see *why* their sink is
/// rejected.
async fn check_host_not_blocked(parsed: &url::Url, kind: &str) -> Result<(), String> {
    let host = parsed
        .host()
        .ok_or_else(|| format!("{kind} target is missing a host"))?;

    // Fast path: IP literal. No DNS round-trip needed, and more importantly
    // no way for the operator to hide an IMDS IP behind a CNAME (the parser
    // already gave us the concrete address).
    match host {
        url::Host::Ipv4(ip) => {
            if let Some(reason) = classify_blocked_ipv4(ip) {
                return Err(format!(
                    "{kind} target {ip} is in a blocked range ({reason}); \
                     set CAIRN_ALLOW_INTERNAL_WEBHOOKS=1 to permit loopback \
                     / private destinations in dev."
                ));
            }
            return Ok(());
        }
        url::Host::Ipv6(ip) => {
            if let Some(reason) = classify_blocked_ipv6(ip) {
                return Err(format!(
                    "{kind} target {ip} is in a blocked range ({reason}); \
                     set CAIRN_ALLOW_INTERNAL_WEBHOOKS=1 to permit loopback \
                     / private destinations in dev."
                ));
            }
            return Ok(());
        }
        url::Host::Domain(_) => {}
    }

    // DNS path: resolve host:port and refuse if ANY resolved address is
    // blocked. Using port 0 is fine — `lookup_host` resolves the name
    // regardless of port, and we only inspect the IP half of each
    // returned `SocketAddr`.
    //
    // **DNS failure policy**: if the resolver errors (NXDOMAIN, timeout,
    // …) we fail OPEN. Justification:
    //   * An unresolvable host cannot actually be hit from the webhook
    //     dispatcher either — the delivery will fail on the same DNS
    //     error — so there is no extra reachability gained by accepting
    //     the registration.
    //   * Fail-closed here breaks registration for any domain that's
    //     temporarily DNS-flaky at the moment the operator hits Save.
    //   * The DNS-rebinding scenario (public IP at registration,
    //     internal IP at dispatch) is addressed by
    //     [`enforce_outbound_webhook_target`] called at dispatch time,
    //     not by this registration-time check.
    // We still enforce the block list on every IP the resolver DOES
    // return — that's the core SSRF guarantee.
    let host_str = host.to_string();
    let lookup_target = format!("{host_str}:0");
    let addrs = match tokio::net::lookup_host(&lookup_target).await {
        Ok(iter) => iter,
        Err(_) => return Ok(()),
    };
    for sock in addrs {
        let ip = sock.ip();
        if let Some(reason) = classify_blocked_ip(ip) {
            // SEC-007: DO NOT echo the resolved IP. When the host is a
            // domain name, printing the resolved address confirms
            // internal network topology ("oh, `corp-alertmanager.
            // example.com` resolves to 10.42.7.9") to the API caller,
            // who in a multi-tenant deployment is not necessarily the
            // operator who owns the DNS zone. Report only the blocked
            // range category (`reason`). IP literals are fine to echo
            // because the caller already provided the IP — no new info
            // is leaked (that branch is handled in the fast path
            // above).
            return Err(format!(
                "{kind} target '{host_str}' resolves to a blocked destination \
                 range ({reason}); set CAIRN_ALLOW_INTERNAL_WEBHOOKS=1 to \
                 permit loopback / private destinations in dev."
            ));
        }
    }
    Ok(())
}

/// Classify an `IpAddr` against the SSRF block list. Returns the
/// human-readable range name when blocked, `None` when safe for outbound.
fn classify_blocked_ip(ip: IpAddr) -> Option<&'static str> {
    match ip {
        IpAddr::V4(v4) => classify_blocked_ipv4(v4),
        IpAddr::V6(v6) => classify_blocked_ipv6(v6),
    }
}

/// IPv4 SSRF block list.
///
/// Ranges:
/// * 0.0.0.0/8        — "this network" (RFC 1122)
/// * 10.0.0.0/8       — RFC 1918 private
/// * 100.64.0.0/10    — Carrier-grade NAT (RFC 6598)
/// * 127.0.0.0/8      — loopback
/// * 169.254.0.0/16   — link-local, including IMDS
/// * 172.16.0.0/12    — RFC 1918 private
/// * 192.168.0.0/16   — RFC 1918 private
/// * 224.0.0.0/4      — multicast (RFC 5735)
/// * 240.0.0.0/4      — reserved for future use (RFC 5735)
///
/// The multicast / reserved ranges are included defensively — a webhook
/// dispatcher hitting a multicast address is always a bug.
fn classify_blocked_ipv4(ip: Ipv4Addr) -> Option<&'static str> {
    let [a, b, _c, _d] = ip.octets();
    if a == 0 {
        return Some("0.0.0.0/8 unspecified");
    }
    if a == 10 {
        return Some("10.0.0.0/8 private");
    }
    if a == 100 && (64..=127).contains(&b) {
        return Some("100.64.0.0/10 CGNAT");
    }
    if a == 127 {
        return Some("127.0.0.0/8 loopback");
    }
    if a == 169 && b == 254 {
        return Some("169.254.0.0/16 link-local / IMDS");
    }
    if a == 172 && (16..=31).contains(&b) {
        return Some("172.16.0.0/12 private");
    }
    if a == 192 && b == 168 {
        return Some("192.168.0.0/16 private");
    }
    if (224..=239).contains(&a) {
        return Some("224.0.0.0/4 multicast");
    }
    if (240..=255).contains(&a) {
        return Some("240.0.0.0/4 reserved");
    }
    None
}

/// IPv6 SSRF block list.
///
/// Ranges:
/// * ::1/128      — loopback
/// * ::/128       — unspecified
/// * fc00::/7     — unique local address (ULA, RFC 4193) — private
/// * fe80::/10    — link-local
/// * ff00::/8     — multicast
///
/// Also blocks IPv4-mapped IPv6 addresses when the embedded IPv4 is
/// blocked (e.g. `::ffff:127.0.0.1`), so the SSRF check is not bypassed
/// by switching address family.
fn classify_blocked_ipv6(ip: Ipv6Addr) -> Option<&'static str> {
    if ip.is_unspecified() {
        return Some("::/128 unspecified");
    }
    if ip.is_loopback() {
        return Some("::1/128 loopback");
    }
    // Embedded-IPv4 forms — if the embedded v4 is blocked, so is the
    // v6. `to_ipv4()` covers BOTH the IPv4-mapped form `::ffff:a.b.c.d`
    // and the (deprecated) IPv4-compatible form `::a.b.c.d`. We use
    // `to_ipv4` not `to_ipv4_mapped` because some network stacks still
    // honour the compatible form — an attacker writing
    // `https://[::127.0.0.1]/` MUST hit the same v4 block list as
    // `https://127.0.0.1/`. (Review: gemini/copilot, PR #533 round 1.)
    if let Some(v4) = ip.to_ipv4() {
        if let Some(reason) = classify_blocked_ipv4(v4) {
            return Some(reason);
        }
    }
    let segs = ip.segments();
    // fc00::/7 — first 7 bits are 1111110 → first byte is 0xfc or 0xfd.
    let first_byte = (segs[0] >> 8) as u8;
    if first_byte & 0xfe == 0xfc {
        return Some("fc00::/7 ULA / private");
    }
    // fe80::/10 — first 10 bits are 1111111010.
    if (segs[0] & 0xffc0) == 0xfe80 {
        return Some("fe80::/10 link-local");
    }
    // ff00::/8 — first byte is 0xff.
    if first_byte == 0xff {
        return Some("ff00::/8 multicast");
    }
    None
}

fn require_host(parsed: &url::Url, kind: &str) -> Result<(), String> {
    match parsed.host_str() {
        Some(h) if !h.is_empty() => Ok(()),
        _ => Err(format!(
            "{kind} target is missing a host (got '{parsed}')",
            parsed = parsed.as_str()
        )),
    }
}

fn validate_email_target(target: &str) -> Result<(), String> {
    // RFC 5321 is much stricter than we need; the goal here is to reject
    // typos like `alice` or `@example.com`, not to replicate a full
    // grammar. Require exactly one `@`, non-empty local part, and a
    // domain with at least one `.` and non-empty labels.
    let (local, domain) = target
        .split_once('@')
        .ok_or_else(|| format!("email target must contain exactly one '@' (got '{target}')"))?;
    // `split_once` splits on the first '@'; reject targets with extra '@'s.
    if domain.contains('@') {
        return Err(format!(
            "email target must contain exactly one '@' (got '{target}')"
        ));
    }
    if local.is_empty() {
        return Err("email target has empty local part (before '@')".to_owned());
    }
    if domain.is_empty() {
        return Err("email target has empty domain (after '@')".to_owned());
    }
    // Domain must have at least one dot, and no empty labels
    // (no leading/trailing dot, no `..`).
    if !domain.contains('.') {
        return Err(format!(
            "email target domain '{domain}' must contain a dot (e.g. example.com)"
        ));
    }
    if domain.starts_with('.') || domain.ends_with('.') || domain.contains("..") {
        return Err(format!("email target domain '{domain}' has empty labels"));
    }
    if target.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return Err("email target must not contain whitespace or control characters".to_owned());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ch(kind: &str, target: &str) -> NotificationChannel {
        NotificationChannel {
            kind: kind.to_owned(),
            target: target.to_owned(),
        }
    }

    /// Policy used by pre-existing tests: strict scheme + allow internal
    /// destinations. This preserves the behavior those tests were
    /// asserting (they predate #451/#452 and only cared about scheme
    /// semantics) without weakening the production defaults.
    fn scheme_only() -> WebhookValidationPolicy {
        WebhookValidationPolicy {
            allow_insecure_scheme: false,
            allow_internal_destinations: true,
        }
    }

    fn scheme_only_insecure() -> WebhookValidationPolicy {
        WebhookValidationPolicy {
            allow_insecure_scheme: true,
            allow_internal_destinations: true,
        }
    }

    // ── webhook / slack ──────────────────────────────────────────────────

    #[tokio::test]
    async fn webhook_accepts_https() {
        assert!(
            validate_channel(&ch("webhook", "https://example.com/hook"), scheme_only())
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn slack_accepts_https() {
        assert!(validate_channel(
            &ch("slack", "https://hooks.slack.com/services/T/B/XXX"),
            scheme_only(),
        )
        .await
        .is_ok());
    }

    #[tokio::test]
    async fn webhook_rejects_garbage() {
        let err = validate_channel(&ch("webhook", "not-a-url"), scheme_only())
            .await
            .unwrap_err();
        assert!(err.contains("not a valid URL"), "got: {err}");
    }

    #[tokio::test]
    async fn webhook_rejects_ftp() {
        let err = validate_channel(&ch("webhook", "ftp://example.com/path"), scheme_only())
            .await
            .unwrap_err();
        assert!(err.contains("unsupported scheme"), "got: {err}");
    }

    #[tokio::test]
    async fn webhook_rejects_javascript() {
        let err = validate_channel(&ch("webhook", "javascript:alert(1)"), scheme_only())
            .await
            .unwrap_err();
        // url crate parses this as scheme=javascript, path=alert(1), no host.
        // Either the scheme branch or the host branch is fine — both are
        // the right answer.
        assert!(
            err.contains("unsupported scheme") || err.contains("missing a host"),
            "got: {err}",
        );
    }

    #[tokio::test]
    async fn webhook_rejects_http_without_flag() {
        let err = validate_channel(&ch("webhook", "http://evil.com/hook"), scheme_only())
            .await
            .unwrap_err();
        assert!(err.contains("must use https"), "got: {err}");
    }

    #[tokio::test]
    async fn webhook_accepts_http_localhost_with_insecure_and_internal() {
        let policy = scheme_only_insecure();
        for host in ["localhost", "127.0.0.1", "[::1]"] {
            let url = format!("http://{host}:3000/hook");
            let res = validate_channel(&ch("webhook", &url), policy).await;
            assert!(res.is_ok(), "{url}: {res:?}");
        }
    }

    #[tokio::test]
    async fn webhook_rejects_http_non_localhost_even_with_flag() {
        // Back-compat with the pre-#451 semantics: even when both opt-ins
        // are live, `http://` may ONLY reach loopback. A public host
        // over http is a confused-deputy signal and stays a 422.
        let policy = scheme_only_insecure();
        let err = validate_channel(&ch("webhook", "http://evil.com/hook"), policy)
            .await
            .unwrap_err();
        assert!(err.contains("loopback"), "got: {err}");
    }

    #[tokio::test]
    async fn webhook_rejects_http_localhost_without_scheme_flag() {
        let err = validate_channel(&ch("webhook", "http://localhost:3000/hook"), scheme_only())
            .await
            .unwrap_err();
        assert!(err.contains("must use https"), "got: {err}");
    }

    #[tokio::test]
    async fn webhook_rejects_empty_target() {
        let err = validate_channel(&ch("webhook", "   "), scheme_only())
            .await
            .unwrap_err();
        assert!(err.contains("must not be empty"), "got: {err}");
    }

    #[tokio::test]
    async fn webhook_rejects_missing_host() {
        // A bare `https:` with no authority should fail.
        let err = validate_channel(&ch("webhook", "https:"), scheme_only())
            .await
            .unwrap_err();
        assert!(
            err.contains("missing a host") || err.contains("not a valid URL"),
            "got: {err}",
        );
    }

    // ── SSRF block list (#451, #452) ─────────────────────────────────────

    #[tokio::test]
    async fn webhook_rejects_imds_ipv4_literal_over_https() {
        // #451: https to 169.254.169.254 is the canonical IMDS-theft URL.
        // MUST 422 at registration regardless of scheme.
        let err = validate_channel(
            &ch("webhook", "https://169.254.169.254/latest/meta-data/"),
            WebhookValidationPolicy::strict(),
        )
        .await
        .unwrap_err();
        assert!(err.contains("169.254"), "got: {err}");
        assert!(
            err.contains("link-local") || err.contains("IMDS"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn webhook_rejects_imds_ipv4_literal_over_http_with_insecure_flag() {
        // Even with the scheme opt-in, the SSRF block list must fire.
        let policy = WebhookValidationPolicy {
            allow_insecure_scheme: true,
            allow_internal_destinations: false,
        };
        let err = validate_channel(
            &ch("webhook", "http://169.254.169.254/latest/meta-data/"),
            policy,
        )
        .await
        .unwrap_err();
        assert!(err.contains("169.254"), "got: {err}");
    }

    #[tokio::test]
    async fn webhook_rejects_rfc1918_10_block() {
        let err = validate_channel(
            &ch("webhook", "https://10.1.2.3/hook"),
            WebhookValidationPolicy::strict(),
        )
        .await
        .unwrap_err();
        assert!(err.contains("10.0.0.0/8"), "got: {err}");
    }

    #[tokio::test]
    async fn webhook_rejects_rfc1918_172_block() {
        for ip in ["172.16.0.1", "172.20.0.1", "172.31.255.254"] {
            let url = format!("https://{ip}/hook");
            let err = validate_channel(&ch("webhook", &url), WebhookValidationPolicy::strict())
                .await
                .unwrap_err();
            assert!(err.contains("172.16.0.0/12"), "got: {err} for {ip}");
        }
    }

    #[tokio::test]
    async fn webhook_172_block_allows_public_range_edges() {
        // 172.15.x.x and 172.32.x.x are public — must not trigger the block.
        for ip in ["172.15.0.1", "172.32.0.1"] {
            let url = format!("https://{ip}/hook");
            let res =
                validate_channel(&ch("webhook", &url), WebhookValidationPolicy::strict()).await;
            assert!(res.is_ok(), "{ip} should be allowed: {res:?}");
        }
    }

    #[tokio::test]
    async fn webhook_rejects_rfc1918_192_168_block() {
        let err = validate_channel(
            &ch("webhook", "https://192.168.1.1/hook"),
            WebhookValidationPolicy::strict(),
        )
        .await
        .unwrap_err();
        assert!(err.contains("192.168.0.0/16"), "got: {err}");
    }

    #[tokio::test]
    async fn webhook_rejects_https_loopback_literal() {
        // #452: https://127.0.0.1 must 422 even with default strict
        // policy (the old code only rejected http-loopback).
        let err = validate_channel(
            &ch("webhook", "https://127.0.0.1:8080/hook"),
            WebhookValidationPolicy::strict(),
        )
        .await
        .unwrap_err();
        assert!(err.contains("127.0.0.0/8"), "got: {err}");
    }

    #[tokio::test]
    async fn webhook_rejects_https_loopback_ipv6_literal() {
        let err = validate_channel(
            &ch("webhook", "https://[::1]/hook"),
            WebhookValidationPolicy::strict(),
        )
        .await
        .unwrap_err();
        assert!(err.contains("loopback"), "got: {err}");
    }

    #[tokio::test]
    async fn webhook_rejects_ipv6_ula() {
        let err = validate_channel(
            &ch("webhook", "https://[fc00::1]/hook"),
            WebhookValidationPolicy::strict(),
        )
        .await
        .unwrap_err();
        assert!(err.contains("fc00::/7"), "got: {err}");
    }

    #[tokio::test]
    async fn webhook_rejects_ipv6_link_local() {
        let err = validate_channel(
            &ch("webhook", "https://[fe80::1]/hook"),
            WebhookValidationPolicy::strict(),
        )
        .await
        .unwrap_err();
        assert!(err.contains("fe80::/10"), "got: {err}");
    }

    #[tokio::test]
    async fn webhook_rejects_cgnat() {
        // 100.64-127 is CGNAT — an attacker on a shared network might
        // target another tenant's infra over it.
        let err = validate_channel(
            &ch("webhook", "https://100.64.0.1/hook"),
            WebhookValidationPolicy::strict(),
        )
        .await
        .unwrap_err();
        assert!(err.contains("CGNAT"), "got: {err}");
    }

    #[tokio::test]
    async fn webhook_rejects_unspecified() {
        let err = validate_channel(
            &ch("webhook", "https://0.0.0.0/hook"),
            WebhookValidationPolicy::strict(),
        )
        .await
        .unwrap_err();
        assert!(err.contains("unspecified"), "got: {err}");
    }

    #[tokio::test]
    async fn webhook_rejects_ipv4_mapped_ipv6_loopback() {
        // `::ffff:127.0.0.1` — the v4-mapped form of 127.0.0.1. Must not
        // bypass the v4 block list.
        let err = validate_channel(
            &ch("webhook", "https://[::ffff:127.0.0.1]/hook"),
            WebhookValidationPolicy::strict(),
        )
        .await
        .unwrap_err();
        assert!(err.contains("loopback"), "got: {err}");
    }

    #[tokio::test]
    async fn webhook_rejects_ipv4_compatible_ipv6_loopback() {
        // `::127.0.0.1` — the deprecated v4-compatible form. Some
        // network stacks still honour it, so it must classify against
        // the same v4 block list as `127.0.0.1`. Regression test added
        // in response to PR #533 review (gemini/copilot).
        let err = validate_channel(
            &ch("webhook", "https://[::127.0.0.1]/hook"),
            WebhookValidationPolicy::strict(),
        )
        .await
        .unwrap_err();
        assert!(err.contains("loopback"), "got: {err}");
    }

    #[tokio::test]
    async fn webhook_rejects_ipv4_compatible_ipv6_imds() {
        // `::169.254.169.254` — v4-compatible form of the AWS IMDS v4
        // literal. Must hit the link-local block.
        let err = validate_channel(
            &ch("webhook", "https://[::169.254.169.254]/hook"),
            WebhookValidationPolicy::strict(),
        )
        .await
        .unwrap_err();
        assert!(
            err.contains("169.254") || err.contains("link-local"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn webhook_accepts_public_ip_literal() {
        // 93.184.216.34 is example.com — public IP. Must pass.
        assert!(validate_channel(
            &ch("webhook", "https://93.184.216.34/hook"),
            WebhookValidationPolicy::strict(),
        )
        .await
        .is_ok());
    }

    #[tokio::test]
    async fn webhook_internal_flag_allows_loopback_ipv4() {
        let policy = WebhookValidationPolicy {
            allow_insecure_scheme: true,
            allow_internal_destinations: true,
        };
        assert!(
            validate_channel(&ch("webhook", "http://127.0.0.1:9999/hook"), policy)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn webhook_insecure_scheme_alone_does_not_disable_ssrf_block_list_for_https() {
        // Regression test against the PR #533 review finding: earlier
        // revision had `from_config` OR `allow_internal_destinations`
        // with `allow_insecure_scheme`, which re-opened #451 for any
        // deployment with `CAIRN_ALLOW_INSECURE_WEBHOOKS=1`. With the
        // fix, `allow_insecure_scheme` alone keeps the https SSRF
        // gate on — IMDS target must still 422.
        let policy = WebhookValidationPolicy {
            allow_insecure_scheme: true,
            allow_internal_destinations: false,
        };
        let err = validate_channel(
            &ch("webhook", "https://169.254.169.254/latest/meta-data/"),
            policy,
        )
        .await
        .unwrap_err();
        assert!(err.contains("169.254"), "got: {err}");
    }

    #[tokio::test]
    async fn webhook_internal_flag_allows_link_local() {
        // With the opt-in, even 169.254 is allowed — operators
        // explicitly accept the risk.
        let policy = WebhookValidationPolicy {
            allow_insecure_scheme: false,
            allow_internal_destinations: true,
        };
        assert!(
            validate_channel(&ch("webhook", "https://169.254.169.254/hook"), policy,)
                .await
                .is_ok()
        );
    }

    // ── email ────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn email_accepts_normal() {
        assert!(
            validate_channel(&ch("email", "alice@example.com"), scheme_only())
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn email_rejects_no_at() {
        let err = validate_channel(&ch("email", "alice-example.com"), scheme_only())
            .await
            .unwrap_err();
        assert!(err.contains("exactly one '@'"), "got: {err}");
    }

    #[tokio::test]
    async fn email_rejects_no_domain_dot() {
        let err = validate_channel(&ch("email", "alice@localhost"), scheme_only())
            .await
            .unwrap_err();
        assert!(err.contains("must contain a dot"), "got: {err}");
    }

    #[tokio::test]
    async fn email_rejects_empty_local() {
        let err = validate_channel(&ch("email", "@example.com"), scheme_only())
            .await
            .unwrap_err();
        assert!(err.contains("empty local part"), "got: {err}");
    }

    #[tokio::test]
    async fn email_rejects_empty_domain() {
        let err = validate_channel(&ch("email", "alice@"), scheme_only())
            .await
            .unwrap_err();
        assert!(err.contains("empty domain"), "got: {err}");
    }

    #[tokio::test]
    async fn email_rejects_double_at() {
        let err = validate_channel(&ch("email", "a@b@c.com"), scheme_only())
            .await
            .unwrap_err();
        assert!(err.contains("exactly one '@'"), "got: {err}");
    }

    #[tokio::test]
    async fn email_rejects_whitespace() {
        let err = validate_channel(&ch("email", "alice @example.com"), scheme_only())
            .await
            .unwrap_err();
        assert!(err.contains("whitespace"), "got: {err}");
    }

    // ── unknown kinds pass through ───────────────────────────────────────

    #[tokio::test]
    async fn unknown_kind_accepts_any_target() {
        assert!(
            validate_channel(&ch("pagerduty", "arbitrary-key"), scheme_only())
                .await
                .is_ok()
        );
        assert!(validate_channel(&ch("telegram", "@chat_id"), scheme_only())
            .await
            .is_ok());
    }

    // ── batch helper ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn validate_channels_reports_index() {
        let channels = vec![
            ch("webhook", "https://ok.example.com/hook"),
            ch("webhook", "not-a-url"),
        ];
        let err = validate_channels(&channels, scheme_only())
            .await
            .unwrap_err();
        assert!(err.starts_with("channels[1]:"), "got: {err}");
    }

    #[tokio::test]
    async fn validate_channels_empty_is_ok() {
        assert!(validate_channels(&[], scheme_only()).await.is_ok());
    }

    // ── dispatch-time guard ──────────────────────────────────────────────

    #[tokio::test]
    async fn enforce_outbound_blocks_imds() {
        // Dispatch-time re-check against DNS rebinding. Same block list
        // as registration.
        let err = enforce_outbound_webhook_target(
            "https://169.254.169.254/creds",
            WebhookValidationPolicy::strict(),
        )
        .await
        .unwrap_err();
        assert!(err.contains("169.254"), "got: {err}");
    }

    #[tokio::test]
    async fn enforce_outbound_allows_public() {
        assert!(enforce_outbound_webhook_target(
            "https://example.com/hook",
            WebhookValidationPolicy::strict(),
        )
        .await
        .is_ok());
    }
}
