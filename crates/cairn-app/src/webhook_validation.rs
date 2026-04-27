//! Notification-channel target validation (#235).
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
//!   URL. Scheme is `https://` by default; `http://` is allowed only when
//!   both
//!   1. the operator opts in via `CAIRN_ALLOW_INSECURE_WEBHOOKS=1` **or**
//!      the server is running in `DeploymentMode::Local`, **and**
//!   2. the host is a loopback address (`localhost`, `127.0.0.1`, `::1`).
//!
//!   Any other scheme (`ftp://`, `file://`, `javascript:…`, etc.) is rejected.
//! * `kind = email` — basic RFC 5321 shape check: non-empty local part, a
//!   single `@`, and a non-empty domain with a dot. No deep DNS / MX
//!   validation (that's a delivery-time concern).
//! * Other kinds (`pagerduty`, `telegram`, …) are accepted without URL
//!   validation — the UI sends chat IDs / tokens that aren't URLs. If
//!   scheme checks are wanted for those later, add them per-kind here.
//!
//! Returns `Ok(())` on success or `Err(String)` with a message that is safe
//! to show the operator verbatim in the 422 body.

use std::sync::OnceLock;

use cairn_api::bootstrap::{BootstrapConfig, DeploymentMode};
use cairn_domain::notification_prefs::NotificationChannel;

/// Env var that opts a team-mode deployment into accepting `http://localhost`
/// webhook targets. Production clusters should leave this unset so the
/// handler insists on `https://`.
pub const ALLOW_INSECURE_WEBHOOKS_ENV: &str = "CAIRN_ALLOW_INSECURE_WEBHOOKS";

/// Cached result of `CAIRN_ALLOW_INSECURE_WEBHOOKS`. The env var is read at
/// most once per process; subsequent calls to [`insecure_webhooks_allowed`]
/// hit this cache instead of the libc `getenv` lock. Env vars are process-
/// level static for the lifetime of the server, so caching is safe.
static INSECURE_WEBHOOKS_ENV_CACHE: OnceLock<bool> = OnceLock::new();

fn insecure_webhooks_env_set() -> bool {
    *INSECURE_WEBHOOKS_ENV_CACHE.get_or_init(|| match std::env::var(ALLOW_INSECURE_WEBHOOKS_ENV) {
        Ok(v) => matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => false,
    })
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

/// Validate a single notification channel. See module docs for the rule set.
pub fn validate_channel(channel: &NotificationChannel, allow_insecure: bool) -> Result<(), String> {
    let target = channel.target.trim();
    if target.is_empty() {
        return Err(format!(
            "channel target must not be empty (kind = {})",
            channel.kind
        ));
    }
    match channel.kind.as_str() {
        "webhook" | "slack" => validate_url_target(&channel.kind, target, allow_insecure),
        "email" => validate_email_target(target),
        // Unknown / future kinds round-trip unchanged — the delivery layer
        // is responsible for rejecting targets it can't dispatch.
        _ => Ok(()),
    }
}

/// Validate every channel in a request. Returns the first failure (callers
/// surface it as a 422). Index is included in the error so the UI can point
/// at the offending row.
pub fn validate_channels(
    channels: &[NotificationChannel],
    allow_insecure: bool,
) -> Result<(), String> {
    for (idx, ch) in channels.iter().enumerate() {
        if let Err(msg) = validate_channel(ch, allow_insecure) {
            return Err(format!("channels[{idx}]: {msg}"));
        }
    }
    Ok(())
}

fn validate_url_target(kind: &str, target: &str, allow_insecure: bool) -> Result<(), String> {
    let parsed = url::Url::parse(target).map_err(|e| {
        format!(
            "{kind} target is not a valid URL: {e} (expected https://… with \
             an absolute host, e.g. https://hooks.example.com/path)"
        )
    })?;

    match parsed.scheme() {
        "https" => {
            require_host(&parsed, kind)?;
            Ok(())
        }
        "http" => {
            if !allow_insecure {
                return Err(format!(
                    "{kind} target must use https:// (got http://). Set \
                     CAIRN_ALLOW_INSECURE_WEBHOOKS=1 to allow http://localhost \
                     in dev."
                ));
            }
            require_host(&parsed, kind)?;
            if !is_loopback_host(&parsed) {
                return Err(format!(
                    "{kind} target http:// is only permitted for loopback \
                     hosts (localhost, 127.0.0.1, ::1); got {host}",
                    host = parsed.host_str().unwrap_or("<none>")
                ));
            }
            Ok(())
        }
        other => Err(format!(
            "{kind} target has unsupported scheme '{other}:'; only https:// \
             (or http://localhost in dev) is allowed"
        )),
    }
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

fn is_loopback_host(parsed: &url::Url) -> bool {
    match parsed.host() {
        Some(url::Host::Domain(d)) => d.eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        None => false,
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

    // ── webhook / slack ──────────────────────────────────────────────────

    #[test]
    fn webhook_accepts_https() {
        assert!(validate_channel(&ch("webhook", "https://example.com/hook"), false).is_ok());
    }

    #[test]
    fn slack_accepts_https() {
        assert!(validate_channel(
            &ch("slack", "https://hooks.slack.com/services/T/B/XXX"),
            false,
        )
        .is_ok());
    }

    #[test]
    fn webhook_rejects_garbage() {
        let err = validate_channel(&ch("webhook", "not-a-url"), false).unwrap_err();
        assert!(err.contains("not a valid URL"), "got: {err}");
    }

    #[test]
    fn webhook_rejects_ftp() {
        let err = validate_channel(&ch("webhook", "ftp://example.com/path"), false).unwrap_err();
        assert!(err.contains("unsupported scheme"), "got: {err}");
    }

    #[test]
    fn webhook_rejects_javascript() {
        let err = validate_channel(&ch("webhook", "javascript:alert(1)"), false).unwrap_err();
        // url crate parses this as scheme=javascript, path=alert(1), no host.
        // Either the scheme branch or the host branch is fine — both are
        // the right answer.
        assert!(
            err.contains("unsupported scheme") || err.contains("missing a host"),
            "got: {err}",
        );
    }

    #[test]
    fn webhook_rejects_http_without_flag() {
        let err = validate_channel(&ch("webhook", "http://evil.com/hook"), false).unwrap_err();
        assert!(err.contains("must use https"), "got: {err}");
    }

    #[test]
    fn webhook_rejects_http_non_localhost_even_with_flag() {
        let err = validate_channel(&ch("webhook", "http://evil.com/hook"), true).unwrap_err();
        assert!(err.contains("loopback"), "got: {err}");
    }

    #[test]
    fn webhook_accepts_http_localhost_with_flag() {
        for host in ["localhost", "127.0.0.1", "[::1]"] {
            let url = format!("http://{host}:3000/hook");
            let res = validate_channel(&ch("webhook", &url), true);
            assert!(res.is_ok(), "{url}: {res:?}");
        }
    }

    #[test]
    fn webhook_rejects_http_localhost_without_flag() {
        let err =
            validate_channel(&ch("webhook", "http://localhost:3000/hook"), false).unwrap_err();
        assert!(err.contains("must use https"), "got: {err}");
    }

    #[test]
    fn webhook_rejects_empty_target() {
        let err = validate_channel(&ch("webhook", "   "), false).unwrap_err();
        assert!(err.contains("must not be empty"), "got: {err}");
    }

    #[test]
    fn webhook_rejects_missing_host() {
        // A bare `https:` with no authority should fail.
        let err = validate_channel(&ch("webhook", "https:"), false).unwrap_err();
        assert!(
            err.contains("missing a host") || err.contains("not a valid URL"),
            "got: {err}",
        );
    }

    // ── email ────────────────────────────────────────────────────────────

    #[test]
    fn email_accepts_normal() {
        assert!(validate_channel(&ch("email", "alice@example.com"), false).is_ok());
    }

    #[test]
    fn email_rejects_no_at() {
        let err = validate_channel(&ch("email", "alice-example.com"), false).unwrap_err();
        assert!(err.contains("exactly one '@'"), "got: {err}");
    }

    #[test]
    fn email_rejects_no_domain_dot() {
        let err = validate_channel(&ch("email", "alice@localhost"), false).unwrap_err();
        assert!(err.contains("must contain a dot"), "got: {err}");
    }

    #[test]
    fn email_rejects_empty_local() {
        let err = validate_channel(&ch("email", "@example.com"), false).unwrap_err();
        assert!(err.contains("empty local part"), "got: {err}");
    }

    #[test]
    fn email_rejects_empty_domain() {
        let err = validate_channel(&ch("email", "alice@"), false).unwrap_err();
        assert!(err.contains("empty domain"), "got: {err}");
    }

    #[test]
    fn email_rejects_double_at() {
        let err = validate_channel(&ch("email", "a@b@c.com"), false).unwrap_err();
        assert!(err.contains("exactly one '@'"), "got: {err}");
    }

    #[test]
    fn email_rejects_whitespace() {
        let err = validate_channel(&ch("email", "alice @example.com"), false).unwrap_err();
        assert!(err.contains("whitespace"), "got: {err}");
    }

    // ── unknown kinds pass through ───────────────────────────────────────

    #[test]
    fn unknown_kind_accepts_any_target() {
        assert!(validate_channel(&ch("pagerduty", "arbitrary-key"), false).is_ok());
        assert!(validate_channel(&ch("telegram", "@chat_id"), false).is_ok());
    }

    // ── batch helper ─────────────────────────────────────────────────────

    #[test]
    fn validate_channels_reports_index() {
        let channels = vec![
            ch("webhook", "https://ok.example.com/hook"),
            ch("webhook", "not-a-url"),
        ];
        let err = validate_channels(&channels, false).unwrap_err();
        assert!(err.starts_with("channels[1]:"), "got: {err}");
    }

    #[test]
    fn validate_channels_empty_is_ok() {
        assert!(validate_channels(&[], false).is_ok());
    }
}
