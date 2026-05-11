//! Boot-time scrub of operator-environment credential variables.
//!
//! # The bug this prevents
//!
//! `cairn-app` is launched from an interactive shell that may export
//! credential env vars for operator convenience (`GH_TOKEN`,
//! `GITHUB_TOKEN`, `AWS_*`, etc.). Those env vars propagate into
//! `cairn-app`'s process environment, and from there into every
//! `bash` subprocess the harness-tools layer spawns for sub-agents.
//! Sub-agents then make tool calls (`gh`, `aws`, …) that
//! authenticate against operator-private credentials they were
//! never meant to see, OR worse — see an **invalid** token that
//! shadows the host's valid keychain credentials and breaks every
//! tool call until the agent gives up.
//!
//! R19 dogfood (2026-05-08, issue #773): `cairn-app` inherited a
//! stale `GH_TOKEN=ghp_...` from the operator's shell. `gh` CLI
//! prefers `GH_TOKEN` over `~/.config/gh/hosts.yml`. The token was
//! invalid; every `gh auth status` returned 401; the executor
//! sub-agent looped 71 iterations on `unset GH_TOKEN; gh auth status`
//! without ever writing a file. `unset` works inside one bash
//! invocation but doesn't persist — the next `bash(...)` tool call
//! spawns a new subprocess and `GH_TOKEN` is back. The wedge is
//! structural and only fixable by removing the env var at the
//! cairn-app process layer.
//!
//! # What this module does
//!
//! At boot time, [`scrub_credential_env_vars`] walks an allowlist
//! of well-known credential env vars and removes them from
//! `std::env` (via `std::env::remove_var`). Subsequent
//! `Command::new(...)` calls in `cairn-harness-tools` (which is
//! where bash subprocess spawning lives) inherit cairn-app's
//! environment, so the scrub at the parent process layer covers
//! every spawned bash without modifying upstream `harness-bash`.
//!
//! Operator override: `CAIRN_INHERIT_OPERATOR_ENV=1` (truthy)
//! disables the scrub. Use this only for local dev where the
//! host's env vars are intentionally trusted.
//!
//! Sub-agents that genuinely need a credential get it through
//! cairn's credential service (POST /v1/admin/tenants/.../credentials
//! → connection → bash-tool reads it via the credential manager,
//! NOT via inherited operator env). That path is unaffected by this
//! scrub — the credentials live in cairn's own store, not in
//! `std::env`.
//!
//! # Out of scope (deferred)
//!
//! Per-spawn env scrub at the `harness-bash` layer. The upstream
//! `harness-bash` crate spawns subprocesses with the full inherited
//! env. A defense-in-depth follow-up would intercept that and
//! filter the same allowlist on each spawn. The dogfood wedge is
//! closed by the boot-time scrub alone — even if some other code
//! `set_var`s a credential after boot, it would only affect the
//! cairn-app process's own behaviour, not the spawned sub-agents
//! (which inherit from cairn-app's env at spawn time, snapshotted
//! after this scrub runs).

/// Allowlist of credential env vars to scrub at boot. Each entry is
/// the **exact** env var name (case-sensitive on Unix). The list
/// covers the major credential vendors operators commonly export:
///
/// - GitHub: `GH_TOKEN`, `GITHUB_TOKEN`
/// - AWS: every `AWS_*` (handled via prefix match below — listed
///   here for documentation only)
/// - Azure: every `AZURE_*` (prefix match)
/// - Google Cloud: `GOOGLE_APPLICATION_CREDENTIALS`,
///   `GOOGLE_API_KEY`
/// - Anthropic / OpenAI / etc.: `ANTHROPIC_API_KEY`, `OPENAI_API_KEY`
/// - Generic vendor patterns: `*_API_KEY`, `*_TOKEN`, `*_SECRET`
///   are covered by the suffix match below
///
/// A literal-name match catches the common case quickly. Prefix /
/// suffix matches catch the long tail without listing every
/// vendor by hand.
const LITERAL_CREDENTIAL_VARS: &[&str] = &[
    "GH_TOKEN",
    "GITHUB_TOKEN",
    "GOOGLE_APPLICATION_CREDENTIALS",
    "GOOGLE_API_KEY",
    "ANTHROPIC_API_KEY",
    "OPENAI_API_KEY",
    "OPENROUTER_API_KEY",
    "ZAI_API_KEY",
    "GROQ_API_KEY",
    "MINIMAX_API_KEY",
    "DEEPSEEK_API_KEY",
    "XAI_API_KEY",
    "MISTRAL_API_KEY",
    "BEDROCK_ACCESS_KEY",
    "VERTEX_AI_KEY",
];

/// Prefix matches — catches multi-key vendor families.
const CREDENTIAL_PREFIXES: &[&str] = &["AWS_", "AZURE_", "GCP_"];

/// Suffix matches — catches generic patterns like `FOO_API_KEY`,
/// `BAR_SECRET`, `BAZ_TOKEN`. Excludes a few well-known
/// false-positives (e.g. `RUST_LOG` is not a credential).
const CREDENTIAL_SUFFIXES: &[&str] = &[
    "_API_KEY",
    "_API_TOKEN",
    "_SECRET",
    "_SECRET_KEY",
    "_ACCESS_KEY",
    "_ACCESS_TOKEN",
];

/// Env vars cairn-app itself uses for legitimate operator
/// configuration. These MUST NOT be scrubbed even if their name
/// matches a suffix pattern. Add entries here as cairn introduces
/// new operator-facing env vars that look credential-shaped.
///
/// The GITHUB_* entries here are read by cairn-app's GitHub-plugin
/// (cairn-integrations + cairn-github) at boot to authenticate as
/// a GitHub App. They are operator-configured cairn inputs —
/// analogous to CAIRN_ADMIN_TOKEN — not credentials cairn-app
/// should hide from itself. The integration test
/// github_repo_allowlist_persists_across_restart (caught by CI on
/// PR #783) regresses without this exemption: the scrub removes
/// GITHUB_WEBHOOK_SECRET (matches the _SECRET suffix) and the
/// plugin then refuses to attach repos.
const ALLOWLIST_NEVER_SCRUB: &[&str] = &[
    // cairn's own admin token — read by `bin_state` / middleware,
    // intentionally inherited from the operator's shell.
    "CAIRN_ADMIN_TOKEN",
    // cairn's credential-manager master key — read by the
    // credential service at boot.
    "CAIRN_CREDENTIAL_KEY",
    "CAIRN_CREDENTIAL_KEY_FILE",
    // Waitpoint HMAC secret — read by FabricServices at boot.
    "CAIRN_FABRIC_WAITPOINT_HMAC_SECRET",
    // GitHub App credentials — read by cairn-github / the plugin
    // host at boot for inbound webhook verification + outbound
    // installation-token minting. Operators wire these via
    // systemd / env / docker-compose; cairn-app needs them in its
    // own process env, and they do NOT leak into sub-agent bash
    // (sub-agents authenticate to GitHub via `gh` CLI's hosts.yml
    // or via cairn's credential service, not via these vars).
    "GITHUB_APP_ID",
    "GITHUB_PRIVATE_KEY",
    "GITHUB_PRIVATE_KEY_FILE",
    "GITHUB_WEBHOOK_SECRET",
    "GITHUB_INSTALLATION_ID",
];

/// Env var that disables the scrub. When set to a truthy value
/// (`1`, `true`, case-insensitive), all variables stay in
/// `std::env` and operator-environment leakage into sub-agents is
/// the operator's responsibility.
pub const INHERIT_OVERRIDE_ENV_VAR: &str = "CAIRN_INHERIT_OPERATOR_ENV";

/// Outcome of a single boot-time scrub. Returned for logging /
/// testing visibility — operators see in cairn-app's startup log
/// exactly which vars were removed (by name; values are NOT logged
/// even partially because they are credentials).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScrubReport {
    /// Names of env vars that were present at scrub time and got
    /// removed. Order is the iteration order of `std::env::vars()`
    /// (effectively unordered — sort if you need a stable view).
    pub removed: Vec<String>,
    /// `true` when `CAIRN_INHERIT_OPERATOR_ENV` was set truthy and
    /// the scrub was skipped.
    pub skipped_via_override: bool,
}

/// Decide whether `name` matches the credential allowlist (and is
/// not on the never-scrub allowlist).
fn is_credential_var(name: &str) -> bool {
    if ALLOWLIST_NEVER_SCRUB.contains(&name) {
        return false;
    }
    if LITERAL_CREDENTIAL_VARS.contains(&name) {
        return true;
    }
    if CREDENTIAL_PREFIXES.iter().any(|p| name.starts_with(p)) {
        return true;
    }
    if CREDENTIAL_SUFFIXES.iter().any(|s| name.ends_with(s)) {
        return true;
    }
    false
}

/// Truthiness test for the override env var. Accepts `1`, `true`,
/// `yes`, `on` (case-insensitive, whitespace-trimmed). Empty /
/// whitespace-only / unset is `false`.
///
/// Per Gemini review on PR #783: handle case + whitespace via a
/// single normalized comparison rather than the previous match-arm
/// alternation. Caching via `OnceLock` was suggested too but
/// declined — the function runs **exactly once** per process at
/// boot (one syscall total), and caching breaks the unit tests'
/// per-case state (they alternate override on/off across tests via
/// `EnvGuard` Drop). The single-syscall path is the right tradeoff.
fn is_override_set() -> bool {
    std::env::var(INHERIT_OVERRIDE_ENV_VAR)
        .ok()
        .map(|v| v.trim().to_ascii_lowercase())
        .filter(|v| !v.is_empty())
        .map(|v| matches!(v.as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false)
}

/// Walk `std::env::vars()`, identify credential vars, and remove
/// them. Returns a [`ScrubReport`] describing what was removed.
///
/// This MUST be called before any subprocess spawn — typically at
/// the very top of `real_main` after `dotenvy::dotenv()`. Earlier
/// is safer; later means the window between cairn-app start and
/// scrub is one in which a leaked sub-agent could still see the
/// credential.
///
/// # Safety
///
/// `std::env::remove_var` is unsafe in multi-threaded programs
/// because env mutation is racy with concurrent `getenv` calls.
/// The boot-time call is single-threaded by definition (we're at
/// the top of `real_main` before tokio multi-threaded runtime
/// spawns), so this is safe in the documented call site.
pub fn scrub_credential_env_vars() -> ScrubReport {
    if is_override_set() {
        return ScrubReport {
            removed: Vec::new(),
            skipped_via_override: true,
        };
    }

    // Collect names FIRST, then remove. Mutating `std::env` while
    // iterating over its snapshot is undefined behaviour on some
    // platforms (the vars() iterator caches lazily on Linux, but
    // we don't rely on that).
    //
    // Per Gemini review on PR #783: use `vars_os()` not `vars()`.
    // `vars()` panics on env entries with non-UTF8 keys or values
    // (rare but possible: locale-broken sysadmins, exotic CI
    // images). For a security-critical boot-time scrub, panicking
    // means cairn-app refuses to start at all — worse than a
    // best-effort scrub of the UTF-8 subset. Filter out non-UTF8
    // keys silently (their names cannot match our ASCII-only
    // credential allowlist anyway, so the security guarantee is
    // unaffected for the cases we care about).
    let to_remove: Vec<String> = std::env::vars_os()
        .filter_map(|(k, _v)| k.into_string().ok())
        .filter(|k| is_credential_var(k))
        .collect();

    for name in &to_remove {
        // SAFETY: called at boot before any threads are spawned;
        // see function-level rustdoc.
        std::env::remove_var(name);
    }

    ScrubReport {
        removed: to_remove,
        skipped_via_override: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Env mutation is process-global; serialize tests that touch
    /// it. `Mutex` not `tokio::Mutex` because we're sync.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    /// Restore env state at scope exit so tests don't leak.
    struct EnvGuard {
        saved: Vec<(String, Option<String>)>,
    }
    impl EnvGuard {
        fn new(names: &[&str]) -> Self {
            let saved = names
                .iter()
                .map(|n| (n.to_string(), std::env::var(*n).ok()))
                .collect();
            Self { saved }
        }
    }
    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (k, v) in &self.saved {
                match v {
                    Some(v) => std::env::set_var(k, v),
                    None => std::env::remove_var(k),
                }
            }
        }
    }

    #[test]
    fn classifier_recognises_literal_credentials() {
        assert!(is_credential_var("GH_TOKEN"));
        assert!(is_credential_var("GITHUB_TOKEN"));
        assert!(is_credential_var("OPENAI_API_KEY"));
        assert!(is_credential_var("ZAI_API_KEY"));
    }

    #[test]
    fn classifier_recognises_prefixes() {
        assert!(is_credential_var("AWS_ACCESS_KEY_ID"));
        assert!(is_credential_var("AWS_SECRET_ACCESS_KEY"));
        assert!(is_credential_var("AWS_SESSION_TOKEN"));
        assert!(is_credential_var("AZURE_CLIENT_ID"));
        assert!(is_credential_var("GCP_SERVICE_ACCOUNT"));
    }

    #[test]
    fn classifier_recognises_suffixes() {
        assert!(is_credential_var("MYVENDOR_API_KEY"));
        assert!(is_credential_var("FOO_SECRET"));
        assert!(is_credential_var("BAR_ACCESS_TOKEN"));
    }

    #[test]
    fn classifier_excludes_unrelated_vars() {
        assert!(!is_credential_var("PATH"));
        assert!(!is_credential_var("HOME"));
        assert!(!is_credential_var("RUST_LOG"));
        assert!(!is_credential_var("TERM"));
        assert!(!is_credential_var("USER"));
        // CAIRN-side admin token is intentionally inherited.
        assert!(!is_credential_var("CAIRN_ADMIN_TOKEN"));
        assert!(!is_credential_var("CAIRN_CREDENTIAL_KEY"));
        assert!(!is_credential_var("CAIRN_FABRIC_WAITPOINT_HMAC_SECRET"));
    }

    #[test]
    fn classifier_preserves_github_plugin_vars() {
        // Regression guard for PR #783 CI failure:
        // `github_repo_allowlist_persists_across_restart` broke when
        // the suffix patterns scrubbed `GITHUB_WEBHOOK_SECRET` (matches
        // `_SECRET`). These vars are operator-configured cairn-app
        // inputs — analogous to CAIRN_ADMIN_TOKEN — not credentials
        // cairn should hide from itself.
        assert!(!is_credential_var("GITHUB_APP_ID"));
        assert!(!is_credential_var("GITHUB_PRIVATE_KEY"));
        assert!(!is_credential_var("GITHUB_PRIVATE_KEY_FILE"));
        assert!(!is_credential_var("GITHUB_WEBHOOK_SECRET"));
        assert!(!is_credential_var("GITHUB_INSTALLATION_ID"));
        // `GITHUB_TOKEN` is still a credential — it's the operator-
        // shell PAT that shadows hosts.yml. Keep on the scrub list.
        assert!(is_credential_var("GITHUB_TOKEN"));
    }

    #[test]
    fn override_handles_case_and_whitespace() {
        // Gemini review on PR #783: case-insensitive + trimmed.
        let _lock = ENV_LOCK.lock().unwrap();
        let _guard = EnvGuard::new(&[INHERIT_OVERRIDE_ENV_VAR]);
        for (val, expected) in [
            ("1", true),
            ("true", true),
            ("TRUE", true),
            ("True", true),
            ("yes", true),
            ("YES", true),
            ("on", true),
            ("ON", true),
            (" 1 ", true),
            ("\ttrue\n", true),
            // false-shaped values
            ("0", false),
            ("false", false),
            ("no", false),
            ("off", false),
            ("", false),
            ("   ", false),
            ("anything-else", false),
        ] {
            std::env::set_var(INHERIT_OVERRIDE_ENV_VAR, val);
            assert_eq!(
                is_override_set(),
                expected,
                "is_override_set({val:?}) expected {expected}, got {}",
                is_override_set(),
            );
        }
    }

    #[test]
    fn classifier_excludes_never_scrub_even_if_pattern_matches() {
        // Regression guard: if someone adds CAIRN_FOO_SECRET as a
        // legitimate operator-config var in the future, they MUST
        // also add it to ALLOWLIST_NEVER_SCRUB to avoid losing it
        // at boot. This test pins the current never-scrub set.
        for name in ALLOWLIST_NEVER_SCRUB {
            assert!(
                !is_credential_var(name),
                "{name} is on the never-scrub allowlist but classifier returned true — \
                 ALLOWLIST_NEVER_SCRUB is bypassed"
            );
        }
    }

    #[test]
    fn scrub_removes_credentials_when_override_unset() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _guard = EnvGuard::new(&[
            "GH_TOKEN",
            "AWS_ACCESS_KEY_ID",
            "OPENAI_API_KEY",
            "PATH",
            INHERIT_OVERRIDE_ENV_VAR,
        ]);
        // Ensure override is OFF.
        std::env::remove_var(INHERIT_OVERRIDE_ENV_VAR);
        // Set credentials we expect to be scrubbed.
        std::env::set_var("GH_TOKEN", "ghp_test_aaaaaaaaaa");
        std::env::set_var("AWS_ACCESS_KEY_ID", "AKIA_TEST_BBBB");
        std::env::set_var("OPENAI_API_KEY", "sk-test-cccc");

        let report = scrub_credential_env_vars();

        assert!(!report.skipped_via_override);
        assert!(report.removed.contains(&"GH_TOKEN".to_string()));
        assert!(report.removed.contains(&"AWS_ACCESS_KEY_ID".to_string()));
        assert!(report.removed.contains(&"OPENAI_API_KEY".to_string()));
        // The vars must actually be gone.
        assert!(std::env::var("GH_TOKEN").is_err());
        assert!(std::env::var("AWS_ACCESS_KEY_ID").is_err());
        assert!(std::env::var("OPENAI_API_KEY").is_err());
        // PATH must be untouched.
        assert!(std::env::var("PATH").is_ok());
    }

    #[test]
    fn scrub_skipped_when_override_truthy() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _guard = EnvGuard::new(&["GH_TOKEN", INHERIT_OVERRIDE_ENV_VAR]);
        std::env::set_var(INHERIT_OVERRIDE_ENV_VAR, "1");
        std::env::set_var("GH_TOKEN", "ghp_test_dddddddddd");

        let report = scrub_credential_env_vars();

        assert!(report.skipped_via_override);
        assert!(report.removed.is_empty());
        // Var stays.
        assert_eq!(std::env::var("GH_TOKEN").unwrap(), "ghp_test_dddddddddd");
    }

    #[test]
    fn scrub_preserves_cairn_admin_token() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _guard = EnvGuard::new(&["CAIRN_ADMIN_TOKEN", "GH_TOKEN", INHERIT_OVERRIDE_ENV_VAR]);
        std::env::remove_var(INHERIT_OVERRIDE_ENV_VAR);
        std::env::set_var("CAIRN_ADMIN_TOKEN", "dev-admin-token");
        std::env::set_var("GH_TOKEN", "ghp_test_eeee");

        let report = scrub_credential_env_vars();

        // GH_TOKEN scrubbed.
        assert!(report.removed.contains(&"GH_TOKEN".to_string()));
        // CAIRN_ADMIN_TOKEN preserved — cairn-app reads it for auth
        // middleware. Operators can rotate it at runtime via
        // POST /v1/admin/rotate-token; losing it at boot would
        // refuse every authenticated request.
        assert!(!report.removed.contains(&"CAIRN_ADMIN_TOKEN".to_string()));
        assert_eq!(
            std::env::var("CAIRN_ADMIN_TOKEN").unwrap(),
            "dev-admin-token"
        );
    }
}
