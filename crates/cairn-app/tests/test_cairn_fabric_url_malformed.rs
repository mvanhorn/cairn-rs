//! PR-A (backend-config-url): when `CAIRN_FABRIC_URL` is set to a
//! malformed string, cairn-app must exit non-zero with a clear
//! operator-facing error — not panic, not a stack trace, not a
//! cryptic `url::ParseError` debug-print.
//!
//! This is the malformed-URL negative path identified by audit #410:
//! PR-A shipped positive-boot coverage (`valkey://` + default) but
//! left the parse-error surface unexercised end-to-end. A future
//! regression to `parse_fabric_url`'s error formatting (e.g. swapping
//! `?` for `unwrap` on the `url::Url::parse` call) would have
//! panicked the subprocess before this test was added.
//!
//! # Why log inspection (not /health/ready)
//!
//! The subprocess exits during `AppState::new`, before any HTTP
//! listener binds. We scan stderr for the verbatim error string
//! (`FabricConfig::from_env failed: CAIRN_FABRIC_URL is not a valid
//! URL`) and verify the exit code is non-zero. Matching the message
//! tightly means a refactor that changes the error wording to a
//! cryptic form will trip this test.

mod support;

use std::process::Stdio;

use support::fabric_url_subprocess::{
    cairn_app_command, restore_fabric_env, scan_stderr, scan_with_timeout, scrub_fabric_env,
    wait_exit_within_grace,
};

#[tokio::test]
async fn malformed_cairn_fabric_url_exits_non_zero_with_clear_error() {
    // Scrub any inherited `CAIRN_FABRIC_*` from the outer env so this
    // test's intent can't be overridden by a stale dev-shell export
    // (Gemini review comment on PR #553).
    let prev_env = scrub_fabric_env();

    let mut cmd = cairn_app_command("malformed-url-test", "malformed");
    cmd
        // Intentionally malformed — url::Url::parse rejects "not a url"
        // because no scheme is present.
        .env("CAIRN_FABRIC_URL", "not a url")
        // The eprintln! fatal line in main.rs writes to stderr.
        .stdout(Stdio::null())
        .stderr(Stdio::piped());

    let mut child = cmd
        .spawn()
        .expect("failed to spawn cairn-app — did cargo build it?");

    let stderr = child.stderr.take().expect("piped stderr present");

    let (matched, transcript) = scan_with_timeout(
        &mut child,
        scan_stderr(stderr, |line| {
            // Match the stable substring operators see. The full error
            // is `FabricConfig::from_env failed: CAIRN_FABRIC_URL is
            // not a valid URL: <url::ParseError debug>` — we pin on
            // the two stable fragments so a `url` crate upgrade that
            // tweaks the Debug form of its ParseError doesn't silently
            // break this test.
            line.contains("FabricConfig::from_env failed")
                && line.contains("CAIRN_FABRIC_URL is not a valid URL")
        }),
        "parse-error stderr scan",
    )
    .await;

    // Subprocess must have exited on its own — not from our kill.
    // `wait()` here resolves promptly because the exit path in
    // main.rs is `std::process::exit(1)` on AppState::new failure.
    let status = wait_exit_within_grace(&mut child, &transcript).await;

    restore_fabric_env(prev_env);

    assert!(
        matched,
        "subprocess did not surface `CAIRN_FABRIC_URL is not a valid URL` on stderr — \
         parse-error reporting regressed.\n\
         ---- stderr transcript ----\n{transcript}\n---- end ----"
    );
    assert!(
        !status.success(),
        "subprocess exited 0 despite malformed CAIRN_FABRIC_URL — must exit non-zero.\n\
         ---- stderr transcript ----\n{transcript}\n---- end ----"
    );
}
