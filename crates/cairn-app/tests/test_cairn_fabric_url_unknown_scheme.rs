//! PR-A (backend-config-url): an unsupported scheme on
//! `CAIRN_FABRIC_URL` (most commonly `redis://`, which PR-A
//! intentionally rejected in favour of explicit `valkey://` /
//! `rediss://`) must produce the documented `unknown fabric URL
//! scheme` operator-facing error — not a query-param complaint, not
//! a crash, not a cryptic default.
//!
//! Closes the third of the three negative-path gaps called out by
//! audit #410. The positive-path coverage that PR-A shipped
//! (`valkey://` boots, default-URL falls back to localhost) left this
//! footgun untested: a regression to `parse_fabric_url`'s scheme match
//! arm (e.g. accidentally accepting `redis://` as an alias, or falling
//! through to a generic parser-error path) would have shipped silently
//! before this test was added.
//!
//! # Why log inspection (not /health/ready)
//!
//! Same shape as `test_cairn_fabric_url_malformed.rs`: the subprocess
//! exits during `AppState::new`, before any HTTP listener binds. The
//! `unknown fabric URL scheme: redis; expected one of: valkey, rediss`
//! error surfaces on stderr via `eprintln!` in `main.rs`. We match the
//! stable fragments (`unknown fabric URL scheme` + the scheme name
//! `redis`) and assert non-zero exit.

mod support;

use std::process::Stdio;

use support::fabric_url_subprocess::{
    cairn_app_command, restore_fabric_env, scan_stderr, scan_with_timeout, scrub_fabric_env,
    wait_exit_within_grace,
};

#[tokio::test]
async fn redis_scheme_rejected_with_clear_error() {
    let prev_env = scrub_fabric_env();

    let mut cmd = cairn_app_command("redis-scheme-test", "redis-scheme");
    cmd
        // `redis://` is NOT an alias — PR-A explicitly rejected it in
        // favour of `valkey://` / `rediss://`. The boot must fail
        // loud with the documented scheme-list error.
        .env("CAIRN_FABRIC_URL", "redis://localhost:6379")
        .stdout(Stdio::null())
        .stderr(Stdio::piped());

    let mut child = cmd
        .spawn()
        .expect("failed to spawn cairn-app — did cargo build it?");

    let stderr = child.stderr.take().expect("piped stderr present");

    let (matched, transcript) = scan_with_timeout(
        &mut child,
        scan_stderr(stderr, |line| {
            // The full error from main.rs is:
            //   FabricConfig::from_env failed: unknown fabric URL \
            //   scheme: redis; expected one of: valkey, rediss
            // We pin on the stable documented phrase + the concrete
            // scheme name we passed, so a reword that loses either cue
            // trips the test.
            line.contains("unknown fabric URL scheme") && line.contains("redis")
        }),
        "unknown-scheme stderr scan",
    )
    .await;

    let status = wait_exit_within_grace(&mut child, &transcript).await;

    restore_fabric_env(prev_env);

    assert!(
        matched,
        "subprocess did not surface `unknown fabric URL scheme: redis` on stderr — \
         scheme-rejection error reporting regressed.\n\
         ---- stderr transcript ----\n{transcript}\n---- end ----"
    );
    assert!(
        !status.success(),
        "subprocess exited 0 despite unsupported scheme — must exit non-zero.\n\
         ---- stderr transcript ----\n{transcript}\n---- end ----"
    );
}
