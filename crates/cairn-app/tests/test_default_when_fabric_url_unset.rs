//! PR-A (backend-config-url): when `CAIRN_FABRIC_URL` is unset,
//! cairn-app must fall back to the default `valkey://localhost:6379`
//! endpoint. This is an integration-level sanity check on top of the
//! `default_config_from_env_when_url_unset` unit test in
//! `cairn-fabric/src/config.rs`.
//!
//! # Why log inspection (not /health/ready)
//!
//! The test asserts the cairn-app subprocess emits the
//! `host=localhost port=6379` line from `FabricRuntime::start`'s
//! `tracing::info!` — that proves the URL parser + env-var handling
//! fed the correct default into the runtime config. The subprocess
//! is killed the moment the line is observed, so the test is
//! deterministic whether or not a reachable Valkey on
//! `localhost:6379` exists on the host (CI usually doesn't have one;
//! some dev machines do — see the Copilot review on PR #356).

mod support;

use std::process::Stdio;

use support::fabric_url_subprocess::{
    cairn_app_command, restore_fabric_env, scan_stdout, scan_with_timeout, scrub_fabric_env,
};

#[tokio::test]
async fn default_to_localhost_6379_when_cairn_fabric_url_unset() {
    // Scrub inherited CAIRN_FABRIC_* so the default-path arm fires
    // regardless of dev-shell state.
    let prev_env = scrub_fabric_env();

    let mut cmd = cairn_app_command("default-test", "default");
    cmd.stdout(Stdio::piped()).stderr(Stdio::null());

    let mut child = cmd
        .spawn()
        .expect("failed to spawn cairn-app — did cargo build it?");

    let stdout = child.stdout.take().expect("piped stdout present");

    // Read stdout line-by-line and short-circuit the moment we see
    // the `connecting to valkey host=localhost port=6379` tracing
    // event. Killing the subprocess immediately afterwards keeps the
    // test deterministic regardless of whether a reachable Valkey on
    // localhost:6379 lets cairn-app boot all the way to steady state
    // (in which case stdout would never reach EOF and a drain-until-
    // EOF would hang until the test timeout).
    let (matched, transcript) = scan_with_timeout(
        &mut child,
        scan_stdout(stdout, |stripped| {
            stripped.contains("connecting to valkey")
                && stripped.contains("host=localhost")
                && stripped.contains("port=6379")
        }),
        "default-URL stdout scan",
    )
    .await;

    let _ = child.kill().await;
    let _ = child.wait().await;

    restore_fabric_env(prev_env);

    assert!(
        matched,
        "subprocess did not surface `host=localhost port=6379` on the \
         `connecting to valkey` line — default fallback regressed.\n\
         ---- subprocess stdout ----\n{transcript}\n---- end ----"
    );
}
