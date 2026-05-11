//! PR-A (backend-config-url): the `rediss://` scheme must surface as
//! `tls=true` on the `connecting to valkey` tracing line, proving the
//! scheme parser correctly propagates TLS state into the
//! `ValkeyConnection` that cairn-app hands to ferriskey.
//!
//! `valkey://` (plain) and `rediss://` (TLS) are the only two schemes
//! PR-A accepts, so pinning the TLS-flag log on the `rediss://` path
//! is the minimum coverage for "scheme equivalence under url-config"
//! per audit #410. A regression that lost the `rediss` → `vk.tls = true`
//! mapping would surface here as the log line printing `tls=false`
//! despite the URL scheme.
//!
//! # Why log inspection (not /health/ready)
//!
//! We don't need a reachable TLS-enabled Valkey for this test — the
//! log line fires inside `FabricRuntime::start` before the first
//! connection attempt (see `crates/cairn-fabric/src/boot.rs` —
//! `tracing::info!(tls = vk.tls, …, "connecting to valkey")`).
//! Killing the subprocess the moment we see the line keeps the test
//! deterministic without depending on a TLS Valkey fixture.

mod support;

use std::process::Stdio;

use support::fabric_url_subprocess::{
    cairn_app_command, restore_fabric_env, scan_stdout, scan_with_timeout, scrub_fabric_env,
};

#[tokio::test]
async fn rediss_scheme_propagates_tls_true_into_valkey_connection() {
    let prev_env = scrub_fabric_env();

    let mut cmd = cairn_app_command("rediss-test", "rediss");
    cmd
        // `rediss://` implies TLS. We use a host:port the subprocess
        // will never reach (invalid hostname shape is fine — the log
        // line fires before any TCP attempt). Value is stable and
        // unlikely to resolve to anything real.
        .env(
            "CAIRN_FABRIC_URL",
            "rediss://tls-probe.invalid.cairn-test:16379",
        )
        .stdout(Stdio::piped())
        .stderr(Stdio::null());

    let mut child = cmd
        .spawn()
        .expect("failed to spawn cairn-app — did cargo build it?");

    let stdout = child.stdout.take().expect("piped stdout present");

    let (matched, transcript) = scan_with_timeout(
        &mut child,
        scan_stdout(stdout, |stripped| {
            // Host comes through verbatim — it's just the
            // `url::Url::host_str`. Port is 16379 because we set it
            // explicitly above. `tls=true` is the assertion under
            // test.
            stripped.contains("connecting to valkey")
                && stripped.contains("host=tls-probe.invalid.cairn-test")
                && stripped.contains("port=16379")
                && stripped.contains("tls=true")
        }),
        "rediss-TLS stdout scan",
    )
    .await;

    let _ = child.kill().await;
    let _ = child.wait().await;

    restore_fabric_env(prev_env);

    assert!(
        matched,
        "subprocess did not surface `tls=true` on `connecting to valkey` line — \
         rediss scheme → TLS mapping regressed.\n\
         ---- subprocess stdout ----\n{transcript}\n---- end ----"
    );
}
