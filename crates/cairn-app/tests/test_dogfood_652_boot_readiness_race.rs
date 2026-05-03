//! Issue #652 — boot-time readiness race.
//!
//! Dogfood round 2 reported that PUT/POST on JSON endpoints returned
//! 422 `Failed to deserialize the JSON body into the target type: missing
//! field <X>` immediately after cairn-app printed
//! `readiness: /health/ready now returns 200`. The same body, same URL,
//! seconds later returned 200/201.
//!
//! Root cause (pre-fix): `mark_ready()` was spawned into an independent
//! `tokio::spawn` task alongside the awaited-inline `axum::serve(...)`.
//! On a multi-thread runtime the spawned task could run and flip the
//! atomic BEFORE the serve future was first polled — so operator boot
//! automation polling `/health/ready` could observe 200 while the axum
//! accept loop was still cold, leaving the first request stuck in the
//! kernel listen backlog long enough for the request-handling pipeline
//! to fail extractor hydration on partial state.
//!
//! Fix (see `main.rs`): spawn `axum::serve` first, yield the scheduler
//! twice to guarantee the serve task has been polled (i.e. the accept
//! loop is running), and only then call `mark_ready()`. This test
//! guards that ordering: from the moment `/health/ready` returns 200,
//! a subsequent PUT with a well-formed body must NOT 422 with
//! `missing field`.

mod support;

use reqwest::StatusCode;
use serde_json::json;
use std::process::Stdio;
use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::time::{sleep, timeout};

/// The `generate_model` key accepts any non-empty, length-capped
/// string id. `gpt-4o-mini` is a stable LiteLLM catalog entry; using
/// it here keeps the test self-documenting even though the validator
/// no longer consults the catalog at PUT time (#656 moved that check
/// to orchestrate time).
const MODEL_KEY: &str = "generate_model";
const MODEL_VALUE: &str = "gpt-4o-mini";

/// How long to wait for the startup banner. Cold CI boots around 10s.
const BANNER_TIMEOUT: Duration = Duration::from_secs(30);
/// How long to wait for `/health/ready` to return 200 once the banner
/// has been observed. Generous for CI; production boots flip readiness
/// within a few hundred milliseconds of the accept loop starting.
const READY_TIMEOUT: Duration = Duration::from_secs(15);

/// Raw subprocess harness — like `LiveHarness` but intentionally does
/// NOT rotate the admin token. Token rotation hits a non-health admin
/// endpoint, which would mask the race window by forcing a
/// successful POST to complete before the test's own PUT fires. We
/// want the test to hit the very first state-mutating request after
/// readiness flips.
struct SeedTokenHarness {
    base_url: String,
    admin_token: String,
    client: reqwest::Client,
    _child: Child,
}

impl SeedTokenHarness {
    async fn setup() -> Self {
        let (valkey_host, valkey_port) = cairn_fabric::test_harness::valkey_endpoint().await;

        let suffix = uuid::Uuid::new_v4().simple().to_string()[..8].to_owned();
        // Pad out the seed token to 16+ chars so the service-token
        // registry's min-length check in `new_service_token_registry`
        // doesn't refuse it on boot.
        let seed_admin = format!("seed-admin-{suffix}-padding");

        let bin = env!("CARGO_BIN_EXE_cairn-app");
        let mut cmd = Command::new(bin);
        cmd.arg("--mode")
            .arg("team")
            .arg("--port")
            .arg("0")
            .arg("--addr")
            .arg("127.0.0.1")
            .arg("--db")
            .arg("memory")
            .arg("--allow-missing-sandbox-primitives")
            .env(
                "CAIRN_FABRIC_URL",
                format!("valkey://{valkey_host}:{valkey_port}"),
            )
            .env("CAIRN_FABRIC_LANE", format!("test-{suffix}"))
            .env("CAIRN_FABRIC_WORKER_ID", format!("worker-{suffix}"))
            .env("CAIRN_FABRIC_INSTANCE_ID", format!("instance-{suffix}"))
            .env("CAIRN_ADMIN_TOKEN", &seed_admin)
            .env(
                "CAIRN_FABRIC_WAITPOINT_HMAC_SECRET",
                "00000000000000000000000000000000000000000000000000000000000000aa",
            )
            .env("CAIRN_FABRIC_WAITPOINT_HMAC_KID", "cairn-test-k1")
            .env(
                "CAIRN_CREDENTIAL_KEY",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            )
            .env("RUST_LOG", "warn,cairn_app=info")
            .env_remove("CAIRN_LOG_DIR")
            .env_remove("CAIRN_TEST_STARTUP_DELAY_MS")
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        let mut child = cmd
            .spawn()
            .expect("failed to spawn cairn-app binary — did cargo build it?");

        let stderr = child.stderr.take().expect("piped stderr present");
        let bound_url = timeout(BANNER_TIMEOUT, wait_for_listening(stderr))
            .await
            .expect("cairn-app did not print listening banner within timeout")
            .expect("cairn-app exited before printing listening banner");

        let base_url = bound_url.replace("0.0.0.0", "127.0.0.1");
        // Short per-request timeout so a race-failure mode that eats
        // the connection can't masquerade as a test hang.
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("reqwest client builds");

        Self {
            base_url,
            admin_token: seed_admin,
            client,
            _child: child,
        }
    }

    /// Poll `/health/ready` until it returns 200, OR bail with the last
    /// non-2xx status. Mirrors what the dogfood operator's shell does.
    async fn wait_for_ready(&self) -> StatusCode {
        let deadline = Instant::now() + READY_TIMEOUT;
        let mut last_status = StatusCode::SERVICE_UNAVAILABLE;
        while Instant::now() < deadline {
            let res = match self
                .client
                .get(format!("{}/health/ready", self.base_url))
                .send()
                .await
            {
                Ok(r) => r,
                Err(_) => {
                    sleep(Duration::from_millis(50)).await;
                    continue;
                }
            };
            last_status = res.status();
            if last_status == StatusCode::OK {
                return StatusCode::OK;
            }
            sleep(Duration::from_millis(50)).await;
        }
        panic!(
            "`/health/ready` never returned 200 within {READY_TIMEOUT:?}; \
             last status {last_status}"
        );
    }
}

async fn wait_for_listening(stderr: tokio::process::ChildStderr) -> Option<String> {
    let mut lines = BufReader::new(stderr).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        if let Some(rest) = line.strip_prefix("cairn-app listening on ") {
            let url = rest.trim().to_owned();
            tokio::spawn(async move { while let Ok(Some(_line)) = lines.next_line().await {} });
            return Some(url);
        }
    }
    None
}

/// The regression guard. Mirrors the dogfood user's shell-level repro:
///   1. Boot cairn-app (team mode, in-memory db).
///   2. Poll `/health/ready` until 200.
///   3. Immediately (no intermediate request) fire a PUT with a valid
///      body against `PUT /v1/settings/defaults/system/system/generate_model`.
///   4. Assert the response is 200 OK, NOT 422 with `missing field`.
///
/// Before the fix: observable 422 with `missing field 'value'` on some
/// fraction of cold boots (race). After the fix: deterministic 200.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_request_after_readiness_is_not_422_missing_field() {
    let h = SeedTokenHarness::setup().await;
    assert_eq!(h.wait_for_ready().await, StatusCode::OK);

    let url = format!(
        "{}/v1/settings/defaults/system/system/{}",
        h.base_url, MODEL_KEY,
    );
    let res = h
        .client
        .put(&url)
        .bearer_auth(&h.admin_token)
        .json(&json!({ "value": MODEL_VALUE }))
        .send()
        .await
        .expect("PUT /v1/settings/defaults reaches server");

    let status = res.status();
    let body = res.text().await.unwrap_or_default();

    // The exact regression shape: 422 + "missing field" in body. Even if
    // a future refactor changed the 422 wording, catching the MOMENT
    // the body is dropped/empty protects the contract.
    assert!(
        !(status == StatusCode::UNPROCESSABLE_ENTITY && body.contains("missing field")),
        "#652 regression: first PUT after /health/ready = 200 returned \
         422 with `missing field` — the request body was silently dropped \
         by the middleware/extractor chain. status={status} body={body}"
    );

    assert_eq!(
        status,
        StatusCode::OK,
        "expected 200 OK on first PUT after readiness; got {status} body={body}"
    );
}

/// A tighter repro: fire three PUTs back-to-back-to-back in the same
/// cold boot, with no delay between them. If the race is intermittent
/// — only some cold boots 422 — a single-shot test might flake-pass;
/// firing multiple requests increases the chance of hitting the
/// window deterministically. Any `missing field` 422 in any of the
/// three is treated as a regression.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn three_back_to_back_puts_after_readiness_all_succeed() {
    let h = SeedTokenHarness::setup().await;
    assert_eq!(h.wait_for_ready().await, StatusCode::OK);

    // Three different keys so the handler doesn't short-circuit on a
    // duplicate-row optimization in the defaults service. All are in
    // MODEL_ID_KEYS (see `handlers/health.rs`); after #656 they accept
    // any non-empty, length-capped string — the catalog/connection
    // check moved to orchestrate time.
    for key in &["generate_model", "brain_model", "stream_model"] {
        let url = format!("{}/v1/settings/defaults/system/system/{}", h.base_url, key);
        let res = h
            .client
            .put(&url)
            .bearer_auth(&h.admin_token)
            .json(&json!({ "value": MODEL_VALUE }))
            .send()
            .await
            .unwrap_or_else(|e| panic!("PUT {key} reaches server: {e}"));

        let status = res.status();
        let body = res.text().await.unwrap_or_default();
        assert!(
            !(status == StatusCode::UNPROCESSABLE_ENTITY && body.contains("missing field")),
            "#652 regression on key={key}: 422 with `missing field`. body={body}"
        );
        assert_eq!(
            status,
            StatusCode::OK,
            "key={key}: expected 200 OK; got {status} body={body}"
        );
    }
}
