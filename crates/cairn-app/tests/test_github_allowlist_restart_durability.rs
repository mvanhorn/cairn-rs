//! Integration test for #556: repo allowlist persists across cairn-app
//! restart via the plugin-layer `JsonFileAllowlistStore`.
//!
//! Flow:
//!   1. Spawn cairn-app with GitHub env vars + `CAIRN_PLUGIN_STATE_DIR`.
//!   2. Add a repo to the allowlist via `POST /v1/projects/.../repos`.
//!   3. SIGKILL the subprocess (simulating a crash).
//!   4. Restart with the same `CAIRN_PLUGIN_STATE_DIR`.
//!   5. Assert the repo is still on the allowlist via
//!      `GET /v1/projects/.../repos`.
//!
//! Without the #556 plugin-layer persistence, step 5 returned an empty
//! list — the in-memory `ProjectRepoAccessService` rebuilt itself from
//! scratch on every boot.

mod support;

use std::fs;
use std::path::PathBuf;

use serde_json::{json, Value};
use support::live_fabric::LiveHarness;
use tempfile::TempDir;

/// Minimal valid RSA key for the GitHub plugin's `AppCredentials::new`
/// validation. Never used for signing a real JWT — the test only
/// exercises the allowlist persistence path, not any outbound GitHub
/// API call.
///
/// Reuses the pre-existing `cairn-integrations` test fixture rather
/// than inlining a PEM blob here, both to stay DRY and to keep
/// secret-scanners from flagging a second copy. The fixture is
/// test-only, embedded via `include_bytes!`, and never shipped to any
/// binary artifact.
const TEST_RSA_KEY_PEM: &[u8] =
    include_bytes!("../../cairn-integrations/tests/fixtures/test_rsa_key.pem");

/// Write the test RSA key to `dir/test_rsa_key.pem` and return its path.
fn write_test_rsa_key(dir: &std::path::Path) -> PathBuf {
    let path = dir.join("test_rsa_key.pem");
    fs::write(&path, TEST_RSA_KEY_PEM).expect("write test RSA key");
    path
}

/// URL-encode the `tenant/workspace/project` triple into one Axum path
/// segment.
fn project_path(h: &LiveHarness) -> String {
    format!("{}%2F{}%2F{}", h.tenant, h.workspace, h.project)
}

#[tokio::test]
async fn github_repo_allowlist_persists_across_restart() {
    // Persistent directories shared by the "before restart" and
    // "after restart" cairn-app subprocesses.
    let plugin_state_dir = TempDir::new().expect("plugin state temp dir");
    let gh_key_dir = TempDir::new().expect("github key temp dir");
    let gh_key_path = write_test_rsa_key(gh_key_dir.path());

    let gh_key_path_str = gh_key_path
        .to_str()
        .expect("github key path must be utf-8")
        .to_owned();
    let plugin_dir_str = plugin_state_dir
        .path()
        .to_str()
        .expect("plugin dir path must be utf-8")
        .to_owned();

    // Same env vars used on both the initial spawn and `restart()`.
    let extra_env = [
        ("GITHUB_APP_ID", "12345"),
        ("GITHUB_PRIVATE_KEY_FILE", gh_key_path_str.as_str()),
        ("GITHUB_WEBHOOK_SECRET", "unused-but-required"),
        ("CAIRN_PLUGIN_STATE_DIR", plugin_dir_str.as_str()),
    ];

    let mut h = LiveHarness::setup_with_env(&extra_env).await;
    let p = project_path(&h);
    let base = h.base_url.clone();

    // 1. Before restart — attach a repo.
    let res = h
        .client()
        .post(format!("{base}/v1/projects/{p}/repos"))
        .bearer_auth(&h.admin_token)
        .json(&json!({ "repo_id": "avifenesh/cairn-rs" }))
        .send()
        .await
        .expect("attach reaches server");
    assert_eq!(
        res.status().as_u16(),
        200,
        "attach status, body: {}",
        res.text().await.unwrap_or_default(),
    );

    // Verify the file was written by the plugin — this is the
    // distinguishing signal between "persistence installed" and
    // "persistence silently dropped on the floor".
    let expected_file = plugin_state_dir
        .path()
        .join("github")
        .join("allowlist.json");
    assert!(
        expected_file.exists(),
        "plugin-layer allowlist file must exist at {} after attach",
        expected_file.display(),
    );
    let file_bytes = fs::read(&expected_file).expect("allowlist file readable");
    let parsed: Value = serde_json::from_slice(&file_bytes).expect("allowlist file parseable");
    assert_eq!(
        parsed.pointer("/version").and_then(|v| v.as_u64()),
        Some(1),
        "on-disk allowlist schema version must be 1, got {parsed}"
    );

    // 2. SIGKILL + restart — simulates a crashed cairn-app coming back
    // up. The new process reads the same `CAIRN_PLUGIN_STATE_DIR` and
    // must surface the repo that was added pre-crash.
    h.sigkill_and_restart()
        .await
        .expect("sigkill + restart must succeed");

    // 3. GET /repos must still see the repo after restart.
    let res = h
        .client()
        .get(format!("{base}/v1/projects/{p}/repos"))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("post-restart list reaches server");
    assert_eq!(res.status().as_u16(), 200);
    let body: Value = res.json().await.expect("post-restart list json");
    let repos = body
        .get("repos")
        .and_then(|v| v.as_array())
        .expect("response must have repos array");
    assert_eq!(
        repos.len(),
        1,
        "expected exactly one repo on allowlist post-restart, got {body}"
    );
    assert_eq!(
        repos[0].get("repo_id").and_then(|v| v.as_str()),
        Some("avifenesh/cairn-rs"),
        "expected 'avifenesh/cairn-rs' on allowlist post-restart, got {body}"
    );
}

#[tokio::test]
async fn github_repo_allowlist_revoke_persists_across_restart() {
    // Companion test to the grant-survives test: a revoke must also
    // persist. If we only persisted grants, a revoked repo would zombie-
    // reappear on restart.

    let plugin_state_dir = TempDir::new().expect("plugin state temp dir");
    let gh_key_dir = TempDir::new().expect("github key temp dir");
    let gh_key_path = write_test_rsa_key(gh_key_dir.path());

    let gh_key_path_str = gh_key_path.to_str().unwrap().to_owned();
    let plugin_dir_str = plugin_state_dir.path().to_str().unwrap().to_owned();

    let extra_env = [
        ("GITHUB_APP_ID", "12345"),
        ("GITHUB_PRIVATE_KEY_FILE", gh_key_path_str.as_str()),
        ("GITHUB_WEBHOOK_SECRET", "unused-but-required"),
        ("CAIRN_PLUGIN_STATE_DIR", plugin_dir_str.as_str()),
    ];

    let mut h = LiveHarness::setup_with_env(&extra_env).await;
    let p = project_path(&h);
    let base = h.base_url.clone();

    // Attach + revoke before restart.
    h.client()
        .post(format!("{base}/v1/projects/{p}/repos"))
        .bearer_auth(&h.admin_token)
        .json(&json!({ "repo_id": "avifenesh/cairn-rs" }))
        .send()
        .await
        .expect("attach")
        .error_for_status()
        .expect("attach 2xx");

    let res = h
        .client()
        .delete(format!("{base}/v1/projects/{p}/repos/avifenesh/cairn-rs"))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("revoke reaches server");
    assert_eq!(res.status().as_u16(), 204, "revoke must 204");

    // Restart.
    h.sigkill_and_restart()
        .await
        .expect("sigkill + restart must succeed");

    // GET /repos must be empty — the revoke survived.
    let res = h
        .client()
        .get(format!("{base}/v1/projects/{p}/repos"))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("post-restart list reaches server");
    assert_eq!(res.status().as_u16(), 200);
    let body: Value = res.json().await.expect("post-restart list json");
    assert_eq!(
        body.get("repos")
            .and_then(|v| v.as_array())
            .map(|a| a.len()),
        Some(0),
        "expected empty allowlist after revoke + restart, got {body}",
    );
}
