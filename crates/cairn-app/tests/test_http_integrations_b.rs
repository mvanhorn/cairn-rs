//! PR BL-b — integration surface (verify-installation + local_fs).
//!
//! Covers two end-to-end contracts the new Integrations/ProjectRepos UX
//! depends on:
//!
//! 1. `POST /v1/integrations/github/verify-installation` surfaces a 502
//!    `github_api_error` when the pasted credentials don't match a real
//!    GitHub App. The endpoint never mutates server state, so we can
//!    assert behaviour without network access: the synthetic PEM we
//!    generate is valid RSA (passes `jsonwebtoken` parsing) but the
//!    app_id/installation_id aren't registered upstream, so GitHub
//!    rejects the JWT and we bubble that up as 502.
//!
//! 2. `POST /v1/integrations` with the new `local_fs` provider type
//!    registers successfully, lists back with the expected shape, and
//!    accepts a second attach via
//!    `POST /v1/projects/:project/repos { host: "local_fs" }` which
//!    then appears in the merged repo list with `host == "local_fs"`.

mod support;

use serde_json::{json, Value};
use support::live_fabric::LiveHarness;

fn project_path(h: &LiveHarness) -> String {
    format!("{}%2F{}%2F{}", h.tenant, h.workspace, h.project)
}

/// Generate a throw-away PKCS#1 RSA key at test-runtime via the
/// `openssl` CLI so we never commit key material (GitGuardian would
/// flag it, rightly). The public half is not registered with GitHub,
/// so any API call using this key will 401 — which is the branch
/// `verify_github_installation_rejects_unregistered_app` exercises.
///
/// Returns `None` when openssl isn't available (minimal containers,
/// etc.) so the caller can gracefully skip the test instead of
/// panicking.
fn generate_test_pem() -> Option<String> {
    let out = std::process::Command::new("openssl")
        .args(["genrsa", "-traditional", "2048"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    String::from_utf8(out.stdout).ok()
}

#[tokio::test]
async fn verify_github_installation_rejects_unregistered_app() {
    let Some(pem) = generate_test_pem() else {
        eprintln!("skipping: openssl CLI unavailable");
        return;
    };
    let h = LiveHarness::setup().await;

    let res = h
        .client()
        .post(format!(
            "{}/v1/integrations/github/verify-installation",
            h.base_url
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "app_id": 999_999_999u64,
            "private_key": pem,
            "installation_id": 987_654_321u64,
        }))
        .send()
        .await
        .expect("verify-installation reaches server");

    // Without network access the request fails at reqwest layer; with
    // network it fails at GitHub with 401. Either way the handler
    // surfaces 502 `github_api_error`. The 400 case would only fire
    // if we'd sent a malformed PEM, which we don't.
    let status = res.status().as_u16();
    let body_text = res.text().await.unwrap_or_default();
    assert_eq!(
        status, 502,
        "expected 502 github_api_error, got {status} / {body_text}",
    );
    assert!(
        body_text.contains("github_api_error"),
        "expected github_api_error in body, got {body_text}",
    );
}

#[tokio::test]
async fn verify_github_installation_rejects_empty_private_key() {
    let h = LiveHarness::setup().await;

    let res = h
        .client()
        .post(format!(
            "{}/v1/integrations/github/verify-installation",
            h.base_url
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "app_id": 1u64,
            "private_key": "",
            "installation_id": 1u64,
        }))
        .send()
        .await
        .expect("verify-installation reaches server");

    assert_eq!(res.status().as_u16(), 400);
}

#[tokio::test]
async fn verify_github_installation_rejects_garbage_pem() {
    let h = LiveHarness::setup().await;

    let res = h
        .client()
        .post(format!(
            "{}/v1/integrations/github/verify-installation",
            h.base_url
        ))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "app_id": 1u64,
            "private_key": "not a real PEM",
            "installation_id": 1u64,
        }))
        .send()
        .await
        .expect("verify-installation reaches server");

    assert_eq!(res.status().as_u16(), 400);
}

#[tokio::test]
async fn local_fs_integration_registers_and_lists() {
    // The integrations registration entry point shares the same
    // `CAIRN_LOCAL_FS_BASE` jail as the per-project allowlist (see
    // `LocalFsPlugin::new` and PR #721 expansion: the registration
    // path was the sibling site codex's first patch missed).
    let base = tempfile::tempdir().expect("base tempdir");
    let base_dir = base.path().to_string_lossy().into_owned();
    let h = LiveHarness::setup_with_env(&[("CAIRN_LOCAL_FS_BASE", base_dir.as_str())]).await;

    // Create a real directory on disk inside the fence so the
    // plugin's path check passes. tempfile gives us one that
    // auto-cleans.
    let inside = tempfile::tempdir_in(base.path()).expect("inside tempdir");
    let path = inside.path().to_string_lossy().into_owned();

    let res = h
        .client()
        .post(format!("{}/v1/integrations", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "id": "local-fs-test",
            "type": "local_fs",
            "config": {
                "path": path,
                "display_name": "Test LocalFS",
            },
        }))
        .send()
        .await
        .expect("register reaches server");

    assert_eq!(
        res.status().as_u16(),
        200,
        "register status, body: {}",
        res.text().await.unwrap_or_default(),
    );

    // GET back — should list the new integration with configured=true.
    let res = h
        .client()
        .get(format!("{}/v1/integrations/local-fs-test", h.base_url))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("get reaches server");
    assert_eq!(res.status().as_u16(), 200);
    let body: Value = res.json().await.expect("get json");
    assert_eq!(
        body.get("id").and_then(|v| v.as_str()),
        Some("local-fs-test")
    );
    assert_eq!(
        body.get("display_name").and_then(|v| v.as_str()),
        Some("Test LocalFS"),
    );
    assert_eq!(body.get("configured").and_then(|v| v.as_bool()), Some(true));
}

/// Regression test for PR #721 expansion: the integrations entry
/// point (`POST /v1/integrations` with `type=local_fs`) must reject
/// path attachments outside `CAIRN_LOCAL_FS_BASE`. Codex's first cut
/// only covered the per-project allowlist endpoint; without this
/// test, an attacker could use the integrations API to register
/// `/etc` and bypass the fence entirely.
#[tokio::test]
async fn local_fs_integration_rejects_path_outside_base() {
    let base = tempfile::tempdir().expect("base tempdir");
    let base_dir = base.path().to_string_lossy().into_owned();
    let h = LiveHarness::setup_with_env(&[("CAIRN_LOCAL_FS_BASE", base_dir.as_str())]).await;

    // Path outside the fence — sibling temp dir, not under `base`.
    let outside = tempfile::tempdir().expect("outside tempdir");
    let outside_path = outside.path().to_string_lossy().into_owned();

    let res = h
        .client()
        .post(format!("{}/v1/integrations", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "id": "local-fs-escape",
            "type": "local_fs",
            "config": {
                "path": outside_path,
                "display_name": "should not register",
            },
        }))
        .send()
        .await
        .expect("register reaches server");

    let status = res.status().as_u16();
    let body = res.text().await.unwrap_or_default();
    assert!(
        (400..500).contains(&status),
        "expected 4xx for path outside fence, got {status}: {body}"
    );
    assert!(
        body.to_lowercase().contains("local_fs"),
        "error must mention local_fs in {body}"
    );
}

// The "registers when CAIRN_LOCAL_FS_BASE is unset" path is covered by
// the unit test `rejects_when_base_env_unset` in
// `cairn-integrations::local_fs::tests`. We deliberately don't add an
// integration variant: `LiveHarness::setup_with_env` only supports
// adding env vars, not removing them, so we can't deterministically
// guarantee the subprocess sees an unset `CAIRN_LOCAL_FS_BASE` (any
// CI runner with the var pre-set would mask the regression).

#[tokio::test]
async fn local_fs_project_repo_attach_and_list() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let base_dir = tmp.path().to_string_lossy().into_owned();
    let h = LiveHarness::setup_with_env(&[("CAIRN_LOCAL_FS_BASE", base_dir.as_str())]).await;
    let p = project_path(&h);
    let base = &h.base_url;

    let repo = tempfile::tempdir_in(tmp.path()).expect("repo tempdir");
    // The handler persists the canonicalised path (TOCTOU defence:
    // a symlink resolved at attach-time stays pinned to its real
    // target). Mirror that here so the response and list-row
    // `repo_id` assertions below compare canonical-vs-canonical
    // — `tempdir_in` returns a path that may already canonicalise
    // to a different prefix (e.g. macOS `/var` → `/private/var`).
    let path = repo
        .path()
        .canonicalize()
        .expect("canonicalise repo path")
        .to_string_lossy()
        .into_owned();

    // Attach the local path as a local_fs repo.
    let res = h
        .client()
        .post(format!("{base}/v1/projects/{p}/repos"))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "repo_id": path,
            "host": "local_fs",
        }))
        .send()
        .await
        .expect("attach reaches server");
    assert_eq!(
        res.status().as_u16(),
        200,
        "attach status, body: {}",
        res.text().await.unwrap_or_default(),
    );
    let body: Value = res.json().await.expect("attach json");
    assert_eq!(body.get("host").and_then(|v| v.as_str()), Some("local_fs"));
    assert_eq!(
        body.get("repo_id").and_then(|v| v.as_str()),
        Some(path.as_str())
    );

    // List should now include the local_fs entry alongside any github repos.
    let res = h
        .client()
        .get(format!("{base}/v1/projects/{p}/repos"))
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("list reaches server");
    let body: Value = res.json().await.expect("list json");
    let repos = body
        .get("repos")
        .and_then(|v| v.as_array())
        .cloned()
        .expect("repos array");
    let has_local = repos.iter().any(|r| {
        r.get("host").and_then(|v| v.as_str()) == Some("local_fs")
            && r.get("repo_id").and_then(|v| v.as_str()) == Some(path.as_str())
    });
    assert!(has_local, "expected local_fs entry in list, got {repos:?}");

    // Detach via the dedicated local-paths endpoint.
    let res = h
        .client()
        .delete(format!("{base}/v1/projects/{p}/local-paths"))
        .bearer_auth(&h.admin_token)
        .json(&json!({ "path": path }))
        .send()
        .await
        .expect("delete reaches server");
    assert_eq!(res.status().as_u16(), 204);
}

#[tokio::test]
async fn project_repo_attach_rejects_unknown_host() {
    let h = LiveHarness::setup().await;
    let p = project_path(&h);

    let res = h
        .client()
        .post(format!("{}/v1/projects/{p}/repos", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "repo_id": "avifenesh/cairn-rs",
            "host": "bitbucket",
        }))
        .send()
        .await
        .expect("attach reaches server");
    assert_eq!(res.status().as_u16(), 400);
}

#[tokio::test]
async fn project_repo_attach_returns_501_for_known_unimplemented_hosts() {
    let h = LiveHarness::setup().await;
    let p = project_path(&h);

    for host in ["gitlab", "gitea", "confluence"] {
        let res = h
            .client()
            .post(format!("{}/v1/projects/{p}/repos", h.base_url))
            .bearer_auth(&h.admin_token)
            .json(&json!({
                "repo_id": "someowner/somerepo",
                "host": host,
            }))
            .send()
            .await
            .expect("attach reaches server");
        assert_eq!(
            res.status().as_u16(),
            501,
            "host {host} should be 501, got {}",
            res.status().as_u16(),
        );
    }
}

/// Dogfood issue #637 regression: after an operator attaches a local_fs
/// path via `POST /v1/projects/:p/repos`, `POST /v1/runs/:id/orchestrate`
/// must use that path as the run's working directory — not silently fall
/// back to `/tmp/cairn-runs/<run_id>/` with a debug log
/// "no repo allowlisted for project".
///
/// Before the fix, the two paths disagreed: the write lands in the
/// in-memory `ProjectLocalPaths` bucket while the orchestrator's
/// resolver only consulted `ProjectRepoAccessService` (the github
/// allowlist). The resolver now checks both buckets in order
/// (github → local_fs → ephemeral).
///
/// Observable: the resolver emits
/// `"using local_fs working directory from project allowlist"` at
/// `info` when the local_fs branch is taken. The test points
/// `CAIRN_LOG_DIR` at a per-test tempdir so the subprocess writes its
/// daily-rotating `cairn.*.log` there; after calling orchestrate we
/// grep that file for the positive log line AND assert the
/// "no repo allowlisted" fallback line is absent for this run_id.
///
/// The orchestrate call is expected to return 503 `no_brain_provider`
/// because the test doesn't configure an LLM connection — that's fine,
/// `working_dir_for_run` runs BEFORE the provider lookup so the
/// resolver still fires and logs.
#[tokio::test]
async fn orchestrate_uses_local_fs_path_from_project_allowlist() {
    // Per-test log dir — isolates stderr capture from sibling tests
    // sharing the Valkey container. `tempfile::TempDir` auto-cleans
    // at drop; we Clone the path into the harness env because the
    // subprocess outlives the `tmp_log` binding otherwise.
    let tmp_log = tempfile::tempdir().expect("log tempdir");
    let log_dir_path = tmp_log.path().to_string_lossy().into_owned();

    let tmp_repo_base = tempfile::tempdir().expect("repo base tempdir");
    let local_fs_base = tmp_repo_base.path().to_string_lossy().into_owned();

    let h = LiveHarness::setup_with_env(&[
        // `extra_env` is applied AFTER the harness's own `env(...)`
        // calls, so this overrides the default `env_remove` on the
        // subprocess side. The file appender rotates daily, so the
        // subprocess writes `cairn.log.YYYY-MM-DD` under this dir.
        ("CAIRN_LOG_DIR", log_dir_path.as_str()),
        // Explicit override for the subprocess's RUST_LOG so this
        // test doesn't silently regress if someone tightens the
        // harness default. The resolver emits the "using local_fs"
        // line at INFO on the cairn_app target and the ephemeral
        // fallback line at DEBUG — both live under `cairn_app`, so
        // `cairn_app=debug` is required to make the negative
        // "ephemeral fallback did NOT fire" assertion meaningful.
        ("RUST_LOG", "warn,cairn_app=debug"),
        ("CAIRN_LOCAL_FS_BASE", local_fs_base.as_str()),
    ])
    .await;
    let p = project_path(&h);
    let base = &h.base_url;

    // Real directory the operator "attached" — tempfile keeps it
    // alive for the whole test, so the resolver's existence check
    // passes.
    let tmp_repo = tempfile::tempdir_in(tmp_repo_base.path()).expect("repo tempdir");
    // Use the canonicalised path so the resolver's log line (which
    // emits the canonical form persisted by the attach handler) and
    // this test's `contains(&repo_path)` assertion agree on every
    // platform. Without this, `tempdir_in` on macOS or any path
    // accessed through a `/tmp` symlink would fail the assertion
    // even when the resolver did the right thing.
    let repo_path = tmp_repo
        .path()
        .canonicalize()
        .expect("canonicalise repo path")
        .to_string_lossy()
        .into_owned();

    // 1. Attach local_fs path via the same endpoint the dogfood
    //    operator used.
    let res = h
        .client()
        .post(format!("{base}/v1/projects/{p}/repos"))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "repo_id": repo_path,
            "host": "local_fs",
        }))
        .send()
        .await
        .expect("attach reaches server");
    assert_eq!(
        res.status().as_u16(),
        200,
        "attach status, body: {}",
        res.text().await.unwrap_or_default(),
    );

    // 2. Session + run scoped to this harness's unique triple —
    //    keeps the run_id disjoint from parallel tests that might
    //    reuse `default_project`.
    let suffix = &h.project;
    let session_id = format!("sess_637_{suffix}");
    let run_id = format!("run_637_{suffix}");
    let tenant = &h.tenant;
    let workspace = &h.workspace;
    let project = &h.project;

    let r = h
        .client()
        .post(format!("{base}/v1/sessions"))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "tenant_id": tenant,
            "workspace_id": workspace,
            "project_id": project,
            "session_id": session_id,
        }))
        .send()
        .await
        .expect("session reaches server");
    assert_eq!(
        r.status().as_u16(),
        201,
        "session: {}",
        r.text().await.unwrap_or_default(),
    );

    let r = h
        .client()
        .post(format!("{base}/v1/runs"))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "tenant_id": tenant,
            "workspace_id": workspace,
            "project_id": project,
            "session_id": session_id,
            "run_id": run_id,
        }))
        .send()
        .await
        .expect("run reaches server");
    assert_eq!(
        r.status().as_u16(),
        201,
        "run: {}",
        r.text().await.unwrap_or_default(),
    );

    // 3. Orchestrate. With no LLM configured this returns 503
    //    no_brain_provider — but that's AFTER `working_dir_for_run`
    //    runs and emits its breadcrumb, which is what we assert on.
    let orch_res = h
        .client()
        .post(format!("{base}/v1/runs/{run_id}/orchestrate"))
        .bearer_auth(&h.admin_token)
        .json(&json!({
            "goal": "local_fs allowlist regression",
            "max_iterations": 1,
        }))
        .timeout(std::time::Duration::from_secs(30))
        .send()
        .await
        .expect("orchestrate reaches server");
    let orch_status = orch_res.status().as_u16();
    let orch_body = orch_res.text().await.unwrap_or_default();
    // 503 no_brain_provider is the expected (and benign) outcome in
    // this test — we're verifying the allowlist resolver, not an
    // end-to-end LLM call. Any 2xx is also acceptable (would happen
    // if a future harness default ever registered a mock provider).
    // A 5xx that isn't 503 signals a real bug introduced on top of
    // the resolver path.
    assert!(
        orch_status == 503 || (200..300).contains(&orch_status),
        "orchestrate unexpected status {orch_status}: {orch_body}",
    );

    // 4. Drain any buffered log lines by sleeping briefly — the
    //    subprocess's non-blocking appender writes on a background
    //    task, so there's a small window between our HTTP response
    //    and the log line landing on disk.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let log_body = read_log_dir(tmp_log.path());
    assert!(
        !log_body.is_empty(),
        "expected cairn-app to write logs under {log_dir_path}, got nothing",
    );

    // Positive assertion: the resolver emitted its "using local_fs"
    // breadcrumb for THIS run_id and THIS path on a single log line.
    // Checking all three on a single line (rather than three separate
    // `contains` over the whole buffer) protects against a future
    // unrelated log that happens to mention this run's id and path
    // from masking a real regression where the positive line is
    // missing.
    let positive_marker = "using local_fs working directory from project allowlist";
    let positive_hit = log_body.lines().any(|line| {
        line.contains(positive_marker) && line.contains(&run_id) && line.contains(&repo_path)
    });
    assert!(
        positive_hit,
        "expected positive local_fs resolver log for run_id={run_id} and path={repo_path}. Log body:\n{log_body}",
    );

    // Negative assertion: the pre-fix fallback log MUST NOT appear
    // for this run_id. A match would mean the resolver couldn't see
    // the local_fs attach — which is exactly the dogfood #637 bug.
    let fallback_marker = "no repo allowlisted for project; using ephemeral run directory";
    let fallback_for_this_run = log_body
        .lines()
        .filter(|line| line.contains(&run_id))
        .any(|line| line.contains(fallback_marker));
    assert!(
        !fallback_for_this_run,
        "regression: resolver fell back to ephemeral for run_id={run_id} despite local_fs allowlist. Log body:\n{log_body}",
    );
}

/// Read every `cairn.*.log` file in `log_dir` and concatenate their
/// contents. The file name is date-stamped (`cairn.log.YYYY-MM-DD`) so
/// we glob rather than hard-code the rotation suffix.
fn read_log_dir(log_dir: &std::path::Path) -> String {
    let mut out = String::new();
    let Ok(entries) = std::fs::read_dir(log_dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path
            .file_name()
            .and_then(|s| s.to_str())
            .map(|s| s.starts_with("cairn.log"))
            .unwrap_or(false)
        {
            if let Ok(body) = std::fs::read_to_string(&path) {
                out.push_str(&body);
            }
        }
    }
    out
}

#[tokio::test]
async fn project_repo_default_host_is_github_backward_compat() {
    let h = LiveHarness::setup().await;
    let p = project_path(&h);
    let base = &h.base_url;

    // Attach without `host` field — should default to github.
    let res = h
        .client()
        .post(format!("{base}/v1/projects/{p}/repos"))
        .bearer_auth(&h.admin_token)
        .json(&json!({ "repo_id": "avifenesh/cairn-rs" }))
        .send()
        .await
        .expect("attach reaches server");
    assert_eq!(res.status().as_u16(), 200);
    let body: Value = res.json().await.expect("attach json");
    assert_eq!(body.get("host").and_then(|v| v.as_str()), Some("github"));
}
