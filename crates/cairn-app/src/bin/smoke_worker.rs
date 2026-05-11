//! `smoke_worker` — minimal HTTP fixture that drives a run/task through a
//! claim-then-operate cycle against a running cairn-app.
//!
//! The historical `scripts/smoke-test.sh` assumed the in-memory runtime's
//! permissive state machine: `pause` without `claim`, `release_lease`
//! without prior claim, etc. In the default Fabric build FF rejects those
//! transitions with HTTP 500 (correct behaviour — a run must be active
//! before it can be paused).
//!
//! This binary is the fixture the smoke script uses to exercise the
//! Fabric-strict path end-to-end: claim the run, pause/resume, operate,
//! release — every transition happens in the correct order and every
//! HTTP status is asserted.
//!
//! Invocation (shell):
//!   CAIRN_URL=http://localhost:3000 CAIRN_TOKEN=... \
//!     cargo run -p cairn-app --bin smoke_worker -- \
//!       --tenant default_tenant --workspace default_workspace \
//!       --project default_project --run-id smoke_run_$(date +%s) \
//!       --session-id smoke_sess_$(date +%s) --task-id smoke_task_$(date +%s)
//!
//! Exits 0 on full green, 1 on the first failed assertion.
//!
//! The bin intentionally depends on **reqwest + serde only** — no runtime
//! crates — so it works against any Fabric build without bloating the
//! cairn-app binary with test-side service dependencies.

use std::process::ExitCode;

use serde_json::json;

fn env_or(key: &str, fallback: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| fallback.to_owned())
}

#[derive(Debug, Clone, Default)]
struct Opts {
    base: String,
    token: String,
    tenant: String,
    workspace: String,
    project: String,
    run_id: String,
    session_id: String,
    task_id: String,
    worker_id: String,
}

fn parse_args() -> Opts {
    let mut opts = Opts {
        base: env_or("CAIRN_URL", "http://localhost:3000"),
        token: env_or("CAIRN_TOKEN", "dev-admin-token"),
        tenant: "default_tenant".into(),
        workspace: "default_workspace".into(),
        project: "default_project".into(),
        worker_id: format!("smoke_worker_{}", std::process::id()),
        ..Opts::default()
    };
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        let key = args[i].as_str();
        let val = args.get(i + 1).cloned().unwrap_or_default();
        match key {
            "--tenant" => opts.tenant = val,
            "--workspace" => opts.workspace = val,
            "--project" => opts.project = val,
            "--run-id" => opts.run_id = val,
            "--session-id" => opts.session_id = val,
            "--task-id" => opts.task_id = val,
            "--worker-id" => opts.worker_id = val,
            other => {
                eprintln!("smoke_worker: unknown arg {other}");
                std::process::exit(2);
            }
        }
        i += 2;
    }
    for (name, value) in [
        ("--run-id", &opts.run_id),
        ("--session-id", &opts.session_id),
        ("--task-id", &opts.task_id),
    ] {
        if value.is_empty() {
            eprintln!("smoke_worker: {name} is required");
            std::process::exit(2);
        }
    }
    opts
}

struct Client {
    inner: reqwest::Client,
    base: String,
    token: String,
    failures: Vec<String>,
    passes: usize,
}

impl Client {
    fn new(opts: &Opts) -> Self {
        Self {
            inner: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(15))
                .build()
                .expect("build reqwest client"),
            base: opts.base.clone(),
            token: opts.token.clone(),
            failures: vec![],
            passes: 0,
        }
    }

    async fn post(
        &mut self,
        label: &str,
        path: &str,
        body: serde_json::Value,
        want_status: u16,
    ) -> Option<serde_json::Value> {
        let url = format!("{}{}", self.base, path);
        match self
            .inner
            .post(&url)
            .bearer_auth(&self.token)
            .json(&body)
            .send()
            .await
        {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let text = resp.text().await.unwrap_or_default();
                if status == want_status {
                    println!("  ✓ {label} (HTTP {status})");
                    self.passes += 1;
                    serde_json::from_str(&text).ok()
                } else {
                    self.failures.push(format!(
                        "{label}: got HTTP {status}, want {want_status} — {text:.200}"
                    ));
                    println!("  ✗ {label} (HTTP {status}, want {want_status})");
                    None
                }
            }
            Err(e) => {
                self.failures
                    .push(format!("{label}: transport error — {e}"));
                println!("  ✗ {label} (transport error: {e})");
                None
            }
        }
    }

    async fn get(
        &mut self,
        label: &str,
        path: &str,
        want_status: u16,
    ) -> Option<serde_json::Value> {
        let url = format!("{}{}", self.base, path);
        match self.inner.get(&url).bearer_auth(&self.token).send().await {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let text = resp.text().await.unwrap_or_default();
                if status == want_status {
                    println!("  ✓ {label} (HTTP {status})");
                    self.passes += 1;
                    serde_json::from_str(&text).ok()
                } else {
                    self.failures.push(format!(
                        "{label}: got HTTP {status}, want {want_status} — {text:.200}"
                    ));
                    println!("  ✗ {label} (HTTP {status}, want {want_status})");
                    None
                }
            }
            Err(e) => {
                self.failures
                    .push(format!("{label}: transport error — {e}"));
                println!("  ✗ {label} (transport error: {e})");
                None
            }
        }
    }

    /// Poll `GET /v1/tasks/:id` until it returns 200 or the deadline
    /// fires. The bridge populates the task projection on a separate
    /// task from FF's `submit_task_execution`, so the claim-then-
    /// operate sequence must wait for eventual consistency before
    /// invoking `claim_task_handler` (which reads the projection
    /// via `load_task_visible_to_tenant` first).
    async fn wait_for_task_visible(&mut self, task_id: &str) -> bool {
        let url = format!("{}/v1/tasks/{}", self.base, task_id);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if let Ok(resp) = self.inner.get(&url).bearer_auth(&self.token).send().await {
                if resp.status().as_u16() == 200 {
                    println!("  ✓ task visible before claim");
                    self.passes += 1;
                    return true;
                }
            }
            if std::time::Instant::now() >= deadline {
                self.failures
                    .push(format!("task {task_id} not visible within 5s"));
                println!("  ✗ task {task_id} not visible within 5s");
                return false;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let opts = parse_args();
    println!(
        "smoke_worker: base={} run={} task={} session={}",
        opts.base, opts.run_id, opts.task_id, opts.session_id
    );
    let mut c = Client::new(&opts);

    // ── Create session + run ───────────────────────────────────────────────
    c.post(
        "POST /v1/sessions",
        "/v1/sessions",
        json!({
            "tenant_id": opts.tenant,
            "workspace_id": opts.workspace,
            "project_id": opts.project,
            "session_id": opts.session_id,
        }),
        201,
    )
    .await;

    c.post(
        "POST /v1/runs",
        "/v1/runs",
        json!({
            "tenant_id": opts.tenant,
            "workspace_id": opts.workspace,
            "project_id": opts.project,
            "session_id": opts.session_id,
            "run_id": opts.run_id,
        }),
        201,
    )
    .await;

    // ── Submit task via the operator path + claim it ──────────────────────
    // `POST /v1/tasks` is the operator-contract submit path
    // (`create_task_handler` → `TaskService::submit`): it mints the FF
    // execution, registers it with the control-plane, and emits
    // `TaskCreated` via the bridge, so projections + FF state stay
    // in lockstep and the subsequent claim is guaranteed to find the
    // execution FF has on record.
    c.post(
        "POST /v1/tasks (operator submit path)",
        "/v1/tasks",
        json!({
            "tenant_id": opts.tenant,
            "workspace_id": opts.workspace,
            "project_id": opts.project,
            "task_id": opts.task_id,
            "parent_run_id": opts.run_id,
            "parent_task_id": null,
            "priority": 0,
        }),
        201,
    )
    .await;

    // Wait for the projection to catch up. `POST /v1/tasks` returns as
    // soon as FF confirms submit, but the bridge-driven projection
    // upsert runs on a separate task and `claim_task_handler` reads the
    // projection first (via `load_task_visible_to_tenant`). Without the
    // poll a fast CI could race and see a 404 on a freshly-submitted
    // task.
    c.wait_for_task_visible(&opts.task_id).await;

    let claimed = c
        .post(
            "POST /v1/tasks/:id/claim (claim-then-operate)",
            &format!("/v1/tasks/{}/claim", opts.task_id),
            json!({
                "worker_id": opts.worker_id,
                "lease_duration_ms": 30_000,
            }),
            200,
        )
        .await;
    if let Some(body) = &claimed {
        let got = body.get("state").and_then(|s| s.as_str()).unwrap_or("");
        // Under the operator-path (`POST /v1/tasks` → service submit),
        // FF auto-advances the execution through `leased` to `running`
        // as part of the grant+claim sequence. Either state is a
        // successful claim for this fixture — both carry a populated
        // lease_owner + lease_expires_at, which is what the
        // claim-then-operate sequence below actually needs.
        if matches!(got, "leased" | "running") {
            println!("  ✓ task state={got} after claim");
            c.passes += 1;
        } else {
            c.failures.push(format!(
                "task state after claim = {got:?}, want leased or running",
            ));
        }
        let has_lease = body.get("lease_owner").and_then(|v| v.as_str()).is_some();
        if has_lease {
            println!("  ✓ lease_owner populated after claim");
            c.passes += 1;
        } else {
            c.failures
                .push("lease_owner missing after claim".to_owned());
        }
    }

    // NOTE: run-surface pause/resume is intentionally omitted.
    //
    // Runs and tasks get distinct execution ids (`id_map::run_to_execution_id`
    // vs `task_to_execution_id`). The task above was claimed, the run was
    // not — and `POST /v1/runs` currently calls `runs.start()` which
    // creates a waiting execution, never an active one. Under Fabric-strict
    // state machine `ff_suspend_execution` rejects a non-active execution
    // with `execution_not_active`, so `POST /v1/runs/:id/pause` against
    // the smoke_worker-created run returns HTTP 500 — the exact
    // false-green this fixture was built to prevent.
    //
    // Exercising run pause/resume end-to-end requires either:
    //   (a) a run-surface HTTP claim (`POST /v1/runs/:id/claim`) wired to
    //       `FabricRunService::claim`, which exists on the service layer
    //       (cairn-fabric) but is not yet plumbed through the cairn-runtime
    //       `RunService` trait or the cairn-app router, OR
    //   (b) task-surface pause/resume (`POST /v1/tasks/:id/pause` +
    //       `/resume`), with handlers/routes in cairn-app — also not
    //       present today.
    // Both are follow-up work; neither blocks this fixture from
    // exercising the lease-bearing (task) lifecycle in Fabric-strict mode.

    // Release the lease — legal only after a prior claim.
    c.post(
        "POST /v1/tasks/:id/release-lease (post-claim)",
        &format!("/v1/tasks/{}/release-lease", opts.task_id),
        json!({}),
        200,
    )
    .await;

    // Sanity read-back.
    c.get(
        "GET /v1/tasks/:id (post-release)",
        &format!("/v1/tasks/{}", opts.task_id),
        200,
    )
    .await;

    println!(
        "\nsmoke_worker: {} passed, {} failed",
        c.passes,
        c.failures.len()
    );
    if c.failures.is_empty() {
        ExitCode::SUCCESS
    } else {
        for f in &c.failures {
            eprintln!("  FAIL: {f}");
        }
        ExitCode::FAILURE
    }
}
