//! #422 + #423: pagination honesty lock-in.
//!
//! Before these tests, 20+ list handlers returned `has_more: false`
//! unconditionally — even when the store clearly had more rows.
//! Operators paging via the UI silently hit the wall at page 1.
//!
//! These tests seed enough rows to exceed the first page, then assert:
//!   - page 1 (`limit=N`, `offset=0`) returns N items with
//!     `has_more == true`
//!   - the last page returns <N items with `has_more == false`
//!   - offset-based paging returns disjoint items
//!   - /v1/costs (#423) honours the new limit/offset contract
//!
//! Per `feedback_integration_tests_only` these hit the real cairn-app
//! subprocess via `LiveHarness` — no mocks, no in-process pokes.

mod support;

use serde_json::{json, Value};
use support::live_fabric::LiveHarness;

/// Walk-the-list assertion the signals endpoint must satisfy. Seeds 25
/// signals (via the public `POST /v1/signals` endpoint) and asserts
/// that a page-size-10 pagination produces `has_more=true` on the first
/// two pages and `has_more=false` on the final partial page.
///
/// This endpoint was on the #422 list (`has_more: false` literal) and
/// the fix threads `limit + 1` through `SignalService::list_by_project`.
#[tokio::test]
async fn signals_pagination_reports_honest_has_more() {
    let h = LiveHarness::setup().await;

    // Seed 25 signals. Each `signal_id` is unique — the ingest path
    // dedups on `signal_id` and we want 25 distinct rows.
    const TOTAL: usize = 25;
    for i in 0..TOTAL {
        let r = h
            .client()
            .post(format!("{}/v1/signals", h.base_url))
            .bearer_auth(&h.admin_token)
            .json(&json!({
                "tenant_id": h.tenant,
                "workspace_id": h.workspace,
                "project_id": h.project,
                "signal_id": format!("sig_{}_{i:03}", h.project),
                "source": "test_harness",
                "payload": { "i": i },
                "timestamp_ms": 1_700_000_000_000u64 + i as u64,
            }))
            .send()
            .await
            .expect("ingest signal reaches server");
        assert_eq!(
            r.status().as_u16(),
            201,
            "seed signal {i}: {}",
            r.text().await.unwrap_or_default()
        );
    }

    // Page 1: offset=0, limit=10 → 10 items, has_more=true.
    let page1 = list_signals(&h, 10, 0).await;
    assert_eq!(
        page1["items"]
            .as_array()
            .expect("items array on page 1")
            .len(),
        10,
        "page 1 must return exactly limit=10 items: {page1:?}"
    );
    assert_eq!(
        page1["hasMore"],
        Value::Bool(true),
        "25 seeded, page 1 of 10 → has_more must be true: {page1:?}"
    );

    // Page 2: offset=10, limit=10 → 10 items, has_more=true.
    let page2 = list_signals(&h, 10, 10).await;
    assert_eq!(
        page2["items"].as_array().unwrap().len(),
        10,
        "page 2 must return 10: {page2:?}"
    );
    assert_eq!(
        page2["hasMore"],
        Value::Bool(true),
        "25 seeded, page 2 of 10 → has_more must still be true: {page2:?}"
    );

    // Final page: offset=20, limit=10 → 5 items, has_more=false.
    let tail = list_signals(&h, 10, 20).await;
    assert_eq!(
        tail["items"].as_array().unwrap().len(),
        5,
        "tail page must contain the residual 5 items: {tail:?}"
    );
    assert_eq!(
        tail["hasMore"],
        Value::Bool(false),
        "tail page exhausts the list → has_more must be false: {tail:?}"
    );

    // Pages must be disjoint — a single row must not appear on two
    // pages. Collect the `signal_id` of every item on pages 1+2+tail
    // and assert the set size matches the seeded count.
    let ids: std::collections::HashSet<String> = page1["items"]
        .as_array()
        .unwrap()
        .iter()
        .chain(page2["items"].as_array().unwrap().iter())
        .chain(tail["items"].as_array().unwrap().iter())
        .map(|it| {
            // SignalRecord serializes with field `id` (the signal id).
            it["id"]
                .as_str()
                .expect("id on every signal record")
                .to_owned()
        })
        .collect();
    assert_eq!(
        ids.len(),
        TOTAL,
        "all seeded ids must appear exactly once across pages; got {} unique",
        ids.len()
    );
}

/// Workers list endpoint — same pagination contract. #422 fix threads
/// `limit + 1` through `ExternalWorkerService::list`.
#[tokio::test]
async fn workers_pagination_reports_honest_has_more() {
    let h = LiveHarness::setup().await;

    // Seed 25 workers.
    const TOTAL: usize = 25;
    for i in 0..TOTAL {
        let r = h
            .client()
            .post(format!("{}/v1/workers/register", h.base_url))
            .bearer_auth(&h.admin_token)
            .json(&json!({
                "worker_id": format!("wk_{}_{i:03}", h.project),
                "display_name": format!("test worker {i}"),
            }))
            .send()
            .await
            .expect("register worker reaches server");
        assert_eq!(
            r.status().as_u16(),
            201,
            "seed worker {i}: {}",
            r.text().await.unwrap_or_default()
        );
    }

    let page1 = list_workers(&h, 10, 0).await;
    assert_eq!(page1["items"].as_array().unwrap().len(), 10, "page 1 size");
    assert_eq!(
        page1["hasMore"],
        Value::Bool(true),
        "workers page 1 of 10 must say has_more=true: {page1:?}"
    );

    let tail = list_workers(&h, 10, 20).await;
    assert_eq!(
        tail["items"].as_array().unwrap().len(),
        5,
        "workers tail page size"
    );
    assert_eq!(
        tail["hasMore"],
        Value::Bool(false),
        "workers tail page must say has_more=false: {tail:?}"
    );
}

/// Signal subscriptions — same pagination contract. #422 fix threads
/// `limit + 1` through `SignalRouterService::list_by_project`.
#[tokio::test]
async fn signal_subscriptions_pagination_reports_honest_has_more() {
    let h = LiveHarness::setup().await;

    // Seed 15 subscriptions — the router's subscribe service allows
    // per-kind subscriptions, so we vary the kind to avoid dedup.
    // A target (run or mailbox) is required; we use mailbox targets
    // so the test doesn't need a seeded run.
    //
    // The router mints `subscription_id = format!("signal_sub_{now_ms}")`
    // off the wall clock, so rapid-fire subscribe() calls collide on
    // the same millisecond and the projection's `upsert_subscription`
    // collapses them to one row. We insert a tiny delay between
    // POSTs so each subscription is keyed by a distinct timestamp;
    // 2 ms is above wall-clock resolution on every supported host.
    const TOTAL: usize = 15;
    for i in 0..TOTAL {
        let r = h
            .client()
            .post(format!("{}/v1/signals/subscriptions", h.base_url))
            .bearer_auth(&h.admin_token)
            .json(&json!({
                "tenant_id": h.tenant,
                "workspace_id": h.workspace,
                "project_id": h.project,
                "signal_kind": format!("kind_{i:03}"),
                "target_mailbox_id": format!("mbx_{i:03}"),
            }))
            .send()
            .await
            .expect("create subscription reaches server");
        assert_eq!(
            r.status().as_u16(),
            201,
            "seed sub {i}: {}",
            r.text().await.unwrap_or_default()
        );
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }

    // Page 1 of 10.
    let page1 = list_signal_subscriptions(&h, 10, 0).await;
    assert_eq!(page1["items"].as_array().unwrap().len(), 10);
    assert_eq!(
        page1["hasMore"],
        Value::Bool(true),
        "subs page 1 of 10 must say has_more=true: {page1:?}"
    );

    // Tail page returns 5, has_more=false.
    let tail = list_signal_subscriptions(&h, 10, 10).await;
    assert_eq!(tail["items"].as_array().unwrap().len(), 5);
    assert_eq!(
        tail["hasMore"],
        Value::Bool(false),
        "subs tail page must say has_more=false: {tail:?}"
    );
}

/// #423: `/v1/costs` honours `limit` + `offset` with an honest
/// `has_more`. Before this fix the handler fetched every
/// `session_costs` row for the tenant in one payload — a six-month-old
/// tenant with hundreds of thousands of rows would OOM the server.
///
/// We seed 120 rows (above the default page size of 200 is overkill,
/// and below 100 doesn't exercise the default limit) and assert the
/// default page is capped at the configured limit with an honest
/// `has_more`.
#[tokio::test]
async fn tenant_costs_pagination_reports_honest_has_more() {
    let h = LiveHarness::setup().await;

    // Seed 120 session-cost events. Each event feeds a distinct
    // `SessionCostRecord` on the InMemory read model (one row per
    // `(tenant, session)` pair — we vary `session_id` so every event
    // produces a new row rather than accumulating on the same record).
    //
    // The events use `tenant_id: "default"` because `/v1/costs` is
    // tenant-scoped via the bearer identity, and the test harness's
    // admin token is registered against `TenantKey("default")`. The
    // unique-per-test isolation comes from the subprocess boundary
    // (each `LiveHarness::setup()` spawns a fresh in-memory store),
    // not from the tenant-id itself.
    const TOTAL: usize = 120;
    let mut envelopes = Vec::with_capacity(TOTAL);
    for i in 0..TOTAL {
        envelopes.push(json!({
            "event_id": format!("scu_{}_{i:03}", h.project),
            "source": { "source_type": "system" },
            "ownership": {
                "scope": "project",
                "tenant_id": "default",
                "workspace_id": h.workspace,
                "project_id": h.project,
            },
            "causation_id": null,
            "correlation_id": null,
            "payload": {
                "event": "session_cost_updated",
                "project": {
                    "tenant_id": "default",
                    "workspace_id": h.workspace,
                    "project_id": h.project,
                },
                "session_id": format!("sess_{i:03}"),
                "tenant_id": "default",
                "delta_cost_micros": 1_000u64 + i as u64,
                "delta_tokens_in": 100u64,
                "delta_tokens_out": 50u64,
                "provider_call_id": format!("call_{i:03}"),
                "updated_at_ms": 1_700_000_000_000u64 + i as u64 * 1_000,
            },
        }));
    }
    let r = h
        .client()
        .post(format!("{}/v1/events/append", h.base_url))
        .bearer_auth(&h.admin_token)
        .json(&envelopes)
        .send()
        .await
        .expect("seed session cost events");
    assert!(
        r.status().is_success(),
        "seed events status: {} body: {}",
        r.status().as_u16(),
        r.text().await.unwrap_or_default()
    );

    // Default limit (200) — returns all 120 with `has_more: false`.
    let default_page = list_costs(&h, None, None).await;
    let items = default_page["items"].as_array().expect("items on costs");
    assert_eq!(
        items.len(),
        TOTAL,
        "default limit=200 returns all 120 seeded rows: {default_page:?}"
    );
    assert_eq!(
        default_page["hasMore"],
        Value::Bool(false),
        "120 rows < 200 default limit → has_more must be false"
    );

    // Explicit `limit=50`, `offset=0` — first page, 50 items,
    // has_more=true.
    let page1 = list_costs(&h, Some(50), Some(0)).await;
    assert_eq!(
        page1["items"].as_array().unwrap().len(),
        50,
        "page1 of 50: {page1:?}"
    );
    assert_eq!(
        page1["hasMore"],
        Value::Bool(true),
        "120 rows, limit=50 → page 1 must say has_more=true"
    );

    // `limit=50`, `offset=100` — tail of 20, has_more=false.
    let tail = list_costs(&h, Some(50), Some(100)).await;
    assert_eq!(
        tail["items"].as_array().unwrap().len(),
        20,
        "tail of 20: {tail:?}"
    );
    assert_eq!(
        tail["hasMore"],
        Value::Bool(false),
        "tail page has no successor → has_more=false"
    );

    // `limit` clamps at 1000: asking for 10_000 returns 120 (the full
    // seeded set) without error.
    let huge = list_costs(&h, Some(10_000), None).await;
    assert_eq!(
        huge["items"].as_array().unwrap().len(),
        TOTAL,
        "oversize limit is clamped silently; returns the full set: {huge:?}"
    );
    assert_eq!(
        huge["hasMore"],
        Value::Bool(false),
        "limit=10_000 (clamped to 1000) with 120 rows → has_more=false"
    );
}

// ── helpers ────────────────────────────────────────────────────────────

async fn list_costs(h: &LiveHarness, limit: Option<usize>, offset: Option<usize>) -> Value {
    let mut url = format!("{}/v1/costs?", h.base_url);
    if let Some(l) = limit {
        url.push_str(&format!("limit={l}&"));
    }
    if let Some(o) = offset {
        url.push_str(&format!("offset={o}&"));
    }
    // Strip trailing `?` or `&` for cleanliness; the server ignores
    // trailing separators but tidier URLs make log triage easier.
    let url = url.trim_end_matches(['&', '?']);
    // `/v1/costs` is tenant-scoped via the caller's bearer identity —
    // no explicit tenant_id query param needed.
    let r = h
        .client()
        .get(url)
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("list costs reaches server");
    assert_eq!(
        r.status().as_u16(),
        200,
        "list costs status: {}",
        r.text().await.unwrap_or_default()
    );
    r.json().await.expect("list costs json")
}

async fn list_signals(h: &LiveHarness, limit: usize, offset: usize) -> Value {
    let url = format!(
        "{}/v1/signals?tenant_id={}&workspace_id={}&project_id={}&limit={}&offset={}",
        h.base_url, h.tenant, h.workspace, h.project, limit, offset
    );
    let r = h
        .client()
        .get(url)
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("list signals reaches server");
    assert_eq!(
        r.status().as_u16(),
        200,
        "list signals status: {}",
        r.text().await.unwrap_or_default()
    );
    r.json().await.expect("list signals json")
}

async fn list_workers(h: &LiveHarness, limit: usize, offset: usize) -> Value {
    let url = format!(
        "{}/v1/workers?limit={}&offset={}",
        h.base_url, limit, offset
    );
    let r = h
        .client()
        .get(url)
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("list workers reaches server");
    assert_eq!(
        r.status().as_u16(),
        200,
        "list workers status: {}",
        r.text().await.unwrap_or_default()
    );
    r.json().await.expect("list workers json")
}

async fn list_signal_subscriptions(h: &LiveHarness, limit: usize, offset: usize) -> Value {
    let url = format!(
        "{}/v1/signals/subscriptions?tenant_id={}&workspace_id={}&project_id={}&limit={}&offset={}",
        h.base_url, h.tenant, h.workspace, h.project, limit, offset
    );
    let r = h
        .client()
        .get(url)
        .bearer_auth(&h.admin_token)
        .send()
        .await
        .expect("list subs reaches server");
    assert_eq!(
        r.status().as_u16(),
        200,
        "list subs status: {}",
        r.text().await.unwrap_or_default()
    );
    r.json().await.expect("list subs json")
}
