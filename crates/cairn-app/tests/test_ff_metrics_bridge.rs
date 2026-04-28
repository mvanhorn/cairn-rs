//! `/metrics` surfaces FF (ff-observability) metrics alongside cairn's.
//!
//! Regression guard for the PR #117 follow-up: cairn-fabric constructs
//! a shared `ff_observability::Metrics` registry and hands a clone to
//! the FF Engine; cairn-app's `/metrics` handler appends FF's
//! Prometheus text-exposition to the response. FF is compiled with
//! `ff-observability/enabled`, so the real OTEL → Prometheus exporter
//! runs (the no-op shim would return empty text and the assertions
//! below would fail).
//!
//! Why specific metric-name assertions: FF's `real.rs` defines the
//! `# HELP`/`# TYPE` lines even before any samples land, so the names
//! are present on every scrape regardless of whether any request has
//! hit the engine. If FF rename/drops these in a future minor, this
//! test fails and forces us to bump the docs + CHANGELOG in lockstep.
//!
//! We assert three representative names that land in /metrics during
//! normal cairn-app startup:
//!   * `ff_scanner_cycle` — FF's scanners tick in the background from
//!     `Engine::start`, so at least one sample lands quickly after
//!     startup. Counter; OTEL's Prometheus exporter appends `_total`.
//!   * `ff_scanner_cycle_duration` — histogram paired with the above,
//!     recorded in the same code path. OTEL appends `_seconds` for
//!     `unit="s"` instruments → `ff_scanner_cycle_duration_seconds`.
//!   * `ff_cancel_backlog_depth` — observable gauge. Polled by OTEL
//!     on every collection; emitted on first scrape regardless of
//!     whether cairn ever set a non-zero depth.
//!
//! These cover counter + histogram + gauge; enough to catch silent
//! drift if FF renames or removes these metrics in a future version.
//!
//! # Deterministic timing (fixes #401)
//!
//! Previous version slept 2 s hoping FF's scanner had ticked at least
//! once by then. Per `feedback_no_such_thing_as_flake.md` bare sleeps
//! before a projection-level read are races. We poll instead: scrape
//! /metrics repeatedly with 100 ms step and 10 s ceiling, break on
//! first response that contains all three expected FF metric names.
//! On a healthy run the first scan after scanner boot (~750 ms) has
//! them; pathological CI nodes get up to 10 s before the test fails
//! loudly.

mod support;

use std::time::Duration;

use support::live_fabric::LiveHarness;

#[tokio::test]
async fn metrics_endpoint_exposes_ff_metrics() {
    let h = LiveHarness::setup().await;

    // FF metrics we must see on /metrics. Names per ff-observability
    // 0.3.2 `real.rs` `mod name`, with OTEL's Prometheus-exporter
    // suffix rules applied (`_total` for counters, `_seconds` for
    // `unit="s"`).
    let required: [&str; 3] = [
        "ff_scanner_cycle_total",
        "ff_scanner_cycle_duration_seconds",
        "ff_cancel_backlog_depth",
    ];

    // Poll /metrics with 100 ms step + 10 s ceiling. FF's fastest
    // scanner (delayed_promoter) ticks at 750 ms, so in a healthy run
    // the first scrape after ~800 ms carries all three names. We
    // break early on success.
    let deadline = tokio::time::Instant::now() + Duration::from_millis(10_000);
    let mut iterations: u32 = 0;

    loop {
        iterations += 1;
        let res = h
            .client()
            .get(format!("{}/metrics", h.base_url))
            .send()
            .await
            .expect("/metrics endpoint reachable");
        let last_status = res.status().as_u16();
        let last_ct = res
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_owned();
        let last_body = res.text().await.expect("body");

        // Content-type / cairn-native metric must always be present.
        // An /metrics outage is a handler bug, not a scanner race —
        // fail fast rather than letting the loop time out.
        assert_eq!(last_status, 200, "/metrics returns 200");
        assert!(
            last_ct.starts_with("text/plain"),
            "prometheus exposition is text/plain, got {last_ct:?}"
        );
        assert!(
            last_body.contains("http_requests_total"),
            "cairn's http_requests_total still present: {last_body}"
        );

        if required.iter().all(|name| last_body.contains(name)) {
            return;
        }

        if tokio::time::Instant::now() >= deadline {
            let missing: Vec<&str> = required
                .iter()
                .copied()
                .filter(|name| !last_body.contains(name))
                .collect();
            panic!(
                "FF metric(s) {missing:?} absent from /metrics after {iterations} polls \
                 over 10 s (last status={last_status}, ct={last_ct:?}). Body tail:\n{}",
                // Tail so the panic message stays bounded on very long exposition.
                last_body
                    .chars()
                    .rev()
                    .take(4_000)
                    .collect::<String>()
                    .chars()
                    .rev()
                    .collect::<String>(),
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
