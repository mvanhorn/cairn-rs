//! Shared synchronisation helpers for metrics-tap tests.
//!
//! The tap runs on a tokio broadcast channel — appending an event to
//! the store hands it off to the tap task, which then updates the
//! prometheus counters. Tests that assert on the rendered output need
//! a deterministic way to wait for the hand-off to complete. Before
//! #407 the codebase used a 200ms bare sleep; these helpers replace
//! that with bounded-deadline polling.
//!
//! Lives under `tests/support` so `metrics_core.rs` and
//! `metrics_providers.rs` share the same deadline, cadence, and
//! panic formatting — changes to any of those three apply to both
//! suites at once.
//!
//! Feature-gated on `metrics-core | metrics-providers` because the
//! helpers depend on `cairn_app::metrics::AppMetrics` which itself is
//! gated. Both features compile the module in; either-or is enough.

#![cfg(any(feature = "metrics-core", feature = "metrics-providers"))]

use std::time::Duration;

use cairn_app::metrics::AppMetrics;

/// Poll `render_prometheus()` until every `expected` substring is
/// present, with a bounded deadline. Deterministic replacement for
/// the 200ms bare sleep that #407 called out as a race: broadcast
/// delivery can stretch past the old 200ms budget under CI load and
/// silently drop assertions, turning real bugs into "flakes."
///
/// Panics (with the rendered output) if the deadline fires without
/// every substring landing. The 2s cap is generous (in-process
/// broadcast + counter writes resolve in the microsecond range; CI
/// noise lives in the tens-of-ms range) and keeps the suite snappy
/// while giving CPU-starved runners headroom. A 5ms poll cadence
/// keeps the common case effectively synchronous.
pub async fn wait_for_metrics(metrics: &AppMetrics, expected: &[&str]) {
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        let output = metrics.render_prometheus();
        if expected.iter().all(|needle| output.contains(needle)) {
            return;
        }
        if std::time::Instant::now() >= deadline {
            let missing: Vec<&str> = expected
                .iter()
                .copied()
                .filter(|n| !output.contains(n))
                .collect();
            panic!(
                "wait_for_metrics: deadline hit; missing lines: {missing:?}\n\
                 ---- rendered prometheus ----\n{output}\n---- end ----",
            );
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Assert from a single `render_prometheus()` snapshot that none of
/// the `forbidden` substrings are present. Used for the "unrelated
/// events must not bump lifecycle counters" regression: the caller
/// first waits for a positive-edge counter (via `wait_for_metrics`),
/// then calls this helper synchronously. The positive-edge wait
/// guarantees the tap has processed every prior append; the
/// forbidden-substring check is deterministic from that point
/// onward because `process_event` is fully synchronous.
pub fn assert_metrics_absent(metrics: &AppMetrics, forbidden: &[&str]) {
    let output = metrics.render_prometheus();
    for needle in forbidden {
        assert!(
            !output.contains(needle),
            "forbidden substring `{needle}` present in rendered prometheus:\n{output}",
        );
    }
}
