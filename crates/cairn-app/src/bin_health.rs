//! Detailed health endpoint — deep health checks for every subsystem.

#[allow(unused_imports)]
use crate::*;

use axum::extract::State;
use axum::Json;
use serde::Serialize;
use std::time::Instant;

// ── Detailed health handler ───────────────────────────────────────────────────

/// Per-subsystem health entry.
#[derive(Serialize)]
pub(crate) struct CheckEntry {
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    latency_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    models: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    size: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    capacity: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rss_mb: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    heap_mb: Option<u64>,
}

#[derive(Serialize)]
pub(crate) struct DetailedHealthChecks {
    store: CheckEntry,
    ollama: CheckEntry,
    event_buffer: CheckEntry,
    memory: CheckEntry,
}

#[derive(Serialize)]
pub(crate) struct DetailedHealthResponse {
    status: &'static str,
    checks: DetailedHealthChecks,
    uptime_seconds: u64,
    version: &'static str,
    started_at: String,
    /// RFC 011: current process role.
    role: String,
}

/// Read resident set size from `/proc/self/status` (Linux only).
/// Returns (rss_kb, vm_size_kb).  Returns (0, 0) on other platforms.
///
/// Uses `tokio::fs` so the async `/v1/health/detailed` handler does not
/// park the runtime on a sync `/proc` read. `/proc` reads are kernel-synth
/// data and typically sub-millisecond, but a /proc read under memory
/// pressure can block — tokio::fs shunts that to the blocking pool.
pub(crate) async fn read_proc_memory() -> (u64, u64) {
    #[cfg(target_os = "linux")]
    {
        if let Ok(text) = tokio::fs::read_to_string("/proc/self/status").await {
            let mut rss = 0u64;
            let mut vm = 0u64;
            for line in text.lines() {
                if line.starts_with("VmRSS:") {
                    rss = line
                        .split_whitespace()
                        .nth(1)
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(0);
                } else if line.starts_with("VmSize:") {
                    vm = line
                        .split_whitespace()
                        .nth(1)
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(0);
                }
            }
            return (rss, vm);
        }
    }
    (0, 0)
}

/// `GET /v1/health/detailed` — deep health status for every subsystem.
pub(crate) async fn detailed_health_handler(
    State(state): State<AppState>,
) -> Json<DetailedHealthResponse> {
    // ── Store check ───────────────────────────────────────────────────────────
    let store_start = Instant::now();
    let store_ok = if let Some(pg) = &state.pg {
        pg.adapter.health_check().await.is_ok()
    } else if let Some(sq) = &state.sqlite {
        sq.adapter.health_check().await.is_ok()
    } else {
        state.runtime.store.head_position().await.is_ok()
    };
    let store_latency = store_start.elapsed().as_millis() as u64;

    let store_check = CheckEntry {
        status: if store_ok { "healthy" } else { "unhealthy" },
        latency_ms: Some(store_latency),
        models: None,
        size: None,
        capacity: None,
        rss_mb: None,
        heap_mb: None,
    };

    // ── Ollama check ──────────────────────────────────────────────────────────
    let ollama_check = if let Some(provider) = &state.ollama {
        let t = Instant::now();
        match provider.health_check().await {
            Ok(tags) => CheckEntry {
                status: "healthy",
                latency_ms: Some(t.elapsed().as_millis() as u64),
                models: Some(tags.models.len()),
                size: None,
                capacity: None,
                rss_mb: None,
                heap_mb: None,
            },
            Err(_) => CheckEntry {
                status: "unhealthy",
                latency_ms: None,
                models: None,
                size: None,
                capacity: None,
                rss_mb: None,
                heap_mb: None,
            },
        }
    } else {
        CheckEntry {
            status: "unconfigured",
            latency_ms: None,
            models: None,
            size: None,
            capacity: None,
            rss_mb: None,
            heap_mb: None,
        }
    };

    // ── Event buffer (not present in main.rs; always at capacity 0) ──────────
    // The SSE ring buffer lives in lib.rs AppState only.  For completeness we
    // report it as healthy with unknown size.
    let event_buffer_check = CheckEntry {
        status: "healthy",
        latency_ms: None,
        size: None,
        capacity: None,
        models: None,
        rss_mb: None,
        heap_mb: None,
    };

    // ── Process memory ────────────────────────────────────────────────────────
    let (rss_kb, _vm_kb) = read_proc_memory().await;
    let memory_check = CheckEntry {
        status: "healthy",
        rss_mb: Some(rss_kb / 1024),
        heap_mb: None, // allocator-level heap not easily available without jemalloc
        latency_ms: None,
        models: None,
        size: None,
        capacity: None,
    };

    // ── Overall status ────────────────────────────────────────────────────────
    let degraded = !store_ok || matches!(ollama_check.status, "unhealthy");

    let overall = if degraded { "degraded" } else { "healthy" };

    // ISO-8601 started_at from uptime
    let uptime = state.started_at.elapsed().as_secs();
    let started_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .saturating_sub(uptime);
    let started_at = format!(
        "{}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        1970 + started_secs / 31_557_600, // approx — good enough for display
        ((started_secs % 31_557_600) / 2_629_800) + 1,
        ((started_secs % 2_629_800) / 86_400) + 1,
        (started_secs % 86_400) / 3_600,
        (started_secs % 3_600) / 60,
        started_secs % 60,
    );

    Json(DetailedHealthResponse {
        status: overall,
        checks: DetailedHealthChecks {
            store: store_check,
            ollama: ollama_check,
            event_buffer: event_buffer_check,
            memory: memory_check,
        },
        uptime_seconds: uptime,
        version: env!("CARGO_PKG_VERSION"),
        started_at,
        role: state.process_role.as_str().to_owned(),
    })
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    /// Regression test for #505: `read_proc_memory` MUST be async so the
    /// detailed-health handler does not park the tokio runtime on a sync
    /// `/proc` read.
    #[tokio::test]
    async fn read_proc_memory_returns_nonzero_rss_on_linux() {
        // Running in a live tokio runtime exercises the tokio::fs path.
        let (rss_kb, vm_kb) = read_proc_memory().await;
        // The current test binary is a live process — both values should be
        // populated. Sanity-check, not a tight bound (CI VMs vary widely).
        assert!(rss_kb > 0, "VmRSS should be non-zero in the test process");
        assert!(vm_kb > 0, "VmSize should be non-zero in the test process");
        assert!(
            vm_kb >= rss_kb,
            "VmSize ({vm_kb} KiB) should be >= VmRSS ({rss_kb} KiB)"
        );
    }

    /// Structural guard: the module source MUST NOT regress to
    /// `std::fs::read_to_string` inside the async handler path — that would
    /// reintroduce the sync-IO-on-async-runtime issue fixed by #505.
    #[test]
    fn source_has_no_sync_fs_read_to_string() {
        let src = include_str!("bin_health.rs");
        // Strip this test itself (which mentions the forbidden string).
        let upto_tests = src.split("#[cfg(all(test").next().unwrap_or(src);
        assert!(
            !upto_tests.contains("std::fs::read_to_string"),
            "bin_health.rs must not use std::fs::read_to_string in the async path; use tokio::fs"
        );
    }
}
