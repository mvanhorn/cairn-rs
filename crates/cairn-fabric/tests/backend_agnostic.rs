//! PR-B proof-point: exercise the backend-agnostic public surface of
//! cairn-fabric without touching the Valkey runtime.
//!
//! This test binary references **only** items that must remain
//! available when `fabric-valkey` is off:
//!
//! - `FabricError` (error variants + `Display`)
//! - `FabricConfig` + `flowfabric::core::backend::BackendConfig`
//!   (`CAIRN_FABRIC_URL` env parsing)
//! - `engine::Engine` + `engine::ControlPlaneBackend` (trait
//!   definitions — dyn-dispatchable shape)
//! - `engine::ExecutionSnapshot` + mirror `control_plane_types::*`
//! - `id_map` deterministic UUID v5 conversions
//! - `state_map` PublicState ↔ cairn domain state conversions
//! - `fcall::names::*` FCALL name constants
//! - `helpers::{now_ms, sanitize_signal_component,
//!   parse_public_state, try_parse_project_key}` (the
//!   ferriskey-free helpers that stay compiled without the feature)
//!
//! **Source-level guarantee.** Every `use cairn_fabric::…` in this
//! file resolves to a symbol declared in an always-on module (see
//! `src/lib.rs` above the `#[cfg(feature = "fabric-valkey")] pub mod`
//! block). If a future change moves one of these symbols into a
//! `fabric-valkey`-gated module OR introduces a ferriskey type into
//! its signature, this file stops compiling. That property holds
//! regardless of what Cargo's feature-unifier decides elsewhere in
//! the workspace.
//!
//! **Cargo-level caveat.** `cargo test -p cairn-fabric --test
//! backend_agnostic --no-default-features` may still compile the
//! cairn-fabric library with `fabric-valkey` active, because Cargo
//! unifies dev-dep features at the package level and the
//! `cairn-orchestrator` dev-dep (consumed by `test_orchestrator_stream`
//! inside the separate `integration` test target) carries a default
//! `cairn-fabric` dependency. Breaking that unification would require
//! moving the orchestrator test out of cairn-fabric/tests — tracked as
//! a follow-up, not part of PR-B's scope.
//!
//! The test body is minimal-assertion on purpose: the *compile* is
//! the assertion. Per `feedback_integration_tests_only.md` we do not
//! claim the Valkey backend works from this file; that surface has
//! its own live-Valkey integration suite in `tests/integration.rs`
//! under the `test-harness` feature.

use std::sync::Mutex;

use cairn_fabric::engine::{ControlPlaneBackend, Engine, ExecutionSnapshot};
use cairn_fabric::error::FabricError;
use cairn_fabric::fcall::names;
use cairn_fabric::helpers::{
    now_ms, parse_public_state, sanitize_signal_component, try_parse_project_key,
};
use cairn_fabric::{FabricConfig, FabricError as ReexportedError};

/// `CAIRN_FABRIC_URL` + the other `CAIRN_FABRIC_*` env vars are
/// process-global. Cargo runs tests inside one binary on a shared
/// thread pool so two tests that both mutate them race — one's
/// `set_var` lands between another's `set_var` and `from_env`.
/// Gate every env-mutating test here through a binary-scoped mutex.
static ENV_LOCK: Mutex<()> = Mutex::new(());

/// Every env var `FabricConfig::from_env` consults. Clearing this
/// full set before each env-sensitive test makes the test hermetic
/// against the CI runner's environment — e.g. an external
/// `CAIRN_FABRIC_WAITPOINT_HMAC_SECRET` value that fails HMAC
/// validation would otherwise surface as a spurious parse failure
/// long before our URL-parsing assertions run. Mirrors the
/// `clear_fabric_env` helper in `src/config.rs::tests`.
const ALL_FABRIC_ENV_VARS: &[&str] = &[
    "CAIRN_FABRIC_URL",
    "CAIRN_FABRIC_HOST",
    "CAIRN_FABRIC_PORT",
    "CAIRN_FABRIC_TLS",
    "CAIRN_FABRIC_CLUSTER",
    "CAIRN_FABRIC_LANE",
    "CAIRN_FABRIC_WORKER_ID",
    "CAIRN_FABRIC_INSTANCE_ID",
    "CAIRN_FABRIC_NAMESPACE",
    "CAIRN_FABRIC_LEASE_TTL_MS",
    "CAIRN_FABRIC_GRANT_TTL_MS",
    "CAIRN_FABRIC_MAX_TASKS",
    "CAIRN_FABRIC_SIGNAL_DEDUP_TTL_MS",
    "CAIRN_FABRIC_FCALL_TIMEOUT_MS",
    "CAIRN_FABRIC_WORKER_CAPABILITIES",
    "CAIRN_FABRIC_WAITPOINT_HMAC_SECRET",
    "CAIRN_FABRIC_WAITPOINT_HMAC_KID",
];

fn clear_all_fabric_env() {
    for key in ALL_FABRIC_ENV_VARS {
        std::env::remove_var(key);
    }
}

/// Asserts that the crate's backend-agnostic re-exports match the
/// shapes downstream callers rely on. Every symbol referenced here
/// must resolve from an always-on (non-`fabric-valkey`) module.
#[test]
fn backend_agnostic_surface_is_available() {
    // ── Error type: always-on ────────────────────────────────────
    let err: FabricError = FabricError::Config("test".into());
    assert!(err.to_string().contains("test"));

    // `FabricError` is re-exported at crate root (matches existing
    // `use cairn_fabric::FabricError;` callers in cairn-app).
    let _reexport: ReexportedError = FabricError::Validation {
        reason: "test".into(),
    };

    // ── Config parsers: always-on ────────────────────────────────
    // `FabricConfig::from_env` is callable without a live backend;
    // the default (no env var) resolves to a Valkey shape because
    // `BackendConfig::valkey(..)` is a FF type. Critically, we do
    // NOT call `config.client_builder()` here — that method is
    // gated behind `fabric-valkey` and the gate is the whole point
    // of this file.
    let _env_guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_all_fabric_env();
    let config = FabricConfig::from_env().expect("from_env on defaults must succeed");
    assert_eq!(config.lease_ttl_ms, 180_000);
    drop(_env_guard);

    // ── Helpers (ferriskey-free): always-on ──────────────────────
    let _ms: u64 = now_ms();
    assert_eq!(sanitize_signal_component("a:b:c"), "a_b_c");
    assert_eq!(
        parse_public_state("active"),
        flowfabric::core::state::PublicState::Active
    );
    let pk = try_parse_project_key("t/w/p").expect("t/w/p parses");
    assert_eq!(pk.tenant_id.as_str(), "t");

    // ── fcall name constants: always-on ──────────────────────────
    // These are `pub const &str`s used by cairn-app + the future
    // Postgres backend. Reference a few to anchor the contract.
    let _create: &str = names::FF_CREATE_EXECUTION;
    let _deliver: &str = names::FF_DELIVER_SIGNAL;

    // ── Engine + ControlPlaneBackend traits: always-on ──────────
    // We cannot construct a `ValkeyEngine` here (it is gated), but
    // we can take the trait as a type parameter — any future
    // PostgreSQL impl must satisfy these same bounds.
    fn assert_dyn_compatible<T: ?Sized>() {}
    assert_dyn_compatible::<dyn Engine>();
    assert_dyn_compatible::<dyn ControlPlaneBackend>();
    assert_dyn_compatible::<ExecutionSnapshot>();

    // ── id_map / state_map: always-on ────────────────────────────
    // `id_map::project_to_lane` is backend-agnostic — pure UUID v5
    // derivation. Used by both cairn-app and (future) PR-C Postgres.
    let lane = cairn_fabric::id_map::project_to_lane(&pk);
    assert!(!lane.as_str().is_empty());
}

/// Anchors the public surface of the `fcall` ARGV-builder module.
/// The builders are pure logic (no ferriskey, no FF backend types)
/// and must stay referenceable from code that does not enable
/// `fabric-valkey`, so PR-C's Postgres backend can reuse them
/// unchanged.
#[test]
fn fcall_argv_builders_are_backend_agnostic() {
    // Take a function pointer to one builder to force its full
    // signature into the test-binary compile graph. If a ferriskey
    // type leaks into any `fcall::*` builder in the future, this
    // reference stops resolving under the backend-agnostic surface.
    let _builder = cairn_fabric::fcall::budget::build_create_budget;
    // `verify_builder_counts` is the always-on debug-assert guard
    // on every `FabricRuntime::fcall` call site. Reference it so a
    // ferriskey type sneaking into its signature breaks this test.
    let _verify = cairn_fabric::fcall::verify_builder_counts;
}

/// `FabricConfig::from_env` parsing — the Valkey URL shape — is
/// backend-agnostic env parsing and must stay referenceable from
/// code that does not enable `fabric-valkey`. PR-A left the
/// `postgres://` scheme rejected until PR-C wires the Postgres
/// backend; we assert both arms here to lock the env-var surface
/// shape.
#[test]
fn fabric_config_url_parsing_stable_without_valkey_feature() {
    // Cairn's `config::tests::ENV_LOCK` gates concurrent
    // CAIRN_FABRIC_URL mutation across the crate's own tests; this
    // file runs as a separate test binary so we do not share that
    // lock. Use our own binary-scoped lock above, and clear the full
    // CAIRN_FABRIC_* surface so a stray env var (e.g. a malformed
    // HMAC secret from the CI runner) cannot fail `from_env` before
    // our URL-parsing assertions run.
    let _env_guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_all_fabric_env();

    // 1. Valkey URL round-trips — operator-facing happy path.
    std::env::set_var("CAIRN_FABRIC_URL", "valkey://valkey.example:6380");
    let config = FabricConfig::from_env().expect("valkey URL parses");
    match &config.backend.connection {
        flowfabric::core::backend::BackendConnection::Valkey(vk) => {
            assert_eq!(vk.host, "valkey.example");
            assert_eq!(vk.port, 6380);
        }
        other => panic!("expected Valkey backend, got {other:?}"),
    }

    // 2. `postgres://` stays rejected at the URL parser until PR-C
    //    ships. The rejection message is operator-facing — assert
    //    its shape so a future parser reshape does not silently
    //    change the error operators see. Matches the negative
    //    assertion in `config::tests::rejects_postgres_url_scheme`.
    std::env::set_var(
        "CAIRN_FABRIC_URL",
        "postgres://cairn:secret@localhost:5432/cairn_fabric",
    );
    let err = FabricConfig::from_env().expect_err("postgres URL still rejected pre-PR-C");
    let msg = err.to_string();
    assert!(
        msg.contains("unknown fabric URL scheme: postgres"),
        "rejection message must name the scheme: {msg}"
    );

    std::env::remove_var("CAIRN_FABRIC_URL");
}
