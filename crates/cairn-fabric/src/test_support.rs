//! Shared test-only helpers for cairn-fabric unit tests.
//!
//! Gated on `#[cfg(test)]` so the symbols are only compiled into the
//! crate's unit-test binary. Integration tests in `tests/integration/`
//! cannot see `#[cfg(test)]` items (they compile against the library
//! crate without `cfg(test)`), so those files keep local helpers —
//! typically one that accepts `&TestHarness` so it threads the
//! cluster-wide `partition_config()` through.

use flowfabric::core::backend::BackendConfig;
use flowfabric::core::partition::PartitionConfig;
use flowfabric::core::types::{ExecutionId, LaneId};
use uuid::Uuid;

/// Default `BackendConfig` for unit tests that only need *some* backend
/// value populated (e.g. capability-threading tests that never hit the
/// backend at all).
///
/// Picks whichever backend is available under the current feature set.
/// Today that's always Valkey (cairn-fabric is still Valkey-only). When
/// PR-B lands the `valkey`/`postgres` cargo feature gate, this helper
/// is the single place to switch — unit tests that call it will Just
/// Work under `--no-default-features` or `--features postgres-only`
/// instead of breaking on a hard-coded `BackendConfig::valkey(..)`
/// literal in the test body (see issue #508).
pub(crate) fn default_test_backend() -> BackendConfig {
    BackendConfig::valkey("localhost", 6379)
}

/// Mint a deterministic-but-distinct `ExecutionId` for tests.
///
/// Uses `ExecutionId::deterministic_solo` with a UUID v5 derived from
/// `seed`. Distinct seeds produce distinct ExecutionIds, so FF's dedup
/// slot does NOT fire between them — which is the invariant every
/// spend-without-dedup test in the crate relies on. Tests that need a
/// pinned ExecutionId for dedup coverage just pass the same seed twice.
pub(crate) fn test_eid(seed: &str) -> ExecutionId {
    let uuid = Uuid::new_v5(&Uuid::NAMESPACE_DNS, seed.as_bytes());
    ExecutionId::deterministic_solo(&LaneId::new("test"), &PartitionConfig::default(), uuid)
}
