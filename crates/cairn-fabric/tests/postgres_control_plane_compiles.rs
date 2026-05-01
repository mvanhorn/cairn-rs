//! Compile-only sentinel for PR-C3. Proves the `PostgresControlPlane`
//! stub type is `pub`-exported from `cairn_fabric::engine` when the
//! `fabric-postgres` feature is active. If the type is removed or moved
//! behind a different gate, this test binary stops compiling — failing
//! loud before the stub ever runs.
//!
//! PR-C4 replaces this file with real integration assertions against a
//! live Postgres testcontainer.

#![cfg(feature = "fabric-postgres")]

use cairn_fabric::engine::PostgresControlPlane;

#[test]
fn postgres_control_plane_type_exists() {
    // Compile-only proof: if `PostgresControlPlane` is not pub-exported
    // from `cairn_fabric::engine`, the `use` above fails. The `TypeId`
    // lookup is a no-op that just keeps the symbol live under `-D warnings`.
    let _ = std::any::TypeId::of::<PostgresControlPlane>();
}
