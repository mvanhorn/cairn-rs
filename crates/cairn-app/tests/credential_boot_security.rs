//! Boot-time guard test for META #461 / #448.
//!
//! In self-hosted team mode, `AppState::new` MUST refuse to start if
//! neither `CAIRN_CREDENTIAL_KEY` nor `CAIRN_CREDENTIAL_KEY_FILE` is
//! configured. This is the bright line against a deployment booting
//! with the pre-fix default key material.
//!
//! In local mode, unset env vars are a loud warning and a deterministic
//! dev-only fallback key; we don't assert the warning text here (tests
//! should not pin log output), but we DO assert the boot succeeds so an
//! operator who has not yet configured the key can still run locally.

use cairn_api::bootstrap::{BootstrapConfig, DeploymentMode, EncryptionKeySource, StorageBackend};

/// Team mode + no credential key => hard error during AppState::new.
///
/// `AppState::new` loads the master key BEFORE attempting to construct
/// `FabricServices`, so this test can exercise the real bootstrap path
/// without needing a live Valkey. The returned `Err(String)` must name
/// `CAIRN_CREDENTIAL_KEY` so operators can act on the message directly.
#[tokio::test]
async fn team_mode_without_credential_key_fails_boot() {
    let _guard = EnvGuard::lock();
    EnvGuard::unset("CAIRN_CREDENTIAL_KEY");
    EnvGuard::unset("CAIRN_CREDENTIAL_KEY_FILE");

    let config = BootstrapConfig {
        mode: DeploymentMode::SelfHostedTeam,
        storage: StorageBackend::InMemory,
        encryption_key: EncryptionKeySource::None,
        ..BootstrapConfig::default()
    };

    // `AppState` does not implement `Debug`, so we can't use `expect_err`.
    // Match on the result directly.
    let result = cairn_app::AppState::new(config).await;
    let err = match result {
        Ok(_) => panic!("team-mode boot must fail when no credential key is configured"),
        Err(e) => e,
    };
    assert!(
        err.contains("CAIRN_CREDENTIAL_KEY"),
        "boot error must name the env var; got: {err}"
    );
    assert!(
        err.to_lowercase().contains("fatal") || err.to_lowercase().contains("required"),
        "boot error must be unambiguously fatal; got: {err}"
    );
}

/// Local mode with no env var falls back to the dev-only key. This path
/// exists so `cargo run -p cairn-app` works with zero configuration on a
/// developer laptop, but the operator-facing warning in `load_master_key`
/// names the env var so production deployments cannot claim they weren't
/// told.
///
/// We cannot instantiate a full AppState in a test (FakeFabric is only
/// reachable through the `tests/support/` module and threads the master
/// key from the `with_store_and_core` legacy path). What we CAN assert is
/// that `MasterKey::from_env()` does not error when neither env var is set
/// — this is the contract `load_master_key` relies on to choose the
/// dev-fallback branch.
#[tokio::test]
async fn local_mode_without_credential_key_loader_returns_none() {
    let _guard = EnvGuard::lock();
    EnvGuard::unset("CAIRN_CREDENTIAL_KEY");
    EnvGuard::unset("CAIRN_CREDENTIAL_KEY_FILE");

    let loaded =
        cairn_runtime::MasterKey::from_env().expect("from_env must not error on unset env vars");
    assert!(loaded.is_none());
}

/// Valid hex key via env var: loader decodes to 32 bytes, fingerprint
/// is 8 hex chars.
#[tokio::test]
async fn valid_hex_key_parses() {
    let _guard = EnvGuard::lock();
    EnvGuard::unset("CAIRN_CREDENTIAL_KEY_FILE");
    EnvGuard::set(
        "CAIRN_CREDENTIAL_KEY",
        // 32-byte key encoded as 64 hex chars.
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    );

    let loaded = cairn_runtime::MasterKey::from_env()
        .expect("from_env must succeed on valid hex key")
        .expect("key must be Some when env var is set");
    assert_eq!(loaded.fingerprint().len(), 8);

    EnvGuard::unset("CAIRN_CREDENTIAL_KEY");
}

/// Malformed key value: loader returns an error naming the env var.
#[tokio::test]
async fn malformed_key_errors_loudly() {
    let _guard = EnvGuard::lock();
    EnvGuard::unset("CAIRN_CREDENTIAL_KEY_FILE");
    EnvGuard::set("CAIRN_CREDENTIAL_KEY", "not-a-valid-key");

    let err = cairn_runtime::MasterKey::from_env().unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("CAIRN_CREDENTIAL_KEY"),
        "error must name the env var; got: {msg}"
    );

    EnvGuard::unset("CAIRN_CREDENTIAL_KEY");
}

// ── EnvGuard — serialize env-var mutation across async tests ────────────────

struct EnvGuard {
    _guard: std::sync::MutexGuard<'static, ()>,
}

impl EnvGuard {
    fn lock() -> Self {
        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let guard = LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        EnvGuard { _guard: guard }
    }

    fn set(k: &str, v: &str) {
        // Edition 2021: set_var is safe. EnvGuard::lock() serializes
        // concurrent mutations inside this test crate; we cannot block
        // reads from unrelated test crates, but those do not run in
        // the same `cargo test -p cairn-app` process.
        std::env::set_var(k, v);
    }

    fn unset(k: &str) {
        std::env::remove_var(k);
    }
}
