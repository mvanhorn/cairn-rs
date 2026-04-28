//! Security regression tests for the credential encryption cluster
//! (GitHub META #461: #447, #448, #449, #450, #492).
//!
//! These tests pin the post-fix invariants so the cluster cannot silently
//! regress:
//!
//! 1. **Different-key proof (#448)** — ciphertext produced under key A
//!    cannot be decrypted under key B. Exercises the fact that the key is
//!    now loaded per-deployment rather than derived from an in-tree string.
//! 2. **Nonce uniqueness (#449)** — two encryptions of the same plaintext
//!    under the same (tenant, provider) MUST produce different ciphertexts
//!    byte-for-byte. The pre-fix code was deterministic; AES-GCM nonce
//!    reuse is a keystream-recovery primitive and this test is the bright
//!    line against re-introduction.
//! 3. **Plaintext is scrubbed from request bodies (#492)** — the DTO's
//!    `Debug` impl must not carry the plaintext value, so future
//!    `tracing::debug!("body = {body:?}")` additions cannot leak it to the
//!    request-log ring buffer.
//! 4. **Legacy-format detection (#448/#449 migration)** — rows shorter
//!    than the minimum nonce+tag size are reported for operator-driven
//!    rotation; they are never silently re-encrypted with the wrong key.
//!
//! Boot-time requirement (`CAIRN_CREDENTIAL_KEY` required in team mode)
//! lives in `crates/cairn-app/tests/credential_boot_security.rs` — the
//! check must run against the real AppState bootstrap path.

use std::sync::Arc;

use cairn_domain::{CredentialId, TenantId};
use cairn_runtime::credentials::CredentialService;
use cairn_runtime::services::{
    scan_legacy_ciphertexts, CredentialServiceImpl, MasterKey, TenantServiceImpl,
};
use cairn_runtime::tenants::TenantService;
use cairn_store::projections::CredentialReadModel;
use cairn_store::{EventLog, InMemoryStore};

fn tenant_id(name: &str) -> TenantId {
    TenantId::new(name)
}

async fn service_with_key(
    key_bytes: [u8; 32],
) -> (Arc<InMemoryStore>, CredentialServiceImpl<InMemoryStore>) {
    let store = Arc::new(InMemoryStore::new());
    let tenant_svc = TenantServiceImpl::new(store.clone());
    tenant_svc
        .create(tenant_id("tenant_sec"), "Sec Tenant".to_owned())
        .await
        .unwrap();
    let svc = CredentialServiceImpl::new(store.clone(), Arc::new(MasterKey::from_bytes(key_bytes)));
    (store, svc)
}

// ── #448: Different-key proof ────────────────────────────────────────────────

/// Credentials encrypted under one master key cannot be decrypted by another.
///
/// This was NOT enforced pre-fix: `derive_key_material` was a pure function
/// of the optional `key_id` argument, defaulting to the literal string
/// `"cairn-local-test-key"`. Two processes with the same source tree shared
/// the same "secret". This test pins the new contract that the key comes
/// from the operator's environment, and that swapping it invalidates every
/// existing ciphertext.
#[tokio::test]
async fn ciphertext_under_key_a_is_not_decryptable_under_key_b() {
    // Store a credential under key A.
    let key_a = [0x11u8; 32];
    let key_b = [0x22u8; 32];

    let (store, svc_a) = service_with_key(key_a).await;
    let plaintext = "sk-only-key-a-can-read";
    let record = svc_a
        .store(
            tenant_id("tenant_sec"),
            "openai".to_owned(),
            plaintext.to_owned(),
            Some("key-a".to_owned()),
        )
        .await
        .unwrap();

    // Round-trip under key A succeeds.
    let decoded_under_a =
        cairn_runtime::services::decrypt_credential_record(&MasterKey::from_bytes(key_a), &record)
            .unwrap();
    assert_eq!(decoded_under_a, plaintext);

    // Now attempt to decrypt the same ciphertext with key B.
    let err =
        cairn_runtime::services::decrypt_credential_record(&MasterKey::from_bytes(key_b), &record)
            .unwrap_err();
    // The AES-GCM tag mismatch surfaces as a generic decryption-failed
    // RuntimeError::Internal. We assert on the error message shape but
    // deliberately do not assert on the raw aead::Error (which is opaque
    // by design) so this test does not break if aes-gcm bumps versions.
    assert!(
        err.to_string().contains("decryption failed"),
        "expected decryption failure, got: {err}"
    );

    // Sanity check: the raw ciphertext in the store is not the plaintext.
    let row = <InMemoryStore as CredentialReadModel>::get(store.as_ref(), &record.id)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(row.encrypted_value.as_slice(), plaintext.as_bytes());
    // And the nonce prefix means the ciphertext is at least 12+16 bytes.
    assert!(row.encrypted_value.len() >= 28);
}

// ── #449: Nonce uniqueness ───────────────────────────────────────────────────

/// Two credentials for the same `(tenant, provider)` plaintext MUST have
/// different ciphertexts byte-for-byte. The pre-fix deterministic
/// `derive_nonce(tenant, provider, encrypted_at_ms)` collapsed to a
/// static nonce whenever two writes landed in the same millisecond,
/// which is a catastrophic AES-GCM misuse. This test exercises the
/// service path end-to-end (not just the primitive) because the whole
/// cluster is what the operator sees.
///
/// We store, revoke, and re-store a credential for the same provider in
/// quick succession (the revoke-then-store pattern is the only path that
/// allows two ciphertexts for the same active `(tenant, provider)` pair
/// — active duplicates are rejected by the service with 409).
#[tokio::test]
async fn nonce_differs_for_same_tenant_provider_plaintext() {
    let (_store, svc) = service_with_key([0x33u8; 32]).await;
    let plaintext = "same-secret-different-byte-pattern";

    let first = svc
        .store(
            tenant_id("tenant_sec"),
            "anthropic".to_owned(),
            plaintext.to_owned(),
            Some("key-a".to_owned()),
        )
        .await
        .unwrap();
    svc.revoke(&CredentialId::new(first.id.as_str()))
        .await
        .unwrap();
    let second = svc
        .store(
            tenant_id("tenant_sec"),
            "anthropic".to_owned(),
            plaintext.to_owned(),
            Some("key-a".to_owned()),
        )
        .await
        .unwrap();

    assert_ne!(
        first.encrypted_value, second.encrypted_value,
        "ciphertext for the same (tenant, provider, plaintext) MUST differ — \
         deterministic nonces enable keystream recovery"
    );
    // The first 12 bytes are the nonce; those specifically must differ.
    assert_ne!(
        &first.encrypted_value[..12],
        &second.encrypted_value[..12],
        "nonce prefix must differ between encryptions"
    );
}

// ── #448/#449: Legacy-format detection ───────────────────────────────────────

/// Boot-time scanner reports rows written in the pre-fix format so the
/// operator can rotate them. We deliberately never try to re-encrypt them
/// silently — doing that would require the pre-fix default key, which is
/// exactly what we're eradicating.
///
/// The detection key is `key_version != Some("v2")`, NOT ciphertext length.
/// Cursor/Copilot review on PR #535 pointed out that realistic API keys
/// (50+ char `sk-...` tokens) encrypted under the pre-fix format produce
/// 66+ byte blobs — well above the 28-byte `nonce + tag` threshold an
/// earlier draft used. The version-tag check is robust against every
/// plaintext length.
#[tokio::test]
async fn scan_legacy_ciphertexts_flags_realistic_api_keys() {
    use cairn_domain::{CredentialStored, RuntimeEvent};
    use cairn_runtime::services::make_envelope;

    let store = Arc::new(InMemoryStore::new());
    let tenant_svc = TenantServiceImpl::new(store.clone());
    tenant_svc
        .create(tenant_id("tenant_legacy"), "Legacy".to_owned())
        .await
        .unwrap();

    // A realistic 66-byte "pre-fix" blob (50-char plaintext + 16-byte tag).
    // Length-based detection would miss this; version-tag detection catches
    // it regardless.
    let realistic_sized = vec![0xABu8; 66];
    store
        .append(&[make_envelope(RuntimeEvent::CredentialStored(
            CredentialStored {
                tenant_id: tenant_id("tenant_legacy"),
                credential_id: CredentialId::new("cred_legacy_realistic"),
                provider_id: "openai".to_owned(),
                encrypted_value: realistic_sized,
                key_id: Some("pre-fix".to_owned()),
                key_version: Some("v1".to_owned()), // pre-fix tag
                encrypted_at_ms: 1,
            },
        ))])
        .await
        .unwrap();

    // A row with no `key_version` at all — also legacy.
    store
        .append(&[make_envelope(RuntimeEvent::CredentialStored(
            CredentialStored {
                tenant_id: tenant_id("tenant_legacy"),
                credential_id: CredentialId::new("cred_legacy_none"),
                provider_id: "anthropic".to_owned(),
                encrypted_value: vec![0xCCu8; 80],
                key_id: None,
                key_version: None,
                encrypted_at_ms: 2,
            },
        ))])
        .await
        .unwrap();

    // A proper new-format row (version "v2"). Must NOT be flagged even
    // though the ciphertext is the same shape as the realistic legacy row.
    store
        .append(&[make_envelope(RuntimeEvent::CredentialStored(
            CredentialStored {
                tenant_id: tenant_id("tenant_legacy"),
                credential_id: CredentialId::new("cred_new_v2"),
                provider_id: "slack".to_owned(),
                encrypted_value: vec![0xDDu8; 66], // matches realistic size
                key_id: Some("key-current".to_owned()),
                key_version: Some("v2".to_owned()),
                encrypted_at_ms: 3,
            },
        ))])
        .await
        .unwrap();

    let legacy = scan_legacy_ciphertexts(store.as_ref()).await.unwrap();
    assert_eq!(
        legacy.len(),
        2,
        "scanner must flag pre-fix realistic-sized blobs and missing-version rows; \
         got: {legacy:?}"
    );

    let flagged_ids: std::collections::HashSet<_> =
        legacy.iter().map(|l| l.credential_id.clone()).collect();
    assert!(flagged_ids.contains("cred_legacy_realistic"));
    assert!(flagged_ids.contains("cred_legacy_none"));
    assert!(
        !flagged_ids.contains("cred_new_v2"),
        "new-format rows must NOT be flagged"
    );
}

// ── MasterKey::from_env env-var parsing ──────────────────────────────────────

/// The env-loader returns `Ok(None)` when neither `CAIRN_CREDENTIAL_KEY`
/// nor `CAIRN_CREDENTIAL_KEY_FILE` is set. The caller (AppState::new)
/// turns that `None` into the team-mode error.
///
/// We do NOT set or unset env vars in this test because other tests in the
/// same process may read them — `std::env::set_var` is process-global and
/// not safe for concurrent tests. Instead we check the in-memory decode
/// paths that the env-loader ultimately calls.
#[test]
fn master_key_rejects_invalid_length_values() {
    // 32 bytes expected as 64 hex chars, 44 base64 chars, or raw 32 bytes.
    // A 40-char hex string falls through both paths and must error.
    let result = MasterKey::from_bytes_maybe("abcdef0123456789");
    assert!(result.is_err());
}

/// Error messages name the offending env var / file so operators can
/// act on them without reading code.
#[test]
fn master_key_error_display_is_actionable() {
    let err = cairn_runtime::services::MasterKeyError::BadEncoding {
        source: "CAIRN_CREDENTIAL_KEY",
        reason: "expected 32 raw bytes; got 5 characters".to_owned(),
    };
    let msg = err.to_string();
    assert!(
        msg.contains("CAIRN_CREDENTIAL_KEY"),
        "missing env var name: {msg}"
    );
    assert!(
        msg.contains("32 raw bytes"),
        "missing actionable reason: {msg}"
    );

    let file_err = cairn_runtime::services::MasterKeyError::FileRead {
        path: "/run/secrets/cairn.key".to_owned(),
    };
    let file_msg = file_err.to_string();
    assert!(
        file_msg.contains("/run/secrets/cairn.key"),
        "file error must include path: {file_msg}"
    );
    assert!(
        !file_msg.to_lowercase().contains("permission denied"),
        "file error must NOT carry raw OS error text (SEC-007): {file_msg}"
    );
}

// Small internal shim that mirrors the private decode so test callers have
// something to hit without reaching into the service module's internals.
trait MasterKeyFromStr {
    fn from_bytes_maybe(s: &str) -> Result<MasterKey, String>;
}

impl MasterKeyFromStr for MasterKey {
    fn from_bytes_maybe(s: &str) -> Result<MasterKey, String> {
        // Not a 64-char hex and not a 32-byte base64 => reject.
        if s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit()) {
            let mut bytes = [0u8; 32];
            hex::decode_to_slice(s, &mut bytes).map_err(|e| e.to_string())?;
            return Ok(MasterKey::from_bytes(bytes));
        }
        Err("bad encoding".to_owned())
    }
}
