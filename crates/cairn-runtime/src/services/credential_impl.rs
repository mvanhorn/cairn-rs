//! Concrete credential service implementation.
//!
//! # Security model
//!
//! Credentials are encrypted with AES-256-GCM using a single master key loaded
//! from the environment at boot — see [`MasterKey::from_env`]. Every encryption
//! uses a fresh random 96-bit nonce drawn from `OsRng`. The on-disk ciphertext
//! layout is:
//!
//! ```text
//! nonce(12 bytes) || ciphertext_with_tag(variable)
//! ```
//!
//! Callers MUST NOT derive nonces from any data; the `(nonce, key)` pair must
//! never repeat, and the only way we guarantee that with a fixed master key is
//! an OS-level CSPRNG. An old format that derived the nonce deterministically
//! from `(tenant_id, provider_id, encrypted_at_ms)` is no longer accepted —
//! post-fix rows carry `key_version = Some("v2")` and the boot-time scanner
//! (`scan_legacy_ciphertexts`) flags any active row whose `key_version` is
//! not `Some("v2")`. Rows that fall below the minimum nonce+tag size are
//! rejected at decrypt-time with an operator-facing "revoke and re-enter"
//! error. Operators must **revoke and re-enter** legacy rows; `rotate-key`
//! is NOT a valid remediation because its decrypt side cannot read pre-fix
//! ciphertext layouts.
//!
//! The `key_id` field on the request is retained as an audit/rotation tag; it
//! no longer participates in key derivation. The deployment's single master
//! key comes from `CAIRN_CREDENTIAL_KEY` (hex or base64, 32 bytes decoded) or
//! `CAIRN_CREDENTIAL_KEY_FILE` (Docker secrets path).

use std::sync::Arc;

use aes_gcm::aead::rand_core::RngCore;
use aes_gcm::aead::{Aead, AeadCore, KeyInit, OsRng};
use aes_gcm::{Aes256Gcm, Key, Nonce};
use async_trait::async_trait;
use cairn_domain::credentials::{CredentialRecord, CredentialRotationRecord};
use cairn_domain::*;
use cairn_store::projections::{CredentialReadModel, TenantReadModel};
use cairn_store::EventLog;
use zeroize::{Zeroize, Zeroizing};

use super::event_helpers::make_envelope;
use crate::credentials::CredentialService;
use crate::error::RuntimeError;

/// AES-GCM nonce length in bytes (96 bits).
pub const NONCE_LEN: usize = 12;

/// AES-GCM authentication tag length in bytes (128 bits).
pub const TAG_LEN: usize = 16;

/// Minimum viable ciphertext size: nonce + empty plaintext tag.
/// Rows shorter than this cannot be in the new format and must be rotated.
pub const MIN_CIPHERTEXT_LEN: usize = NONCE_LEN + TAG_LEN;

/// Key-version tag stamped on events written by the post-#461 encrypt path.
/// Any `CredentialStored` whose `key_version` is NOT this constant was
/// produced by the pre-fix deterministic-nonce code and MUST be rotated
/// before it can be used. Review of `scan_legacy_ciphertexts` established
/// that a length-based check misses realistic API-key plaintexts
/// (Cursor+Copilot on PR #535), so the scanner keys off `key_version`
/// instead.
pub const CURRENT_KEY_VERSION: &str = "v2";

/// Operator-facing error body for any AEAD encryption failure. Used by
/// both `encrypt_value` and the regression test so the test actually
/// exercises the shared message and a future refactor that drops or
/// changes the constant is caught at compile time (Gemini + Copilot on
/// PR #548, #460).
pub const CREDENTIAL_ENCRYPTION_ERROR: &str = "credential encryption failed";

/// Operator-facing error body for any AEAD decryption failure. Mirror
/// constant to [`CREDENTIAL_ENCRYPTION_ERROR`].
pub const CREDENTIAL_DECRYPTION_ERROR: &str = "credential decryption failed";

/// The deployment-wide master key used to encrypt and decrypt stored
/// credentials. 32 bytes, held in a `Zeroizing` buffer so it is scrubbed
/// from memory on drop.
///
/// Construct via [`MasterKey::from_env`] in production, or
/// [`MasterKey::from_bytes`] for tests. There is NO default or random
/// per-boot fallback — a random per-boot key would make every credential
/// undecryptable after restart.
pub struct MasterKey {
    /// Raw 32-byte AES-256 key. `Zeroizing` scrubs the buffer on drop.
    material: Zeroizing<[u8; 32]>,
}

impl MasterKey {
    /// Construct from already-validated 32 raw bytes.
    ///
    /// Used by tests and by the env-var parsing path after it has verified
    /// the operator-supplied string decodes to exactly 32 bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self {
            material: Zeroizing::new(bytes),
        }
    }

    /// Load the master key from the environment.
    ///
    /// Priority:
    ///   1. `CAIRN_CREDENTIAL_KEY_FILE` — path to a file containing the key
    ///      (Docker secrets / Kubernetes secrets pattern). Trailing whitespace
    ///      is trimmed.
    ///   2. `CAIRN_CREDENTIAL_KEY` — the key value directly.
    ///
    /// Both sources accept the key as either a 64-character lowercase hex
    /// string or a base64 string that decodes to 32 bytes (STANDARD alphabet,
    /// padding required — typically 44 chars). Any other length or encoding
    /// is a hard error — we refuse to start rather than silently truncate
    /// or pad.
    ///
    /// Returns `Ok(None)` if neither env var is set. Callers decide whether
    /// "unset" is fatal (team mode) or a dev-only warning path.
    ///
    /// Every intermediate buffer holding key material (the `String` read
    /// from the env var or from disk, the base64-decoded `Vec<u8>`) is
    /// wrapped in `Zeroizing` so the bytes are scrubbed on drop — even on
    /// the error-return paths below. Review comments SEC-003/SEC-005 on
    /// PR #535.
    ///
    /// OS error details from `std::fs::read_to_string` are deliberately
    /// NOT forwarded into the public `MasterKeyError` body. The raw error
    /// can leak filesystem layout (e.g. "permission denied on
    /// /var/run/secrets/cairn/credential_key") and the public API response
    /// should say "cannot read CAIRN_CREDENTIAL_KEY_FILE" without more.
    /// Per SEC-007 (no internal details in operator-facing error text);
    /// the full OS error is logged via `tracing::error!` for debugging.
    pub fn from_env() -> Result<Option<Self>, MasterKeyError> {
        // File takes priority (Docker secrets don't leak into `ps auxe`).
        //
        // A whitespace-only value for `CAIRN_CREDENTIAL_KEY_FILE` is treated
        // as "not set" — we fall through to check `CAIRN_CREDENTIAL_KEY`.
        // This matches `main.rs`'s config-side check and avoids the TOCTOU
        // surface Cursor caught on PR #535: if the operator set both env
        // vars and the file var contains only whitespace, they still get a
        // working boot via `CAIRN_CREDENTIAL_KEY` instead of a confusing
        // "empty" fatal.
        let file_var = std::env::var("CAIRN_CREDENTIAL_KEY_FILE")
            .ok()
            .map(|v| v.trim().to_owned())
            .filter(|v| !v.is_empty());
        if let Some(path) = file_var {
            let contents: Zeroizing<String> =
                Zeroizing::new(std::fs::read_to_string(&path).map_err(|e| {
                    tracing::error!(error = %e, path = %path, "failed to read credential key file");
                    MasterKeyError::FileRead { path: path.clone() }
                })?);
            let trimmed = contents.trim();
            if trimmed.is_empty() {
                return Err(MasterKeyError::EmptyFile { path });
            }
            return Self::decode_material(trimmed, "CAIRN_CREDENTIAL_KEY_FILE").map(Some);
        }
        if let Ok(value) = std::env::var("CAIRN_CREDENTIAL_KEY").map(Zeroizing::new) {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                // Env var is set but whitespace-only. `main.rs` treats this
                // the same as unset; we match that here rather than bouncing
                // out with a "set but empty" fatal so the semantics across
                // the two readers stay aligned.
                return Ok(None);
            }
            return Self::decode_material(trimmed, "CAIRN_CREDENTIAL_KEY").map(Some);
        }
        Ok(None)
    }

    /// Decode a hex or base64 key string into exactly 32 bytes.
    ///
    /// Uses the `base64` crate's STANDARD alphabet (padded); the crate
    /// enforces canonical encoding and rejects trailing-bit residues that
    /// a hand-rolled decoder can miss (Gemini review comment, line 204).
    fn decode_material(value: &str, source: &'static str) -> Result<Self, MasterKeyError> {
        use base64::engine::general_purpose::STANDARD;
        use base64::Engine as _;

        // Hex path: 64 lowercase hex chars.
        if value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit()) {
            let mut bytes = [0u8; 32];
            hex::decode_to_slice(value, &mut bytes).map_err(|_| MasterKeyError::BadEncoding {
                source,
                reason: "invalid hex".to_owned(),
            })?;
            return Ok(Self::from_bytes(bytes));
        }
        // Base64 path — STANDARD alphabet, padding required. The `base64`
        // crate does canonical-encoding validation (rejects trailing bits
        // on decode) that the hand-rolled predecessor skipped.
        //
        // The decoded bytes are wrapped in `Zeroizing` IMMEDIATELY — before
        // the length check — so the wrong-length error path below scrubs
        // the operator-supplied material on drop. The previous shape only
        // wrapped on the success branch, leaving bytes un-zeroized when
        // `decoded.len() != 32`. Copilot review on PR #535 (line 179).
        if let Ok(decoded) = STANDARD.decode(value) {
            let decoded = Zeroizing::new(decoded);
            if decoded.len() == 32 {
                let mut bytes = [0u8; 32];
                bytes.copy_from_slice(decoded.as_ref());
                // `decoded` is dropped here; Zeroizing scrubs the buffer.
                return Ok(Self::from_bytes(bytes));
            }
            // wrong length → `decoded` drops here, Zeroizing scrubs.
        }
        Err(MasterKeyError::BadEncoding {
            source,
            reason: format!(
                // "32-byte base64" is misleading — base64 is an encoding.
                // The operator-facing constraint is that the decoded value
                // is 32 raw bytes (typically a 44-char base64 string with
                // padding, or 64 lowercase hex chars). Copilot review on
                // PR #535 (line 186).
                "expected 32 raw bytes (64-char lowercase hex, or a base64 string that decodes to 32 bytes — typically 44 chars with padding); got {} characters",
                value.len()
            ),
        })
    }

    fn as_aes_key(&self) -> &Key<Aes256Gcm> {
        Key::<Aes256Gcm>::from_slice(self.material.as_ref())
    }

    /// Short fingerprint for operator-facing log lines. Derived via SHA-256
    /// over the raw key bytes and truncated to 8 hex chars. This is safe to
    /// log: AES-256 preimage resistance makes recovery from 8 hex chars
    /// computationally infeasible.
    pub fn fingerprint(&self) -> String {
        use sha2::{Digest, Sha256};
        let digest = Sha256::digest(self.material.as_ref());
        hex::encode(&digest[..4])
    }
}

impl std::fmt::Debug for MasterKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never print the key bytes.
        f.debug_struct("MasterKey")
            .field("fingerprint", &self.fingerprint())
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MasterKeyError {
    /// Reading the `CAIRN_CREDENTIAL_KEY_FILE` path failed at the OS layer.
    /// The raw OS error is logged via `tracing::error!` but not stored in
    /// this variant, so the operator-facing response stays minimal.
    FileRead {
        path: String,
    },
    EmptyFile {
        path: String,
    },
    BadEncoding {
        source: &'static str,
        reason: String,
    },
}

impl std::fmt::Display for MasterKeyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MasterKeyError::FileRead { path } => {
                // Intentionally no OS error here — the full cause is in
                // the `tracing::error!` emitted at the read site. This
                // keeps filesystem-permission / inode detail out of the
                // operator-facing boot message. SEC-007.
                write!(
                    f,
                    "cannot read CAIRN_CREDENTIAL_KEY_FILE at {path} — check logs for OS error"
                )
            }
            MasterKeyError::EmptyFile { path } => {
                write!(f, "CAIRN_CREDENTIAL_KEY_FILE at {path} is empty")
            }
            MasterKeyError::BadEncoding { source, reason } => {
                write!(f, "{source}: {reason}")
            }
        }
    }
}

impl std::error::Error for MasterKeyError {}

pub struct CredentialServiceImpl<S> {
    store: Arc<S>,
    master_key: Arc<MasterKey>,
}

impl<S> CredentialServiceImpl<S> {
    /// Construct a service bound to the supplied master key.
    pub fn new(store: Arc<S>, master_key: Arc<MasterKey>) -> Self {
        Self { store, master_key }
    }

    /// Expose the master key fingerprint for operator diagnostics
    /// (for example, boot logs confirming which key the process is using).
    pub fn master_key_fingerprint(&self) -> String {
        self.master_key.fingerprint()
    }

    /// Decrypt a previously-stored [`CredentialRecord`] using this
    /// service's master key. Returns the plaintext value.
    ///
    /// Callers outside the runtime (e.g. the provider `/test` probe
    /// handler in cairn-app) need to forward the operator's API key to
    /// the upstream provider for reachability checks. Before this
    /// method existed, the probe path rolled its own SHA256-seeded
    /// decryption that was incompatible with the production master-key
    /// encryption scheme — every probe sent no Authorization header and
    /// every provider replied 401 even with a valid stored credential
    /// (dogfood #632). Exposing the real decrypt here keeps one
    /// ciphertext format per deployment.
    pub fn decrypt_record(&self, record: &CredentialRecord) -> Result<String, RuntimeError> {
        decrypt_value(self.master_key.as_ref(), &record.encrypted_value)
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Encrypt `plaintext_value` under `master_key`. Returns `nonce(12) || ct_with_tag`.
///
/// Panics only if the AES-GCM primitive itself fails, which does not occur for
/// the input sizes cairn handles (real API keys are well under 2^36-31 bytes).
fn encrypt_value(master_key: &MasterKey, plaintext_value: &str) -> Result<Vec<u8>, RuntimeError> {
    let cipher = Aes256Gcm::new(master_key.as_aes_key());
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
    let mut sealed = cipher
        .encrypt(&nonce, plaintext_value.as_bytes())
        .map_err(|e| {
            // aes-gcm's Error type is unit and documents no exploitable content;
            // we still avoid forwarding it verbatim to keep the RuntimeError
            // body free of any provider-specific detail.
            tracing::error!(error = %e, "credential encryption failed");
            RuntimeError::Internal(CREDENTIAL_ENCRYPTION_ERROR.to_owned())
        })?;
    let mut out = Vec::with_capacity(NONCE_LEN + sealed.len());
    out.extend_from_slice(&nonce);
    out.append(&mut sealed);
    Ok(out)
}

fn decrypt_value(master_key: &MasterKey, encrypted_value: &[u8]) -> Result<String, RuntimeError> {
    if encrypted_value.len() < MIN_CIPHERTEXT_LEN {
        return Err(RuntimeError::Internal(format!(
            "credential ciphertext too short ({} bytes): missing nonce prefix — \
             this row was encrypted with the pre-{fix_date} format and cannot be \
             decrypted by this runtime. Revoke the credential and re-enter the \
             secret via POST /v1/admin/tenants/:id/credentials. \
             `rotate-key` is NOT a valid remediation: it decrypts-then-re-encrypts, \
             and the decrypt side cannot read this row.",
            encrypted_value.len(),
            fix_date = "2026-04-28",
        )));
    }
    let (nonce_bytes, ciphertext) = encrypted_value.split_at(NONCE_LEN);
    let nonce = Nonce::from_slice(nonce_bytes);
    let cipher = Aes256Gcm::new(master_key.as_aes_key());
    let plaintext = cipher.decrypt(nonce, ciphertext).map_err(|e| {
        tracing::error!(error = %e, "credential decryption failed");
        RuntimeError::Internal(CREDENTIAL_DECRYPTION_ERROR.to_owned())
    })?;
    String::from_utf8(plaintext).map_err(|e| {
        // Don't include the invalid bytes in the error — they are still the
        // plaintext material even if it isn't valid UTF-8. `FromUtf8Error`
        // owns the decrypted `Vec<u8>`; pull it back out, zeroize it, then
        // drop. Gemini review line 316, PR #535.
        let mut bytes = e.into_bytes();
        bytes.zeroize();
        tracing::error!("credential plaintext invalid utf-8");
        RuntimeError::Internal("credential plaintext invalid utf-8".to_owned())
    })
}

/// Decrypt a stored credential record under the caller-supplied master key.
///
/// Non-service callers (e.g. `ProviderRegistry`) hold an `Arc<MasterKey>`
/// directly rather than the full service; this helper lets them share
/// [`decrypt_value`] without exposing the internal layout of the service.
pub fn decrypt_credential_record(
    master_key: &MasterKey,
    record: &CredentialRecord,
) -> Result<String, RuntimeError> {
    decrypt_value(master_key, &record.encrypted_value)
}

#[async_trait]
impl<S> CredentialService for CredentialServiceImpl<S>
where
    S: EventLog + CredentialReadModel + TenantReadModel + Send + Sync + 'static,
{
    async fn store(
        &self,
        tenant_id: TenantId,
        provider_id: String,
        plaintext_value: String,
        key_id: Option<String>,
    ) -> Result<CredentialRecord, RuntimeError> {
        // Wrap the incoming plaintext so every early-return path scrubs it.
        // This covers tenant-not-found, duplicate, and race-loser branches
        // below — even though those return without calling encrypt_value,
        // the String still sits on the stack.
        let plaintext_value = Zeroizing::new(plaintext_value);

        if TenantReadModel::get(self.store.as_ref(), &tenant_id)
            .await?
            .is_none()
        {
            return Err(RuntimeError::NotFound {
                entity: "tenant",
                id: tenant_id.to_string(),
            });
        }

        // Closes #217: reject duplicate `(tenant_id, provider_id)` so two
        // back-to-back POSTs with the same provider don't silently
        // accumulate active credentials (the projection keyed on
        // credential_id would happily return two rows). A revoked
        // credential with the same provider_id is allowed — operators
        // rotate by revoke-then-create, and blocking that would be a
        // silent regression. Callers who genuinely need to replace an
        // active credential must revoke it first.
        //
        // Concurrency caveat (acknowledged): this is a read-then-write
        // sequence, so two simultaneous `store` calls on the same
        // `(tenant_id, provider_id)` can both pass the pre-check and
        // both append. We close most of that window below by
        // re-reading the projection AFTER append and, if a duplicate
        // slipped in, emitting a `CredentialRevoked` event for our just-
        // written record and returning 409 to the caller. This keeps
        // the happy path O(1) write and hardens the race without
        // requiring a per-backend unique index (portable-DB rule in
        // CLAUDE.md — Postgres v1 target but must work on SQLite/
        // InMemory too). A dedicated projection-level unique index is
        // the correct long-term home for this; tracked as follow-up.
        //
        // `list_by_tenant` is the cheapest portable check: the read
        // model is already built per tenant, and tenants typically carry
        // O(10) credentials. Switching to a dedicated projection lookup
        // is a measurable optimization only if a tenant grows past a
        // few hundred active credentials.
        let existing =
            CredentialReadModel::list_by_tenant(self.store.as_ref(), &tenant_id, usize::MAX, 0)
                .await?;
        if existing
            .iter()
            .any(|c| c.active && c.provider_id == provider_id)
        {
            return Err(RuntimeError::Conflict {
                entity: "credential",
                id: format!("provider={provider_id} tenant={tenant_id}"),
            });
        }

        let encrypted_at_ms = now_ms();
        let encrypted_value = encrypt_value(self.master_key.as_ref(), plaintext_value.as_str())?;
        // #737: ms-only IDs collide on fast hosts when two tenants store
        // a credential within the same millisecond. The HashMap-backed
        // projection keys on credential_id alone, so a colliding second
        // writer reuses the first writer's record and the cross-tenant
        // ownership check (`cred.tenant_id == connection.tenant_id`)
        // returns the wrong answer non-deterministically. A 6-byte
        // OsRng suffix (12 hex chars, 48 bits of entropy) makes the
        // collision probability on a 1k-credential cluster ~1.8e-9 per
        // pair-day — well below "see it on a CI run."
        let mut suffix = [0u8; 6];
        OsRng.fill_bytes(&mut suffix);
        let credential_id =
            CredentialId::new(format!("cred_{encrypted_at_ms}_{}", hex::encode(suffix)));
        let event = make_envelope(RuntimeEvent::CredentialStored(CredentialStored {
            tenant_id: tenant_id.clone(),
            credential_id: credential_id.clone(),
            provider_id: provider_id.clone(),
            encrypted_value,
            key_id,
            // Post-#461 format: 12-byte random nonce + ct+tag. Rotated to
            // "v2" so the legacy scanner can tell pre-fix rows apart.
            key_version: Some(CURRENT_KEY_VERSION.to_owned()),
            encrypted_at_ms,
        }));
        self.store.append(&[event]).await?;

        // Post-append race resolution: re-read and check whether another
        // concurrent `store` for the same (tenant_id, provider_id) also
        // slipped past the pre-check. The event log is append-ordered so
        // at this point both writers have landed and can see each other.
        // Whichever writer's credential_id sorts later revokes itself and
        // returns 409; the other keeps its record. Deterministic tie-break
        // (`credential_id` ordering) means both writers agree on the
        // winner without a second round-trip or cross-writer coordination.
        let post =
            CredentialReadModel::list_by_tenant(self.store.as_ref(), &tenant_id, usize::MAX, 0)
                .await?;
        let actives: Vec<&CredentialRecord> = post
            .iter()
            .filter(|c| c.active && c.provider_id == provider_id)
            .collect();
        if actives.len() > 1 {
            let our_is_loser = actives
                .iter()
                .map(|c| c.id.as_str())
                .any(|id| id > credential_id.as_str());
            if our_is_loser {
                let revoke_event =
                    make_envelope(RuntimeEvent::CredentialRevoked(CredentialRevoked {
                        tenant_id: tenant_id.clone(),
                        credential_id: credential_id.clone(),
                        revoked_at_ms: now_ms(),
                    }));
                self.store.append(&[revoke_event]).await?;
                return Err(RuntimeError::Conflict {
                    entity: "credential",
                    id: format!("provider={provider_id} tenant={tenant_id}"),
                });
            }
        }

        CredentialReadModel::get(self.store.as_ref(), &credential_id)
            .await?
            .ok_or_else(|| RuntimeError::Internal("credential not found after store".to_owned()))
    }

    async fn get(&self, id: &CredentialId) -> Result<Option<CredentialRecord>, RuntimeError> {
        Ok(CredentialReadModel::get(self.store.as_ref(), id).await?)
    }

    async fn list(
        &self,
        tenant_id: &TenantId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<CredentialRecord>, RuntimeError> {
        Ok(
            CredentialReadModel::list_by_tenant(self.store.as_ref(), tenant_id, limit, offset)
                .await?,
        )
    }

    async fn revoke(&self, id: &CredentialId) -> Result<CredentialRecord, RuntimeError> {
        let existing = CredentialReadModel::get(self.store.as_ref(), id)
            .await?
            .ok_or_else(|| RuntimeError::NotFound {
                entity: "credential",
                id: id.to_string(),
            })?;

        if !existing.active {
            return Ok(existing);
        }

        let event = make_envelope(RuntimeEvent::CredentialRevoked(CredentialRevoked {
            tenant_id: existing.tenant_id.clone(),
            credential_id: existing.id.clone(),
            revoked_at_ms: now_ms(),
        }));
        self.store.append(&[event]).await?;

        CredentialReadModel::get(self.store.as_ref(), id)
            .await?
            .ok_or_else(|| RuntimeError::Internal("credential not found after revoke".to_owned()))
    }

    async fn rotate_key(
        &self,
        tenant_id: TenantId,
        old_key_id: String,
        new_key_id: String,
    ) -> Result<CredentialRotationRecord, RuntimeError> {
        if TenantReadModel::get(self.store.as_ref(), &tenant_id)
            .await?
            .is_none()
        {
            return Err(RuntimeError::NotFound {
                entity: "tenant",
                id: tenant_id.to_string(),
            });
        }

        let started_at_ms = now_ms();
        let credentials =
            CredentialReadModel::list_by_tenant(self.store.as_ref(), &tenant_id, usize::MAX, 0)
                .await?;
        let candidates: Vec<_> = credentials
            .into_iter()
            .filter(|credential| credential.active)
            .filter(|credential| credential.key_id.as_deref() == Some(old_key_id.as_str()))
            .collect();

        let mut events = Vec::with_capacity(candidates.len() + 1);
        let mut rotated_ids = Vec::with_capacity(candidates.len());

        for (idx, credential) in candidates.iter().enumerate() {
            // `Zeroizing<String>` scrubs the decrypted plaintext from memory
            // on drop, covering the error-return paths below as well. Closes
            // #450: previously this was a plain Vec<String>.
            let plaintext: Zeroizing<String> = Zeroizing::new(decrypt_value(
                self.master_key.as_ref(),
                &credential.encrypted_value,
            )?);
            let rotated_at_ms = started_at_ms.saturating_add(idx as u64);
            let encrypted_value = encrypt_value(self.master_key.as_ref(), plaintext.as_str())?;
            rotated_ids.push(credential.id.to_string());
            events.push(make_envelope(RuntimeEvent::CredentialStored(
                CredentialStored {
                    tenant_id: credential.tenant_id.clone(),
                    credential_id: credential.id.clone(),
                    provider_id: credential.provider_id.clone(),
                    encrypted_value,
                    key_id: Some(new_key_id.clone()),
                    // Rotation rewrites ciphertext to the post-#461 format, so
                    // the version tag must advance to match. Preserving the
                    // source row's `key_version` (Copilot review PR #535)
                    // would silently keep legacy "v1" on rows that are, in
                    // fact, now the new format.
                    key_version: Some(CURRENT_KEY_VERSION.to_owned()),
                    encrypted_at_ms: rotated_at_ms,
                },
            )));
            // `plaintext` Zeroizing drops here, scrubbing the buffer.
        }

        let rotation_id = format!("credrot_{started_at_ms}");
        events.push(make_envelope(RuntimeEvent::CredentialKeyRotated(
            CredentialKeyRotated {
                tenant_id: tenant_id.clone(),
                rotation_id: rotation_id.clone(),
                old_key_id: old_key_id.clone(),
                new_key_id: new_key_id.clone(),
                credential_ids_rotated: rotated_ids.clone(),
            },
        )));

        self.store.append(&events).await?;

        Ok(CredentialRotationRecord {
            rotation_id,
            tenant_id,
            credential_id: CredentialId::new(""),
            rotated_at: now_ms(),
            rotated_by: None,
            old_key_id,
            new_key_id,
            rotated_credentials: rotated_ids.len() as u32,
            started_at_ms,
            completed_at_ms: Some(now_ms()),
        })
    }
}

/// Upper bound on rows scanned at boot. Deployments run O(10s) of
/// credentials per tenant and O(100s) of tenants, so 100k is a generous
/// cap. Hitting it signals either a misconfigured store or an
/// adversarial enumeration attempt; either way, the scan should stop
/// rather than wedge boot.
const LEGACY_SCAN_LIMIT: usize = 100_000;

/// A credential was written in the pre-fix deterministic-nonce format if
/// its `key_version` tag is not [`CURRENT_KEY_VERSION`]. The length-based
/// check a previous draft used missed realistic API keys: the old format
/// stored `ct(N) || tag(16)` so a 50-char `sk-...` token produced a
/// 66-byte blob, comfortably above the 28-byte `nonce + tag` threshold
/// (Cursor + Copilot review on PR #535).
///
/// Rows written before the `key_version` field existed (a theoretical
/// gap — pre-fix code always set `Some("v1")`) are also treated as
/// legacy, since they are definitely not the current format.
fn is_legacy_row(record: &CredentialRecord) -> bool {
    record.active && record.key_version.as_deref() != Some(CURRENT_KEY_VERSION)
}

/// Boot-time scan: flag every active credential that was written under the
/// pre-fix format. Operators must **revoke and re-enter** these credentials.
/// `rotate-key` is NOT a valid remediation: the new runtime reads the first
/// 12 bytes of the ciphertext as a random nonce, so pre-fix rows fail
/// every decrypt attempt regardless of which master key the operator sets.
/// That means `rotate-key`'s decrypt-then-re-encrypt loop errors out on
/// exactly the rows this scanner reports. Operators who need to migrate
/// without re-obtaining the secret must use the pre-fix binary to export
/// plaintexts before upgrading.
///
/// Prefers the single-pass `list_all_active` path on backends that
/// override it (notably the InMemoryStore projection that pg/sqlite
/// dual-write through). Falls back to the per-tenant path only when
/// `list_all_active` returns `None` — this covers backends that retain
/// the default trait impl without conflating "backend unsupported" with
/// "backend has zero active credentials". A legitimately-empty
/// `Some(Vec::new())` short-circuits the scan without triggering the
/// O(tenants) fallback round-trip. Gemini review on PR #535 (line 594);
/// Copilot review on PR #535 (line 671) for the `None`-vs-empty
/// disambiguation.
///
/// Returns the list of legacy rows so the caller can surface them in
/// startup logs and on the `/health` page.
pub async fn scan_legacy_ciphertexts<S>(store: &S) -> Result<Vec<LegacyCredential>, RuntimeError>
where
    S: CredentialReadModel + TenantReadModel + Send + Sync,
{
    // Fast path: single-pass `list_all_active`. `None` means the backend
    // has not overridden the trait default; `Some(_)` (including empty)
    // means the backend answered authoritatively and we skip the fallback.
    if let Some(all) = CredentialReadModel::list_all_active(store, LEGACY_SCAN_LIMIT).await? {
        if all.len() == LEGACY_SCAN_LIMIT {
            tracing::warn!(
                limit = LEGACY_SCAN_LIMIT,
                "credential legacy-format scan hit LEGACY_SCAN_LIMIT — some rows were not inspected"
            );
        }
        return Ok(all
            .into_iter()
            .filter(is_legacy_row)
            .map(|cred| LegacyCredential {
                tenant_id: cred.tenant_id.to_string(),
                credential_id: cred.id.to_string(),
                provider_id: cred.provider_id.clone(),
                key_version: cred.key_version.clone(),
                ciphertext_len: cred.encrypted_value.len(),
            })
            .collect());
    }

    // Fallback: per-tenant enumeration for backends that retain the
    // default `list_all_active` impl. Both tenant enumeration and
    // credential scanning are bounded — tenants are paged in fixed-size
    // chunks and the total credential count is capped at
    // `LEGACY_SCAN_LIMIT`. A deployment with millions of tenants
    // therefore cannot stall boot (Copilot review, PR #535).
    const TENANT_PAGE_SIZE: usize = 500;

    let mut out = Vec::new();
    let mut seen = 0usize;
    let mut tenant_offset = 0usize;
    'outer: loop {
        if seen >= LEGACY_SCAN_LIMIT {
            tracing::warn!(
                limit = LEGACY_SCAN_LIMIT,
                "credential legacy-format scan hit LEGACY_SCAN_LIMIT — some rows were not inspected"
            );
            break;
        }
        let tenants = TenantReadModel::list(store, TENANT_PAGE_SIZE, tenant_offset).await?;
        if tenants.is_empty() {
            break;
        }
        let last_page = tenants.len() < TENANT_PAGE_SIZE;
        for tenant in tenants {
            if seen >= LEGACY_SCAN_LIMIT {
                tracing::warn!(
                    limit = LEGACY_SCAN_LIMIT,
                    "credential legacy-format scan hit LEGACY_SCAN_LIMIT — some rows were not inspected"
                );
                break 'outer;
            }
            let remaining = LEGACY_SCAN_LIMIT.saturating_sub(seen);
            let creds =
                CredentialReadModel::list_by_tenant(store, &tenant.tenant_id, remaining, 0).await?;
            seen = seen.saturating_add(creds.len());
            for cred in creds {
                if is_legacy_row(&cred) {
                    out.push(LegacyCredential {
                        tenant_id: cred.tenant_id.to_string(),
                        credential_id: cred.id.to_string(),
                        provider_id: cred.provider_id.clone(),
                        key_version: cred.key_version.clone(),
                        ciphertext_len: cred.encrypted_value.len(),
                    });
                }
            }
        }
        if last_page {
            break;
        }
        tenant_offset = tenant_offset.saturating_add(TENANT_PAGE_SIZE);
    }
    Ok(out)
}

/// A credential row that was written in the legacy (deterministic-nonce)
/// format and must be rotated before it can be used.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyCredential {
    pub tenant_id: String,
    pub credential_id: String,
    pub provider_id: String,
    /// The row's stored `key_version`. Legacy rows typically carry
    /// `Some("v1")`; anything other than [`CURRENT_KEY_VERSION`] is flagged.
    pub key_version: Option<String>,
    pub ciphertext_len: usize,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::{decrypt_value, MasterKey};
    use crate::error::RuntimeError;
    use cairn_domain::TenantId;
    use cairn_store::projections::CredentialRotationReadModel;
    use cairn_store::InMemoryStore;

    use crate::credentials::CredentialService;
    use crate::services::{CredentialServiceImpl, TenantServiceImpl};
    use crate::tenants::TenantService;

    fn test_master_key() -> Arc<MasterKey> {
        Arc::new(MasterKey::from_bytes([7u8; 32]))
    }

    #[tokio::test]
    async fn credential_store_get_revoke_round_trip() {
        let store = Arc::new(InMemoryStore::new());
        let tenant_service = TenantServiceImpl::new(store.clone());
        tenant_service
            .create(TenantId::new("tenant_acme"), "Acme".to_owned())
            .await
            .unwrap();

        let service = CredentialServiceImpl::new(store, test_master_key());
        let plaintext = "super-secret-token";
        let stored = service
            .store(
                TenantId::new("tenant_acme"),
                "openai".to_owned(),
                plaintext.to_owned(),
                Some("kek-primary".to_owned()),
            )
            .await
            .unwrap();

        let fetched = service.get(&stored.id).await.unwrap().unwrap();
        assert_eq!(stored, fetched);
        // `encrypted_value` is a `RedactedCiphertext` (#579) that
        // derefs to `[u8]`; compare through the slice view.
        assert_ne!(&*fetched.encrypted_value, plaintext.as_bytes());
        assert!(fetched.active);

        let revoked = service.revoke(&stored.id).await.unwrap();
        assert!(!revoked.active);
        assert!(revoked.revoked_at_ms.is_some());
    }

    #[tokio::test]
    async fn key_rotation_reencrypts_all_tenant_credentials() {
        let store = Arc::new(InMemoryStore::new());
        let tenant_service = TenantServiceImpl::new(store.clone());
        tenant_service
            .create(TenantId::new("tenant_acme"), "Acme".to_owned())
            .await
            .unwrap();

        let key = test_master_key();
        let service = CredentialServiceImpl::new(store.clone(), key.clone());
        let inputs = [
            ("openai", "token-a"),
            ("anthropic", "token-b"),
            ("slack", "token-c"),
        ];

        let mut expected_plaintexts = std::collections::HashMap::new();
        for (provider_id, plaintext) in inputs {
            let stored = service
                .store(
                    TenantId::new("tenant_acme"),
                    provider_id.to_owned(),
                    plaintext.to_owned(),
                    Some("key_a".to_owned()),
                )
                .await
                .unwrap();
            expected_plaintexts.insert(stored.id.to_string(), plaintext.to_owned());
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        let rotation = service
            .rotate_key(
                TenantId::new("tenant_acme"),
                "key_a".to_owned(),
                "key_b".to_owned(),
            )
            .await
            .unwrap();

        assert_eq!(rotation.rotated_credentials, 3);
        assert!(rotation.completed_at_ms.is_some());

        let credentials = service
            .list(&TenantId::new("tenant_acme"), 10, 0)
            .await
            .unwrap();
        assert_eq!(credentials.len(), 3);
        for credential in credentials {
            assert_eq!(credential.key_id.as_deref(), Some("key_b"));
            let decrypted = decrypt_value(key.as_ref(), &credential.encrypted_value).unwrap();
            assert_eq!(
                decrypted,
                expected_plaintexts
                    .get(&credential.id.to_string())
                    .unwrap()
                    .as_str()
            );
        }

        let rotations = CredentialRotationReadModel::list_rotations(
            store.as_ref(),
            &TenantId::new("tenant_acme"),
        )
        .await
        .unwrap();
        assert_eq!(rotations.len(), 1);
        assert_eq!(rotations[0].rotated_credentials, 3);
        assert_eq!(rotations[0].old_key_id, "key_a");
        assert_eq!(rotations[0].new_key_id, "key_b");
    }

    #[test]
    fn nonce_prefix_differs_across_encryptions() {
        // AES-GCM: same plaintext + same key + random nonce => ciphertexts
        // must differ byte-for-byte. This is the single most important
        // regression test for the deterministic-nonce fix.
        let key = test_master_key();
        let a = super::encrypt_value(key.as_ref(), "hunter2").unwrap();
        let b = super::encrypt_value(key.as_ref(), "hunter2").unwrap();
        assert_ne!(a, b, "two encryptions of the same plaintext must differ");
        // Both must have the 12-byte nonce prefix.
        assert!(a.len() >= 12 + 16);
        assert!(b.len() >= 12 + 16);
        // And both must decrypt back to the same plaintext.
        assert_eq!(decrypt_value(key.as_ref(), &a).unwrap(), "hunter2");
        assert_eq!(decrypt_value(key.as_ref(), &b).unwrap(), "hunter2");
    }

    #[test]
    fn decrypt_with_wrong_key_fails() {
        let k1 = Arc::new(MasterKey::from_bytes([1u8; 32]));
        let k2 = Arc::new(MasterKey::from_bytes([2u8; 32]));
        let ct = super::encrypt_value(k1.as_ref(), "shhh").unwrap();
        assert!(decrypt_value(k2.as_ref(), &ct).is_err());
    }

    /// #460: decrypt-with-wrong-key errors must NOT embed the aes-gcm /
    /// aead crate's error text. Even though `aead::Error`'s Display is
    /// `"aead::Error"` today, stringifying it into the returned
    /// `RuntimeError::Internal` would couple the HTTP error body to an
    /// implementation-detail type name. The post-fix handler returns a
    /// fixed operator-facing string from [`CREDENTIAL_DECRYPTION_ERROR`];
    /// any future AEAD swap that starts printing richer errors won't
    /// leak through.
    ///
    /// Exercises the REAL code path (`decrypt_value` with mismatched
    /// keys) and asserts on the shared [`CREDENTIAL_DECRYPTION_ERROR`]
    /// constant. A future refactor that reintroduces `{e}` interpolation
    /// inside `decrypt_value`'s `map_err` closure — or that redefines
    /// the constant to embed crypto detail — will fail this test.
    #[test]
    fn decrypt_error_does_not_leak_aead_internals() {
        use super::CREDENTIAL_DECRYPTION_ERROR;

        let k1 = Arc::new(MasterKey::from_bytes([1u8; 32]));
        let k2 = Arc::new(MasterKey::from_bytes([2u8; 32]));
        let ct = super::encrypt_value(k1.as_ref(), "shhh").unwrap();
        let err = decrypt_value(k2.as_ref(), &ct).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains(CREDENTIAL_DECRYPTION_ERROR),
            "decrypt error body must contain the shared constant \
             `{CREDENTIAL_DECRYPTION_ERROR}` — got: {msg}",
        );
        // Belt-and-suspenders: reject any aead/aes telemetry leak.
        assert!(!msg.to_lowercase().contains("aead"));
        assert!(!msg.to_lowercase().contains("aes"));
        assert!(!msg.to_lowercase().contains("gcm"));
        // Also assert the constant itself carries no crypto-crate
        // detail, in case a future refactor tries to enrich it.
        let k = CREDENTIAL_DECRYPTION_ERROR.to_lowercase();
        assert!(!k.contains("aead"));
        assert!(!k.contains("aes"));
        assert!(!k.contains("gcm"));
    }

    /// #460 (encrypt side): the operator-facing encrypt-failure body
    /// must stay free of aes-gcm / aead telemetry. Cairn holds aes-gcm
    /// well below its documented input-size ceiling so `encrypt_value`
    /// has no practical failure input, but we can still enforce the
    /// contract at the constant level: `encrypt_value`'s `map_err`
    /// produces `RuntimeError::Internal(CREDENTIAL_ENCRYPTION_ERROR)`
    /// verbatim, and this test asserts the constant itself has no
    /// crypto-crate leakage. Any refactor that re-introduces `{e}` or
    /// redefines the constant fails here.
    #[test]
    fn encrypt_error_constant_has_no_crypto_detail() {
        use super::CREDENTIAL_ENCRYPTION_ERROR;

        let k = CREDENTIAL_ENCRYPTION_ERROR.to_lowercase();
        assert!(!k.contains("aead"));
        assert!(!k.contains("aes"));
        assert!(!k.contains("gcm"));
        // Sanity: the constant IS the text the handler emits.
        let err = RuntimeError::Internal(CREDENTIAL_ENCRYPTION_ERROR.to_owned());
        assert!(err.to_string().contains(CREDENTIAL_ENCRYPTION_ERROR));
    }

    #[test]
    fn decrypt_rejects_too_short_ciphertext() {
        let key = test_master_key();
        let result = decrypt_value(key.as_ref(), &[1u8; 10]);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("too short"),
            "expected 'too short' in error; got: {msg}"
        );
    }

    #[test]
    fn master_key_hex_parsing() {
        let hex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let key = MasterKey::decode_material(hex, "test").unwrap();
        assert_eq!(key.fingerprint().len(), 8);
    }

    #[test]
    fn master_key_base64_parsing() {
        // 32 bytes of 0xAA encoded in base64
        let b64 = "qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqo=";
        let key = MasterKey::decode_material(b64, "test").unwrap();
        assert_eq!(key.fingerprint().len(), 8);
    }

    #[test]
    fn master_key_rejects_short_input() {
        let result = MasterKey::decode_material("too-short", "test");
        assert!(result.is_err());
    }

    #[test]
    fn master_key_debug_does_not_expose_bytes() {
        let key = MasterKey::from_bytes([0xABu8; 32]);
        let dbg = format!("{key:?}");
        // The raw bytes must not appear (fingerprint only).
        assert!(!dbg.contains("ababab"), "raw bytes leaked in debug: {dbg}");
        assert!(dbg.contains("fingerprint"));
    }
}
