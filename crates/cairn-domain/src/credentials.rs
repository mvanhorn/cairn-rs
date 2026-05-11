use crate::ids::{CredentialId, TenantId};
use serde::{Deserialize, Serialize};
use zeroize::{Zeroize, ZeroizeOnDrop, Zeroizing};

/// Ciphertext wrapper that (1) zeroes its heap buffer on drop and
/// (2) redacts itself in `Debug` output.
///
/// Exists so containers like [`CredentialRecord`] can keep using
/// `#[derive(Debug)]` without new fields silently bypassing redaction:
/// only this type knows how to print itself, and the record prints
/// through it.
///
/// Closes #579 (Gemini PR-#582 review): the previous version
/// hand-rolled `impl Debug for CredentialRecord`, which risked new
/// fields being forgotten and shipping raw in logs.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize, Zeroize, ZeroizeOnDrop)]
#[serde(transparent)]
pub struct RedactedCiphertext(Zeroizing<Vec<u8>>);

impl RedactedCiphertext {
    /// Wrap raw ciphertext bytes. Prefer `From<Vec<u8>>` at call sites;
    /// the inherent constructor exists for spots that can't rely on
    /// inference.
    #[inline]
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(Zeroizing::new(bytes))
    }

    /// Length of the underlying ciphertext in bytes.
    ///
    /// Safe to log / surface in telemetry — length is not sensitive.
    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the ciphertext is empty. Mostly for sanity checks after
    /// encrypting.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl std::fmt::Debug for RedactedCiphertext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `{:#?}`-style output on the outer struct will still print this
        // single field as `<redacted; N bytes>`, which is exactly what
        // we want for operator triage.
        write!(f, "<redacted; {} bytes>", self.0.len())
    }
}

impl From<Vec<u8>> for RedactedCiphertext {
    #[inline]
    fn from(bytes: Vec<u8>) -> Self {
        Self::new(bytes)
    }
}

impl std::ops::Deref for RedactedCiphertext {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<[u8]> for RedactedCiphertext {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// Tenant-scoped credential record for provider and channel access.
///
/// `encrypted_value` is a [`RedactedCiphertext`]: it zeroes its heap
/// buffer on drop (#579) and redacts itself in `Debug` output so a
/// stray `tracing::debug!("{record:?}")` or a panic backtrace cannot
/// surface crypto material.
///
/// While the bytes are AES-GCM-encrypted and not plaintext secrets,
/// the ciphertext is still cryptographic material that FIPS / FedRAMP
/// profiles require to be zeroed — and leaving it in released heap
/// pages needlessly aids post-breach forensics.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CredentialRecord {
    pub id: CredentialId,
    pub tenant_id: TenantId,
    pub name: String,
    pub credential_type: String,
    pub encrypted_value: RedactedCiphertext,
    pub created_at: u64,
    pub updated_at: u64,
    #[serde(default = "default_true")]
    pub active: bool,
    #[serde(default)]
    pub provider_id: String,
    #[serde(default)]
    pub encrypted_at_ms: Option<u64>,
    #[serde(default)]
    pub key_id: Option<String>,
    #[serde(default)]
    pub key_version: Option<String>,
    #[serde(default)]
    pub revoked_at_ms: Option<u64>,
}

fn default_true() -> bool {
    true
}

/// Audit record for a credential rotation event.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CredentialRotationRecord {
    pub rotation_id: String,
    pub tenant_id: TenantId,
    // Early rotation events (pre-id-tracking) wrote only
    // `rotation_id`; back-compat deserialise with an empty
    // placeholder so the projection keeps reading older log pages.
    #[serde(default = "crate::ids::empty_credential_id")]
    pub credential_id: CredentialId,
    #[serde(default)]
    pub rotated_at: u64,
    #[serde(default)]
    pub rotated_by: Option<String>,
    #[serde(default)]
    pub started_at_ms: u64,
    #[serde(default)]
    pub completed_at_ms: Option<u64>,
    #[serde(default)]
    pub old_key_id: String,
    #[serde(default)]
    pub new_key_id: String,
    /// Count of credentials rotated in this operation.
    #[serde(default)]
    pub rotated_credentials: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_record(ciphertext: Vec<u8>) -> CredentialRecord {
        CredentialRecord {
            id: CredentialId::new("cred_1"),
            tenant_id: TenantId::new("tenant_acme"),
            name: "openai-api-key".to_owned(),
            credential_type: "api_key".to_owned(),
            encrypted_value: RedactedCiphertext::new(ciphertext),
            created_at: 100,
            updated_at: 100,
            active: true,
            provider_id: String::new(),
            encrypted_at_ms: None,
            key_id: None,
            key_version: None,
            revoked_at_ms: None,
        }
    }

    #[test]
    fn credential_record_carries_tenant_scope() {
        let record = sample_record(vec![1, 2, 3]);
        assert_eq!(record.tenant_id.as_str(), "tenant_acme");
    }

    /// #579: Debug output must not leak ciphertext bytes. The wrapper
    /// keeps the byte length for operator triage but hides the
    /// material so a stray `tracing::debug!("{record:?}")` or a panic
    /// backtrace cannot surface crypto material in logs.
    #[test]
    fn debug_output_redacts_encrypted_value() {
        // Distinctive byte pattern whose decimal values don't collide
        // with any numeric field in `sample_record` (created_at=100,
        // updated_at=100, both Options are None).
        let secret_bytes: Vec<u8> = vec![0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE];
        let record = sample_record(secret_bytes.clone());

        let debug = format!("{record:?}");

        // Length + redaction marker surface, ciphertext does not.
        assert!(
            debug.contains("<redacted;"),
            "Debug output must contain the redaction marker; got: {debug}"
        );
        assert!(
            debug.contains(&format!("{} bytes", secret_bytes.len())),
            "Debug output must surface the ciphertext length; got: {debug}"
        );

        // A naive `derive(Debug)` on `Vec<u8>` prints decimal values
        // separated by commas: `[222, 173, 190, 239, 202, 254]`. Look
        // for that exact pattern (with and without outer brackets) so
        // we fail loudly if anyone drops the custom Debug impl on
        // `RedactedCiphertext`.
        let naive_joined = secret_bytes
            .iter()
            .map(|b| b.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        assert!(
            !debug.contains(&naive_joined),
            "Debug output must not contain the raw byte sequence {naive_joined}; got: {debug}"
        );
        // Hex-style Debug (`{:#x?}`) prints `0xde, 0xad, …`.
        let hex_joined = secret_bytes
            .iter()
            .map(|b| format!("0x{b:x}"))
            .collect::<Vec<_>>()
            .join(", ");
        assert!(
            !debug.to_lowercase().contains(&hex_joined),
            "Debug output must not contain hex byte sequence {hex_joined}; got: {debug}"
        );
    }

    /// #579: `RedactedCiphertext` implements `ZeroizeOnDrop` so the
    /// ciphertext is scrubbed from the heap on drop. Asserting the
    /// trait bound via a generic function means this stops compiling
    /// if anyone removes the scrubbing wrapper.
    #[test]
    fn encrypted_value_is_zeroize_on_drop() {
        fn assert_zeroize_on_drop<T: ZeroizeOnDrop>(_: &T) {}

        let record = sample_record(vec![0x01, 0x02, 0x03]);
        assert_zeroize_on_drop(&record.encrypted_value);
    }

    /// `RedactedCiphertext` round-trips through `serde_json` as a
    /// transparent byte array, matching the pre-#579 wire shape.
    /// Breaking this would silently corrupt snapshot restores / event
    /// replay in `pg::projections::credential_stored`.
    #[test]
    fn redacted_ciphertext_serde_roundtrip() {
        let bytes = vec![0x10, 0x20, 0x30, 0x40];
        let wrapped = RedactedCiphertext::new(bytes.clone());
        let json = serde_json::to_string(&wrapped).expect("serialise");
        let back: RedactedCiphertext = serde_json::from_str(&json).expect("deserialise");
        assert_eq!(&*back, bytes.as_slice());
    }

    /// Direct Debug of the wrapper is redacted too, which matters for
    /// `dbg!(&record.encrypted_value)` — a common shape in incident
    /// triage.
    #[test]
    fn wrapper_debug_is_redacted_standalone() {
        let wrapped = RedactedCiphertext::new(vec![0xAA; 32]);
        assert_eq!(format!("{wrapped:?}"), "<redacted; 32 bytes>");
    }
}
