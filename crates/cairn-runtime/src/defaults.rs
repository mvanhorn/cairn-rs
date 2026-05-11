use async_trait::async_trait;
use cairn_domain::{DefaultSetting, ProjectKey, Scope};
use serde::{de::DeserializeOwned, Serialize};

use crate::error::RuntimeError;

/// RFC 032 PR-1: hard cap on the serialized bytes of a single typed
/// default-setting payload. Non-string default values today fall
/// through cairn-app's `validate_setting_value` string cap (4 KiB)
/// into axum's global 10 MiB body limit — meaning a
/// multi-megabyte `CompletionContract::Structured { schema: ... }`
/// blob would persist into the event log and replay on every cold
/// boot. 64 KiB is headroom for the largest realistic operator
/// contract (a JSON Schema for a structured deliverable is typically
/// well under 4 KiB) without opening the door to accidental blobs.
///
/// Enforced by the `set_struct` default method below. Callers that
/// need larger payloads should not be using the defaults service —
/// the defaults service is for small, infrequently-written policy
/// values, not bulk data.
pub const TYPED_DEFAULT_MAX_BYTES: usize = 64 * 1024;

/// Error from the typed default-setting helpers. Distinct from
/// [`RuntimeError`] so call sites can react to the size-cap case
/// without matching on the `Internal(String)` variant.
#[derive(Debug)]
pub enum TypedDefaultError {
    /// Serialization of the caller's typed value to JSON failed.
    /// Surfaces a serde error (e.g. a non-string key in a map).
    NotSerializable(serde_json::Error),
    /// The serialized payload exceeded [`TYPED_DEFAULT_MAX_BYTES`].
    /// The cap is per-value, not per-call; splitting a large blob
    /// across keys is also the wrong pattern — use a dedicated
    /// projection if you need to persist structured bulk data.
    TooLarge { size: usize, cap: usize },
    /// Deserialization of the stored JSON back into the caller's
    /// type failed. Indicates drift — a value written under one
    /// type's shape is being read under another, or the stored
    /// payload has been corrupted. Callers should log and treat
    /// this the same as "no value present" rather than panic.
    Corrupt(serde_json::Error),
    /// Underlying runtime error from the defaults service itself
    /// (event-log append failure, projection read failure, etc).
    Runtime(RuntimeError),
}

impl std::fmt::Display for TypedDefaultError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotSerializable(e) => write!(f, "typed default not serializable: {e}"),
            Self::TooLarge { size, cap } => write!(
                f,
                "typed default payload exceeds cap: {size} bytes > {cap} bytes"
            ),
            Self::Corrupt(e) => write!(f, "typed default stored value corrupt: {e}"),
            Self::Runtime(e) => write!(f, "typed default runtime error: {e}"),
        }
    }
}

impl std::error::Error for TypedDefaultError {}

impl From<RuntimeError> for TypedDefaultError {
    fn from(value: RuntimeError) -> Self {
        Self::Runtime(value)
    }
}

#[async_trait]
pub trait DefaultsService: Send + Sync {
    async fn set(
        &self,
        scope: Scope,
        scope_id: String,
        key: String,
        value: serde_json::Value,
    ) -> Result<DefaultSetting, RuntimeError>;

    async fn clear(&self, scope: Scope, scope_id: String, key: String) -> Result<(), RuntimeError>;

    async fn resolve(
        &self,
        project_key: &ProjectKey,
        key: &str,
    ) -> Result<Option<serde_json::Value>, RuntimeError>;

    /// RFC 032 PR-1: typed set. Serializes `value` to JSON,
    /// enforces [`TYPED_DEFAULT_MAX_BYTES`], then writes via the
    /// existing [`DefaultsService::set`] path. The cap fires
    /// before any event-log append — a too-large payload never
    /// reaches the store.
    ///
    /// This is the only write path callers should use for
    /// typed defaults. The untyped [`DefaultsService::set`] path
    /// is kept for historical string / number / bool values and
    /// for the resolver's own layered-read story; new typed
    /// persistence (e.g. [`cairn_domain::CompletionContract`],
    /// per RFC 032) goes through here.
    async fn set_struct<T>(
        &self,
        scope: Scope,
        scope_id: String,
        key: String,
        value: &T,
    ) -> Result<DefaultSetting, TypedDefaultError>
    where
        T: Serialize + Send + Sync + ?Sized,
    {
        // Serialize to bytes first so we can enforce the cap before
        // allocating the `serde_json::Value` tree that `set` takes.
        // Previous revision went `to_value` → `to_vec` — two
        // serialization passes plus one tree-allocation. This path
        // is one `to_vec` + one `from_slice`, and the slice parse
        // is a bounded walk over at most `TYPED_DEFAULT_MAX_BYTES`
        // that only runs when the payload is under the cap.
        let bytes = serde_json::to_vec(value).map_err(TypedDefaultError::NotSerializable)?;
        if bytes.len() > TYPED_DEFAULT_MAX_BYTES {
            return Err(TypedDefaultError::TooLarge {
                size: bytes.len(),
                cap: TYPED_DEFAULT_MAX_BYTES,
            });
        }
        let json: serde_json::Value =
            serde_json::from_slice(&bytes).map_err(TypedDefaultError::NotSerializable)?;
        self.set(scope, scope_id, key, json)
            .await
            .map_err(Into::into)
    }

    /// RFC 032 PR-1: typed resolve. Reads the layered defaults via
    /// [`DefaultsService::resolve`] then deserializes the resolved
    /// JSON into the caller's type. Returns:
    /// * `Ok(Some(T))` when a value exists and deserializes cleanly.
    /// * `Ok(None)` when no value exists at any layer.
    /// * `Err(Corrupt(_))` when a value exists but does not
    ///   deserialize into `T` — caller decides whether to fall back
    ///   to a default, clear the bad value, or propagate.
    ///
    /// Do NOT panic on `Corrupt` — the gate (RFC 032 PR-4) treats
    /// it as "no contract resolved" and falls through to inference.
    async fn get_struct<T>(
        &self,
        project_key: &ProjectKey,
        key: &str,
    ) -> Result<Option<T>, TypedDefaultError>
    where
        T: DeserializeOwned + Send,
    {
        let Some(value) = self
            .resolve(project_key, key)
            .await
            .map_err(Into::<TypedDefaultError>::into)?
        else {
            return Ok(None);
        };
        let parsed = serde_json::from_value::<T>(value).map_err(TypedDefaultError::Corrupt)?;
        Ok(Some(parsed))
    }
}
