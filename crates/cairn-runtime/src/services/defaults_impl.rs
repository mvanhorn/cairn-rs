use std::sync::Arc;

use async_trait::async_trait;
use cairn_domain::{
    DefaultSetting, DefaultSettingCleared, DefaultSettingSet, DefaultsLayer, DefaultsResolver,
    LayeredDefaultsResolver, ProjectKey, RuntimeEvent, Scope,
};
use cairn_store::projections::DefaultsReadModel;
use cairn_store::EventLog;

use super::event_helpers::make_envelope;
use crate::defaults::DefaultsService;
use crate::error::RuntimeError;

pub struct DefaultsServiceImpl<S> {
    store: Arc<S>,
    resolver: LayeredDefaultsResolver,
}

impl<S> DefaultsServiceImpl<S> {
    pub fn new(store: Arc<S>) -> Self {
        Self {
            store,
            resolver: LayeredDefaultsResolver,
        }
    }
}

fn normalized_scope_id(scope: Scope, scope_id: &str) -> String {
    match scope {
        Scope::System => "system".to_owned(),
        _ => scope_id.to_owned(),
    }
}

#[async_trait]
impl<S> DefaultsService for DefaultsServiceImpl<S>
where
    S: EventLog + DefaultsReadModel + Send + Sync + 'static,
{
    async fn set(
        &self,
        scope: Scope,
        scope_id: String,
        key: String,
        value: serde_json::Value,
    ) -> Result<DefaultSetting, RuntimeError> {
        // RFC 032 PR-1 review (Gemini security-medium): the typed
        // `set_struct` path already caps at TYPED_DEFAULT_MAX_BYTES,
        // but the untyped `set` was reachable without any size
        // check — any caller holding a raw `serde_json::Value` could
        // write up to axum's 10 MiB body limit into the event log,
        // which would then replay on every cold boot. The cap is a
        // concrete-impl concern (the trait default can't measure
        // serialized size without re-serializing `value` back to
        // bytes), so the single production `DefaultsServiceImpl`
        // enforces it here. Historical callers (string / bool /
        // number scalars) stay well under the cap; the check only
        // catches accidental blobs.
        let bytes = serde_json::to_vec(&value).map_err(|e| {
            RuntimeError::Internal(format!("default setting value not serializable: {e}"))
        })?;
        if bytes.len() > crate::defaults::TYPED_DEFAULT_MAX_BYTES {
            return Err(RuntimeError::Internal(format!(
                "default setting value exceeds {}-byte cap ({} bytes). \
                 Use a dedicated projection for bulk data; defaults are \
                 for small policy values only.",
                crate::defaults::TYPED_DEFAULT_MAX_BYTES,
                bytes.len(),
            )));
        }
        let normalized_scope_id = normalized_scope_id(scope, &scope_id);
        let event = make_envelope(RuntimeEvent::DefaultSettingSet(DefaultSettingSet {
            scope,
            scope_id: normalized_scope_id.clone(),
            key: key.clone(),
            value,
        }));
        self.store.append(&[event]).await?;
        DefaultsReadModel::get(self.store.as_ref(), scope, &normalized_scope_id, &key)
            .await?
            .ok_or_else(|| RuntimeError::Internal("default setting not found after set".to_owned()))
    }

    async fn clear(&self, scope: Scope, scope_id: String, key: String) -> Result<(), RuntimeError> {
        let normalized_scope_id = normalized_scope_id(scope, &scope_id);
        let event = make_envelope(RuntimeEvent::DefaultSettingCleared(DefaultSettingCleared {
            scope,
            scope_id: normalized_scope_id,
            key,
        }));
        self.store.append(&[event]).await?;
        Ok(())
    }

    async fn resolve(
        &self,
        project_key: &ProjectKey,
        key: &str,
    ) -> Result<Option<serde_json::Value>, RuntimeError> {
        let mut layers = Vec::new();
        for (scope, scope_id) in [
            (Scope::Project, project_key.project_id.as_str()),
            (Scope::Workspace, project_key.workspace_id.as_str()),
            (Scope::Tenant, project_key.tenant_id.as_str()),
            (Scope::System, "system"),
        ] {
            if let Some(setting) =
                DefaultsReadModel::get(self.store.as_ref(), scope, scope_id, key).await?
            {
                layers.push(DefaultsLayer {
                    scope: setting.scope,
                    key: setting.key,
                    value: setting.value,
                });
            }
        }
        Ok(self.resolver.resolve(&layers, key))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use cairn_domain::{ProjectKey, Scope};
    use cairn_store::InMemoryStore;
    use serde::{Deserialize, Serialize};

    use crate::defaults::{DefaultsService, TypedDefaultError, TYPED_DEFAULT_MAX_BYTES};
    use crate::services::DefaultsServiceImpl;

    #[tokio::test]
    async fn defaults_resolve_workspace_override_then_tenant_fallback_after_clear() {
        let store = Arc::new(InMemoryStore::new());
        let service = DefaultsServiceImpl::new(store);
        let project = ProjectKey::new("tenant_defaults", "ws_defaults", "project_defaults");

        service
            .set(
                Scope::Tenant,
                "tenant_defaults".to_owned(),
                "model".to_owned(),
                serde_json::json!("gpt-4"),
            )
            .await
            .unwrap();
        service
            .set(
                Scope::Workspace,
                "ws_defaults".to_owned(),
                "model".to_owned(),
                serde_json::json!("gpt-3.5"),
            )
            .await
            .unwrap();

        let first = service.resolve(&project, "model").await.unwrap();
        assert_eq!(first, Some(serde_json::json!("gpt-3.5")));

        service
            .clear(
                Scope::Workspace,
                "ws_defaults".to_owned(),
                "model".to_owned(),
            )
            .await
            .unwrap();

        let second = service.resolve(&project, "model").await.unwrap();
        assert_eq!(second, Some(serde_json::json!("gpt-4")));
    }

    // ── RFC 032 PR-1: typed-defaults coverage ─────────────────────────────

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct SamplePolicy {
        enabled: bool,
        priority: u32,
        label: String,
    }

    #[tokio::test]
    async fn set_struct_round_trips_through_get_struct() {
        // The typed helpers must compose: a value written via
        // `set_struct` must deserialize via `get_struct` on the same
        // project key. This pins the shape that RFC 032 PR-4 relies
        // on when reading back a persisted `CompletionContract`.
        let store = Arc::new(InMemoryStore::new());
        let service = DefaultsServiceImpl::new(store);
        let project = ProjectKey::new("t_typed", "w_typed", "p_typed");

        let policy = SamplePolicy {
            enabled: true,
            priority: 7,
            label: "experimental".to_owned(),
        };
        service
            .set_struct(
                Scope::Project,
                "p_typed".to_owned(),
                "policy".to_owned(),
                &policy,
            )
            .await
            .unwrap();

        let back: Option<SamplePolicy> = service.get_struct(&project, "policy").await.unwrap();
        assert_eq!(back, Some(policy));
    }

    #[tokio::test]
    async fn get_struct_returns_none_when_no_layer_has_value() {
        let store = Arc::new(InMemoryStore::new());
        let service = DefaultsServiceImpl::new(store);
        let project = ProjectKey::new("t_typed", "w_typed", "p_typed");

        let back: Option<SamplePolicy> = service.get_struct(&project, "missing").await.unwrap();
        assert_eq!(back, None);
    }

    #[tokio::test]
    async fn set_struct_rejects_payload_exceeding_cap_before_write() {
        // A pathologically large payload (here: a vector of 100 KiB
        // of junk inside a typed wrapper) must be rejected at the
        // cap check BEFORE the event log is touched. This is the
        // invariant RFC 032 PR-1 §2.2 requires — the cap fires at
        // the runtime layer, not just at the HTTP layer.
        #[derive(Serialize, Deserialize)]
        struct Oversized {
            junk: String,
        }
        let store = Arc::new(InMemoryStore::new());
        let service = DefaultsServiceImpl::new(store.clone());

        let oversized = Oversized {
            junk: "a".repeat(TYPED_DEFAULT_MAX_BYTES * 2),
        };
        let err = service
            .set_struct(
                Scope::Project,
                "p_cap".to_owned(),
                "big".to_owned(),
                &oversized,
            )
            .await
            .unwrap_err();

        match err {
            TypedDefaultError::TooLarge { size, cap } => {
                assert!(size > cap, "reported size must exceed cap");
                assert_eq!(cap, TYPED_DEFAULT_MAX_BYTES);
            }
            other => panic!("expected TooLarge; got {other:?}"),
        }

        // The write was rejected BEFORE the append — nothing
        // persisted. Confirm by reading back: no value resolves.
        let project = ProjectKey::new("t_cap", "w_cap", "p_cap");
        let back: Option<Oversized> = service.get_struct(&project, "big").await.unwrap();
        assert!(back.is_none(), "oversized payload must never persist");
    }

    #[tokio::test]
    async fn set_struct_accepts_payload_at_boundary() {
        // Regression guard around the cap boundary: a payload just
        // UNDER the cap must succeed. Leaves a byte of slack for
        // the JSON structural overhead (quotes + braces around the
        // string) which the caller doesn't control.
        #[derive(Serialize, Deserialize)]
        struct Snug {
            body: String,
        }
        let store = Arc::new(InMemoryStore::new());
        let service = DefaultsServiceImpl::new(store);

        // JSON overhead: `{"body":""}` is 11 bytes. Leave a little
        // extra slack for the count mismatch so the serialized
        // payload comfortably fits.
        let snug = Snug {
            body: "a".repeat(TYPED_DEFAULT_MAX_BYTES - 32),
        };
        service
            .set_struct(
                Scope::Project,
                "p_snug".to_owned(),
                "fits".to_owned(),
                &snug,
            )
            .await
            .expect("at-boundary payload must succeed");
    }

    #[tokio::test]
    async fn untyped_set_enforces_same_cap_as_set_struct() {
        // RFC 032 PR-1 review (Gemini security-medium): the base
        // `set` method must enforce the same size cap as the typed
        // wrapper. Otherwise callers could bypass the cap by
        // dropping into the untyped API. Pins that contract.
        let store = Arc::new(InMemoryStore::new());
        let service = DefaultsServiceImpl::new(store);

        // Build an oversized raw Value.
        let big = serde_json::json!({
            "junk": "a".repeat(TYPED_DEFAULT_MAX_BYTES * 2),
        });
        let err = service
            .set(
                Scope::Project,
                "p_raw_cap".to_owned(),
                "big".to_owned(),
                big,
            )
            .await
            .expect_err("untyped set must reject oversized payload");
        let msg = err.to_string();
        assert!(msg.contains("cap"), "error must name the cap; got: {msg}");
    }

    #[tokio::test]
    async fn get_struct_surfaces_corrupt_when_shape_mismatches() {
        // If a caller wrote under shape `A` and reads under shape
        // `B`, the typed helper returns `Corrupt` — not Ok(None),
        // not panic. RFC 032 PR-4's gate path treats Corrupt as
        // "no contract resolved" and falls through to inference;
        // pinning the error shape here keeps that contract.
        let store = Arc::new(InMemoryStore::new());
        let service = DefaultsServiceImpl::new(store);
        let project = ProjectKey::new("t_drift", "w_drift", "p_drift");

        // Write shape A (a string).
        service
            .set(
                Scope::Project,
                "p_drift".to_owned(),
                "drift".to_owned(),
                serde_json::json!("I am a string"),
            )
            .await
            .unwrap();

        // Read as shape B (a struct).
        let err = service
            .get_struct::<SamplePolicy>(&project, "drift")
            .await
            .expect_err("shape mismatch must surface as Err(Corrupt)");
        match err {
            TypedDefaultError::Corrupt(_) => {}
            other => panic!("expected Corrupt; got {other:?}"),
        }
    }
}
