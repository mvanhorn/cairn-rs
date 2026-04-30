use async_trait::async_trait;
use cairn_domain::{DefaultSetting, Scope};

use crate::error::StoreError;

#[async_trait]
pub trait DefaultsReadModel: Send + Sync {
    async fn get(
        &self,
        scope: Scope,
        scope_id: &str,
        key: &str,
    ) -> Result<Option<DefaultSetting>, StoreError>;

    async fn list_by_scope(
        &self,
        scope: Scope,
        scope_id: &str,
    ) -> Result<Vec<DefaultSetting>, StoreError>;
}

/// Stable TEXT encoding of `Scope` for the `default_settings.scope`
/// column. Matches the domain `#[serde(rename_all = "snake_case")]`
/// contract so pg/sqlite and the in-memory composite-key format all
/// agree on the on-disk form.
pub fn defaults_scope_str(scope: Scope) -> &'static str {
    match scope {
        Scope::System => "system",
        Scope::Tenant => "tenant",
        Scope::Workspace => "workspace",
        Scope::Project => "project",
    }
}

/// Inverse of [`defaults_scope_str`]. Unknown values indicate projection
/// corruption.
pub fn rehydrate_defaults_scope(raw: &str) -> Result<Scope, StoreError> {
    match raw {
        "system" => Ok(Scope::System),
        "tenant" => Ok(Scope::Tenant),
        "workspace" => Ok(Scope::Workspace),
        "project" => Ok(Scope::Project),
        other => Err(StoreError::Internal(format!(
            "default_settings.scope = {other:?} is not a known Scope"
        ))),
    }
}
