//! Shared list-query DTO for paginated endpoints.
//!
//! Full `RuntimeReadEndpoints` trait (which depends on cairn-store
//! record types) stays in the `cairn-api` crate — only the DTOs
//! cairn-memory / cairn-feed implementors need live here so the
//! implementor crates do not need to pull cairn-api.

use serde::{Deserialize, Serialize};

/// Query parameters for list endpoints.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ListQuery {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub status: Option<String>,
    pub category: Option<String>,
}

impl ListQuery {
    pub fn effective_limit(&self) -> usize {
        self.limit.unwrap_or(50).min(200)
    }

    pub fn effective_offset(&self) -> usize {
        self.offset.unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_query_defaults() {
        let query = ListQuery::default();
        assert_eq!(query.effective_limit(), 50);
        assert_eq!(query.effective_offset(), 0);
    }

    #[test]
    fn list_query_clamps_limit() {
        let query = ListQuery {
            limit: Some(500),
            ..Default::default()
        };
        assert_eq!(query.effective_limit(), 200);
    }
}
