//! Shared HTTP response DTOs.
//!
//! Only the DTOs shared between cairn-api consumers and downstream
//! implementor crates live here. HTTP-layer types that are specific to
//! cairn-api's route registration (route catalog, `RouteEntry`,
//! `HttpMethod`, `RouteClassification`, `RouteRegistry`) stay in
//! cairn-api because they have no downstream implementor.

use serde::{Deserialize, Serialize};

/// Standard paginated list response used by preserved endpoints.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListResponse<T> {
    pub items: Vec<T>,
    pub has_more: bool,
}

/// Standard success acknowledgement for mutation endpoints.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OkResponse {
    pub ok: bool,
}

/// Health check response for `GET /health`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthResponse {
    pub ok: bool,
}

/// Structured API error returned by HTTP handlers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct ApiError {
    pub status_code: u16,
    pub code: String,
    pub message: String,
    #[serde(default)]
    pub request_id: Option<String>,
}

impl ApiError {
    pub fn new(status_code: u16, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            status_code,
            code: code.into(),
            message: message.into(),
            request_id: None,
        }
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(404, "not_found", message)
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(401, "unauthorized", message)
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(400, "bad_request", message)
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}: {}", self.status_code, self.code, self.message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_response_serialization() {
        let response = ListResponse {
            items: vec!["a".to_owned(), "b".to_owned()],
            has_more: true,
        };
        let json = serde_json::to_value(&response).unwrap();
        assert_eq!(json["hasMore"], true);
        assert_eq!(json["items"].as_array().unwrap().len(), 2);
    }
}
