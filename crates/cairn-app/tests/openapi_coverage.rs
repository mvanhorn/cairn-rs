//! OpenAPI coverage and correctness assertions.
//!
//! These tests parse the static `OPENAPI_JSON` spec shipped at
//! `/v1/openapi.json` and check it against the actual router wiring.
//! They exist to catch three classes of drift:
//!
//!   1. Spec lying about authentication (#420) — e.g. a documented
//!      "No auth required" endpoint that actually requires bearer.
//!   2. Spec missing routes the router serves (#421) — endpoints
//!      added to `router.rs` / `bin_router.rs` without a matching
//!      `paths` entry.
//!   3. Endpoints returning `has_more: false` unconditionally (#422).
//!      Lock-in tests live in `test_pagination_honesty.rs`.

use serde_json::Value;

fn parse_spec() -> Value {
    serde_json::from_str(cairn_app::openapi_spec::OPENAPI_JSON)
        .expect("OPENAPI_JSON must be valid JSON")
}

/// #420: `/v1/stream` must declare bearer security (header + `?token=`
/// fallback). Previously the spec set `"security": []` and described
/// the endpoint as "No auth required" — SDK generators emitted clients
/// that failed with 401 on first connect.
#[test]
fn stream_endpoint_declares_bearer_security() {
    let spec = parse_spec();
    let stream = &spec["paths"]["/v1/stream"]["get"];

    assert!(
        !stream.is_null(),
        "/v1/stream GET missing from OpenAPI paths"
    );

    let security = &stream["security"];
    assert!(
        security.is_array(),
        "/v1/stream security must be an array (not inherited via omission) \
         so SDK generators emit the `?token=` fallback correctly. Got: {security:?}"
    );
    let security_arr = security.as_array().unwrap();
    assert!(
        !security_arr.is_empty(),
        "/v1/stream security must be a NON-EMPTY array referencing bearerAuth. \
         Empty `security: []` lies about auth and breaks generated clients (issue #420). \
         Got: {security_arr:?}"
    );

    let first = &security_arr[0];
    assert!(
        first.get("bearerAuth").is_some(),
        "/v1/stream security[0] must reference `bearerAuth`. Got: {first:?}"
    );

    // Description must note the `?token=` fallback for EventSource.
    let desc = stream["description"].as_str().unwrap_or("");
    assert!(
        desc.contains("?token=") || desc.to_lowercase().contains("token"),
        "/v1/stream description should document the `?token=` query-parameter \
         fallback so SDK consumers generate working browser clients. Got: {desc:?}"
    );

    // `token` query parameter must be documented so codegen picks it up.
    let params = stream["parameters"].as_array().cloned().unwrap_or_default();
    let has_token_param = params.iter().any(|p| {
        p.get("name").and_then(Value::as_str) == Some("token")
            && p.get("in").and_then(Value::as_str) == Some("query")
    });
    assert!(
        has_token_param,
        "/v1/stream must declare a `token` query parameter so SDK clients can \
         pass bearer via URL on EventSource connections. Got: {params:?}"
    );
}

/// Spec-level (global) security requirement must still be bearerAuth —
/// guard against accidental removal.
#[test]
fn global_security_requires_bearer() {
    let spec = parse_spec();
    let security = spec["security"]
        .as_array()
        .expect("spec top-level `security` must be an array");
    assert!(
        security.iter().any(|req| req.get("bearerAuth").is_some()),
        "Top-level security must include `bearerAuth`. Got: {security:?}"
    );
}

/// Sanity: the spec must parse cleanly as strict JSON.
#[test]
fn spec_is_valid_json() {
    let _spec = parse_spec();
}

/// #421: every preserved route (the compatibility-catalog source of
/// truth at `cairn_api::http::preserved_route_catalog`) must have a
/// matching `(method, path)` entry in the OpenAPI `paths` map. This
/// is the enforcement gate that stops routes from drifting silently
/// out of the spec.
///
/// Path-only checks (earlier version of this test) missed drift where
/// a path existed in OpenAPI but the specific HTTP method was
/// undocumented (Copilot PR review #546): a POST-only endpoint with a
/// sibling GET documented would pass a path-only check but still omit
/// the POST method from the spec. The upgraded check maps Axum's
/// `HttpMethod` enum to OpenAPI's lowercase method-name keys and
/// requires both to be present.
#[test]
fn every_preserved_route_is_documented() {
    use cairn_api::http::HttpMethod;
    let spec = parse_spec();
    let paths = spec["paths"]
        .as_object()
        .expect("spec paths must be an object");

    let method_key = |m: &HttpMethod| -> &'static str {
        match m {
            HttpMethod::Get => "get",
            HttpMethod::Post => "post",
            HttpMethod::Put => "put",
            HttpMethod::Delete => "delete",
            HttpMethod::Patch => "patch",
        }
    };

    let mut missing: Vec<String> = Vec::new();
    for entry in cairn_api::http::preserved_route_catalog() {
        // Axum `:id` → OpenAPI `{id}`.
        let oas_path = entry
            .path
            .split('/')
            .map(|seg| {
                if let Some(name) = seg.strip_prefix(':') {
                    format!("{{{name}}}")
                } else {
                    seg.to_owned()
                }
            })
            .collect::<Vec<_>>()
            .join("/");
        let key = method_key(&entry.method);
        let Some(methods) = paths.get(&oas_path).and_then(|v| v.as_object()) else {
            missing.push(format!("{:?} {oas_path} (path missing)", entry.method));
            continue;
        };
        if !methods.contains_key(key) {
            missing.push(format!(
                "{:?} {oas_path} (path present but {key:?} method undocumented)",
                entry.method
            ));
        }
    }

    assert!(
        missing.is_empty(),
        "OpenAPI spec is missing {} preserved route entries — a (method, path) \
         pair was added to `preserved_route_catalog()` without the matching \
         `openapi_spec.rs` entry. Every /v1/ route MUST be documented per \
         method (see CLAUDE.md 'Every API change must update the OpenAPI \
         spec'). Missing:\n  {}",
        missing.len(),
        missing.join("\n  ")
    );
}

/// #421: the bearer-auth security scheme must be declared in
/// `components.securitySchemes` — this is what makes the top-level
/// and per-path `security: [{"bearerAuth": []}]` entries resolvable
/// by SDK generators.
#[test]
fn bearer_auth_security_scheme_is_declared() {
    let spec = parse_spec();
    let scheme = &spec["components"]["securitySchemes"]["bearerAuth"];
    assert!(
        !scheme.is_null(),
        "components.securitySchemes.bearerAuth missing — top-level `security: \
         [{{\"bearerAuth\": []}}]` would be unresolvable."
    );
    assert_eq!(
        scheme["type"], "http",
        "bearerAuth scheme type must be http: {scheme:?}"
    );
    assert_eq!(
        scheme["scheme"], "bearer",
        "bearerAuth scheme must use `scheme: bearer`: {scheme:?}"
    );
}
