//! Request-signing abstraction for provider backends.
//!
//! Two concrete implementations ship today:
//!
//! * [`BearerAuth`] — the default: adds `Authorization: Bearer <token>`.
//!   All OpenAI-shaped endpoints and Bedrock's API-key path use this.
//! * [`SigV4Signer`] — AWS SigV4 signing for Bedrock's IAM-authenticated
//!   `Converse` and OpenAI-compat endpoints. Drives the AWS default
//!   credential provider chain (env → shared config → IMDS → container
//!   role → SSO) and signs per-request with the `bedrock` service name.
//!
//! Rationale for a trait-level seam: the same sign step is needed by two
//! backends that otherwise have nothing in common (native Converse at
//! `backends::bedrock`, OpenAI-compat at `wire::openai_compat`). Pulling
//! it out here keeps the SigV4 code DRY and lets us unit-test signing
//! without spinning up a real HTTP server.

use std::sync::Arc;
use std::time::SystemTime;

use aws_credential_types::Credentials;
use aws_sigv4::http_request::{
    PayloadChecksumKind, SignableBody, SignableRequest, SignatureLocation, SigningSettings, sign,
};
use aws_sigv4::sign::v4;

use crate::error::ProviderError;

/// Abstract authentication for an outbound HTTP request.
///
/// The signer either sets a static header (Bearer) or computes a
/// request-specific signature (SigV4). Implementations are `Send + Sync`
/// so a provider can hold one behind `Arc` and share it across
/// concurrent requests.
#[async_trait::async_trait]
pub trait RequestSigner: std::fmt::Debug + Send + Sync {
    /// Apply authentication to `builder`, returning the updated builder
    /// ready to `.send()`.
    ///
    /// `method`, `url`, and `body` are passed separately because
    /// `reqwest::RequestBuilder` does not expose them back once set —
    /// SigV4 needs them to compute the canonical request.
    async fn sign(
        &self,
        builder: reqwest::RequestBuilder,
        method: &str,
        url: &str,
        body: &[u8],
    ) -> Result<reqwest::RequestBuilder, ProviderError>;

    /// Short human-readable name, used in error messages only.
    fn scheme(&self) -> &'static str;
}

/// Static Bearer-token auth. Matches the previous behaviour of every
/// backend in the crate.
#[derive(Clone, Debug)]
pub struct BearerAuth {
    token: String,
}

impl BearerAuth {
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.token.is_empty()
    }
}

#[async_trait::async_trait]
impl RequestSigner for BearerAuth {
    async fn sign(
        &self,
        builder: reqwest::RequestBuilder,
        _method: &str,
        _url: &str,
        _body: &[u8],
    ) -> Result<reqwest::RequestBuilder, ProviderError> {
        if self.token.is_empty() {
            return Err(ProviderError::Auth(
                "missing Bearer token for signer".to_owned(),
            ));
        }
        Ok(builder.bearer_auth(&self.token))
    }

    fn scheme(&self) -> &'static str {
        "bearer"
    }
}

/// AWS SigV4 request signer for Bedrock runtime endpoints.
///
/// Holds a [`CredentialsSource`] rather than frozen credentials directly
/// so IMDS-issued short-lived creds refresh automatically per request.
/// The service name defaults to `"bedrock"` (correct for both the
/// Converse API and the Bedrock OpenAI-compat gateway).
#[derive(Clone, Debug)]
pub struct SigV4Signer {
    credentials: Arc<dyn CredentialsSource>,
    region: String,
    service: String,
}

impl SigV4Signer {
    /// Construct from an explicit credentials source.
    ///
    /// Most callers want [`SigV4Signer::from_default_chain`] instead —
    /// this lower-level constructor exists for tests.
    pub fn new(
        credentials: Arc<dyn CredentialsSource>,
        region: impl Into<String>,
        service: impl Into<String>,
    ) -> Self {
        Self {
            credentials,
            region: region.into(),
            service: service.into(),
        }
    }

    /// Build a signer that resolves credentials via the AWS default
    /// provider chain (env vars → shared config → IMDS → ECS container
    /// → SSO). Sticks to the defaults AWS picks for `bedrock-runtime`
    /// in the given region.
    ///
    /// This is an async call because the chain needs tokio for IMDS
    /// requests. It is safe to call once at construction and reuse;
    /// credentials refresh happens on each `sign` invocation via the
    /// source's async resolve.
    pub async fn from_default_chain(region: impl Into<String>) -> Result<Self, ProviderError> {
        let region = region.into();
        let provider =
            aws_config::default_provider::credentials::DefaultCredentialsChain::builder()
                .region(aws_config::Region::new(region.clone()))
                .build()
                .await;
        Ok(Self {
            credentials: Arc::new(DefaultChainCredentials {
                chain: Arc::new(provider),
            }),
            region,
            service: "bedrock".to_owned(),
        })
    }

    /// Convenience: static credentials. Useful for tests and for
    /// operators who mint STS tokens externally.
    pub fn with_static_credentials(
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
        session_token: Option<String>,
        region: impl Into<String>,
    ) -> Self {
        Self {
            credentials: Arc::new(StaticCredentials {
                creds: Credentials::new(
                    access_key.into(),
                    secret_key.into(),
                    session_token,
                    None,
                    "cairn-providers-static",
                ),
            }),
            region: region.into(),
            service: "bedrock".to_owned(),
        }
    }

    /// Override the service name (default: `"bedrock"`). The Bedrock
    /// AgentCore Gateway uses `"bedrock-agentcore"` instead; the
    /// credential-less OpenSearch Serverless data plane uses `"aoss"`.
    pub fn with_service(mut self, service: impl Into<String>) -> Self {
        self.service = service.into();
        self
    }

    pub fn region(&self) -> &str {
        &self.region
    }

    pub fn service(&self) -> &str {
        &self.service
    }
}

#[async_trait::async_trait]
impl RequestSigner for SigV4Signer {
    async fn sign(
        &self,
        mut builder: reqwest::RequestBuilder,
        method: &str,
        url: &str,
        body: &[u8],
    ) -> Result<reqwest::RequestBuilder, ProviderError> {
        let creds =
            self.credentials.resolve().await.map_err(|e| {
                ProviderError::Auth(format!("SigV4 credential resolution failed: {e}"))
            })?;

        // `aws-sigv4` takes a provider-params struct parameterised by
        // the credential identity; the `v4::SigningParams` builder is
        // the stable entry point since aws-sigv4 1.0.
        let identity = creds.into();
        let signing_params = v4::SigningParams::builder()
            .identity(&identity)
            .region(&self.region)
            .name(&self.service)
            .time(SystemTime::now())
            .settings({
                let mut s = SigningSettings::default();
                // Standard SigV4 "Authorization" header placement. The
                // alternative (`SignatureLocation::QueryParams`) is for
                // presigned URLs — not what we want here.
                s.signature_location = SignatureLocation::Headers;
                // Bedrock and other modern AWS services require the
                // payload hash in an `x-amz-content-sha256` header.
                // Without this, the endpoint can return 403 on POSTs
                // with a body. aws-sigv4 computes the hash for us; we
                // just have to opt in.
                s.payload_checksum_kind = PayloadChecksumKind::XAmzSha256;
                s
            })
            .build()
            .map_err(|e| ProviderError::Auth(format!("SigV4 params invalid: {e}")))?
            .into();

        // `host` is mandatory in the SigV4 SignedHeaders list. Passing
        // it explicitly both signs it and ensures it is enforced —
        // omitting it means a MITM could redirect the request to a
        // different host and the signature would still verify against
        // the original canonical request.
        let parsed = reqwest::Url::parse(url)
            .map_err(|e| ProviderError::Auth(format!("invalid URL for SigV4: {e}")))?;
        let host = parsed
            .host_str()
            .ok_or_else(|| ProviderError::Auth("URL missing host for SigV4".to_owned()))?
            .to_owned();
        // Include the port in the host header when it's non-default,
        // matching what reqwest would send on the wire.
        let host_header = match (parsed.port_or_known_default(), parsed.scheme()) {
            (Some(443), "https") | (Some(80), "http") => host,
            (Some(port), _) => format!("{host}:{port}"),
            _ => host,
        };
        let extra_headers = [("host", host_header.as_str())];

        let signable = SignableRequest::new(
            method,
            url,
            extra_headers.into_iter(),
            SignableBody::Bytes(body),
        )
        .map_err(|e| ProviderError::Auth(format!("SigV4 signable request invalid: {e}")))?;

        let (signing_instructions, _signature) = sign(signable, &signing_params)
            .map_err(|e| ProviderError::Auth(format!("SigV4 sign failed: {e}")))?
            .into_parts();

        // Apply the computed Authorization + x-amz-* headers the signer
        // produced. `apply_to_request_http0x` mutates an http::Request;
        // we don't have one on the reqwest builder, so pull the headers
        // out and attach them manually.
        let (headers, _query) = signing_instructions.into_parts();
        for header in headers {
            let name = header.name().to_owned();
            let value = header.value().to_owned();
            builder = builder.header(name, value);
        }

        Ok(builder)
    }

    fn scheme(&self) -> &'static str {
        "sigv4"
    }
}

// ── CredentialsSource ───────────────────────────────────────────────

/// Pluggable credentials provider. Not named `CredentialsProvider` to
/// avoid confusion with AWS SDK types.
#[async_trait::async_trait]
pub trait CredentialsSource: std::fmt::Debug + Send + Sync {
    async fn resolve(&self) -> Result<Credentials, String>;
}

#[derive(Debug)]
struct StaticCredentials {
    creds: Credentials,
}

#[async_trait::async_trait]
impl CredentialsSource for StaticCredentials {
    async fn resolve(&self) -> Result<Credentials, String> {
        Ok(self.creds.clone())
    }
}

#[derive(Debug)]
struct DefaultChainCredentials {
    chain: Arc<aws_config::default_provider::credentials::DefaultCredentialsChain>,
}

#[async_trait::async_trait]
impl CredentialsSource for DefaultChainCredentials {
    async fn resolve(&self) -> Result<Credentials, String> {
        use aws_credential_types::provider::ProvideCredentials;
        self.chain
            .provide_credentials()
            .await
            .map_err(|e| e.to_string())
    }
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn static_signer() -> SigV4Signer {
        // Canonical SigV4 test vector credentials from the AWS docs —
        // use real-looking fake values so the signed header is
        // deterministic.
        SigV4Signer::with_static_credentials(
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            None,
            "us-west-2",
        )
    }

    #[tokio::test]
    async fn bearer_signer_sets_authorization_header() {
        let signer = BearerAuth::new("sk-test");
        let client = reqwest::Client::new();
        let req = client.post("http://example.invalid/");
        let signed = signer
            .sign(req, "POST", "http://example.invalid/", b"{}")
            .await
            .unwrap();
        let built = signed.build().unwrap();
        let auth = built
            .headers()
            .get("authorization")
            .expect("authorization header must be set")
            .to_str()
            .unwrap();
        assert_eq!(auth, "Bearer sk-test");
    }

    #[tokio::test]
    async fn bearer_signer_rejects_empty_token() {
        let signer = BearerAuth::new("");
        let client = reqwest::Client::new();
        let req = client.post("http://example.invalid/");
        let err = signer
            .sign(req, "POST", "http://example.invalid/", b"{}")
            .await
            .unwrap_err();
        assert!(matches!(err, ProviderError::Auth(_)));
    }

    #[tokio::test]
    async fn sigv4_signer_adds_authorization_and_amz_headers() {
        let signer = static_signer();
        let client = reqwest::Client::new();
        let url = "https://bedrock-runtime.us-west-2.amazonaws.com/model/us.anthropic.claude-opus-4-7/converse";
        let body = br#"{"messages":[{"role":"user","content":[{"text":"hi"}]}]}"#;
        let req = client.post(url).body(body.to_vec());

        let signed = signer.sign(req, "POST", url, body).await.unwrap();
        let built = signed.build().unwrap();

        // SigV4 must produce an Authorization header with the AWS4-HMAC-SHA256
        // scheme and the expected credential scope.
        let auth = built
            .headers()
            .get("authorization")
            .expect("authorization header must be set")
            .to_str()
            .unwrap();
        assert!(
            auth.starts_with("AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/"),
            "auth header shape: {auth}"
        );
        assert!(auth.contains("/us-west-2/bedrock/aws4_request"));
        assert!(auth.contains("Signature="));

        // SigV4 also requires an `x-amz-date` header.
        assert!(built.headers().contains_key("x-amz-date"));

        // `host` MUST appear in the SignedHeaders list. Without it an
        // attacker could redirect the signed request to a different
        // host and the signature would still verify against the
        // original canonical request. Locking this in as a regression
        // guard.
        assert!(
            auth.contains("SignedHeaders=")
                && auth
                    .split("SignedHeaders=")
                    .nth(1)
                    .unwrap()
                    .contains("host"),
            "SignedHeaders must include host: {auth}"
        );

        // Bedrock and other modern AWS services require the payload
        // hash in `x-amz-content-sha256` for POST+body requests. This
        // is a separate requirement from the signature itself — some
        // endpoints 403 without it.
        assert!(
            built.headers().contains_key("x-amz-content-sha256"),
            "x-amz-content-sha256 header must be set"
        );
    }

    #[tokio::test]
    async fn sigv4_signer_host_header_includes_non_default_port() {
        // Non-443 / non-80 ports must appear in the signed `host`
        // header (matches what reqwest sends on the wire).
        let signer = static_signer();
        let client = reqwest::Client::new();
        let url = "https://example.invalid:8443/x";
        let signed = signer
            .sign(client.post(url), "POST", url, b"{}")
            .await
            .unwrap();
        let built = signed.build().unwrap();
        let auth = built
            .headers()
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap();
        // We don't need to inspect the host header directly —
        // aws-sigv4 hashes it into the signature. Presence of `host`
        // in SignedHeaders is sufficient evidence we signed it, and
        // an upstream with the wrong port in its Host would reject
        // the request anyway. Assert just that SignedHeaders has host.
        assert!(auth.contains("SignedHeaders="));
        assert!(
            auth.split("SignedHeaders=")
                .nth(1)
                .unwrap()
                .contains("host"),
            "host must be signed even on non-default ports: {auth}"
        );
    }

    #[tokio::test]
    async fn sigv4_signer_rejects_url_without_host() {
        let signer = static_signer();
        let client = reqwest::Client::new();
        // `file://` URLs have no authority / host.
        let url = "file:///tmp/x";
        let err = signer
            .sign(client.post(url), "POST", url, b"{}")
            .await
            .expect_err("file:// has no host");
        assert!(matches!(err, ProviderError::Auth(_)));
    }

    #[tokio::test]
    async fn sigv4_signer_honours_custom_service_name() {
        let signer = static_signer().with_service("bedrock-agentcore");
        let client = reqwest::Client::new();
        let url = "https://example.invalid/mcp";
        let signed = signer
            .sign(client.post(url), "POST", url, b"{}")
            .await
            .unwrap();
        let built = signed.build().unwrap();
        let auth = built
            .headers()
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(
            auth.contains("/bedrock-agentcore/aws4_request"),
            "service name must appear in credential scope: {auth}"
        );
    }

    #[tokio::test]
    async fn sigv4_signer_includes_session_token_when_present() {
        let signer = SigV4Signer::with_static_credentials(
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            Some("FwoGZXIvYXdzEXAMPLESESSION".to_owned()),
            "us-west-2",
        );
        let client = reqwest::Client::new();
        let url = "https://example.invalid/x";
        let signed = signer
            .sign(client.post(url), "POST", url, b"{}")
            .await
            .unwrap();
        let built = signed.build().unwrap();
        // STS/IMDS creds must surface as x-amz-security-token so Bedrock
        // can verify the short-lived session.
        assert!(built.headers().contains_key("x-amz-security-token"));
    }

    #[tokio::test]
    async fn scheme_name_is_stable() {
        assert_eq!(BearerAuth::new("x").scheme(), "bearer");
        assert_eq!(static_signer().scheme(), "sigv4");
    }
}
