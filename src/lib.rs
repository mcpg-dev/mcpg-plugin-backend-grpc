//! gRPC backend binding plugin for mcpg (`kind: "grpc"`).
//!
//! Dispatches tool calls as gRPC-over-HTTP JSON POSTs to
//! `{endpoint}/{service}/{method}`. The transport, security, and
//! lifecycle machinery — per-credential `reqwest::Client` cache, DNS
//! rebinding / SSRF guard, per-call CEL + `cred://` resolution,
//! body-limit truncation, downstream-error retry shaping — all come
//! from the shared `net-core` crate via [`NetworkProfileRuntime`]. This
//! crate only adds the gRPC framing: the `/{service}/{method}` request
//! target and the gRPC-shaped response envelope.
//!
//! Mirrors the http plugin's `register_profile` → resolve → exec →
//! envelope flow; the gateway projects the returned envelope onto
//! `tools/call`, recovering `is_error` from the `downstreamError` slot
//! (non-200 → error), exactly as it does for the http backend.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use mcpg_plugin_backend_net_core::exec;
use mcpg_plugin_backend_net_core::retry::{self, DownstreamHttpError};
use mcpg_plugin_backend_net_core::runtime::{NetworkProfileRuntime, build_expr_context};
use mcpg_plugin_backend_net_core::types::{
    HttpBackendMethod, HttpCallMode, HttpRequestProfile, HttpResponseSummary, RetrySafetyContext,
};
use mcpg_plugin_protocol::{
    BackendError, BackendHost, BackendPlugin, BackendRequest, BackendResponse, PluginManifest,
    firstparty_manifest,
};
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::RwLock;

pub mod cdylib;

/// Embedded plugin descriptor — passed to the gateway registrar at
/// startup.
pub const BINDING_DESCRIPTOR_YAML: &str = include_str!("../plugin.yaml");

/// Default per-call timeout. Matches the gateway binding default
/// (`default_binding_timeout_ms`) so a binding that omits `timeout_ms`
/// resolves to the identical value on either path.
fn default_timeout_ms() -> u64 {
    2_000
}
fn default_max_response_bytes() -> usize {
    65_536
}

/// Operator-facing spec the gateway serializes when calling
/// `register_profile`. Mirrors `GrpcBackendConfig` in the gateway crate.
#[derive(Debug, Clone, Deserialize)]
struct GrpcBackendSpec {
    url: String,
    service: String,
    method: String,
    #[serde(default)]
    headers: BTreeMap<String, String>,
    #[serde(default = "default_timeout_ms")]
    timeout_ms: u64,
    #[serde(default = "default_max_response_bytes")]
    max_response_bytes: usize,
    #[serde(default)]
    allow_private_backends: bool,
}

/// Per-binding runtime state: the shared net-core resolution runtime
/// (URL + headers + client cache) plus the structural gRPC service /
/// method (never templated — gRPC routing is fixed at config time).
#[derive(Clone)]
struct GrpcProfile {
    net: NetworkProfileRuntime,
    service: String,
    method: String,
}

/// `BackendPlugin` implementation for `kind: "grpc"`.
pub struct GrpcBackendPlugin {
    manifest: PluginManifest,
    profiles: RwLock<BTreeMap<String, GrpcProfile>>,
}

impl Default for GrpcBackendPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl GrpcBackendPlugin {
    #[must_use]
    pub fn new() -> Self {
        Self {
            manifest: firstparty_manifest! {
                id: "dev.mcpg.backend.grpc",
                name: "gRPC Binding",
                class: Backend,
            },
            profiles: RwLock::new(BTreeMap::new()),
        }
    }
}

impl std::fmt::Debug for GrpcBackendPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcBackendPlugin")
            .field("id", &self.manifest.id)
            .finish()
    }
}

#[async_trait]
impl BackendPlugin for GrpcBackendPlugin {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    fn kind(&self) -> &str {
        "grpc"
    }

    async fn register_profile(
        &self,
        backend_name: &str,
        spec: &serde_json::Value,
        host: Arc<dyn BackendHost>,
    ) -> Result<(), BackendError> {
        let parsed: GrpcBackendSpec =
            serde_json::from_value(spec.clone()).map_err(|e| BackendError::InvalidSpec {
                message: format!("gRPC binding spec: {e}"),
            })?;

        if parsed.url.trim().is_empty() {
            return Err(BackendError::InvalidSpec {
                message: "url must not be empty".into(),
            });
        }
        if !parsed.url.starts_with("http://") && !parsed.url.starts_with("https://") {
            return Err(BackendError::InvalidSpec {
                message: format!(
                    "url must start with http:// or https://, got '{}'",
                    parsed.url
                ),
            });
        }
        if parsed.service.trim().is_empty() {
            return Err(BackendError::InvalidSpec {
                message: "service must not be empty".into(),
            });
        }
        if parsed.method.trim().is_empty() {
            return Err(BackendError::InvalidSpec {
                message: "method must not be empty".into(),
            });
        }
        if parsed.timeout_ms == 0 {
            return Err(BackendError::InvalidSpec {
                message: "timeout_ms must be greater than 0".into(),
            });
        }
        if parsed.max_response_bytes == 0 {
            return Err(BackendError::InvalidSpec {
                message: "max_response_bytes must be greater than 0".into(),
            });
        }
        // `url`/`service`/`method` are transport-only routing facts the
        // plugin treats as plaintext (the request target), never as a
        // credential-bearing value — a `cred://` ref there is an operator
        // mistake that would leak a resolved secret into the URL path. The
        // gateway also enforces this generically via the manifest
        // `transport_only_fields` declaration; this is the owning plugin's
        // matching reject.
        for (field, value) in [
            ("url", parsed.url.as_str()),
            ("service", parsed.service.as_str()),
            ("method", parsed.method.as_str()),
        ] {
            if value.contains("cred://") {
                return Err(BackendError::InvalidSpec {
                    message: format!("{field} must not contain a cred:// reference"),
                });
            }
        }

        // gRPC always POSTs a JSON body and expects a 200 JSON reply.
        let profile = HttpRequestProfile {
            url: parsed.url.clone(),
            method: HttpBackendMethod::Post,
            headers: parsed.headers.clone(),
            expected_status_codes: vec![200],
            require_json_response: true,
            max_response_bytes: parsed.max_response_bytes,
            timeout: std::time::Duration::from_millis(parsed.timeout_ms),
            allow_private_backends: parsed.allow_private_backends,
        };

        let secret_refs: Vec<String> = spec
            .get("__mcpg_secret_refs")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default();

        let net = NetworkProfileRuntime::register(
            backend_name,
            parsed.url,
            parsed.headers,
            profile,
            host,
            secret_refs,
        )
        .map_err(|e| BackendError::InvalidSpec {
            message: format!("gRPC binding spec: {e}"),
        })?;

        self.profiles.write().await.insert(
            backend_name.to_owned(),
            GrpcProfile {
                net,
                service: parsed.service,
                method: parsed.method,
            },
        );
        Ok(())
    }

    async fn execute(
        &self,
        backend_name: &str,
        request: BackendRequest,
    ) -> Result<BackendResponse, BackendError> {
        let profile = {
            let guard = self.profiles.read().await;
            guard
                .get(backend_name)
                .cloned()
                .ok_or_else(|| BackendError::ProfileNotFound {
                    backend_name: backend_name.to_owned(),
                })?
        };

        let arguments: Value = if request.payload.is_empty() {
            serde_json::json!({})
        } else {
            serde_json::from_slice(&request.payload).map_err(|e| BackendError::InvalidSpec {
                message: format!("gRPC plugin payload is not valid JSON: {e}"),
            })?
        };

        let tool_name = request
            .headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("mcpg-tool-name"))
            .map(|(_, v)| v.as_str())
            .unwrap_or(backend_name)
            .to_owned();

        let trace_headers: Vec<(String, String)> = request
            .headers
            .iter()
            .filter(|(k, _)| {
                let lower = k.to_ascii_lowercase();
                lower == "traceparent" || lower == "tracestate"
            })
            .cloned()
            .collect();
        let idempotency_key = request.idempotency.as_ref().map(|hint| hint.key.clone());
        let operator_has_idempotency_key = profile.net.operator_has_header("idempotency-key");

        let expr_ctx = build_expr_context(&arguments, &tool_name, &request);
        let envelope = match profile
            .net
            .resolve_client(&expr_ctx, &request, backend_name)
            .await
        {
            Ok(resolved) => {
                // gRPC routes to `/{service}/{method}` at the endpoint's
                // host — the operator URL's path (if any) is replaced,
                // matching the inline gateway path.
                let target = match grpc_target_url(
                    &resolved.resolved_url,
                    &profile.service,
                    &profile.method,
                ) {
                    Ok(u) => u,
                    Err(e) => {
                        return Ok(error_response(build_grpc_envelope(
                            &tool_name,
                            backend_name,
                            &profile.service,
                            &profile.method,
                            Err(&e),
                        )));
                    }
                };
                let response = exec::execute_http_call(
                    &resolved.client,
                    profile.net.profile(),
                    HttpCallMode::JsonBody,
                    &arguments,
                    None,
                    &trace_headers,
                    idempotency_key.as_deref(),
                    operator_has_idempotency_key,
                    &target,
                )
                .await;
                build_grpc_envelope(
                    &tool_name,
                    backend_name,
                    &profile.service,
                    &profile.method,
                    response.as_ref().map_err(String::as_str),
                )
            }
            Err(e) => build_grpc_envelope(
                &tool_name,
                backend_name,
                &profile.service,
                &profile.method,
                Err(&e),
            ),
        };

        Ok(error_response(envelope))
    }

    fn audit_metadata(&self, _backend_name: &str) -> serde_json::Map<String, serde_json::Value> {
        let mut map = serde_json::Map::new();
        map.insert("grpc.transport".to_owned(), serde_json::json!("plugin"));
        map
    }
}

/// Serialize an envelope into a `BackendResponse`. The gateway reads
/// `downstreamError != null` to set `is_error`; the body is never
/// truncated at this layer (the body-limit cap already applied inside
/// the exec read).
fn error_response(envelope: Value) -> BackendResponse {
    let payload = serde_json::to_vec(&envelope).unwrap_or_else(|_| b"{}".to_vec());
    BackendResponse {
        payload,
        truncated: false,
    }
}

/// Build the gRPC request URL: the endpoint's scheme + authority with
/// the path replaced by `/{service}/{method}` (query dropped). Mirrors
/// the inline gateway path, which POSTed to `/{service}/{method}` at the
/// configured host regardless of any path in the endpoint URL.
fn grpc_target_url(base: &str, service: &str, method: &str) -> Result<String, String> {
    let mut u = url::Url::parse(base).map_err(|e| format!("invalid gRPC endpoint URL: {e}"))?;
    u.set_path(&format!("/{service}/{method}"));
    u.set_query(None);
    Ok(u.to_string())
}

/// Build the gRPC structured-content envelope. Carries the
/// gRPC-specific fields the inline gateway path surfaced (service /
/// method / statusCode / response) plus the shared `downstreamError`
/// slot the gateway reads for `is_error` (non-200 → error).
fn build_grpc_envelope(
    tool_name: &str,
    backend_name: &str,
    service: &str,
    method: &str,
    response: Result<&HttpResponseSummary, &str>,
) -> Value {
    match response {
        Ok(summary) => {
            let downstream: Option<DownstreamHttpError> = retry::validate_expected_status_codes(
                &[200],
                summary.status_code,
                summary.retry_after_ms,
                RetrySafetyContext::PotentiallyNonIdempotentJsonCall,
            );
            let response_json: Option<Value> = serde_json::from_str(&summary.body).ok();
            serde_json::json!({
                "toolName": tool_name,
                "profile": backend_name,
                "service": service,
                "method": method,
                "statusCode": summary.status_code,
                "durationMs": summary.duration_ms,
                "bodyTruncated": summary.body_truncated,
                "body": summary.body,
                "response": response_json,
                "downstreamError": downstream
                    .map(|e| serde_json::to_value(e).unwrap_or(Value::Null))
                    .unwrap_or(Value::Null),
            })
        }
        Err(error) => {
            let downstream = retry::transport_downstream_error(
                error,
                RetrySafetyContext::PotentiallyNonIdempotentJsonCall,
            );
            serde_json::json!({
                "toolName": tool_name,
                "profile": backend_name,
                "service": service,
                "method": method,
                "error": error,
                "downstreamError": serde_json::to_value(&downstream).unwrap_or(Value::Null),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binding_plugin_kind_is_grpc() {
        let plugin = GrpcBackendPlugin::new();
        assert_eq!(plugin.kind(), "grpc");
    }

    #[test]
    fn manifest_advertises_first_party_id() {
        let plugin = GrpcBackendPlugin::new();
        assert_eq!(plugin.manifest().id, "dev.mcpg.backend.grpc");
    }

    #[test]
    fn grpc_target_replaces_path() {
        let u = grpc_target_url("https://grpc.example.com/ignored?x=1", "pkg.Svc", "Method")
            .expect("url");
        assert_eq!(u, "https://grpc.example.com/pkg.Svc/Method");
    }

    #[test]
    fn envelope_flags_non_200_as_downstream_error() {
        let summary = HttpResponseSummary {
            status_code: 503,
            content_type: Some("application/json".to_owned()),
            retry_after_ms: None,
            body: "{}".to_owned(),
            body_truncated: false,
            duration_ms: 1,
        };
        let env = build_grpc_envelope("t", "b", "Svc", "M", Ok(&summary));
        assert!(!env["downstreamError"].is_null());
    }

    #[test]
    fn envelope_200_has_no_downstream_error() {
        let summary = HttpResponseSummary {
            status_code: 200,
            content_type: Some("application/json".to_owned()),
            retry_after_ms: None,
            body: r#"{"ok":true}"#.to_owned(),
            body_truncated: false,
            duration_ms: 1,
        };
        let env = build_grpc_envelope("t", "b", "Svc", "M", Ok(&summary));
        assert!(env["downstreamError"].is_null());
        assert_eq!(env["response"], serde_json::json!({"ok": true}));
    }

    #[tokio::test]
    async fn register_rejects_empty_service() {
        let plugin = GrpcBackendPlugin::new();
        let spec = serde_json::json!({
            "url": "https://grpc.example.com",
            "service": "",
            "method": "Get",
        });
        let err = plugin
            .register_profile("test", &spec, mcpg_plugin_protocol::noop_backend_host())
            .await
            .expect_err("should reject empty service");
        assert!(matches!(err, BackendError::InvalidSpec { .. }));
    }

    // --- Conformance: the plugin is the single source of truth for its
    // defaults + value-validation + transport-only cred:// reject (the
    // checks that used to live in the gateway's GrpcBackendConfig). ---

    /// Omitting `timeout_ms` / `max_response_bytes` resolves to the same
    /// defaults the gateway binding applied (2000ms / 64 KiB) — the
    /// secure/default value is materialized by the plugin, not the gateway.
    #[tokio::test]
    async fn register_applies_default_timeout_and_size() {
        let plugin = GrpcBackendPlugin::new();
        let spec = serde_json::json!({
            "url": "https://grpc.example.com",
            "service": "pkg.Svc",
            "method": "Get",
        });
        plugin
            .register_profile("test", &spec, mcpg_plugin_protocol::noop_backend_host())
            .await
            .expect("registers with defaults");
        let guard = plugin.profiles.read().await;
        let profile = guard.get("test").expect("profile stored");
        assert_eq!(
            profile.net.profile().timeout,
            std::time::Duration::from_millis(2_000),
            "timeout_ms defaults to 2000 (gateway binding default)",
        );
        assert_eq!(
            profile.net.profile().max_response_bytes,
            65_536,
            "max_response_bytes defaults to 64 KiB",
        );
    }

    /// A bad scheme is rejected as `InvalidSpec` (value-validation moved
    /// from the gateway's `GrpcBackendConfig::validate`).
    #[tokio::test]
    async fn register_rejects_non_http_url() {
        let plugin = GrpcBackendPlugin::new();
        let spec = serde_json::json!({
            "url": "ftp://grpc.example.com",
            "service": "pkg.Svc",
            "method": "Get",
        });
        let err = plugin
            .register_profile("test", &spec, mcpg_plugin_protocol::noop_backend_host())
            .await
            .expect_err("should reject non-http url");
        assert!(matches!(err, BackendError::InvalidSpec { .. }));
    }

    /// `timeout_ms: 0` and `max_response_bytes: 0` are rejected.
    #[tokio::test]
    async fn register_rejects_zero_timeout_and_size() {
        let plugin = GrpcBackendPlugin::new();
        for bad in [
            serde_json::json!({ "url": "https://g", "service": "s", "method": "m", "timeout_ms": 0 }),
            serde_json::json!({ "url": "https://g", "service": "s", "method": "m", "max_response_bytes": 0 }),
        ] {
            let err = plugin
                .register_profile("test", &bad, mcpg_plugin_protocol::noop_backend_host())
                .await
                .expect_err("should reject zero value");
            assert!(matches!(err, BackendError::InvalidSpec { .. }));
        }
    }

    /// A bare `cred://` ref in a transport-only field (url/service/method)
    /// is rejected — these are plaintext routing facts, never credential
    /// carriers, so a `cred://` there would leak a resolved secret.
    #[tokio::test]
    async fn register_rejects_cred_in_transport_only_field() {
        let plugin = GrpcBackendPlugin::new();
        for bad in [
            serde_json::json!({ "url": "cred://vault/url", "service": "s", "method": "m" }),
            serde_json::json!({ "url": "https://g", "service": "cred://vault/svc", "method": "m" }),
            serde_json::json!({ "url": "https://g", "service": "s", "method": "cred://vault/m" }),
        ] {
            let err = plugin
                .register_profile("test", &bad, mcpg_plugin_protocol::noop_backend_host())
                .await
                .expect_err("should reject cred:// in transport-only field");
            assert!(matches!(err, BackendError::InvalidSpec { .. }));
        }
    }
}
