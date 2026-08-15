//! cdylib sync bridge — adapts the async [`GrpcBackendPlugin`]
//! ([`mcpg_plugin_protocol::BackendPlugin`]) onto the sync FFI trait the
//! cdylib vtable expects ([`SyncBackendPlugin`]).
//!
//! Minimal vs the http bridge: gRPC is request/reply, so it inherits the
//! buffered `execute_streaming` default (single `Done` chunk) and the
//! no-op `cancel_stream` / `complete_template_variable`. Only
//! manifest / kind / register_profile / execute / audit_metadata are
//! forwarded, each `block_on`-ing the async inner plugin on a private
//! multi-thread runtime. The make-time [`HostHandle`] is wrapped as an
//! `Arc<dyn BackendHost>` (via [`HostHandleBackendHost`]) for
//! `register_profile`'s `cred://` resolution.

use std::sync::Arc;

use mcpg_plugin_protocol::{
    BackendError, BackendPlugin, BackendRequest, BackendResponse, PluginManifest,
};
use mcpg_plugin_sdk::ffi::SyncBackendPlugin;
use mcpg_plugin_sdk::{HostHandle, HostHandleBackendHost};

use crate::GrpcBackendPlugin;

/// Build the private multi-thread runtime the bridge uses to `block_on`
/// the async inner plugin. Two workers + `enable_all`, matching the
/// http/nats/sql/LLM bridges.
fn build_bridge_runtime(thread_name: &str) -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name(thread_name.to_owned())
        .enable_all()
        .build()
        .unwrap_or_else(|e| panic!("grpc cdylib: tokio runtime init failed: {e}"))
}

/// `SyncBackendPlugin` bridge over [`GrpcBackendPlugin`].
pub struct GrpcBackendCdylib {
    inner: GrpcBackendPlugin,
    host: Arc<dyn mcpg_plugin_protocol::BackendHost>,
    rt: tokio::runtime::Runtime,
}

impl GrpcBackendCdylib {
    /// Infallible cdylib factory. `config_json` is ignored — gRPC carries
    /// no plugin-level config (per-binding url/service/method/headers
    /// arrive via `register_profile`).
    pub fn from_host_config(_config_json: &str, host: HostHandle) -> Self {
        Self {
            inner: GrpcBackendPlugin::new(),
            host: Arc::new(HostHandleBackendHost::new(host)),
            rt: build_bridge_runtime("mcpg-backend-grpc"),
        }
    }
}

impl SyncBackendPlugin for GrpcBackendCdylib {
    fn manifest(&self) -> &PluginManifest {
        BackendPlugin::manifest(&self.inner)
    }

    fn kind(&self) -> &str {
        BackendPlugin::kind(&self.inner)
    }

    fn register_profile(
        &self,
        profile_name: &str,
        spec: &serde_json::Value,
    ) -> Result<(), BackendError> {
        self.rt.block_on(BackendPlugin::register_profile(
            &self.inner,
            profile_name,
            spec,
            Arc::clone(&self.host),
        ))
    }

    fn execute(
        &self,
        profile_name: &str,
        request: BackendRequest,
    ) -> Result<BackendResponse, BackendError> {
        self.rt
            .block_on(BackendPlugin::execute(&self.inner, profile_name, request))
    }

    fn audit_metadata(&self, profile_name: &str) -> serde_json::Map<String, serde_json::Value> {
        BackendPlugin::audit_metadata(&self.inner, profile_name)
    }
}

// cdylib export — one `backend` entity under `dev.mcpg.backend.grpc`.
mcpg_plugin_sdk::declare_plugin! {
    plugin_id: "dev.mcpg.backend.grpc",
    plugin_version: env!("CARGO_PKG_VERSION"),
    descriptor_yaml: include_str!("../plugin.yaml"),
    capabilities: &[::mcpg_plugin_protocol::capability::Capability::NetworkOutbound],
    // gRPC: pipeline-capable (a `kind: grpc` pipeline step), no dynamic
    // tool list, health is advisory (Skip), label defaults to the kind.
    // `url`/`service`/`method` are transport-only routing facts — the
    // gateway's generic spec-walk asserts no `cred://` lands there.
    backend_profile: ::mcpg_plugin_protocol::manifest::BackendProfile {
        pipeline_capable: true,
        // gRPC speaks HTTP/2 framing — a plain TCP reachability probe applies
        // (the host strips any scheme from `url` before connecting).
        health_probe: ::mcpg_plugin_protocol::manifest::HealthProbeDecl::Tcp,
        transport_only_fields: ::std::vec![
            "/url".to_owned(),
            "/service".to_owned(),
            "/method".to_owned(),
        ],
        ..::core::default::Default::default()
    },
    entities: [
        backend as binding {
            inner_name: "",
            plugin_type: GrpcBackendCdylib,
            factory: |cfg, host: ::mcpg_plugin_sdk::HostHandle|
                GrpcBackendCdylib::from_host_config(cfg, host),
        },
    ],
}
