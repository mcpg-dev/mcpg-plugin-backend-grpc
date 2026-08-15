# gRPC Binding — `dev.mcpg.backend.grpc`

> class `backend` · `native` · package `mcpg-plugin-backend-grpc` · artifact `libmcpg_plugin_backend_grpc.so` · BUSL-1.1

Backend binding plugin for the MCPG gateway that calls a gRPC service
over its JSON transcoding surface. Each call POSTs the caller's
arguments as a JSON body to `{origin}/{service}/{method}`, so it targets
a gRPC endpoint fronted by a JSON transcoder (grpc-gateway, the Envoy
gRPC-JSON filter, Connect, or an equivalent) rather than speaking
protobuf framing itself. Reach for it to expose one RPC method as a
governed MCP tool, resource, or prompt. It declares `network_outbound`
as a required capability, so the matching `plugins[]` entry has to grant
it or the gateway refuses the plugin at boot.

## What it does
- POSTs the call arguments as a JSON body to `{origin}/{service}/{method}`
  and expects HTTP 200 with a JSON reply.
- Rewrites the request target from the binding's `url`: scheme and
  authority are kept, and any path or query on the configured URL is
  replaced by `/{service}/{method}`.
- Resolves the URL and every header value as a CEL template per call, so
  `${arguments.*}` and `${context.*}` reach the wire.
- Substitutes `${cred://issuer/target}` tokens in header values through
  the gateway's credential path, per caller identity, at dispatch time.
- Caches one `reqwest::Client` per resolved-credential bundle and evicts
  on credential revocation, secret rotation, and idle expiry.
- Blocks connections to private, loopback, and link-local addresses
  unless the binding opts in, and pins the validated address for the
  life of the client.
- Truncates the response body at `max_response_bytes` and reports the
  truncation in the envelope.
- Advertises a TCP reachability health probe against the host and port
  of the binding's `url`, and is usable as a pipeline step.

## Configuration
The `plugins:` entry loads the cdylib and takes no `config:` block; the
per-call configuration lives in each binding's `backend:` block, keyed by
the `kind: grpc` discriminator.

```yaml
plugins:
  - id: dev.mcpg.backend.grpc
    class: backend
    kind: native
    source:
      path: ./plugins/libmcpg_plugin_backend_grpc.so
    granted_capabilities:
      - network_outbound

mcp:
  capabilities:
    tools:
      - name: users.get
        description: Look up a user record.
        backend:
          kind: grpc
          url: "https://users.internal:8443"
          service: user.v1.UserService
          method: GetUser
          timeout_ms: 2000
          max_response_bytes: 65536
          headers:
            authorization: "Bearer ${cred://users-oauth/api}"
        input_schema:
          type: object
          properties:
            id: { type: string }
          required: [id]
```

| Field | Type | Default | Description |
|---|---|---|---|
| `url` | string | — (required) | Endpoint base URL. Must start with `http://` or `https://`; only scheme and authority are used. Transport-only: a `cred://` reference here is rejected at registration. |
| `service` | string | — (required) | Fully-qualified service name (`package.v1.Service`). Transport-only. |
| `method` | string | — (required) | RPC method name. Transport-only. |
| `headers` | map<string,string> | `{}` | Request headers. Values are CEL templates and may carry `${cred://issuer/target}`. |
| `timeout_ms` | u64 | `2000` | Per-call timeout. Must be greater than 0. |
| `max_response_bytes` | usize | `65536` | Response body cap; oversized bodies are truncated. Must be greater than 0. |
| `allow_private_backends` | bool | `false` | Permit connections to private/loopback/link-local addresses. Container-network deployments only. |

## Security
`url`, `service`, and `method` are transport-only routing facts, so
registration rejects any `cred://` reference in them — a resolved secret
must never land in a request path. Credentials belong in header values,
where the `${cred://issuer/target}` token form is the only thing that
resolves: a bare `cred://…` outside `${}` is a literal and travels to the
upstream untouched, which is what keeps a caller from smuggling a
credential reference through a request argument.

Outbound connections go through a DNS rebinding guard. Resolution walks
the address list, picks the first address outside the private, loopback,
CGNAT, and link-local ranges, and pins it on the client, so a DNS record
that flips to an internal address after validation cannot be reached; a
host that resolves only to private addresses fails the call with an
operator-facing guard error. Redirects are disabled, and hop-by-hop or
proxy-topology headers are dropped rather than forwarded: `host`,
`connection`, `content-length`, `forwarded`, `via`, `x-real-ip`,
`x-request-id`, any `x-forwarded-*`, and — because every call is a JSON
POST — `accept` and `content-type`.

## Response envelope
`execute` returns a JSON document carrying `toolName`, `profile`,
`service`, `method`, `statusCode`, `durationMs`, `bodyTruncated`, the
raw `body`, the parsed `response`, and a `downstreamError` slot. That
slot is populated for a non-200 status or a transport failure; each
error carries a stable `code`, `retryable`, `retryClass`,
`backoffStrategy`, and `suggestedAction` so a caller can decide whether
a retry is safe.

## Connection pooling
One `reqwest::Client` is cached per resolved-credential bundle, keyed by
a BLAKE3 digest over the post-substitution URL and header values. A
binding with no credential tokens collapses to a single cached client for
every call. The cache holds at most 256 clients, a background sweep runs
every 60 seconds and drops entries idle for 15 minutes, and entries are
evicted immediately when the gateway signals credential revocation or
rotation of a secret this binding referenced.

## MCP surfaces & composition
The binding is declared per capability under `mcp.capabilities.*`; the
same `backend:` block shape works on every surface.

### As a pipeline step
`kind: grpc` is pipeline-capable. Step keys other than `id` and
`input_transform` flatten into the spec.

```yaml
backend:
  kind: pipeline
  steps:
    - kind: grpc
      id: fetch_user
      url: "https://users.internal:8443"
      service: user.v1.UserService
      method: GetUser
```

### As a resource
```yaml
mcp:
  capabilities:
    resources:
      - name: users.roster
        description: The current user roster.
        uri: "users://roster"
        mime_type: application/json
        backend:
          kind: grpc
          url: "https://users.internal:8443"
          service: user.v1.UserService
          method: ListUsers
```

### As a prompt
```yaml
mcp:
  capabilities:
    prompts:
      - name: users.introduce
        description: Introduce a user to a teammate.
        prompt_arguments:
          - name: id
            required: true
        backend:
          kind: grpc
          url: "https://users.internal:8443"
          service: user.v1.UserService
          method: GetUser
```

### Schemas & annotations
Every binding accepts the MCP descriptor fields as siblings of
`backend:` — `title`, `input_schema`, `output_schema`, `icons`, and
`annotations` (`read_only`, `destructive`, `idempotent`, `open_world`).
A sibling `retry:` block (`max_attempts` default `3`,
`initial_backoff_ms` default `200`, `retry_on_status_codes` default
`[429, 502, 503, 504]`, `retry_on_transport_error` default true) governs
gateway-side retries, and `governance:` carries the trust floor and CEL
authorization for the surface.

## Build
Default feature set is OFF (avoids `mcpg_plugin_register` linker
collisions in the workspace build); opt in to the cdylib:

```bash
cargo build -p mcpg-plugin-backend-grpc --features cdylib-export --release   # → target/release/libmcpg_plugin_backend_grpc.so
```

Releases publish a platform-agnostic OCI artifact, so a `plugins:` entry
can set `source.oci` to
`ghcr.io/mcpg-dev/source-code/plugins/backend-grpc:protocol-1` instead of
`source.path` and let the gateway resolve the right os/arch/libc build
for its host.

## Testing
```bash
cargo test -p mcpg-plugin-backend-grpc
```

The suite runs offline. Unit tests cover spec validation, target-URL
rewriting, and envelope classification; the integration suite drives a
local `wiremock` server, so no external gRPC endpoint is required.

## Sign & load (production)
Sign the artifact, pin/verify via the entry's `signature:` block, and honour
revocations. See <https://mcpg.dev/docs/security/plugin-security>.

## See also
- Backend binding reference: <https://mcpg.dev/docs/reference/backends>
- Pipeline step kinds: <https://mcpg.dev/docs/reference/pipeline-steps>
- Licence terms for this plugin — BUSL-1.1, with an Additional Use Grant
  for production use and a Change License of Apache-2.0: [LICENSE](LICENSE)
- Siblings sharing the same HTTP core: `libs/plugins/backend/http`, `libs/plugins/backend/graphql`, `libs/plugins/backend/net-core`
