# Lix Server Protocol

The Lix Server Protocol is the canonical HTTP contract for hosting a Lix
workspace. Its methods, `/lix/v1` paths, wire formats, session behavior, and
reference implementation live in the `lix` crate. HTTP frameworks and
deployment policy do not.

Enable the implementation explicitly:

```toml
[dependencies]
lix = { version = "0.11", features = ["server-protocol"] }
```

The feature is off by default. Embedded users do not compile the HTTP protocol
surface or its optional dependencies.

## Host one workspace

Open a Lix workspace and retain one `LixServerProtocol` for the entire time the
workspace is hosted:

```rust,no_run
use std::sync::Arc;
use lix::server_protocol::{
    LixServerProtocol, ServerProtocolContext, ServerProtocolPrincipal,
};

# async fn example(request: lix::server_protocol::ServerProtocolRequest)
#     -> Result<lix::server_protocol::ServerProtocolResponse, lix::LixError> {
let workspace = Arc::new(lix::open_lix().await?);
let protocol = LixServerProtocol::new(workspace);

// Authentication happens in the host, before protocol dispatch.
let context = ServerProtocolContext {
    principal: ServerProtocolPrincipal::Authenticated {
        account_id: "01920000-0000-7000-8000-000000000001".to_owned(),
        idempotency_scope: "identity-provider:user-123".to_owned(),
    },
    durable_terminal_storage_notifier: None,
};

let response = protocol.handle(request, context).await;
# Ok(response)
# }
```

`ServerProtocolRequest` and `ServerProtocolResponse` use the ecosystem `http`
types and `ServerProtocolBody`. An Axum, Hyper, Actix, or custom host only needs
to convert its body into `ServerProtocolBody`, invoke `handle`, and forward the
returned status, headers, extensions, and body.

The host owns the outer URL. For example, it may mount a workspace at
`/workspaces/{workspace_id}` and strip that prefix before dispatch. Everything
beginning at `/lix/v1` is owned by the protocol and must not be renamed.

## Authentication

The protocol does not validate bearer tokens, cookies, API keys, or mTLS
certificates. The host validates credentials and then constructs trusted
in-process context:

- Use `ServerProtocolPrincipal::Authenticated` for a verified identity.
- Use `ServerProtocolPrincipal::Anonymous` only when the host deliberately
  permits anonymous access.
- Never derive `account_id` or `idempotency_scope` from unverified request
  headers or query parameters.

On session creation the implementation ensures that an authenticated Lix
account exists, pins the session to it, and scopes mutation idempotency to the
trusted principal. Reusing a session through another principal returns `403`.
The client cannot select `activeAccountId` during the handshake.

Invalid credentials should be rejected by the host with `401` before calling
the protocol. Valid credentials that do not own an existing protocol session
receive the protocol's canonical `403` error envelope.

## Connect a JavaScript client

```ts
import { openLix } from "@lix-js/sdk";

const lix = await openLix({
  server: {
    mode: "remote",
    url: "https://example.com/workspaces/acme",
    headers: async () => ({
      Authorization: `Bearer ${await getAccessToken()}`,
    }),
  },
});
```

The SDK appends `/lix/v1/`, opens a server-issued session, sends the
`Lix-Session-Id` capability on later requests, and reconnects observation
streams. The handshake response reports the account chosen by the host.

## Host responsibilities

Before dispatch, the host must:

- authenticate and select a trusted principal;
- resolve the outer workspace identifier and retain its runtime;
- strip the outer route prefix;
- decompress request content and enforce a compressed-body safety limit;
- convert the decoded body to `ServerProtocolBody`.

After dispatch, the host may apply response compression, CORS, deadlines,
rate limits, and telemetry. It must preserve protocol status codes, headers,
body bytes, and streaming behavior. SSE responses must retain the workspace
runtime until their body closes.

`DurableTerminalStorageNotifier`, `is_terminal_storage_response`, and
`terminal_storage_stream_signal` let a production host replace a terminal
storage runtime without parsing wire error bodies or SSE frames.

Call `LixServerProtocol::close()` during workspace shutdown. Dropping a live
server invalidates its in-memory session capabilities.

## Contract

The machine-readable HTTP surface is
[`packages/lix/server-protocol.openapi.yaml`](../packages/lix/server-protocol.openapi.yaml).
Behavior that OpenAPI cannot fully express—session pinning, transaction
ownership, idempotency replay, observation ordering, and terminal storage
semantics—is normative in the reference implementation and its tests.
