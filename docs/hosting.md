---
description: Run Lix against the official host at lixray.com, or host Lixes yourself with the Lix Server Protocol.
---

# Hosting

A hosted Lix lives on a server. The server owns its storage and
authentication. Clients can execute directly on the server or keep a
synchronized local replica.

| Client mode | Use for                                                             |
| :---------- | :------------------------------------------------------------------ |
| `remote`    | The simplest setup. Every operation executes on the server.         |
| `sync`      | Responsive and offline apps. Operations execute on a local replica. |

Both modes use the same Lix URL and server protocol. See
[Collaboration and Sync](./collaboration-and-sync.md) to choose a mode.

The simplest client uses remote mode:

```ts
import { openLix } from "@lix-js/sdk";

const lix = await openLix({
  server: {
    mode: "remote",
    url: "https://lixray.com/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
  },
});
```

There are two ways to get a server:

| Option                           | Who runs it  | Use for                                   |
| :------------------------------- | :----------- | :---------------------------------------- |
| [lixray.com](https://lixray.com) | The Lix team | Getting started, and teams without a host |
| Your own host                    | You          | Your infrastructure, your auth, your data |

Both speak the same protocol. Only the URL changes in client code.

## Official host: lixray.com

[LixRay](https://lixray.com) is the official Lix host. Copy the immutable Lix
connection URL and pass it to `openLix()`:

```ts
import { openLix } from "@lix-js/sdk";

const lix = await openLix({
  server: {
    mode: "remote",
    url: "https://lixray.com/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
    headers: async () => ({
      Authorization: `Bearer ${await getAccessToken()}`,
    }),
  },
});
```

The connection URL is an absolute HTTPS URL whose path is exactly
`/lix/{uuid}`. HTTP is accepted only for loopback development. It carries no
query, fragment, credentials, or deployment-path prefix. Human-readable
namespace and project URLs are separate web-page addresses, not Lix connection
URLs.

Files, SQL, branches, history, and `observe()` work the same way they do
locally. See [Collaboration and Sync](./collaboration-and-sync.md).

## Host it yourself

Companies that must keep repositories on their own infrastructure can run their
own host. The `lix` crate contains the server implementation, so a host does not
implement the protocol. It provides HTTP and authentication and forwards
requests.

Enable the feature:

```toml
[dependencies]
lix = { version = "0.11", features = ["server-protocol"] }
```

Open a repository and keep one `LixServerProtocol` for as long as it is hosted:

```rust
use lix::server_protocol::{
    ServerProtocolContext, ServerProtocolPrincipal,
};

let protocol = lix::open_lix()
    .with_storage(storage)
    .serve()
    .with_lix_id("01936f4e-7b6c-7c3d-8f9a-123456789abc")
    .await?;

// Your host authenticates first, then calls the protocol.
let context = ServerProtocolContext {
    principal: ServerProtocolPrincipal::Authenticated {
        account_id: account_id.to_owned(),
        idempotency_scope: "identity-provider:user-123".to_owned(),
    },
    durable_terminal_storage_notifier: None,
};

let response = protocol.handle(request, context).await;
```

`with_lix_id` binds the host's stable resource UUID. It can differ from the
portable identity stored inside a restored snapshot. The protocol validates
that every request targets this bound UUID.

`request` is a `ServerProtocolRequest`, which is
`http::Request<ServerProtocolBody>`. The response is
`http::Response<ServerProtocolBody>`. Converting your framework's body type into
`ServerProtocolBody` is the only adapter code you write.

Your host is responsible for three things:

1. **Authenticate the request** and choose a principal. The protocol does not
   read tokens, cookies, or certificates. Never derive an `account_id` from an
   unverified header.
2. **Resolve the Lix** identified by `{lix_id}` without creating an unknown
   target. Pass the complete root `/lix/v1/{lix_id}/...` request to the
   protocol; it validates the immutable ID before dispatch. If your product is
   mounted below a deployment prefix, strip that prefix at the reverse-proxy
   boundary before dispatch. SDK connection locators themselves never contain
   a deployment prefix.
3. **Forward the request and the response.** Preserve protocol status codes,
   headers, and body bytes. Keep the Lix runtime alive until every streaming
   response body closes, including server-sent events (SSE) and snapshot
   downloads.

Use `ServerProtocolPrincipal::Anonymous` only where you deliberately allow
anonymous access. Reject bad credentials with `401` before dispatch. Call
`LixServerProtocol::close()` on shutdown.

Clients connect exactly as they do to lixray.com. Only the URL changes.

The SDK accepts `https://host/lix/{lix_id}`, derives the versioned API URL,
opens a session, and reconnects observation streams on its own. HTTPS is
required except for HTTP loopback addresses used in local development.

For the wire format, session behavior, and the OpenAPI document, see
[Lix Server Protocol](./server-protocol.md).

### Storage on your own host

The host chooses where bytes live. `SlateDB` stores a repository on
S3-compatible object storage; `RocksDB` stores it on a local disk. Clients never
configure this:

```text
JS client ── HTTP ──▶ your Lix server ──▶ SlateDB ──▶ S3
```

See [Persistence and Storage](./persistence.md).
