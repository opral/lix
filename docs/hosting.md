---
description: Run Lix against the official host at lixray.com, or host repositories yourself with the Lix Server Protocol.
---

# Hosting

A hosted repository lives on a server. The server owns its storage and
authentication. Clients can execute directly on the server or keep a
synchronized local replica.

| Client mode | Use for                                                             |
| :---------- | :------------------------------------------------------------------ |
| `remote`    | The simplest setup. Every operation executes on the server.         |
| `sync`      | Responsive and offline apps. Operations execute on a local replica. |

Both modes use the same repository URL and server protocol. See
[Collaboration and Sync](./collaboration-and-sync.md) to choose a mode.

The simplest client uses remote mode:

```ts
import { openLix } from "@lix-js/sdk";

const lix = await openLix({
  server: {
    mode: "remote",
    url: "https://lixray.com/@acme/repository",
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

[LixRay](https://lixray.com) is the official Lix host. Create a repository there
and point `openLix()` at its URL:

```ts
import { openLix } from "@lix-js/sdk";

const lix = await openLix({
  server: {
    mode: "remote",
    url: "https://lixray.com/@acme/repository",
    headers: async () => ({
      Authorization: `Bearer ${await getAccessToken()}`,
    }),
  },
});
```

The URL is the repository URL, `https://lixray.com/@<namespace>/<repository>`.
It must be absolute and carry no query or fragment.

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

`request` is a `ServerProtocolRequest`, which is
`http::Request<ServerProtocolBody>`. The response is
`http::Response<ServerProtocolBody>`. Converting your framework's body type into
`ServerProtocolBody` is the only adapter code you write.

Your host is responsible for three things:

1. **Authenticate the request** and choose a principal. The protocol does not
   read tokens, cookies, or certificates. Never derive an `account_id` from an
   unverified header.
2. **Resolve the repository** the URL points to, and strip your own route
   prefix. You own the outer URL, for example `/repositories/{id}`. Everything
   from `/lix/v1` on belongs to the protocol and must not be renamed.
3. **Forward the request and the response.** Preserve protocol status codes,
   headers, and body bytes. Keep the repository alive until a server-sent
   events (SSE) body closes.

Use `ServerProtocolPrincipal::Anonymous` only where you deliberately allow
anonymous access. Reject bad credentials with `401` before dispatch. Call
`LixServerProtocol::close()` on shutdown.

Clients connect exactly as they do to lixray.com. Only the URL changes.

The SDK appends `/lix/v1/`, opens a session, and reconnects observation streams
on its own.

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
