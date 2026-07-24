# Lix server protocol

`lix_server_protocol::LixProtocolServer` exposes the canonical HTTP protocol
for one workspace. A host owns authentication, workspace routing, storage
construction, and process lifecycle. The protocol server owns the root
`lix_sdk::Lix` handle and a bounded registry of independent remote sessions.

```rust,no_run
use std::sync::Arc;
use axum::Router;
use lix_sdk::{OpenLixOptions, open_lix};
use lix_server_protocol::LixProtocolServer;

# async fn example() -> Result<(), lix_sdk::LixError> {
let root = Arc::new(open_lix(OpenLixOptions::default()).await?);
let protocol = LixProtocolServer::new(root);
let app = Router::new().merge(protocol.router());

// During workspace shutdown:
protocol.close().await?;
# let _ = app;
# Ok(())
# }
```

The host must retain one `LixProtocolServer` for the workspace lifetime. It
must not reconstruct the server for each HTTP request, because the in-memory
session registry is part of the protocol's correctness boundary. Requests for
one workspace must also reach that same in-process instance; a restart or a
route to another instance intentionally makes the old session return `410`.

## Session lifecycle

An initial `GET /lix/v1` without `Lix-Session-Id` opens an independent session
pinned to the root workspace's current branch. Supplying
`?activeBranchId=<branch-id>` instead pins the new session to that existing
branch. Its response contains `protocolVersion`, `activeBranchId`, and a
cryptographically random `sessionId`. The client sends that value as
`Lix-Session-Id` on every later request, including a resumed handshake and
observation streams. Switching one pinned session never changes another
session or the root workspace selector.

Missing or malformed identifiers return `400`. Unknown, expired, evicted, or
closed identifiers return `410 Gone`; the client must open a new logical
session and reload stale application state before mutating rather than
silently continuing with a different acknowledged view. Handshake responses
send `Cache-Control: no-store` so a browser or intermediary cannot reuse one
client's session capability for another client.
`DELETE /lix/v1/session` closes the identified session. Repeating that delete
with the same well-formed identifier returns `204 No Content`, so client close
is idempotent.

Sessions use a 30-minute idle timeout and a 64-session workspace cap by
default. JSON requests have an explicit 64 MiB ceiling so base64-encoded blobs
can carry the engine's 32 MiB maximum plugin archive; multiplex observation
streams accept at most 32 subscriptions. `ProtocolServerOptions` can override
the session limits and request ceiling. Expired sessions are
removed opportunistically. At capacity, the least-recently-used idle session
is evicted; if every session is leased by an active HTTP request or SSE stream,
the new handshake returns `503` instead of closing active work.

The protocol server owns `/lix/v1`, request validation, wire values, Lix error
mapping, and multiplexed observations. Host-specific routes such as
authentication, health checks, and compare-and-swap filesystem mutations stay
outside it. Session identifiers are opaque capabilities: hosts should not log
or persist them.

## Binary file upsert

Clients that explicitly want file **upsert** semantics can check for
`capabilities.binaryFileUpsert === true` in the handshake response and send a
protected request to:

```text
POST /lix/v1/file/upsert?path=<percent-encoded-absolute-file-path>
Lix-Session-Id: <session-id>
Content-Type: application/octet-stream
```

The body is the raw file bytes, including an empty body for a present empty
file. The endpoint creates a missing file or replaces an existing file's data,
uses the normal transactional filesystem write path, and returns the standard
`ExecuteResponse` envelope with `rowsAffected: 1`. It has the same configured
request-body ceiling as JSON protocol requests.

This is intentionally a structured file-transfer operation, not a transparent
replacement for arbitrary SQL `UPDATE`: callers choose its upsert behavior
explicitly. The path must be a percent-encoded absolute Lix file path (for
example, `%2Fassets%2Freport.pdf`).
