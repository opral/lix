# Lix server protocol

`lix_server_protocol::LixProtocolServer` exposes the canonical HTTP protocol
for one workspace. A host owns authentication, workspace routing, storage
construction, and process lifecycle. The protocol server owns the root
`lix_sdk::Lix` handle and a bounded registry of independent remote sessions.

A JavaScript client connects with `openLix({ server })`:

```ts
import { openLix } from "@lix-js/sdk";

const lix = await openLix({
  server: {
    mode: "remote",
    url: "https://example.com/workspaces/acme",
  },
});
```

The host chooses the storage. For shared S3 deployments, use the shipped
`lix_slatedb_storage` implementation backed by an S3-compatible object store.
The JavaScript client does not connect to S3 directly.

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

This small server example uses the default in-memory storage. Production hosts
should construct the `Lix` instance with durable storage before passing it to
`LixProtocolServer`.

The host must retain one `LixProtocolServer` for the workspace lifetime. It
must not reconstruct the server for each HTTP request, because the in-memory
session registry is part of the protocol's correctness boundary. Requests for
one workspace must also reach that same in-process instance; a restart or a
route to another instance intentionally makes the old session return `410`.

## Session lifecycle

An initial `GET /lix/v1` without `Lix-Session-Id` opens an independent session
pinned to the root workspace's current branch. Supplying
`?activeBranchId=<branch-id>` instead pins the new session to that existing
branch. Supplying `activeAccountId` attributes every change from the new
session to that existing active global account. The query parameter is a
trusted-deployment convenience, not authentication. Internet-facing hosts
must derive the account from authentication and attach
`TrustedActiveAccountId` as an in-process request extension on every request;
it overrides the creation query and rejects later use of the session by a
different account. Omitting both selects the built-in anonymous account. The
response contains `protocolVersion`, `activeBranchId`, `activeAccountId`,
`activeAccountId`, and a cryptographically random `sessionId`. The client sends that value as
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
the session limits, request ceiling, and workspace request-base cache budget.
Expired sessions are
removed opportunistically. At capacity, the least-recently-used idle session
is evicted; if every session is leased by an active HTTP request or SSE stream,
the new handshake returns `503` instead of closing active work.
An open remote transaction owns an RAII lifecycle pin in the same idleness
predicate. Committing, rolling back, cancellation, and session teardown all
release that pin by dropping the transaction state; no terminal path manually
repairs a second activity flag.

Request blob splices use a bounded, per-session FIFO cache: at most eight
entries and 16 MiB aggregate, with only blobs from 32 KiB through 16 MiB
eligible. This retains one 10,680,000-byte CSV or 10,000,000-byte JSON base;
caching its similarly sized successor evicts the predecessor, so repeated
localized edits rotate one large base instead of accumulating document copies.
All server-side session caches share a 128 MiB workspace budget. If admitting a
base would exceed that budget, the cache declines it and the next edit falls
back to the protocol's complete-blob retry without affecting correctness.
Across one execute or atomic batch, reconstructed blob bytes are separately
bounded by the configured expanded JSON request ceiling (64 MiB by default).
The client uses the same 16 MiB aggregate base budget. An admitted complete
base is SHA-256-verified once; a splice then reconstructs and hashes its
unavoidable contiguous result once and shares that immutable payload among the
SQL parameter, validated provenance, and successor cache. Client-provided
digest and splice fields are never trusted without that proof.

The protocol server owns `/lix/v1`, request validation, wire values, Lix error
mapping, and multiplexed observations. Host-specific routes such as
authentication, health checks, and compare-and-swap filesystem mutations stay
outside it. Session identifiers are opaque capabilities: hosts should not log
or persist them.

Multiplex observation sends an initial full snapshot for each subscription.
Contiguous later snapshots may instead carry a sequence-based blob or row
splice against the immediately preceding transport snapshot. A client that
does not have that exact base must reconnect and begin again with a full
snapshot; it must never apply a splice to an arbitrary cached result.
Subscriptions with byte-identical SQL and wire parameters share one engine
observation, one transport base, and one encoded delta payload inside the
multiplex request. Subscription identifiers remain independent, but adding an
identical subscriber no longer repeats query evaluation, blob comparison, or
Base64 conversion. Distinct SQL or parameter payloads remain separate groups.

The ignored release diagnostic compares the former repeated conversion with
the shared fanout path:

```sh
cargo test --release -p lix_server_protocol \
  multiplex_blob_delta_fanout_perf -- --ignored --nocapture
```

For a localized edit in a 10 MiB blob, 16-subscriber p50 conversion fell from
6,979 microseconds to 428 microseconds (16.28x). Four-subscriber p50 fell from
1,658 microseconds to 402 microseconds (4.12x). One subscriber remains neutral.

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
file. The endpoint creates a missing file or replaces an existing file's content,
uses the normal transactional filesystem write path, and returns the standard
`ExecuteResponse` envelope with `rowsAffected: 1`. It has the same configured
request-body ceiling as JSON protocol requests.

Media clients use this same endpoint for a sequential resumable write by
supplying a client-generated `Lix-Upload-Id` and `Content-Range: bytes
<start>-<end>/<total>`. Non-final bodies are exactly 16 MiB and aligned to a
16 MiB offset; the final body may be shorter. An incomplete write returns 308
and `Range: bytes=0-<last-acknowledged-byte>`. The last part publishes one
ordinary file version and returns the normal response with `rowsAffected: 1`.
Upload receipts and CAS chunks are durable, so a new session can continue at
the acknowledged offset. Parts are sequential only: parallel, out-of-order,
unknown-length, and server-side composition flows are outside this contract.

This is intentionally a structured file-transfer operation, not a transparent
replacement for arbitrary SQL `UPDATE`: callers choose its upsert behavior
explicitly. The path must be a percent-encoded absolute Lix file path (for
example, `%2Fassets%2Freport.pdf`).

## Binary file upsert batch

Clients that need one atomic commit for several ordinary file upserts can
check `capabilities.binaryFileUpsertBatch === true` and send:

```text
POST /lix/v1/file/upsert-batch
Lix-Session-Id: <session-id>
Content-Type: application/octet-stream
```

The body is a deterministic big-endian frame:

```text
u32 entry_count
repeat entry_count times:
  u32 UTF-8 path byte length
  u32 raw content byte length
  path bytes
  content bytes
```

`entry_count` must be between 1 and 1,024; paths must be unique, valid UTF-8,
and consume the complete frame. Empty file content is valid. The server keeps content
as slices of the request body rather than making a second payload copy. A valid
request stages all entries in one transaction and returns the standard
`ExecuteResponse` with `rowsAffected` equal to the entry count; any validation
or engine error commits none of the entries. Larger client batches should be
split into frames of at most 1,024 files. The configured request-body ceiling
is the same as for the rest of the protocol.

## Binary file read

Clients that explicitly want one file's rendered bytes can check for
`capabilities.binaryFileRead === true` and send a protected request to:

```text
GET /lix/v1/file?path=<percent-encoded-absolute-file-path>
Lix-Session-Id: <session-id>
Range: bytes=<start>-<inclusive-end> # optional
```

The successful response has `Content-Type: application/octet-stream`,
`Cache-Control: no-store`, and raw file bytes as its body. It carries
`Lix-File-Found: true` for a present file (including a present empty file) or
`Lix-File-Found: false` for a missing file, whose body is empty. This route has
the same active-branch, plugin rendering, and per-session acknowledgement
semantics as `SELECT content FROM lix_file WHERE path = $1`; it is not a generic
SQL read endpoint.

The route accepts one forward byte range and returns `206 Partial Content`,
`Accept-Ranges: bytes`, `Content-Range`, and the selected bytes. Open-ended
ranges are accepted. Multipart and suffix ranges are deliberately rejected:
media clients use forward reads, and keeping one response body preserves the
same file-read abstraction. Ordinary CAS-backed files load only the manifest
and intersecting chunks. Flat document deltas retain their established full
reconstruction path because they optimize localized document edits rather
than immutable media playback.

## Remote protocol qualification

The ignored release benchmark compares a direct pinned Lix session backed by
SlateDB with the same operation through a loopback
`LixProtocolServer`. Both arms use immutable clones of one physical fixture,
the same accounted object store and injected latency, independent equivalent
cache directories, and fully materialized results:

```sh
cargo test -p lix_server_protocol --release \
  slatedb_direct_versus_remote_reads -- --ignored --nocapture
```

The matrix covers exact-path reads, ordered listings of 100 paths, and raw
4 KiB, 100 KiB, and 1 MiB file downloads with warm memory, warm disk, and cold
caches at 0, 10, and 25 milliseconds of object-store latency. It also seeds
10,000 ordered `lix_key_value` rows and measures a state query, its initial
observation snapshot, and the write-to-next-event path for one changed row.
The update measurement opens and consumes the initial snapshot before starting
the timer, so it reports the steady-state observation cost rather than hiding
it behind setup. Its output includes `remote_sse_event_bytes_p50`, which makes
full-snapshot versus delta transport cost directly visible.

File and listing operations fail when the remote p50 or p95 exceeds twice its
direct counterpart, or when the remote arm averages more than one additional
object-store read per measured operation. The 10k state operations are
diagnostic baselines rather than parity gates; `observe-state-10k-update` runs
only against a warm live runtime because it intentionally keeps one stream
open. Remote requests advertise zstd and materialize and decode the full
response inside the timer, matching browser content negotiation. Output also
reports per-arm request totals and maxima and separately sampled in-process
handler time for raw downloads and the 10k state query.

Use `LIX_REMOTE_READ_SAMPLES` and
`LIX_REMOTE_READ_WARM_MEMORY_SAMPLES` to change distribution sizes. Comma-
separated `LIX_REMOTE_READ_LATENCIES_MS`,
`LIX_REMOTE_READ_CACHE_STATES`, and `LIX_REMOTE_READ_OPERATIONS` select a
diagnostic subset without changing the fixtures or measurement path.
