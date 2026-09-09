# Lix Server Protocol

The Lix Server Protocol is the HTTP contract for talking to a remote Lix. Its
stable HTTP API major is `v1`. Collection creation uses `/lix/v1`; repository
operations live under `/lix/v1/{lix_id}`.

It defines the methods, wire formats, session behavior, and error envelopes.
It does not define HTTP frameworks, authentication schemes, or deployment
policy.

Application developers should start with
[Collaboration](./collaboration-and-sync.md). This page documents the
server wire contract.

## Why it exists

The protocol is the interop layer between clients and hosts.

A server that implements `/lix/v1/{lix_id}` is a Lix server, and every Lix
client works against it unchanged. Point a client at a different server and
nothing in the client changes but the connection URL. The OpenAPI document plus
the normative behavior below is the complete per-Lix contract.

The `lix` crate contains a reusable Rust handler and this repository ships a
[reference server](https://github.com/opral/lix/tree/main/packages/server). A host can run or customize
that server, embed the Rust handler, or independently implement the same wire
contract in another language. See [Hosting](./hosting.md).

## Surface

| Group       | Paths                                                                                         |
| :---------- | :-------------------------------------------------------------------------------------------- |
| Lifecycle   | `/lix/v1`, `/lix/v1/{lix_id}`                                                   |
| Handshake   | `/lix/v1/{lix_id}`, `/lix/v1/{lix_id}/session`                                  |
| SQL         | `/lix/v1/{lix_id}/execute`, `/lix/v1/{lix_id}/execute-batch`                    |
| Transaction | `/lix/v1/{lix_id}/transaction/{begin,execute,commit,rollback}`                  |
| Files       | `/lix/v1/{lix_id}/file`, `/lix/v1/{lix_id}/file/upsert{,-batch}`               |
| Sync        | `/lix/v1/{lix_id}/sync/{push,pull,history,checkpoints,blob,chunk}`                          |
| Versioning  | `/lix/v1/{lix_id}/branch/{create,switch}`, `/lix/v1/{lix_id}/{undo,redo}`       |
| Observation | `/lix/v1/{lix_id}/observe`, `/lix/v1/{lix_id}/observe/multiplex`                |
| Snapshot    | `/lix/v1/{lix_id}/snapshot`                                                     |

SDK users pass the complete stable locator `https://host/lix/{lix_id}`.
`openLix()` rewrites that terminal locator to `/lix/v1/{lix_id}`, opens a
session, carries the server-issued `Lix-Session-Id` on later requests, and
reconnects observation streams. Raw HTTP clients use the versioned paths
directly.

## Repository lifecycle

- `POST /lix/v1` creates an empty repository when no body is supplied. A body
  with `Content-Type: application/vnd.lix.snapshot` creates it from a complete
  snapshot, including untracked rows. The response contains `{ id, url }`.
- `DELETE /lix/v1/{lix_id}` deletes the hosted repository and invalidates its
  sessions. Local replicas are not deleted.
- Reads, handshakes, and sync requests for missing repositories return `404`;
  they never create a repository implicitly.

Creation validates and durably installs the repository before exposing it.
An `Idempotency-Key` lets a client recover the original result after losing a
creation response. Creation does not overwrite another repository. Hosts apply
their authentication and provisioning policy before executing these operations.

These lifecycle operations are part of Lix interoperability, alongside SQL and
sync. SDK `create_lix`/`createLix` and `delete_lix`/`deleteLix` use this contract.

## Identity and sessions

The protocol does not read bearer tokens, cookies, API keys, or certificates. It
receives an already-trusted principal in process and never derives identity from
request headers.

Protocol requests except snapshot download require exactly one
`lix-server-protocol-version: 7` header. Missing, duplicate, malformed, or older
versions return `426 LIX_PROTOCOL_VERSION_MISMATCH` before opening a session or
executing SQL. Clients must upgrade together with the checkpoint metadata and
SQL API changes.

On session creation it ensures the Lix account exists, pins the session to it,
and scopes mutation idempotency to that principal. A session reused through a
different principal returns `403`. Clients cannot select `activeAccountId`
during the handshake.

SQL and file mutations accept an optional `Idempotency-Key` header. Replaying a
key after a lost response applies the mutation once. Sync pushes are instead
idempotent by immutable commit identity and compare-and-swap branch updates.

## Sync

Sync is Lix-scoped: the immutable ID in the path selects the Lix. Connected
replica mutations execute on the authority; the background sync worker brings
the resulting committed state into the local replica.

- `POST /lix/v1/{lix_id}/sync/push` atomically uploads immutable commits and applies
  compare-and-swap branch-ref updates.
- `GET /lix/v1/{lix_id}/sync/pull` returns pinned hot-state metadata when all query
  parameters are omitted: the repository cursor, default branch, and branch
  heads. With `snapshotBranchId` and `snapshotHeadCommitId`, it returns a
  bounded current-row page pinned to that immutable head; `snapshotAfter`
  continues the page scan. Each branch also carries a `hotStateRootId` over its
  live, tombstone-filtered rows so the replica can verify the assembled pages.
  With `after`, it long-polls the repository event sequence.
- `GET /lix/v1/{lix_id}/sync/checkpoints?cursor=...&limit=...` pages the global
  checkpoint inventory, including checkpoints outside current branch histories.
  Use the repository cursor returned by metadata pull, then pass each response
  `continuation` as `after`. Pages contain at most 512 immutable commit headers,
  ordered by commit ID. If the repository cursor changes, the server returns
  `409` and bootstrap restarts from fresh metadata. Inventory headers carry
  `isCheckpoint: true`; they do not include historical state or binary content.
- `GET /lix/v1/{lix_id}/sync/history` fetches exact immutable commits by repeated
  `commitId` parameters, together with bounded topology certificates. The
  bootstrap worker fetches the distinct branch-head bodies and current-row
  pages concurrently after reading metadata. History hydration does not change
  the live cursor or branch refs.
- `GET /lix/v1/{lix_id}/sync/blob?blobId=...` loads a canonical flat FastCDC manifest.
  `POST /lix/v1/{lix_id}/sync/blob` registers that manifest once every chunk is present,
  or returns the exact missing chunk IDs.
- `GET /lix/v1/{lix_id}/sync/chunk?chunkId=...` and
  `PUT /lix/v1/{lix_id}/sync/chunk?chunkId=...` transfer raw chunks. Both identities are
  64-character lowercase BLAKE3 hex digests; chunks are at most 4 MiB.

All sync routes require exactly one `lix-sync-protocol-version: 9` header.
Missing, duplicate, malformed, or incompatible versions are rejected before
reading or publishing sync data. The handshake advertises
`syncCheckpointInventory: true`. Commit bodies and headers both carry immutable
`isCheckpoint` metadata; membership is preserved independently of branch refs.

Bootstrap installs checkpoint headers alongside current branch heads and working
bases. Historical checkpoint state remains deferred until an explicit history
or snapshot read requests it; bootstrap does not scan every checkpoint state or
fetch its binary content.

The live pull protocol has one repository cursor. It has no schema or
branch filter and no separate branch-catalog request. Commit payloads are
complete; binary content remains referenced through the binary CAS rather than
being embedded in commit JSON. Upload is one retryable loop: register the
manifest, PUT only the returned missing chunks, then register the same manifest
again. There is no separate presence request.

Every commit member and snapshot row encodes its physical replication identity
as `(schemaKey, fileId, rowPk)`. This is deliberately not the public SQL/SDK
`lix_row_ref`: one logical file reference may aggregate several physical rows.
The sync-only `rowPk` is an ordered array of typed components. Each component is
an object with `type` equal to
`uuid`, `integer`, `string`, or `bytes`; `value` is respectively a canonical
UUID string, a JSON integer, a string, or a base64 string. For example:

```json
[
  { "type": "uuid", "value": "01936f4e-7b6c-7c3d-8f9a-123456789abc" },
  { "type": "integer", "value": 42 }
]
```

Plain scalar arrays are not valid `rowPk` values: they lose the distinction
between UUID and string primary-key components.

Pull pages contain at most 512 events or snapshot rows. Pushes contain at most
512 total commits plus ref updates; exact history requests contain at most 128
commit IDs. Pull and history responses are capped at 64 MiB. Delta, history,
and snapshot-row clients request a smaller page or batch after a `413`. Branch
metadata is the only unpaged bootstrap component; a repository whose branch
catalog alone exceeds the response cap cannot bootstrap until the catalog is
reduced or branch-metadata paging is added.

Merge provenance is commit-scoped. A merge commit's
`selectedSourceCommitId` is exactly its second graph parent; its non-authored
members are the complete selected delta from that source state relative to the
first parent. A non-merge checkpoint may also contain complete non-authored
members, but has no `selectedSourceCommitId`: it is a self-contained state
transition rather than a merge provenance claim. The receiver stages all
members explicitly. Physical whole-delta aliases remain a local storage
optimization and never change the wire contract. There is no sync-only
provenance shadow state or extra source body fetch.

## Contract

An explicit transaction owns an independent context on the session's branch and
account. Requests to `/execute` and `/observe` on the originating session remain
available and read committed data; `/transaction/execute` reads its staged writes.
Commit makes those writes visible to observers, while rollback publishes no
changes. A session still permits only one active explicit transaction at a time.

The machine-readable surface is
[`packages/lix/server-protocol.openapi.yaml`](https://github.com/opral/lix/blob/main/packages/lix/server-protocol.openapi.yaml).

Behavior that OpenAPI cannot express — session pinning, transaction ownership,
idempotency replay, observation ordering, and terminal storage semantics — is
specified by the behavioral requirements in this documentation. The Rust
implementation and its tests demonstrate those requirements; they do not make
the implementation itself part of the protocol.

To run a server, see [Hosting](./hosting.md).

### Typed sync rows (sync protocol version 9)

Every live sync member and snapshot row includes `snapshotPayload`, the base64-encoded canonical Schema v1 typed row, alongside its JSON `snapshot` projection. Tombstones encode both fields as null. Receivers verify canonical encoding, primary-key identity, and agreement with the JSON projection before installing the payload. Preserving type information and schema fingerprints lets custom and plugin-defined rows sync without rebuilding them against the engine's built-in catalog. A retained row may predate the currently registered schema, so import preserves its authoring fingerprint rather than validating it against the current catalog. SQL reads retain their existing resolved-schema validation. Storage compression does not affect the wire encoding. Sync protocol 8 and earlier peers must upgrade; there is no JSON-only or checkpoint-marker fallback.

## Reference-host provisioning

The reference server also exposes an **internal host operation**, separate from
interoperable `POST /lix/v1` creation:

`POST /internal/repositories/{uuid}/provision` accepts JSON
`{"mode":"create-new"}` or `{"mode":"adopt-existing"}` and returns `{ id, url }`
with the requested canonical UUID. It requires a configured internal bearer
token; a server without that token configured rejects this operation. Gateways
must authorize ownership of the control-plane repository before invoking it.
Normal reads, handshakes, and sync never invoke provisioning.

`create-new` initializes fresh physical storage and durably publishes the
requested ID using the lifecycle catalog. It refuses uncatalogued physical
storage at that ID. `adopt-existing` verifies existing repository metadata and
canonical branch heads/working baselines, applies supported format migrations,
and publishes the existing storage in the catalog without replacing its data.
Missing or incomplete storage fails adoption. Both operations may be retried;
an already-live catalog entry is returned unchanged, and deleted repositories
are never recreated.

Before rolling out the lifecycle catalog to an existing host, quiesce old
writers and explicitly adopt **all** legacy repository IDs, including public
and demo repositories. Opening SlateDB may fence another writer and adoption
may migrate the storage format. Authenticated UI provisioning alone is not a
migration for anonymously accessed repositories. Preserve failed adoption cases
for operator investigation; never fall back from adoption failure to creating
an empty repository. New control-plane rows and demo fixtures must explicitly
provision their storage before exposing links that require it.
