# Lix Server Protocol

The Lix Server Protocol is the HTTP contract for talking to a remote Lix. Its
stable HTTP API major is `v1` and every operation lives under
`/lix/v1/{lix_id}`.

It defines the methods, wire formats, session behavior, and error envelopes.
It does not define HTTP frameworks, authentication schemes, or deployment
policy.

Application developers should start with
[Collaboration and Sync](./collaboration-and-sync.md). This page documents the
server wire contract.

## Why it exists

The protocol is the interop layer between clients and hosts.

A server that implements `/lix/v1/{lix_id}` is a Lix server, and every Lix
client works against it unchanged. Point a client at a different server and
nothing in the client changes but the connection URL. The OpenAPI document plus
the normative behavior below is the complete per-Lix contract.

The `lix` crate contains the reference implementation, so a server provides HTTP
and authentication and forwards requests to it. See [Hosting](./hosting.md).

## Surface

| Group       | Paths                                                                                         |
| :---------- | :-------------------------------------------------------------------------------------------- |
| Handshake   | `/lix/v1/{lix_id}`, `/lix/v1/{lix_id}/session`                                  |
| SQL         | `/lix/v1/{lix_id}/execute`, `/lix/v1/{lix_id}/execute-batch`                    |
| Transaction | `/lix/v1/{lix_id}/transaction/{begin,execute,commit,rollback}`                  |
| Files       | `/lix/v1/{lix_id}/file`, `/lix/v1/{lix_id}/file/upsert{,-batch}`               |
| Sync        | `/lix/v1/{lix_id}/sync/{push,pull,history,blob,chunk}`                          |
| Versioning  | `/lix/v1/{lix_id}/branch/{create,switch}`, `/lix/v1/{lix_id}/{undo,redo}`       |
| Observation | `/lix/v1/{lix_id}/observe`, `/lix/v1/{lix_id}/observe/multiplex`                |
| Snapshot    | `/lix/v1/{lix_id}/snapshot`                                                     |

SDK users pass the complete stable locator `https://host/lix/{lix_id}`.
`openLix()` rewrites that terminal locator to `/lix/v1/{lix_id}`, opens a
session, carries the server-issued `Lix-Session-Id` on later requests, and
reconnects observation streams. Raw HTTP clients use the versioned paths
directly.

## Identity and sessions

The protocol does not read bearer tokens, cookies, API keys, or certificates. It
receives an already-trusted principal in process and never derives identity from
request headers.

On session creation it ensures the Lix account exists, pins the session to it,
and scopes mutation idempotency to that principal. A session reused through a
different principal returns `403`. Clients cannot select `activeAccountId`
during the handshake.

SQL and file mutations accept an optional `Idempotency-Key` header. Replaying a
key after a lost response applies the mutation once. Sync pushes are instead
idempotent by immutable commit identity and compare-and-swap branch updates.

## Sync

Sync is Lix-scoped: the immutable ID in the path selects the Lix. A local write
commits to the local Lix first and reaches these endpoints only from the
background sync worker.

- `POST /lix/v1/{lix_id}/sync/push` atomically uploads immutable commits and applies
  compare-and-swap branch-ref updates.
- `GET /lix/v1/{lix_id}/sync/pull` returns pinned hot-state metadata when all query
  parameters are omitted: the repository cursor, default branch, and branch
  heads. With `snapshotBranchId` and `snapshotHeadCommitId`, it returns a
  bounded current-row page pinned to that immutable head; `snapshotAfter`
  continues the page scan. Each branch also carries a `hotStateRootId` over its
  live, tombstone-filtered rows so the replica can verify the assembled pages.
  With `after`, it long-polls the repository event sequence.
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

The machine-readable surface is
[`packages/lix/server-protocol.openapi.yaml`](../packages/lix/server-protocol.openapi.yaml).

Behavior that OpenAPI cannot express — session pinning, transaction ownership,
idempotency replay, observation ordering, and terminal storage semantics — is
normative in the reference implementation and its tests.

To run a server, see [Hosting](./hosting.md).
