---
description: Share one repository across services, agent sandboxes, and browsers, with guidance on startup and synchronization.
---

# Collaboration

Users, agents, and devices share one authoritative repository through a Lix
server:

- [SDK clients](./persistence.md#remote-mode) query the server directly.
- [Sandboxes and machines](./persistence.md#filesystem-sync) synchronize local files.
- [Browser apps](./persistence.md#browser-opfs) read and edit an OPFS replica.

Use [LixRay](https://lixray.com/docs) or [host your own server](./hosting.md).
See [storage adapters](./persistence.md#how-storage-adapters-fit) for persistence options.

## Connection reference

<a id="choose-a-client-mode"></a>

`openLix()` defaults `server.mode` to `"remote"`, executing SQL on the server.
Supply storage and `server.mode: "partial_replica"` to create a **partial replica
with on-demand sync**. Remote mode rejects storage; partial-replica mode requires
it. No `"sync"` alias or full `"replica"` mode is supported.

| | `remote` | `partial_replica` |
| --- | --- | --- |
| Reads and writes execute | On the server | On a local replica |
| Client storage | None; do not pass `storage` | An explicit durable adapter |
| Network round trip | Every operation | Background synchronization; uncached data may need a fetch |
| Successful write | Accepted by the server | Committed locally; may not yet be on the server |
| Offline work | No | Covered reads and writes with resident dependencies |

### Remote mode

Use `server: { url: lixConnectionUrl }` for SDK access with
server-acknowledged writes. It creates no local repository or synchronized files.

### Partial-replica mode

Use `server: { url: lixConnectionUrl, mode: "partial_replica" }` with `FilesystemStorage`
for files on disk or `OpfsStorage` for a browser replica. Current data and new
commits sync automatically; SQL fetches missing native inputs on demand.

`await lix.execute(...)` confirms a local commit, not server receipt.
Uploads run in the background; no `sync()` call is needed.

### Connection URL and authentication

Use the host's absolute HTTPS connection URL with path `/lix/{uuid}`, not its
project page URL. HTTP is accepted only on loopback. Both modes accept headers
and async credential refresh:

```ts
server: {
  url: lixConnectionUrl,
  headers: async () => ({
    Authorization: `Bearer ${await getAccessToken()}`,
  }),
}
```

## Opening and reconnecting

A fresh partial replica loads bounded metadata before `openLix()` resolves.
SQL hydrates missing native inputs on demand.
Existing replicas can open locally and reconnect in the background, potentially
starting behind the server.

Offline, covered reads and writes with resident dependencies work; operations
requiring missing native inputs fail explicitly. Pending commits upload after reconnect.

### Replica format upgrades

Existing full replicas require explicit conversion before opening with
`server.mode: "partial_replica"`. Normal opening never falls back to eager full
bootstrap. Conversion preserves the source and pending work; unsupported pending
changes require explicit recovery. See the [migration guide](./partial-replica-migration.md)
for supported formats, conversion, retained-source recovery and cleanup.

Use the local recovery API after opening:

```ts
const sources = await lix.replicaRecoverySources();
for (const source of sources.filter((item) => item.recoveryRequired)) {
  const data = await lix.exportReplicaRecovery(source.id);
  // Save data locally, including its unresolved-content descriptions.
  const receipt = await lix.recoverReplica(source.id);
  // Review receipt.branchIds separately from the user's current branch.
}
```

Recovery restores captured tracked rows onto separate branches with stable
identities, based on the repository root. Their captured state is independent of
the branch from which recovery is requested. A retry returns its existing receipt rather than overwriting edits
made on a recovery branch. Local-only data remains in the local export/source;
it is not uploaded by restoration. The export also records original branch and
checkpoint coordinates and available commit/blob data. Unavailable content is
reported explicitly. Export and restoration do not delete the source, and a
local restoration receipt is not a server acknowledgement or proof that all
historical content was recovered.

Recovery exports are materialized in memory and bounded: 100,000 current rows,
64 MiB per blob and 128 MiB of blob content, with a separate 128 MiB budget for
unfinished upload content. Exceeding a limit either reports unavailable content
or fails the export without changing the source. Retained sources currently have
no automatic cleanup, so successive upgrades can increase local storage use.

Completed upload receipts and redundant checkpoint bookkeeping do not by
themselves indicate unsynced edits. Pending writes and local-only data remain
reachable through recovery. Permanent push rejection reports an error while
preserving pending work; it never silently discards those edits.

## Receive collaborative updates

Both remote and sync clients can observe queries:

```ts
const files = lix.observe("SELECT path FROM lix_file ORDER BY path");

const initial = await files.next();
const update = await files.next();
```

Remote clients receive updated query results from the server. Sync clients
apply incoming commits locally, then update affected observations.

Share a branch to see collaborators' accepted changes; use separate branches
for work requiring review.

## Concurrent changes

The server orders accepted updates. When a partial replica uploads changes
based on an older head, the server reconciles them through the same native row
merge pipeline used by branch merges. Changes to different rows or columns are
preserved. For overlapping values, the default is last write wins in server
acceptance order. Client timestamps and change identifiers do not decide the
winner. An explicit branch merge uses its incoming source as the default winner.

Registered schema/plugin merge hooks participate in that same pipeline and can
return a merged row. Files parsed by plugins inherit row merging and serialize
the resolved rows; opaque file content is atomic. Ordinary concurrent edits do
not require user conflict resolution. Incompatible schema or plugin changes
still require a supported migration.

Acknowledgments and background updates bring replicas to the authoritative
result while preserving newer pending local edits. A retry of an already
accepted upload retains its identity and cannot become a new winning write.
Resident reads and writes remain local; they do not wait for server confirmation.

Historical reads hydrate immutable commit data on demand and cache it in the
replica's storage. Repeating a cached read does not fetch that history again.
For explicit transactions, prefetch uncached historical inputs before beginning
the transaction; a transaction cannot change its captured snapshot to hydrate
missing history.

## Presence

Use a separate service for presence: cursors, selections, typing, online status,
and avatars. Lix synchronizes repository data.

## Closing

Call `await lix.close()` for cleanup. Remote mode closes the server session.
Sync mode stops its background worker without waiting for network delivery.
Durable pending commits resume uploading on the next open.

Sync has no public API to await server confirmation. Use remote mode when each
successful write requires server acknowledgment.

Closing does not delete either repository. Use `deleteLix()` for explicit
hosted deletion.
