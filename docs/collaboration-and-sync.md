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

`openLix()` selects where SDK operations execute with `server.mode`.
Both client modes connect to the same server.

| | `remote` | `sync` |
| --- | --- | --- |
| Reads and writes execute | On the server | On a local replica |
| Client storage | None; do not pass `storage` | An explicit durable adapter in JavaScript |
| Network round trip | Every operation | Background synchronization; uncached data may need a fetch |
| Successful write | Accepted by the server | Committed locally; may not yet be on the server |
| Offline work | No | Cached reads and local writes |

### Remote mode

Use `server: { mode: "remote", url: lixConnectionUrl }` for SDK access with
server-acknowledged writes. It creates no local repository or synchronized files.

### Sync mode

Use `server: { mode: "sync", url: lixConnectionUrl }` with `FilesystemStorage`
for files on disk or `OpfsStorage` for a browser replica. Current data and new
commits sync automatically; older history and binary content download on demand.

`await lix.execute(...)` confirms a local commit, not server receipt.
Uploads run in the background; no `sync()` call is needed.

### Connection URL and authentication

Use the host's absolute HTTPS connection URL with path `/lix/{uuid}`, not its
project page URL. HTTP is accepted only on loopback. Both modes accept headers
and async credential refresh:

```ts
server: {
  mode: "sync",
  url: lixConnectionUrl,
  headers: async () => ({
    Authorization: `Bearer ${await getAccessToken()}`,
  }),
}
```

## Opening and reconnecting

A fresh replica downloads current working state before `openLix()` resolves.
Existing replicas can open locally and reconnect in the background, potentially
starting behind the server.

Offline, cached reads and local writes work; undownloaded history and binary
content are unavailable. Pending commits upload after reconnect.

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

Concurrent commits are retained and reconciled through normal branch merging.
Sync has no separate conflict API.

## Presence

Use a separate service for presence: cursors, selections, typing, online status,
and avatars. Lix synchronizes repository data.

## Closing

Call `await lix.close()` for cleanup. Remote mode closes the server session.
Sync mode waits for active local work and allows pending uploads, but does not
guarantee server receipt. Durable commits resume uploading on the next open.

Sync has no public API to await server confirmation. Use remote mode when each
successful write requires server acknowledgment.
