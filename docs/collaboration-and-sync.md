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

`openLix()` selects execution from the supplied connections: `server` alone
executes remotely; `storage` plus `server` maintains a synchronized local read replica.
Both connect to the same hosted repository.

| | `remote` | `sync` |
| --- | --- | --- |
| Reads and writes execute | On the server | Certified current-state reads locally; mutations and history on the server |
| Client storage | None; do not pass `storage` | An explicit durable adapter |
| Network round trip | Every operation | Background synchronization; uncached data may need a fetch |
| Successful write | Accepted by the server | Accepted by the server |
| Offline work | No | No offline mutation guarantee; reads may require server certification |

### Remote mode

Use `server: { url: lixConnectionUrl }` for SDK access with
server-acknowledged writes. It creates no local repository or synchronized files.

### Sync mode

Use `server: { url: lixConnectionUrl }` with `FilesystemStorage`
for files on disk or `OpfsStorage` for a browser replica. Current data and new
commits sync automatically; older history and binary content download on demand.

Mutations execute on the authority. Successful mutation calls confirm server
acceptance; the local replica receives the resulting certified state. No
manual `sync()` call is needed.

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

A fresh replica downloads current working state before `openLix()` resolves.
Existing replicas retain local cached state, but connected opening and
operations may require the server. Mutations, history, and uncached content
require connectivity; cached reads can also require fresh authority certification.

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
A synchronized handle releases its local storage session and server session.
Closing does not delete either repository. Use `deleteLix()` for explicit
hosted deletion.
