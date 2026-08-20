---
description: Connect clients to one Lix server using remote mode or a synchronized local replica.
---

# Collaboration and sync

Collaboration requires a Lix server. The server connects users and devices
through one shared repository.

Use [LixRay](https://lixray.com/docs) for a hosted Lix server, or
[host a Lix server yourself](./hosting.md). Once you have a repository URL,
choose how each client connects.

## Choose a client mode

|                          | Remote mode                   | Sync mode                         |
| :----------------------- | :---------------------------- | :-------------------------------- |
| Reads and writes execute | On the server                 | On a local replica                |
| Local storage            | Not required                  | Recommended                       |
| Network round trip       | Every operation               | Outside the normal operation path |
| Offline work             | No                            | Cached reads and local writes     |
| Best for                 | Thin clients and server tools | Interactive and offline apps      |

Both modes collaborate through the same server and use the same Lix API.

## Remote mode

Remote mode is the simplest way to connect:

```ts
import { openLix } from "@lix-js/sdk";

const lix = await openLix({
  server: {
    mode: "remote",
    url: repositoryUrl,
  },
});
```

The client does not open a local repository. Reads and writes execute on the
server, so a successful operation has been accepted by the server. Every
operation includes a network round trip.

Use remote mode when the application is always online, should not store
repository data locally, or must know that the server accepted each successful
write.

## Sync mode

Sync mode keeps a local working copy of the server repository. Current data and
new commits synchronize automatically. Older history and binary content
download only when needed.

```ts
import { openLix } from "@lix-js/sdk";
import { OpfsStorage } from "@lix-js/storage-opfs";

const lix = await openLix({
  storage: new OpfsStorage({
    name: repositoryId,
  }),
  server: {
    mode: "sync",
    url: repositoryUrl,
    headers: async () => ({
      Authorization: `Bearer ${await getAccessToken()}`,
    }),
  },
});
```

The repository URL identifies the server repository. The OPFS name identifies
its local working copy within the current browser origin. Use a stable OPFS
name for each repository.

Reads and writes execute locally. They do not wait for a server round trip,
which makes sync mode suitable for responsive editors and interactive apps.

`await lix.execute(...)` means that the local transaction committed. It does
not mean that the server has received the commit. Lix uploads committed changes
in the background. The application does not call `sync()`.

Use sync mode when interactions should feel immediate, the app should continue
working offline, or repository data should persist in the browser.

## Opening and reconnecting

A fresh local replica downloads the repository's current working state before
`openLix()` resolves. A previously opened replica can open from local storage
and reconnect in the background. It may initially be behind the server.

While offline:

- Cached reads continue to work.
- Writes commit to local storage.
- Pending commits upload after reconnect.
- History or binary content that has never been downloaded is unavailable.

## Receive collaborative updates

Both remote and sync clients can observe queries:

```ts
const files = lix.observe("SELECT path FROM lix_file ORDER BY path");

const initial = await files.next();
const update = await files.next();
```

When another client changes the shared repository, a remote client receives the
updated query result from the server. A sync client applies the incoming commit
locally and then updates affected observations. Application code uses the same
`observe()` API in both modes.

Use the same branch when collaborators should see each other's accepted
changes. Use separate branches when work must be reviewed before it joins the
target branch.

## Concurrent changes

Two sync clients may commit before receiving each other's changes. Lix keeps
both commits and reconciles diverged branch heads through its normal commit and
merge behavior. Sync does not add a separate conflict API.

## Presence

Lix synchronizes repository data. Cursor positions, selections, typing status,
online status, and user avatars are temporary presence data. Lix does not
provide them. Use a separate presence service.

## Closing

Call `await lix.close()` during normal cleanup.

In remote mode, this closes the server session. In sync mode, it waits for
active local work and gives background synchronization an opportunity to upload
pending commits. It does not guarantee that every local commit reached the
server. Durable commits can continue uploading the next time the repository
opens.

Sync mode currently has no public API for waiting until a commit is confirmed
by the server. Use remote mode when each successful write must be acknowledged
by the server.
