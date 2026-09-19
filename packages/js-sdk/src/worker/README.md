# Browser repository ownership

For OPFS, one elected **dedicated worker** owns the engine and the direct storage
provider. The page holds a client-lifetime Web Lock; the worker holds the
repository-owner lock and the backend's physical file locks. All disappear with
their owning realm. Dedicated workers allow OPFS synchronous access handles;
this path does not require SharedWorker support.

- `repository-connection.ts`: one candidate per repository/document, discovery,
  generation fencing, bounded connection/shutdown, and page lifetime.
- `entry.repository.browser.ts`: election and client lifetime supervision.
- `repository-host.ts`: per-client sessions, authority admission, and shared root.
- `repository-session.ts`: logical handle IDs and bounded recovery across owners.
- `request-lifecycle.ts`: finite request deadlines and uncertain-write errors.

The repository lock is unversioned so incompatible builds cannot elect parallel
owners. BroadcastChannel carries a versioned protocol. Physical OPFS locks remain
the final fence against older, separately deployed storage workers. No lock is
stolen from a live owner. A paused owner can therefore produce a bounded error;
it cannot permit a second writer.

On replacement, reopen the root, recreate sessions from acknowledged branch and
account context, and re-register live observations. A new observation snapshot
reflects durable state; its logical event sequence continues. Callback IDs are
remapped for each generation so late credentials or HTTP responses cannot be
accepted by a replacement worker. The ordinary authority checks run again.

Only session setup, observation setup/pulls, and explicit metadata reads can be
reissued. SQL is not classified by its text and is never automatically replayed.
An interrupted potentially committing operation returns
`LIX_WRITE_OUTCOME_UNKNOWN`. Transactions and snapshot export streams belong to
one owner generation; callers must start new ones after loss. Recovery does not
replay a transaction, a commit, or a branch mutation.

Tests should exercise the public packed SDK and OPFS provider, not just a transport
mock. `packages/storage-opfs/scripts/test-repository-owner.mjs` covers physical
ownership, simultaneous tabs, abrupt owner death, context/observation recovery,
transaction rollback, and repeated reopen. Unit tests cover discarded late
callbacks, unknown write outcomes, and bounded recovery. The admission browser
suite separately checks authenticated identity and offline behavior.
