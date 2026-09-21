# Replacing a browser replica

The authority decides which replica identities may publish. A browser may replace
an inaccessible local replica after a bounded opening attempt. It must retire the
old identity on the authority before publishing the new local storage pointer.
Local storage names alone do not revoke a worker's ability to upload.

Clients using replacement send a stable `lix-replica-id` header on every request.
Identities contain 1–512 visible ASCII bytes, without spaces or control characters.
The identity is scoped by the authority's authenticated account and repository;
it is not an authentication credential. Reconnecting must reuse the identity of
the local replica rather than minting a new identity for existing pending work.

`POST /lix/v1/{repository}/sync/replica/replace` accepts JSON
`{ "replicaId": "old", "replacementId": "new" }` on an ordinary authenticated
protocol session. It returns `{ "replicaId": "new" }` only after durable retirement.
Repeated requests for the same old identity return its original replacement,
including when an earlier response was lost. A retired identity is never revived.
The replacement must use a new physical local store bootstrapped from the authority.

A storage-owned authority lease excludes independent replica-serving engines over
one physical store. Clones share that lease, and detached operations retain it.
Backends without authority-owner exclusion refuse replica-tagged requests and
replacement. SlateDB combines its shared owner gate with its existing durable
writer fencing across DB instances; Memory and RocksDB reuse physical engine
ownership. This is separate from ordinary untagged protocol access.

Retirement takes an exclusive publication barrier; ordinary tagged publications
share it, preserving backend write concurrency and commit grouping.
The gate belongs to detached durable work, not to the HTTP request's lifetime.
An upload already publishing may finish before retirement; later uploads fail
with `LIX_REPLICA_RETIRED`, including through a new server session. Retirement
records survive server restart and remain private authority control state.
Retired replicas stop synchronization while retaining their pending work.

Server protocol 12 rejects older bundles on every protocol request, including
requests using sessions created before deployment. Older bundles do not send
replica identities and cannot safely participate in replacement. Sync wire and
storage formats remain unchanged.

LixRay records the active replacement and retained storage names under a browser
lock. A concurrent opener reuses the published replacement. A lost response leaves
the old pointer intact; retry asks the authority for the same replacement. Opening
makes at most one automatic replacement attempt. Failure to contact the authority
does not switch storage or discard data. Existing storage is retained for explicit
recovery of unsynced work; it is never automatically uploaded by the replacement.

The authority capability marker advances from v5 to v6 through the registered
opening migration. Its preservation witness permits only that marker change;
all other logical records, including retirement records, remain covered. Older
server writers fail their exact capability-marker precondition, and older
servers reject the upgraded marker on reopen.

For the incremental admission cost, run the ignored `replica_fencing_profile`
server-protocol test with output enabled. It compares tagged and untagged
operations over the same in-memory authority; production storage latency is
additional. Tagged durable operations add one point read and a publication gate.

A local sample on 2026-09-21 measured 1.32 microseconds per untagged operation
and 3.66 microseconds per fenced operation (1,000 operations each, test profile,
in-memory storage). This measures incremental fencing overhead, not network or
production persistence latency. Open transactions do not delay retirement;
retirement rejects their commit while allowing rollback to release resources.
