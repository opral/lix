# Explicit migration and retained-source recovery

With storage and `server.mode: "partial_replica"`, the browser uses a **partial
replica with on-demand sync**. Normal partial-replica opening
accepts its dedicated storage layout and installs bounded metadata. It does not
run the former eager full-replica upgrade as a fallback.

JavaScript callers must explicitly set `server.mode: "partial_replica"` when
opening the converted storage. Omitting the mode defaults to `"remote"`, which
rejects storage. Replace the former `"sync"` spelling; there is no compatibility
alias. Conversion and recovery helpers remain explicit storage operations.

This change introduces repository format 79. Registered full-repository
migrations cover formats 72–78. Previously unsupported formats, including the
existing format-68 hard cut, still fail explicitly; this does not introduce a
compatibility shim for them. Released format-72 and format-75 fixtures retain
their tested schema, file/checkpoint and cold-reopen semantics after migration.

The upgrade rebuilds native primary-key lookup catalogs from authoritative state,
including explicit tombstones. This repairs incomplete catalogs left by earlier
merge and columnar-storage paths while preserving current rows, history and
pending branch controls. The repair runs in the migration epoch, before format
79 is published; ordinary partial-replica opening does not scan repository rows.

For a closed existing replica, JavaScript exposes
`convertReplicaToPartial({ storage, server, branchId? })`; Rust exposes
`convert_replica_to_partial(storage, server, branch_id)`. Conversion owns storage
exclusively, authenticates repository/account identity, and retains the original
source. Its reconciliation journal preserves exact upload targets, attempt IDs,
restart receipts, and native merge outcomes across interrupted replies. A fresh
monotonic baseline lease proof is checked before publication. Migration costs
are separate from normal opening costs.

If an older partial replica format cannot supply native pending history, explicit format
migration retains its captured source for recovery. Conversion can return
`LIX_PARTIAL_REPLICA_CONVERSION_RECOVERY_REQUIRED` after that format migration.
Open the current full recovery repository locally, inspect
`replicaRecoverySources()`, and export available work with
`exportReplicaRecovery(id)`.

`recoverReplicaWithServer(id, server)` / Rust
`recover_replica_with_server(id, server)` restores supported rows on separate
recovery branches. The server must authenticate the same repository and account.
The operation uses an isolated writer context and exact missing-history/chunk
requests, with bounded retry and transfer pages. It does not attach a background
worker, pull current state, or upload recovered branches. Existing provider and
session ownership remains in force; closing the JS handle waits for its active
operation. `recoverReplica(id)` remains the local-only variant.

Recovery preserves file identities and starts branches at the repository root,
so unrelated caller rows are not inherited. Original bytes remain retained after
success. Receipts are durable and idempotent; they describe local restoration,
not authority acknowledgment. Always inspect unresolved entries and export
limits before treating a recovered source as complete.

Conversion reconciles pending work on all existing ordinary branches, including
interrupted native merge replies. It also supports new/restored branches whose
pending global changes consist only of branch-descriptor additions. The native
global merge and all new references publish atomically; original local heads,
fork/checkpoint references and commit identities are preserved. The requested
selected branch may be one that does not yet exist on the authority.

A later descriptor-only global advance is handled through a terminally fenced
successor attempt, or by recovering an already-committed exact merge receipt.
The successor preserves the original source coordinates and accepted uploads,
proves authenticated ancestry and revalidates against the new global state.
Lost replies before and after those advances are covered by a real HTTP test.
No source head is rewritten to make it appear acknowledged.

Other pending global changes (such as account/schema/plugin/default-branch
changes), resets and checkpoint changes remain unsupported by automatic
reconciliation. Conversion fails explicitly and retains the original source in
those cases. A local recovery receipt alone does not clear the retained-source
recovery requirement.

After successful conversion, cleanup of temporary authority pins is best effort.
For a closed partial storage provider, call
`retryReplicaMigrationCleanup({ storage, server }): Promise<number>`; Rust exposes
`retry_replica_migration_cleanup(storage, server)`. The returned count identifies
cleanup journals finalized by that call; repeated successful calls return zero.
Exact durable merge receipts, surviving branch ancestry and control guards must
prove the pins are redundant before removal. A lost response preserves retry
coordinates and does not roll back the usable partial replica. This explicit
maintenance operation may inspect bounded journal pages; ordinary opening never
scans migration journals.
