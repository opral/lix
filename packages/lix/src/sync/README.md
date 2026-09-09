# Repository sync

Lix synchronizes its native repository facts rather than maintaining a second
row protocol:

- complete immutable commits, with merge provenance in the second parent and
  self-contained checkpoint members;
- compare-and-swap branch ref updates on one ordered repository cursor;
- BLAKE3/FastCDC blob manifests and chunks.

`repository.rs` exports, imports, and persists those facts. `runtime.rs` is the
one bootstrap, outbox, long-poll, reconciliation, and retry state machine.
`contract.rs` is its transport interface. `platform.rs` and `platform/` contain
the only native/browser divergence: tasks, timers, HTTP, and cancellation.

An initial pull pins the cursor, default branch, and branch heads. The runtime
then fetches distinct head commit bodies with bounded topology certificates and
immutable head-pinned current-row pages concurrently. Live events transfer
complete commits and ref moves. Older commit bodies and binary chunks load
separately on demand and never advance the live cursor.

Checkpoint inventory pages carry canonical headers without hydrating historical
state. Bootstrap records which headers came only from that inventory; only
those deferred checkpoints may omit jump boundaries. Serving-history headers
and normal history imports still require their jump closure. A missing deferred
graph node triggers ordinary bounded history demand before traversal continues.
Known jump generations, self-jumps, invalid spans, and inventory/body identity
mismatches are validated before publication.

Sync-mode format upgrades retain the previous source and bootstrap a new physical
generation from the authenticated authority. The source is never reused as a
later candidate. A durable manifest records source identity and recovery status
before activation; failed bootstrap leaves the source active and retryable.
Ordinary writes are fenced by the exact active pointer, and retained-source
readers cannot write. Standalone and authority repositories use format migrations.

Admission checks identity and classifies local work, without requiring a clean
replica. Acknowledged branch/head/checkpoint coordinates and redundant engine
bookkeeping do not block opening. Pending edits, incomplete uploads and custom
local-only rows remain preserved. Unknown identity metadata fails without altering
the source; an arbitrary server URL is not sufficient proof of identity.

`replica_recovery_sources`, `export_replica_recovery`, and `recover_replica` expose
retained work after opening. Export reads logical serving rows, original branch
coordinates, available commit/blob data and unresolved dependencies. Restoration
creates independent branches preserving row/file IDs, with a transactionally
recorded, tracked provenance receipt that survives synchronization and later
rebuilds. Local-only rows stay local. A restoration
receipt neither authorizes source deletion nor proves server acceptance.

Generation retention and pointer publication are metadata-only, with no source
copy or history traversal. Classification scans branch controls and current rows;
bootstrap costs current server state plus ordinary checkpoint inventory. Recovery
runs separately and can require reading substantial retained data. Source listing
scales with the number of retained generations. Storage-space exhaustion fails
safely instead of recycling retained work.

In particular, retention/activation metadata work is O(1), and listing sources is
O(G) for G generations. Classification visits branch controls, current serving
rows and upload metadata; it does not traverse historical commits. Explicit
recovery also visits available pending history and payload bytes. Blob/file
lookups use maps rather than repeatedly scanning all files for every row. Path
reconstruction additionally pays for the directory components in the exported
paths. Normal transaction validation and index maintenance still apply when
publishing recovered rows.
