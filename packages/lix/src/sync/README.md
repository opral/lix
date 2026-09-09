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

On a v77 replica format upgrade, epoch admission proves that every local branch
coordinate matches a durable authority receipt, including the global branch.
In v77 every checkpoint also authored a tracked global marker, so this checks
off-branch checkpoint work without traversing history. Pending resets, physical
untracked rows and tombstones, unfinished uploads, and recovery references block
the upgrade. Unknown replica formats fail closed.

A clean replica uses the existing bootstrap in a hidden epoch, checks repository
and account identity, closes its temporary sessions, validates, and only then
publishes the epoch. The old epoch remains intact. Standalone and authoritative
repositories retain their normal format migration path.

The safety check scans branch controls and current serving rows with a metadata
projection. Its cost is O(B log B + R), where B is the number of branch controls
and R is the total current rows visited across branches, plus receipt decoding.
Peak scan memory is proportional to the largest branch's current row batch. It
does not decode or walk historical commits. Rebuilding then pays the ordinary
current-state bootstrap cost, including the checkpoint header inventory; it does
not materialize historical checkpoint snapshots.
