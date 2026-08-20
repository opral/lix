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
