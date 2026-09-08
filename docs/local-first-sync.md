# Local-first sync

Sync mode serves a durable local replica. `execute` and explicit transaction
commit resolve after local durability, and observations see that commit before
server acceptance. Remote mode retains server-acknowledged writes.

## State and reconciliation

The durable replica receipt records confirmed server heads, checkpoints, roots,
and cursor. Visible branch controls may advance beyond that receipt. Native
immutable commits and those controls form the upload outbox; there is no second
application mutation language or copy of the write logic.

The worker uploads dependency-closed bounded batches. Immutable commit identity
and expected head/checkpoint coordinates make a lost response retryable. A
server acknowledgment advances the receipt without overwriting a newer local
descendant, including compact checkpoint source dependencies.

Scoped checkpoints also record their captured local head in a private,
fixed-size record written atomically with the checkpoint. This lets upload
admission recognize local progress when checkpoint compaction removes that
head from canonical ancestry. The record is not a history parent or wire state
alias, and garbage collection removes it with its checkpoint commit.

An incompatible server ref move discards all pending state in that replica and
restores its confirmed server coordinates atomically. This includes pending
work on other branches: checkpoints also write a global catalog, and schema
changes can be dependencies of multiple branches. The conservative reset avoids
uploading an orphan checkpoint or a discarded dependency. Own acknowledgments
preserve newer local descendants. Permanent write rejections also reset pending
state; temporary transport errors and credential-refresh failures retain it.
Sync never automatically merges divergent heads. This is server-wins, not
wall-clock last-write-wins.

A reset publishes corrected refs and receipt atomically and preserves immutable
cached history. Garbage collection retains both the confirmed baseline and
pending dependency closure so offline checkpoints cannot erase recovery inputs.

## Reads, history, and exports

Current rows and the working checkpoint baseline are installed at bootstrap.
Ordinary writes and reads execute against the embedded engine. Historical reads
hydrate missing immutable boundaries and blobs lazily into durable storage.
Coherent mixed current/history batches retry the whole captured read after
hydration. Explicit transactions require historical inputs to be prefetched
before opening their fixed snapshot.

Warm reopen uses local state and reconnects in the background. Close cancels
network work; it does not await delivery. Canonical snapshot export opens a
short-lived authority session on demand and exports accepted server state,
which may not yet contain pending local commits.

## Complexity and profiling

Let N be working-set rows, H cold history depth, P pending commits, and E pending
dependency edges. Foreground operations pay their existing local engine cost,
without a server round trip or a foreground certification scan. Full checkpoint
publication aliases captured state; partial checkpoints still process the
selected dependency closure. Folder moves retain filesystem validation costs.
Checkpoint sync provenance adds one constant-size metadata write per checkpoint,
without rewriting the pending checkpoint queue. Background pending walks load
these source records by commit ID as needed.

One upload plan constructs dependency order in O((P + E) log P), replacing a
quadratic next-ready scan. Its traversal stops at confirmed boundaries rather
than walking cold history. Upload requests are bounded and at most four batches
run before incoming demands and a finite pull receive service. Rebuilding the
remaining plan per bounded batch can still make a large offline drain quadratic
in P; this background limitation is separate from foreground latency.

Ordinary ref deltas load only their affected branch controls and perform no
branch scans. Incompatible-state resets inspect O(B) branch metadata to guard
atomic client resets, where B is the local branch count. An exceptional reset
with removed local rows rebuilds the affected current-state generation in O(N).

Certified changed-root publication retains the current O(N log N) bound. Warm
cached history queries perform no repeated history transfer; cold reads pay for
the required immutable closure, not an unconditional repository-history load.

The ignored `local_first_foreground_profile_scorecard` in `sync_mode.rs` uses
matched 32/256-row fixtures with 0/100 ms injected network RTT, recording latency,
allocations for reads, writes, folder moves, and checkpoints.
The profile isolates foreground completion; background convergence is checked
separately. Timings are diagnostics, while offline correctness and request-count
assertions are regression gates.

### Measured foreground comparison

Same-host measurements against main `56bd1a061` and this implementation, using
identical filesystem-replica/in-memory-authority fixtures with 100 ms injected
HTTP RTT:

| Operation | Main, 32 rows | Local-first, 32 rows | Main, 256 rows | Local-first, 256 rows |
| --- | ---: | ---: | ---: | ---: |
| Current point read | 0.91 ms | 0.82 ms | 0.86 ms | 0.88 ms |
| Current write | 313.80 ms | 1.16 ms | 353.82 ms | 1.40 ms |
| Partial checkpoint | 333.09 ms | 2.18 ms | 391.13 ms | 2.44 ms |
| Folder move | 329.79 ms | 2.19 ms | 405.84 ms | 4.46 ms |
| Full checkpoint | 332.05 ms | 1.01 ms | 390.21 ms | 1.06 ms |

Main pays approximately three network round trips plus publication work.
Local-first completion does not wait for those requests. At 256 rows, measured
full-checkpoint allocation fell from 274.55 MB to 1.18 MB; current-write
allocation fell from 248.55 MB to 1.44 MB. Folder-move allocation still grows
with the filesystem working set (3.25 MB at 32 rows, 7.57 MB at 256 rows), so
these results do not claim constant-time folder validation.

These are single-operation diagnostic samples from Cargo's test profile, not
release-build percentile promises. The allocator scope is process-global and
can include the background worker and in-process authority. Compare latency
and allocation only with these matched fixtures; offline tests establish the
absence of a required successful foreground network request independently of
machine timing. The measured profile does not claim background drain or
server reconciliation has constant cost.

Reproduce from the repository root:

```sh
LIX_LOCAL_FIRST_PROFILE_OUTPUT=/tmp/local-first-profile.json \
  cargo +nightly-2026-05-21 test --manifest-path tooling/Cargo.toml \
  -p lix_e2e --features sdk-tests,server-protocol --test sync_mode \
  local_first_foreground_profile_scorecard -- --ignored --exact --nocapture
```

The JSON artifact uses `lix.local-first-foreground-profile.v1` and records all
32/256-row and 0/100 ms RTT cases. Run the same harness on the baseline checkout
for a comparison; do not compare unrelated older benchmark binaries.
