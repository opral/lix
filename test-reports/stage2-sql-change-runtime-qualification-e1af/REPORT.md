# Dormant SQL changelog runtime-qualification report

Status: `DORMANT / EXPECTED-RED`

This report freezes a future runtime contract only. No production source was
changed, and no compiler, adapter, SQL runtime, or benchmark command was run
for this package.

## Acceptance boundary

The package is anchored to accepted e1af and approved v4. The e1af source
still has the known readiness gaps: `ChangelogQuerySource` lacks the typed
ForkTree reader, `providers/change.rs` still reaches legacy tracked/changelog/
commit-graph paths, and `providers/diff.rs` still owns a raw store and builds
a closure-local facade. Therefore all runtime commands are dormant until the
future production child passes the frozen source gate and compiler frontier.

The package does not redefine v4 semantics. It operationalizes its ten
fixtures as a three-adapter read contract:

| Case | Required result |
| --- | --- |
| direct change | authenticated `lix_change` row and canonical digest |
| derived commit | authenticated `lix_commit` change, no duplicate |
| authenticated absence | empty result only for an absent requested key |
| missing catalog/record | typed corruption error before output |
| malformed change | typed corruption error before output |
| wrong kind/domain | typed corruption error before output |
| wrong embedded change ID | typed identity error before output |
| duplicate logical ID | typed corruption error before output |
| merged ordering | canonical order identical across adapters |
| LIMIT after merge/read | full validation/order precedes LIMIT; prefix digest matches |

History and diff are not optional extras: they are measured public consumers
of the same operation-owned read and must produce the same deterministic
digest after cold reopen. The SQL history route's chronology and ForkTree
state source remain separate source-review boundaries; the runtime package
must not hide that with a second read or a fallback.

## Required evidence record

Each adapter cell must preserve its exact command, candidate SHA/tree, source
and harness hashes, raw log hash, machine identity, Rust/toolchain identity,
database path, setup exclusion, seed digest, query digest table, counters,
and post-close disk/LSM inventory. The raw log must include the JSON record
and the textual fail-closed error for every negative control.

The first gate is 1K logical rows and history depth 4. This is intentionally
small: it isolates reader authority and semantic correctness before any
scaling claim. The package does not authorize 10K/50K or performance
optimization. Any future widening needs a new immutable package or explicit
successor.

## Expected dormant calibration

The only expected present-state outcome is:

```text
source binding on exact e1af: RED
compiler/runtime qualification: NOT RUN
Memory/RocksDB/SlateDB results: NONE CLAIMED
```

The exact v4 fd2 RED identity remains
`74d2a1d2512ece658aa213e235142935c161a81bd3d859b2c1ffa8ae2006c0a5`. A
future candidate that fails any source, compiler, digest, one-read, or
fail-closed requirement is rejected; the package never converts an absent
runtime result into acceptance.
