# History-independence / canonicalization decision contract

Status: **UNRUN — decision pending an immutable production candidate and
future adapter execution**.

The standalone dependency-free model gate for this correction is **PASS,
6/6**. It is not production or adapter runtime evidence. The model executable
SHA-256 was `f6db5617abd2109d3601e10229b996ac09e6083de504644fa54c39ffe1229310`.

## Question

For each pair, can two construction histories with identical final logical
rows/files/blob identities be served with the same semantics while reducing
history-induced physical work? The oracle compares final logical state first;
authenticated root IDs are evidence, not an equality requirement. Different
physical roots are valid when they encode different retained chronology,
selectors, checkpoints, or object ownership.

The candidate must not derive an identity from caller order, bypass object or
domain authentication, reuse a stale selector, hide a missing object behind a
fallback, or publish a partial state. Any such result is a correctness reject
even if physical bytes improve.

## Pair families

The exact deterministic fixtures are specified in `WORKLOADS.md` and modeled
by executable constructors and tests in `history_independence_model.rs`:

1. `insert-order`: the same 32 keyed rows inserted in ascending versus a
   fixed reverse/permutation order;
2. `batching`: the same rows in one transaction versus fixed 1-, 4-, and
   16-row batches, with identical final blob IDs;
3. `branch-checkpoint`: a linear construction versus branch creation,
   checkpoint, branch-local edits, and merge back to the same final rows;
4. `intermediate-edits`: put/delete/reinsert and overwrite sequences that
   converge to the same final rows and file/blob identities;
5. `shared-blobs`: equal content referenced by multiple final rows/files plus
   unique content, followed by removal of an obsolete history/checkpoint.

Each fixture has two repository histories (`A`, `B`) and a separately
constructed control. The test must assert exact equality of canonical final
row/file/blob identity digests and compare history/selector/root IDs as
diagnostics. It must also reopen before measurement is accepted.

The model's blob/content ObjectId is domain-separated from complete content
bytes only; it never includes the row key. The pair test explicitly asserts
equality of rows, BlobIds, content-object ID sets, logical digests, and
reopened digests for every family. This makes shared content across different
rows and histories measurable rather than accidentally counting one object
per row.

## Required measurements

Every result row is keyed by `(pair, history, adapter, phase, seed)`. Record
the same deterministic input digest and result digest in every row. Required
fields are listed in `WORKLOADS.md`:

* authenticated repository/global/branch/state/catalog root IDs and equality
  flags;
* logical rows/bytes, unique object count/bytes, shared object count/bytes,
  and obsolete objects reclaimed by final-reference GC;
* diff reads/keys/bytes and history reads/keys/bytes;
* synchronization bytes, publication calls/wall/CPU, allocations and peak
  RSS;
* backend reads/keys/bytes/writes and immediate/settled disk bytes;
* cold-reopen result digest, corruption outcome, and failure digest.

The pure model also emits deterministic estimates for publication bytes,
synchronization bytes, diff/history reads, allocations, settled disk,
history-only bytes, and final-reference-GC reclaimable bytes. These are model
fields only; adapter counters replace them before any decision.

Separate import/replay, status/diff, history, branch/checkpoint, merge,
reopen, and final-reference-GC phases. Do not combine fixture construction
cost with steady-state reads. Record physical roots rather than silently
normalizing them.

## Acceptance and rejection

The following is a decision gate, not a promise that b59 currently passes:

1. **Semantic gate:** A/B final logical digests, row/file identities, blob
   identities, ordering, NULL/tombstone behavior, branch visibility, history,
   diff, checkpoint, merge, reopen, and GC reachability must agree. Missing,
   malformed, wrong-domain, wrong-ID, truncated, or transplanted objects must
   fail closed before publication, with no partial writes and no changed
   authoritative state.
2. **Canonicalization gate:** compare the candidate with the paired
   history-sensitive control. A root-ID difference is diagnostic; it is not a
   failure. At least one predeclared primary metric must improve by **more
   than 10%** on the intended workload at a meaningful size, and no primary
   semantic/storage metric may regress by **more than 5%** in any required
   adapter/cell. A single cherry-picked metric cannot override a critical
   regression in reads, synchronization, publication, allocations, RSS,
   backend work, or settled disk.
3. **Scale gate:** run 10K first. Run 50K only if the focused 10K semantic
   and regression gates pass. Any 500K extension is last and remains optional;
   it cannot repair a failed 10K gate.

The primary metrics are physical object bytes, synchronization bytes,
history/diff read bytes, publication wall/CPU, allocations, peak RSS, and
settled disk. The intended target must be declared before execution.

### Perfect-elimination ceiling

For metric `m`, compute the control's removable component `E_m` from direct
counter evidence (duplicate object bytes, redundant manifest/index bytes,
history-only reads, synchronization bytes, or obsolete retained bytes), and
report:

```text
ceiling_m = E_m / control_m
```

Use zero when there is no removable component; do not infer it from a timing
delta. The overall ceiling is the maximum **honestly removable** primary
component for the declared target, with its numerator and denominator
reported. If the overall ceiling is `<= 10%`, reject/no-cut without broadening
the matrix. If it is `> 10%`, the observed candidate still must satisfy the
semantic gate, a `>10%` meaningful improvement, and the `<=5%` critical
regression guard.

The model's duplicate/shared-object accounting is only a planning ceiling.
Adapter counters and settled disk establish the actual ceiling. Physical root
equality is never counted as a benefit by itself.

## Corruption and atomicity matrix

The executable model's `AuthenticatedStore` validates domain-separated object
IDs against an authenticated pre-call fingerprint and validates the complete
request before mutating its object map. For every adapter, inject one fault at
a time into a named object or selector:
wrong object ID, wrong domain, altered bytes, missing object, truncated
manifest/tree edge, root/manifest mismatch, stale branch/global selector,
transplanted object from the other repository, missing shared object, and
deleted final reference. Require a typed failure, unchanged pre-call
authoritative fingerprint, zero partial publication, and deterministic
failure digest. A valid absence/tombstone is not an error and must remain
distinct from a missing required object.

Final-reference GC runs only after both histories are fully flushed and
reopened: shared final objects remain, unreachable history/checkpoint objects
are reclaimed, and all remaining roots still authenticate after reopen.

## Future command status

The exact commands, isolated target directories, timeout policy, and artifact
schema are in `RUN_COMMANDS.md`. They are intentionally not run in this
package because the assignment requires a report-only oracle and no runtime or
production build.
