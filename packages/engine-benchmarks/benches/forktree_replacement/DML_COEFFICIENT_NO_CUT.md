# ForkTree DML blocking-coefficient audit — terminal NO-CUT

Verdict: **NO-CUT**. Neither rejected-control coefficient has an admissible
model-only >10% correction. The predicate-pushdown contract remains correct
and unchanged; 10K was not admitted.

## RocksDB allocation calls

Exact 1K control/model counts were 22,167 versus 27,417 (+23.684%). Model
phase attribution:

| Owner | Calls | Share of model total |
|---|---:|---:|
| coherent view open | 34 | 0.124% |
| Lix binder/bridge, including target calls | 26,417 | 96.351% |
| authenticated target calls inside binder | 572 | 2.086% |
| postimage-to-mutation lowering | 285 | 1.039% |
| ForkTree publication | 555 | 2.024% |
| counter/output remainder | 126 | 0.460% |

The target itself is not the blocker. Perfectly deleting all 572 target calls
would improve total calls only 2.086% and leave 26,845 calls, still +21.103%
over current Lix. Even deleting every measured model phase outside the
binder's non-target work leaves 25,845 calls, +16.592%.

The dominant residual is the cfg-only owned-row DTO/materialization seam around
the unchanged Lix binder: external `SqlDmlBenchRow` strings/JSON are converted
to private materialized live-state batches and back to postimages. Removing
that seam requires Stage 2 to implement the proven target directly beneath the
private provider/batch boundary. A model-only replacement would duplicate the
row representation or SQL/provider authority, while changing #1260/sql2 or
cursor APIs is out of scope. Its admissible model-only perfect-elimination
ceiling is therefore 2.086%, below the >10% cut gate.

The measured model still wins wall (-38.338%), CPU (-37.620%), allocated bytes
(-56.072%), RSS (-60.692%), logical write bytes (-50.646%), and settled disk
(-31.382%) on the exact audit binary. Allocation-call count alone remains the
strict blocker.

## SlateDB physical write bytes

Exact 1K current/model physical writes were 2 objects / 1,654 bytes versus
1 object / 3,089 bytes (+86.759%). ForkTree is already at one physical write
object. Its one atomic root transition contains six authenticated objects /
2,993 bytes:

| Authenticated owner | Objects | Bytes |
|---|---:|---:|
| path-copied nodes | 3 | 1,671 |
| leaf nodes | 2 | 786 |
| internal nodes | 1 | 885 |
| mandatory non-node objects | 3 | 1,322 |

The non-node remainder is the selected value-pack plus chronology/publication
objects. Slate framing/LSM overhead is only 96 bytes (`3,089 - 2,993`), or
3.108% of physical bytes. Perfectly eliminating all backend overhead leaves
2,993 bytes, still +80.956% over current and well above the 1,736.7-byte
current+5% threshold.

Closing the remaining 1,352.3-byte threshold gap requires changing or deleting
authenticated node/value/commit/delta bytes. That is a ForkTree format/path
ownership redesign, not batching: the write is already one object and one
atomic publication. It is prohibited by this lane's no-second-format and
no-authority-change contract. No admissible batching/ownership cut exists.

The model still reduces Slate write-object count 50%, logical write bytes
50.586%, settled disk 76.711%, wall 56.917%, CPU 56.868%, allocated bytes
69.589%, allocation calls 3.647%, RSS 57.541%, physical read objects 81.690%,
and physical read bytes 94.413%. Physical write bytes remain the strict
coefficient tradeoff.

## Preserved contract

- Lix alone owns parser/binder/`RETURNING`/`ON CONFLICT`/batch/FK/savepoint
  semantics and write-target selection.
- Canonical PK equality/`IN` remains exact point/batched pushdown.
- Mixed `OR`, NULL, and noncanonical predicates remain DataFusion residuals
  over the authenticated ordered range iterator.
- One transaction-scoped coherent read view has no selector or write authority
  and is dropped before one atomic root publication.
- Complexity remains `O(D + P + R log_B N + E)` for exact identities and
  `O(N + E)` for residual ranges.

Ryzen-V should treat the Rocks call delta as a production integration-seam
acceptance test and the Slate byte delta as an explicit current-format tradeoff
or a separate replacement-format decision. Neither can be honestly hidden or
fixed in this test-only adapter.
