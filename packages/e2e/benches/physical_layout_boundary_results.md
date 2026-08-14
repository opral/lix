# EXP-BOUNDARY-14 — terminal NO-WIN

## Provenance

- Exact approved C2 parent: `b384d051b4ae888ba84cadcd5f9da719deb0f5f8`
- Parent tree: `f1f525a39ff17287f476b0337cfa326be4f09bd9`
- Experiment source SHA-256: `024517ebb16dbb65b58ce10fb2b3ce8b1487c3699ccc3750f09f8d561c09cace`
- Definitive executable SHA-256: `98f202e7ddda5e20bada59edbb867924d26d83e650aff6df1cbf76b6ea227471`
- Definitive 1K/10K CSV SHA-256: `385c6e94e417c4f1bab816665bc8dbe8ab68e1f1f7a8805fb066708e619a08d7`
- Time/RSS log SHA-256: `b52273d9ed1d42bd14d6d36d1c9a77a1158adca2e0f0ef3d6e94407e918fcb5d`

This is an additive C2 model. All variants use byte-identical leaf and internal
page encodings. Only the canonical leaf partition rule differs, so retained
ObjectIds and new object bytes directly measure structural sharing.

## Compared policies

1. Fixed decoded-byte target with a hard 4 KiB maximum.
2. Content-defined boundary from a deterministic rolling hash over each
   canonical full key, with the trigger threshold derived from the next entry's
   canonical bytes and the fixed 2 KiB half-page remainder.
3. Stable four-byte key-prefix/fence transitions, subject to the same 2–4 KiB
   bounds.

The rolling predicate is independent of insertion order and workload. It
resynchronizes at a later canonical full-key anchor after an insertion. An
earlier page-state accumulator run was rejected as invalid and is not used.

## Smallest crossover decision

The definitive crossover covers integer, UUID, text, and composite keys at
N=1K and N=10K. It includes present/missing points, range-100/full scans,
D=1/10/1% updates, insert-before/after, key shifts, ten repeated updates,
sparse/dense object diff, disjoint merge, branch-root equality, canonical
insertion order, page corruption and truncation.

Aggregate results versus fixed boundaries:

| Policy | Point p50 mean | Worst point cell | Sparse mutation bytes | Sparse puts |
|---|---:|---:|---:|---:|
| rolling full key | -6.6% | **+8.8%** | -26.4% | -19.8% |
| prefix fence | -9.5% | **+17.3%** | -11.0% | +5.6% |

The rolling policy is structurally effective, but violates the mandatory
no-critical-regression guard. At N=10K, integer point-present regresses 8.2%
and point-missing regresses 8.8%. Prefix fences regress integer points by
10.7–11.3% at N=10K and have a 17.3% worst cell.

Representative rolling structural results at N=10K:

- int8 D=1 insert-after: 301,269 -> 5,948 new bytes;
- UUID D=1 insert-after: 989,473 -> 19,441 new bytes;
- int8 D=10 insert-after: 833,254 -> 81,015 new bytes;
- text D=10 insert-after: 784,762 -> 67,694 new bytes.

Those wins do not qualify because important OLTP must stay within 5% for every
critical key family. Peak process RSS for the complete crossover was 27,748
KiB.

## Authority and controls

Every page authenticates its domain, schema fingerprint, complete key/value
slot directory, bounds, ordered keys, payload extents and child ObjectIds. The
model rejects wrong-domain mutation, truncation, ObjectId/key substitution,
duplicate/unordered keys and malformed child ordering. Reversed insertion
order canonicalizes to the same root. Non-final pages remain hard bounded, and
UUID adversarial-key trees authenticate under every policy.

## Disposition

EXP-BOUNDARY-14 is a qualified rejection. The fixed boundary remains the
canonical choice under the stated guard. N=50K/100K, RocksDB/SlateDB,
reopen/settled-byte and VCS expansion are explicitly **UNRUN** because the
smallest crossover already contains critical OLTP regressions above 5%.
No production candidate or reviewer is warranted.
