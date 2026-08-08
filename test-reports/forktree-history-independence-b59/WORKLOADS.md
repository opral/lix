# Deterministic history pairs and result schema

All generated keys, branch names, checkpoint names, bytes, and seeds are
fixed. A future adapter harness must use exactly these values and write the
input digest alongside every result. The fixture size is parameterized by
`N ∈ {1_000, 10_000, 50_000, 500_000}`; only 10K is the focused admission
cell, 50K is conditional, and 500K is last/optional.

## Pair definitions

For every `N`, construct final keys `row/000000` through `row/{N-1:06}`.
Blob bytes are deterministic functions of the row index, with shared content
used by every fourth row. The modeled BlobId/ObjectId is derived from the
domain and complete content bytes, never from the row key; identical content
therefore produces one shared object even when referenced by different rows.
All other content is unique and content-addressed.
The following pairs must finish with exactly the same final row key, value,
tombstone, file identity, and blob identity sets:

| Pair | A | B | Intentional difference |
|---|---|---|---|
| `insert-order` | ascending single-row puts | fixed permutation `((i*37) mod N)` | insertion order |
| `batching` | one batch | batches of 1, 4, and 16 rows | batch boundaries/publication count |
| `branch-checkpoint` | linear puts | branch at `N/3`, checkpoint `cp-1`, branch edits, merge to final | branch/checkpoint construction |
| `intermediate-edits` | final put once | old blob put, delete, reinsert, overwrite, final put | intermediate edits |
| `shared-blobs` | shared rows then unique rows | unique rows then shared rows, remove obsolete history | shared/final reference shape |

The branch/checkpoint pair must also compare branch-visible rows and the
selected historical member before the final merge. The intermediate pair
must compare history and diff outputs before convergence. These are not
allowed to be reduced to final-state-only comparisons.

The executable model first asserts, for every pair, equality of final rows,
BlobIds/content-object IDs, logical digest, and cold-reopen digest. It then
records construction/history-root differences as diagnostics and measures
publication/synchronization bytes, diff/history reads, allocations, settled
disk, and final-reference-GC reclamation. A pair that does not satisfy the
equality assertions is a semantic failure, not a canonicalization result.

## Required result fields

Each JSONL result row must contain:

```text
base_sha, pair_id, history_id, adapter, phase, seed, input_digest,
logical_digest, result_digest, reopened_digest,
repository_root, global_selector_root, branch_selector_root,
global_state_root, local_state_root, commit_catalog_root, change_catalog_root,
roots_equal_to_pair, logical_rows, logical_bytes,
unique_object_count, unique_object_bytes, shared_object_count, shared_object_bytes,
diff_reads, diff_read_keys, diff_read_bytes, history_reads, history_read_keys,
history_read_bytes, synchronization_bytes, publication_calls,
publication_wall_ns, publication_cpu_ns, allocated_bytes, peak_rss_bytes,
backend_reads, backend_read_keys, backend_read_bytes, backend_writes,
immediate_disk_bytes, settled_disk_bytes, gc_reclaimed_objects,
gc_reclaimed_bytes, corruption_case, outcome, failure_digest
```

`roots_equal_to_pair` is informational. `outcome` must distinguish `pass`,
`valid_absence`, and `fail_closed`; it must never turn a missing required
object into `valid_absence`.

The model-only fields additionally include content-object ID sets,
history-only object bytes, publication bytes, and the explicit
`perfect_elimination_ceiling` numerator/denominator/ratio. The adapter result
must replace estimates with measured counters and retain the same field
meaning.

## Corruption and GC controls

Run the following one-at-a-time against each pair after a clean baseline:

* wrong object ID and wrong object domain;
* one altered or truncated object byte and one malformed tree edge;
* missing required commit/catalog/root/object and a transplanted object from
  the other repository;
* stale global/branch selector, missing shared object, and deleted final
  reference.

For each, require the pre-call authenticated fingerprint, no partial writes,
typed fail-closed error, and deterministic failure digest. A real tombstone or
empty result remains a successful `valid_absence` result.

After both histories are flushed and reopened, remove only an obsolete
history/checkpoint/final reference. Shared objects still referenced by the
surviving final root must remain readable; only unreachable objects may be
reclaimed. Reopen and re-authenticate the surviving roots after GC.
