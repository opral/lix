# ForkTree physical layout experiment C

## Provenance and scope

- Experimental source anchor: PR #1469 commit `ff5795c93fc70dd0c6e77fc08e6a311727603c7b`, tree `1630ea3836e5c95237140d6d54e28b3bf5b2d425`.
- Comparator anchor: `origin/main` commit `6085ac656baf1634dd152c5e23da03589c2edea9`, tree `71e6479df9b0c056c0db79177e169fec988f84eb`.
- The schema-vocabulary benchmark overlay is the separate commit `f3987313938800a9d367f83bbc801bfe8c35e434`; it changes no production code.
- This additive model is not wired into production and does not claim SQL runtime results. It measures authenticated object geometry using opaque canonical typed-row bytes plus a schema fingerprint.
- The pinned ff57 real transaction-path control remains disqualified: it fails before measurement with `registered schema snapshot missing string schema_key` (log SHA-256 `82dad4297bf2179ca950a87a8ee72e77daf6b8626df6c1387cd271e0dcef83b4`). Runtime qualification therefore belongs only to the exact 2cf rebind, where the seven-type registry contract is green.

## Decision

Select C2 schema-partitioned slotted pages. Reject C1 one-row-per-object because its scan/object fanout is excessive. Reject C3 PAX as the sole authority because its sidecar doubles leaf objects and proofs while improving only partial projection; it does not win the full-row OLTP path.

Use this deterministic size class:

```text
target_decoded_bytes = clamp(next_power_of_two(4 * maximum_canonical_row_bytes), 4 KiB, 16 KiB)
```

This selects 4 KiB for rows through 1 KiB and 16 KiB for 4 KiB rows. Oversized rows are a single bounded page and the decoder rejects any object above 256 KiB.

The canonical split policy is independent of compression:

1. Accumulate canonical uncompressed decoded bytes.
2. Never split before half the target, except an oversized single row.
3. After the minimum, evaluate domain-separated BLAKE3 over the complete canonical row key. The byte-weighted predicate targets the remaining half-page occupancy.
4. Split before adding a row that would exceed the hard target.
5. Apply the same key-anchored rule to internal nodes.
6. Compress only after partitioning. Compression never changes a boundary or decoded allocation bound.

An adversary can force either predicate outcome by selecting keys, but cannot create unbounded occupancy or unbounded page count: the mandatory maximum and minimum bound occupancy to `[target/2, target]` for every non-final, non-oversized page. The worst key-choice effect is bounded to a factor of two in page count.

## Decisive crossover (N=50,000)

Range is 1%/500 rows. Proof objects include the leaf and authenticated internal path. Values are compressed object bytes.

| Row width | Target | Point bytes / objects | Range bytes / objects | Full scan bytes | D=1 replace bytes / puts | D=100 | D=1000 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 64 B | 4 KiB | 4,422 / 4 | 31,644 / 22 | 2,732,257 | 3,696 / 4 | 236,253 / 171 | 1,672,719 / 1,072 |
| 256 B | 4 KiB | 4,095 / 4 | 53,749 / 52 | 5,066,771 | 3,609 / 4 | 243,827 / 208 | 1,277,862 / 1,167 |
| 1 KiB | 4 KiB | 4,923 / 4 | 151,192 / 208 | 14,831,404 | 4,504 / 4 | 259,374 / 223 | 1,548,504 / 1,564 |
| 4 KiB | 16 KiB | 13,778 / 4 | 342,742 / 204 | 33,386,958 | 12,916 / 4 | 752,695 / 202 | 2,415,316 / 1,135 |

N=1,000/10,000/50,000 changes tree height and proof bytes but does not change the selected target. Increasing D changes touched pages rather than the target crossover.

PK identity is stored once in the prefix-compressed page key directory; opaque row payload excludes duplicate PK cells. At N=50,000 the logical key bytes are 1,800,000 in every case. Logical payload/stored authenticated object bytes are respectively: 3.2/2.73 MB (64 B), 12.8/5.07 MB (256 B), 51.2/14.83 MB (1 KiB), and 204.8/33.39 MB (4 KiB). The rewrite columns above are physical new object bytes and can therefore be compared directly with the separate logical key/payload columns in the CSV.

At 256-byte rows, C2 4 KiB scans 5.07 MB versus C1's 13.95 MB, while point and D=1 each remain about 4 KiB. At 4 KiB rows, 16 KiB pages reduce scan bytes to 33.39 MB; 4 KiB pages offer no useful packing because each page approaches one row.

PAX's strongest case is partial projection of wide rows: at 4 KiB rows and a 256 KiB target it reads 40,473 bytes instead of 68,439 bytes for a full point. That benefit does not offset the additional authenticated sidecar object for point, mutation, full scan, or corruption handling. PAX may be reconsidered only as a non-authoritative derived accelerator.

## Insert, delete, and branch sharing

The content-defined split policy removes the catastrophic full-tree shift observed with fixed sequential grouping. N=50,000 results:

| Row width | Insert D=1 bytes / puts / leaf delta | Delete D=1 | Insert D=100 | Delete D=100 | Branch-copy new page bytes |
|---:|---:|---:|---:|---:|---:|
| 64 B | 3,710 / 4 / 0 | 3,650 / 4 / 0 | 434,610 / 303 / +6 | 411,094 / 285 / -8 | 0 |
| 256 B | 3,619 / 4 / 0 | 3,529 / 4 / 0 | 385,183 / 349 / +10 | 384,016 / 339 / -9 | 0 |
| 1 KiB | 4,710 / 5 / +1 | 4,287 / 4 / 0 | 486,803 / 532 / +52 | 423,449 / 439 / -29 | 0 |
| 4 KiB | 13,114 / 5 / +1 | 12,297 / 4 / 0 | 1,166,026 / 463 / +52 | 1,070,735 / 389 / -30 | 0 |

A 10,000-row single insertion preserves 953/954 existing leaf ObjectIds; a deletion preserves 950/954. Prefix/suffix interval assertions prove every page outside the bounded adjacent boundary interval retains its byte-identical ObjectId. An unchanged branch has a distinct authenticated branch-ref object but shares the exact page-object set and root, adding zero page objects or page bytes. Changing one row changes exactly one leaf (plus the PAX sidecar when applicable) and one authenticated object on each root-path level.

## Boundary and corruption controls

For 10,000 rows of 256 bytes at 4 KiB:

| Key distribution | Pages | Min/max decoded bytes | Hash boundaries | Forced-max boundaries | Underfilled non-final |
|---|---:|---:|---:|---:|---:|
| Sequential same-prefix | 954 | 2,188 / 4,036 | 592 | 361 | 0 |
| Random | 955 | 1,856 / 3,984 | 602 | 352 | 0 |
| Adversarial force-max | 834 | 1,284 / 3,788 | 0 | 833 | 0 |
| Adversarial force-min | 1,429 | 1,284 / 2,223 | 1,428 | 0 | 0 |

The low minimum is the final page only. The highly compressible 10,000 x 4 KiB control produced 824 leaves, completed without quadratic growth, and had a maximum decoded leaf of 61,880 bytes under a 64 KiB target. The verifier starts from an authenticated branch-ref ObjectId, derives branch identity and root linkage from its bytes, and recursively authenticates ObjectId, envelope, domain, schema/layout fingerprint, embedded min/max bounds, ordered parent edges, row counts, directory offsets, payload bounds, and PAX sidecar binding. Independent envelope, domain, fingerprint, bound, directory, payload, and root-link mutations all fail closed.

All 36 physical RocksDB/SlateDB cells wrote the exact authenticated branch and page closure, flushed, dropped, cold reopened, batch-fetched the closure, and recursively reauthenticated every typed edge within the hard allocation limit.

## Evidence

- Final combined model and physical-backend sweep: `/root/repos/lix-evidence/experiment-c-ff57/model/backend-sweep-reviewed.csv`, SHA-256 `ad2452a2df410dfbb2029ae9f3ca8af8a19b6886a302181d36c4d8d0091dc615`; stderr is empty (SHA-256 `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`).
- Rejected intermediate sweeps remain evidence only and are not used for the decision.

## Final-rebind requirement

The selected C2 model must be rebound by object identity to commit `2cf539744e7864f79bf1994e002f47cfd3281dc0`, tree `89a6e9a0623483268cb7841f757446c5e29559dd`. The 64/256-byte dominant fixtures are schema-bound scalar tuples with no JSONB; the 1/4 KiB cases add one optional opaque varlen cell. The page layer carries those canonical tuple bytes and schema fingerprint opaquely; it must not parse text, uuid, int8, float8, boolean, jsonb, or timestamptz fields.
