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
| 64 B | 4 KiB | 4,301 / 4 | 31,129 / 22 | 2,689,375 | 3,584 / 4 | 231,826 / 171 | 1,647,269 / 1,072 |
| 256 B | 4 KiB | 3,983 / 4 | 52,453 / 52 | 4,945,598 | 3,525 / 4 | 238,457 / 208 | 1,248,302 / 1,167 |
| 1 KiB | 4 KiB | 4,795 / 4 | 146,008 / 208 | 14,312,119 | 4,406 / 4 | 253,049 / 223 | 1,506,490 / 1,564 |
| 4 KiB | 16 KiB | 13,607 / 4 | 337,183 / 204 | 32,830,923 | 12,768 / 4 | 744,875 / 202 | 2,379,986 / 1,135 |

N=1,000/10,000/50,000 changes tree height and proof bytes but does not change the selected target. Increasing D changes touched pages rather than the target crossover.

PK identity is stored once in the prefix-compressed page key directory; opaque row payload excludes duplicate PK cells. At N=50,000 the logical key bytes are 1,800,000 in every case. Logical payload/stored authenticated object bytes are respectively: 3.2/2.69 MB (64 B), 12.8/4.95 MB (256 B), 51.2/14.31 MB (1 KiB), and 204.8/32.83 MB (4 KiB). The rewrite columns above are physical new object bytes and can therefore be compared directly with the separate logical key/payload columns in the CSV.

At 256-byte rows, C2 4 KiB scans 4.95 MB versus C1's 13.84 MB, while point and D=1 each remain about 4 KiB. At 4 KiB rows, 16 KiB pages reduce scan bytes to 32.83 MB; 4 KiB pages offer no useful packing because each page approaches one row.

PAX's strongest case is partial projection of wide rows: at 4 KiB rows and a 256 KiB target it reads 11.9 KiB instead of about 38 KiB for a full page. That benefit does not offset the additional authenticated sidecar object for point, mutation, full scan, or corruption handling. PAX may be reconsidered only as a non-authoritative derived accelerator.

## Insert, delete, and branch sharing

The content-defined split policy removes the catastrophic full-tree shift observed with fixed sequential grouping. N=50,000 results:

| Row width | Insert D=1 bytes / puts / leaf delta | Delete D=1 | Insert D=100 | Delete D=100 | Branch-copy new page bytes |
|---:|---:|---:|---:|---:|---:|
| 64 B | 3,601 / 4 / 0 | 3,533 / 4 / 0 | 427,471 / 303 / +6 | 403,968 / 285 / -8 | 0 |
| 256 B | 3,557 / 4 / 0 | 3,443 / 4 / 0 | 376,667 / 349 / +10 | 375,633 / 339 / -9 | 0 |
| 1 KiB | 4,598 / 5 / +1 | 4,193 / 4 / 0 | 472,615 / 532 / +52 | 411,410 / 439 / -29 | 0 |
| 4 KiB | 12,941 / 5 / +1 | 12,157 / 4 / 0 | 1,151,554 / 463 / +52 | 1,058,014 / 389 / -30 | 0 |

A 10,000-row single insertion preserves 953/954 existing leaf ObjectIds; a deletion preserves 950/954. All pages outside the bounded adjacent boundary interval retain byte-identical ObjectIds. An unchanged branch copies the authenticated root and adds zero page objects or page bytes; branch descriptor metadata is outside this page-only model.

## Boundary and corruption controls

For 10,000 rows of 256 bytes at 4 KiB:

| Key distribution | Pages | Min/max decoded bytes | Hash boundaries | Forced-max boundaries | Underfilled non-final |
|---|---:|---:|---:|---:|---:|
| Sequential same-prefix | 954 | 2,188 / 4,036 | 592 | 361 | 0 |
| Random | 955 | 1,856 / 3,984 | 602 | 352 | 0 |
| Adversarial force-max | 834 | 1,284 / 3,788 | 0 | 833 | 0 |
| Adversarial force-min | 1,429 | 1,284 / 2,223 | 1,428 | 0 | 0 |

The low minimum is the final page only. The highly compressible 10,000 x 4 KiB control produced 824 leaves, completed without quadratic growth, and had a maximum decoded leaf of 61,800 bytes under a 64 KiB target. Corrupt object bytes are rejected by ObjectId mismatch before decode.

All 36 physical RocksDB/SlateDB cells wrote the exact authenticated object set, flushed, dropped, cold reopened, fetched the root, reauthenticated ObjectId, and decoded within the hard allocation limit.

## Evidence

- Final combined model and physical-backend sweep: `/root/repos/lix-evidence/experiment-c-ff57/model/backend-sweep-cdc-final.csv`, SHA-256 `0018792c3a11b2188ecab447d3fb27f10e977a018ba6a48c5be3407f7ca70839`; stderr is empty (SHA-256 `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`).
- Rejected intermediate sweeps remain evidence only and are not used for the decision.

## Final-rebind requirement

The selected C2 model must be rebound by object identity to commit `2cf539744e7864f79bf1994e002f47cfd3281dc0`, tree `89a6e9a0623483268cb7841f757446c5e29559dd`. The 64/256-byte dominant fixtures are schema-bound scalar tuples with no JSONB; the 1/4 KiB cases add one optional opaque varlen cell. The page layer carries those canonical tuple bytes and schema fingerprint opaquely; it must not parse text, uuid, int8, float8, boolean, jsonb, or timestamptz fields.
