# Immutable revision-pack oracle results

This experiment asks whether another hard-cut binary-CAS representation can
remove at least 10% of the complete replay databases without increasing
payload requests. It reconstructs every CAS value through the production read
path, then packs the identical values with Git's similarity search. Git pack
objects are the established comparison: full objects and copy/insert deltas
share one pack, and `--depth=1` restricts every delta to one base. See Git's
[pack format][git-pack-format] and [pack-object selection][git-pack-objects].

The oracle is deliberately engine-generic. It does not consume plugin type,
file extension, Git/LFS status, or a text/binary storage policy. The reported
text-like/binary split is diagnostic accounting only; all payloads enter the
same packing experiment. Index bytes and pack headers are included.

## Exhaustive lower bound

The fixed, final-tree-verified SlateDB databases are the accepted semantic-root
checkpoint artifacts: VS Code 100 commits, Brands 80, and Wesnoth 15. The
oracle uses a 4,096-object search window, larger than every corpus, and reports
both a one-hop pack and a hybrid which independently keeps the smaller of the
Git-selected representation or a Zstd full anchor.

| corpus | current CAS rows | one-hop pack | hybrid Zstd-9 | chained pack |
| --- | ---: | ---: | ---: | ---: |
| VS Code | 49,813,305 | 45,865,176 | 45,474,035 | 45,637,040 |
| Brands | 15,553,043 | 13,888,964 | 13,780,089 | 13,888,822 |
| Wesnoth | 3,155,642 | 1,770,466 | 1,685,711 | 1,685,350 |
| **aggregate** | **68,521,990** | **61,524,606 (-10.21%)** | **60,939,835 (-11.06%)** | **61,211,212 (-10.67%)** |

The best theoretical replacement reduces the complete 76,822,721-byte corpus
to 69,240,566 bytes: **9.87%**, below the 10% whole-database gate. It also uses
Zstd-9 and exhaustive search. On VS Code alone, Git's one-hop selection took
14.6 seconds and the chained selection took 31.6 seconds, before any Lix
transaction, checkpoint, or adapter work. Chains do not improve the aggregate
bound and would add reconstruction CPU even if one coalesced range read hid
their I/O depth.

SlateDB already publishes immutable CAS values into segments and coalesces
ranges from the same segment. A new adapter pack trait cannot improve this
bound by removing requests; the missing bytes are representation savings, not
per-object files or round trips. RocksDB's BlobDB follows the same useful
separation—small keys in the LSM and immutable large values in blob files—so
moving the values again does not create another universal 10% cut. See
[RocksDB BlobDB][rocks-blobdb].

## Rejected production prototypes

Two production-shaped candidates were replayed with all bundled WASM plugins,
periodic checkpoints, storage flush, and final Git-tree verification.

### Generic computed replacement deltas

Every ordinary replacement offered its already-known previous content hash to
the CAS. The CAS performed format-neutral block matching, refused delta chains,
and persisted a delta only when its exact encoding saved at least 12.5%.
Plugins supplied no content type or Git policy.

| corpus | replay before | replay candidate | physical before | physical candidate |
| --- | ---: | ---: | ---: | ---: |
| VS Code | 4,695.012 ms | 4,744.948 ms (+1.06%) | 56,912,396 | 56,799,638 (-0.20%) |
| Brands | 370.679 ms | 402.252 ms (+8.52%) | 15,715,693 | 15,715,640 (flat) |
| Wesnoth | 126.817 ms | 131.595 ms (+3.77%) | 4,194,632 | 4,090,507 (-2.48%) |
| **aggregate** | **5,192.508 ms** | **5,278.795 ms (+1.66%)** | **76,822,721** | **76,605,785 (-0.28%)** |

Brands produced only three computed deltas. Same-file replacement provenance
therefore does not explain Git's media-corpus advantage, and the extra base
reads and matching CPU fail the performance requirement. The prototype was
removed.

### Stronger full-anchor compression

Changing only binary-CAS anchors from Zstd level 1 to level 3 kept semantic
pages and adapter compression unchanged.

| corpus | replay before | replay candidate | physical before | physical candidate |
| --- | ---: | ---: | ---: | ---: |
| VS Code | 4,695.012 ms | 4,775.676 ms (+1.72%) | 56,912,396 | 56,597,533 (-0.55%) |
| Brands | 370.679 ms | 375.363 ms (+1.26%) | 15,715,693 | 15,512,065 (-1.30%) |
| Wesnoth | 126.817 ms | 124.133 ms (-2.12%) | 4,194,632 | 3,973,270 (-5.28%) |
| **aggregate** | **5,192.508 ms** | **5,275.172 ms (+1.59%)** | **76,822,721** | **76,082,868 (-0.96%)** |

This candidate was also removed. Higher levels improve the oracle bound but
cannot cross the whole-database gate and would increase write/checkpoint CPU.

## Decision

Do not add revision packs, computed binary deltas, a new immutable adapter
trait, or stronger CAS compression for these corpora. Even an exhaustive
offline lower bound misses the requested whole-database threshold, while both
online approximations regress replay or save less than 1% aggregate. A future
candidate needs a different representation with a lower bound comfortably
past 10% before production implementation.

[git-pack-format]: https://git-scm.com/docs/gitformat-pack
[git-pack-objects]: https://git-scm.com/docs/git-pack-objects
[rocks-blobdb]: https://github.com/facebook/rocksdb/wiki/BlobDB
