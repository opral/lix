# EXP-KEYDIR-10 — terminal NO-WIN

## Provenance

- Exact C2 model parent: `b384d051b4ae888ba84cadcd5f9da719deb0f5f8`
- Parent tree: `f1f525a39ff17287f476b0337cfa326be4f09bd9`
- Parent subject: `report: bind page experiment to final main`
- Experiment source SHA-256: `898752d8b0962c71faa6a3da975c8b6d71175f8375130ef5ff50adc63bbcbd93`
- Definitive release executable SHA-256: `9dfe7d28bee7e4b2ebfccf1162f69d45a82828be3270ef0861719a46bc81439a`
- Definitive CSV SHA-256: `03809fc7a1636044f719ffa9579d498f99c8f5fbdce1b14b42cee128ef15b25b`
- `/usr/bin/time -v` log SHA-256: `d579c63795efbf2a8bb828e06738c3147cce03310a4cf7c68737197530feec6d`

This is an additive physical-layout model. It does not modify production or
claim a RocksDB/SlateDB result. Per the experiment contract, backend and VCS
qualification are run only after a promising smallest crossover; none exists.

## Compared authenticated layouts

All metadata is serialized inside the same immutable C2 page object and is
covered by that object's BLAKE3 ObjectId.

1. Binary search over canonical prefix-compressed restart anchors.
2. An implicit Eytzinger heap permutation over the same restart anchors.
3. One authenticated 8-bit fingerprint per key.
4. One authenticated 16-bit fingerprint per key.

Fingerprints are negative filters only. Every positive result reconstructs and
compares the complete key. They cannot avoid the lexicographic comparison
needed to locate a key inside a sorted prefix-compressed restart block.

The definitive timed path borrows prebuilt queries and reuses one reconstruction
buffer. It performs no per-lookup query or reconstructed-key heap allocation.
Seven samples cover integer, UUID, text and composite keys at
N=1K/10K/50K/100K, present and missing points, hot and cache-disturbed modes.
The matrix also records range-100, full scan, and D=1 rebuild/rewrite controls.

## Decisive point crossover

The following values aggregate 64 cells per alternative against the current
restart directory.

| Layout | Mean p50 | Mean p95 | Best p50 cell | Worst p50 cell |
|---|---:|---:|---:|---:|
| Eytzinger anchors | +0.71% | +0.71% | -4.09% | +22.01% |
| Fingerprint 8 | +22.76% | +22.54% | +12.23% | +35.71% |
| Fingerprint 16 | +22.58% | +22.26% | +12.23% | +35.71% |

No alternative delivers the required greater-than-5% important OLTP win.
Eytzinger is neutral on average, has a critical regression above 5%, and is
larger. Both fingerprint layouts regress every point cell.

At N=100K the current hot point p50 is 1.079–1.140 microseconds present and
1.141–1.164 microseconds missing across the four key types.

## Authenticated bytes and mutation cost

At N=100K, alternative total authenticated directory bytes versus restart:

| Key | Eytzinger | Fingerprint 8 | Fingerprint 16 |
|---|---:|---:|---:|
| int8 | +3.68% | +14.72% | +29.44% |
| UUID | +1.27% | +5.10% | +10.19% |
| text | +2.56% | +10.23% | +20.47% |
| composite | +2.46% | +9.83% | +19.66% |

Across the 16 D=1 cells, Eytzinger rebuild CPU is +1.06% and changed object
bytes are +2.47%. Fingerprint-8 is +199.22% CPU/+9.89% bytes; fingerprint-16
is +203.23% CPU/+19.79% bytes because canonical fingerprints must be derived
for every rebuilt key. Range and full-scan key iteration are layout-neutral;
the alternatives only add authenticated bytes, so their single-pass timings
are non-decisive noise and are not credited as wins.

Peak process RSS for the full 448-cell matrix is 54,456 KiB. Backend calls,
backend bytes and settled database bytes are unqualified because the screening
model did not pass the threshold required to run adapter cells.

## Authority and corruption controls

The model verifier authenticates and parses the domain/version, geometry,
prefix-compressed entries, restart offsets, Eytzinger permutation and
fingerprints byte-for-byte. It rejects:

- ObjectId-preserving corruption;
- rehashed truncation or trailing metadata;
- malformed restart offsets or restart shared prefixes;
- malformed Eytzinger permutations;
- fingerprint/key mismatch and false-positive missing lookups;
- duplicate or unordered keys;
- key substitution against the expected canonical inventory.

Reversed insertion order is canonicalized and produces identical page
ObjectIds. Positive lookups compare the full reconstructed key. The run exited
zero after all controls.

## Decision

Retain the current canonical binary/restart directory. Do not add Eytzinger,
fingerprint or learned-index metadata. A learned interpolation index was not
implemented because no tested metadata alternative passed the required
smallest crossover; further format tuning would violate the stop rule.

EXP-KEYDIR-10 is a qualified rejection. No production candidate, adapter run,
sub-agent review or merge is warranted.
