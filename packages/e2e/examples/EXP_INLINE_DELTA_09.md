# EXP-INLINE-DELTA-09: mutation area inside the C2 leaf object

## Verdict

**QUALIFIED NO-WIN. Global consecutive no-win streak: 9/20.**

The inline-delta layout is coherent and avoids a second patch-object fetch,
but it does not improve the important OLTP path. The best deterministic cap
remains generally neutral on reads while updates repeatedly regress by more
than 5%. It also increases written and settled bytes because every immutable
leaf rewrite still contains the complete base bytes plus its inline area.

No production integration or independent reviewer was started.

## Physical contract

C2 and the candidate store identical Schema-v1 tuples and canonical StateKeys.
The candidate retains C2's root directory and exactly one authenticated sorted
leaf object per page. Each leaf contains:

- one canonical compact sorted base;
- one sorted bounded inline map of `Value` or `Tombstone` mutations;
- no external patch, global delta, cache, fallback, JSON, second index or dual
  authority.

Reads fetch and authenticate one leaf, validate its root-directory position,
then apply the inline map over the base. Updates batch by leaf, replace the
canonical per-key inline entry, and write that one page plus the root. Entry
and encoded-byte thresholds deterministically compact the overlay into the
base. Both the pre-compaction and compacted objects are individually complete,
so publication cannot expose a partial second authority.

Controls cover value replacement, tombstone suppression, duplicate mutation
order, malformed tags, wrong StateKey/ordinal and base-page binding, page
substitution, missing objects, truncation, deterministic compaction under
reversed mutation order, immutable publication, corruption and cold reopen.

## Cap sweep and canonical policy

The 10K RocksDB/SlateDB sweep covered entry caps 2/4/8/16 and byte caps
128/256/512/1024/2048. Larger overlays avoid compaction but retain more bytes
inside every rewritten leaf and raise update latency. Representative D=10
RocksDB update ratios were:

| Cap | Update | Written bytes | Compactions over H=20 |
|---|---:|---:|---:|
| 2 entries / 256 B | 1.04x | 1.01x | 98 |
| 4 entries / 512 B | 1.06x | 1.02x | 48 |
| 8 entries / 1024 B | 1.13x | 1.05x | 18 |
| 16 entries / 2048 B | 1.16x | 1.10x | 8 |

The derived width policy targets roughly 256 bytes of encoded mutations,
rounds to a power-of-two entry class in 2/4/8/16, and selects a power-of-two
byte cap in 128-2048. It selects 2 entries / 256 bytes for the ordinary native
tuple widths. This is schema-width-derived, not runtime tuning.

## Primary matrix

Ratios are inline-delta / C2; lower is better. Twenty update and point samples
were recorded per cell.

| Backend/cell | Point hit | Update p50 | Update p95 | 1K range | Full scan | Diff |
|---|---:|---:|---:|---:|---:|---:|
| Rocks 10K D=1 | 1.00x | 1.05x | 1.02x | 0.98x | 0.91x | 1.01x |
| Rocks 10K D=10 | 1.00x | 1.07x | 1.07x | 1.00x | 0.98x | 1.00x |
| Rocks 100K D=1 | 1.00x | 1.04x | 1.03x | 1.01x | 0.98x | 0.96x |
| Rocks 100K D=10 | 1.00x | 1.05x | 1.04x | 1.01x | 1.00x | 1.01x |
| Rocks 100K D=1% | 1.10x | 1.09x | 1.10x | 0.95x | 0.99x | 1.00x |
| Slate 10K D=1 | 1.00x | 1.05x | 0.91x | 1.02x | 0.94x | 1.01x |
| Slate 10K D=10 | 1.00x | 1.11x | 0.95x | 1.02x | 1.04x | 1.00x |
| Slate 100K D=1 | 1.00x | 1.01x | 0.83x | 1.03x | 0.98x | 1.00x |
| Slate 100K D=10 | 1.03x | 1.07x | 1.12x | 1.00x | 0.99x | 1.08x |
| Slate 100K D=1% | 1.03x | 1.08x | 1.09x | 1.15x | 1.00x | 1.03x |

Point misses are essentially neutral. UUID/text/composite D=10 update ratios
are 1.03-1.06x on RocksDB and 1.01-1.10x on SlateDB. At 50K/D=10, repeated,
uniform and random-leaf updates are 1.04-1.06x on RocksDB and 1.03-1.10x on
SlateDB. The failure therefore is not tied to one PK or distribution.

Branch/history/reopen digests pass throughout. The primary sweep covers
N=1K/10K/50K/100K and D=1/10/1%; PK variants cover 10K; repeated/uniform/
random distributions cover 10K/50K.

## Physical amplification

At 100K/H=20, settled-byte ratios are approximately 1.006x for D=1,
1.008x for D=10, and 1.000-1.024x for D=1%. Candidate page bytes are always
higher because the immutable object repeats the full C2 base and adds overlay
metadata. The design avoids a second backend get, but C2 never had one; both
layouts already fetch exactly one leaf.

This is the architectural reason for the no-win: placing the delta inside the
same immutable content-addressed object cannot avoid copying or writing the
base object bytes. It adds merge/codec work without reducing authoritative
I/O.

## Reproduction

```text
EXP_INLINE_PK_KIND=integer EXP_INLINE_PATTERN=uniform \
EXP_DELTA_PAGE_BACKENDS=rocksdb,slatedb \
EXP_DELTA_PAGE_SIZES=1000,10000,50000,100000 \
EXP_DELTA_PAGE_HISTORIES=20 EXP_DELTA_PAGE_DELTAS=1,10,1pct \
EXP_DELTA_PAGE_ROOT=/root/repos/evidence/exp-inline-delta-09/final-integer-uniform-h20 \
timeout 1200 /root/repos/.target-exp-delta-page-01/release/examples/exp_delta_page_01
```

Release binary SHA-256:
`fbc23b73cf97e1082be2386637e544212cafaa3b9ed7bcf32307a4c4130590b0`

Raw log SHA-256:

- primary integer: `12406d30fd46718df2837b11191077767fbd563578d33f05406330aac9e7da48`
- UUID: `2cc639651e83f2e9353ee2663df555e493884e72a98edcb148ab6cf31a20c8ff`
- text: `c2b7da2c60f052b57833f0a889c2589494823abfac35924faafaa180d98559e8`
- composite: `7682af837cdd7750f5f18b33c62d4d237610f42262003215c6a61572af891386`
- repeated: `8cb1c026d21faeb6074430917313510430160dc7e4c6a27e669274c54d5338ec`
- uniform: `998412a48633fa89f3abec3c0bffbec9b4babe3dca7c704fe0d7772a5b6e7797`
- random: `8fa894132506d62f214f1379261302dd3aa84a536611c04cb4c19b9f1942df31`
- cap 2/256: `93ef64d7f5256ed12937d8279f02e87c589c655f9e5c5dc67fd12142d91c02fb`

The primary matrix consumed 9.18s user CPU, 0.86s system CPU, 9.61s wall and
186,616 KiB maximum RSS.
