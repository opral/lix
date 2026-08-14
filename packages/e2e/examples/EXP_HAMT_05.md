# EXP-HAMT-05: authenticated HAMT current-state layout

## Verdict

**QUALIFIED NO-WIN. Global radical-layout no-win streak: 5/20.**

The authenticated HAMT materially improves exact point lookup and sometimes
sparse RocksDB updates, but it fails the OLTP-first gate. Without a second
ordered authority, every bounded range and full scan must authenticate and
enumerate the complete HAMT and then canonical-sort keys. SlateDB path-copy
updates also regress substantially. Storage savings for dense updates do not
override those latency regressions under the lexicographic acceptance rule.

No production integration or reviewer was started.

## Compared layouts

The control is the approved C2 schema-partitioned slotted-page model. Both
layouts store identical Schema-v1 typed tuple bytes and full canonical
`StateKey` bytes.

The candidate is the sole current-state authority:

- full encoded `StateKey` is hashed for traversal;
- 16-way canonical bitmap branches and collision leaves bounded to 8 rows;
- immutable content-addressed nodes with canonical split/collapse and sorted
  leaf encoding;
- path-copy updates and ObjectId-equality-pruned recursive root diff;
- no ordered side index, cache, fallback, JSON, or dual authority;
- range and scan enumerate authenticated nodes and sort canonical keys.

Forward and reverse insertion produce the same root. Decode/runtime controls
reject malformed bitmap cardinality, oversized/colliding leaves, duplicate or
noncanonical keys, truncation, missing/wrong child objects, and forged roots.
Cold reopen validates the complete resulting state.

## Workload

- PK geometry: integer, UUID, text, composite.
- Sizes: 1K, 10K, 50K, 100K.
- Mutation sets: 1, 10, 1%; uniform, random, repeated-key, and adversarial
  prefix distributions.
- Operations: hot/cold hit and miss, update, 1K bounded range, full readback,
  root diff, branch sharing, history, corruption, and reopen.
- Backends: RocksDB and SlateDB.
- Twenty update and point samples per cell; p50/p95 plus backend calls/bytes,
  decoded nodes/rows, staged objects/bytes, RSS, and settled bytes.

## Decisive results

Ratios below are HAMT / C2; lower is better.

| Cell | RocksDB | SlateDB |
|---|---:|---:|
| 100K D=1 update p50 | 0.51x | 1.42x |
| 100K D=10 update p50 | 0.80x | 4.78x |
| 100K D=1% update p50 | 0.76x | 4.57x |
| 100K 1K-row bounded range | about 233-236x | about 694-754x |
| Full readback across scale | 2.1-3.75x | 4.3-15.8x |

At 100K/D=1% the SlateDB range reads 55,689 HAMT nodes and all 100,000 rows
(12.0 MB) to return the bounded range, versus 18 C2 object keys and 1,088
decoded rows (159 KB). This is the architectural failure, not harness noise.

Adversarial shared-prefix keys amplify the update cost: at 10K, RocksDB D=10
and D=100 are 1.93x and 2.44x; SlateDB is 9.38x and 15.00x. UUID, text, and
composite PK runs reproduce the same shape: at 10K/D=10, RocksDB bounded range
is 18.7-20.0x C2 and SlateDB is 47.2-51.1x.

Dense-history storage can favor HAMT, but sparse cells regress. At 100K/D=1,
RocksDB settled bytes are 14.41 MB versus 11.38 MB C2 and SlateDB is 15.38 MB
versus 11.35 MB. At D=1%, HAMT falls to 64.91/68.88 MB versus C2
280.30/140.15 MB, but that does not compensate for critical OLTP latency.

## Root cause and decision

The HAMT is a good exact-key map, but it cannot also be an efficient ordered
entity store without another index. Adding one would violate this experiment's
single-authority contract. Its range cost is necessarily O(N) authentication
plus O(N log N) canonical sorting, while C2 provides O(log N + R) ordered
access. SlateDB additionally magnifies the many small path-copy object writes.

Therefore EXP-HAMT-05 is rejected for the current-state physical layout. The
point and dense-write wins are real but not independently promotable.

## Reproduction and immutable inputs

Release binary SHA-256:
`0ce093dee84efc471df8311c7a87977947b6ebc82ebf089d1f2bccf08972abdc`

Primary command:

```text
EXP_HAMT_PK_KIND=integer EXP_HAMT_PATTERN=uniform \
EXP_DELTA_PAGE_BACKENDS=rocksdb,slatedb \
EXP_DELTA_PAGE_SIZES=1000,10000,50000,100000 \
EXP_DELTA_PAGE_HISTORIES=20 EXP_DELTA_PAGE_DELTAS=1,10,1pct \
EXP_DELTA_PAGE_ROOT=/root/repos/evidence/exp-hamt-05/integer-uniform-h20 \
timeout 1200 /root/repos/.target-exp-delta-page-01/release/examples/exp_delta_page_01
```

Raw log SHA-256:

- integer uniform: `df6b0e4a0feb6e564a1217de1e6f62fd3c585bfed9b6d4dc7fe3b7c29b8deffd`
- integer adversarial prefix: `0b9443e32eee16a4b7380e887f1531d882a628b01c809158f47e4b28b513b1a9`
- UUID: `6e5ac18273ea3484095fc0ea31993546c2531084e35fd41c662cf327d62d637e`
- text: `a7c98863230b5f9ef3acc977da047abba01eb22e2770190264a9d0ec346d98af`
- composite: `b669ffb59679e8b09bbefbfcf83f8529521ca2163ab8497624047d2bc680abae`
- corruption/canonicality preflight: `cbcdc3d4642639cc25ff91b02c8822db5adfbf5c0037a6696c4f158a028c4b16`

The primary matrix used 25.39 s user CPU, 7.37 s system CPU, 30.10 s wall,
and 375,372 KiB maximum RSS.
