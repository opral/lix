# ForkTree b59 DuckDB OLAP comparator rebind

Status: **TEST/REPORT-ONLY; UNRUN for b59**.

This package reconstructs a transportable OLAP comparator contract against the
exact accepted ForkTree frontier below. It uses the nearest available
comparator only as input evidence; its recorded timings and raw-log hashes
are not b59 runtime results.

## Exact b59 binding

```text
ForkTree head:   b59e1f11a51153e0a787a81f0f25bf104d150aaf
ForkTree tree:   700fd04d21bc40c05425c9fc9e10d65c9e1eda24
ForkTree parent: 713455a3557907ce705d06f720fcdc4486bddd4a
```

The package contains no production source, Cargo target, adapter, current-main
comparison, or runtime claim. The future harness must be built from the
candidate being qualified and must report its own source, executable, and
result-digest hashes.

## Input evidence and limitation

The nearest available comparator input is immutable branch
`origin/codex/olap-duckdb-comparator-2a0`:

```text
head:   cd76d29406ed7e00711a5b5ba9c40da537524dd3
tree:   585d9906eb9ae931f3dea2fb7d7a0b724d6eccba
parent: 2a0e8512bb37c9da2050c99c366e5ac05bb01553
EVIDENCE.md SHA-256: e783ea32afc679b019b20482bbc44edce83a653cb36722c693d2455b43642084
RESULTS.csv SHA-256: 20f6b010fa770b3a24e69cf7e13a44cda4977d0b3ee3b705dcc49c95e56b3f99
RAW_SHA256SUMS SHA-256: 0e6b229f47f07154599cf75984b6ce1d6843a5c55b4ef90a37f5ca970dc12296
```

The originally requested report hash `0b56c824c7d803f65a356d08d6c4643992decd97f885f7042c4abcea1cf68b07`
is not present in this workspace or in the advertised branch. The inherited
measurements below are therefore labeled input evidence, not measurements of
b59. The b59 semantic rebind is a contract and source-boundary requirement;
all b59 adapter cells remain dormant.

## Comparator boundary and attribution

The DuckDB control is standalone DuckDB `1.10505.0` with bundled native
SQLite-independent storage. It has no ForkTree authentication, version
control, branch/history, selector, epoch, publication, or filesystem/OLTP
authority. DuckDB wall/CPU/RSS/read counters are comparator-engine evidence;
its Rust allocation counter covers only the Rust Arrow output bridge.

The ForkTree side must attribute separately:

* one coherent authenticated `StorageRead` and selector/root validation;
* authenticated block/value-pack traversal and physical adapter reads/bytes;
* provider planning/filter/projection/materialization work;
* DataFusion/operator work; and
* publication/VC/OLTP/filesystem counters, which must remain zero for read-only
  OLAP cells and must not be charged to DuckDB.

No result may treat DuckDB bytes or timing as an authority-equivalent storage
comparison.

## Frozen fixture and exact result contract

All cells use the same deterministic fixture at 10,000, 50,000, and 500,000
rows. There are 32 lanes, 16 wide columns, and a 256-byte UTF-8 payload. Narrow
IDs are `/~forktree-olap/{ordinal:09}`; `lane = ordinal % 32`,
`score = (ordinal * 97 + 13) % 100003`, and `active = ordinal % 3 != 0`.
The dimension table has lanes `0..31` and labels `dimension-{lane:02}`.

The nine query labels, exact SQL shapes, ordering, limits, and model result
digest encoding are frozen in [QUERY_CONTRACT.md](QUERY_CONTRACT.md). Query
execution order is exactly `Query::ALL`: point, range, narrow scan, wide scan,
filter, group, order/limit, join, projection.

The digest is BLAKE3 over row count, each row's column count, and typed cells:

```text
Null       = tag 0
Integer    = tag 1 + signed i64 big-endian bytes
Text       = tag 2 + u64 big-endian byte length + UTF-8 bytes
Boolean    = tag 3 + one byte (0 or 1)
```

The exact warm/cold/model digests must be emitted by the future b59 harness.
No digest is claimed by this package; inherited raw log hashes are preserved in
`RAW_SHA256SUMS` only as provenance.

## Quantitative gate

The first b59 report must compare the exact target and baseline for every
query and size, publishing wall, CPU, allocations, RSS, backend calls/keys/
bytes, logical rows/bytes, physical object reads/bytes, writes, and settled
disk. A candidate optimization is eligible only with a measured **at least
10%** improvement in its targeted provider/materialization measure on both
RocksDB and SlateDB, or a separately justified major resource win.

Every critical guardrail must remain within **+5%** on both adapters: OLTP
transaction behavior, VC/history/branch/checkpoint semantics and latency,
filesystem/file semantics and latency, point/range OLAP latency, CPU,
allocations, RSS, backend work, writes, settled disk, cold reopen, and exact
result digest. Any authority, corruption, write, reopen, or digest failure is
a blocker regardless of speed.

The inherited comparator suggests a 51–60% provider/materialization ceiling at
50K for block-to-bounded-Arrow batching, but this is a hypothesis/input
observation from the 2a0 comparator and is not a b59 result.

## Runtime status

No compile, benchmark, SQL execution, adapter cell, current-main comparison,
or b59 runtime test was performed. Future order is:

```text
pure model/source gate
→ pinned DuckDB control
→ b59-bound Memory semantic gate
→ b59-bound RocksDB cells
→ b59-bound SlateDB cells
```

Every cell is isolated and capped at 20 minutes. Stop on the first semantic,
authority, counter, reopen, or guardrail failure.

`SHA256SUMS` freezes every package artifact. `source_verifier.sh` is static
only and must pass before any future harness is built.
