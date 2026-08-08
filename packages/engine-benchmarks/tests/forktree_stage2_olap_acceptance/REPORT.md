# Stage 2 OLAP acceptance oracle

## Status

**Frozen test/report-only oracle; production execution pending an immutable runnable Stage 2 head.**

The oracle is based on exact current main `a12b76c8690130df5f9cb44a51e9cf3a3bcdb6b3`, tree
`9a705d36392e88d8f5f363b2b23d373deec3321d`. It imports the exact frozen 15-cell comparator tables and
digests without changing them:

- query medians SHA-256 `2018e22a677e427d693cf66d903a6f11de09eb4104e2106cbfcb4acfc2485a2e`;
- run/RSS/disk table SHA-256 `347217cd9e54f04ec05ac599b84712edca1d809636f0ec456d50815ab4771dc5`;
- result digests SHA-256 `98c15486d8f14fcdf9afa2ed803c6fcde5a87a9299381b525a9acd51c4b027d4`.

No production source, persisted format, API, Stage1/Stage2 owner, PR, or compatibility path is changed.
The Cargo change only registers an opt-in benchmark feature and binary. Default/workspace builds do not
compile it.

## Ordered gate

The runner stops on the first failure:

1. 10K RocksDB control, malformed block, substituted block.
2. 10K SlateDB control, malformed block, substituted block.
3. Only after all six pass, 50K RocksDB then SlateDB controls.
4. Only after both 50K controls pass, 500K RocksDB then SlateDB controls.

Each cell is capped at 20 minutes. Control cells execute point, ten-row PK range, wide-table two-column
projection, aggregate/group-by, and join. Result BLAKE3 and row counts must match the frozen comparator
before and after flush/drop/cold reopen.

The production owner must report exactly one coherent StorageRead per query, authenticated block
batching, projection before row allocation, latency, CPU, Rust allocations, maximum RSS, backend calls,
objects/bytes, query writes, and settled disk. Query-phase writes must be zero. Range and projection must
improve at least 10% versus exact a12; every other representative query and every critical resource must
remain within +5% of a12. RocksDB call/byte ceilings use the accepted 2a0e model because exact a12 lacks
equivalent physical-object counters.

## SlateDB residual

The known 50K model residual—six physical reads instead of current Lix's five for a one-range query,
twelve instead of ten for the join—is a hard failure. The same integer-sensitive +5% rule applies at all
scales. A manager can waive only `physical_read_objects`, only on SlateDB, and only through a checked-in
JSON artifact that binds exact candidate head, scale, query, observed value, computed limit, manager
identity, report SHA-256, and an aggregate representative-query improvement of at least 20%. The runner
cannot waive latency, CPU, allocations, RSS, bytes, writes, disk, digest, reopen, coherent-read, batching,
projection-order, or corruption failures.

## Compile boundary

Exact a12 and the current mutable Stage 2 production branch do not contain `AcceptancePhysicalLayout`.
Per assignment, no build was attempted against those non-runnable heads. Feature-enabled compilation on
a12 is intentionally red at the two missing storage-benches-only SPI imports. The production owner must
provide the sealed sole implementation; the benchmark bridge contains no model or alternate owner.

The runnable policy/self-test command is:

```sh
node --test scripts/forktree-stage2-olap-gate.test.mjs
```

Once an immutable runnable candidate exists:

```sh
cargo bench -p lix_benchmarks --bench forktree_stage2_olap_acceptance \
  --features storage-benches,slatedb,forktree-stage2-olap-acceptance --no-run
node scripts/forktree-stage2-olap-gate.mjs \
  --binary <exact-binary> --evidence <new-empty-directory>
```

The first command is workspace-locked and capped at 20 minutes. The second command enforces the complete
ordered matrix and per-cell cap. It must be run from a clean detached worktree at the immutable candidate
head with a candidate-specific target directory and hashed executable.
