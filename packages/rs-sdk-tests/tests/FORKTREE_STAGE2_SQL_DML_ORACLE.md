# ForkTree Stage2 SQL DML acceptance oracle

This test-only package binds the approved SQL integration contract to an
executable public-semantics gate. It adds no production implementation,
binder, executor, model loop, benchmark, compatibility path, or persisted
authority.

## Frozen comparator

- Base: `a12b76c8690130df5f9cb44a51e9cf3a3bcdb6b3`
- Base tree: `9a705d36392e88d8f5f363b2b23d373deec3321d`
- Contract report SHA-256:
  `b4a6479b6883556040db140ddef595ddf5531df58cd92d6aeab8435619b6f6a0`

## Required cfg-only SPI

The exact baseline is intentionally compile-red because Stage2 has not yet
wired these two test-only symbols:

```rust
use lix::integration::AcceptancePhysicalLayout;

open_lix()
    .with_storage(storage)
    .with_acceptance_physical_layout(AcceptancePhysicalLayout::ForkTree)
    .await
```

`AcceptancePhysicalLayout` is a closed `Current | ForkTree` enum available
only under `cfg(any(test, feature = "storage-benches"))`. The builder choice is
immutable before initialization and propagates to the engine/session/concrete
transaction physical owner. It is not a SQL/provider registry entry.

## Coverage

One deterministic 18-statement trace uses only `Lix::execute`,
`Lix::execute_batch`, and `LixTransaction::execute` and covers:

- INSERT/UPDATE/DELETE `RETURNING`;
- `ON CONFLICT DO UPDATE` and `DO NOTHING`;
- expression defaults, composite primary keys, NULL, and FK rejection;
- exact statement indexes and duplicate labels;
- post-stage `RETURNING` failure with statement-local checkpoint rollback;
- whole automatic-batch rollback at exact statement index 1;
- explicit transaction rollback;
- same-owner stale rejection and stale unrelated-owner success;
- adapter flush, complete handle drop, cold reopen, and exact public-state hash.

Each adapter test creates separate fresh Current and ForkTree repositories,
runs the identical trace, compares complete public results, then checks frozen
result and final-state SHA-256 digests. Physical identifiers are excluded.

Frozen semantic digests from the exact-`a12` current physical owner are:

- complete public result digest:
  `8ab75635b3ab498f7d77b1552fb0ec923dd661fdf655cd24cc66e0405f0ea6e1`;
- cold-reopened final logical-state digest:
  `3ad9161a21a253c6985b16628d482c016e2f786a19babd9211a1d1a790e8f4b1`.

## Exact baseline evidence

The final source was compiled on `a12` with:

```text
CARGO_TARGET_DIR=/root/repos/lix/target CARGO_BUILD_JOBS=2 cargo test -p lix_tests --test forktree_stage2_sql_dml --no-run
```

It fails only because `lix::integration::AcceptancePhysicalLayout` and
`OpenLixBuilder::with_acceptance_physical_layout` do not yet exist. The full
compiler log is `COMPILE_RED.log`, SHA-256
`b09c9f1662338b53b24df9935f8543f9657265c945a13690196a206416ffef9a`.

Before restoring the intentional red SPI references, a temporary test-local
identity shim routed both enum variants to the current physical owner solely
to type-check and execute the public trace. It did not change production code
or survive in this source. RocksDB and SlateDB both passed and produced the
same two digests above. Logs:

- RocksDB: `1295bfbfcae2e3583eb2ce215098daeaf1e35a6498accf148096475445df2e79`;
- SlateDB: `3c8d851a9c2c5444de2dddcfa65916f7dd450bd845ea1ff3e7665eb5d8094b8f`;
- temporary semantic-smoke executable:
  `9204a9190fd568e22054406856b1925666a65ac0a99225d0a7823874f2b953ef`.

The frozen oracle source SHA-256 before this report was finalized is
`b410b717f45d68e928e93dcf1332de2895db0246202e9ba9a6e5bc10b416c6bb`.

## Acceptance commands after Stage2 wires the SPI

```text
RUST_MIN_STACK=8388608 CARGO_TARGET_DIR=<isolated-target> CARGO_BUILD_JOBS=2 cargo test -p lix_tests --test forktree_stage2_sql_dml forktree_stage2_sql_dml_rocksdb -- --exact --nocapture --test-threads=1
RUST_MIN_STACK=8388608 CARGO_TARGET_DIR=<isolated-target> CARGO_BUILD_JOBS=2 cargo test -p lix_tests --test forktree_stage2_sql_dml forktree_stage2_sql_dml_slatedb -- --exact --nocapture --test-threads=1
```

Both commands must pass unchanged. No environment selector or runtime fallback
is accepted. The stack value is the repository's canonical CI setting in
`.github/workflows/ci.yml`; it is not a physical-layout selector or production
runtime workaround.
