# SQLite scan baseline

The reference SQLite number for lix scan comparisons. Committed here because
the campaign spent weeks quoting a lix:SQLite ratio that **was not
re-derivable from a clean checkout** — the SQLite side lived in nobody's repo.

It is **excluded from the workspace** (own `[workspace]` table, and listed in
the root manifest's `exclude`). It compiles SQLite from C via
`rusqlite/bundled`, which must never enter the lix build graph: a reference
baseline should be reproducible, not paid for on every CI run. Verified: the
crate appears in neither `cargo metadata` (14 workspace packages, absent) nor
`cargo test --workspace` nor `cargo clippy --workspace --all-targets`.

## Running

```
cd benchmarks/sqlite_scan_baseline
cargo run --release -- 10000 9      # rows, reps
```

`rusqlite` and `libsqlite3-sys` are present in the registry cache on the bench
fleet, so `bundled` builds offline.

## Pairing

Run against `expb_scan_baseline` in `packages/e2e/examples` **on the same
host** — the lix:SQLite ratio is only meaningful within a machine. Host class
is an axis: an identical write-path binary measured 5.4x apart between
hetzner (cloud disk, fsync-dominated) and ryzen (NVMe).

Both harnesses use the same row count, the same two query shapes, the same rep
count, and mimalloc.

## Report both denominators

`full_scan` returns every row (scanned == returned). `filtered_one` scans the
whole table and returns one row, because `value` carries no index — the plan is
asserted to contain `SCAN`, so it cannot silently become a seek.

**ns per scanned row and ns per returned row are different numbers and must be
reported separately.** Conflating them is the direct cause of this campaign's
375x-versus-110x confusion.
