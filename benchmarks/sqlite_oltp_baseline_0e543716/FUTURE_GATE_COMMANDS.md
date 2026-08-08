# Standalone SQLite baseline commands

All commands are test/report-only. Each cell is capped at 20 minutes. These
commands never invoke ForkTree and do not qualify an integrated target.

## Pinned build and static checks

```bash
cd benchmarks/sqlite_oltp_baseline_0e543716
cargo fmt -- --check
cargo check --locked --offline
cargo clippy --locked --offline -- -D warnings
cargo build --release --locked --offline
sha256sum Cargo.toml Cargo.lock src/main.rs target/release/sqlite_oltp_baseline_0e543716
```

The build must report bundled `libsqlite3-sys 0.30.1` and runtime SQLite
`3.46.0`. The exact crate/source/archive hashes are in `README.md` and
`MANIFEST.json`.

## Focused smoke cell

```bash
timeout 1200 /usr/bin/time -v \
  target/release/sqlite_oltp_baseline_0e543716 \
  /tmp/sqlite-oltp-baseline-0e543716-direct smoke 1000
```

This runs point reads, CRUD/RETURNING, savepoint rollback, conflicts,
reopen, file-row mutations, and corruption controls on fresh files. The
frozen successful output is `EVIDENCE_SMOKE.txt`.

## Individual cells

```bash
for cell in point-1000 crud transaction-savepoint conflict reopen file-row corruption; do
  timeout 1200 /usr/bin/time -v \
    target/release/sqlite_oltp_baseline_0e543716 \
    "/tmp/sqlite-oltp-baseline-0e543716-$cell" "$cell" 1000
done
```

The first later ForkTree pairing must consume the same cell names, seed,
workload, digest domain, and result contracts. No 10K expansion is implied by
this baseline alone.
