# Dormant future gate commands

These commands are part of the frozen acceptance contract. They must be run
only in a disposable checkout descended from the exact b59 anchor and only
after the candidate is explicitly compile-green. Do not run them against this
compiler-red frontier. Use a writable target outside `/tmp` on hosts where
`/tmp` is a constrained tmpfs.

```sh
set -euo pipefail
ROOT=/path/to/compile-green-candidate
TARGET=/root/repos/target-forktree-public-sql-direct-reader
MODEL_BIN=/root/repos/forktree-public-sql-direct-reader-model
cd "${ROOT}"

bash "${ROOT}/packages/lix/tests/forktree_public_sql_direct_reader_b59/verify_direct_reader_source.sh" \
  "${ROOT}" b59e1f11a51153e0a787a81f0f25bf104d150aaf
cargo fmt --all -- --check
git -C "${ROOT}" diff --check

rustc --edition=2021 --test -D warnings \
  "${ROOT}/packages/lix/tests/forktree_public_sql_direct_reader_b59/forktree_public_sql_direct_reader_oracle.rs" \
  -o "${MODEL_BIN}"
"${MODEL_BIN}" --nocapture --test-threads=1

CARGO_TARGET_DIR="${TARGET}" CARGO_BUILD_JOBS=2 \
  cargo clippy -p lix_benchmarks \
  --test forktree_public_sql_direct_reader_oracle -- -D warnings
CARGO_TARGET_DIR="${TARGET}" CARGO_BUILD_JOBS=2 \
  cargo test -p lix_benchmarks \
  --test forktree_public_sql_direct_reader_oracle --no-run
```

The future Cargo test target is a candidate-side registration of the pure
model; the frozen package itself does not modify workspace manifests. If the
candidate exposes it as a standalone `rustc` test only, the standalone model
command is the authoritative semantic cell and the Cargo commands are omitted
with an explicit report note.

After the source, formatting, model, warnings-denied, and no-run gates are
green, run exactly one fresh fixture per adapter, with one cell at a time and
`timeout 1200`:

```sh
for backend in memory rocksdb slatedb; do
  CARGO_TARGET_DIR="${TARGET}" CARGO_BUILD_JOBS=2 \
    timeout 1200 cargo test -p lix_benchmarks \
    --test forktree_public_sql_direct_reader_oracle \
    --features 'storage-benches slatedb' \
    -- "${backend}_public_sql_direct_reader" --exact --nocapture --test-threads=1
done
```

If the candidate uses a harness-free binary instead of test filters, preserve
the same order and timeout with fresh paths and explicit backend arguments:

```sh
timeout 1200 "${TARGET}/release/deps/forktree_public_sql_direct_reader_oracle" \
  memory /root/repos/forktree-sql-direct-memory
timeout 1200 "${TARGET}/release/deps/forktree_public_sql_direct_reader_oracle" \
  rocksdb /root/repos/forktree-sql-direct-rocks
timeout 1200 "${TARGET}/release/deps/forktree_public_sql_direct_reader_oracle" \
  slatedb /root/repos/forktree-sql-direct-slate
```

The source verifier must inspect the exact direct methods in
`live_state/context.rs`, not merely concatenate that file for token discovery.
Each durable control must perform point/range/PK/projection/overlay cases,
flush/drop/reopen, and malformed selector/catalog/state/row-kind checks. It
must report one coherent read per public operation, authenticated point/range
gets, zero writes/commits during reads, exact public digest, and unchanged
disk/process write bytes. Any fallback, repair, rebuild, cache authority,
second read snapshot, or semantic divergence stops the sequence. The cells
are capped at 20 minutes; no scale, comparator, or multimedia gate is part of
this package.
