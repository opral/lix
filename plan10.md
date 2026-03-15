# Plan 10: Transaction-Wide Payload CAS Batching

## Goal

Make the write side of a tracked filesystem transaction scale as `O(1)` in database roundtrips with respect to the number of changed files.

The target runtime shape is:

- `BEGIN`
- one transaction-wide `preflight`
- one transaction-wide logical `write batch`
- `COMMIT`

Under the current backend contract, the logical `write batch` may still lower to a small constant number of physical `transaction.execute(...)` calls, but it must no longer fan out per file.

## Problem

The tracked append side is now transaction-wide, but payload persistence is still structurally per file.

Today, for each data-bearing file update, the engine builds and executes a small write program containing steps like:

- insert blob manifest row
- upsert blob store row
- bulk insert chunk store rows
- bulk insert manifest-chunk rows

Then the transaction performs the final tracked append batch.

This means the transaction is still effectively:

- one append preflight
- many payload CAS writes
- one append batch

That is not `O(1)` at transaction scope.

## First Principles

The database should not observe file-at-a-time payload persistence.

The engine should:

1. collect every payload-bearing file change in the transaction
2. compute hashes, chunking, and compression in memory
3. deduplicate blobs and chunks across the whole transaction
4. lower the full payload set into one transaction-wide CAS plan
5. append the full tracked commit once

The DB-facing shape should be by table family, not by file.

## Final Architecture

One tracked filesystem transaction produces exactly one `TrackedFilesystemPayloadBatch`.

That batch owns all rows needed for:

- `lix_internal_binary_blob_manifest`
- `lix_internal_binary_blob_store`
- `lix_internal_binary_chunk_store`
- `lix_internal_binary_blob_manifest_chunk`
- final `lix_binary_blob_ref` domain changes already represented in the append batch

The tracked append path then emits:

1. one payload CAS write family batch
2. one final append batch

If backend limits require chunking for parameter count or SQL size, chunking must be by constant-size physical fragment, not by source file. That preserves `O(1)` in file count for normal workloads until hard backend limits are reached.

## Required Invariants

- No per-file `persist_binary_blob_with_fastcdc...` execution loop in tracked writes.
- No helper may call `transaction.execute(...)` once per file payload.
- Payload deduplication is transaction-wide, not file-local.
- The append batch remains transaction-wide and runs once.
- Blob CAS rows are built from semantic payload intent, not re-derived from SQL text.

## Required Structural Changes

### 1. Replace Per-File Payload Programs

Delete the per-file tracked payload persistence shape in:

- `tracked_write_runner.rs`
- `append_commit.rs`
- `runtime_effects.rs`

Specifically, remove the pattern of:

- build one `WriteProgram` per file
- iterate its steps
- execute each step immediately

### 2. Introduce Transaction-Wide Payload IR

Add an engine-owned IR for payload persistence, for example:

```rust
TrackedFilesystemPayloadBatch {
    blobs: Vec<BlobRow>,
    blob_store_rows: Vec<BlobStoreRow>,
    chunk_rows: Vec<ChunkRow>,
    manifest_chunk_rows: Vec<ManifestChunkRow>,
}
```

This IR must be constructed from all lazy exact file data updates in the transaction.

### 3. Deduplicate Across The Whole Transaction

The builder must dedupe by:

- blob hash
- chunk hash
- blob-hash/chunk-index pair where relevant

Two files in the same transaction with identical payloads must not produce duplicate CAS rows.

### 4. Lower By Table Family

Lower the batch into a small constant number of physical SQL batches:

- one manifest insert batch
- one blob store upsert batch
- one chunk store insert batch
- one manifest-chunk insert batch

The final append remains one separate batch.

### 5. Enforce This In The Tracked Runner

The tracked runner should only know:

- preflight result
- transaction-wide payload batch
- final append batch

It should not loop over files to persist payloads.

## Benchmarks

### Focused Bench

Use the existing exact-id data update bench:

```bash
cargo bench -p lix_engine --bench lix_file_update update_existing_row/data_update_100kb -- --nocapture
```

With trace:

```bash
LIX_BENCH_TRACE_UPDATE=1 LIX_BENCH_TRACE_VERBOSE=1 cargo bench -p lix_engine --bench lix_file_update update_existing_row/data_update_100kb -- --nocapture
```

Expected improvement:

- same or fewer `tx_exec`
- lower `payload` / `snapshot_blob` / total traced write time

### Replay Bench

Primary benchmark:

```bash
cargo run --release -p lix_cli -- exp git-replay \
  --repo-path /Users/samuel/git-repos/paraglide-js \
  --output-lix-path /Users/samuel/git-repos/lix-2/artifact/paraglide-js.lix \
  --branch main \
  --num-commits 897 \
  --profile-json /Users/samuel/git-repos/lix-2/artifact/paraglide-js.profile.before.json \
  --force
```

and after the change:

```bash
cargo run --release -p lix_cli -- exp git-replay \
  --repo-path /Users/samuel/git-repos/paraglide-js \
  --output-lix-path /Users/samuel/git-repos/lix-2/artifact/paraglide-js.lix \
  --branch main \
  --num-commits 897 \
  --profile-json /Users/samuel/git-repos/lix-2/artifact/paraglide-js.profile.after.json \
  --force
```

### Outlier Trace

Trace the current worst insert-heavy outlier before/after:

```bash
cargo run --release -p lix_cli -- exp git-replay \
  --repo-path /Users/samuel/git-repos/paraglide-js \
  --output-lix-path /Users/samuel/git-repos/lix-2/artifact/paraglide-js.trace.lix \
  --branch main \
  --num-commits 897 \
  --profile-json /Users/samuel/git-repos/lix-2/artifact/paraglide-js.profile.trace.json \
  --trace-sql-json /Users/samuel/git-repos/lix-2/artifact/paraglide-js.trace-f0dd.json \
  --trace-commit f0dd5d327e4b6491926e733644cb08f1c50ec467 \
  --force
```

Expected improvement:

- fewer write-side `transaction_execute(...)` calls
- lower execute time for large insert commits
- reduced sensitivity to changed file count

## Success Criteria

- Large insert/update replay commits no longer show per-file payload CAS write fanout.
- The transaction-wide write phase is constant-size in physical DB call families.
- The worst replay outliers shift from payload write fanout to either commit flush cost or pure CPU serialization cost.
- The engine no longer contains tracked write code that persists payloads one file at a time.

## Progress Log

- Created `plan10` to remove the remaining per-file payload CAS fanout from tracked filesystem transactions.
- Replaced per-file payload CAS execution with a transaction-wide payload batch shared by:
  - tracked append in `append_commit.rs`
  - pending-file payload persistence in `runtime_effects.rs`
- The payload batch now:
  - deduplicates blob manifests, blob store rows, chunks, and manifest-chunk rows across the whole transaction
  - lowers them by table family
  - chunks only by backend parameter limits, not by source file
- Added a SQL2 validation fast path so planned `lix_binary_blob_ref` rows accept blob hashes already present in the same transaction-wide payload batch without per-row CAS existence queries.
- Validation checkpoints:
  - `cargo check -p lix_engine -p lix_cli`
  - `cargo test -p lix_engine --test transaction_execution transaction_script_path_handles_parameterized_multi_row_lix_file_insert -- --nocapture`
  - `cargo test -p lix_engine --test file_materialization file_update_data_by_path_updates_binary_blob_ref -- --nocapture`
  - `cargo test -p lix_engine --test filesystem_view file_by_version_insert_with_untracked_persists_data -- --nocapture`
- Focused bench result:
  - `lix_file/update_existing_row/data_update_100kb`: `[3.2832 ms 3.3855 ms 3.4944 ms]`
  - this regressed on the single-file path, which is expected to be less favorable for transaction-wide batching because there is only one payload-bearing file to batch
- Replay outlier result on `f0dd5d327e4b6491926e733644cb08f1c50ec467`:
  - before: `138.84 ms total`, `120.37 ms execute`, `2232 transaction_execute` calls
  - after: `107.32 ms total`, `84.89 ms execute`, `20 transaction_execute` calls
  - final traced write shape is now:
    - `1` append preflight
    - `1` blob manifest batch
    - `1` blob store batch
    - `1` chunk store batch
    - `1` manifest-chunk batch
    - `1` append batch
    - plus a small fixed set of non-payload lookup statements and `COMMIT`
