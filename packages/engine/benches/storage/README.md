# Engine Storage Benchmarks

These Criterion benchmarks measure engine-owned storage layers directly,
without going through SQL or the SDK:

- `tracked_state`
- `untracked_state`
- `changelog`
- `binary_cas`
- `json_store`
- `storage/api`

The benchmark target uses `codspeed-criterion-compat`, so it works with normal
`cargo bench` and with CodSpeed.

## Run

```bash
cargo bench -p lix_engine --features storage-benches --bench storage
```

Run one benchmark by filter:

```bash
cargo bench -p lix_engine --features storage-benches --bench storage -- \
  storage/tracked_state/read_point_hit/10k
```

CodSpeed:

```bash
cargo codspeed build -p lix_engine --features storage-benches --bench storage
cargo codspeed run
```

Storage accounting report:

```bash
cargo test -p lix_engine --features storage-benches storage_accounting -- --ignored --nocapture
```

## Benchmarks

The checked-in baseline size is stable: `10k` logical rows or blobs, with
`1KiB` binary payloads for Binary CAS and small JSON payloads for state rows.
Large payload variants intentionally use fewer rows so a full benchmark run
does not allocate multi-gigabyte fixtures.

```text
storage/tracked_state/write_root/10k
storage/tracked_state/read_point_hit/10k
storage/tracked_state/read_point_miss/10k
storage/tracked_state/scan_all/10k
storage/tracked_state/scan_schema/10k
storage/tracked_state/scan_file/10k
storage/tracked_state/update_existing/10k
storage/untracked_state/write_rows/10k
storage/untracked_state/read_point_hit/10k
storage/untracked_state/read_point_miss/10k
storage/untracked_state/scan_all/10k
storage/untracked_state/scan_version/10k
storage/untracked_state/scan_schema/10k
storage/untracked_state/overwrite_existing/10k
storage/changelog/append_changes/10k
storage/changelog/load_change_hit/10k
storage/changelog/load_change_miss/10k
storage/changelog/scan_all/10k
storage/changelog/scan_limit_100/10k
storage/changelog/scan_change_set/10k
commit_graph/change_history_from_commit/10k
storage/binary_cas/write_blobs_1k/10k
storage/binary_cas/read_blob_hit_1k/10k
storage/binary_cas/read_blob_miss_1k/10k
storage/binary_cas/write_duplicate_payload_1k/10k
storage/api/{in_memory,sqlite_tempfile,rocksdb_tempdir}/write_kv_batch_put/1
storage/api/{in_memory,sqlite_tempfile,rocksdb_tempdir}/write_kv_batch_put/10
storage/api/{in_memory,sqlite_tempfile,rocksdb_tempdir}/write_kv_batch_put/100
storage/api/{in_memory,sqlite_tempfile,rocksdb_tempdir}/write_kv_batch_put/1k
storage/api/{in_memory,sqlite_tempfile,rocksdb_tempdir}/write_kv_batch_put/10k
storage/api/{in_memory,sqlite_tempfile,rocksdb_tempdir}/write_kv_batch_mixed_put_delete/10k
storage/api/{in_memory,sqlite_tempfile,rocksdb_tempdir}/write_kv_batch_multi_namespace/10k
storage/api/{in_memory,sqlite_tempfile,rocksdb_tempdir}/write_kv_batch_duplicate_keys/10k
storage/api/{in_memory,sqlite_tempfile,rocksdb_tempdir}/write_kv_batch_value_size/64b
storage/api/{in_memory,sqlite_tempfile,rocksdb_tempdir}/write_kv_batch_value_size/1k
storage/api/{in_memory,sqlite_tempfile,rocksdb_tempdir}/write_kv_batch_value_size/16k
storage/api/{in_memory,sqlite_tempfile,rocksdb_tempdir}/write_kv_batch_value_size/128k
storage/api/{in_memory,sqlite_tempfile,rocksdb_tempdir}/transaction_write_and_commit/1
storage/api/{in_memory,sqlite_tempfile,rocksdb_tempdir}/transaction_write_and_commit/100
storage/api/{in_memory,sqlite_tempfile,rocksdb_tempdir}/transaction_write_and_commit/10k
storage/api/{in_memory,sqlite_tempfile,rocksdb_tempdir}/transaction_rollback_after_write/10k
storage/api/{in_memory,sqlite_tempfile,rocksdb_tempdir}/get_values_hit/100
storage/api/{in_memory,sqlite_tempfile,rocksdb_tempdir}/get_values_hit/1k
storage/api/{in_memory,sqlite_tempfile,rocksdb_tempdir}/get_values_hit/10k
storage/api/{in_memory,sqlite_tempfile,rocksdb_tempdir}/get_values_miss/100
storage/api/{in_memory,sqlite_tempfile,rocksdb_tempdir}/get_values_miss/1k
storage/api/{in_memory,sqlite_tempfile,rocksdb_tempdir}/get_values_miss/10k
storage/api/{in_memory,sqlite_tempfile,rocksdb_tempdir}/get_values_mixed_hit_miss/100
storage/api/{in_memory,sqlite_tempfile,rocksdb_tempdir}/get_values_mixed_hit_miss/1k
storage/api/{in_memory,sqlite_tempfile,rocksdb_tempdir}/get_values_mixed_hit_miss/10k
storage/api/{in_memory,sqlite_tempfile,rocksdb_tempdir}/get_values_duplicate_keys/100
storage/api/{in_memory,sqlite_tempfile,rocksdb_tempdir}/get_values_duplicate_keys/1k
storage/api/{in_memory,sqlite_tempfile,rocksdb_tempdir}/get_values_duplicate_keys/10k
storage/api/{in_memory,sqlite_tempfile,rocksdb_tempdir}/get_values_multi_namespace/10k
storage/api/{in_memory,sqlite_tempfile,rocksdb_tempdir}/scan_keys_prefix/100
storage/api/{in_memory,sqlite_tempfile,rocksdb_tempdir}/scan_keys_prefix/1k
storage/api/{in_memory,sqlite_tempfile,rocksdb_tempdir}/scan_keys_prefix/10k
storage/api/{in_memory,sqlite_tempfile,rocksdb_tempdir}/scan_keys_after_pages/10k
storage/api/{in_memory,sqlite_tempfile,rocksdb_tempdir}/scan_keys_small_limit_of_large_range/100_of_10k
storage/api/{in_memory,sqlite_tempfile,rocksdb_tempdir}/scan_keys_empty_range/10k
storage/api/{in_memory,sqlite_tempfile,rocksdb_tempdir}/scan_keys_prefix_selectivity_1pct/10k
storage/api/{in_memory,sqlite_tempfile,rocksdb_tempdir}/scan_keys_prefix_selectivity_10pct/10k
storage/api/{in_memory,sqlite_tempfile,rocksdb_tempdir}/scan_keys_prefix_selectivity_100pct/10k
storage/api/{in_memory,sqlite_tempfile,rocksdb_tempdir}/transaction_commit_empty
```

Additional high-signal variants are registered for:

- batch sizes: `1`, `10`, `100`, `1k`, `10k`
- state payload sizes: `small/10k`, `1k/10k`, `16k/1k`, `128k/100`
- binary payload sizes: `small/10k`, `1k/10k`, `16k/1k`, `128k/100`
- changelog shared JSON payloads: shared snapshot, shared metadata, and shared
  snapshot+metadata workloads for measuring JsonStore writer dedupe
- key distribution: `sequential_keys`, `random_keys`
- scan selectivity: `1pct`, `10pct`, `100pct`
- projection-aware scans: file-selective header scans that omit
  `snapshot_content`, including `1KiB` out-of-line snapshot variants
- point-read scaling: `100` point reads over `1k`, `10k`, and `100k` rows
- update shape: update/overwrite `10pct`, update/overwrite all, append or insert new keys
- prolly-style tracked-state cases: single-row update in `10k`/`100k` roots,
  single-row append in `10k`/`100k` roots, tombstone/delete writes, and root
  diff traversal for equal/update/delete shapes
- partial snapshot-content update baselines: one logical field changed in a
  `1KiB` snapshot over `100k` rows and a `16KiB` snapshot over `10k` rows
- Binary CAS dedupe: unique payloads, all duplicate payloads, half duplicate payloads

The ignored `storage_accounting` test prints deterministic byte/chunk tables
for the tracked-state physical format: primary tree, header-covering by-file
tree, and snapshot CAS.
