# 10k Entities Benchmark

This benchmark compares two engine paths for the same logical JSON document:

1. File write: insert one `.json` blob with `10_000` props through `lix_file`
2. Direct entity writes: insert `10_000` `json_pointer` rows directly through `lix_state`

The goal is to separate:

- file/plugin detect overhead
- direct semantic row write overhead

Both cases use the real current engine on a fresh file-backed SQLite database.

## Case 1: File Write JSON With 10k Props

Timed section:

- begin a buffered write transaction
- run `INSERT INTO lix_file (id, path, data)`
- commit the transaction

This case includes:

- JSON plugin `detect-changes`
- semantic row commit
- live-state rebuild
- file cache/materialization refresh

## Case 2: Direct Entity Writes 10k

Outside the timer:

- insert an empty `{}` JSON file through `lix_file`

Timed section:

- begin a buffered write transaction
- run one root-row update plus chunked `INSERT INTO lix_state (...) VALUES (...)` statements until all `10_000` property rows are written
- commit the transaction

This case excludes file-to-entity detection, but still includes:

- direct semantic row commit
- live-state rebuild
- file cache/materialization refresh

The benchmark treats committed `json_pointer` row count as the hard invariant for
this case and records the final `lix_file` payload match as an observation.

## Usage

```bash
cargo run --release -p ten_k_entities_benchmark -- \
  --props 10000 \
  --warmups 2 \
  --iterations 10 \
  --output-dir artifact/benchmarks/10k-entities
```

The benchmark writes:

- `artifact/benchmarks/10k-entities/report.json`
- `artifact/benchmarks/10k-entities/report.md`

## Verification

Each case verifies:

- committed `json_pointer` row count in `lix_state_by_version`
- file-write case: final `lix_file` JSON must match the expected payload
- direct-write case: final `lix_file` JSON match is recorded in the report

## Notes

- Warmups absorb first-use wasm/component initialization costs.
- The direct-write case times `10_000` property inserts plus one root-row update so the JSON semantic root stays in sync with the property rows.
- The report includes per-case `write`, `commit`, and `total` timing summaries plus a comparison table.
