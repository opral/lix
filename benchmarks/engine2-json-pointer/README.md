# Engine2 JSON Pointer Benchmark

This benchmark exercises engine2 end to end on a fresh on-disk SQLite-backed KV
store.

The first case measures direct insertion of `json_pointer` semantic rows through
`lix_state`:

- initialize engine2 storage
- open the generated main version
- register `packages/plugin-json-v2/schema/json_pointer.json`
- insert `N` JSON pointer rows in chunked SQL statements
- verify the committed row count through the normal SQL surface

## Usage

```bash
cargo run --release -p engine2_json_pointer_benchmark -- \
  --rows 10000 \
  --warmups 1 \
  --iterations 5 \
  --output-dir artifact/benchmarks/engine2-json-pointer
```

Fast CI smoke:

```bash
cargo run --release -p engine2_json_pointer_benchmark -- \
  --rows 10000 \
  --warmups 0 \
  --iterations 1 \
  --output-dir artifact/benchmarks/engine2-json-pointer
```
