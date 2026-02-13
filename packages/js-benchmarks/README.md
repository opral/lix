# js-benchmarks

Cross-package benchmark suite for comparing old vs new SDK performance.

- `@lix-js/sdk` (old SDK)
- `js-sdk` (new SDK)

## Run

```bash
pnpm -C packages/js-benchmarks run bench:state
```

State insert quick run:

```bash
pnpm -C packages/js-benchmarks run bench:state:quick
```

JSON plugin insert run:

```bash
pnpm -C packages/js-benchmarks run bench:json
```

JSON plugin quick run:

```bash
pnpm -C packages/js-benchmarks run bench:json:quick
```

JSON plugin run without rebuilding packages:

```bash
pnpm -C packages/js-benchmarks run bench:json:run
```

Force ANSI colors (useful for screenshots):

```bash
BENCH_FORCE_COLOR=1 pnpm -C packages/js-benchmarks run bench:json:quick
```

## Output

Results are written to:

- `packages/js-benchmarks/results/state-insert.bench.json`
- `packages/js-benchmarks/results/json-insert.bench.json`

## Notes

The JSON benchmark installs:

- legacy JS plugin (`@lix-js/plugin-json`) in old SDK
- wasm JSON plugin (`plugin-json-v2`) in new `js-sdk`

If new runtime plugin execution is unavailable, the benchmark prints a warning and reports `plugin rows/file (new): 0.0`.

Set `BENCH_REQUIRE_PLUGIN_EXEC=1` to fail the run when plugin rows are zero.
