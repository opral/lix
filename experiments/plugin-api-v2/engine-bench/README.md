# Full-engine CSV and JSON baseline

This harness measures the production v2 CSV path and the current v1 JSON
mechanism through the real Lix SQL file path on RocksDB and cached SlateDB. It
complements the isolated mechanism benchmark one directory above: the measured
operation includes engine state resolution, storage reads, Component Model
lifting/lowering, real CSV or JSON plugin execution, and transactional writes.
Set `LIX_PROFILE_FORMAT=csv` or `json`; omitting it selects CSV.

SQLite is intentionally excluded from every performance result.

## Fixture and session semantics

The deterministic CSV fixture contains 220,000 rows and 10,680,000 bytes. A
timed edit changes one field in the middle row. The deterministic JSON fixture
is an exactly 10,000,000-byte flat object with 220,000 properties. Its values
are each 24 or 25 bytes rather than concentrating the remaining bytes in
a synthetic padding property. A timed JSON edit changes exactly one byte in
the value of the middle property, `property_110000`, without changing file
length. Both lanes alternate between the original and edited file so every
round is a real one-row or one-property transition.

Before timing an edit series, the harness performs exactly once:

```sql
SELECT data FROM lix_file WHERE path = $1
```

That point read grants the session deletion authority for the bytes it actually
received. Plugin compilation, the point read, and construction of the edited
blobs stay outside every write timer. Each timed write is the ordinary SQL
upsert used by clients.

The two reported storage lanes are:

- `rocksdb-fs`: `LocalFilesystem`, including its internal RocksDB engine,
  materialized working files, watcher/synchronization path, and second Wasm
  engine used by filesystem sync.
- `slatedb-cached`: SlateDB over a local object store with the Lixray
  per-workspace policy: 64 MiB disk cache, 4 MiB block cache, and 1 MiB metadata
  cache. This controls SlateDB algorithms/cache budgets but does not reproduce
  remote object-store latency, misses, retries, or tails; production Lixray
  claims require a separate remote-store run.

The harness also accepts raw `rocksdb` and cacheless `slatedb` as diagnostic
controls, but they are not headline results. In particular, raw `rocksdb`
omits production filesystem synchronization and is not a substitute for
`rocksdb-fs`.

Latency runs do not use the I/O-counting wrapper. Logical I/O is collected in
separate single-round runs with `LIX_PROFILE_IO_STATS=1`.

Every measured Component v2 edit (CSV or the JSON v2 arm) resets and prints the
engine's aggregate transition counters as `plugin_v2_counters`. The harness
fails the run unless the warm single-row or single-property edit materializes
no full semantic state, requests and returns fewer than 64 change payloads,
persists exactly one semantic change, hits the private actor document once,
skips the shared renderer for an uncontended current-base edit, and performs
no full document reparse, full renderer invocation, or filesystem-sync full
render. These are work invariants in
addition to the latency gate; a faster sample cannot hide a regression to
document-sized work. CSV additionally retains its stricter 64 MiB efficiency
invariant inside the production sandbox. The JSON paired diagnostic checks
against its explicitly configured equal-arm ceiling and reports the observed
high-water. For provenance-backed
edits, `host_full_diff_bytes_compared` and
`host_full_content_classification_bytes` must both be zero: the host validates
the localized splice and its UTF-8 boundary window instead of rescanning the
complete file before entering the component.

Set `LIX_PROFILE_WARMUPS` to run unreported updates before the measured
`LIX_PROFILE_ROUNDS`. Edit warmups and samples use one continuous alternating
original/edited sequence, so the first measured update remains a real edit;
warmup storage I/O and transition counters are reset before measurement.

## Production cap and diagnostic timing

The production per-component linear-memory ceiling is 256 MiB. This remains a
hard sandbox bound; it is sized from the measured 10 MiB recursive JSON working
set rather than from a Wasm specification limit. The benchmark feature exposes
the SDK runtime only to this profiling target, whose wrapper can replace the
incoming memory limit when `LIX_PROFILE_WASM_MEMORY_MIB` is set. This is a
diagnostic capacity knob, not a production configuration or recommendation.

Every timing collected with that knob prints both the diagnostic ceiling and
the 256 MiB production default. Omitting the variable exercises the production
policy. For `rocksdb-fs`, the benchmark-only ceiling is applied to both the
outer SQL engine and the engine owned by `LocalFilesystem` sync.

## Build and run

From the repository root:

```sh
cargo build --release -p lix_sdk \
  --bench profile_plugin_large_file \
  --features default_wasm_runtime,local_filesystem,__profile_wasm_memory
```

Fixture and plugin-packaging invariants can be checked independently:

```sh
cargo test -p lix_sdk \
  --test profile_plugin_large_file_fixture \
  --features default_wasm_runtime,local_filesystem,__profile_wasm_memory
```

Resolve the executable printed under `target/release/deps`, then create a fresh
case directory. The commands used for a reproducible 220k CSV run have this
shape (the explicit format is optional because CSV is the default):

```sh
env LIX_PROFILE_INITIAL_ROWS=220000 \
  LIX_PROFILE_FORMAT=csv \
  <profile-binary> rocksdb-fs setup <rocks-case>

env LIX_PROFILE_INITIAL_ROWS=220000 \
  LIX_PROFILE_FORMAT=csv \
  LIX_PROFILE_WARMUPS=5 \
  LIX_PROFILE_ROUNDS=20 \
  <profile-binary> rocksdb-fs edit <rocks-case>

env LIX_PROFILE_INITIAL_ROWS=220000 \
  LIX_PROFILE_FORMAT=csv \
  LIX_PROFILE_ROUNDS=11 \
  <profile-binary> rocksdb-fs render <rocks-case>

env LIX_PROFILE_INITIAL_ROWS=220000 \
  LIX_PROFILE_FORMAT=csv \
  LIX_PROFILE_IO_STATS=1 \
  <profile-binary> rocksdb-fs edit <rocks-case>
```

The JSON fixture has a fixed byte and property count, so it does not use
`LIX_PROFILE_INITIAL_ROWS`:

```sh
env LIX_PROFILE_FORMAT=json \
  LIX_PROFILE_WASM_MEMORY_MIB=256 \
  <profile-binary> rocksdb-fs setup <json-rocks-case>

env LIX_PROFILE_FORMAT=json \
  LIX_PROFILE_WASM_MEMORY_MIB=256 \
  LIX_PROFILE_WARMUPS=5 \
  LIX_PROFILE_ROUNDS=20 \
  <profile-binary> rocksdb-fs edit <json-rocks-case>

env LIX_PROFILE_FORMAT=json \
  LIX_PROFILE_WASM_MEMORY_MIB=256 \
  LIX_PROFILE_ROUNDS=11 \
  <profile-binary> rocksdb-fs render <json-rocks-case>

env LIX_PROFILE_FORMAT=json \
  LIX_PROFILE_WASM_MEMORY_MIB=256 \
  LIX_PROFILE_IO_STATS=1 \
  <profile-binary> rocksdb-fs edit <json-rocks-case>
```

For each format, replace `rocksdb-fs` with `slatedb-cached` and use a separate
fresh case for the cached SlateDB lane. This is the production algorithm/cache
configuration over a local object store; remote object-store latency, retries,
and tails remain a separate production-hardening measurement. Do not use the
raw `rocksdb` or cacheless `slatedb` controls as headline results. Wrap a
command in `/usr/bin/time -l` on macOS to collect maximum resident set size
without changing the timed interval printed by the harness.

The harness also prints recursive backend bytes. For `rocksdb-fs` it separates
the internal database, selected materialized file, and plugin archive. For
cached SlateDB it reports object-store and cache directories separately so
cache duplication is not mistaken for durable storage amplification.

## Candidate-only JSON diagnostic

`run_json_diagnostic.py` reproducibly exercises the existing exact
10,000,000-byte, 220,000-property JSON fixture on one candidate binary. It
runs `rocksdb-fs` and `slatedb-cached`; for each backend it creates a pristine
setup template, then gives the one-property edit and exact render separate
template copies and fresh processes. Each measured process performs five
unreported warmups followed by exactly twenty samples under the 256 MiB
diagnostic Wasm ceiling:

```sh
python experiments/plugin-api-v2/engine-bench/run_json_diagnostic.py \
  --candidate <candidate-profile-binary> \
  --run-dir <new-dedicated-json-run-directory>
```

The runner writes combined raw process logs below `<run-dir>/logs` and
atomically updates `<run-dir>/json-diagnostic.json`. The run directory must be
new, empty, or carry the runner's exact ownership marker; a marked directory
can be reused, in which case only the runner-owned templates, cases, and logs
are replaced.

This JSON result is explicitly a candidate-only, Component Model v1 mechanism
diagnostic. It is **not** a PR2 v2 acceptance result and is not analyzed by
either paired gate below.

## JSON v1-v2 paired gate

`run_json_paired.py` compares the real Component v1 and Component v2 JSON
plugins through one benchmark executable. The runner selects the embedded arm
with `LIX_PROFILE_JSON_API=v1|v2`; this avoids treating build-to-build noise as
an API effect. For each backend and metric, it alternates arm order across 12
blocks and gives every fresh process a copy of that API's pristine setup
template. The default design uses five warmups, twenty measured samples per
arm/block, a common 256 MiB diagnostic ceiling, and 10,000 bootstrap draws:

```sh
python experiments/plugin-api-v2/engine-bench/run_json_paired.py \
  --benchmark <profile-binary-containing-both-json-plugins> \
  --run-dir <new-dedicated-json-paired-directory>

python experiments/plugin-api-v2/engine-bench/json_paired_gate.py \
  <new-dedicated-json-paired-directory>/json-v1-v2-paired-raw.json \
  --output <json-paired-gate-result.json>
```

The raw artifact records the executable, runner, and both manifests with byte
counts and SHA-256 hashes, plus exact sample arrays, per-round v2 hot-path
counters, counterbalanced execution order, and hashed raw logs. The analyzer
requires the exact 10,000,000-byte, 220,000-property fixture. On each backend,
the one-sided 95% upper v2/v1 ratio must be strictly below 0.80 for edit p50
and p95. Exact-render guardrails are at most 1.05 for p50 and 1.10 for p95.
The v2 edit counter gate also rejects source reads for the inline one-byte
splice, host full-blob diff/classification, full semantic materialization,
full reparses/renders, missing single-entity persistence/cache hits, or guest
memory above the configured campaign ceiling.

A dependency-free E2E smoke checks packaging, execution, parsing, and analysis
without claiming acceptance:

```sh
python experiments/plugin-api-v2/engine-bench/run_json_paired.py \
  --benchmark <profile-binary-containing-both-json-plugins> \
  --run-dir <new-dedicated-json-smoke-directory> \
  --fast-smoke

python experiments/plugin-api-v2/engine-bench/json_paired_gate.py \
  <new-dedicated-json-smoke-directory>/json-v1-v2-paired-raw.json \
  --allow-smoke
```

The smoke analyzer still exits nonzero when its latency guardrails or hot-path
counters fail; a successful smoke only means the reduced checks passed, never
that the candidate met the acceptance design.

The paired runner keeps the ceiling configurable so both arms always receive
the same bound. Wasm itself does not prescribe a 64 MiB limit.

## CSV paired acceptance run

`run_paired.py` executes the preregistered minimum of 12 fresh-process blocks
per backend. Each block contains exactly five unreported warmups and twenty
measured observations for both the immutable baseline and candidate; arm order
alternates exactly. It records both the localized-update acceptance cells and
separate exact-render regression-guard cells. Point it only at binaries built
with the same toolchain, base commit, features, and diagnostic memory ceiling:

```sh
python experiments/plugin-api-v2/engine-bench/run_paired.py \
  --baseline <immutable-control-binary> \
  --candidate <candidate-binary> \
  --run-dir <new-dedicated-run-directory> \
  --output <raw-paired-samples.json>

python experiments/plugin-api-v2/engine-bench/paired_gate.py \
  <raw-paired-samples.json> \
  --output <paired-gate-result.json>
```

The analyzer uses the fixed seed `0x4c495832` and 10,000 hierarchical cluster
bootstrap draws over log candidate/control ratios. For each backend, the
one-sided 95% upper bound must be strictly below 0.80 for edit p50 and p95.
Exact-render upper bounds must be at most 1.05 for p50 and 1.10 for p95. A
non-passing analyzer exits nonzero without discarding either raw samples or the
result artifact.

## Recorded exact-e1 result

See
[`full-engine-v1-baseline-e1a57ec3.md`](../../../perf-results/plugin-api-v2/full-engine-v1-baseline-e1a57ec3.md)
for the immutable commit, toolchain, raw samples, RSS, storage, logical I/O,
Samply attribution, reproduction commands, and interpretation. Both clean
exact-main profiles are stored beside the report and can be opened directly:

```sh
samply load \
  perf-results/plugin-api-v2/full-engine-v1-rocksdb-fs-edit-220k-e1a57ec3.json.gz
samply load \
  perf-results/plugin-api-v2/full-engine-v1-slatedb-cached-edit-220k-e1a57ec3.json.gz
```

Use `perf-results/plugin-api-v2/analyze_samply.py --binary <profile-binary>
<profile>` for an idle-filtered native summary. The report explains why the
async marker is not used as a sample filter in this build.

The `5ffab346`, `c789a2b1`, and `66ad14da` reports/profiles remain beside the
exact-main artifacts as historical comparisons and are not overwritten.
