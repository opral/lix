# Tracked-state CRUD benchmark

This benchmark compares the same deterministic CRUD operations through Lix,
standalone SQLite, and optionally PostgreSQL. Lix keeps its normal tracked
commit history enabled. The conventional database controls retain only current
state; that asymmetry is intentional for measuring the cost of Lix's always-on
history and must be disclosed with published results.

This is a neutral direct-driver benchmark, not a database wire protocol and not
an official Sysbench result. Sysbench provides database drivers and Lua
workloads; it does not define a "Sysbench protocol." The operation names here
are suitable prerequisites for a later Sysbench-derived mixed-workload runner.

Run Lix and SQLite with:

```sh
cargo bench -p lix_engine_benchmarks --bench tracked_state_crud \
  --features storage-benches
```

Add PostgreSQL by enabling the feature and supplying a dedicated benchmark
database. Each sample uses a connection-local temporary table, so the runner
does not modify persistent schemas:

```sh
LIX_TRACKED_STATE_CRUD_POSTGRES_URL='postgresql://bench:bench@127.0.0.1/bench' \
  cargo bench -p lix_engine_benchmarks --bench tracked_state_crud \
  --features storage-benches,postgres
```

The common matrix covers bulk insert, full scan, one- and ten-key reads, bulk
and point updates, and bulk and point deletes at 1,000 and 10,000 rows. Fixture
creation, connection establishment, table creation, seeding, and statement
preparation are outside each timed sample. Query execution and complete result
transfer into the benchmark process are inside it.

For publishable comparisons, pin engine versions and configuration, use the
same machine and durability policy, retain raw Criterion output, and report
median plus tail latency rather than only relative multipliers. Embedded
SQLite and Lix avoid a network hop while PostgreSQL uses its normal client/server
boundary; state that boundary explicitly in the post.

## Sysbench-derived publication runner

The separate `sysbench_crud` example implements the point-CRUD event shapes
from Sysbench 1.0.20 with one shared generator and timing loop for Lix/SlateDB,
SQLite, and PostgreSQL. Build it without unrelated default storage adapters:

```sh
cargo build -p lix_engine_benchmarks --release \
  --example sysbench_crud --no-default-features \
  --features sqlite,slatedb,postgres
```

Run a fixed-event qualification point directly:

```sh
target/release/examples/sysbench_crud \
  --engine lix-slatedb \
  --workload point-select \
  --table-size 1000 \
  --clients 2 \
  --events-per-client 20 \
  --seed 42
```

The orchestrator builds release mode, runs every CRUD shape against all three
engines in counterbalanced order, records host and revision metadata, validates
each result, and retains the raw JSON:

```sh
node packages/engine-benchmarks/scripts/run-sysbench-crud.mjs \
  --qualify \
  --postgres-url 'postgresql://bench:bench@127.0.0.1:5432/bench'

node packages/engine-benchmarks/scripts/run-sysbench-crud.mjs \
  --postgres-url 'postgresql://bench:bench@127.0.0.1:5432/bench' \
  --output-dir target/sysbench-results/publication-1
```

The publication profile is intentionally long: five workloads, two table
sizes, three client counts, three engines, and five independent repetitions,
each with a 15-second warmup and 60-second measurement. Use `--sizes`,
`--clients`, `--repetitions`, `--warmup-seconds`, and `--time-seconds` only for
explicitly labeled exploratory profiles. Never mix those results into the
fixed publication cohort.

The exact claim and fairness boundary are defined in
`packages/engine-benchmarks/SYSBENCH_PROFILE.md`.
