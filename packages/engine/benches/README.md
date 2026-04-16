# Engine Microbenches

`packages/engine/benches/*` is the engine microbench layer.

These benches exist for regression testing and diagnosis:

- they isolate one bottleneck family at a time
- they should stay fast enough to run regularly
- they should verify a concrete correctness invariant, not just emit timing

Current families:

- `file_update_hot`
- `file_update_history`
- `file_insert_history`
- `state_insert_bulk`
- `state_commit_bulk`
- `state_update_large_doc`
- `state_insert_history`
- `live_state_rebuild`
- `version_create`
- `version_storage`
- `version_diff`
- `version_merge`
- `commit_graph_walk`

## Layering

These microbenches are intentionally distinct from the dedicated `10k` benchmark
in:

- [benchmarks/10k-entities/README.md](/Users/samuel/git-repos/lix-2/benchmarks/10k-entities/README.md)

That benchmark is the higher-level integration target. It exists to answer the
product-facing question:

- can Lix handle the `10k` entity scenario fast enough end to end?

The microbench suite exists to explain that answer:

- if `10k` regresses, which engine phase moved?
- was it bulk state insert, commit cost, history sensitivity, merge cost, or
  commit-graph traversal?

So:

- do not move `benchmarks/10k-entities` into `packages/engine/benches/*`
- do not duplicate the whole `10k` scenario here
- do use these microbenches to localize regressions seen in the `10k` benchmark

## Usage

Run the whole engine microbench suite:

```bash
cargo bench -p lix_engine
```

Run one family:

```bash
cargo bench -p lix_engine --bench state_insert_bulk
cargo bench -p lix_engine --bench live_state_rebuild
cargo bench -p lix_engine --bench version_merge
cargo bench -p lix_engine --bench commit_graph_walk
```

Compile-only verification:

```bash
cargo bench -p lix_engine --no-run
```
