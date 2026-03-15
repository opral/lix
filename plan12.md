# Plan 12: Replace Commit Closure Writes With a Graph Index

## Goal

Refactor the engine so tracked commits no longer write `lix_internal_commit_ancestry` rows proportional to history depth.

Target write shape:

- write commit metadata
- write direct parent edges
- update compact graph index metadata in `O(1)` or small amortized constant work
- do not copy transitive ancestry on every commit

The only public behavior that must remain correct is:

- `lix_state_history`
- `lix_file_history`

Everything else is an internal implementation detail.

This plan is first-principles only:

- no backward compatibility
- no dual architecture
- no closure-table fallback
- no requirement to preserve `lix_internal_commit_ancestry`

## Problem

Today every tracked commit appends full transitive ancestry in `lix_internal_commit_ancestry`.

That makes writes scale with parent history depth instead of changed content size.

This is visible in current write behavior:

- fresh repo: same logical update is fast
- replayed repo with long history: same logical update is materially slower

The write path is paying for graph closure maintenance.

## Final Architecture

### 1. Remove closure-table ancestry from the write path

Delete:

- `lix_internal_commit_ancestry` as a write-maintained table
- per-commit ancestry copy logic in create-commit runtime assembly
- bootstrap ancestry seeding logic

A new commit must write only:

- commit row
- parent edge rows
- compact graph index rows/metadata

### 2. Introduce an internal commit graph index

The graph index remains SQL-backed via `lix_internal_*` tables.

Proposed internal tables:

- `lix_internal_commit_graph_node`
  - `commit_id`
  - `generation`
  - `segment_id`
  - `offset_in_segment`
- `lix_internal_commit_graph_segment`
  - `segment_id`
  - `head_commit_id`
  - `tail_commit_id`
  - `base_generation`
  - `tip_generation`
  - `commit_count`
- `lix_internal_commit_graph_segment_edge`
  - `child_segment_id`
  - `parent_segment_id`
  - `child_commit_id`
  - `parent_commit_id`

### 3. Make the graph index create-commit-friendly

For each new commit:

- compute `generation = max(parent.generation) + 1`
- if the new commit continues a linear segment, append it to that segment
- if the new commit branches or merges, create a new segment and connect segment edges

This keeps write work bounded by parent count and local segment maintenance.

### 4. Rebuild history APIs on graph traversal, not closure rows

`lix_state_history` and `lix_file_history` must derive reachable commits from the graph index, not from `lix_internal_commit_ancestry`.

Internal execution model:

1. resolve root commit(s)
2. traverse graph segments/edges to derive reachable commits
3. order reachable commits by graph order / generation
4. feed reachable commits into existing state-history/file-history derivation

The public contract is:

- correct reachable history
- stable newest-to-oldest ordering
- correct state/file history rows

The public contract is not:

- expose exact closure-table depth semantics
- preserve any internal ancestry table shape

## Query Model

### Reachable commits

History queries must use graph traversal CTEs over graph-index tables.

The traversal should:

- recurse over segment edges instead of per-commit parent edges whenever possible
- use `generation` for pruning
- expand segment members only when needed

### Ordering

History views must be ordered by graph order suitable for user-facing history.

Preferred ordering basis:

- descending `generation`
- then descending commit creation or stable segment-local order

The engine may add a dedicated topo/order column if needed for deterministic ordering.

### Slicing

History slicing should be defined in terms of ordered reachable history, not exact ancestor-hop count.

If the public history APIs currently rely on exact hop depth internally, that internal behavior should be replaced with:

- reachable-commit ordering
- ordinal slicing over that ordered history

## Impacted Engine Areas

### Delete / replace

- `packages/engine/src/state/commit/runtime.rs`
  - remove `append_commit_ancestry_statements(...)` from create-commit runtime assembly
- `packages/engine/src/init/seed.rs`
  - remove `seed_commit_ancestry(...)`
- `packages/engine/src/init/mod.rs`
  - remove closure-table schema and indexes

### Rewrite ancestry consumers

- `packages/engine/src/state/commit/state_source.rs`
- `packages/engine/src/state/timeline.rs`
- `packages/engine/src/filesystem/live_projection.rs`
- `packages/engine/src/sql/public/planner/backend/lowerer.rs`
- `packages/engine/src/plugin/runtime.rs`

These must stop querying `lix_internal_commit_ancestry` and instead use graph-index traversal helpers.

### Add graph-index ownership

Introduce a dedicated internal graph module, e.g.:

- `packages/engine/src/state/commit/graph_index.rs`

It owns:

- graph node writes
- segment append/split rules
- traversal SQL builders
- root reachability helpers for history queries

## Expected Performance Outcome

### Writes

Tracked write cost stops scaling with history depth.

Expected result:

- same logical tracked write on a long-history repo should be near the cost of the same write on a fresh repo
- commit writes become bounded by content/state work, not ancestry closure growth

### History queries

History queries become more graph-aware and somewhat more complex.

Expected result:

- `lix_state_history` and `lix_file_history` remain correct
- read cost may shift from precomputed closure joins to graph traversal over compact index structures
- long linear histories should still perform well if segments are large and stable

## Tradeoffs

### Benefits

- `O(1)` or small amortized constant-time graph maintenance per commit
- much smaller graph storage than full closure rows
- history depth no longer bloats ordinary tracked writes

### Costs

- internal graph logic becomes more sophisticated
- history queries must traverse/expand graph structure
- exact closure-table depth is no longer a primitive

### Chosen priority

Prioritize:

- write scalability
- correctness of `lix_state_history` / `lix_file_history`
- stable ordered history semantics

Do not prioritize:

- preserving closure-table internals
- preserving exact ancestor-hop depth as an internal primitive

## Validation

### Write-path validation

Measure the same logical tracked update against:

- a fresh `.lix`
- a replayed long-history `.lix`

Success criterion:

- the historical write is no longer materially slower because of commit depth alone

### History validation

Compare `lix_state_history` and `lix_file_history` before/after on representative roots:

- linear history
- branch history
- merge history
- large replayed repos

Success criterion:

- same user-visible rows
- same ordering contract
- no dependence on closure-table depth

## Progress Log

- Created plan.
- Established pre-refactor baseline with the 3-file tracked-update harness:
  - fresh avg `20.640 ms`, median `20.110 ms`
  - history avg `51.547 ms`, median `49.932 ms`
- Replaced write-time closure maintenance with graph-node maintenance:
  - added `lix_internal_commit_graph_node`
  - removed create-commit closure-row writes
  - removed bootstrap closure seeding
- Rebuilt public history consumers on recursive edge traversal:
  - `lix_state_history`
  - `lix_file_history`
  - filesystem/plugin history helpers
  - working-changes ancestry checks
- Removed remaining engine references to `lix_internal_commit_ancestry`, including schema creation and lowerer/runtime test expectations.
- Validation passed:
  - `cargo check -p lix_engine`
  - `cargo test -p lix_engine --test state_history_view --test file_history_view -- --nocapture`
  - `cargo test -p lix_engine --lib -- --nocapture`
- Built a fresh 897-commit paraglide replay DB with current code and reran the write-path harness.
  - history DB shape:
    - `lix_internal_live_v1_lix_commit = 890`
    - `lix_internal_live_v1_lix_commit_edge = 890`
    - `lix_internal_commit_graph_node = 890`
    - `max(generation) = 889`
  - post-refactor harness result after warmup:
    - fresh avg `20.034 ms`, median `19.880 ms`
    - history avg `46.111 ms`, median `33.802 ms`
- Result:
  - the direct history-depth closure-write penalty improved materially versus the pre-refactor baseline
  - fresh remained essentially unchanged
  - history-loaded writes are still slower than fresh, so closure maintenance was not the only remaining cost
