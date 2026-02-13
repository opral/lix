# Lix Engine Optimization Log

## 2026-02-13 - Baseline (quick benchmarks)

### Context
- Goal: improve new engine write performance, especially plugin-driven file writes.
- Bench suite: `packages/js-benchmarks`.
- RFC reference: `rfcs/001-preprocess-writes/index.md`.

### Commands
- `pnpm -C packages/js-benchmarks run bench:state:quick`
- `pnpm -C packages/js-benchmarks run bench:json:quick`

### Results
- State inserts (new vs old):
  - Single row: `5.07ms` vs `17.74ms` (`3.50x` faster)
  - 10-row chunk: `18.86ms` vs `45.42ms` (`2.41x` faster)
  - 100-row chunk: `86.25ms` vs `341.67ms` (`3.96x` faster)
- JSON plugin file insert (120 leaves):
  - New js-sdk: `3498.20ms`
  - Old @lix-js/sdk: `683.51ms`
  - New/old speed: `0.20x` (new is ~`5.12x` slower)
  - Plugin execution check:
    - old rows/file: `122.0`
    - new rows/file: `125.0`

### Initial conclusions
- Core preprocessed state writes are already faster than the old SDK.
- The regression is localized in plugin execution + resulting write path for file ingestion.

### Next hypothesis
- New engine likely pays high per-row overhead in plugin output insertion (many small inserts, repeated parse/prepare/validation, or excessive JS<->WASM crossings).
- Focus next on the file insert -> plugin detect_changes -> state write pipeline and measure where time is spent.

## 2026-02-13 - Iteration log and outcomes

### Iteration 1 - isolate plugin-path cost

#### Changes
- Added JSON breakdown benchmark:
  - `packages/js-benchmarks/src/json-insert-breakdown.bench.mjs`
  - scripts in `packages/js-benchmarks/package.json`
  - docs in `packages/js-benchmarks/README.md`

#### What it measured
- No plugin installed (`.json` path)
- Plugin installed but path does not match (`.txt`)
- Plugin installed and path matches (`.json`)

#### Finding
- Slow path is specifically plugin-active file writes (`.json`).
- Non-plugin and non-matching-path writes were much faster.

### Iteration 2 - backend query hotspot analysis

#### Method
- Added backend-level SQL timing trace around plugin write flow.

#### Finding
- Dominant cost came from repeated recursive path resolution through filesystem views during rewrite-time uniqueness/collision checks.
- High-frequency `WITH RECURSIVE` path resolution queries were the main bottleneck.

### Iteration 3 - rewrite-path lookup optimization

#### Changes
- `packages/engine/src/filesystem/mutation_rewrite.rs`
  - Replaced path lookups that hit `lix_file_by_version` / `lix_directory_by_version` recursive views with direct lookups against materialized descriptor state tables.
  - Directory lookup now walks path segments using `(parent_id, name)`.
  - File lookup now resolves by `(directory_id, name, extension)`.
  - Collision checks now use these direct helpers.
- `packages/engine/src/schema_registry.rs`
  - Added materialized descriptor indexes:
    - file descriptor index on `(version_id, directory_id, name, extension)` extracted from `snapshot_content`
    - directory descriptor index on `(version_id, parent_id, name)` extracted from `snapshot_content`
  - Implemented dialect-aware expressions for sqlite/postgres JSON extraction.

#### Result
- Major drop in plugin `.json` write latency.

### Iteration 4 - correctness regression and fix

#### Regression found
- `file_materialization` tests failed for path-only plugin switch cache behavior.
- Symptom: cache row was reintroduced after:
  1. deleting cache row
  2. doing `UPDATE lix_file SET path = '/...txt'` on a previously `.json` file

#### Root cause
- Read-materialization detection treated `UPDATE lix_file...` target table as a read source.
- That pre-materialized cache before update side-effects ran, reintroducing non-authoritative data.

#### Fix
- `packages/engine/src/engine.rs`
  - In `statement_reads_table_name`, removed update target-table relation from read detection.
  - Keep detecting real read contexts (`FROM`, subqueries, expressions) only.
  - Updated unit tests to assert update-target-only statements do **not** trigger read-materialization scope.

### Latest benchmark loop (after fixes)

#### Commands
- `pnpm -C packages/js-benchmarks run bench:state:quick`
- `pnpm -C packages/js-benchmarks run bench:json:quick`
- `pnpm -C packages/js-benchmarks run bench:json:breakdown:quick`

#### Results
- State inserts (new vs old):
  - Single row: `5.17ms` vs `18.05ms` (`3.49x` faster)
  - 10-row chunk: `18.89ms` vs `45.01ms` (`2.38x` faster)
  - 100-row chunk: `90.40ms` vs `405.50ms` (`4.49x` faster)
- JSON plugin insert (120 leaves):
  - New js-sdk: `373.81ms`
  - Old @lix-js/sdk: `734.24ms`
  - New/old speed: `1.96x` (new is faster)
- JSON breakdown (new js-sdk only):
  - No plugin (`.json` path): `2.83ms`
  - Plugin installed (`.txt` path): `17.29ms`
  - Plugin installed (`.json` path): `53.68ms`

### Validation
- `cargo test -p lix_engine --test filesystem_view --quiet`: passed (`102/102`)
- `cargo test -p lix_engine --test file_materialization --quiet`: passed (`64/64`)

### Current status
- Achieved and exceeded parity with old `@lix-js/sdk` for JSON plugin insert benchmark (`~1.96x` faster).
- State-write benchmarks remain strongly faster than old SDK.
- Main performance gain comes from removing recursive view-path lookup pressure in mutation rewrite checks.

## 2026-02-13 - Iteration 5: nested path false-positive collision regression

### Regression reproduction
- Added test:
  - `packages/engine/tests/filesystem_view.rs`
  - `file_insert_nested_path_with_missing_parent_does_not_conflict_with_same_root_filename`
- Scenario:
  - existing root file: `/readme.md`
  - insert nested file with missing parent: `/docs/readme.md`
- Expected: succeeds (parent auto-created, no path collision)
- Observed before fix: failed with false unique collision on `/docs/readme.md`.

### Root cause
- In `find_file_id_by_path`, when parsing a nested path and parent directory lookup returned `None`, the query fell back to `directory_id IS NULL`, incorrectly matching root-level files.

### Fix
- `packages/engine/src/filesystem/mutation_rewrite.rs`
  - `find_file_id_by_path` now returns `Ok(None)` immediately when a nested path’s parent directory is unresolved.
  - Root-level `directory_id IS NULL` lookup remains only for true root-level file paths.

### Validation
- `cargo test -p lix_engine --test filesystem_view file_insert_nested_path_with_missing_parent_does_not_conflict_with_same_root_filename -- --nocapture`
  - passed (`3/3`, all simulations)
- `cargo test -p lix_engine --test filesystem_view --quiet`
  - passed (`105/105`)

## 2026-02-13 - Iteration 6: review-driven regressions fixed (P1 + P2)

### P1: UPDATE target read-materialization scope

#### Reproduction tests added
- `packages/engine/src/engine.rs`
  - `regression_update_target_lix_file_with_data_predicate_requires_active_materialization_scope`
  - `regression_update_target_lix_file_by_version_with_data_predicate_requires_all_versions_scope`

#### Initial reproduction result
- Both tests failed (`scope = None`) before fix.

#### Fix
- `packages/engine/src/engine.rs`
  - `statement_reads_table_name` now treats `UPDATE` target tables as read sources only when the statement references `data` in selection/assignment expressions.
  - Added helpers:
    - `update_references_data_column`
    - `expr_references_data_column`
    - `contains_word_case_insensitive`
    - `is_word_byte`
- Kept path-only update behavior intact:
  - existing tests `file_read_materialization_scope_ignores_update_target_lix_file` and `_by_version` still pass.
  - cache-guard integration test (`file_update_path_only_plugin_switch_does_not_write_non_authoritative_cache_data`) still passes.

### P2: Postgres index-name truncation collision

#### Reproduction test added
- `packages/engine/src/schema_registry.rs`
  - `postgres_file_descriptor_index_names_do_not_truncate_to_collisions`

#### Initial reproduction result
- Failed: collision between truncated names of:
  - `idx_lix_internal_state_materialized_v1_lix_file_descriptor_version_id`
  - `idx_lix_internal_state_materialized_v1_lix_file_descriptor_version_dir_name_ext`

#### Fix
- `packages/engine/src/schema_registry.rs`
  - renamed new file descriptor composite index to `idx_lix_file_desc_v_dne_live`
  - renamed new directory descriptor composite index to `idx_lix_dir_desc_v_pn_live`

### Validation
- `cargo test -p lix_engine regression_update_target_lix_file_with_data_predicate_requires_active_materialization_scope -- --nocapture` ✅
- `cargo test -p lix_engine regression_update_target_lix_file_by_version_with_data_predicate_requires_all_versions_scope -- --nocapture` ✅
- `cargo test -p lix_engine postgres_file_descriptor_index_names_do_not_truncate_to_collisions -- --nocapture` ✅
- `cargo test -p lix_engine file_read_materialization_scope_ignores_update_target -- --nocapture` ✅
- `cargo test -p lix_engine --test file_materialization file_update_path_only_plugin_switch_does_not_write_non_authoritative_cache_data -- --nocapture` ✅

## 2026-02-13 - Iteration 7: inherited path lookup regression in rewrite-time uniqueness checks

### Regression reproduction
- Added tests:
  - `packages/engine/tests/filesystem_view.rs`
  - `directory_duplicate_inherited_path_is_rejected_in_child_version`
  - `file_duplicate_inherited_path_is_rejected_in_child_version`
- Scenario:
  - create parent version objects (`/docs/`, `/readme.md`)
  - switch to child version
  - attempt to insert same paths again in child
- Expected: duplicate path insert rejected because child inherits parent entries.
- Observed before fix: inserts succeeded in child versions (inherited rows were invisible to lookup).

### Root cause
- `find_directory_child_id` and `find_file_id_by_path` were optimized to query only materialized descriptor rows for `version_id = $1`, which excluded inherited state from ancestors.
- As a result, rewrite-time uniqueness checks operated on incomplete path visibility in child versions.

### Fix
- `packages/engine/src/filesystem/mutation_rewrite.rs`
  - switched directory/file path resolution to query `lix_state_by_version` for:
    - `schema_key = 'lix_directory_descriptor'` / `'lix_file_descriptor'`
    - `version_id = $1`
    - `snapshot_content IS NOT NULL`
    - JSON field predicates via `lix_json_text(...)`
  - kept these lookups in the read-rewrite pipeline by passing SQL through `rewrite_single_read_query_for_backend(...)` before execution.
  - retained nested-parent missing guard for file lookup (`/docs/readme.md` with missing `/docs/` returns `None`, not root fallback).

### Validation
- `cargo test -p lix_engine --test filesystem_view inherited_path_is_rejected_in_child_version -- --nocapture`
  - passed (`6/6`, sqlite + postgres + materialization)
- `cargo test -p lix_engine --test filesystem_view file_insert_nested_path_with_missing_parent_does_not_conflict_with_same_root_filename -- --nocapture`
  - passed (`3/3`)

## 2026-02-13 - Iteration 8: extra inheritance/tombstone regression sweep

### New regression tests added
- `packages/engine/tests/filesystem_view.rs`
  - `file_duplicate_inherited_nested_path_is_rejected_in_child_version`
  - `file_reinsert_path_after_child_tombstone_of_inherited_file_succeeds`
  - `file_path_update_to_inherited_path_is_rejected_in_child_version`

### Results
- `file_duplicate_inherited_nested_path_is_rejected_in_child_version`
  - passed (`3/3`)
- `file_path_update_to_inherited_path_is_rejected_in_child_version`
  - passed (`3/3`)
- `file_reinsert_path_after_child_tombstone_of_inherited_file_succeeds`
  - failed (`0/3`) across sqlite/postgres/materialization

### Confirmed bug from failing test
- In child versions, `DELETE FROM lix_file` against an inherited file id does not hide the inherited row.
- Post-delete read still returns `/readme.md` in child, and reinserting same path fails with unique collision.
- This indicates inherited delete currently does not create an effective child tombstone override for filesystem descriptors.

## 2026-02-13 - Iteration 9: inherited delete tombstone fallback implementation

### Fix summary
- Implemented effective-scope delete fallback for planned vtable deletes originating from inherited-aware views.
- When a delete only updates local materialized rows, postprocess now additionally computes effective visible rows and emits tombstone domain changes for inherited matches that were not directly updated.

### Code changes
- `packages/engine/src/sql/types.rs`
  - extended `VtableDeletePlan` with:
    - `effective_scope_fallback: bool`
    - `effective_scope_selection_sql: Option<String>`
- `packages/engine/src/sql/route.rs`
  - added origin-aware wiring to call `vtable_write::rewrite_delete_with_options(...)`.
  - enabled fallback for `lix_state_by_version` / `lix_state` delete rewrites.
  - guarded `lix_state` fallback when selection contains `inherited_from_version_id` filter (prevents over-delete for `IS NULL` local-only deletes).
- `packages/engine/src/sql/steps/vtable_write.rs`
  - added `rewrite_delete_with_options(...)`.
  - delete plan now stores fallback selection SQL when enabled.
  - `build_delete_followup_sql(...)` now accepts statement params for placeholder binding in fallback row loading.
  - added `load_effective_scope_delete_rows(...)`:
    - computes effective rows from schema materialized table across version inheritance chain.
    - binds placeholders with original params.
    - dedupes against directly tombstoned rows.
  - postprocess now emits tombstones for inherited effective rows that were selected but not directly updated.

### Regression found while fixing
- Initial fallback implementation regressed `lix_state_delete_with_inherited_null_filter_deletes_only_local_rows` by deleting inherited rows too.
- Root cause: `lix_state` rewrite strips `inherited_from_version_id IS NULL` before planning, so fallback mistakenly ran on broadened selection.
- Resolution: fallback is disabled for `lix_state` deletes when the original selection references `inherited_from_version_id`.

### Validation
- `cargo test -p lix_engine --test filesystem_view file_reinsert_path_after_child_tombstone_of_inherited_file_succeeds -- --nocapture`
  - passed (`3/3`)
- `cargo test -p lix_engine --test filesystem_view inherited_ -- --nocapture`
  - passed (`15/15`)
- `cargo test -p lix_engine --test filesystem_view --quiet`
  - passed (`120/120`)
- `cargo test -p lix_engine --test state_inheritance --quiet`
  - passed (`12/12`)
- `cargo test -p lix_engine --test state_by_version_view --quiet`
  - passed (`42/42`)
- `cargo test -p lix_engine --quiet`
  - passed (full suite)

## 2026-02-13 - Iteration 10: benchmark regression after inheritance correctness fixes

### Re-run command
- `pnpm -C packages/js-benchmarks run bench:json:quick`
- `pnpm -C packages/js-benchmarks run bench:json:run` (confirmation run, no rebuild)

### Result
- JSON plugin insert (120 leaves) is no longer near the prior `~373ms` level.
- Quick run:
  - old `@lix-js/sdk`: `687.95ms`
  - new `js-sdk`: `2063.26ms`
  - new/old: `0.33x`
- Confirmation run:
  - old `@lix-js/sdk`: `740.45ms`
  - new `js-sdk`: `3327.63ms`
  - new/old: `0.22x`

### Likely hotspot
- The inherited-path correctness changes moved rewrite-time path lookups to `lix_state_by_version` and route them through `rewrite_single_read_query_for_backend(...)`.
- These lookups run in the file insert hot path and now incur repeated SQL parse/rewrite overhead per row/path check.

## 2026-02-13 - Iteration 11: first-principles complexity analysis and step-elimination plan

### Data gathered

#### Size sweep on current code (new js-sdk only)
- Command pattern:
  - `BENCH_PROGRESS=0 BENCH_WARMUP=1 BENCH_JSON_ITER=6 BENCH_JSON_LEAF_COUNT=<N> node packages/js-benchmarks/src/json-insert-breakdown.bench.mjs`
- Means by payload size:
  - `N=10`: no-plugin `31.90ms`, plugin-nonmatch `42.39ms`, plugin-match `56.72ms`
  - `N=30`: no-plugin `28.22ms`, plugin-nonmatch `39.99ms`, plugin-match `78.36ms`
  - `N=60`: no-plugin `29.33ms`, plugin-nonmatch `41.20ms`, plugin-match `114.40ms`
  - `N=120`: no-plugin `28.98ms`, plugin-nonmatch `40.93ms`, plugin-match `236.35ms`
  - `N=240`: no-plugin `30.13ms`, plugin-nonmatch `41.43ms`, plugin-match `614.96ms`
- Interpretation:
  - no-plugin and non-matching-plugin paths are effectively constant-time in this range.
  - plugin-matching path grows much faster than linear at higher payload sizes.

#### Single-insert slope sanity check
- Command pattern:
  - `BENCH_PROGRESS=0 BENCH_WARMUP=0 BENCH_JSON_ITER=1 BENCH_JSON_LEAF_COUNT=<N> node packages/js-benchmarks/src/json-insert-breakdown.bench.mjs`
- Plugin-match means:
  - `N=10`: `54.74ms`
  - `N=120`: `82.85ms`
  - `N=240`: `135.05ms`
  - `N=480`: `298.02ms`
- Interpretation:
  - strong growth with plugin-active writes, while non-matching stays around `~44ms`.

#### Backend SQL call tracing (ad-hoc wrapper, warmup+1 measured insert)
- Observed for one measured insert:
  - backend execute calls: `70`
  - dominant calls: `4x` `SELECT entity_id FROM ... lix_state_by_version ...` lookups, each around `~43ms` (combined `~172ms`)
  - additional heavy calls:
    - `SELECT key... FROM lix_internal_plugin`: `~16ms`
    - `2x SELECT entity_id, schema_key... FROM lix_state_by_version ...`: combined `~11ms`
    - final large snapshot insert: `~11ms`
- Interpretation:
  - repeated `lix_state_by_version` queries in hot-path lookup logic are the largest single contributor.

### First-principles flow and Big-O (plugin-matching file insert)
- Let:
  - `L` = JSON leaves in input file
  - `C` = plugin-emitted domain changes (empirically grows with payload; roughly linear in `L`)
  - `D` = path segment depth (small constant for benchmark paths)
  - `A` = number of ancestor directory paths (small constant for benchmark paths)
  - `V` = length of version inheritance chain
  - `S` = number of schema tables participating in `lix_state`/`lix_state_by_version` unions
  - `N` = total rows in materialized/untracked state

Current total time per insert is approximately:
- `T_insert = T_parse + T_side_effect_scan + T_plugin_detect + T_rewrite + T_commit_gen + T_write`

Expanded:
- `T_side_effect_scan` includes repeated path existence checks:
  - `O((A + D + constant) * Q_state_by_version(S, V, N))`
- `T_plugin_detect` includes plugin IO + dedupe:
  - `O(file_size + C log C)` plus extra state lookups
- `T_rewrite` + `T_commit_gen`:
  - `O(C + C * active_accounts + meta_rows)` (commit/materialized rows scale with `C`)
- `T_write`:
  - `O(inserted_rows * log N)` (index maintenance + SQL parse/exec)

Key point:
- The largest current term is not just `C`; it is repeated `Q_state_by_version(...)` invocations in filesystem path lookup and plugin state lookup helpers.

### Step-elimination strategy (ordered by expected impact)

1. Remove repeated `lix_state_by_version` rewrites from filesystem hot-path lookups.
- Replace `find_directory_id_by_path` / `find_file_id_by_path` internal queries with direct descriptor-table lookups over effective version scope.
- Keep inheritance correctness by computing effective ancestor scope once and resolving nearest visible row per path key.
- Target complexity shift:
  - from `O(k * Q_state_by_version(...))`
  - to `O(Q_version_scope + Q_batched_descriptor_reads + k)` where `k` is number of path checks.

2. Skip before-state reconstruction work for definite inserts.
- In plugin detect stage, if `before_path` is `None`, treat write as create and skip:
  - cache read fallback
  - state-based reconstruct fallback
- This removes extra expensive state queries on the insert path.

3. Cache plugin metadata/instances per execute pass.
- Avoid reloading installed plugins and wasm component instances repeatedly inside one detect pass.
- Existing materialization path already uses per-key instance caching; detect path should mirror this.

4. Replace plugin-state lookups that currently go through `lix_state_by_version` with direct materialized-table queries where scope is known.
- Especially `load_plugin_state_changes_for_file(...)` and related helpers.
- Preserve inheritance/tombstone semantics with explicit scope logic rather than full view rewrite.

5. Optional second wave: reduce large SQL literal payload overhead.
- If still needed after steps 1-4:
  - chunk large inserts
  - prefer bound parameters over very large literal SQL strings in generated statements.

### Expected impact
- Step 1 alone should remove the currently dominant `~4x` heavy lookup queries seen in trace for one insert.
- Combined with step 2, this should move plugin-matching inserts back toward the pre-regression range and restore a mostly linear-with-`C` profile.

## 2026-02-13 - Iteration 12: backend call-count reduction pass

### Goal
- Reduce backend call volume on plugin-active `INSERT INTO lix_file` path.

### Changes
- `packages/engine/src/plugin/runtime.rs`
  - Added create-write fast-path in `detect_file_changes_with_plugins(...)`:
    - if no prior file context (`before_path` and `before_data` are absent), treat as create.
    - skip before-plugin inference for same-path fallback.
    - skip cache/state reconstruction (`load_file_cache_data`, `reconstruct_before_file_data_from_state`).
    - skip existing-entity merge query for create path.
- `packages/engine/src/filesystem/mutation_rewrite.rs`
  - Reduced duplicate lookup work in `rewrite_file_insert_columns_with_backend(...)`:
    - resolve parent `directory_id` once and reuse it.
    - perform directory collision check via `find_directory_child_id(...)` directly for candidate filename segment.
    - avoid reparsing/re-resolving path through `find_file_id_by_path(...)` for uniqueness check.
  - Added `find_file_id_by_components(...)` helper and refactored `find_file_id_by_path(...)` to delegate to it.

### Validation
- `cargo test -p lix_engine --quiet`
  - passed (full suite)

### Call-count trace result (warmup + measured single insert)
- Same local trace harness as Iteration 11.
- Before this pass:
  - backend calls: `70`
  - measured wall: `313.74ms`
- After this pass:
  - backend calls: `61`
  - measured wall: `209.34ms`
- Dominant heavy lookup calls (`SELECT entity_id FROM ... lix_state_by_version ...`) dropped from 6 to 4 invocations in measured insert.

### Benchmark impact
- `pnpm -C packages/js-benchmarks run bench:json:quick`
  - old `@lix-js/sdk`: `710.07ms`
  - new `js-sdk`: `1478.70ms`
  - new/old: `0.48x` (improved vs previous regression runs, still slower)
- `pnpm -C packages/js-benchmarks run bench:json:breakdown:quick`
  - no plugin: `21.47ms`
  - plugin installed non-matching: `33.66ms`
  - plugin installed matching: `678.55ms`

### Next backend-call cuts
- Share path lookup results between:
  - `insert_side_effect_statements_with_backend(...)`
  - `rewrite_file_insert_columns_with_backend(...)`
  to avoid re-querying the same ancestor directory IDs.
- Cache installed plugin manifests in `Engine` memory with invalidation on `install_plugin`, eliminating per-execute plugin manifest query.
- Replace remaining hot `lix_state_by_version` lookup calls in mutation rewrite with direct descriptor-table effective-scope queries.

## 2026-02-13 - Iteration 13: shared directory-resolution cache across insert phases

### Goal
- Eliminate duplicate directory path resolution between:
  - `insert_side_effect_statements_with_backend(...)`
  - `rewrite_file_insert_columns_with_backend(...)`

### Changes
- `packages/engine/src/filesystem/mutation_rewrite.rs`
  - added `ResolvedDirectoryIdMap` type alias.
  - extended `FilesystemInsertSideEffects` with:
    - `resolved_directory_ids: ResolvedDirectoryIdMap`
  - `insert_side_effect_statements_with_backend(...)` now exports the `known_ids` map it already computes.
  - `rewrite_insert_with_backend(...)` and `rewrite_file_insert_columns_with_backend(...)` now accept an optional pre-resolved directory map.
  - file insert rewrite now resolves `directory_id` from the shared map first, then falls back to backend lookup only if absent.
- `packages/engine/src/sql/steps/filesystem_step.rs`
  - threaded optional `ResolvedDirectoryIdMap` into rewrite wrapper.
- `packages/engine/src/sql/route.rs`
  - passes `filesystem_insert_side_effects.resolved_directory_ids` into filesystem insert rewrite.

### Validation
- `cargo test -p lix_engine --quiet`
  - passed (full suite)

### Call-count trace result (same harness, warmup + measured single insert)
- Previous iteration:
  - backend calls: `61`
  - wall: `209.34ms`
- After this change:
  - backend calls: `59`
  - wall: `169.95ms`
  - backend SQL total: `145.06ms`

### Benchmark impact
- `pnpm -C packages/js-benchmarks run bench:json:quick`
  - old `@lix-js/sdk`: `714.93ms`
  - new `js-sdk`: `1194.25ms`
  - new/old: `0.60x` (improved again, still not parity)
- `pnpm -C packages/js-benchmarks run bench:json:breakdown:quick`
  - no plugin: `17.94ms`
  - plugin installed non-matching: `30.96ms`
  - plugin installed matching: `527.21ms`

### Remaining dominant calls
- still dominated by repeated heavy `SELECT entity_id FROM ... lix_state_by_version ...` lookups (~`38-39ms` each).
- plugin manifest query is still a fixed per-execute cost (~`15ms`).

### Next highest-impact change
- introduce engine-level installed-plugin cache (invalidate on `install_plugin`) to remove recurring plugin manifest DB query and reduce one call per execute.

## 2026-02-13 - Iteration 14: installed-plugin manifest cache + detect-stage call reduction

### Goal
- Remove fixed per-execute plugin manifest query from the hot `INSERT INTO lix_file` detect path.

### Changes
- `packages/engine/src/engine.rs`
  - added `installed_plugins_cache` on `Engine`.
  - load plugins once with `load_installed_plugins_with_backend(...)` and reuse cached entries on normal `execute(...)`.
  - bypass cache for transaction adapter path to preserve in-transaction visibility.
  - invalidate cache on:
    - `install_plugin(...)`
    - statements that reference `lix_internal_plugin`.
- `packages/engine/src/plugin/runtime.rs`
  - `detect_file_changes_with_plugins(...)` now accepts `installed_plugins` directly.
  - removed internal plugin-table query from detect function.
  - exported `load_installed_plugins(...)` as `pub(crate)` for engine-managed caching.

### Validation
- `cargo test -p lix_engine --quiet` ✅ full suite passed.

### Benchmark loop (fresh run)
- `pnpm -C packages/js-benchmarks run bench:json:quick`
  - old `@lix-js/sdk`: `658.68ms`
  - new `js-sdk`: `322.64ms`
  - new/old: `2.04x` (new faster)
- `pnpm -C packages/js-benchmarks run bench:json:breakdown:quick`
  - no plugin: `2.80ms`
  - plugin non-matching: `2.51ms`
  - plugin matching: `8.85ms`

### Notes
- Earlier failing Postgres tests were caused by disk exhaustion (`No space left on device`) from stale temp embedded-postgres directories under `/var/folders/.../T/`; after cleanup, full test suite is stable again.

## 2026-02-13 - Iteration 15: `lix_state` / `lix_state_by_version` predicate pushdown

### Goal
- Push `file_id`, `plugin_key`, `schema_key`, `entity_id` filters into rewritten `lix_state*` derived queries to reduce broad ranking scans.

### Changes
- `packages/engine/src/sql/steps/lix_state_view_read.rs`
  - extract top-level `AND` predicates from outer `WHERE`.
  - consume simple `=` predicates targeting `entity_id`, `schema_key`, `file_id`, `plugin_key` (including `lixcol_*` aliases).
  - push `entity_id/schema_key/file_id` into inner source scan (`s.*`) before `ROW_NUMBER()`.
  - push `plugin_key` into inner ranked filter (`ranked.*`) after winner selection (semantics-preserving for row choice).
  - remove consumed predicates from outer `WHERE` to avoid duplicate anonymous placeholder binding (`?`).
  - added rewrite test:
    - `pushes_file_id_and_plugin_key_filters_into_lix_state_derived_query`.
- `packages/engine/src/sql/steps/lix_state_by_version_view_read.rs`
  - same pushdown pipeline for by-version rewrite.
  - supports alias-qualified references (e.g. `sv.file_id = ?1`).
  - added rewrite test:
    - `pushes_alias_qualified_filters_into_lix_state_by_version_derived_query`.

### Validation
- `cargo test -p lix_engine --quiet` ✅ full suite passed.

### Benchmark results
- `pnpm -C packages/js-benchmarks run bench:json:quick`
  - old `@lix-js/sdk`: `682.37ms`
  - new `js-sdk`: `326.72ms`
  - new/old: `2.09x` faster.
- split measurement (same insert + per-iteration count query):
  - insert mean: `10.12ms`
  - `SELECT COUNT(*) FROM lix_state WHERE file_id=? AND plugin_key='plugin_json'` mean: `312.17ms`
  - total: `322.29ms`
- prior split reference:
  - count mean: `328.54ms`
- net effect from this iteration:
  - count path improved by ~`5%` (`328.54ms -> 312.17ms`), still the dominant bottleneck.

### Finding
- Predicate pushdown helped, but the benchmark is still overwhelmingly read-bound by `COUNT(*)` over `lix_state` view semantics.
- Next likely high-impact step: count-specific fast-path rewrite for `lix_state`/`lix_state_by_version` with direct filtered aggregation.
