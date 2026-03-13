# Filesystem Plan

## Goal

Make filesystem CRUD history-insensitive.

- Exact file and directory CRUD should be `O(1)` average with respect to commit history.
- Path-based lookup should be `O(depth)`.
- Branch creation should be `O(1)`.
- Commits should touch only changed filesystem keys, not re-resolve effective state from historical rows.

The current bottleneck is that `lix_file` writes prefetch through live projection SQL that rebuilds current file, directory, and blob state from descriptor candidates using recursive CTEs, unions, and `ROW_NUMBER()` ranking. That makes one logical update get slower as accumulated state grows.

## First-Principles Model

Separate two concerns:

- History: append-only facts for audit, replay, merge, and time-travel.
- Present state: the current filesystem view for a given `version_id`.

Ordinary CRUD must hit present state directly. It must not derive present state from history on the hot path.

For branching, present state cannot be physically copied per branch. Instead, branches should point to persistent roots and use copy-on-write.

## Authoritative State

Introduce an authoritative filesystem state layer:

- `lix_internal_fs_version_root`
- `lix_internal_fs_file_live`
- `lix_internal_fs_directory_live`

These replace legacy filesystem materialization as the source of truth for CRUD.

Legacy tables to remove from the CRUD path:

- `lix_internal_live_v1_lix_file_descriptor`
- `lix_internal_live_v1_lix_directory_descriptor`
- `lix_internal_live_v1_lix_binary_blob_ref`

Those tables may be deleted entirely if no other subsystem depends on them.

## Data Model

### 1. Version Root

`lix_internal_fs_version_root`

- `version_id` primary key
- `root_generation_id`
- `commit_id`
- `updated_at`

This table maps each branch-like `version_id` to the current live filesystem generation.

### 2. File Live Rows

`lix_internal_fs_file_live`

- `generation_id`
- `version_id`
- `file_id`
- `directory_id`
- `name`
- `extension`
- `hidden`
- `metadata`
- `blob_hash`
- `size_bytes`
- `created_at`
- `updated_at`
- `change_id`
- `commit_id`
- primary key: `(generation_id, file_id)`
- unique index: `(generation_id, directory_id, name, extension)`

### 3. Directory Live Rows

`lix_internal_fs_directory_live`

- `generation_id`
- `version_id`
- `directory_id`
- `parent_directory_id`
- `name`
- `hidden`
- `metadata`
- `created_at`
- `updated_at`
- `change_id`
- `commit_id`
- primary key: `(generation_id, directory_id)`
- unique index: `(generation_id, parent_directory_id, name)`

## Copy-On-Write Semantics

`generation_id` is the persistent state root identity.

Each `version_id` points to one current `generation_id`. A new branch starts by pointing at the same root as its parent. No file or directory rows are copied when the branch is created.

When a write happens on a version:

1. Read that version's current `generation_id`.
2. Allocate a new `generation_id`.
3. Copy only rows that are changed into the new generation.
4. Reuse all unchanged rows from the parent generation by logical inheritance.
5. Atomically repoint `version_id` to the new generation at commit time.

The key requirement is that reads for generation `G` must see:

- rows written directly in `G`
- otherwise the nearest ancestor generation row for that key

There are two implementation options.

### Option A: Parent-Linked Generations

Add:

- `lix_internal_fs_generation`
  - `generation_id` primary key
  - `parent_generation_id`
  - `commit_id`
  - `version_id`
  - `created_at`

Reads walk parent generations until they find the row for a given key.

Pros:

- simplest copy-on-write model
- branch creation is `O(1)`
- writes touch only changed keys

Cons:

- exact CRUD becomes `O(number of generation hops)` unless periodically compacted
- this does not meet the strict `O(1)` target by itself

### Option B: Flattened Copy-On-Write Generations

Keep `generation_id`, but each committed generation contains a fully resolved live index for all keys visible at that root. New generations are built from the previous generation by copying changed rows and reusing storage pages structurally below SQLite.

In plain SQLite tables, this is not realistic without copying many rows. So if the strict target is real, Option B needs an explicit persistent key-value structure rather than ordinary relational rows.

Conclusion:

- parent-linked table inheritance is not enough
- to get both `O(1)` CRUD and `O(1)` branching, the live filesystem state must be represented as a persistent map, not as ordinary copied SQL rows

## Recommended Design

Use a persistent key-space with copy-on-write roots.

Logical indexes:

- `files_by_id`
- `directories_by_id`
- `file_child_by_parent_name_ext`
- `directory_child_by_parent_name`

Each generation root stores pointers to these persistent maps.

New tables:

- `lix_internal_fs_generation`
  - `generation_id` primary key
  - `parent_generation_id`
  - `files_by_id_root`
  - `directories_by_id_root`
  - `file_child_root`
  - `directory_child_root`
  - `commit_id`
  - `version_id`
  - `created_at`

- `lix_internal_fs_map_node`
  - persistent map nodes for the copy-on-write structure
  - node payload stores either child pointers or leaf payloads

The exact node encoding can be decided later:

- HAMT-like
- B-tree-like persistent pages
- radix/trie hybrid for path-oriented keys

The important invariant is:

- updating one file or directory creates only `O(log N)` new nodes in each affected map
- all unchanged nodes are shared with the parent generation

That gives:

- branch creation: `O(1)` by copying root pointers
- exact CRUD: `O(1)` average for hash-trie, or `O(log N)` deterministic for persistent tree
- path lookup: `O(depth)` with parent/name index, or `O(depth * lookup_cost)` depending on node type

If we want the simplest defensible target, we should state:

- exact CRUD: effectively `O(1)` average
- exact CRUD: `O(log N)` worst-case depending on persistent map choice

That is still the correct asymptotic fix because history depth disappears from the cost.

## Write Algorithm

For `UPDATE lix_file SET ... WHERE id = ?`:

1. Resolve active `version_id`.
2. Load `generation_id` from `lix_internal_fs_version_root`.
3. Look up the current file row in `files_by_id`.
4. If path fields change, validate against `file_child_by_parent_name_ext`.
5. If data changes, persist blob content and compute `blob_hash`.
6. Create updated file row payload.
7. Copy-on-write update:
   - `files_by_id`
   - `file_child_by_parent_name_ext` if name or directory changed
8. Create new generation root with updated map roots.
9. Append history/change/commit records.
10. Repoint `version_id` to the new generation in the same transaction.

For directory writes, do the same with:

- `directories_by_id`
- `directory_child_by_parent_name`

## Read Algorithm

Exact lookup by file id:

1. Resolve `version_id -> generation_id`
2. Query `files_by_id`
3. Return payload

No recursive effective-state SQL.
No descriptor ranking.
No generic materialization lookup.

Path lookup:

1. Start at root directory
2. Traverse path segments using `directory_child_by_parent_name`
3. For the final segment, resolve file through `file_child_by_parent_name_ext`

This is `O(depth)`.

## Merge and History

History remains append-only:

- commits
- changes
- change-set membership
- ancestry

Merges operate on filesystem roots or changed keys between roots, not by re-running live projection queries.

The filesystem live layer should support:

- diff between generation roots
- three-way merge on keys
- conflict detection on the same file/directory keys

That is the right place for branching semantics, not ordinary CRUD queries.

## What to Delete

Delete legacy filesystem effective-state reconstruction from the CRUD path:

- `build_live_file_prefetch_projection_sql()`
- filesystem descriptor candidate/ranking queries
- write-time file prefetch through live projection
- generic exact committed-state resolution for filesystem CRUD

Delete filesystem-specific materialized state tables if nothing else depends on them:

- `lix_file_descriptor`
- `lix_directory_descriptor`
- `lix_binary_blob_ref`

Retain generic history/state infrastructure only where it is still needed for:

- non-filesystem schemas
- debugging
- historical inspection

## Staged Implementation

### Stage 1

Introduce the new authoritative filesystem live layer and route `lix_file` CRUD to it.

Success criterion:

- no `WITH RECURSIVE` live projection query on ordinary file update

### Stage 2

Add branch root indirection:

- `version_id -> generation_id`
- branch creation becomes root-pointer copy

### Stage 3

Back the live layer with a persistent map implementation.

Success criterion:

- exact-id file update cost is independent of history depth
- branch creation remains `O(1)`

### Stage 4

Delete legacy filesystem materialization and projection code.

## Benchmark Target

The benchmark in `packages/engine/benches/lix_file_recursive_update.rs` should converge to:

- `depth=1` and `depth=128` having roughly the same update cost
- any remaining delta explained by cache/locality, not by history-shaped SQL

The benchmark is the guardrail:

- CRUD must not get slower as history grows

## Final Decision

Do not optimize the current recursive SQL path.

Replace it with:

- authoritative live filesystem state
- copy-on-write version roots
- persistent keyed indexes

That is the only first-principles path that makes:

- CRUD history-insensitive
- branching cheap
- commits proportional to changed keys rather than accumulated filesystem history
