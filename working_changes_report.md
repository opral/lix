# Working Changes Architecture Report

## Scope

This document models the **current** `lix_working_changes` architecture in `packages/engine`.
It focuses on:

- `version.working_commit_id`
- how working changes are represented
- where synthetic artifacts exist
- how reads are produced through `lix_working_changes`

## Executive Summary

`lix_working_changes` is a read surface over a hybrid model:

1. **Real commit graph data**
- `lix_version.commit_id` and `lix_version.working_commit_id`
- real `lix_commit`, `lix_change_set`, `lix_change_set_element`, `lix_change`, `lix_commit_edge`

2. **Synthetic working projection rows** (ephemeral)
- written into `lix_internal_state_untracked`
- tagged with metadata `{"lix_internal_working_projection":true}`
- include synthetic `lix_change` + synthetic `lix_change_set_element` rows
- use IDs prefixed with `working_projection:`

Important correction:
- `working_commit_id -> change_set_id` points to a **real** change set.
- The synthetic part is the **projected rows** (change + CSE) used for read-time working diff behavior.

## Core Data Model

### Version pointers

Each version has:

- `commit_id`: current committed tip for that version
- `working_commit_id`: mutable working head used for checkpoint rotation and working projection

The pointer is also represented in `lix_version_pointer` snapshots.

### Commit graph

Each commit has:

- `id`
- `change_set_id`
- `parent_commit_ids`
- `change_ids`

Edges are stored in `lix_commit_edge`.

### Working change set identity

`working_commit_id` references a commit row whose `change_set_id` is the current working change set identity.
That `change_set_id` is not synthetic.

## Real vs Synthetic Artifacts

| Artifact | Real/Synthetic | Storage | Notes |
|---|---|---|---|
| `lix_commit` (working commit row) | Real | materialized + untracked overlays | Real commit object for working pointer |
| working `change_set_id` on commit snapshot | Real | in `lix_commit.snapshot_content` | Stable ID until checkpoint rotation |
| `lix_change` rows with `working_projection:*` IDs | Synthetic | `lix_internal_state_untracked` | Ephemeral projected working rows |
| `lix_change_set_element` rows pointing to `working_projection:*` | Synthetic | `lix_internal_state_untracked` | Ephemeral projected membership |
| metadata marker | Synthetic marker | `metadata` column | `{"lix_internal_working_projection":true}` |
| `lix_internal_source_change_id` metadata field | Synthetic metadata | on synthetic `lix_change` snapshot metadata | Preserves original change id for external reporting |

## End-to-End Flow

## 1) Initialization / Version creation

For each version:

- a real `working_commit_id` is created
- a real working `change_set_id` is created
- an edge from `commit_id -> working_commit_id` is created

ASCII:

```text
version V
  commit_id = C_tip
  working_commit_id = C_work

C_tip ----> C_work
          (parent edge)

C_work.snapshot.change_set_id = CS_work   (real)
```

## 2) Tracked writes (normal execution)

Writes produce real domain changes and real commit updates via commit generation/runtime.
The working pointer remains separate from the committed tip pointer.

Conceptually:

```text
writes -> domain changes -> commit generation ->
  - new real commit/tip metadata
  - real change_set_element rows
  - real change rows
```

## 3) Read path preparation (critical)

Before read-only queries are executed, engine calls working projection refresh.

```text
read query
  -> prepare_execution_with_backend(...)
  -> requirements.read_only_query == true
  -> maybe_refresh_working_change_projection_for_read_query(active_version_id)
```

### Projection refresh internals

High-level algorithm:

1. Resolve active version pointer (`commit_id`, `working_commit_id`).
2. Resolve working commit's real `change_set_id`.
3. Delete old synthetic projection rows (by metadata + id prefixes).
4. Load real commit graph + CSE rows.
5. Compute baseline as first parent of `working_commit_id` (fallback to self).
6. Traverse commits from current tip back until baseline (exclusive).
7. Select latest change per `(entity_id, schema_key, file_id)` from:
   - traversed commits
   - current working change set
8. Upsert synthetic rows:
   - synthetic `lix_change` row id: `working_projection:{active_version_id}:{change_set_id}:{schema_key}:{file_id}:{entity_id}`
   - synthetic `lix_change_set_element` entity id: `{change_set_id}~{synthetic_change_id}`
   - metadata marker + `lix_internal_source_change_id`
9. Upsert projected `lix_commit` snapshot for working commit with projected `change_ids`.

ASCII:

```text
(real graph + real cse + real changes)
            |
            v
   refresh_working_projection_for_read_query
            |
            +--> clear previous synthetic rows
            |
            +--> select latest per entity triple
            |
            +--> write synthetic rows to untracked:
                   - lix_change(working_projection:...)
                   - lix_change_set_element(CS_work~working_projection:...)
                   - metadata={"lix_internal_working_projection":true}
                   - source id in metadata.lix_internal_source_change_id
```

## 4) `lix_working_changes` query rewrite and output

`SELECT ... FROM lix_working_changes` is rewritten to a derived CTE query.

Key CTEs:

- `active_version` -> current version id
- `version_pointer` -> current version pointer snapshot
- `wc` -> `working_commit_id`
- `cc` -> `commit_id`
- `wcs` -> working change set id from `wc`
- `ccs` -> baseline change set id from `cc`
- `working_change_rows` -> reads projected `lix_change` from untracked
- `working_change_set_element_rows` -> reads projected CSE from untracked
- baseline CTEs combine untracked + materialized non-projection rows

Status output uses before/after presence and id mismatch:

- `added`
- `modified`
- `removed`

Rows are filtered to changed states only (no unchanged rows).

### Change id normalization

`after_change_id` uses:

- `metadata.lix_internal_source_change_id` if present
- otherwise synthetic id

So external results avoid leaking `working_projection:*` ids where source ids are known.

ASCII:

```text
SELECT ... FROM lix_working_changes
        |
        v
rewrite -> derived query with CTEs
        |
        +--> working side (synthetic rows in untracked)
        +--> baseline side (real rows, projection rows excluded)
        |
        v
status classification -> final rows
```

## 5) Checkpoint rotation

When checkpoint is created:

- current `working_commit_id` is promoted to checkpoint commit
- `version.commit_id` becomes old `working_commit_id`
- new `working_commit_id` + new working `change_set_id` are created
- edge `checkpoint -> new_working` is created
- synthetic projection rows for old working set are deleted

ASCII:

```text
Before checkpoint:
  version.commit_id = C_tip
  version.working_commit_id = C_work

Checkpoint:
  promote C_work as checkpoint
  create C_work2 with new CS_work2
  update version:
    commit_id = C_work
    working_commit_id = C_work2

Graph:
  C_tip -> C_work -> C_work2
```

## Invariants

- `working_commit_id` is unique per version and references an existing commit.
- `working_commit.change_set_id` references an existing change set.
- Synthetic projection rows are globally scoped (`version_id='global'`) but tagged and cleaned aggressively.
- `lix_working_changes` returns changed rows only (no unchanged union branch).
- Output change ids should map to source change ids (via metadata) rather than projection IDs.

## Practical Implications

- The model keeps query-time semantics simple for agents (`SELECT ... FROM lix_working_changes`) while preserving internal flexibility.
- Synthetic projection rows are an implementation detail to stabilize reads, not source-of-truth history.
- The source of truth remains real `lix_change`, `lix_change_set_element`, `lix_commit`, and `lix_commit_edge` history.

## Relevant Engine Files

- `packages/engine/src/sql/execution/shared_path.rs`
- `packages/engine/src/sql/side_effects.rs`
- `packages/engine/src/sql/history/projections.rs`
- `packages/engine/src/working_projection.rs`
- `packages/engine/src/sql/planning/rewrite_engine/steps/lix_working_changes_view_read.rs`
- `packages/engine/src/checkpoint/create_checkpoint.rs`
- `packages/engine/tests/working_changes_view.rs`
- `packages/engine/tests/working_change_set.rs`
