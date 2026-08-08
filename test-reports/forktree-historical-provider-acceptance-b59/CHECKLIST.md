# H4 historical-provider semantic acceptance checklist

This checklist is bound to `b59e1f11a51153e0a787a81f0f25bf104d150aaf` and is
for the first production caller migration only. It is not a permission to
edit production in this package.

## One historical authority and one retained read

Every historical operation must enter through one caller-owned
`StorageRead`/`CoherentView` and keep that read for selector, repository root,
CommitCatalog, CommitObject, retained closure, state roots, and terminal
lowering. The provider may not call `begin_read`, retry on a new snapshot,
consult a second cache/index, or fall back to the superseded tracked-state or
columnar reader.

The required call graph is:

```text
caller-owned StorageRead
  -> ForkTreeReadFacade historical operation
     -> authenticated repository selector/root
     -> required CommitCatalog entry for requested commit
     -> CommitObject + retained member/back-edge closure
     -> state point/range on the same read
     -> one terminal SQL row lowering
```

`None`/empty output is valid only after the requested commit and all selected
roots authenticate and the requested identity is genuinely absent. Missing,
malformed, wrong-kind, substituted, or identity-mismatched commit/catalog/root
data is a typed corruption/storage error for both point and scan forms.

## Shared cell semantics

The model and adapter oracle must preserve these distinctions:

| authenticated cell | public result |
| --- | --- |
| no row for a validated key | absence (`None`/empty slot) |
| `StateCell::Null` | visible row, null content, `is_deleted = false` |
| `StateCell::Value` | visible row with exact value bytes/metadata |
| `StateCell::Tombstone` with inclusion | visible deleted row, null content, `is_deleted = true` |
| tombstone without inclusion | filtered from visible rows by the existing surface contract |
| any invalid authority/object | typed error, never empty success |

File and directory identities are disjoint domains. The same textual ID may
legitimately occur once as a file and once as a directory; it must not be
collapsed. File rows retain `file_id`, `directory_id`, `path`, and file
descriptor identity. Directory rows retain directory ID, parent ID, name, and
resolved path. A removed path is represented as a null previous/current path,
not as an unrelated identity or a tombstone-shaped file.

## Surface contracts

### `lix_file_history`

* An exact `lixcol_as_of_commit_id` route selects the authenticated historical
  commit; no exact anchor may silently fall back to the pinned active head.
* Public file identity is `file_id`; path resolution uses the authenticated
  directory state at the observed commit and its required ancestors.
* Descriptor, directory, blob, plugin-state, and plugin-owner events are
  grouped by `(file_id, as_of_commit_id, observed_commit_id)`.
* Source-change IDs are sorted and deduplicated within a grouped event.
* Final rows are ordered by `(file_id, as_of_commit_id, depth,
  observed_commit_id)`; SQL `LIMIT` is applied after grouping, ordering, and
  public filtering.
* A JSON null remains null. A tombstone remains `is_deleted = true`; it is not
  treated as an absent descriptor or a null value. Missing blob bytes are
  reported according to the existing blob contract and cannot fabricate file
  content.

### `lix_directory_history`

* The exact historical anchor and authenticated commit/root rules are the
  same as file history.
* Public identity is `directory_id`; parent/name/path resolution uses the
  directory tree at the observed commit. A file with the same textual ID is a
  different identity.
* Directory events include affected descendants only when the authenticated
  observed directory state proves that relationship.
* Grouping is by `(directory_id, as_of_commit_id, observed_commit_id)`;
  source-change IDs are sorted/deduplicated.
* Final rows are ordered by `(directory_id, as_of_commit_id, depth,
  observed_commit_id)` and `LIMIT` is post-group/post-order.
* Null, deleted, and absent directory descriptors remain distinguishable.

### `lix_diff`

* `from_commit_id` and `to_commit_id` are exact authenticated commit inputs;
  missing or corrupt source/target commit/root data errors before an empty
  result is produced.
* Filters for `schema_key`, typed `entity_pk`, and nullable `file_id` are
  conjunctive and preserve file identity. Impossible filters return a typed
  contradiction/empty result only when the two authenticated endpoints are
  valid.
* Added, modified, and removed rows preserve before/after change IDs;
  missing-side values are not converted into null-cell confusion.
* Checkpoint and undo/redo marker rows are excluded from ordinary `lix_diff`.
* Result ordering is deterministic by canonical identity and diff kind, and
  `LIMIT` is applied after filtering and marker exclusion.

### `lix_checkpoint`

* Checkpoint rows are selected from the authenticated checkpoint marker
  schema, never from arbitrary commits or undo/redo markers.
* Branch selection is exact: active branch, one requested branch, or the
  explicitly enumerated branch set. Global scope is not a checkpoint branch.
* Depth predicates are interpreted as commit distance from the selected
  branch head. Contradictory or negative ranges produce a valid empty result
  only after branch selection authority is valid.
* Rows are ordered deterministically by `(branch_id, depth, commit_id)` and
  the provider limit is applied after branch/depth selection.
* Missing branch head, missing checkpoint marker, missing commit catalog entry,
  wrong-kind marker, or malformed marker/root is an error, not an empty
  checkpoint list.

### Filesystem working-diff surfaces

The four surfaces are file and directory working diff, each in active-branch
and by-branch form.

* Each selected branch must have an authenticated checkpoint baseline; a
  missing baseline is an error, not a zero-row fallback.
* The diff is computed between the authenticated checkpoint commit and the
  authenticated branch head on the same retained read.
* File and directory descriptor identities are evaluated separately. A
  directory change may affect descendant file paths only through authenticated
  directory state; it must not invent a file identity.
* Each row preserves `id`, `path`, `previous_path`, `change_type`, and branch
  identity. Added/removed/modified classification is based on before/after
  authenticated presence, with null path on the missing side.
* Results are ordered by branch then identity (`id`), and `LIMIT` applies after
  all selected heads are combined and ordered.
* Tombstones are an internal diff input; they are not silently emitted as
  ordinary file/directory values or dropped before before/after comparison.

## Required adversarial matrix

Run point and scan/batch forms for every applicable surface:

1. Valid commit/root plus genuinely absent key: `None`/empty only here.
2. Missing selected CommitCatalog entry: typed error before empty output.
3. Missing selected root object: typed error.
4. Same-size wrong-kind root or malformed selector/catalog/commit/root: typed
   error.
5. Catalog key/object ID mismatch and commit/root identity mismatch: typed
   error.
6. Value, JSON null, tombstone included, and tombstone filtered: exact cell
   distinctions.
7. Same textual file ID and directory ID: two identities, no collision.
8. Exact order, duplicate source-change collapse, depth range, branch filter,
   projection, and LIMIT at zero/one/boundary/over-limit.
9. Checkpoint marker versus undo/redo marker selection.
10. File rename, directory rename, descendant directory change, add/remove,
    and unchanged unrelated identity.
11. Flush, drop, cold reopen, then repeat all authority and semantic cases.
12. Counters prove one retained read, zero retry, zero second view, zero
    legacy-reader fallback, and zero cache substitution.

The same pure model must pass before any adapter cell. Adapter order is
Memory, RocksDB, SlateDB; each cell is capped at 20 minutes and stops at the
first failure.

## Source boundary

The first production migration may change only the caller adapters for these
surfaces plus directly required private lowering glue. It must not add a
second historical authority, persisted index, compatibility reader, migration,
raw storage-space fallback, or cache that changes absence semantics. The
corrected historical resolver in `forktree/serving.rs` remains the sole
CommitCatalog/root authority.
