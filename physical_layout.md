# Physical Layout

This document is the hard-cut physical layout target for tracked Lix state.
It separates durable changelog facts, direct lookup indexes, and derived state
read models. Compaction and packing are later storage optimizations, not the
core truth shape.

## Summary

```text
┌──────────────────────────────────────────────────────────────┐
│ append-only changelog fact plane                             │
├──────────────────────────────────────────────────────────────┤
│ changelog.commit              commit_id -> CommitRecord      │
│ changelog.commit_change_ref_chunk                            │
│   commit_id/chunk_no -> chunk                                │
│ changelog.change              change_id -> ChangeRecord      │
│ json_store.json               json_ref -> JSON bytes         │
└───────────────────────────────┬──────────────────────────────┘
                                │ referenced by versions / projected into state
                                ▼
┌──────────────────────────────────────────────────────────────┐
│ untracked state / reachability plane                         │
├──────────────────────────────────────────────────────────────┤
│ untracked_state.row          version refs, workspace refs     │
│ optional reachability/history indexes                        │
└───────────────────────────────┬──────────────────────────────┘
                                │ derived serving state
                                ▼
┌──────────────────────────────────────────────────────────────┐
│ derived state/read plane                                      │
├──────────────────────────────────────────────────────────────┤
│ tracked_state.commit_root     commit_id -> root_ref          │
│ tracked_state.tree_chunk      chunk_id -> ProllyNode         │
└──────────────────────────────────────────────────────────────┘
```

The important cut:

```text
Do not pack together objects that have different primary read keys.
```

Lix's primary read keys are:

```text
commit_id
change_id
commit_id -> change_id set
schema_key + file_id + entity_id at commit_id
json_ref
```

The physical layout therefore has directly addressable commits, directly
addressable changes, chunked commit change refs, a derived state root, and a
separate payload store. Readers must not depend on decoding one giant
transaction segment to reach a single commit or change.

## Model

Core rules:

```text
changelog.commit is canonical for one logical commit's header facts.
changelog.commit_change_ref_chunk is canonical for a commit's ordered change refs.
changelog.change is canonical for one row/entity change and its payload refs.
json_store.json is canonical for JSON payload bytes by json_ref.
tracked_state is a derived, rebuildable index over changelog facts.
Version refs are untracked-state rows and are the MVP reachability roots.
```

Dependency direction:

```text
transaction coordinator
  ├─ writes json_store.json payloads
  ├─ writes changelog.change records
  ├─ writes changelog.commit_change_ref_chunk records
  ├─ writes changelog.commit record
  └─ writes optional version refs, rebuildable indexes, commit roots, tree chunks

changelog facts ──► tracked_state is allowed
tracked_state ────► changelog facts is forbidden
```

Mental model:

```text
changelog.commit
  directly keyed catalog object for one Lix Commit
  contains logical commit header, parents, commit row change, and author metadata

changelog.commit_change_ref_chunk
  directly keyed semantic chunk of commit change refs for one commit
  represents commit_id -> set<change_id>
  ordered by schema_key, file_id, and entity_id in the canonical layout

changelog.change
  directly keyed catalog object for one row/entity Change
  contains change_id, schema/entity/file identity, payload refs, and created_at

tracked_state.commit_root
  per-commit pointer to the derived tracked-state tree

tracked_state.tree_chunk
  shared Dolt-like/prolly tree node storage
  stores schema_key + file_id + entity_id -> latest change_id + cached refs

json_store.json
  direct large/deduplicated JSON payloads by json_ref
```

The dotted names are catalog/object-type names, not filesystem paths.

## Catalog

```text
space: changelog.commit
key:
  commit_id
value:
  CommitRecord {
    format_version
    commit_id
    parent_commit_ids[]
    change_id
    author_account_ids[]
    created_at
  }
```

```text
space: changelog.commit_change_ref_chunk
key:
  commit_id | chunk_no
value:
  CommitChangeRefChunk {
    format_version
    commit_id
    entries[] {
      schema_key
      file_id optional
      entity_id
      change_id
    }
  }
```

```text
space: changelog.change
key:
  change_id
value:
  ChangeRecord {
    format_version
    change_id
    schema_key
    entity_id
    file_id optional
    snapshot_ref optional json_ref
    metadata_ref optional json_ref
    created_at
  }
```

```text
space: tracked_state.commit_root
key:
  commit_id
value:
  CommitRoot {
    format_version
    commit_id
    root_ref
    parent_root_refs[]
    changed_key_count
    row_count_estimate
  }
```

```text
space: tracked_state.tree_chunk
key:
  chunk_id
value:
  ProllyNode {
    entries:
      schema_key + file_id + entity_id -> {
        change_id
        deleted
        snapshot_ref cache
        metadata_ref cache
        optional scalar/header cache
      }
  }
```

```text
space: json_store.json
key:
  json_ref
value:
  normalized JSON bytes
```

Untracked refs and optional accelerators:

```text
space: untracked_state.row
key:
  version_id | schema_key | entity_id | file_id optional
value:
  snapshot_ref / metadata_ref
```

Version refs are `lix_version_ref` rows inside `untracked_state.row`. They are
moving pointers and reachability roots, not changelog truth.

The optional indexes below are not part of the MVP write path. They are
rebuildable extension points for product paths that need global reachability,
key history, or ancestry acceleration.

```text
space: changelog.index.change_reachability
key:
  change_id | commit_id
value:
  commit_change_ref_chunk_no | entry_ordinal
```

```text
space: changelog.index.key_history
key:
  schema_key | file_id | entity_id | commit_id
value:
  change_id
```

```text
space: changelog.index.commit_ancestry
key:
  commit_id | ancestor/skip marker
value:
  ancestor_commit_id
```

Direct `commit_id` and `change_id` lookup do not require locator indexes in the
MVP layout because `changelog.commit` and `changelog.change` are keyed by those
IDs.

## 80% Invariants

Backend atomicity:

```text
Write-set order:
  1. stage/write json_store.json payloads
  2. stage changelog.change records
  3. stage changelog.commit_change_ref_chunk records
  4. stage changelog.commit record
  5. stage optional refs, rebuildable indexes, commit roots, tree chunks
  6. commit the storage transaction atomically
```

Within one backend transaction, no staged object is readable outside that
transaction until commit. The backend commit is the durability and atomicity
barrier. Lix does not add an engine WAL, fsync policy, recovery protocol, or
separate lifecycle row above backend commits in the MVP.

Any index, commit root, or tree chunk object can be ignored or rebuilt from
changelog facts. Indexes are accelerators, not reachability or truth sources.

Commit closure:

```text
Staging changelog.commit for commit C is valid only if:
  - C's changelog.commit record exists and decodes.
  - every parent_commit_id already has a changelog.commit record, or has one
    staged earlier in the same atomic write set.
  - every commit change-ref chunk for C decodes.
  - entries are strictly increasing by schema_key, file_id, and entity_id
    within each chunk and across chunks ordered by chunk_no.
  - no schema_key + file_id + entity_id tuple appears twice in the commit
    change-ref stream.
  - every commit change-ref entry's change_id resolves to an existing
    changelog.change in staged or stored changelog truth.
  - every commit change-ref entry's schema_key, file_id, and entity_id match
    the referenced changelog.change fields.
```

Logical ID uniqueness:

```text
commit_id and change_id uniqueness is a changelog truth invariant.
Because commits and changes are directly keyed, a writer can detect collisions
with ordinary key existence checks in the same write transaction.

Normal transaction writers treat any existing commit_id or change_id as an
error. Repair/import paths may treat an existing identical record as idempotent,
but an existing different record is always a collision/corruption error.
```

Truth closure:

```text
CommitRecord
  -> CommitChangeRefChunk[]
  -> changelog.change
  -> json_store.json

tracked_state.commit_root and tracked_state.tree_chunk are not in the truth closure.
Physical pack/block placement is not logical identity.
```

Canonical row key:

```text
schema_key + file_id + entity_id
```

Commit and change identity:

```text
CommitRecord.change_id is the commit's own change id for the lix_commit projection.
changelog.change objects are first-class row/entity changes.
CommitChangeRefChunk entries reference change_id.
tracked_state.tree_chunk leaves reference change_id.
change_id is logical row-change identity, not physical placement.
json_ref is payload/content identity.
commit_id is commit identity.
```

Change reachability:

```text
A raw changelog.change hit proves durable physical truth only. It does not prove
the change is reachable from any commit or version ref.

A change is reachable from commit C only when C's change refs contain the
change_id. A change is reachable from a version ref only when the ref's
commit ancestry contains a commit whose change refs contain the change_id.
Optional reachability indexes return candidates only; readers still validate
through commit records and commit change-ref chunks.
```

Commit change refs:

```text
Within one logical changelog.commit, commit change refs are coalesced to at most
one winning change_id per schema_key + file_id + entity_id tuple. They are
references to `changelog.change` records, not embedded ChangeRecord bytes, and
there is no separate commit-level set object or id in the physical layout.

Multiple writes to the same schema_key + file_id + entity_id tuple within one
transaction produce one net durable changelog.change for the tracked state model.

Commit change-ref chunks are canonical in schema_key, file_id, entity_id order.
This favors merge planning, conflict checks, projection rebuild, and branch diff.

If an operation log is later needed for audit, it is a separate object and not
part of tracked_state tree semantics.

The changelog is a durable state-change log, not a per-operation audit log. If
one transaction inserts, updates, and deletes the same schema_key + file_id +
entity_id tuple, the MVP stores the net tracked-state change, not every
intermediate operation.
```

## Addressing

Lix separates row-change identity from payload identity:

```text
change_id
  addresses the exact row/entity change
  optimized for merge planning, projection rebuild, GC, and direct truth
  hydration

json_ref
  addresses JSON payload bytes
  optimized for payload dedup, verification, and large payload reuse

commit_id
  addresses one logical commit and its change refs
```

`changelog.change` stores `snapshot_ref` and `metadata_ref` as logical
`json_ref` values. `json_ref` is the canonical payload identity. The MVP stores
payload bytes in `json_store.json`; inline payload bytes are deferred as a later
storage optimization.

This differs from systems that content-address most objects. The tradeoff is
intentional: two identical payloads can still be different row changes with
different commits, metadata, commit change refs, and history. Lix dedups payload bytes;
it does not collapse row-change identity.

Logical `ChangeRecord` objects are pure row facts. They do not store an `op`
field, a separate deletion flag, a physical locator, a segment directory, or
ownership semantics.

```text
delete
  = snapshot_ref is null

insert/update/delete
  = interpretation from tracked_state.diff before/after refs

commit inclusion
  = changelog.commit_change_ref_chunk entries reference change_id

payload placement
  = snapshot_ref / metadata_ref are json_ref values
  = readers resolve json_ref through json_store.json
```

If inline payload bytes are added later, `JsonRef::for_content(inline_bytes)`
must equal the declared `json_ref`. Inline bytes can witness a payload but do
not replace `json_ref` as the canonical payload identity.

## Tracked State Root

Every durable commit gets one logical `tracked_state.commit_root`.

That root is the derived state index for all tracked rows at the commit. This
matches the current Lix shape more than "one root per schema": current
`tracked_state.commit_root` is keyed by `commit_id`, and its tree keys are
`schema_key + file_id + entity_id`.

Dolt's analogous shape is:

```text
commit
  -> RootValue
       table_name -> table/root ref
         -> row prolly map
```

Unchanged Dolt tables keep the same table refs. Lix should preserve the same
property through prolly subtree sharing: a commit updates only the key ranges
touched by its row changes; unchanged schema/file/key ranges keep the same child
refs.

```text
commit_id
  -> tracked_state.commit_root
  -> tracked_state.tree_chunk
  -> schema_key + file_id + entity_id lookup / diff / scan
  -> change_id + deleted/snapshot_ref/metadata_ref cache
  -> changelog.change only for truth hydration
```

`tracked_state.commit_root` points into a real prolly-style tree stored in
`tracked_state.tree_chunk`. The tree is derived, and it is also a covering
state index for the hot path:

```text
schema_key + file_id + entity_id ->
  change_id
  deleted
  snapshot_ref cache
  metadata_ref cache
  optional scalar/header cache
```

The cache values are copies of changelog truth. If they are missing, stale, or
discarded, they are rebuilt from durable commit change refs and
`changelog.change` facts. The primary key space is ordered by:

```text
schema_key + file_id + entity_id
```

Schema reads are prefix/range reads over that key space. If schema count or
schema-level churn later makes a single composite-key tree too broad, the same
logical model can evolve to a Dolt-style root-of-roots:

```text
tracked_state.tree_chunk
  schema_key -> schema_root_ref
    -> file_id + entity_id -> change_id + cached refs
```

That is an internal physical optimization. The read model remains
`commit -> tracked root -> schema_key + file_id + entity_id -> cached row refs`.

```text
root-covered read: O(log_B N)
truth hydration:   O(log_B N + direct change lookup)
write:             naive O(K log_B N), batched expected O(T + K)
branch diff:       target O(D), precise expected O(T + D), pathological O(N)
full scan:         O(N)

N = tracked rows in the commit root
K = changed rows in one commit
D = emitted changed keys between two roots
T = changed/visited prolly nodes whose hashes differ
B = prolly fanout / leaf capacity
```

Every durable commit persists its `tracked_state.commit_root`. Mutations into
the tree are sorted by schema_key, file_id, and entity_id and applied as one
batch per commit. Tree bytes written are `O(T * node_bytes)`; scattered keys can make `T` approach
`K * tree_height`. This is the write-amplification tradeoff for `O(log_B N)`
reads and Dolt-style `O(D)` target diffs.

A durable commit remains readable if its `tracked_state.commit_root` or
`tracked_state.tree_chunk` objects are missing. The slow path rebuilds from the nearest
available ancestor root plus durable commit change refs. If no ancestor
root exists, rebuild starts from the initial root through reachable ancestry.
Normal hot-path read bounds do not apply until the derived root is rebuilt.

Prolly chunk boundaries are a function of schema_key, file_id, and entity_id
only, not `change_id`, `snapshot_ref`, `metadata_ref`, or cached leaf values.
Re-pointing an existing key to a new change_id must not move chunk boundaries.

No MVP delta layers, checkpoint policy, or slice-replay read path. Later
optimizations should happen inside prolly node encoding, payload colocation,
and optional secondary indexes without changing the read model. A later
write-amplification escape hatch may let persisted roots lag
the newest commits and derive the tail in memory. A later Neon-style alternative
is delta layers plus periodic full roots, trading direct root lookup for bounded
replay.

## Workflow Cases

```text
insert rows
  changelog: +1 CommitRecord, +N commit change-ref entries in chunks,
             +N changelog.change records
  tracked: update parent root with inserted keys -> change_id
  json_store.json: only large/deduplicated payloads

update rows
  changelog: +1 CommitRecord, +N commit change-ref entries in chunks,
             +N changelog.change records
  tracked: update parent root with updated keys -> change_id
  json_store.json: only large/deduplicated payloads

delete rows
  changelog: +1 CommitRecord, +N commit change-ref entries in chunks,
             +N changelog.change records with snapshot_ref = null
  tracked: update parent root with deleted keys -> change_id
  json_store.json: none

version from commit
  changelog: no row facts
  tracked: reuse existing root
  untracked: update lix_version_ref row to commit_id

write on version
  changelog: new commit has parent = previous version head
  tracked: derive new root from previous version head root
  untracked: move lix_version_ref row -> new_commit_id

merge commit
  changelog: merge CommitRecord + commit change-ref chunks for chosen change_ids
  tracked: derive merge root by applying chosen and newly written change_ids
  copied: no reused changelog.change records, no reused payload bytes
```

Merge commit change refs are the deterministic net state after planning:

```text
CommitChangeRefChunk entries contain change_ids that survive conflict checks.
For one schema_key + file_id + entity_id tuple, the merge result has one
winning change_id.
If the merge writes a new row change for the same key, that new change wins.
CommitChangeRefChunk entries are for changed/chosen keys, not all keys in the result.
Merge storage is O(M + A), never O(N).
```

Merge reuse path:

```text
merge_commit
  -> changelog.commit
  -> changelog.commit_change_ref_chunk entries
  -> change_id
  -> changelog.change
  -> schema_key / file_id / entity_id / snapshot_ref / metadata_ref
  -> payload hydration only if needed
```

Conflicts are not a storage concept in the MVP. The merge planner can throw on
divergent same-key changes and write no unresolved-conflict records.

## Read Paths

Exact row at version:

```text
tracked_state.commit_root(commit_id)
  -> root_ref
  -> tracked_state.tree_chunk lookup(schema_key, file_id, entity_id)
  -> change_id + deleted/snapshot_ref/metadata_ref cache
  -> hydrate snapshot_ref / metadata_ref only if requested
```

Truth hydration for audit/rebuild:

```text
change_id
  -> changelog.change
```

Physical truth lookup:

```text
change_id
  -> changelog.change
  -> O(1) direct lookup
  -> not a reachability proof
```

Reachable change from commit:

```text
change_id + commit_id context
  -> changelog.commit(commit_id)
  -> commit change-ref chunks contain change_id
  -> changelog.change(change_id)
```

Global reachable-change query:

```text
change_id
  -> optional changelog.index.change_reachability candidates
  -> validate candidate commit and commit change refs

without changelog.index.change_reachability:
  scan reachable commit change refs from version refs
```

Commit change refs:

```text
commit_id
  -> changelog.commit
  -> stream changelog.commit_change_ref_chunk/<commit_id>/*
```

Commit full changed row facts:

```text
commit_id
  -> changelog.commit
  -> stream commit change-ref chunks
  -> batch get changelog.change by change_id
  -> hydrate payloads only if needed
```

Key/header scan:

```text
tracked_state.commit_root
  -> tracked_state.tree_chunk key ranges
  -> schema_key + file_id + entity_id, deleted bits, cached scalar refs
  -> no payload hydration
```

Full scan:

```text
tracked_state.commit_root
  -> tracked_state.tree_chunk leaves
  -> change_id + cached row refs
  -> batch hydrate json_store.json only if payloads requested
```

Changed keys / branch diff:

```text
base_commit -> tracked_state.commit_root -> root_ref
head_commit -> tracked_state.commit_root -> root_ref
  -> compare subtree hashes by key range
  -> descend changed ranges
  -> return old/new change_id values
  -> do not materialize JSON strings
```

Reader contract:

```text
Commit reads load `changelog.commit` directly by commit_id.

Reachable change reads prove reachability through commit change refs and,
when needed, version refs. They may use optional reachability indexes as
accelerators, but must validate that the candidate commit exists and its
change refs contain the change_id.

Physical/debug reads can read changelog.commit or changelog.change directly by
ID, but a raw changelog.change hit does not prove root reachability.
```

## Complexity Contract

Notation:

```text
N = tracked rows at a commit
K = changed rows in one write
A = newly written merge rows
M = reused merge change_ids
D = emitted changed keys between two roots
T = changed/visited prolly nodes whose hashes differ
B = prolly fanout / leaf capacity
C = commits
R = changelog.change objects
P = payload bytes read/written
Q = commit change-ref entries or root entries emitted by an operation
H = history entries for one key/entity
```

Target bounds:

```text
version from commit
  O(1)

exact row at commit
  root-covered refs/header: O(log_B N)
  external payload by cached ref: add P
  truth hydration: add O(1) direct change lookup + P if payload requested

get_many(m keys)
  arbitrary refs/header:   O(m log_B N)
  sorted/clustered refs:   O(log_B N + m)
  external payloads:       add P
  truth hydration:         add O(m) direct change lookups + P

insert/update/delete K rows
  changelog.change:        O(K) writes
  commit change-ref chunks: O(K) entries, O(ceil(K/chunk_capacity)) chunk writes
  changelog.commit:        O(1)
  tracked root:            naive O(K log_B N), batched expected O(T + K)

commit -> changed change_ids
  O(1 + Q)

commit -> full changed row facts
  O(Q) commit change-ref scan + O(Q) direct change lookups + P if payload requested

commit -> inserts/updates/deletes classification
  root-covered classification: O(T + D)
  truth hydration:             add O(D) direct change lookups
  optional later op cache may lower constants

change_id lookup
  O(1) expected direct keyed lookup

entity/key history
  with future key index: O(log R + H)
  today:                 O(R)

schema/file range scan
  root-covered refs/header: O(log_B N + Q)
  external payloads:        add P
  truth hydration:          add O(Q) direct change lookups + P

full scan
  root-covered refs/header: O(N)
  external payloads:        add P
  truth hydration:          add O(N) direct change lookups + P

branch diff
  target O(D) in Dolt/prolly terms
  precise expected O(T + D)
  pathological O(N) if chunk/hash sharing fails broadly

merge
  conflict planning: precise expected O(T + D), pathological O(N)
  truth hydration:   add O(D) direct change lookups if planner needs Change facts
  write result:      naive O((M + A) log_B N), batched expected O(T + M + A)
  storage:           O(M + A), never O(N)

ancestry / merge-base
  parent edges: O(visited ancestors)
  target O(log C) or better requires skip/closure index

rebuild optional changelog indexes
  O(R + C + commit change-ref entries)

rebuild tracked root for one commit
  from existing parent root: O(Q log_B N) naive, batched expected O(T + Q)
  if cached refs must be rebuilt from Change facts: add O(Q)
  if ancestor roots are missing: sum this over missing commits from nearest root

GC mark/sweep
  O(reachable commits + reachable commit change-ref entries + reachable changes
    + reachable payload refs)
```

## GC / Compaction

Keep this small for the first implementation:

```text
MVP:
  direct immutable changelog.commit records
  direct immutable changelog.change records
  direct immutable changelog.commit_change_ref_chunk records
  mark/sweep whole objects only
  no changelog rewrite
```

Reachability roots:

```text
lix_version_ref rows in untracked_state.row
```

Reachability edges:

```text
untracked_state.row/lix_version_ref -> changelog.commit
commit -> parent commits
commit -> commit change-ref chunks
commit change-ref chunk -> change_ids
change_id -> changelog.change
changelog.change -> json_store.json, for large/deduplicated payloads
```

Derived retention:

```text
tracked_state objects are retained only for reachable commits
tracked_state never keeps changelog or payload facts alive
optional changelog.index.* records keep nothing alive
tracked_state.commit_root and tracked_state.tree_chunk are derived, rebuildable objects
```

Chunk sizing guidance:

```text
commit change-ref chunks: 32-128 KiB
tracked_state leaf chunks: existing prolly tuning
```

Future compaction is out of scope for this MVP. If added later, it must not
reintroduce a giant segment as the only practical lookup path for `commit_id`,
`change_id`, or `commit_id -> change_id set`.

## GC Cases

GC marks from reachability roots, not from derived indexes:

```text
lix_version_ref row in untracked_state.row
  -> commit_id
  -> changelog.commit
  -> parent commits
  -> commit change-ref chunks
  -> change_ids
  -> changelog.change
  -> json_store.json, for large/deduplicated payloads
```

`tracked_state.*` and `changelog.index.*` keep nothing alive. They are retained
only when attached to reachable commits and may be deleted/rebuilt.

| Case                                                                 | Keep                                                                                          | Sweep                                               | Note                                                                       |
| -------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- | --------------------------------------------------- | -------------------------------------------------------------------------- |
| Reachable normal commit                                              | `changelog.commit`, commit change-ref chunks, referenced `changelog.change` objects, payloads | unrelated objects                                   | Standard mark path.                                                        |
| Unreachable commit with no referenced changes                        | nothing after retention horizon                                                               | commit, commit change-ref chunks, changes, payloads | Retention horizon is policy, not layout.                                   |
| Unreachable commit, reused change reachable                          | reused `changelog.change` and payloads                                                        | unrelated commits if otherwise unreachable          | Change reachability flows through commit change refs, not origin metadata. |
| Reachable commit, missing `tracked_state.commit_root` or tree chunks | commit/change/payload truth                                                                   | missing root stays missing until rebuilt            | Reader rebuilds derived root from changelog facts.                         |
| Reachable tree chunks, unreachable commit                            | nothing solely because of tree chunks                                                         | tree chunks/commit root may be swept                | Derived state is not a retention root.                                     |
| Large JSON referenced by reachable change                            | `json_store.json` payload                                                                     | none                                                | Payload is in truth closure through `changelog.change`.                    |
| Large JSON referenced only by unreachable change                     | none after retention horizon                                                                  | payload                                             | Sweep after no reachable change references it.                             |
| Stale/missing optional index                                         | reachable truth objects                                                                       | stale index records                                 | Rebuild from changelog facts and reachable commits.                        |

## Clean Cuts

```text
1. Make changelog.commit directly keyed by commit_id.
2. Make changelog.change directly keyed by change_id.
3. Make the commit change refs first-class and chunked by commit_id/chunk_no.
4. Keep json_ref as payload/content identity and change_id as row-change identity.
5. Use version refs as MVP reachability roots.
6. Make tracked_state only a derived commit root plus tree chunks.
7. Keep optional changelog indexes rebuildable and non-authoritative.
8. Keep tracked-state repair flowing from durable changelog facts to derived roots.
```
