# Physical Layout

This document is the 80% physical layout target for tracked Lix state. It keeps
the core storage model theoretically sound while leaving codec, compaction, and
backend details for implementation.

## Current Layout

```text
┌─────────────────────┐
│ commit_store.commit │
├─────────────────────┤
│ key: commit_id      │
│ commit header       │
│ - change_id         │
│ - parent_commit_ids │
│ - change_pack_count │
│ - membership_count  │
└────┬─────────────┬──┘
     │             │ optional, not written by the probe below
     │             ▼
     │   ┌──────────────────────────┐
     │   │ commit_store.change_pack │
     │   │ key: commit_id + pack_id │
     │   │ authored changes[]       │
     │   └──────────────────────────┘
     │
     ▼
┌──────────────────────────┐
│ tracked_state.delta_pack │
├──────────────────────────┤
│ key: commit_id           │
│ tracked row deltas[]     │
│ - schema_key             │
│ - entity_id              │
│ - file_id                │
│ - deleted                │
│ - change_locator         │
│ - snapshot_ref ──────────┼──────┐
│ - metadata_ref ──────────┼──┐   │
└──────────────────────────┘  │   │
                              │   │ json_ref
                              ▼   ▼
                    ┌───────────────────┐
                    │  json_store.pack  │
                    │ packed small JSON │
                    └───────────────────┘

                    ┌───────────────────┐
                    │  json_store.json  │
                    │ direct large JSON │
                    └───────────────────┘
```

Current problem:

```text
tracked_state.delta_pack contains row deltas and also acts as the authored
change source for this tracked write path.

That makes the lower commit/changelog layer depend on tracked_state facts.
```

## Current Baseline

Measured with:

```sh
cargo test --manifest-path packages/engine/Cargo.toml --test log11_physical_tracked -- --ignored --nocapture
```

100 inserts in one SQL statement:

```sql
INSERT INTO json_pointer (...) VALUES (... 100 rows ...)
```

```text
commit_store.commit       added=1 bytes=205
tracked_state.delta_pack  added=1 bytes=13,287
json_store.pack           added=1 bytes=34,513
```

100 updates in one SQL statement by primary key:

```sql
UPDATE json_pointer
SET value = CASE path
  WHEN '<pk-1>' THEN lix_json(...)
  WHEN '<pk-2>' THEN lix_json(...)
  ...
END
WHERE path IN ('<pk-1>', '<pk-2>', ...)
```

```text
commit_store.commit       added=1 bytes=205
tracked_state.delta_pack  added=1 bytes=13,318
json_store.pack           added=1 bytes=20,896
json_store.json           added=1 bytes=110,035
```

100 deletes in one SQL statement by primary key:

```sql
DELETE FROM json_pointer WHERE path IN ('<pk-1>', '<pk-2>', ...)
```

```text
commit_store.commit       added=1 bytes=205
tracked_state.delta_pack  added=1 bytes=13,187
```

## Proposed Model

Core rules:

```text
changelog.commit is canonical for one logical commit's header and membership
facts.
changelog.change is canonical for one row/entity change and its payload refs.
A row's durable truth is the changelog.change plus referenced payload objects.
changelog.segment is the physical container for changelog.commit and
changelog.change objects.
tracked_state is a derived, rebuildable index over changelog facts.
```

Dependency direction:

```text
transaction coordinator
  ├─ writes changelog.commit into changelog.segment
  ├─ writes changelog.change objects into changelog.segment
  ├─ writes rebuildable changelog indexes
  └─ writes tracked_state.projection -> tracked_state.root

changelog.commit / changelog.change / changelog.segment ──► tracked_state is forbidden
```

Mental model:

```text
changelog.commit
  catalog object for one Lix Commit
  not just the commit header
  decodes to Commit: commit header and membership records

changelog.change
  catalog object for one row/entity Change
  decodes to Change: tracked key, snapshot/metadata refs, and optional inline payloads

changelog.segment
  physical append/container object
  stores SegmentCommit[] and SegmentChange[]

tracked_state.root
  derived Dolt-like/prolly root per commit
  key -> latest change_id + cached row refs
  optimized for exact reads, scans, and branch diffs

json_store.json
  direct large/deduplicated JSON payloads by json_ref
```

Naming note:

```text
changelog.commit is the catalog object for one logical Commit:
  CommitHeader, MembershipRecords, and CommitDirectory.

changelog.change is the catalog object for one logical Change:
  change_id, tracked key, snapshot_ref, metadata_ref, and inline payloads.

changelog.segment is the physical append/container object:
  SegmentHeader, SegmentDirectory, SegmentCommit[], and SegmentChange[].

SegmentCommit is the encoded segment member for a Commit.
SegmentChange is the encoded segment member for a Change.

The dotted names are catalog/object-type names, not filesystem paths.
```

## 80% Invariants

Atomic visibility:

```text
Publish order:
  1. write json_store.json payloads
  2. write changelog.segment
  3. write optional rebuildable indexes/projections/roots
  4. publish commit_visibility last

A commit is visible only after its non-derived commit_visibility record is
published.
A branch ref names a visible commit_id. Moving a branch ref to a commit must
atomically ensure commit_visibility exists for that commit.
changelog.index.by_commit is rebuildable and is not the visibility source.
Any index/projection/root object for an unpublished commit can be ignored or rebuilt.
```

Visibility and physical relocation:

```text
commit_visibility is the canonical publication record for commit_id.
It may include segment_id + offset/len/checksum for recovery/direct lookup.
Those locator fields are relocatable physical placement, not commit identity.

Segment scavenge may update commit_visibility locator fields, but must not
change commit_id, parent_commit_ids, membership change_ids, or commit
checksum/hash. Old segment bytes remain retained until new physical locations
are durably published.
```

Truth closure:

```text
CommitHeader
  -> MembershipRecords
  -> changelog.change
  -> InlinePayloads or json_store.json

tracked_state.root is not in the truth closure.
Physical pack/segment placement is not logical identity.
```

Tracked key:

```text
TrackedKey = schema_key + file_id + entity_id
```

Commit and change identity:

```text
CommitHeader.lix_commit_change_id is the derived own change id for the
lix_commit projection.
changelog.change objects are first-class row/entity changes.
MembershipRecords reference change_id.
tracked_state.root leaves reference change_id.
by_change maps change_id -> physical changelog.change location.
change_id is logical identity, not physical placement.
```

Change visibility:

```text
by_change is a physical locator index only.
A changelog.change is visible to normal readers only when its change_id is
reachable from a visible changelog.commit membership record.
A raw by_change hit does not imply visibility.
```

Directories and indexes:

```text
SegmentDirectory makes each physical segment self-describing.
CommitDirectory makes each logical changelog.commit self-describing.
Both directories are enough to rebuild global indexes.
changelog.index.* accelerates cross-segment lookup.
by_commit and by_change are mandatory rebuildable indexes.
by_key is mandatory if key/entity history is a product path; without it,
key-history queries may scan changelog.change objects.
by_commit stores parent edges and generation numbers; skip/closure indexes are
needed for bounded ancestry/merge-base queries.
```

Key indexes:

```text
by_key_value optional:
  TrackedKey -> candidate change_ids
  answers "which row versions existed for this key?"

by_key_commit optional:
  TrackedKey -> candidate commit_id + member_change_id pairs
  answers "which commits included/touched this key?"

Merge commits can include an existing change_id without authoring a new
changelog.change, so value history and commit-touch history are distinct.
```

Logical and physical segmenting:

```text
One `changelog.commit` represents exactly one logical commit.
A physical changelog.segment may contain many changelog commits and changes.
commit_visibility publishes individual changelog commits, not whole physical segments.
Physical segments have hard byte/record caps to bound directory memory and GC
retention amplification.
```

Commit membership coalescing:

```text
Within one logical changelog.commit, membership is coalesced to at most one
winning member_change_id per TrackedKey.

Multiple writes to the same TrackedKey within one transaction produce one net
durable changelog.change for the tracked state model.

If an operation log is later needed for audit, it is a separate object and not
part of tracked_state.root semantics.
```

## Proposed Catalog

```text
                         ┌──────────────────────────────┐
                         │ transaction coordinator       │
                         │ builds one atomic write set   │
                         └───────────────┬──────────────┘
                                         │
        ┌────────────────────────────────┼────────────────────────────────┐
        ▼                                ▼                                ▼
┌──────────────────────────────┐ ┌──────────────────────────────┐ ┌──────────────────────────────┐
│ changelog.segment            │ │ changelog indexes            │ │ tracked_state.projection     │
│ PHYSICAL CONTAINER           │ │ rebuildable accelerators     │ │ commit -> root_ref           │
├──────────────────────────────┤ ├──────────────────────────────┤ ├──────────────────────────────┤
│ key: segment_id              │ │ by_commit                    │ │ key: commit_id               │
│                              │ │   commit_id                  │ │ root_ref                     │
│ SegmentHeader                │ │   -> segment_id              │ │ parent projection refs       │
│ SegmentDirectory             │ │    + offset/len/checksum     │ │ changed_key_count            │
│   commit_id/change_id        │ │                              │ │ row_count estimate           │
│   -> offset/len/checksum     │ │ by_change                    │ └──────────────┬───────────────┘
│                              │ │   change_id                  │                │
│ segment.commits[]            │ │   -> segment_id              │                ▼
│   SOURCE OF COMMIT FACTS     │ │    + offset/len/checksum     │ ┌──────────────────────────────┐
│                              │ │                              │ │ tracked_state.root           │
│ segment.changes[]            │ │ by_key optional              │ │ derived prolly tree          │
│   SOURCE OF ROW FACTS        │ │   tracked key                │ ├──────────────────────────────┤
│                              │ │   -> candidate change_ids    │ │ key ranges                   │
│ SegmentCommit decodes to:    │ └──────────────────────────────┘ │ subtree hashes               │
│ - CommitHeader              │                                  │ leaf: tracked key            │
│ - MembershipRecords          │                                  │   -> change_id               │  │
│ - CommitDirectory            │                                  │   -> deleted                 │  │
│                              │                                  │   -> snapshot_ref cache      │  │
│                              │                                  │   -> metadata_ref cache      │  │
│                              │                                  └──────────────────────────────┘  │
│ SegmentChange decodes to:    │                                                                    │
│ - change_id                 │                                                                    │
│ - TrackedKey                │                                                                    │
│ - snapshot_ref              │                                                                    │
│ - metadata_ref              │                                                                    │
│ - InlinePayloads            │                                                                    │
└──────────────┬───────────────┘                                  └──────────────────────────────┘  │
               │ tracked_state and memberships point to changelog.change by change_id                │
               └────────────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────┐
│ commit_visibility            │
│ NON-DERIVED PUBLISH EDGE     │
├──────────────────────────────┤
│ commit_id                    │
│ -> segment_id + offset/len   │
│    + checksum                │
│ readers trust this for       │
│ commit visibility            │
└──────────────────────────────┘

Large payloads:

changelog.change / InlinePayloads
  └─ json_ref ──► json_store.json
```

## Segment Shape

```text
changelog.segment/<segment_id>
  SegmentHeader
    segment_id
    format_version
    commit_count
    change_count
    byte_count
    payload_count
    checksum / hash

  SegmentDirectory
    commit_id -> offset / len / checksum
    change_id -> offset / len / checksum

  segment.commits[]   // SegmentCommit[]
  segment.changes[]   // SegmentChange[]
```

```text
Commit
  CommitHeader
    commit_id
    parent_commit_ids
    lix_commit_change_id
    membership_count
    checksum / hash

  MembershipRecords
    member_change_id: change_id
    member_role: authored/adopted
    source_parent_ordinal optional

  CommitDirectory
    tracked_key -> member_change_id
    member_change_id -> membership_record_ordinal
```

```text
Change
  change_id
  authored_commit_id optional provenance
  schema_key
  entity_id
  file_id
  TrackedKey = schema_key + file_id + entity_id
  snapshot_ref
  metadata_ref

  InlinePayloads
    packed small JSON payloads
    json_ref -> offset/len

  ChangeDirectory
    payload_ref -> offset/len, indexed
```

## Addressing

Lix separates authored change identity from payload identity:

```text
change_id
  addresses the exact authored row/entity change
  optimized for merge membership, projection rebuild, GC, and physical repacking

by_change
  maps change_id to the current physical changelog.change location
  rebuildable from changelog.segment directories

json_ref
  addresses JSON payload bytes
  optimized for payload dedup, verification, and large payload reuse
```

`changelog.change` stores `snapshot_ref` / `metadata_ref` as logical `json_ref`
values, not physical payload locations. Inline placement lives in the
changelog.change `ChangeDirectory.payload_ref -> offset/len`; if a ref is not
local to the changelog change, readers load it from `json_store.json`.

This differs from systems that content-address most objects. The tradeoff is
intentional: two identical payloads can still be different authored changes with
different commits, metadata, membership, and history. Lix dedups payload bytes;
it does not collapse authored change identity.

`changelog.change` objects are pure row facts. They do not store `op`, `tombstone`, or
physical segment placement.

```text
delete
  = snapshot_ref is null

insert/update/delete
  = interpretation from tracked_state.diff before/after refs

commit ownership
  = changelog.commit membership records reference change_id
  = authored_commit_id is provenance on changelog.change, not ownership required
    for reachability

payload placement
  = snapshot_ref / metadata_ref are json_ref values
  = ChangeDirectory.payload_ref -> offset/len finds inline payloads
  = missing local payload_ref resolves through json_store.json
```

Inline payload scope:

```text
Inline payloads are local to one changelog.change object in the MVP.
ChangeDirectory.payload_ref -> offset/len resolves only local inline payloads.
If the json_ref is not present in the local ChangeDirectory, readers resolve it
through json_store.json.
Cross-change or segment-level inline payload dedup is a later physical
optimization.
```

## Tracked State Root

Every durable commit gets one logical `tracked_state.root`.

That root is the derived state index for all tracked rows at the commit. This
matches the current Lix shape more than "one root per schema": current
`tracked_state.tree.root` is keyed by `commit_id`, and its tree keys are
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
  -> tracked_state.projection
  -> tracked_state.root
  -> TrackedKey lookup / diff / scan
  -> change_id + deleted/snapshot_ref/metadata_ref cache
  -> changelog.change only for truth hydration
```

`tracked_state.root` is a real prolly-style tree, but derived. It is also a
covering state index for the hot path:

```text
TrackedKey ->
  change_id
  deleted
  snapshot_ref cache
  metadata_ref cache
  optional scalar/header cache
```

The cache values are copies of changelog truth. If they are missing, stale, or
discarded, they are rebuilt from `changelog.commit` membership and
`changelog.change` facts. The primary key space is ordered by:

```text
TrackedKey = schema_key + file_id + entity_id
```

Schema reads are prefix/range reads over that key space. If schema count or
schema-level churn later makes a single composite-key tree too broad, the same
logical model can evolve to a Dolt-style root-of-roots:

```text
tracked_state.root
  schema_key -> schema_root_ref
    -> file_id + entity_id -> change_id + cached refs
```

That is an internal physical optimization. The visible read model remains
`commit -> tracked root -> TrackedKey -> cached row refs`.

```text
root-covered read: O(log_B N)
truth hydration:   O(log_B N + by_change lookup)
write:       naive O(K log_B N), batched expected O(T + K)
branch diff: target O(D), precise expected O(T + D), pathological O(N)
full scan:   O(N)

N = tracked rows in the commit root
K = changed rows in one commit
D = emitted changed keys between two roots
T = changed/visited prolly nodes whose hashes differ
B = prolly fanout / leaf capacity
```

Every durable commit persists its `tracked_state.root`. Mutations into the root
are sorted by `TrackedKey` and applied as one batch per commit. Tree bytes written
are `O(T * node_bytes)`; scattered keys can make `T` approach
`K * tree_height`. This is the write-amplification tradeoff for `O(log_B N)`
reads and Dolt-style `O(D)` target diffs. The precise implementation accounting
is `O(T + D)` because the diff must visit changed internal/leaf nodes before
emitting changed keys.

A visible commit remains readable if its `tracked_state.projection` or
`tracked_state.root` is missing. The slow path rebuilds from the nearest
available ancestor root plus visible commit membership records. If no ancestor
root exists, rebuild starts from the initial root through reachable ancestry.
Normal hot-path read bounds do not apply until the derived root is rebuilt.

Prolly chunk boundaries are a function of `TrackedKey` only, not `change_id`,
`snapshot_ref`, `metadata_ref`, or cached leaf values.
Re-pointing an existing key to a new change_id must not move chunk boundaries.
That is the Dolt keys-only chunking rule that keeps ordinary updates from
inflating `T`.

No MVP delta layers, checkpoint policy, or slice-replay read path. Later
optimizations should happen inside prolly node encoding, segment packing,
payload colocation, and optional secondary indexes without changing the read
model. A later write-amplification escape hatch may let persisted roots lag the
newest commits and derive the tail in memory, Sapling IndexedLog-style. A later
Neon-style alternative is delta layers plus periodic full roots, trading direct
root lookup for bounded replay.

## Workflow Cases

```text
insert rows
  changelog: +1 CommitHeader, +N MembershipRecords, +N changelog.change objects
  tracked: update parent root with inserted keys -> change_id
  json_store.json: only large payloads

update rows
  changelog: +1 CommitHeader, +N MembershipRecords, +N changelog.change objects
  tracked: update parent root with updated keys -> change_id
  json_store.json: only large payloads

delete rows
  changelog: +1 CommitHeader, +N MembershipRecords, +N changelog.change objects
    with snapshot_ref = null
  tracked: update parent root with deleted keys -> change_id
  json_store.json: none

branch from commit
  changelog: no row facts
  tracked: reuse existing root
  metadata: branch/ref -> commit_id

write on branch
  changelog: new commit has parent = previous branch head
  tracked: derive new root from previous branch head root
  metadata: move branch/ref -> new_commit_id

merge commit
  changelog: merge CommitHeader + MembershipRecords for adopted change_ids
  tracked: derive merge root by applying adopted and authored change_ids
  copied: no adopted changelog.change objects, no adopted payload bytes
```

Merge membership is deterministic net state after planning:

```text
MembershipRecords contain adopted change_ids that survive conflict checks.
For a tracked key, the merge result has one visible winning change_id.
If the merge authors a row change for the same key, that authored row wins.
MembershipRecords are for changed/adopted keys, not all keys in the result.
Merge storage is O(M + A), never O(N).
```

Merge adoption path:

```text
merge_commit
  -> commit_visibility / by_commit
  -> segment_id + offset/len/checksum
  -> changelog.commit bytes
  -> MembershipRecords.member_change_id
  -> by_change
  -> changelog.change
  -> tracked key / snapshot_ref / metadata_ref
  -> payload hydration only if needed
```

Conflicts are not a storage concept in the MVP. The merge planner can throw on
divergent same-key changes and write no unresolved-conflict records.

## Read Paths

Exact row at version:

```text
tracked_state.projection(commit_id)
  -> root_ref
  -> tracked_state.root.lookup(tracked key)
  -> change_id + deleted/snapshot_ref/metadata_ref cache
  -> hydrate snapshot_ref / metadata_ref only if requested
```

Truth hydration for audit/rebuild:

```text
change_id
  -> by_change physical location
  -> changelog.change
```

Key/header scan:

```text
tracked_state.projection
  -> tracked_state.root key ranges
  -> tracked keys, deleted bits, cached scalar refs
  -> no payload hydration
```

Full scan:

```text
tracked_state.projection
  -> tracked_state.root leaves
  -> change_id + cached row refs
  -> batch hydrate inline payloads and json_store.json
```

Changed keys / branch diff:

```text
tracked_state.root(base_commit)
tracked_state.root(head_commit)
  -> compare subtree hashes by key range
  -> descend changed ranges
  -> return old/new change_id values
  -> do not materialize JSON strings
```

Commit/change lookup:

```text
commit_id -> commit_visibility/by_commit -> segment_id + offset/len/checksum
change_id -> changelog.index.by_change -> segment_id + offset/len/checksum
```

## Complexity Contract

Notation:

```text
N = tracked rows visible at a commit
K = changed rows in one write
A = authored merge rows
M = adopted merge change_ids
D = emitted changed keys between two roots
T = changed/visited prolly nodes whose hashes differ
B = prolly fanout / leaf capacity
C = commits
R = changelog.change objects
P = payload bytes read/written
I(k) = cost to resolve k change_ids through by_change
S_change = distinct segments touched while hydrating changelog.change
S_payload = distinct payload blobs/segments touched while hydrating payloads
Q = membership records or root entries emitted by an operation
```

Target bounds:

```text
branch from commit
  O(1)

exact row at commit
  root-covered refs/header: O(log_B N)
  external payload by cached ref: add P + S_payload
  inline payload / truth hydration: add I(1) + S_change + P

get_many(m keys)
  arbitrary refs/header:   O(m log_B N)
  sorted/clustered refs:   O(log_B N + m)
  external payloads:       add P + S_payload
  inline/truth hydration:  add I(m) + S_change + P

insert/update/delete K rows
  segment:      O(K + P)
  tracked root: naive O(K log_B N), batched expected O(T + K)
  indexes:      O(K) by_change writes, O(1) by_commit write per commit
  membership:   O(K) change_id refs in changelog.commit

commit -> changed change_ids
  O(1 + Q)

commit -> full changed row facts
  O(Q + I(Q) + S_change + P + S_payload)

commit -> inserts/updates/deletes classification
  root-covered classification: O(T + D)
  truth hydration:             add I(D) + S_change
  optional later op cache may lower constants

change_id lookup
  O(1) expected via by_change

entity/key history
  with by_key:    O(log R + H)
  without by_key: O(R)

schema/file range scan
  root-covered refs/header: O(log_B N + Q)
  external payloads:        add P + S_payload
  inline/truth hydration:   add I(Q) + S_change + P

full scan
  root-covered refs/header: O(N)
  external payloads:        add P + S_payload
  inline/truth hydration:   add I(N) + S_change + P

branch diff
  target O(D) in Dolt/prolly terms
  precise expected O(T + D)
  pathological O(N) if chunk/hash sharing fails broadly

merge
  conflict planning: precise expected O(T + D), pathological O(N)
  truth hydration:   add I(D) + S_change if planner needs Change facts
  write result:      naive O((M + A) log_B N), batched expected O(T + M + A)
  storage:           O(M + A), never O(N)

ancestry / merge-base
  parent edges + generation numbers: O(visited ancestors)
  target O(log C) or better requires skip/closure index

rebuild changelog indexes
  O(R + C + membership_records)

rebuild tracked root for one commit
  from existing parent root: O(Q log_B N) naive, batched expected O(T + Q)
  if cached refs must be rebuilt from Change facts: add I(Q) + S_change
  if ancestor roots are missing: sum this over missing commits from nearest root

GC mark/sweep
  O(reachable commits + reachable membership records + reachable changes
    + reachable payload refs + segment objects)
  retained bytes are whole reachable segments until segment scavenge
```

## GC / Compaction

Keep this small for the first implementation:

```text
MVP:
  immutable segments
  mark/sweep whole objects only
  no segment rewrite
  no changelog reference rewrite

Reachability roots:
  branch heads
  pinned commits
  sync/remote refs

Reachability edges:
  commit -> parent commits
  commit -> membership change_ids
  change_id -> changelog.change
  changelog.change -> json_store.json, for large payloads

Provenance:
  changelog.change.authored_commit_id is provenance only
  an adopted change does not keep its authoring commit or parents alive unless
  that commit is also reachable through refs, pins, or commit ancestry

Derived retention:
  tracked_state objects are retained only for reachable commits
  tracked_state never keeps changelog or payload facts alive

Later:
  MVP segments are L0-like: time-partitioned, all-key segments.
  Segment scavenge is L1-like key/range compaction.
  segment scavenge may copy live changelog commits and changes into new physical segments
  and update commit_visibility / by_commit / by_change physical locations
  change_id values do not change during physical repacking
```

## GC Cases

GC marks from publication roots, not from derived indexes:

```text
branch/ref/pin/remote root
  -> visible commit_id
  -> changelog.commit
  -> parent commits
  -> membership change_ids
  -> changelog.change
  -> json_store.json, for large payloads
```

`tracked_state.*`, `changelog.index.*`, and cached projections keep nothing
alive. They are retained only when attached to reachable commits and may be
deleted/rebuilt.

| Case                                                   | Keep                                                                           | Sweep                                             | Note                                                               |
| ------------------------------------------------------ | ------------------------------------------------------------------------------ | ------------------------------------------------- | ------------------------------------------------------------------ |
| Reachable normal commit                                | `changelog.commit`, membership `changelog.change` objects, referenced payloads | unrelated objects                                 | Standard mark path.                                                |
| Unreachable commit with no referenced changes          | nothing after retention horizon                                                | commit, changes, payloads                         | Retention horizon is policy, not layout.                           |
| Unreachable authoring commit, adopted change reachable | adopted `changelog.change` and payloads                                        | authoring commit if otherwise unreachable         | `authored_commit_id` is provenance only.                           |
| Reachable commit, missing `tracked_state.root`         | commit/change/payload truth                                                    | missing root stays missing until rebuilt          | Reader rebuilds derived root from changelog facts.                 |
| Reachable `tracked_state.root`, unreachable commit     | nothing solely because of root                                                 | root/projection may be swept                      | Derived state is not a retention root.                             |
| Large JSON referenced by reachable change              | `json_store.json` payload                                                      | none                                              | Payload is in truth closure through `changelog.change`.            |
| Large JSON referenced only by unreachable change       | none after retention horizon                                                   | payload                                           | Sweep after no reachable change references it.                     |
| Inline payload in mixed live/dead segment              | whole segment in MVP                                                           | none until scavenge                               | Whole-segment retention causes byte amplification.                 |
| Mixed live/dead physical segment                       | whole segment in MVP                                                           | old segment after scavenge                        | Scavenge copies live commits/changes and updates physical indexes. |
| Stale/missing `by_commit` / `by_change` / `by_key`     | visible truth objects                                                          | stale index records                               | Rebuild from segment directories and visible commits.              |
| Missing `commit_visibility`                            | nothing is published by that edge                                              | unpublished commit/change objects if unreferenced | Segment bytes alone do not publish commits.                        |
| Pinned commit or remote/sync ref                       | same closure as branch head                                                    | unrelated objects                                 | Pins and remote refs are GC roots.                                 |

## Clean Cuts

```text
1. Replace commit_store/tracked_state.delta_pack source split with changelog.segment.
2. Make tracked_state only a derived prolly root plus projection refs.
3. Move small JSON payloads into changelog.change InlinePayloads.
4. Keep large/deduplicated payloads in json_store.json.
5. Make change_id the logical identity for row/entity changes.
6. Keep changelog indexes rebuildable.
7. Delete changelog/commit_store fallback into tracked_state.
```
