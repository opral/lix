# ForkTree replacement-layout prototype

This benchmark-only prototype tests a replacement physical model. It is not a
production index and is deliberately not registered with Lix serving paths.

## Authority and invariants

- One immutable `forktree_objects` key space stores every authenticated object.
  The key is BLAKE3 of the complete tagged encoding. Tree nodes, semantic
  deltas, commits, blob manifests, and blob chunks therefore share one object
  identity and one prospective reachability universe.
- One tiny mutable `forktree_refs` key space stores branch/checkpoint pins and
  one publication/GC epoch. A root move and epoch rotation are one adapter
  commit guarded by exact preconditions.
- Every commit names one tree root and one semantic delta. Nodes use canonical
  encodings and sorted entries. Leaves use deterministic 8-row blocks with
  level-1 Zstandard packing and map sorted keys to authenticated `(value-pack
  hash, slot)` references. Bulk construction writes one value pack per leaf so
  point and range reads never materialize a whole-import pack. Internal nodes
  use deterministic eight-child blocks and front-coded separators; child hashes
  remain the authoritative links. Each transaction writes one deterministic
  value pack for all changed values. Ordinary semantic deltas name that pack
  once and list its aligned sorted keys; the initial delta is a compact
  authenticated bulk-root declaration rather than a redundant enumeration of
  the imported snapshot. Packs remain in the same object space and reachability
  graph; they are not a side index or second format. Initial trees are
  deterministically bulk packed; value-only updates rewrite only touched leaves
  and ancestor paths.
- Blob manifests and FastCDC chunks use the same object keys, commit edge, and
  reachability walk as row objects. An unchanged branch copies only a selector;
  a localized edit writes only newly identified chunks and metadata.
- Reclamation snapshots the epoch before discovering all selectors, authenticates
  every traversed object, scans the object space in bounded pages, and rotates
  the same epoch on each deleting page. A publication after root discovery
  therefore rejects the stale sweep rather than losing a newly live object.
- Derived caches or indexes are outside the authoritative model and must be
  rebuildable from pinned commit roots.

## Complexity

Let `B` be leaf capacity, `F` internal fanout, `N` live rows, `K` sorted
mutations, and `Z` newly materialized objects.

- Bulk build: `O(N)` CPU/reads, `O(N/B)` node objects, and bounded encoder
  working memory apart from the caller-owned sorted fixture.
- Focused value-only apply: `O(K log_F N + Z)` object reads/writes and
  `O(K + B log_F N)` working memory. Unchanged subtrees are referenced by hash.
- Point read: `O(log_F N)` object reads and `O(B + log_F N)` decoded memory.
- Hash-skipping diff synthesis: equal hashes stop traversal; aligned trees cost
  `O(D log_F N + Z_d)` for changed regions, with a full-tree fallback required
  when key-set edits change packing boundaries.
- Branch/checkpoint/undo/redo movement: `O(1)` mutable ref writes plus one epoch
  rotation; no tree objects are copied.
- Blob ingest and whole-payload rechunking: `O(L)` CPU. A localized edit writes
  `O(Z + metadata)` immutable bytes, while unchanged chunks remain hash
  references. Range read is `O(requested bytes + touched chunk bytes)` and
  bounded by the maximum chunk size.
- Global reclamation: `O(P + R + O)` for selectors `P`, reachable objects `R`,
  and scanned orphan candidates `O`. Scan memory is one page, but the prototype
  retains `O(R)` unique object IDs; a production implementation still needs a
  bounded or external mark representation.

The current update gate intentionally excludes key-set edits. Canonical local
repacking for inserts/deletes is a subsequent prototype question, not a hidden
compatibility path.

The exact vertical-slice measurements, known limits, and hard-cut decision are
in [RESULTS.md](RESULTS.md).

## Evidence boundary

Properties taken from prior work are kept separate from this synthesis:

- Fully persistent B-trees establish external-memory persistence bounds.
- PaC-trees establish that purely functional ordered trees can combine blocked
  leaves, compression, and batch operations.
- ForkBase POS-trees and Dolt Prolly-trees establish content-addressed,
  content-defined blocked Merkle trees with structural sharing and hash-skipping
  comparison.
- Persistent B-epsilon trees establish buffered persistent update bounds, but
  the recent construction is partially persistent rather than fully persistent.
- cMVBT establishes a continuous-GC design for a concurrent multiversion B-tree;
  it does not prove GC behavior for this immutable object synthesis.
- Sapling's segmented commit graph motivates keeping high-level graph traversal
  separate from lazily loaded commit detail; it does not determine row layout.

ForkTree's single object space, fixed canonical bulk packing, semantic delta
objects, and tiny ref/epoch plane are our synthesis and must be judged by the
measurements and invariants in this benchmark.
