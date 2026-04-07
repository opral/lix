---
date: "2026-04-03"
og:description: "500 real commits replayed with no corruption bugs. Without the semantic layer, Lix is ~8x faster than Git, but semantic writes still bottleneck on write amplification."
og:image: "./8x-faster.png"
og:image:alt: "March 2026 update: no corruption bugs, 8x faster than Git, semantic writes still too slow"
---

# March 2026 Update: No Corruption Bugs, 8x Faster Than Git, Semantic Writes Still Too Slow

**TL;DR**

- Workload testing worked: 500 real commits replayed with no state corruption bugs
- Semantic writes still hit a write-amplification bottleneck on large files (500ms+)
- Without the semantic layer, the file-write-plus-commit workflow is ~8x faster than Git
- April goal: sub 100ms for 10k entity inserts

## Workload testing

[Last month](/blog/february-2026-update) we set out to do real workload testing in March to reveal performance bottlenecks and bugs that prevent production usage of lix.

The test replays 500 real commits from the [paraglide-js](https://github.com/opral/paraglide-js) repo. For each commit, it sets up the "before" state outside the timer, applies the same file changes, and measures how long Lix takes to commit. The simulated scenario: "I edited some files, now I'm committing."

Three findings came out of this.

### Finding 1: It works

The best result from the workload replay is that it worked. Replaying 500 real commits did not reveal state corruption bugs. That matters more than the benchmark number because correctness is the prerequisite for everything else.

### Finding 2: Semantic writes still bottleneck on write amplification

> [!NOTE]
> **Refresher: What is the semantic layer?**
>
> The semantic layer is the representation where a file stops being a blob and becomes entities and changes.
>
> ```
>   contract.docx
>        ↓
>   paragraphs / tables / images
>        ↓
>   diff / merge / history on those units
> ```

The bottleneck is write amplification. A single file write fans out into many entity rows. Inserting a file with 10k entities means the engine has to process 10k entity rows. On the current path, semantic writes are multi-second operations. Any interaction above 100ms stops feeling instantaneous, so this needs to come down.

```
  contract.docx            Lix engine                    SQL database
  ┌──────────────────┐     ┌─────────────────────┐       ┌──────────────┐
  │ Paragraph 1      │     │ process 10,000      │       │              │
  │ Paragraph 2      │     │ entity rows         │       │ INSERT row 1 │
  │ Paragraph 3      │────►│                     │──────►│ INSERT row 2 │
  │ Table 1          │     │ validate, transform,│       │ ...          │
  │   Row 1          │     │ detect changes      │       │ INSERT row   │
  │   Row 2          │     │                     │       │   10,000     │
  │ Image 1          │     │ 💥 too slow          │       │              │
  │ ...              │     └─────────────────────┘       └──────────────┘
  │ Paragraph 4,291  │
  └──────────────────┘
  1 file write             N entities to process           N SQL row inserts
```

The engine is not fast enough to handle these large batches. The goal for April is to get 10k entity inserts under 100ms.

### Finding 3: Without the semantic layer, the file-write-plus-commit workflow is ~8x faster than Git

Unexpected good news. Without the semantic layer (treating files as blobs), Lix completes the same file-write-plus-commit workload in ~5 ms where Git takes ~39 ms.[^1]

[^1]: Measured on a MacBook Pro M5 Pro (18-core), SQLite in WAL mode.

| Phase       | Git        | Lix       |
| ----------- | ---------- | --------- |
| File writes | ~0.2 ms    | ~3.6 ms   |
| Commit      | ~39 ms     | ~1 ms     |
| **Total**   | **~39 ms** | **~5 ms** |

The difference comes down to architecture. Lix applies mutations inside an open SQLite transaction. Committing is closing that transaction (~1 ms). The comparison runs `git add -A` followed by `git commit`, which scans the working tree, updates the index, and writes tree and commit objects.

This is encouraging, but it's the blob layer only. The semantic layer is what makes Lix useful for non-code files, and that's where the work is.

### Why not skip the semantic layer entirely?

If Lix is already fast without the semantic layer, why not just store blobs and diff on the fly?

This is really a source-of-truth decision, not a storage decision. Lix can keep both a blob and semantic state, but only one can be authoritative:

```
  Option A: Blob is source of truth, diffs computed on the fly

  ┌──────────────┐       ┌──────────────┐
  │ contract.docx│──────►│  re-parse    │──────► diffs (computed every time)
  │   (blob)     │       │  on every op │
  └──────────────┘       └──────────────┘


  Option B: Diffs are source of truth, blob derived on demand

  ┌──────────────┐       ┌──────────────┐
  │    diffs     │──────►│  serialize   │──────► contract.docx (derived)
  │  (stored)    │       │  on demand   │
  └──────────────┘       └──────────────┘
```

If both are independently writable, they can drift.

Git gets away with blob-first storage because its default diff and merge model is line-oriented and works well for ordinary text. For smaller structured text files like JSON, re-parsing on demand can still be acceptable. But as files grow, the cost per operation grows with them:

| File type           | Size      | Rebuild cost per operation |
| ------------------- | --------- | -------------------------- |
| `.js` source file   | ~0.005 MB | trivial                    |
| Large JSON config   | ~0.5 MB   | acceptable                 |
| `.docx` with images | ~5 MB     | slow                       |
| `.xlsx` spreadsheet | 5-20 MB   | 💥 too slow                |

OOXML files like `.docx` and `.xlsx` are ZIP packages made of many XML parts, so rebuilding semantic state from the blob on every merge, history read, or sync means repeatedly paying unzip, parse, and tree-diff costs. A cache avoids repeated rebuilds, but now there are two representations to keep consistent — every write path must update both, and bugs in that synchronization are silent data corruption.

So Lix makes semantic state canonical and materializes the blob on demand when someone actually needs the file bytes. The tradeoff is that blob writes pay an upfront parsing cost — which is the write-amplification bottleneck we're now fixing.

Long term, most app and agent writes should bypass blob parsing entirely. They will write entities directly, so the hot path avoids both blob parsing and blob serialization.

That means the semantic layer must be fast.

## Prolly trees for cheap versioning

Solving write speed alone isn't enough — storage also needs to scale across versions. Without content deduplication, creating a new version means duplicating all entity data. A 10k-entity Word document across 5 versions = 50k rows stored.

```
  Without deduplication:

  version: main              version: draft
  ┌──────────────────┐       ┌──────────────────┐
  │ 10,000 entities  │       │ 10,000 entities  │  ← full copy
  └──────────────────┘       └──────────────────┘
  💥 10,000 rows              💥 10,000 rows (copied)
```

[Prolly trees](https://docs.dolthub.com/architecture/storage-engine/prolly-tree) are the most promising fit for this. Entities are grouped into chunks with boundaries determined by content hashes. If one paragraph changes, only the chunk containing that paragraph is new. The rest is shared across versions.

```
  With Prolly trees:

  version: main                       version: draft
  (original)                          (paragraph 3 edited)
  ┌──────────────────┐                ┌──────────────────┐
  │ Paragraph 1      │                │ Paragraph 1      │
  │ Paragraph 2      │                │ Paragraph 2      │
  │ Paragraph 3      │                │ Paragraph 3 ✎    │
  │ Table 1          │                │ Table 1          │
  │ ...              │                │ ...              │
  │ Paragraph 4,291  │                │ Paragraph 4,291  │
  └──────────────────┘                └──────────────────┘
          │                                   │
          ▼                                   ▼
  ┌──────────────┐                    ┌──────────────┐
  │   chunk A  ──┼────────────────────┼── chunk A    │  ← shared
  │   chunk B    │                    │   chunk B'   │  ← different (contains edited paragraph 3)
  │   chunk C  ──┼────────────────────┼── chunk C    │  ← shared
  │   chunk D  ──┼────────────────────┼── chunk D    │  ← shared
  └──────────────┘                    └──────────────┘

  ✅ Creating a version = pointing to the same chunks
  ✅ Only changed chunks are stored separately
```

## What's next in April

**1. Sub 100 ms for 10k entity inserts.** SQLite itself can insert 10,000 rows in under 10 ms. That means the raw database is not the bottleneck — the engine's per-row overhead is. The target is aggressive, but the headroom is there.

**2. Prolly trees for cheap branching.** Branching and merging need content-deduplicated storage across versions. Without that, large semantic files get copied over and over again. With prolly trees, unchanged chunks are shared and only changed chunks are new.

**3. Real workload testing with the semantic layer enabled.** March already showed that the replay workload works and did not surface corruption bugs on the blob path. April is about proving the same thing with semantic writes turned on.

If April lands those three pieces, we can release Lix for broader testing.
