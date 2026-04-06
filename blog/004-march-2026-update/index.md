---
date: "2026-04-03"
og:description: "Real workload testing revealed the semantic layer is too slow. Here's why and how we'll fix it."
og:image: "./8x-faster.png"
og:image:alt: "March 2026 update: workload testing succeeded, semantic layer too slow"
---

# March 2026 Update: Workload Testing Succeeded, Semantic Layer Too Slow

**TL;DR**

- Workload testing revealed the semantic layer is too slow (500ms+ for large files)
- Without the semantic layer, Lix commits are ~8x faster than Git
- April goal: sub 100ms file writes for files with 10k entities via Prolly tree chunking

## Workload testing

[Last month](/blog/february-2026-update) we set out to do real workload testing in March to reveal performance bottlenecks and bugs that prevent production usage of lix.

The test replays 500 real commits from the [paraglide-js](https://github.com/opral/paraglide-js) repo. For each commit, it sets up the "before" state outside the timer, applies the same file changes, and measures how long Lix takes to commit. The simulated scenario: "I edited some files, now I'm committing."

Two findings came out of this.

### Finding 1: The semantic layer is too slow

> [!NOTE]
> **Refresher: What is the semantic layer?**
>
> Lix's semantic layer parses files into structured entities. Instead of "binary files differ", Lix understands what actually changed inside a file. For example, a `.docx` becomes paragraphs, tables, images.
>
> ```
> Git sees:                     Lix sees:
>
> -Binary files differ          paragraph 3 in `contract.docx` changed:
>                               - "The contract expires on March 1st."
>                               + "The contract expires on April 1st."
> ```

A file insert with N entities is translated to N direct SQL rows.

Thus, inserting a file with, for example, 10k entities translates to one file write that triggers at least 10k rows being written to the SQL database. Testing revealed that writing Word files can quickly turn into 500ms+ operations where >80% of the time is spent writing SQL rows. Any interaction above 100ms is perceived as lag by humans, so this needs to come down.

```
  contract.docx
  ┌──────────────────┐
  │ Paragraph 1      │        SQL database
  │ Paragraph 2      │        ┌──────────────────────────┐
  │ Paragraph 3      │───────►│ INSERT row 1 (paragraph) │
  │ Table 1          │        │ INSERT row 2 (paragraph) │
  │   Row 1          │        │ INSERT row 3 (paragraph) │
  │   Row 2          │        │ INSERT row 4 (table)     │
  │   Row 3          │        │ INSERT row 5 (row)       │
  │ Image 1          │        │ INSERT row 6 (row)       │
  │ ...              │        │ INSERT row 7 (row)       │
  │ Paragraph 4,291  │        │ INSERT row 8 (image)     │
  └──────────────────┘        │ ...                      │
                              │ INSERT row 10,000        │
  1 file write                └──────────────────────────┘
                              💥 10,000 row inserts
```

### Finding 2: Without the semantic layer, Lix is ~8x faster than Git

Unexpected good news. Without the semantic layer (treating files as blobs), Lix commits in ~5 ms where Git takes ~39 ms for the same workloads.

| Phase       | Git        | Lix       |
| ----------- | ---------- | --------- |
| File writes | ~0.2 ms    | ~3.6 ms   |
| Commit      | ~39 ms     | ~1 ms     |
| **Total**   | **~39 ms** | **~5 ms** |

The difference comes down to architecture. Lix applies mutations inside an open SQLite transaction. Committing is closing that transaction (~1 ms). Git's commit path runs `git add -A` and `git commit`, scanning the working tree, updating the index, and writing tree and commit objects.

This is encouraging, but it's the blob layer only. The semantic layer is what makes Lix useful for non-code files, and that's where the work is.

## Making the semantic layer fast with Prolly trees

The fix is chunking. Instead of inserting one row per entity, group entities into chunks and store each chunk as a single row. 10,000 entities become ~40 chunk inserts instead of 10,000 row inserts.

[Prolly trees](https://docs.dolthub.com/architecture/storage-engine/prolly-tree) are a chunking algorithm where chunk boundaries are determined by content hashes, not fixed positions. That's important because it also solves a second problem: cheap branching.

```
  Before (naive):                     After (chunked):

  contract.docx                       contract.docx
  ┌──────────────────┐                ┌──────────────────┐
  │ Paragraph 1      │                │ Paragraph 1      │
  │ Paragraph 2      │                │ Paragraph 2      │
  │ Paragraph 3      │                │ Paragraph 3      │
  │ Table 1          │                │ Table 1          │
  │ ...              │                │ ...              │
  │ Paragraph 4,291  │                │ Paragraph 4,291  │
  └──────────────────┘                └──────────────────┘
          │                                   │
          ▼                                   ▼
  ┌──────────────────┐                ┌──────────────────┐
  │ INSERT row 1     │                │ INSERT chunk 1   │
  │ INSERT row 2     │                │  (entities 1-256)│
  │ INSERT row 3     │                │ INSERT chunk 2   │
  │ INSERT row 4     │                │  (entities 257-  │
  │ ...              │                │   512)           │
  │ INSERT row 10,000│                │ ...              │
  └──────────────────┘                │ INSERT chunk ~40 │
                                      └──────────────────┘
  💥 10,000 row inserts                ✅ ~40 row inserts
```

### Bonus: cheap branching

The content-based chunking also solves a (future) branching problem. Without deduplication, creating a new version (branch) means duplicating all entity data. A 10k-entity Word document across 5 versions = 50k rows stored.

```
  Without deduplication:

  version: main              version: draft
  ┌──────────────────┐       ┌──────────────────┐
  │ 10,000 entities  │       │ 10,000 entities  │  ← full copy
  └──────────────────┘       └──────────────────┘
  💥 10,000 rows              💥 10,000 rows (copied)
```

Prolly trees fix this. If one paragraph changes, only the chunk containing that paragraph is new. The rest is shared across versions.

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

### Why not skip the semantic layer entirely?

If Lix is already fast without the semantic layer, why not just store blobs and diff on the fly?

It's an either/or decision. Lix has to pick one source of truth:

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

Trying to do both (blob and diffs as writable) leads to data corruption because they can diverge.

Option A works for Git because source code files are small text. Re-parsing on every operation is cheap. For smaller files like a 300 KB `.docx` or JSON config, it's still acceptable. But as files grow, the cost per operation grows with them:

| File type           | Size      | Re-parse per operation |
| ------------------- | --------- | ---------------------- |
| `.js` source file   | ~0.005 MB | trivial                |
| Large JSON config   | ~0.5 MB   | acceptable             |
| `.docx` with images | ~5 MB     | slow                   |
| `.xlsx` spreadsheet | 5-20 MB   | 💥 too slow            |

Every operation has to compute the diff from scratch: decompress the archive, parse XML, build the entity tree, then diff the two trees. A merge diffs three versions (base, ours, theirs). A history view diffs every version in the timeline. Parsing is fast, but diffing large entity trees is slow. For a 5 MB Word document, that adds up to seconds of wait time per operation. For real-time sync, it's a dealbreaker.

Lix chose Option B. Parse once at write time, then every downstream operation is fast. The blob gets serialized on demand when someone actually needs the file.

That means the semantic layer must be fast.

## What's next in April

**Semantic layer: sub 100 ms for 10k entities.** With Prolly tree chunking, a file with 10,000 entities (a large JSON or a real-world `.docx`) must insert in under 100 ms.

Anything below 100 ms is not perceived as lag. 10,000 entities is the upper end of real-world Word documents we're testing against. This means deviating from the SQL database's standard B-tree storage and managing our own tree structure on top. Whether the tradeoff pays off in practice is what April will tell.
