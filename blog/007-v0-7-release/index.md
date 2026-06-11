---
date: "2026-06-12"
authors: ["samuelstroschein"]
og:description: "Lix v0.7 ships file format plugins and a rebuilt storage engine. CSV, Markdown, and text files become queryable state. Merges run 1.8x faster and commits write half the bytes."
og:image: "./cover.svg"
og:image:alt: "Lix v0.7 release cover announcing plugins and the storage engine rebuild"
---

# Lix v0.7 Release: Plugins and a Rebuilt Storage Engine

![Lix v0.7 release cover](./cover.svg)

**TL;DR**

- Plugins turn file formats into queryable state. CSV, Markdown, and plain text ship in v0.7. A CSV cell edit is a row-level change you can query, diff, and merge.
- The storage engine was rebuilt across twelve PRs. A 10k-row merge through the full plugin pipeline runs 1.8x faster, point reads 2.2x faster, and a commit writes 47% fewer bytes.
- `lix.fs` and filesystem sync: read and write files through an fs API, and mirror a lix into a plain directory and back.

## Files become state

v0.6 stored files as blobs with history. v0.7 understands them.

A plugin teaches Lix a file format. When a file with a matching plugin is written, Lix does not store a new blob. It detects what changed inside the file and stores those changes as state:

```ts
await lix.fs.writeFile("/orders.csv", updatedCsvBytes);

// One edited cell is one change, queryable like any other state.
const changes = await lix.execute(
  "SELECT entity_pk, schema_key FROM lix_change WHERE file_id = 'orders-file'",
);
```

CSV, Markdown, and plain-text plugins ship with v0.7, including reorder detection: moving a row or a paragraph is recorded as a move, not a delete plus an insert. Files without a plugin keep the v0.6 behavior: content-defined chunked blobs, deduplicated across versions.

This is the difference between "the file changed" and "row 4812's price column changed". Diffs become semantic, merges become row-granular, and agents can query exactly what they touched.

## The storage engine rebuild

Plugins multiply the load on the storage engine. A 10k-row CSV is 10k tracked entities, each with its own history. v0.7 spent twelve PRs rebuilding the physical layout, each one measured against the last:

| Benchmark                              | start of the rebuild | v0.7     |
| -------------------------------------- | -------------------- | -------- |
| merge_10k, e2e CSV plugin pipeline     | 347.8 ms             | 190.0 ms |
| read_one_by_pk, engine                 | 213.1 us             | 96.2 us  |
| bytes written per 1k-row insert commit | 827.5 KB             | 436.5 KB |
| backend puts per 1k-row insert commit  | 2,031                | 1,074    |
| space truncate, 20k rows               | 10.7 ms              | ~0.6 ms  |

The design that came out of it:

- Every payload is stored exactly once. A row's current-state entry references the change that produced it; the change record owns the content. The previous design stored payloads in a separate content-addressed store with refs from two places.
- The backend interface is space-aware. Each engine keyspace maps to its own SQLite table instead of one interleaved tree behind prefixed keys. Point reads descend small per-space B-trees. Dropping a space truncates a table.
- Keys and values got smaller: binary UUIDs instead of text, front-coded keys inside chunks, change and commit ids deduplicated into chunk-local dictionaries, and compression only where measurement showed the payload population benefits.

The final design also has fewer concepts than v0.6: one payload location instead of three, no compression on the hot read path, and read latency at parity while writing 47% fewer backend rows per commit.

Every step is recorded in the engine's optimization log, including two designs that were built, measured, and discarded along the way, with the benchmark methodology to reproduce the numbers.

## Filesystem sync

A lix can now mirror to a plain directory and back:

```ts
const lix = await openLix({
  backend: withFilesystemSync(new SqliteBackend({ path: "app.lix" }), {
    directory: "./workspace",
  }),
});
```

Edit files in the directory with any tool and the changes flow into Lix with full history. Switch branches and the directory follows.

## Also in v0.7

- `INSERT ... ON CONFLICT` upserts for entity state.
- `lix.fs` file API (`writeFile`, `readFile`) for the common path instead of raw SQL on `lix_file`.
- e2e benchmarks in the repository, the same ones the table above comes from.

