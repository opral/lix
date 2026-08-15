---
description: Store text, binary files, audio, video, and other large media in a Lix repository.
---

# Files and media

Lix stores text and binary files in `lix_file`. This includes images, audio,
video, archives, and application-specific formats.

You use normal file paths and bytes. Lix handles the storage details.

## Write a file

Write file content with SQL. In JavaScript:

```ts
await lix.execute("INSERT INTO lix_file (path, content) VALUES ($1, $2)", [
  "/media/intro.wav",
  audioBytes,
]);
```

In Rust, use the same SQL model:

```rust
lix.execute(
    "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
    &[
        lix::Value::Text("/media/intro.wav".to_owned()),
        lix::Value::Blob(audio_bytes.into()),
    ],
)
.await?;
```

## How large files are stored

Lix stores file bytes in a content-addressed store. Lix splits large files into
chunks. It stores equal chunks once, even when they appear in several files or
branches.

The chunk store gives Lix three properties:

- integrity checks on stored content;
- chunk-level deduplication;
- garbage collection when content is no longer reachable.

Applications never call the chunk store or the server's transfer protocol
directly. Read and write `lix_file` with SQL.

## What belongs in the repository

Store source files in Lix, along with the metadata needed to understand them. Derived
data such as thumbnails, waveform caches, and temporary render files can stay
outside the repository when your application can rebuild them.

## Storage

Binary content uses the repository's selected storage adapter. Use RocksDB or
the local filesystem for native applications. Use SlateDB with S3-compatible
object storage for hosted repositories. See
[Persistence and Storage](./persistence.md).
