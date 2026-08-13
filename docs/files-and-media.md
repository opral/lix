---
description: Store text, binary files, audio, video, and other large media in a Lix repository.
---

# Files and media

Lix stores text and binary files in `lix_file`. This includes images, audio,
video, archives, and application-specific formats.

You use normal file paths and bytes. Lix handles the storage details.

## Write a file

Write file content through SQL in every SDK:

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

Lix stores file bytes in a content-addressed store. Large files are split into
chunks. Equal chunks are stored once, even when they appear in several files,
versions, or branches.

This gives large files:

- content verification;
- chunk-level deduplication;
- garbage collection when content is no longer reachable.

The content-addressed store and the server's binary transfer paths are
implementation details. Applications use `lix_file` through SQL instead of
calling storage or protocol internals.

## What belongs in the repository

Store source files and the metadata needed to understand them in Lix. Derived
data such as thumbnails, waveform caches, and temporary render files can stay
outside the repository when your application can rebuild them.

## Storage

Binary content uses the repository's selected storage adapter. Use RocksDB or
the local filesystem for native applications. Use SlateDB with S3-compatible
object storage for hosted repositories. See
[Persistence and Storage](./persistence.md).
