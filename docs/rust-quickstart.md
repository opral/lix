---
description: Install the Lix Rust SDK, write a file, inspect its history, and undo a change.
---

# Rust quickstart

This guide creates an in-memory Lix repository, writes a file, reads its
history, and undoes the latest change.

## Install

```bash
cargo add lix
cargo add tokio --features macros,rt-multi-thread
```

## Write and update a file

```rust
#[tokio::main]
async fn main() -> Result<(), lix::LixError> {
    let lix = lix::open_lix().await?;

    lix.execute(
        "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
        &[
            lix::Value::Text("/hello.txt".to_owned()),
            lix::Value::Blob(b"Hello".as_slice().into()),
        ],
    )
    .await?;
    lix.execute(
        "UPDATE lix_file SET content = $1 WHERE path = $2",
        &[
            lix::Value::Blob(b"Hello from Lix".as_slice().into()),
            lix::Value::Text("/hello.txt".to_owned()),
        ],
    )
    .await?;

    let history = lix
        .execute(
            "SELECT path, lixcol_depth \
             FROM lix_file_history() \
             WHERE path = $1 \
             ORDER BY lixcol_depth",
            &[lix::Value::Text("/hello.txt".to_owned())],
        )
        .await?;

    println!("{} versions", history.rows().len());

    lix.undo().await?;
    lix.close().await?;
    Ok(())
}
```

Lix records both writes automatically. You do not need to create commits.
Depth `0` is the state at the head. Higher numbers walk back through history.

The repository is in memory and disappears when the process ends. For native
persistence, use `lix-storage-rocksdb` or `lix-storage-filesystem`. See
[Persistence and Storage](./persistence.md).

## Next

- [Store application data](./schemas.md)
- [Work with files and media](./files-and-media.md)
- [Branch, review, and merge](./branching.md)
- [Persistence and Storage](./persistence.md)
- [Rust API reference](https://docs.rs/lix)
