---
description: Install the Lix Rust SDK, write a file, inspect its history, and undo a change.
---

# Rust quickstart

This guide creates an in-memory Lix repository, writes a file, reads its history, and undoes the latest change.

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
            "SELECT from_path, to_path, diff_type, lixcol_position \
             FROM lix_history('lix_file') \
             WHERE from_path = $1 OR to_path = $1 \
             ORDER BY lixcol_position",
            &[lix::Value::Text("/hello.txt".to_owned())],
        )
        .await?;

    println!("{} versions", history.rows().len());

    lix.undo().await?;
    lix.close().await?;
    Ok(())
}
```

Every write becomes a commit automatically. You never run a commit command. Position `0` is the head commit. Higher positions walk back through the first-parent chain.

`execute()` runs one statement. To run several statements atomically, pass an array of statements to `lix.execute_batch`. Do not concatenate SQL into one script string.

The repository is in memory and disappears when the process ends. To keep it on disk, use `lix-storage-rocksdb` or `lix-storage-filesystem`. See [Storage](./persistence.md).

## Remote and synchronized access

Supply only a server for remote execution. Supply storage and a server for a partial replica that syncs in the background. See [Storage](./persistence.md) for the setup and [Hosting](./hosting.md#rust) for creating and deleting hosted repositories from Rust.

## Next

- [Store application data](./schemas.md)
- [Work with files and media](./files-and-media.md)
- [Branch, review, and merge](./branching.md)
- [Storage](./persistence.md)
- [Rust API reference](https://docs.rs/lix)
