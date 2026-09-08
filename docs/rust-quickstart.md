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
             FROM lix_history('lix_file') \
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

`execute()` runs one statement. To run several statements atomically, pass an
array of statements to `lix.execute_batch`. Do not concatenate SQL into one
script string.

The repository is in memory and disappears when the process ends. For native
persistence, use `lix-storage-rocksdb` or `lix-storage-filesystem`. See
[Storage](./persistence.md).

## Local, remote, and synchronized access

Rust defines the lifecycle; JavaScript exposes bindings to it. Supply only a
server for remote execution, or explicit storage and a server for a
synchronized local read replica. Connected mutations execute on the server:

```rust
use lix::{create_lix, delete_lix, open_lix, ServerOptions};

let local = open_lix().await?;
let repository = create_lix()
    .with_server(ServerOptions::new("https://example.com"))
    .from_lix(&local)
    .await?;
let remote = open_lix()
    .with_server(ServerOptions::new(&repository.url))
    .await?;
remote.execute("SELECT * FROM lix_file", &[]).await?;
remote.close().await?;

// Use a durable adapter supplied by a storage package.
let replica = open_lix()
    .with_storage(storage)
    .with_server(ServerOptions::new(&repository.url))
    .await?;
replica.close().await?;

delete_lix()
    .with_server(ServerOptions::new(&repository.url))
    .await?;
```

Omit `.from_lix(&local)` to create an empty hosted repository. Creation returns
the same result on retries using `.with_idempotency_key(key)` and the same
snapshot content. When omitted, a key is generated for each call.
The result is
`HostedLix { id, url }` and copies the source at one point in time; it does not
connect the source. A copy preserves history and untracked rows. Configure
credentials with `ServerOptions::with_headers`. To reconnect the original
durable storage, pause writes, create the hosted copy, close the source, and
reopen that same storage with the returned server URL. Diverged local history
is rejected without replacement.

Opening a missing server repository returns an error. Deleting a hosted
repository does not delete its local replicas, and reopening a replica cannot
silently recreate a deleted hosted repository.

## Next

- [Store application data](./schemas.md)
- [Work with files and media](./files-and-media.md)
- [Branch, review, and merge](./branching.md)
- [Storage](./persistence.md)
- [Rust API reference](https://docs.rs/lix)
