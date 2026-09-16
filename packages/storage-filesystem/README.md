# `lix-storage-filesystem`

Filesystem-backed storage for Lix. The Rust crate and JavaScript package expose
the same adapter with independently versioned releases.

## Exclusive repository lock

`FilesystemStorage` takes an exclusive lock on the repository's RocksDB database
at `.lix/.internal/rocksdb/LOCK`. Only one process can own a repository path at a
time. A second process attempting to open the same repository is refused with a
`LixError` whose `code` is `LIX_STORAGE_IN_USE`. Match this code rather than
the error message. Other storage failures retain their own error category. This applies to
both the Rust crate and the JavaScript package.

Applications with multiple processes must route repository access through the
process that owns the storage, for example via IPC or a local API.

## JavaScript

```ts
import { openLix } from "@lix-js/sdk";
import { FilesystemStorage } from "@lix-js/storage-filesystem";

const storage = new FilesystemStorage({ path: "./repository" });
const lix = await openLix({ storage });
```

The whole repository is synchronized by default. Pass `syncAllFiles: false`
and use `storage.importPaths(paths)` for selective synchronization.

## Rust

```rust
use lix::open_lix;
use lix_storage_filesystem::FilesystemStorage;

# async fn example() -> Result<(), lix::LixError> {
let storage = FilesystemStorage::new("./repository").open()?;
let lix = open_lix().with_storage(storage.clone()).await?;
storage.start_sync(&lix).await?;
# Ok(())
# }
```

The storage owns synchronization until `storage.stop_sync().await?` or until
the final storage/repository instance is dropped. Explicitly stopping is useful
for tests and before immediately reopening the same path.
