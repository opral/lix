# `lix-storage-filesystem`

Filesystem-backed storage for Lix. The Rust crate and JavaScript package expose
the same adapter with independently versioned releases.

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
let _sync = storage.start_sync(&lix).await?;
# Ok(())
# }
```
