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

## Mirror write guarantees

File content updates are staged in the destination directory and atomically
replace that file. A reader opening the destination sees either the complete old
contents or the complete new contents, never an intermediate write. Replacement
failure leaves the previous file in place. This guarantee is per file, not per
transaction or directory, and relies on the filesystem's atomic rename support.
Existing open readers may continue reading the old file.

The mirror does not fsync file contents or directory entries. A successful mirror
write is not a power-loss durability guarantee; the repository's durability
policy applies to the database. Startup synchronization reads disk changes first,
so after an interrupted synchronization an older complete mirror can be imported
as a new repository change. Atomic replacement does not resolve that recovery
ambiguity or provide a durable mirror checkpoint.

Names matching `.lix-mirror-<16 ASCII letters or digits>.tmp` are reserved for
staging and excluded from synchronization, including explicit imports. Normal
failures clean them up immediately; directory scans remove abandoned regular
staging files left by process termination on a best-effort basis. Selective
synchronization does not scan unrelated directories for cleanup.

Replacement preserves existing file permission bits and uses ordinary creation
permissions for new files. It replaces the file identity: hard links and existing
open handles retain the old file, and ownership, ACLs, and extended attributes are
not copied. Filesystems or Windows handles that prevent replacement cause an
error; there is no fallback to truncating the destination.
