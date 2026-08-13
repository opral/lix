# Lix Rust SDK

```rust
let lix = lix::open_lix().await?;
```

`lix` opens an in-memory repository by default. It owns Lix's SQL, files,
branches, transactions, and Wasmtime plugin runtime without pulling in a
persistent backend.

Plugin authors also depend directly on `lix` and use the target-selected
[`lix::plugin`](PLUGIN.md) authoring API. No separate plugin SDK crate is
required.

For persistence, add an adapter crate and configure it before awaiting:

```rust,no_run
use lix::open_lix;
use lix_storage_rocksdb::RocksDB;

# async fn example() -> Result<(), lix::LixError> {
let storage = RocksDB::open("./repository.rocksdb")?;
let lix = open_lix().with_storage(storage).await?;
# Ok(())
# }
```

`open_lix()` opens the client's primary session. Its active branch is restored
from client state (`lix_primary_session_branch_id`) and falls back to the
repository's tracked `lix_default_branch_id`, which points to `main` in a new
repository. `lix.open_session()` creates an independent session on the primary
session's current branch; `lix.open_session_at(branch_id)` selects a branch
explicitly. Switching an independent session never changes the primary-session
preference or the repository default.

Storage adapters implement the public `lix::storage` contract and release on
their own cadence. The official packages are `lix-storage-rocksdb`,
`lix-storage-rocksdb`, `lix-storage-slatedb`, and `lix-storage-filesystem`.
