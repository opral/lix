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

`open_lix()` opens the client's primary session on the repository's tracked
`lix_default_branch_id`, which points to `main` in a new repository. Applications
own window- or session-specific branch selection and switch explicitly.
`lix.open_another_session().await?` creates an independent session
on the primary session's current branch. Add
`.with_account(account_id).await?` when that session represents another
account. Switch the returned repository handle when it should work on another
branch. Switching any session never changes the repository default.

Storage adapters implement the public `lix::storage` contract and release on
their own cadence. The official packages are `lix-storage-rocksdb`,
`lix-storage-slatedb`, and `lix-storage-filesystem`.
