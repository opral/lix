# Lix Rust SDK

```rust
let lix = lix::open_lix().await?;
```

`lix` opens an in-memory workspace by default. It owns Lix's SQL, files,
branches, transactions, and Wasmtime plugin runtime without pulling in a
persistent backend.

For persistence, add an adapter crate and configure it before awaiting:

```rust,no_run
use lix::open_lix;
use lix_storage_rocksdb::RocksDB;

# async fn example() -> Result<(), lix::LixError> {
let storage = RocksDB::open("./workspace.rocksdb")?;
let lix = open_lix().with_storage(storage).await?;
# Ok(())
# }
```

Storage adapters implement the public `lix::storage` contract and release on
their own cadence. The official packages are `lix-storage-rocksdb`,
`lix-storage-rocksdb`, `lix-storage-slatedb`, and `lix-storage-filesystem`.
