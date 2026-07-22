//! Compatibility exports for filesystem persistence.
//!
//! `lix_sdk::LocalFilesystem` uses the shared RocksDB storage adapter so all
//! RocksDB behavior and optimizations have one implementation.

#[cfg(feature = "rocksdb")]
pub use lix_rocksdb_storage::{
    RocksDB as RocksDBFilesystem, RocksDBRead as RocksDBFilesystemRead,
    RocksDBWrite as RocksDBFilesystemWrite,
};
