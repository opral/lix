//! Filesystem-specialized persistence for Lix.
//!
//! This internal crate owns the RocksDB implementation behind
//! `lix_sdk::LocalFilesystem`.

#[cfg(feature = "rocksdb")]
mod rocksdb;

#[cfg(feature = "rocksdb")]
pub use rocksdb::{RocksDBFilesystem, RocksDBFilesystemRead, RocksDBFilesystemWrite};
