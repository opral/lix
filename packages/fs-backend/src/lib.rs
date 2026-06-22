//! Filesystem-specialized persistence backends for Lix.
//!
//! This internal crate owns the RocksDB implementation behind
//! `lix_sdk::FsBackend`.

#[cfg(feature = "rocksdb")]
mod rocksdb;

#[cfg(feature = "rocksdb")]
pub use rocksdb::{RocksDbFilesystemBackend, RocksDbFilesystemRead, RocksDbFilesystemWrite};
