//! Filesystem-specialized persistence backends for Lix.
//!
//! This crate intentionally owns filesystem backend experiments directly
//! instead of routing RocksDB through the generic `lix_backends` crate.

#[cfg(feature = "rocksdb")]
mod rocksdb;

#[cfg(feature = "rocksdb")]
pub use rocksdb::{
    RocksDbBlobOptions, RocksDbFilesystemBackend, RocksDbFilesystemBackendOptions,
    RocksDbFilesystemRead, RocksDbFilesystemWrite,
};
