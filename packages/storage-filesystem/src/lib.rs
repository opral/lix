//! Filesystem-backed Lix storage and synchronization.
//!
//! This adapter uses the RocksDB storage adapter for Lix metadata while keeping
//! a working directory synchronized with a Lix repository session.

mod filesystem;

pub use filesystem::{
    FilesystemStorage, FilesystemStorageOptions, FilesystemStorageRead, FilesystemStorageSync,
    FilesystemStorageWrite,
};
pub use lix_storage_rocksdb::{
    RocksDB as RocksDBFilesystem, RocksDBRead as RocksDBFilesystemRead,
    RocksDBWrite as RocksDBFilesystemWrite,
};
