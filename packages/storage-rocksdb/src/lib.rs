//! RocksDB storage implementation for the Lix engine storage API.

mod rocksdb;

pub use rocksdb::{RocksDB, RocksDBFactory, RocksDBFixture, RocksDBRead, RocksDBWrite};

/// Whether this build records the scan key-buffer census.
///
/// **A census that is compiled out reports zero, and zero is exactly what a
/// scan route that never ran reports.** This constant is what lets a test tell
/// the two apart in its own failure message rather than sending the reader off
/// to look for a routing bug that does not exist.
///
/// It exists because that already happened: a `git checkout` of
/// `packages/e2e/Cargo.toml`, run to drop an unrelated bench overlay, reverted
/// the `lix_storage_rocksdb/storage-benches` feature edge that lived in the
/// same file, and the census went silent while every other counter kept
/// working.
pub const SCAN_KEY_BUFFER_CENSUS_ENABLED: bool = cfg!(feature = "storage-benches");
