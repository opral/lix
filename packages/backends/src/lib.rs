//! Concrete backend implementations for the Lix engine backend API.

#[cfg(feature = "redb")]
mod redb;
#[cfg(feature = "rocksdb")]
mod rocksdb;
#[cfg(feature = "sqlite")]
mod sqlite;

#[cfg(feature = "redb")]
pub use redb::{RedbBackend, RedbBackendFactory, RedbBackendFixture, RedbRead, RedbWrite};
#[cfg(feature = "rocksdb")]
pub use rocksdb::{
    RocksDbBackend, RocksDbBackendFactory, RocksDbBackendFixture, RocksDbRead, RocksDbWrite,
};
#[cfg(feature = "sqlite")]
pub use sqlite::{
    SQLITE_FORMAT_VERSION, SqliteBackend, SqliteBackendFactory, SqliteBackendFixture,
    SqliteBackendOptions, SqliteRead, SqliteWrite,
};
