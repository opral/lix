//! SlateDB storage implementation for the Lix engine storage API.

mod slatedb;

pub use slatedb::{
    SlateDB, SlateDBCacheOptions, SlateDBFactory, SlateDBFixture, SlateDBObjectStoreOptions,
    SlateDBOptions, SlateDBRead, SlateDBWrite,
};
