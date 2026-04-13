mod chunking;
mod codec;
mod gc;
mod init;
mod read;
pub(crate) mod schema;
mod write;

use crate::transaction::WriteBatch;
use crate::{LixBackend, LixBackendTransaction, LixError, SqlDialect};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BinaryBlobWrite<'a> {
    pub file_id: &'a str,
    pub version_id: &'a str,
    pub data: &'a [u8],
}

pub(crate) use init::init;

pub(crate) async fn load_blob_data_by_hash(
    backend: &dyn LixBackend,
    blob_hash: &str,
) -> Result<Option<Vec<u8>>, LixError> {
    read::load_binary_blob_data_by_hash(backend, blob_hash).await
}

pub(crate) async fn blob_exists(
    backend: &dyn LixBackend,
    blob_hash: &str,
) -> Result<bool, LixError> {
    read::blob_exists(backend, blob_hash).await
}

pub(crate) async fn persist_blob_writes_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    writes: &[BinaryBlobWrite<'_>],
) -> Result<(), LixError> {
    write::persist_blob_writes_in_transaction(transaction, writes).await
}

pub(crate) fn append_blob_writes_to_write_batch(
    write_batch: &mut WriteBatch,
    dialect: SqlDialect,
    writes: &[BinaryBlobWrite<'_>],
) -> Result<(), LixError> {
    write::append_blob_writes_to_write_batch(write_batch, dialect, writes)
}

pub(crate) async fn garbage_collect_unreachable_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
) -> Result<(), LixError> {
    gc::garbage_collect_unreachable_binary_cas_in_transaction(transaction).await
}

pub(crate) fn binary_blob_store_relation_name() -> &'static str {
    schema::INTERNAL_BINARY_BLOB_STORE
}

pub(crate) fn binary_file_version_ref_relation_name() -> &'static str {
    schema::INTERNAL_BINARY_FILE_VERSION_REF
}

pub(crate) fn internal_exact_relation_names() -> &'static [&'static str] {
    &[
        schema::INTERNAL_BINARY_BLOB_MANIFEST,
        schema::INTERNAL_BINARY_BLOB_MANIFEST_CHUNK,
        schema::INTERNAL_BINARY_BLOB_STORE,
        schema::INTERNAL_BINARY_CHUNK_STORE,
        schema::INTERNAL_BINARY_FILE_VERSION_REF,
    ]
}
