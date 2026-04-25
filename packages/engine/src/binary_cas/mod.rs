mod chunking;
mod codec;
mod context;
pub(crate) mod kv;
pub(crate) mod read;
pub(crate) mod store;

use crate::binary_cas::store::{BinaryCasBackendRef, BinaryCasTransactionRef};
use crate::transaction::WriteBatch;
use crate::{LixError, SqlDialect};

#[allow(dead_code)]
const LEGACY_BINARY_BLOB_STORE_RELATION_NAME: &str = "lix_internal_binary_blob_store";
#[allow(dead_code)]
const LEGACY_BINARY_FILE_VERSION_REF_RELATION_NAME: &str = "lix_internal_binary_file_version_ref";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BinaryBlobWrite<'a> {
    pub file_id: &'a str,
    pub version_id: &'a str,
    pub data: &'a [u8],
}

pub(crate) use context::BinaryCasContext;
pub(crate) use read::BlobDataReader;

pub(crate) async fn load_blob_data_by_hash(
    backend: BinaryCasBackendRef<'_>,
    blob_hash: &str,
) -> Result<Option<Vec<u8>>, LixError> {
    // Compatibility wrapper for callers that have not been threaded with
    // BinaryCasContext yet.
    read::load_binary_blob_data_by_hash(backend, blob_hash).await
}

pub(crate) async fn blob_exists(
    backend: BinaryCasBackendRef<'_>,
    blob_hash: &str,
) -> Result<bool, LixError> {
    // Compatibility wrapper for callers that have not been threaded with
    // BinaryCasContext yet.
    read::blob_exists(backend, blob_hash).await
}

pub(crate) async fn persist_blob_writes_in_transaction(
    transaction: BinaryCasTransactionRef<'_>,
    writes: &[BinaryBlobWrite<'_>],
) -> Result<(), LixError> {
    // Compatibility wrapper for callers that have not moved to
    // BinaryCasContext::writer(...) yet.
    kv::persist_blob_writes_in_transaction(transaction, writes).await
}

pub(crate) fn append_blob_writes_to_write_batch(
    _write_batch: &mut WriteBatch,
    _dialect: SqlDialect,
    _writes: &[BinaryBlobWrite<'_>],
) -> Result<(), LixError> {
    // Temporary compatibility shim for legacy commit code. Binary CAS writes
    // now go through `persist_blob_writes_in_transaction`, which is KV-backed.
    Ok(())
}

pub(crate) async fn garbage_collect_unreachable_in_transaction(
    _transaction: BinaryCasTransactionRef<'_>,
) -> Result<(), LixError> {
    // TODO: replace with KV-aware reachability once the changelog owns blob refs.
    Ok(())
}

#[allow(dead_code)]
pub(crate) fn binary_blob_store_relation_name() -> &'static str {
    LEGACY_BINARY_BLOB_STORE_RELATION_NAME
}

#[allow(dead_code)]
pub(crate) fn binary_file_version_ref_relation_name() -> &'static str {
    LEGACY_BINARY_FILE_VERSION_REF_RELATION_NAME
}

#[allow(dead_code)]
pub(crate) fn internal_exact_relation_names() -> &'static [&'static str] {
    &[]
}

pub(crate) async fn init(_backend: BinaryCasBackendRef<'_>) -> Result<(), LixError> {
    Ok(())
}
