mod chunking;
mod codec;
mod context;
pub(crate) mod kv;
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

#[allow(unused_imports)]
pub(crate) use context::{BinaryCasContext, BinaryCasReader, BinaryCasStoreReader, BlobDataReader};

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
