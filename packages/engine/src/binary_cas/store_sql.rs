#![allow(dead_code)]

//! SQL-backed adapter slot for binary CAS persistence.
//!
//! This module is the intended home for `BinaryCasReadStore` and
//! `BinaryCasWriteStore` implementations that still rely on raw backend,
//! transaction, or lower `backend/*` helpers during the MVP.

use async_trait::async_trait;

use crate::binary_cas::store::{
    BinaryCasBackendRef, BinaryCasReadStore, BinaryCasTransactionRef, BinaryCasWriteStore,
};
use crate::binary_cas::BinaryBlobWrite;
use crate::{LixError, QueryResult, Value};

use super::{gc, init, read, schema, write};

pub(crate) struct SqlBinaryCasReadStore<'a> {
    backend: BinaryCasBackendRef<'a>,
}

impl<'a> SqlBinaryCasReadStore<'a> {
    pub(crate) fn new(backend: BinaryCasBackendRef<'a>) -> Self {
        Self { backend }
    }
}

pub(crate) struct SqlBinaryCasWriteStore<'a> {
    transaction: BinaryCasTransactionRef<'a>,
}

impl<'a> SqlBinaryCasWriteStore<'a> {
    pub(crate) fn new(transaction: BinaryCasTransactionRef<'a>) -> Self {
        Self { transaction }
    }
}

pub(crate) async fn execute_query_with_backend(
    backend: BinaryCasBackendRef<'_>,
    sql: &str,
    params: &[Value],
) -> Result<QueryResult, LixError> {
    backend.execute(sql, params).await
}

pub(crate) async fn execute_query_with_transaction(
    transaction: BinaryCasTransactionRef<'_>,
    sql: &str,
    params: &[Value],
) -> Result<QueryResult, LixError> {
    transaction.execute(sql, params).await
}

pub(crate) async fn execute_ddl_batch_with_backend(
    backend: BinaryCasBackendRef<'_>,
    batch_name: &str,
    statements: &[&str],
) -> Result<(), LixError> {
    crate::backend::execute_ddl_batch(backend, batch_name, statements).await
}

pub(crate) async fn add_column_if_missing_with_backend(
    backend: BinaryCasBackendRef<'_>,
    table_name: &str,
    column_name: &str,
    column_sql: &str,
) -> Result<(), LixError> {
    crate::backend::add_column_if_missing(backend, table_name, column_name, column_sql).await
}

pub(crate) async fn init_storage(backend: BinaryCasBackendRef<'_>) -> Result<(), LixError> {
    init::init_storage(backend).await
}

pub(crate) fn chunk_store_relation_name() -> &'static str {
    schema::INTERNAL_BINARY_CHUNK_STORE
}

#[async_trait(?Send)]
impl BinaryCasReadStore for SqlBinaryCasReadStore<'_> {
    async fn blob_exists(&self, blob_hash: &str) -> Result<bool, LixError> {
        read::blob_exists(self.backend, blob_hash).await
    }

    async fn load_blob_data_by_hash(&self, blob_hash: &str) -> Result<Option<Vec<u8>>, LixError> {
        read::load_binary_blob_data_by_hash(self.backend, blob_hash).await
    }
}

#[async_trait(?Send)]
impl BinaryCasWriteStore for SqlBinaryCasWriteStore<'_> {
    async fn persist_blob_writes(
        &mut self,
        writes: &[BinaryBlobWrite<'_>],
    ) -> Result<(), LixError> {
        write::persist_blob_writes_in_transaction(self.transaction, writes).await
    }

    async fn garbage_collect_unreachable(&mut self) -> Result<(), LixError> {
        gc::garbage_collect_unreachable_binary_cas_in_transaction(self.transaction).await
    }
}
