use crate::backend::storage_sql::tables;
use crate::binary_cas::schema::{
    INTERNAL_BINARY_BLOB_MANIFEST, INTERNAL_BINARY_BLOB_MANIFEST_CHUNK, INTERNAL_BINARY_BLOB_STORE,
    INTERNAL_BINARY_CHUNK_STORE, INTERNAL_BINARY_FILE_VERSION_REF,
};
use crate::{LixBackendTransaction, LixError, QueryResult, SqlDialect, Value};

#[async_trait::async_trait(?Send)]
trait BinaryCasGcExecutor {
    fn dialect(&self) -> SqlDialect;
    async fn execute_sql(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError>;
    async fn state_by_version_relation_exists(&mut self) -> Result<bool, LixError>;
}

struct TransactionBinaryCasGcExecutor<'a> {
    transaction: &'a mut dyn LixBackendTransaction,
}

#[async_trait::async_trait(?Send)]
impl<'a> BinaryCasGcExecutor for TransactionBinaryCasGcExecutor<'a> {
    fn dialect(&self) -> SqlDialect {
        self.transaction.dialect()
    }

    async fn execute_sql(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        self.transaction.execute(sql, params).await
    }

    async fn state_by_version_relation_exists(&mut self) -> Result<bool, LixError> {
        state_by_version_relation_exists_in_transaction(self.transaction).await
    }
}

pub(crate) async fn garbage_collect_unreachable_binary_cas_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
) -> Result<(), LixError> {
    let mut executor = TransactionBinaryCasGcExecutor { transaction };
    garbage_collect_unreachable_binary_cas_with_executor(&mut executor).await
}

async fn garbage_collect_unreachable_binary_cas_with_executor(
    executor: &mut dyn BinaryCasGcExecutor,
) -> Result<(), LixError> {
    if !executor.state_by_version_relation_exists().await? {
        return Ok(());
    }

    let state_blob_hash_expr = state_blob_hash_extract_expr_sql(executor.dialect());
    let delete_unreferenced_file_ref_sql =
        delete_unreferenced_binary_file_version_ref_sql(state_blob_hash_expr);
    let delete_unreferenced_manifest_chunk_sql =
        delete_unreferenced_binary_blob_manifest_chunk_sql(state_blob_hash_expr);
    let delete_unreferenced_chunk_store_sql = delete_unreferenced_binary_chunk_store_sql();
    let delete_unreferenced_manifest_sql =
        delete_unreferenced_binary_blob_manifest_sql(state_blob_hash_expr);
    let delete_unreferenced_blob_store_sql = delete_unreferenced_binary_blob_store_sql();

    executor
        .execute_sql(&delete_unreferenced_file_ref_sql, &[])
        .await?;
    executor
        .execute_sql(&delete_unreferenced_manifest_chunk_sql, &[])
        .await?;
    executor
        .execute_sql(&delete_unreferenced_chunk_store_sql, &[])
        .await?;
    executor
        .execute_sql(&delete_unreferenced_manifest_sql, &[])
        .await?;
    executor
        .execute_sql(&delete_unreferenced_blob_store_sql, &[])
        .await?;

    Ok(())
}

async fn state_by_version_relation_exists_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
) -> Result<bool, LixError> {
    match transaction.dialect() {
        SqlDialect::Sqlite => {
            let result = transaction
                .execute(
                    "SELECT 1 \
                     FROM sqlite_master \
                     WHERE name = $1 \
                       AND type IN ('table', 'view') \
                     LIMIT 1",
                    &[Value::Text(tables::state::STATE_BY_VERSION.to_string())],
                )
                .await?;
            Ok(!result.rows.is_empty())
        }
        SqlDialect::Postgres => {
            let result = transaction
                .execute(
                    "SELECT 1 \
                     FROM pg_catalog.pg_class c \
                     JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
                     WHERE n.nspname = current_schema() \
                       AND c.relname = $1 \
                     LIMIT 1",
                    &[Value::Text(tables::state::STATE_BY_VERSION.to_string())],
                )
                .await?;
            Ok(!result.rows.is_empty())
        }
    }
}

fn state_blob_hash_extract_expr_sql(dialect: SqlDialect) -> &'static str {
    match dialect {
        SqlDialect::Sqlite => "json_extract(snapshot_content, '$.blob_hash')",
        SqlDialect::Postgres => "(snapshot_content::jsonb ->> 'blob_hash')",
    }
}

fn delete_unreferenced_binary_file_version_ref_sql(state_blob_hash_expr: &str) -> String {
    format!(
        "WITH referenced AS (\
             SELECT file_id, version_id, {state_blob_hash_expr} AS blob_hash \
             FROM {} \
             WHERE schema_key = 'lix_binary_blob_ref' \
               AND snapshot_content IS NOT NULL \
               AND {state_blob_hash_expr} IS NOT NULL\
         ) \
         DELETE FROM {} \
         WHERE NOT EXISTS (\
             SELECT 1 \
             FROM referenced r \
             WHERE r.file_id = {}.file_id \
               AND r.version_id = {}.version_id \
               AND r.blob_hash = {}.blob_hash\
        )",
        tables::state::STATE_BY_VERSION,
        INTERNAL_BINARY_FILE_VERSION_REF,
        INTERNAL_BINARY_FILE_VERSION_REF,
        INTERNAL_BINARY_FILE_VERSION_REF,
        INTERNAL_BINARY_FILE_VERSION_REF,
    )
}

fn delete_unreferenced_binary_blob_manifest_chunk_sql(state_blob_hash_expr: &str) -> String {
    format!(
        "WITH referenced AS (\
             SELECT DISTINCT {state_blob_hash_expr} AS blob_hash \
             FROM {} \
             WHERE schema_key = 'lix_binary_blob_ref' \
               AND snapshot_content IS NOT NULL \
               AND {state_blob_hash_expr} IS NOT NULL\
         ) \
         DELETE FROM {} \
         WHERE NOT EXISTS (\
             SELECT 1 \
             FROM referenced r \
             WHERE r.blob_hash = {}.blob_hash\
        )",
        tables::state::STATE_BY_VERSION,
        INTERNAL_BINARY_BLOB_MANIFEST_CHUNK,
        INTERNAL_BINARY_BLOB_MANIFEST_CHUNK,
    )
}

fn delete_unreferenced_binary_chunk_store_sql() -> String {
    format!(
        "DELETE FROM {} \
         WHERE NOT EXISTS (\
             SELECT 1 \
             FROM {} mc \
             WHERE mc.chunk_hash = {}.chunk_hash\
         )",
        INTERNAL_BINARY_CHUNK_STORE,
        INTERNAL_BINARY_BLOB_MANIFEST_CHUNK,
        INTERNAL_BINARY_CHUNK_STORE,
    )
}

fn delete_unreferenced_binary_blob_manifest_sql(state_blob_hash_expr: &str) -> String {
    format!(
        "WITH referenced AS (\
             SELECT DISTINCT {state_blob_hash_expr} AS blob_hash \
             FROM {} \
             WHERE schema_key = 'lix_binary_blob_ref' \
               AND snapshot_content IS NOT NULL \
               AND {state_blob_hash_expr} IS NOT NULL\
         ) \
         DELETE FROM {} \
         WHERE NOT EXISTS (\
             SELECT 1 \
             FROM referenced r \
             WHERE r.blob_hash = {}.blob_hash\
         ) \
         AND NOT EXISTS (\
             SELECT 1 \
             FROM {} mc \
             WHERE mc.blob_hash = {}.blob_hash\
        )",
        tables::state::STATE_BY_VERSION,
        INTERNAL_BINARY_BLOB_MANIFEST,
        INTERNAL_BINARY_BLOB_MANIFEST,
        INTERNAL_BINARY_BLOB_MANIFEST_CHUNK,
        INTERNAL_BINARY_BLOB_MANIFEST,
    )
}

fn delete_unreferenced_binary_blob_store_sql() -> String {
    format!(
        "DELETE FROM {} \
         WHERE NOT EXISTS (\
             SELECT 1 \
             FROM {} r \
             WHERE r.blob_hash = {}.blob_hash\
         )",
        INTERNAL_BINARY_BLOB_STORE, INTERNAL_BINARY_FILE_VERSION_REF, INTERNAL_BINARY_BLOB_STORE,
    )
}
