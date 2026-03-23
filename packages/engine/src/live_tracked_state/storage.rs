use crate::live_tracked_state::codec::{PendingChunkWrite, PendingValueWrite};
use crate::live_tracked_state::types::{LiveTrackedRootId, LIVE_TRACKED_HASH_BYTES};
use crate::{LixBackend, LixError, LixTransaction, SqlDialect, Value};
use async_trait::async_trait;

pub(crate) const PROLLY_CHUNK_TABLE: &str = "lix_internal_prolly_chunk";
pub(crate) const PROLLY_ROOT_TABLE: &str = "lix_internal_prolly_root";
pub(crate) const PROLLY_VALUE_TABLE: &str = "lix_internal_prolly_value";

#[async_trait(?Send)]
pub(crate) trait LiveTrackedChunkStore {
    async fn read_chunk(
        &self,
        backend: &dyn LixBackend,
        hash: &[u8; LIVE_TRACKED_HASH_BYTES],
    ) -> Result<Option<Vec<u8>>, LixError>;

    async fn read_many(
        &self,
        backend: &dyn LixBackend,
        hashes: &[[u8; LIVE_TRACKED_HASH_BYTES]],
    ) -> Result<Vec<([u8; LIVE_TRACKED_HASH_BYTES], Vec<u8>)>, LixError>;

    async fn write_chunks(
        &self,
        transaction: &mut dyn LixTransaction,
        chunks: &[PendingChunkWrite],
    ) -> Result<(), LixError>;
}

#[async_trait(?Send)]
pub(crate) trait LiveTrackedRootStore {
    async fn load_root(
        &self,
        backend: &dyn LixBackend,
        commit_id: &str,
    ) -> Result<Option<LiveTrackedRootId>, LixError>;

    async fn store_root(
        &self,
        transaction: &mut dyn LixTransaction,
        commit_id: &str,
        root_id: &LiveTrackedRootId,
    ) -> Result<(), LixError>;
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct SqlLiveTrackedStorage;

impl SqlLiveTrackedStorage {
    pub(crate) async fn ensure_schema(&self, backend: &dyn LixBackend) -> Result<(), LixError> {
        let blob_type = blob_type_name(backend.dialect());
        let without_rowid = without_rowid_suffix(backend.dialect());
        backend
            .execute(
                &format!(
                    "CREATE TABLE IF NOT EXISTS {PROLLY_CHUNK_TABLE} (\
                     chunk_hash {blob_type} PRIMARY KEY,\
                     data {blob_type} NOT NULL\
                     ){without_rowid}"
                ),
                &[],
            )
            .await?;
        backend
            .execute(
                &format!(
                    "CREATE TABLE IF NOT EXISTS {PROLLY_ROOT_TABLE} (\
                     commit_id TEXT PRIMARY KEY,\
                     root_hash {blob_type} NOT NULL\
                     ){without_rowid}"
                ),
                &[],
            )
            .await?;
        backend
            .execute(
                &format!(
                    "CREATE TABLE IF NOT EXISTS {PROLLY_VALUE_TABLE} (\
                     value_hash {blob_type} PRIMARY KEY,\
                     data {blob_type} NOT NULL,\
                     size_bytes BIGINT NOT NULL\
                     ){without_rowid}"
                ),
                &[],
            )
            .await?;
        Ok(())
    }

    pub(crate) async fn write_values(
        &self,
        transaction: &mut dyn LixTransaction,
        values: &[PendingValueWrite],
    ) -> Result<(), LixError> {
        write_blob_rows(
            transaction,
            values,
            3,
            |row| {
                Ok(vec![
                    Value::Blob(row.hash.to_vec()),
                    Value::Blob(row.data.clone()),
                    Value::Integer(
                        i64::try_from(row.size_bytes).map_err(|_| {
                            LixError::unknown("live tracked value size exceeds i64")
                        })?,
                    ),
                ])
            },
            |rows| {
                format!(
                    "INSERT INTO {PROLLY_VALUE_TABLE} (value_hash, data, size_bytes) VALUES {rows} \
                     ON CONFLICT (value_hash) DO NOTHING"
                )
            },
        )
        .await
    }
}

#[async_trait(?Send)]
impl LiveTrackedChunkStore for SqlLiveTrackedStorage {
    async fn read_chunk(
        &self,
        backend: &dyn LixBackend,
        hash: &[u8; LIVE_TRACKED_HASH_BYTES],
    ) -> Result<Option<Vec<u8>>, LixError> {
        let rows = backend
            .execute(
                &format!("SELECT data FROM {PROLLY_CHUNK_TABLE} WHERE chunk_hash = $1 LIMIT 1"),
                &[Value::Blob(hash.to_vec())],
            )
            .await?;
        let Some(row) = rows.rows.first() else {
            return Ok(None);
        };
        let Some(Value::Blob(data)) = row.first() else {
            return Err(LixError::unknown(
                "live tracked chunk read returned a non-blob payload",
            ));
        };
        Ok(Some(data.clone()))
    }

    async fn read_many(
        &self,
        backend: &dyn LixBackend,
        hashes: &[[u8; LIVE_TRACKED_HASH_BYTES]],
    ) -> Result<Vec<([u8; LIVE_TRACKED_HASH_BYTES], Vec<u8>)>, LixError> {
        if hashes.is_empty() {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        let max_rows = max_rows_per_statement(backend.dialect(), 1);
        for chunk in hashes.chunks(max_rows.max(1)) {
            let mut params = Vec::with_capacity(chunk.len());
            let placeholders = chunk
                .iter()
                .enumerate()
                .map(|(index, hash)| {
                    params.push(Value::Blob(hash.to_vec()));
                    format!("${}", index + 1)
                })
                .collect::<Vec<_>>()
                .join(", ");
            let rows = backend
                .execute(
                    &format!(
                        "SELECT chunk_hash, data FROM {PROLLY_CHUNK_TABLE} WHERE chunk_hash IN ({placeholders})"
                    ),
                    &params,
                )
                .await?;
            for row in rows.rows {
                let hash = match row.first() {
                    Some(Value::Blob(hash)) => {
                        let mut out_hash = [0_u8; LIVE_TRACKED_HASH_BYTES];
                        if hash.len() != LIVE_TRACKED_HASH_BYTES {
                            return Err(LixError::unknown(
                                "live tracked chunk hash length is invalid",
                            ));
                        }
                        out_hash.copy_from_slice(hash);
                        out_hash
                    }
                    _ => {
                        return Err(LixError::unknown(
                            "live tracked chunk batch read returned a non-blob hash",
                        ))
                    }
                };
                let data = match row.get(1) {
                    Some(Value::Blob(data)) => data.clone(),
                    _ => {
                        return Err(LixError::unknown(
                            "live tracked chunk batch read returned a non-blob payload",
                        ))
                    }
                };
                out.push((hash, data));
            }
        }
        Ok(out)
    }

    async fn write_chunks(
        &self,
        transaction: &mut dyn LixTransaction,
        chunks: &[PendingChunkWrite],
    ) -> Result<(), LixError> {
        write_blob_rows(
            transaction,
            chunks,
            2,
            |row| {
                Ok(vec![
                    Value::Blob(row.hash.to_vec()),
                    Value::Blob(row.data.clone()),
                ])
            },
            |rows| {
                format!(
                    "INSERT INTO {PROLLY_CHUNK_TABLE} (chunk_hash, data) VALUES {rows} \
                     ON CONFLICT (chunk_hash) DO NOTHING"
                )
            },
        )
        .await
    }
}

#[async_trait(?Send)]
impl LiveTrackedRootStore for SqlLiveTrackedStorage {
    async fn load_root(
        &self,
        backend: &dyn LixBackend,
        commit_id: &str,
    ) -> Result<Option<LiveTrackedRootId>, LixError> {
        let rows = backend
            .execute(
                &format!("SELECT root_hash FROM {PROLLY_ROOT_TABLE} WHERE commit_id = $1 LIMIT 1"),
                &[Value::Text(commit_id.to_string())],
            )
            .await?;
        let Some(row) = rows.rows.first() else {
            return Ok(None);
        };
        let Some(Value::Blob(hash)) = row.first() else {
            return Err(LixError::unknown(
                "live tracked root read returned a non-blob hash",
            ));
        };
        Ok(Some(LiveTrackedRootId::from_slice(hash)?))
    }

    async fn store_root(
        &self,
        transaction: &mut dyn LixTransaction,
        commit_id: &str,
        root_id: &LiveTrackedRootId,
    ) -> Result<(), LixError> {
        transaction
            .execute(
                &format!(
                    "INSERT INTO {PROLLY_ROOT_TABLE} (commit_id, root_hash) VALUES ($1, $2) \
                     ON CONFLICT (commit_id) DO UPDATE SET root_hash = excluded.root_hash"
                ),
                &[
                    Value::Text(commit_id.to_string()),
                    Value::Blob(root_id.as_bytes().to_vec()),
                ],
            )
            .await?;
        Ok(())
    }
}

async fn write_blob_rows<Row, F, S>(
    transaction: &mut dyn LixTransaction,
    rows: &[Row],
    params_per_row: usize,
    mut build_params: F,
    build_sql: S,
) -> Result<(), LixError>
where
    F: FnMut(&Row) -> Result<Vec<Value>, LixError>,
    S: Fn(String) -> String,
{
    if rows.is_empty() {
        return Ok(());
    }

    let max_rows = max_rows_per_statement(transaction.dialect(), params_per_row);
    for chunk in rows.chunks(max_rows.max(1)) {
        let mut params = Vec::with_capacity(chunk.len() * params_per_row);
        let placeholders = chunk
            .iter()
            .enumerate()
            .map(|(row_index, row)| {
                params.extend(build_params(row)?);
                Ok(value_row_placeholders(row_index, params_per_row))
            })
            .collect::<Result<Vec<_>, LixError>>()?;
        let sql = build_sql(placeholders.join(", "));
        transaction.execute(&sql, &params).await?;
    }

    Ok(())
}

fn blob_type_name(dialect: SqlDialect) -> &'static str {
    match dialect {
        SqlDialect::Sqlite => "BLOB",
        SqlDialect::Postgres => "BYTEA",
    }
}

fn without_rowid_suffix(dialect: SqlDialect) -> &'static str {
    match dialect {
        SqlDialect::Sqlite => " WITHOUT ROWID",
        SqlDialect::Postgres => "",
    }
}

fn max_rows_per_statement(dialect: SqlDialect, params_per_row: usize) -> usize {
    let limit = match dialect {
        SqlDialect::Sqlite => 32_000,
        SqlDialect::Postgres => 65_000,
    };
    (limit / params_per_row).max(1)
}

fn value_row_placeholders(row_index: usize, values_per_row: usize) -> String {
    let base = row_index * values_per_row;
    let joined = (0..values_per_row)
        .map(|index| format!("${}", base + index + 1))
        .collect::<Vec<_>>()
        .join(", ");
    format!("({joined})")
}
