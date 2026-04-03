use crate::backend::program::WriteProgram;
use crate::backend::program_runner::execute_write_program_with_transaction;
use crate::binary_cas::chunking::{fastcdc_chunk_ranges, should_materialize_chunk_cas};
use crate::binary_cas::codec::{binary_blob_hash_hex, encode_binary_chunk_payload};
use crate::binary_cas::schema::{
    INTERNAL_BINARY_BLOB_MANIFEST, INTERNAL_BINARY_BLOB_MANIFEST_CHUNK, INTERNAL_BINARY_BLOB_STORE,
    INTERNAL_BINARY_CHUNK_STORE,
};
use crate::{LixBackendTransaction, LixError, SqlDialect, Value};
use std::collections::BTreeMap;

const SQLITE_MAX_BIND_PARAMETERS_PER_STATEMENT: usize = 32_766;
const POSTGRES_MAX_BIND_PARAMETERS_PER_STATEMENT: usize = 65_535;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BinaryBlobWriteInput<'a> {
    pub(crate) file_id: &'a str,
    pub(crate) version_id: &'a str,
    pub(crate) data: &'a [u8],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedBinaryBlobWrite {
    pub(crate) file_id: String,
    pub(crate) version_id: String,
    pub(crate) untracked: bool,
    pub(crate) data: Vec<u8>,
}

impl ResolvedBinaryBlobWrite {
    pub(crate) fn as_input(&self) -> BinaryBlobWriteInput<'_> {
        BinaryBlobWriteInput {
            file_id: self.file_id.as_str(),
            version_id: self.version_id.as_str(),
            data: &self.data,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BinaryBlobManifestRow {
    pub(crate) blob_hash: String,
    pub(crate) size_bytes: i64,
    pub(crate) chunk_count: i64,
    pub(crate) created_at: String,
}

#[derive(Debug, Clone)]
pub(crate) struct BinaryBlobStoreRow {
    pub(crate) blob_hash: String,
    pub(crate) data: Vec<u8>,
    pub(crate) size_bytes: i64,
    pub(crate) created_at: String,
}

#[derive(Debug, Clone)]
pub(crate) struct BinaryChunkStoreRow {
    pub(crate) chunk_hash: String,
    pub(crate) data: Vec<u8>,
    pub(crate) size_bytes: i64,
    pub(crate) codec: String,
    pub(crate) codec_dict_id: Option<String>,
    pub(crate) created_at: String,
}

#[derive(Debug, Clone)]
pub(crate) struct BinaryBlobManifestChunkRow {
    pub(crate) blob_hash: String,
    pub(crate) chunk_index: i64,
    pub(crate) chunk_hash: String,
    pub(crate) chunk_size: i64,
}

#[derive(Debug, Default)]
pub(crate) struct BinaryCasWriteBatch {
    pub(crate) blob_manifest_rows: Vec<BinaryBlobManifestRow>,
    pub(crate) blob_store_rows: Vec<BinaryBlobStoreRow>,
    pub(crate) chunk_store_rows: Vec<BinaryChunkStoreRow>,
    pub(crate) manifest_chunk_rows: Vec<BinaryBlobManifestChunkRow>,
}

pub(crate) async fn persist_resolved_binary_blob_writes_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    writes: &[ResolvedBinaryBlobWrite],
) -> Result<(), LixError> {
    if writes.is_empty() {
        return Ok(());
    }

    let payloads = writes
        .iter()
        .map(ResolvedBinaryBlobWrite::as_input)
        .collect::<Vec<_>>();
    let program = build_binary_blob_fastcdc_write_program(transaction.dialect(), &payloads)?;
    execute_write_program_with_transaction(transaction, program).await?;

    Ok(())
}

pub(crate) fn build_binary_blob_fastcdc_write_program(
    dialect: SqlDialect,
    payloads: &[BinaryBlobWriteInput<'_>],
) -> Result<WriteProgram, LixError> {
    let batch = build_binary_cas_write_batch(payloads)?;
    let mut program = WriteProgram::new();

    push_blob_manifest_rows(&mut program, dialect, &batch.blob_manifest_rows);
    push_blob_store_rows(&mut program, dialect, &batch.blob_store_rows);
    push_chunk_store_rows(&mut program, dialect, &batch.chunk_store_rows);
    push_manifest_chunk_rows(&mut program, dialect, &batch.manifest_chunk_rows);

    Ok(program)
}

fn build_binary_cas_write_batch(
    payloads: &[BinaryBlobWriteInput<'_>],
) -> Result<BinaryCasWriteBatch, LixError> {
    let now = crate::runtime::functions::timestamp::timestamp();
    let mut manifest_rows = BTreeMap::<String, BinaryBlobManifestRow>::new();
    let mut blob_store_rows = BTreeMap::<String, BinaryBlobStoreRow>::new();
    let mut chunk_store_rows = BTreeMap::<String, BinaryChunkStoreRow>::new();
    let mut manifest_chunk_rows = BTreeMap::<(String, i64), BinaryBlobManifestChunkRow>::new();

    for payload in payloads {
        let blob_hash = binary_blob_hash_hex(payload.data);
        let size_bytes = i64::try_from(payload.data.len()).map_err(|_| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "binary blob size exceeds supported range for file '{}' version '{}'",
                payload.file_id, payload.version_id
            ),
        })?;
        let materialize_chunk_cas = should_materialize_chunk_cas(payload.data);
        let chunk_ranges = if materialize_chunk_cas {
            fastcdc_chunk_ranges(payload.data)
        } else {
            Vec::new()
        };
        let chunk_count = i64::try_from(chunk_ranges.len()).map_err(|_| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "binary chunk count exceeds supported range for file '{}' version '{}'",
                payload.file_id, payload.version_id
            ),
        })?;

        manifest_rows
            .entry(blob_hash.clone())
            .or_insert_with(|| BinaryBlobManifestRow {
                blob_hash: blob_hash.clone(),
                size_bytes,
                chunk_count,
                created_at: now.clone(),
            });
        blob_store_rows
            .entry(blob_hash.clone())
            .or_insert_with(|| BinaryBlobStoreRow {
                blob_hash: blob_hash.clone(),
                data: payload.data.to_vec(),
                size_bytes,
                created_at: now.clone(),
            });

        for (chunk_index, (start, end)) in chunk_ranges.iter().copied().enumerate() {
            let chunk_data = payload.data[start..end].to_vec();
            let encoded_chunk = encode_binary_chunk_payload(&chunk_data)?;
            let chunk_hash = binary_blob_hash_hex(&chunk_data);
            let chunk_size = i64::try_from(chunk_data.len()).map_err(|_| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "binary chunk size exceeds supported range for file '{}' version '{}'",
                    payload.file_id, payload.version_id
                ),
            })?;
            let stored_chunk_size =
                i64::try_from(encoded_chunk.data.len()).map_err(|_| LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: format!(
                        "binary stored chunk size exceeds supported range for file '{}' version '{}'",
                        payload.file_id, payload.version_id
                    ),
                })?;
            let chunk_index = i64::try_from(chunk_index).map_err(|_| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "binary chunk index exceeds supported range for file '{}' version '{}'",
                    payload.file_id, payload.version_id
                ),
            })?;

            chunk_store_rows
                .entry(chunk_hash.clone())
                .or_insert_with(|| BinaryChunkStoreRow {
                    chunk_hash: chunk_hash.clone(),
                    data: encoded_chunk.data,
                    size_bytes: stored_chunk_size,
                    codec: encoded_chunk.codec.to_string(),
                    codec_dict_id: encoded_chunk.codec_dict_id,
                    created_at: now.clone(),
                });
            manifest_chunk_rows
                .entry((blob_hash.clone(), chunk_index))
                .or_insert_with(|| BinaryBlobManifestChunkRow {
                    blob_hash: blob_hash.clone(),
                    chunk_index,
                    chunk_hash,
                    chunk_size,
                });
        }
    }

    Ok(BinaryCasWriteBatch {
        blob_manifest_rows: manifest_rows.into_values().collect(),
        blob_store_rows: blob_store_rows.into_values().collect(),
        chunk_store_rows: chunk_store_rows.into_values().collect(),
        manifest_chunk_rows: manifest_chunk_rows.into_values().collect(),
    })
}

fn push_blob_manifest_rows(
    program: &mut WriteProgram,
    dialect: SqlDialect,
    rows: &[BinaryBlobManifestRow],
) {
    push_chunked_payload_statement(
        program,
        dialect,
        rows,
        4,
        build_bulk_insert_binary_blob_manifest_sql,
        |row, params| {
            params.push(Value::Text(row.blob_hash.clone()));
            params.push(Value::Integer(row.size_bytes));
            params.push(Value::Integer(row.chunk_count));
            params.push(Value::Text(row.created_at.clone()));
        },
    );
}

fn push_blob_store_rows(
    program: &mut WriteProgram,
    dialect: SqlDialect,
    rows: &[BinaryBlobStoreRow],
) {
    push_chunked_payload_statement(
        program,
        dialect,
        rows,
        4,
        build_bulk_upsert_binary_blob_store_sql,
        |row, params| {
            params.push(Value::Text(row.blob_hash.clone()));
            params.push(Value::Blob(row.data.clone()));
            params.push(Value::Integer(row.size_bytes));
            params.push(Value::Text(row.created_at.clone()));
        },
    );
}

fn push_chunk_store_rows(
    program: &mut WriteProgram,
    dialect: SqlDialect,
    rows: &[BinaryChunkStoreRow],
) {
    push_chunked_payload_statement(
        program,
        dialect,
        rows,
        6,
        build_bulk_insert_binary_chunk_store_sql,
        |row, params| {
            params.push(Value::Text(row.chunk_hash.clone()));
            params.push(Value::Blob(row.data.clone()));
            params.push(Value::Integer(row.size_bytes));
            params.push(Value::Text(row.codec.clone()));
            params.push(match &row.codec_dict_id {
                Some(codec_dict_id) => Value::Text(codec_dict_id.clone()),
                None => Value::Null,
            });
            params.push(Value::Text(row.created_at.clone()));
        },
    );
}

fn push_manifest_chunk_rows(
    program: &mut WriteProgram,
    dialect: SqlDialect,
    rows: &[BinaryBlobManifestChunkRow],
) {
    push_chunked_payload_statement(
        program,
        dialect,
        rows,
        4,
        build_bulk_insert_binary_blob_manifest_chunk_sql,
        |row, params| {
            params.push(Value::Text(row.blob_hash.clone()));
            params.push(Value::Integer(row.chunk_index));
            params.push(Value::Text(row.chunk_hash.clone()));
            params.push(Value::Integer(row.chunk_size));
        },
    );
}

fn push_chunked_payload_statement<Row>(
    program: &mut WriteProgram,
    dialect: SqlDialect,
    rows: &[Row],
    params_per_row: usize,
    build_sql: impl Fn(&[String]) -> String,
    mut bind_row: impl FnMut(&Row, &mut Vec<Value>),
) {
    if rows.is_empty() {
        return;
    }

    let max_rows_per_statement = max_rows_per_statement_for_dialect(dialect, params_per_row);
    for chunk in rows.chunks(max_rows_per_statement) {
        let placeholders = chunk
            .iter()
            .enumerate()
            .map(|(index, _)| values_row_placeholders_sql(index, params_per_row))
            .collect::<Vec<_>>();
        let mut params = Vec::with_capacity(chunk.len() * params_per_row);
        for row in chunk {
            bind_row(row, &mut params);
        }
        program.push_statement(build_sql(&placeholders), params);
    }
}

fn max_rows_per_statement_for_dialect(dialect: SqlDialect, params_per_row: usize) -> usize {
    let max_params = match dialect {
        SqlDialect::Sqlite => SQLITE_MAX_BIND_PARAMETERS_PER_STATEMENT,
        SqlDialect::Postgres => POSTGRES_MAX_BIND_PARAMETERS_PER_STATEMENT,
    };
    (max_params / params_per_row).max(1)
}

fn values_row_placeholders_sql(row_index: usize, values_per_row: usize) -> String {
    let base = row_index * values_per_row;
    let placeholders = (1..=values_per_row)
        .map(|offset| format!("${}", base + offset))
        .collect::<Vec<_>>()
        .join(", ");
    format!("({placeholders})")
}

fn build_bulk_insert_binary_blob_manifest_sql(rows: &[String]) -> String {
    format!(
        "INSERT INTO {} (blob_hash, size_bytes, chunk_count, created_at) \
         VALUES {} \
         ON CONFLICT (blob_hash) DO NOTHING",
        INTERNAL_BINARY_BLOB_MANIFEST,
        rows.join(", ")
    )
}

fn build_bulk_upsert_binary_blob_store_sql(rows: &[String]) -> String {
    format!(
        "INSERT INTO {} (blob_hash, data, size_bytes, created_at) \
         VALUES {} \
         ON CONFLICT (blob_hash) DO UPDATE SET \
         data = EXCLUDED.data, \
         size_bytes = EXCLUDED.size_bytes",
        INTERNAL_BINARY_BLOB_STORE,
        rows.join(", ")
    )
}

fn build_bulk_insert_binary_chunk_store_sql(rows: &[String]) -> String {
    format!(
        "INSERT INTO {} (chunk_hash, data, size_bytes, codec, codec_dict_id, created_at) \
         VALUES {} \
         ON CONFLICT (chunk_hash) DO NOTHING",
        INTERNAL_BINARY_CHUNK_STORE,
        rows.join(", ")
    )
}

fn build_bulk_insert_binary_blob_manifest_chunk_sql(rows: &[String]) -> String {
    format!(
        "INSERT INTO {} (blob_hash, chunk_index, chunk_hash, chunk_size) \
         VALUES {} \
         ON CONFLICT (blob_hash, chunk_index) DO NOTHING",
        INTERNAL_BINARY_BLOB_MANIFEST_CHUNK,
        rows.join(", ")
    )
}
