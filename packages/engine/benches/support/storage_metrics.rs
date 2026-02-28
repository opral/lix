use lix_engine::LixError;
use serde::Serialize;
use sqlx::{Row, SqlitePool};
use std::path::Path;

#[derive(Debug, Clone, Serialize)]
pub struct StorageMetrics {
    pub db_file_bytes: u64,
    pub wal_file_bytes: u64,
    pub shm_file_bytes: u64,
    pub page_size: u64,
    pub page_count: u64,
    pub freelist_count: u64,
    pub estimated_db_bytes: u64,
    pub table_bytes: u64,
    pub index_bytes: u64,
    pub file_data_cache_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct BinaryChunkDiagnostics {
    pub manifest_rows: u64,
    pub manifest_chunk_refs: u64,
    pub unique_chunks: u64,
    pub chunk_store_bytes: u64,
    pub manifest_logical_bytes: u64,
    pub avg_chunks_per_blob: f64,
    pub chunk_reuse_rate: f64,
    pub bytes_dedup_saved: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct BinaryHistoryStorageMetrics {
    pub blob_store_bytes: u64,
    pub chunk_store_bytes: u64,
    pub blob_manifest_bytes: u64,
    pub blob_manifest_chunk_bytes: u64,
    pub file_version_ref_bytes: u64,
    pub binary_history_index_bytes: u64,
    pub total_binary_history_table_bytes: u64,
    pub total_binary_history_bytes: u64,
    pub logical_history_bytes: u64,
}

pub async fn collect_storage_metrics(db_path: &Path) -> Result<StorageMetrics, LixError> {
    let conn = format!("sqlite://{}", db_path.display());
    let pool = SqlitePool::connect(&conn).await.map_err(|error| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: format!(
            "failed to open sqlite db for storage metrics ({}): {error}",
            db_path.display()
        ),
    })?;

    let page_size = query_pragma_u64(&pool, "PRAGMA page_size").await?;
    let page_count = query_pragma_u64(&pool, "PRAGMA page_count").await?;
    let freelist_count = query_pragma_u64(&pool, "PRAGMA freelist_count").await?;
    let file_data_cache_bytes = query_scalar_u64(
        &pool,
        "SELECT COALESCE(SUM(LENGTH(data)), 0) FROM lix_internal_file_data_cache",
    )
    .await?;

    let table_bytes = query_dbstat_bytes(&pool, "table").await.unwrap_or(0);
    let index_bytes = query_dbstat_bytes(&pool, "index").await.unwrap_or(0);

    let db_file_bytes = file_size_or_zero(db_path);
    let wal_file_bytes = file_size_or_zero(&db_path.with_extension("sqlite-wal"))
        .max(file_size_or_zero(&with_extra_suffix(db_path, "-wal")));
    let shm_file_bytes = file_size_or_zero(&db_path.with_extension("sqlite-shm"))
        .max(file_size_or_zero(&with_extra_suffix(db_path, "-shm")));

    Ok(StorageMetrics {
        db_file_bytes,
        wal_file_bytes,
        shm_file_bytes,
        page_size,
        page_count,
        freelist_count,
        estimated_db_bytes: page_size.saturating_mul(page_count),
        table_bytes,
        index_bytes,
        file_data_cache_bytes,
    })
}

pub async fn collect_binary_history_storage_metrics(
    db_path: &Path,
) -> Result<BinaryHistoryStorageMetrics, LixError> {
    let conn = format!("sqlite://{}", db_path.display());
    let pool = SqlitePool::connect(&conn).await.map_err(|error| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: format!(
            "failed to open sqlite db for binary history storage metrics ({}): {error}",
            db_path.display()
        ),
    })?;

    let chunk_store_bytes =
        query_dbstat_table_bytes_for_table(&pool, "lix_internal_binary_chunk_store")
            .await
            .unwrap_or(0);
    let blob_store_bytes =
        query_dbstat_table_bytes_for_table(&pool, "lix_internal_binary_blob_store")
            .await
            .unwrap_or(0);
    let blob_manifest_bytes =
        query_dbstat_table_bytes_for_table(&pool, "lix_internal_binary_blob_manifest")
            .await
            .unwrap_or(0);
    let blob_manifest_chunk_bytes =
        query_dbstat_table_bytes_for_table(&pool, "lix_internal_binary_blob_manifest_chunk")
            .await
            .unwrap_or(0);
    let file_version_ref_bytes =
        query_dbstat_table_bytes_for_table(&pool, "lix_internal_binary_file_version_ref")
            .await
            .unwrap_or(0);

    let mut binary_history_index_bytes = 0_u64;
    for table_name in [
        "lix_internal_binary_blob_store",
        "lix_internal_binary_chunk_store",
        "lix_internal_binary_blob_manifest",
        "lix_internal_binary_blob_manifest_chunk",
        "lix_internal_binary_file_version_ref",
    ] {
        binary_history_index_bytes = binary_history_index_bytes.saturating_add(
            query_dbstat_index_bytes_for_table(&pool, table_name)
                .await
                .unwrap_or(0),
        );
    }

    let total_binary_history_table_bytes = blob_store_bytes
        .saturating_add(chunk_store_bytes)
        .saturating_add(blob_manifest_bytes)
        .saturating_add(blob_manifest_chunk_bytes)
        .saturating_add(file_version_ref_bytes);
    let total_binary_history_bytes =
        total_binary_history_table_bytes.saturating_add(binary_history_index_bytes);

    let logical_history_bytes =
        if query_table_exists(&pool, "lix_internal_binary_file_version_ref").await? {
            query_scalar_u64(
                &pool,
                "SELECT COALESCE(SUM(size_bytes), 0) FROM lix_internal_binary_file_version_ref",
            )
            .await?
        } else {
            0
        };

    Ok(BinaryHistoryStorageMetrics {
        blob_store_bytes,
        chunk_store_bytes,
        blob_manifest_bytes,
        blob_manifest_chunk_bytes,
        file_version_ref_bytes,
        binary_history_index_bytes,
        total_binary_history_table_bytes,
        total_binary_history_bytes,
        logical_history_bytes,
    })
}

pub async fn collect_binary_chunk_diagnostics(
    db_path: &Path,
) -> Result<Option<BinaryChunkDiagnostics>, LixError> {
    let conn = format!("sqlite://{}", db_path.display());
    let pool = SqlitePool::connect(&conn).await.map_err(|error| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: format!(
            "failed to open sqlite db for binary chunk diagnostics ({}): {error}",
            db_path.display()
        ),
    })?;

    let has_manifest = query_table_exists(&pool, "lix_internal_binary_blob_manifest").await?;
    let has_chunk_store = query_table_exists(&pool, "lix_internal_binary_chunk_store").await?;
    if !has_manifest || !has_chunk_store {
        return Ok(None);
    }

    let row = sqlx::query(
        "SELECT \
            CAST(COALESCE((SELECT COUNT(*) FROM lix_internal_binary_blob_manifest), 0) AS INTEGER), \
            CAST(COALESCE((SELECT SUM(chunk_count) FROM lix_internal_binary_blob_manifest), 0) AS INTEGER), \
            CAST(COALESCE((SELECT COUNT(*) FROM lix_internal_binary_chunk_store), 0) AS INTEGER), \
            CAST(COALESCE((SELECT SUM(size_bytes) FROM lix_internal_binary_chunk_store), 0) AS INTEGER), \
            CAST(COALESCE((SELECT SUM(size_bytes) FROM lix_internal_binary_blob_manifest), 0) AS INTEGER), \
            COALESCE((SELECT AVG(chunk_count) FROM lix_internal_binary_blob_manifest), 0.0)",
    )
    .fetch_one(&pool)
    .await
    .map_err(|error| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: format!("binary chunk diagnostics query failed: {error}"),
    })?;

    let manifest_rows = row.try_get::<i64, _>(0).unwrap_or(0).max(0) as u64;
    let manifest_chunk_refs = row.try_get::<i64, _>(1).unwrap_or(0).max(0) as u64;
    let unique_chunks = row.try_get::<i64, _>(2).unwrap_or(0).max(0) as u64;
    let chunk_store_bytes = row.try_get::<i64, _>(3).unwrap_or(0).max(0) as u64;
    let manifest_logical_bytes = row.try_get::<i64, _>(4).unwrap_or(0).max(0) as u64;
    let avg_chunks_per_blob = row.try_get::<f64, _>(5).unwrap_or(0.0).max(0.0);
    let chunk_reuse_rate = if manifest_chunk_refs > 0 {
        1.0 - (unique_chunks as f64 / manifest_chunk_refs as f64)
    } else {
        0.0
    };

    Ok(Some(BinaryChunkDiagnostics {
        manifest_rows,
        manifest_chunk_refs,
        unique_chunks,
        chunk_store_bytes,
        manifest_logical_bytes,
        avg_chunks_per_blob,
        chunk_reuse_rate,
        bytes_dedup_saved: manifest_logical_bytes.saturating_sub(chunk_store_bytes),
    }))
}

fn file_size_or_zero(path: &Path) -> u64 {
    std::fs::metadata(path).map(|meta| meta.len()).unwrap_or(0)
}

fn with_extra_suffix(path: &Path, suffix: &str) -> std::path::PathBuf {
    let mut as_os = path.as_os_str().to_os_string();
    as_os.push(suffix);
    std::path::PathBuf::from(as_os)
}

async fn query_pragma_u64(pool: &SqlitePool, sql: &str) -> Result<u64, LixError> {
    query_scalar_u64(pool, sql).await
}

async fn query_scalar_u64(pool: &SqlitePool, sql: &str) -> Result<u64, LixError> {
    let row = sqlx::query(sql)
        .fetch_one(pool)
        .await
        .map_err(|error| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: format!("storage metrics query failed ({sql}): {error}"),
        })?;
    let value = row.try_get::<i64, _>(0).unwrap_or(0);
    Ok(value.max(0) as u64)
}

async fn query_dbstat_bytes(pool: &SqlitePool, object_type: &str) -> Result<u64, LixError> {
    let sql = "SELECT COALESCE(SUM(d.pgsize), 0) \
               FROM dbstat d \
               JOIN sqlite_master m ON m.name = d.name \
               WHERE m.type = ?";
    let row = sqlx::query(sql)
        .bind(object_type)
        .fetch_one(pool)
        .await
        .map_err(|error| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: format!("storage metrics dbstat query failed ({object_type}): {error}"),
        })?;
    let value = row.try_get::<i64, _>(0).unwrap_or(0);
    Ok(value.max(0) as u64)
}

async fn query_dbstat_table_bytes_for_table(
    pool: &SqlitePool,
    table_name: &str,
) -> Result<u64, LixError> {
    let sql = "SELECT COALESCE(SUM(d.pgsize), 0) \
               FROM dbstat d \
               JOIN sqlite_master m ON m.name = d.name \
               WHERE m.type = 'table' AND m.name = ?";
    let row = sqlx::query(sql)
        .bind(table_name)
        .fetch_one(pool)
        .await
        .map_err(|error| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: format!("storage metrics dbstat table query failed ({table_name}): {error}"),
        })?;
    let value = row.try_get::<i64, _>(0).unwrap_or(0);
    Ok(value.max(0) as u64)
}

async fn query_dbstat_index_bytes_for_table(
    pool: &SqlitePool,
    table_name: &str,
) -> Result<u64, LixError> {
    let sql = "SELECT COALESCE(SUM(d.pgsize), 0) \
               FROM dbstat d \
               JOIN sqlite_master m ON m.name = d.name \
               WHERE m.type = 'index' AND m.tbl_name = ?";
    let row = sqlx::query(sql)
        .bind(table_name)
        .fetch_one(pool)
        .await
        .map_err(|error| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: format!("storage metrics dbstat index query failed ({table_name}): {error}"),
        })?;
    let value = row.try_get::<i64, _>(0).unwrap_or(0);
    Ok(value.max(0) as u64)
}

async fn query_table_exists(pool: &SqlitePool, table_name: &str) -> Result<bool, LixError> {
    let row =
        sqlx::query("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?)")
            .bind(table_name)
            .fetch_one(pool)
            .await
            .map_err(|error| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: format!(
                    "storage metrics table-exists query failed ({table_name}): {error}"
                ),
            })?;
    let exists = row.try_get::<i64, _>(0).unwrap_or(0);
    Ok(exists == 1)
}
