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

pub async fn collect_storage_metrics(db_path: &Path) -> Result<StorageMetrics, LixError> {
    let conn = format!("sqlite://{}", db_path.display());
    let pool = SqlitePool::connect(&conn).await.map_err(|error| LixError {
        message: format!(
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
        .map_err(|error| LixError {
            message: format!("storage metrics query failed ({sql}): {error}"),
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
        .map_err(|error| LixError {
            message: format!("storage metrics dbstat query failed ({object_type}): {error}"),
        })?;
    let value = row.try_get::<i64, _>(0).unwrap_or(0);
    Ok(value.max(0) as u64)
}
