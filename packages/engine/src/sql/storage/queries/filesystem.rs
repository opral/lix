use super::super::tables;

pub(crate) fn upsert_file_path_cache_sql() -> String {
    format!(
        "INSERT INTO {} \
         (file_id, version_id, directory_id, name, extension, path) \
         VALUES ($1, $2, NULL, $3, $4, $5) \
         ON CONFLICT (file_id, version_id) DO UPDATE SET \
         directory_id = EXCLUDED.directory_id, \
         name = EXCLUDED.name, \
         extension = EXCLUDED.extension, \
         path = EXCLUDED.path",
        tables::filesystem::INTERNAL_FILE_PATH_CACHE,
    )
}

pub(crate) fn delete_file_data_cache_where_sql(predicates_sql: &str) -> String {
    format!(
        "DELETE FROM {} \
         WHERE {predicates_sql}",
        tables::filesystem::INTERNAL_FILE_DATA_CACHE,
    )
}

pub(crate) fn delete_file_path_cache_where_sql(predicates_sql: &str) -> String {
    format!(
        "DELETE FROM {} \
         WHERE {predicates_sql}",
        tables::filesystem::INTERNAL_FILE_PATH_CACHE,
    )
}

pub(crate) fn select_file_data_cache_blob_sql() -> String {
    format!(
        "SELECT data \
         FROM {} \
         WHERE file_id = $1 \
           AND version_id = $2 \
         LIMIT 1",
        tables::filesystem::INTERNAL_FILE_DATA_CACHE,
    )
}

pub(crate) fn binary_blob_exists_sql() -> String {
    format!(
        "SELECT 1 \
         FROM (\
             SELECT blob_hash FROM {} \
             UNION ALL \
             SELECT blob_hash FROM {}\
         ) AS blobs \
         WHERE blob_hash = $1 \
         LIMIT 1",
        tables::filesystem::INTERNAL_BINARY_BLOB_STORE,
        tables::filesystem::INTERNAL_BINARY_BLOB_MANIFEST,
    )
}

pub(crate) fn insert_binary_blob_manifest_sql() -> String {
    format!(
        "INSERT INTO {} (blob_hash, size_bytes, chunk_count, created_at) \
         VALUES ($1, $2, $3, $4) \
         ON CONFLICT (blob_hash) DO NOTHING",
        tables::filesystem::INTERNAL_BINARY_BLOB_MANIFEST,
    )
}

pub(crate) fn insert_binary_chunk_store_sql() -> String {
    format!(
        "INSERT INTO {} (chunk_hash, data, size_bytes, codec, codec_dict_id, created_at) \
         VALUES ($1, $2, $3, $4, $5, $6) \
         ON CONFLICT (chunk_hash) DO NOTHING",
        tables::filesystem::INTERNAL_BINARY_CHUNK_STORE,
    )
}

pub(crate) fn insert_binary_blob_manifest_chunk_sql() -> String {
    format!(
        "INSERT INTO {} (blob_hash, chunk_index, chunk_hash, chunk_size) \
         VALUES ($1, $2, $3, $4) \
         ON CONFLICT (blob_hash, chunk_index) DO NOTHING",
        tables::filesystem::INTERNAL_BINARY_BLOB_MANIFEST_CHUNK,
    )
}

pub(crate) fn upsert_binary_file_version_ref_sql() -> String {
    format!(
        "INSERT INTO {} (file_id, version_id, blob_hash, size_bytes, updated_at) \
         VALUES ($1, $2, $3, $4, $5) \
         ON CONFLICT (file_id, version_id) DO UPDATE SET \
         blob_hash = EXCLUDED.blob_hash, \
         size_bytes = EXCLUDED.size_bytes, \
         updated_at = EXCLUDED.updated_at",
        tables::filesystem::INTERNAL_BINARY_FILE_VERSION_REF,
    )
}

pub(crate) fn delete_unreferenced_binary_chunk_store_sql() -> String {
    format!(
        "DELETE FROM {} \
         WHERE NOT EXISTS (\
             SELECT 1 \
             FROM {} mc \
             WHERE mc.chunk_hash = {}.chunk_hash\
         )",
        tables::filesystem::INTERNAL_BINARY_CHUNK_STORE,
        tables::filesystem::INTERNAL_BINARY_BLOB_MANIFEST_CHUNK,
        tables::filesystem::INTERNAL_BINARY_CHUNK_STORE,
    )
}
