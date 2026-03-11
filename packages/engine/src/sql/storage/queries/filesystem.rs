use super::super::tables;

pub(crate) fn upsert_binary_blob_store_sql() -> String {
    format!(
        "INSERT INTO {} (blob_hash, data, size_bytes, created_at) \
         VALUES ($1, $2, $3, $4) \
         ON CONFLICT (blob_hash) DO UPDATE SET \
         data = EXCLUDED.data, \
         size_bytes = EXCLUDED.size_bytes",
        tables::filesystem::INTERNAL_BINARY_BLOB_STORE,
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
