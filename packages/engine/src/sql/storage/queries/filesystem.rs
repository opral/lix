use super::super::tables;

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
