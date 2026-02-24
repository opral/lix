use super::super::tables;

pub(crate) fn delete_unreferenced_binary_file_version_ref_sql(
    state_blob_hash_expr: &str,
) -> String {
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
        tables::filesystem::INTERNAL_BINARY_FILE_VERSION_REF,
        tables::filesystem::INTERNAL_BINARY_FILE_VERSION_REF,
        tables::filesystem::INTERNAL_BINARY_FILE_VERSION_REF,
        tables::filesystem::INTERNAL_BINARY_FILE_VERSION_REF,
    )
}

pub(crate) fn delete_unreferenced_binary_blob_manifest_chunk_sql(
    state_blob_hash_expr: &str,
) -> String {
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
        tables::filesystem::INTERNAL_BINARY_BLOB_MANIFEST_CHUNK,
        tables::filesystem::INTERNAL_BINARY_BLOB_MANIFEST_CHUNK,
    )
}

pub(crate) fn delete_unreferenced_binary_blob_manifest_sql(state_blob_hash_expr: &str) -> String {
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
        tables::filesystem::INTERNAL_BINARY_BLOB_MANIFEST,
        tables::filesystem::INTERNAL_BINARY_BLOB_MANIFEST,
        tables::filesystem::INTERNAL_BINARY_BLOB_MANIFEST_CHUNK,
        tables::filesystem::INTERNAL_BINARY_BLOB_MANIFEST,
    )
}

pub(crate) fn delete_unreferenced_binary_blob_store_sql() -> String {
    format!(
        "DELETE FROM {} \
         WHERE NOT EXISTS (\
             SELECT 1 \
             FROM {} r \
             WHERE r.blob_hash = {}.blob_hash\
         )",
        tables::filesystem::INTERNAL_BINARY_BLOB_STORE,
        tables::filesystem::INTERNAL_BINARY_FILE_VERSION_REF,
        tables::filesystem::INTERNAL_BINARY_BLOB_STORE,
    )
}
