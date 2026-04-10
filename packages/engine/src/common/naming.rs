const TRACKED_LIVE_TABLE_PREFIX: &str = "lix_internal_live_v1_";
pub(crate) const INTERNAL_BINARY_BLOB_STORE: &str = "lix_internal_binary_blob_store";

pub(crate) fn tracked_relation_name(schema_key: &str) -> String {
    format!("{TRACKED_LIVE_TABLE_PREFIX}{schema_key}")
}
