const TRACKED_LIVE_TABLE_PREFIX: &str = "lix_internal_live_v1_";

pub(crate) fn tracked_relation_name(schema_key: &str) -> String {
    format!("{TRACKED_LIVE_TABLE_PREFIX}{schema_key}")
}
