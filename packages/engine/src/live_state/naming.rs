pub(crate) const INTERNAL_RELATION_PREFIX: &str = "lix_internal_";

pub(crate) fn tracked_relation_name(schema_key: &str) -> String {
    format!("{}{schema_key}", super::TRACKED_RELATION_PREFIX)
}
