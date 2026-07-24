use crate::LixError;

/// Project a single-string history entity pk as the canonical JSON array
/// text exposed by the `lixcol_entity_pk` column.
pub(super) fn entity_pk_json_array(entity_pk: &str) -> Result<String, LixError> {
    serde_json::to_string(&[entity_pk]).map_err(|error| {
        LixError::unknown(format!(
            "failed to encode history entity pk as JSON: {error}"
        ))
    })
}
