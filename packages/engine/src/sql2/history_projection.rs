use serde_json::Value as JsonValue;

use crate::entity_identity::{EntityIdentity, EntityIdentityPart};
use crate::LixError;

/// Shared projection contract for typed history views.
///
/// On tombstone rows (`snapshot_content IS NULL`), identity columns survive by
/// projecting from canonical entity identity. Non-identity columns must remain
/// NULL because there is no snapshot to project payload from.
pub(crate) enum HistoryIdentityProjection<'a> {
    PrimaryKeyPaths(&'a [Vec<String>]),
    SingleColumn { column: &'a str },
}

pub(crate) fn tombstone_identity_column_value(
    column_name: &str,
    entity_id: &str,
    projection: HistoryIdentityProjection<'_>,
) -> Result<Option<JsonValue>, LixError> {
    match projection {
        HistoryIdentityProjection::SingleColumn { column } => {
            if column_name == column {
                Ok(Some(JsonValue::String(entity_id.to_string())))
            } else {
                Ok(None)
            }
        }
        HistoryIdentityProjection::PrimaryKeyPaths(primary_key_paths) => {
            primary_key_tombstone_value(column_name, entity_id, primary_key_paths)
        }
    }
}

fn primary_key_tombstone_value(
    column_name: &str,
    entity_id: &str,
    primary_key_paths: &[Vec<String>],
) -> Result<Option<JsonValue>, LixError> {
    let Some(part_index) = primary_key_paths
        .iter()
        .position(|path| path.as_slice() == [column_name])
    else {
        return Ok(None);
    };

    let identity = EntityIdentity::from_string(entity_id).map_err(|error| {
        LixError::unknown(format!(
            "failed to decode history tombstone entity identity: {error}"
        ))
    })?;
    Ok(identity
        .parts
        .get(part_index)
        .map(entity_identity_part_json_value))
}

fn entity_identity_part_json_value(part: &EntityIdentityPart) -> JsonValue {
    match part {
        EntityIdentityPart::String(value) => JsonValue::String(value.clone()),
        EntityIdentityPart::Bool(value) => JsonValue::Bool(*value),
        EntityIdentityPart::Number(value) => value
            .parse::<i64>()
            .map(|value| JsonValue::Number(value.into()))
            .or_else(|_| {
                value
                    .parse::<u64>()
                    .map(|value| JsonValue::Number(value.into()))
            })
            .ok()
            .or_else(|| {
                value
                    .parse::<f64>()
                    .ok()
                    .and_then(serde_json::Number::from_f64)
                    .map(JsonValue::Number)
            })
            .unwrap_or_else(|| JsonValue::String(value.clone())),
    }
}
