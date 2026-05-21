use serde_json::Value as JsonValue;

use crate::entity_pk::EntityPk;
use crate::LixError;

/// Shared projection contract for typed history views.
///
/// On tombstone rows (`snapshot_content IS NULL`), identity columns survive by
/// projecting from canonical entity primary key. Non-identity columns must remain
/// NULL because there is no snapshot to project payload from.
pub(crate) enum HistoryIdentityProjection<'a> {
    PrimaryKeyPaths(&'a [Vec<String>]),
    SingleColumn { column: &'a str },
}

pub(crate) fn tombstone_identity_column_value(
    column_name: &str,
    entity_pk: &str,
    projection: HistoryIdentityProjection<'_>,
) -> Result<Option<JsonValue>, LixError> {
    match projection {
        HistoryIdentityProjection::SingleColumn { column } => {
            if column_name == column {
                Ok(Some(JsonValue::String(entity_pk.to_string())))
            } else {
                Ok(None)
            }
        }
        HistoryIdentityProjection::PrimaryKeyPaths(primary_key_paths) => {
            primary_key_tombstone_value(column_name, entity_pk, primary_key_paths)
        }
    }
}

fn primary_key_tombstone_value(
    column_name: &str,
    entity_pk: &str,
    primary_key_paths: &[Vec<String>],
) -> Result<Option<JsonValue>, LixError> {
    let Some(part_index) = primary_key_paths
        .iter()
        .position(|path| path.as_slice() == [column_name])
    else {
        return Ok(None);
    };

    let identity = EntityPk::from_json_array_text(entity_pk).map_err(|error| {
        LixError::unknown(format!(
            "failed to decode history tombstone entity primary key: {error}"
        ))
    })?;
    Ok(identity
        .parts
        .get(part_index)
        .map(|part| JsonValue::String(part.clone())))
}
