use serde_json::Value as JsonValue;

use crate::LixError;
use crate::entity_pk::EntityPk;

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
    if !primary_key_paths
        .iter()
        .any(|path| path.first().is_some_and(|root| root == column_name))
    {
        return Ok(None);
    }

    let identity = EntityPk::from_json_array_text(entity_pk).map_err(|error| {
        LixError::unknown(format!(
            "failed to decode history tombstone entity primary key: {error}"
        ))
    })?;
    if identity.parts.len() != primary_key_paths.len() {
        return Err(LixError::unknown(format!(
            "history tombstone entity primary key has {} part(s), but its schema declares {} primary-key path(s)",
            identity.parts.len(),
            primary_key_paths.len()
        )));
    }

    let mut projected = JsonValue::Null;
    for (path, part) in primary_key_paths.iter().zip(&identity.parts) {
        let Some((root, nested_path)) = path.split_first() else {
            return Err(LixError::unknown(
                "history tombstone schema contains an empty primary-key path",
            ));
        };
        if root != column_name {
            continue;
        }
        insert_tombstone_identity_part(
            &mut projected,
            nested_path,
            JsonValue::String(part.clone()),
        )?;
    }
    Ok(Some(projected))
}

fn insert_tombstone_identity_part(
    target: &mut JsonValue,
    path: &[String],
    value: JsonValue,
) -> Result<(), LixError> {
    let Some((segment, remaining)) = path.split_first() else {
        if !target.is_null() && target != &value {
            return Err(LixError::unknown(
                "history tombstone schema has conflicting primary-key paths",
            ));
        }
        *target = value;
        return Ok(());
    };

    if target.is_null() {
        *target = JsonValue::Object(serde_json::Map::new());
    }
    let object = target.as_object_mut().ok_or_else(|| {
        LixError::unknown("history tombstone schema has conflicting primary-key paths")
    })?;
    insert_tombstone_identity_part(
        object.entry(segment.clone()).or_insert(JsonValue::Null),
        remaining,
        value,
    )
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{HistoryIdentityProjection, tombstone_identity_column_value};

    #[test]
    fn reconstructs_nested_composite_identity_roots_for_tombstones() {
        let primary_key_paths = vec![
            vec!["identity".to_string(), "tenant".to_string()],
            vec![
                "identity".to_string(),
                "document".to_string(),
                "id".to_string(),
            ],
            vec!["locale".to_string()],
        ];

        let identity = tombstone_identity_column_value(
            "identity",
            r#"["acme","welcome","en"]"#,
            HistoryIdentityProjection::PrimaryKeyPaths(&primary_key_paths),
        )
        .expect("nested tombstone identity should project");
        let locale = tombstone_identity_column_value(
            "locale",
            r#"["acme","welcome","en"]"#,
            HistoryIdentityProjection::PrimaryKeyPaths(&primary_key_paths),
        )
        .expect("top-level tombstone identity should project");

        assert_eq!(
            identity,
            Some(json!({
                "tenant": "acme",
                "document": { "id": "welcome" }
            }))
        );
        assert_eq!(locale, Some(json!("en")));
    }

    #[test]
    fn rejects_identity_arity_that_does_not_match_the_schema() {
        let error = tombstone_identity_column_value(
            "id",
            r#"["one","two"]"#,
            HistoryIdentityProjection::PrimaryKeyPaths(&[vec!["id".to_string()]]),
        )
        .expect_err("mismatched identity arity should fail");

        assert!(
            error
                .message
                .contains("2 part(s), but its schema declares 1")
        );
    }
}
