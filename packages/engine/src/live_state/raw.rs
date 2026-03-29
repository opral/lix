use std::collections::BTreeMap;

use crate::live_state::storage::{json_value_from_live_row_cell, LiveRowAccess};
use crate::{LixError, Value};
use serde_json::Value as JsonValue;

#[cfg(test)]
use crate::live_state::tracked::TrackedRow;
#[cfg(test)]
use crate::live_state::untracked::UntrackedRow;

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) enum RawRow {
    Tracked(TrackedRow),
    Untracked(UntrackedRow),
}

#[cfg(test)]
impl RawRow {
    #[cfg(test)]
    pub(crate) fn schema_key(&self) -> &str {
        match self {
            Self::Tracked(row) => &row.schema_key,
            Self::Untracked(row) => &row.schema_key,
        }
    }

    #[cfg(test)]
    pub(crate) fn values(&self) -> &BTreeMap<String, Value> {
        match self {
            Self::Tracked(row) => &row.values,
            Self::Untracked(row) => &row.values,
        }
    }
}

#[cfg(test)]
pub(crate) fn snapshot_json(access: &LiveRowAccess, row: &RawRow) -> Result<JsonValue, LixError> {
    snapshot_json_from_values(access, row.schema_key(), row.values())
}

pub(crate) fn snapshot_json_from_values(
    access: &LiveRowAccess,
    schema_key: &str,
    values: &BTreeMap<String, Value>,
) -> Result<JsonValue, LixError> {
    let mut object = serde_json::Map::new();
    for column in access.columns() {
        let Some(value) = values.get(&column.property_name) else {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                &format!(
                    "live row for schema '{}' is missing property '{}'",
                    schema_key, column.property_name
                ),
            ));
        };
        let json_value =
            json_value_from_live_row_cell(value, column.kind, schema_key, &column.column_name)?;
        if json_value.is_null() {
            if column.preserve_null_in_logical_snapshot() {
                object.insert(column.property_name.clone(), JsonValue::Null);
            }
        } else {
            object.insert(column.property_name.clone(), json_value);
        }
    }
    Ok(JsonValue::Object(object))
}

#[cfg(test)]
mod tests {
    use super::{snapshot_json, RawRow};
    use crate::live_state::storage::{
        LiveColumnKind, LiveColumnSpec, LiveRowAccess, LiveTableLayout,
    };
    use crate::live_state::tracked::TrackedRow;
    use crate::live_state::untracked::UntrackedRow;
    use crate::Value;
    use std::collections::BTreeMap;

    fn test_access() -> LiveRowAccess {
        LiveRowAccess::new(LiveTableLayout {
            schema_key: "profile".to_string(),
            columns: vec![LiveColumnSpec {
                property_name: "name".to_string(),
                column_name: "name".to_string(),
                kind: LiveColumnKind::String,
                required: true,
                nullable: false,
            }],
        })
    }

    #[test]
    fn raw_row_preserves_lane_identity() {
        let tracked = RawRow::Tracked(TrackedRow {
            entity_id: "row-1".to_string(),
            schema_key: "profile".to_string(),
            schema_version: "1".to_string(),
            file_id: "file".to_string(),
            version_id: "main".to_string(),
            global: false,
            plugin_key: "plug".to_string(),
            metadata: None,
            change_id: Some("chg-1".to_string()),
            writer_key: None,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            values: BTreeMap::from([("name".to_string(), Value::Text("Ada".to_string()))]),
        });
        let untracked = RawRow::Untracked(UntrackedRow {
            entity_id: "row-2".to_string(),
            schema_key: "profile".to_string(),
            schema_version: "1".to_string(),
            file_id: "file".to_string(),
            version_id: "main".to_string(),
            global: false,
            plugin_key: "plug".to_string(),
            metadata: None,
            writer_key: None,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            values: BTreeMap::from([("name".to_string(), Value::Text("Bea".to_string()))]),
        });

        match tracked {
            RawRow::Tracked(row) => {
                assert_eq!(row.change_id.as_deref(), Some("chg-1"));
                assert!(row.writer_key.is_none());
            }
            RawRow::Untracked(_) => panic!("expected tracked row"),
        }
        assert!(matches!(untracked, RawRow::Untracked(_)));
    }

    #[test]
    fn snapshot_json_uses_row_values_without_lane_specific_logic() {
        let access = test_access();
        let row = RawRow::Tracked(TrackedRow {
            entity_id: "row-1".to_string(),
            schema_key: "profile".to_string(),
            schema_version: "1".to_string(),
            file_id: "file".to_string(),
            version_id: "main".to_string(),
            global: false,
            plugin_key: "plug".to_string(),
            metadata: None,
            change_id: None,
            writer_key: None,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            values: BTreeMap::from([("name".to_string(), Value::Text("Ada".to_string()))]),
        });

        let snapshot = snapshot_json(&access, &row).expect("snapshot should build");
        assert_eq!(
            snapshot,
            serde_json::json!({
                "name": "Ada"
            })
        );
    }
}
