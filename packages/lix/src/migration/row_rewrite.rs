use std::collections::{BTreeMap, BTreeSet};

use lix_schema::Schema;

use crate::LixError;
use crate::changelog::ChangeRecord;
use crate::migration::schema_transition::registered_schema_row_to_v69;
use crate::migration::v68::V68ChangeRecord;
use crate::migration::v68_to_v69_rows::ConversionPlan;

const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";

/// One v68 record plus the materialized outer snapshot bytes it names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct MaterializedV68Change {
    pub(super) record: V68ChangeRecord,
    pub(super) snapshot_json: Option<String>,
    pub(super) metadata_json: Option<String>,
}

#[derive(Debug, Clone)]
pub(super) struct RewrittenChange {
    pub(super) record: ChangeRecord,
}

#[derive(Debug)]
struct HistoricalPlan {
    old_fingerprint: [u8; 32],
    plan: ConversionPlan,
}

/// Historical schema plans indexed by schema key and old wire fingerprint.
#[derive(Debug, Default)]
pub(super) struct HistoricalSchemaCatalog {
    plans: BTreeMap<String, Vec<HistoricalPlan>>,
}

impl HistoricalSchemaCatalog {
    pub(super) fn from_changes(changes: &[MaterializedV68Change]) -> Result<Self, LixError> {
        let mut schemas = BTreeMap::<String, BTreeMap<[u8; 32], Schema>>::new();
        for change in changes {
            if change.record.schema_key != REGISTERED_SCHEMA_KEY {
                continue;
            }
            let Some(snapshot) = change.snapshot_json.as_deref() else {
                continue;
            };
            let outer: serde_json::Value = serde_json::from_str(snapshot).map_err(|error| {
                migration_error(format!("invalid registered-schema snapshot: {error}"))
            })?;
            let Some(schema_key) = outer.get("schema_key").and_then(serde_json::Value::as_str)
            else {
                return Err(migration_error(
                    "registered-schema snapshot is missing schema_key",
                ));
            };
            let value = outer.get("value").cloned().ok_or_else(|| {
                migration_error(format!("registered schema '{schema_key}' is missing value"))
            })?;
            let schema = lix_schema::from_value(value).map_err(|error| {
                migration_error(format!(
                    "registered schema '{schema_key}' is invalid: {error}"
                ))
            })?;
            if schema.key != schema_key {
                return Err(migration_error(format!(
                    "registered schema envelope '{schema_key}' contains '{}'",
                    schema.key
                )));
            }
            let fingerprint = *schema
                .wire_fingerprint()
                .map_err(|error| migration_error(error.to_string()))?
                .as_bytes();
            schemas
                .entry(schema_key.to_owned())
                .or_default()
                .insert(fingerprint, schema.clone());
        }

        let mut canonical = builtin_current_schemas()?
            .into_iter()
            .chain(bundled_current_schemas()?)
            .into_iter()
            .map(|schema| (schema.key.clone(), schema))
            .collect::<BTreeMap<_, _>>();
        for (schema_key, versions) in &schemas {
            if canonical.contains_key(schema_key) {
                continue;
            }
            // v68 does not retain enough liveness information here to split
            // delete-and-reregister histories into independent schema epochs.
            // Reject incompatible historical definitions conservatively even
            // when one may have belonged to an already-deleted lifecycle.
            let candidates = versions
                .values()
                .filter(|candidate| {
                    versions.values().all(|old| {
                        old == *candidate || lix_schema::validate_amendment(old, candidate).is_ok()
                    })
                })
                .collect::<Vec<_>>();
            if candidates.len() != 1 {
                return Err(migration_error(format!(
                    "registered schema '{schema_key}' has no unique maximal amendment across fingerprints {:?}",
                    versions.keys().map(hex).collect::<Vec<_>>()
                )));
            }
            canonical.insert(schema_key.clone(), candidates[0].clone());
        }
        for current in canonical.values() {
            insert_schema_version(&mut schemas, current.clone())?;
            if crate::migration::schema_transition::is_bundled_plugin_schema_key(&current.key) {
                let mut historical = current.clone();
                for column in &mut historical.columns {
                    if matches!(
                        (historical.key.as_str(), column.name.as_str()),
                        ("excalidraw_element", "element_json")
                            | ("excalidraw_file", "file_json")
                            | ("json_array_item", "scalar_json")
                            | ("json_object_member", "scalar_json")
                            | ("json_root", "scalar_json")
                            | ("markdown_node", "format_json")
                            | ("markdown_node", "payload_json")
                    ) {
                        column.data_type = lix_schema::DataType::Text;
                    }
                }
                let markdown_fixture_base =
                    (current.key == "markdown_node").then(|| historical.clone());
                insert_schema_version(&mut schemas, historical)?;
                if let Some(mut fixture_variant) = markdown_fixture_base {
                    if let Some(parent) = fixture_variant
                        .columns
                        .iter_mut()
                        .find(|column| column.name == "parent_id")
                    {
                        parent.data_type = lix_schema::DataType::Text;
                        insert_schema_version(&mut schemas, fixture_variant)?;
                    }
                }
            }
        }

        let mut plans = BTreeMap::new();
        for (schema_key, versions) in schemas {
            let mut version_plans = Vec::with_capacity(versions.len());
            for (old_fingerprint, old) in versions {
                let target = canonical.get(&schema_key).ok_or_else(|| {
                    migration_error(format!("schema '{schema_key}' has no v69 target"))
                })?;
                version_plans.push(HistoricalPlan {
                    old_fingerprint,
                    plan: ConversionPlan::compile(&old, target)
                        .map_err(|error| migration_error(error.to_string()))?,
                });
            }
            plans.insert(schema_key, version_plans);
        }
        Ok(Self { plans })
    }

    pub(super) fn rewrite(
        &self,
        change: &MaterializedV68Change,
    ) -> Result<RewrittenChange, LixError> {
        let record = change.record.clone();
        if record.snapshot.is_none() {
            return Ok(RewrittenChange {
                record: current_record(record, None, change.metadata_json.as_deref())?,
            });
        }
        let snapshot = change.snapshot_json.as_deref().ok_or_else(|| {
            migration_error(format!(
                "live row '{}:{:?}' has no materialized snapshot",
                record.schema_key, record.row_pk
            ))
        })?;
        let mut outer = serde_json::from_str(snapshot).map_err(|error| {
            migration_error(format!(
                "row '{}:{:?}' has invalid JSON: {error}",
                record.schema_key, record.row_pk
            ))
        })?;
        if record.schema_key == REGISTERED_SCHEMA_KEY {
            outer = registered_schema_row_to_v69(&outer)
                .map_err(|error| migration_error(error.to_string()))?;
        }
        let candidates = self.plans.get(&record.schema_key).ok_or_else(|| {
            migration_error(format!(
                "schema '{}' has no historical registration or built-in definition",
                record.schema_key
            ))
        })?;
        // More than one historical schema may accept the same outer JSON. In
        // particular, a row written before an append-nullable amendment also
        // satisfies the amended schema because the absent column materializes
        // as SQL NULL. That is harmless when every matching plan produces the
        // same canonical v69 payload; only distinct results are ambiguous.
        let mut matches = BTreeMap::<Vec<u8>, BTreeSet<[u8; 32]>>::new();
        let mut failures = Vec::new();
        for candidate in candidates {
            match candidate.plan.convert(&outer, &record.row_pk) {
                Ok(converted) => {
                    matches
                        .entry(converted.durable_payload)
                        .or_default()
                        .insert(candidate.old_fingerprint);
                }
                Err(error) => failures.push(error.to_string()),
            }
        }
        if matches.len() != 1 {
            let fingerprints = matches.values().flatten().map(hex).collect::<BTreeSet<_>>();
            return Err(migration_error(format!(
                "row '{}:{:?}' matched {} historical schemas ({:?}); failures: {}",
                record.schema_key,
                record.row_pk,
                matches.len(),
                fingerprints,
                failures.join("; ")
            )));
        }
        let (durable_payload, _) = matches.pop_first().expect("exactly one distinct candidate");
        Ok(RewrittenChange {
            record: current_record(
                record,
                Some(durable_payload),
                change.metadata_json.as_deref(),
            )?,
        })
    }
}

fn current_record(
    record: V68ChangeRecord,
    snapshot: Option<Vec<u8>>,
    metadata_json: Option<&str>,
) -> Result<ChangeRecord, LixError> {
    // v68 permitted metadata to accompany a tombstone. v69 makes deletion
    // canonical solely through snapshot=None, so legacy tombstone metadata
    // must not cross the migration boundary.
    let metadata = if snapshot.is_none() {
        None
    } else {
        metadata_json
            .map(|json| {
                serde_json::from_str(json)
                    .map(lix_schema::Jsonb::from_value)
                    .map_err(|error| migration_error(format!("metadata is invalid JSON: {error}")))
            })
            .transpose()?
    };
    Ok(ChangeRecord {
        format_version: record.format_version,
        change_id: record.change_id,
        account_id: record.account_id,
        schema_key: record.schema_key,
        row_pk: record.row_pk,
        file_id: record.file_id,
        metadata,
        snapshot,
        created_at: record.created_at,
        origin_key: record.origin_key,
    })
}

fn insert_schema_version(
    schemas: &mut BTreeMap<String, BTreeMap<[u8; 32], Schema>>,
    schema: Schema,
) -> Result<(), LixError> {
    let fingerprint = *schema
        .wire_fingerprint()
        .map_err(|error| migration_error(error.to_string()))?
        .as_bytes();
    schemas
        .entry(schema.key.clone())
        .or_default()
        .entry(fingerprint)
        .or_insert(schema);
    Ok(())
}

fn bundled_current_schemas() -> Result<Vec<Schema>, LixError> {
    const SCHEMAS: &[&str] = &[
        include_str!("../../../../plugins/csv/schema/csv_row.json"),
        include_str!("../../../../plugins/csv/schema/csv_table.json"),
        include_str!("../../../../plugins/excalidraw/schema/excalidraw_element.json"),
        include_str!("../../../../plugins/excalidraw/schema/excalidraw_file.json"),
        include_str!("../../../../plugins/excalidraw/schema/excalidraw_scene.json"),
        include_str!("../../../../plugins/json/schema/json_array_item.json"),
        include_str!("../../../../plugins/json/schema/json_object_member.json"),
        include_str!("../../../../plugins/json/schema/json_root.json"),
        include_str!("../../../../plugins/markdown/schema/markdown_node.json"),
        include_str!("../../../../plugins/text/schema/text_line.json"),
    ];
    SCHEMAS
        .iter()
        .map(|json| {
            lix_schema::from_json(json)
                .map_err(|error| migration_error(format!("bundled schema is invalid: {error}")))
        })
        .collect()
}

fn builtin_current_schemas() -> Result<Vec<Schema>, LixError> {
    crate::schema::seed_schema_definitions()
        .into_iter()
        .map(|value| {
            lix_schema::from_value(value.clone())
                .map_err(|error| migration_error(format!("built-in schema is invalid: {error}")))
        })
        .collect()
}

fn hex(bytes: &[u8; 32]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn migration_error(message: impl Into<String>) -> LixError {
    LixError::new("LIX_ERROR_MIGRATION_FAILED", message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changelog::ChangeId;
    use crate::common::LixTimestamp;
    use crate::json_store::LegacyJsonValue;
    use crate::row_pk::RowPk;
    use serde_json::json;

    fn v68_change(
        label: &str,
        schema_key: &str,
        row_pk: RowPk,
        snapshot: Option<serde_json::Value>,
    ) -> MaterializedV68Change {
        let snapshot_json = snapshot.map(|value| serde_json::to_string(&value).unwrap());
        MaterializedV68Change {
            record: V68ChangeRecord {
                format_version: 1,
                change_id: ChangeId::for_test_label(label),
                account_id: crate::ANONYMOUS_ACCOUNT_ID.to_owned(),
                schema_key: schema_key.to_owned(),
                row_pk,
                file_id: None,
                snapshot: snapshot_json
                    .as_deref()
                    .map(LegacyJsonValue::from_json)
                    .unwrap_or(LegacyJsonValue::None),
                metadata: LegacyJsonValue::None,
                created_at: LixTimestamp::expect_parse(
                    "migration test timestamp",
                    "2026-08-19T00:00:00Z",
                ),
                origin_key: None,
            },
            snapshot_json,
            metadata_json: None,
        }
    }

    #[test]
    fn rewrites_builtin_live_rows_and_deletes_to_the_v69_invariant() {
        let catalog = HistoricalSchemaCatalog::from_changes(&[]).unwrap();
        let mut live = v68_change(
            "builtin-live",
            "lix_key_value",
            RowPk::single("probe"),
            Some(json!({"key": "probe", "value": 42})),
        );
        live.record.metadata = LegacyJsonValue::from_json(r#"{"source":"v68"}"#);
        live.metadata_json = Some(r#"{"source":"v68"}"#.to_owned());
        let mut deleted = v68_change(
            "builtin-delete",
            "lix_key_value",
            RowPk::single("probe"),
            None,
        );
        deleted.record.metadata = LegacyJsonValue::from_json(r#"{"source":"v68-delete"}"#);
        deleted.metadata_json = Some(r#"{"source":"v68-delete"}"#.to_owned());

        let rewritten = catalog.rewrite(&live).unwrap();
        assert!(rewritten.record.snapshot.is_some());
        assert_eq!(
            rewritten
                .record
                .metadata
                .as_ref()
                .expect("metadata should migrate")
                .to_json_string()
                .expect("metadata should materialize"),
            r#"{"source":"v68"}"#
        );
        let rewritten_delete = catalog.rewrite(&deleted).unwrap().record;
        assert_eq!(rewritten_delete.snapshot, None);
        assert_eq!(rewritten_delete.metadata, None);
    }

    #[test]
    fn rewrites_registered_custom_schema_rows_without_a_json_fallback() {
        let custom_schema = json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "custom_note",
            "columns": [
                {"name": "id", "type": "text", "nullable": false},
                {"name": "body", "type": "text", "nullable": false}
            ],
            "primary_key": ["id"]
        });
        let registration = v68_change(
            "custom-registration",
            REGISTERED_SCHEMA_KEY,
            RowPk::single("custom_note"),
            Some(json!({"schema_key": "custom_note", "value": custom_schema})),
        );
        let row = v68_change(
            "custom-row",
            "custom_note",
            RowPk::single("note-1"),
            Some(json!({"id": "note-1", "body": "typed"})),
        );
        let catalog = HistoricalSchemaCatalog::from_changes(&[registration.clone(), row.clone()])
            .expect("custom schema should compile");

        assert!(
            catalog
                .rewrite(&registration)
                .unwrap()
                .record
                .snapshot
                .is_some()
        );
        assert!(catalog.rewrite(&row).unwrap().record.snapshot.is_some());
    }

    #[test]
    fn deduplicates_historical_matches_after_append_nullable_amendment() {
        let original = json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "amended_note",
            "columns": [
                {"name": "id", "type": "text", "nullable": false}
            ],
            "primary_key": ["id"]
        });
        let amended = json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "amended_note",
            "columns": [
                {"name": "id", "type": "text", "nullable": false},
                {
                    "name": "annotation",
                    "type": "text",
                    "nullable": true,
                    "default_value": "from-default"
                }
            ],
            "primary_key": ["id"]
        });
        let original_registration = v68_change(
            "amended-original-registration",
            REGISTERED_SCHEMA_KEY,
            RowPk::single("amended_note"),
            Some(json!({"schema_key": "amended_note", "value": original})),
        );
        let mut amended_registration = v68_change(
            "amended-latest-registration",
            REGISTERED_SCHEMA_KEY,
            RowPk::single("amended_note"),
            Some(json!({"schema_key": "amended_note", "value": amended})),
        );
        amended_registration.record.created_at =
            LixTimestamp::expect_parse("migration test timestamp", "2026-08-19T00:00:01Z");
        let historical_row = v68_change(
            "amended-historical-row",
            "amended_note",
            RowPk::single("note-1"),
            Some(json!({"id": "note-1"})),
        );
        let catalog = HistoricalSchemaCatalog::from_changes(&[
            original_registration,
            amended_registration,
            historical_row.clone(),
        ])
        .expect("amended schema history should compile");

        let rewritten = catalog
            .rewrite(&historical_row)
            .expect("identical historical conversions must not be ambiguous")
            .record;
        let decoded = crate::plugin::runtime::WasmTypedRow::decode_durable_payload(
            rewritten.snapshot.expect("live row has a snapshot").into(),
            "amended_note",
            &RowPk::single("note-1"),
        );
        assert_eq!(
            decoded.unwrap().row.get("annotation"),
            Some(&lix_schema::Value::Text("from-default".to_owned()))
        );
    }

    #[test]
    fn selects_the_unique_maximal_custom_schema_amendment() {
        let original = json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "linear_note",
            "columns": [{"name": "id", "type": "text", "nullable": false}],
            "primary_key": ["id"]
        });
        let amended_once = json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "linear_note",
            "columns": [
                {"name": "id", "type": "text", "nullable": false},
                {"name": "annotation", "type": "text", "nullable": true}
            ],
            "primary_key": ["id"]
        });
        let amended_twice = json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "linear_note",
            "columns": [
                {"name": "id", "type": "text", "nullable": false},
                {"name": "annotation", "type": "text", "nullable": true},
                {"name": "reviewer", "type": "text", "nullable": true}
            ],
            "primary_key": ["id"]
        });
        let registrations = [original, amended_once, amended_twice]
            .into_iter()
            .enumerate()
            .map(|(index, value)| {
                v68_change(
                    &format!("linear-registration-{index}"),
                    REGISTERED_SCHEMA_KEY,
                    RowPk::single("linear_note"),
                    Some(json!({"schema_key": "linear_note", "value": value})),
                )
            })
            .collect::<Vec<_>>();

        HistoricalSchemaCatalog::from_changes(&registrations)
            .expect("a linear amendment history has one maximal schema");
    }

    #[test]
    fn rejects_divergent_custom_schema_amendments() {
        let original = json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "divergent_note",
            "columns": [{"name": "id", "type": "text", "nullable": false}],
            "primary_key": ["id"]
        });
        let left = json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "divergent_note",
            "columns": [
                {"name": "id", "type": "text", "nullable": false},
                {"name": "left", "type": "text", "nullable": true}
            ],
            "primary_key": ["id"]
        });
        let right = json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "divergent_note",
            "columns": [
                {"name": "id", "type": "text", "nullable": false},
                {"name": "right", "type": "text", "nullable": true}
            ],
            "primary_key": ["id"]
        });
        let registrations = [original, left, right]
            .into_iter()
            .enumerate()
            .map(|(index, value)| {
                v68_change(
                    &format!("divergent-registration-{index}"),
                    REGISTERED_SCHEMA_KEY,
                    RowPk::single("divergent_note"),
                    Some(json!({"schema_key": "divergent_note", "value": value})),
                )
            })
            .collect::<Vec<_>>();

        let error = HistoricalSchemaCatalog::from_changes(&registrations)
            .expect_err("sibling amendments must not be ordered by timestamp");
        assert!(error.message.contains("no unique maximal amendment"));
    }
}
