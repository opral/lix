use std::collections::{BTreeMap, BTreeSet};

use lix_schema::Schema;

use crate::changelog::ChangeRecord;
use crate::json_store::JsonSlot;
use crate::migration::schema_transition::is_bundled_plugin_schema_key;
use crate::migration::v68_to_v69_rows::ConversionPlan;
use crate::LixError;

const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";

/// One v68 record plus the materialized outer snapshot bytes it names.
#[derive(Debug, Clone)]
pub(super) struct MaterializedV68Change {
    pub(super) record: ChangeRecord,
    pub(super) snapshot_json: Option<String>,
}

#[derive(Debug, Clone)]
pub(super) struct RewrittenChange {
    pub(super) record: ChangeRecord,
    /// New out-of-band JSON content required by `record.snapshot`.
    pub(super) staged_json: Option<String>,
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
    canonical: BTreeMap<String, Schema>,
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
            if !is_bundled_plugin_schema_key(schema_key) {
                continue;
            }
            let value = outer.get("value").cloned().ok_or_else(|| {
                migration_error(format!("registered schema '{schema_key}' is missing value"))
            })?;
            let schema = lix_schema::from_value(value).map_err(|error| {
                migration_error(format!("registered schema '{schema_key}' is invalid: {error}"))
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
                .insert(fingerprint, schema);
        }

        let canonical = bundled_current_schemas()?
            .into_iter()
            .map(|schema| (schema.key.clone(), schema))
            .collect::<BTreeMap<_, _>>();
        for current in canonical.values() {
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

        let mut plans = BTreeMap::new();
        for (schema_key, versions) in schemas {
            let mut version_plans = Vec::with_capacity(versions.len());
            for (old_fingerprint, old) in versions {
                let target = canonical.get(&schema_key).ok_or_else(|| {
                    migration_error(format!("bundled schema '{schema_key}' has no v69 target"))
                })?;
                version_plans.push(HistoricalPlan {
                    old_fingerprint,
                    plan: ConversionPlan::compile(&old, target)
                        .map_err(|error| migration_error(error.to_string()))?,
                });
            }
            plans.insert(schema_key, version_plans);
        }
        Ok(Self { plans, canonical })
    }

    pub(super) fn rewrite(
        &self,
        change: &MaterializedV68Change,
    ) -> Result<RewrittenChange, LixError> {
        let mut record = change.record.clone();
        if record.schema_key == REGISTERED_SCHEMA_KEY {
            let Some(snapshot) = change.snapshot_json.as_deref() else {
                return Ok(RewrittenChange {
                    record,
                    staged_json: None,
                });
            };
            let mut outer: serde_json::Value = serde_json::from_str(snapshot).map_err(|error| {
                migration_error(format!("invalid registered-schema snapshot: {error}"))
            })?;
            let Some(schema_key) = outer.get("schema_key").and_then(serde_json::Value::as_str) else {
                return Err(migration_error("registered-schema snapshot is missing schema_key"));
            };
            if let Some(canonical) = self.canonical.get(schema_key) {
                outer
                    .as_object_mut()
                    .expect("registered schema outer row was proven to be an object")
                    .insert(
                        "value".to_owned(),
                        serde_json::to_value(canonical)
                            .map_err(|error| migration_error(error.to_string()))?,
                    );
            }
            let normalized = serde_json::to_string(&outer)
                .map_err(|error| migration_error(error.to_string()))?;
            record.snapshot = JsonSlot::from_json(&normalized);
            let staged_json = matches!(record.snapshot, JsonSlot::Ref(_)).then_some(normalized);
            return Ok(RewrittenChange {
                record,
                staged_json,
            });
        }
        if !is_bundled_plugin_schema_key(&record.schema_key) || record.snapshot.is_none() {
            return Ok(RewrittenChange {
                record,
                staged_json: None,
            });
        }
        let snapshot = change.snapshot_json.as_deref().ok_or_else(|| {
            migration_error(format!(
                "plugin row '{}:{:?}' has no materialized snapshot",
                record.schema_key, record.row_pk
            ))
        })?;
        let outer = serde_json::from_str(snapshot).map_err(|error| {
            migration_error(format!(
                "plugin row '{}:{:?}' has invalid JSON: {error}",
                record.schema_key, record.row_pk
            ))
        })?;
        let candidates = self.plans.get(&record.schema_key).ok_or_else(|| {
            migration_error(format!(
                "plugin schema '{}' has no historical registration",
                record.schema_key
            ))
        })?;
        let mut matches = Vec::new();
        let mut failures = Vec::new();
        for candidate in candidates {
            match candidate.plan.convert(&outer, &record.row_pk) {
                Ok(converted) => matches.push((candidate.old_fingerprint, converted)),
                Err(error) => failures.push(error.to_string()),
            }
        }
        if matches.len() != 1 {
            let fingerprints = matches
                .iter()
                .map(|(fingerprint, _)| hex(fingerprint))
                .collect::<BTreeSet<_>>();
            return Err(migration_error(format!(
                "plugin row '{}:{:?}' matched {} historical schemas ({:?}); failures: {}",
                record.schema_key,
                record.row_pk,
                matches.len(),
                fingerprints,
                failures.join("; ")
            )));
        }
        let (_, converted) = matches.pop().expect("exactly one candidate");
        record.snapshot = JsonSlot::None;
        record.typed_payload = Some(converted.durable_payload);
        Ok(RewrittenChange {
            record,
            staged_json: None,
        })
    }
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

fn hex(bytes: &[u8; 32]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn migration_error(message: impl Into<String>) -> LixError {
    LixError::new("LIX_ERROR_MIGRATION_FAILED", message.into())
}
