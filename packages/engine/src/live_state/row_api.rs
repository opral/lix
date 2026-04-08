use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value as JsonValue;

use crate::{LixBackend, LixError, Value};

use super::constraints::ScanConstraint;
use super::tracked::{ExactTrackedRowRequest, TrackedScanRequest};
use super::untracked::{ExactUntrackedRowRequest, UntrackedScanRequest};
use super::{
    load_exact_tracked_row_with_backend, load_exact_tracked_tombstone_with_executor,
    load_exact_untracked_row_with_backend, load_live_read_contract_with_backend,
    scan_tracked_rows_with_backend, scan_tracked_tombstones_with_executor,
    scan_untracked_rows_with_backend,
};
use crate::schema::{schema_key_from_definition, SchemaKey};
use crate::version_state::GLOBAL_VERSION_ID;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RowReadMode {
    Tracked,
    Untracked,
    Effective,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RowQuery {
    pub schema_key: String,
    pub version_id: String,
    pub mode: RowReadMode,
    #[serde(default)]
    pub constraints: Vec<ScanConstraint>,
    #[serde(default)]
    pub include_tombstones: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ExactRowQuery {
    pub schema_key: String,
    pub version_id: String,
    pub entity_id: String,
    pub file_id: String,
    pub mode: RowReadMode,
    #[serde(default)]
    pub include_tombstones: bool,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Row {
    pub entity_id: String,
    pub file_id: String,
    pub schema_key: String,
    pub schema_version: String,
    pub version_id: String,
    pub plugin_key: String,
    pub writer_key: Option<String>,
    pub global: bool,
    pub untracked: bool,
    pub snapshot_content: Option<String>,
    pub values: BTreeMap<String, Value>,
    pub tombstone: bool,
}

pub fn decode_registered_schema_row(row: &Row) -> Result<Option<(SchemaKey, JsonValue)>, LixError> {
    if row.tombstone {
        return Ok(None);
    }

    if row.schema_key != "lix_registered_schema" {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "expected lix_registered_schema row, got schema_key={}",
                row.schema_key
            ),
        ));
    }

    let Some(snapshot_content) = row.snapshot_content.as_deref() else {
        return Ok(None);
    };

    let snapshot: JsonValue = serde_json::from_str(snapshot_content).map_err(|err| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("invalid registered schema snapshot JSON: {err}"),
        )
    })?;
    let schema = snapshot.get("value").cloned().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "registered schema snapshot missing value",
        )
    })?;
    let key = schema_key_from_definition(&schema)?;
    Ok(Some((key, schema)))
}

pub async fn scan_rows(backend: &dyn LixBackend, request: &RowQuery) -> Result<Vec<Row>, LixError> {
    match request.mode {
        RowReadMode::Tracked => scan_tracked_rows(backend, request).await,
        RowReadMode::Untracked => scan_untracked_rows(backend, request).await,
        RowReadMode::Effective => scan_effective_rows(backend, request).await,
    }
}

pub async fn load_exact_row(
    backend: &dyn LixBackend,
    request: &ExactRowQuery,
) -> Result<Option<Row>, LixError> {
    match request.mode {
        RowReadMode::Tracked => load_exact_tracked_row(backend, request).await,
        RowReadMode::Untracked => load_exact_untracked_row(backend, request).await,
        RowReadMode::Effective => load_exact_effective_row(backend, request).await,
    }
}

async fn scan_tracked_rows(
    backend: &dyn LixBackend,
    request: &RowQuery,
) -> Result<Vec<Row>, LixError> {
    let contract = load_live_read_contract_with_backend(backend, &request.schema_key).await?;
    let mut rows = scan_tracked_rows_with_backend(
        backend,
        &TrackedScanRequest {
            schema_key: request.schema_key.clone(),
            version_id: request.version_id.clone(),
            constraints: request.constraints.clone(),
            required_columns: Vec::new(),
        },
    )
    .await?
    .into_iter()
    .map(|row| tracked_row_to_row(row, &contract))
    .collect::<Result<Vec<_>, _>>()?;

    if request.include_tombstones {
        let mut executor = backend;
        let tombstones = scan_tracked_tombstones_with_executor(
            &mut executor,
            &TrackedScanRequest {
                schema_key: request.schema_key.clone(),
                version_id: request.version_id.clone(),
                constraints: request.constraints.clone(),
                required_columns: Vec::new(),
            },
        )
        .await?;
        rows.extend(tombstones.into_iter().map(tracked_tombstone_to_row));
    }

    rows.sort_by(row_sort_key);
    Ok(rows)
}

async fn scan_untracked_rows(
    backend: &dyn LixBackend,
    request: &RowQuery,
) -> Result<Vec<Row>, LixError> {
    let contract = load_live_read_contract_with_backend(backend, &request.schema_key).await?;
    let mut rows = scan_untracked_rows_with_backend(
        backend,
        &UntrackedScanRequest {
            schema_key: request.schema_key.clone(),
            version_id: request.version_id.clone(),
            constraints: request.constraints.clone(),
            required_columns: Vec::new(),
        },
    )
    .await?
    .into_iter()
    .map(|row| untracked_row_to_row(row, &contract))
    .collect::<Result<Vec<_>, _>>()?;

    rows.sort_by(row_sort_key);
    Ok(rows)
}

async fn load_exact_tracked_row(
    backend: &dyn LixBackend,
    request: &ExactRowQuery,
) -> Result<Option<Row>, LixError> {
    let contract = load_live_read_contract_with_backend(backend, &request.schema_key).await?;
    if let Some(row) = load_exact_tracked_row_with_backend(
        backend,
        &ExactTrackedRowRequest {
            schema_key: request.schema_key.clone(),
            version_id: request.version_id.clone(),
            entity_id: request.entity_id.clone(),
            file_id: Some(request.file_id.clone()),
        },
    )
    .await?
    {
        return tracked_row_to_row(row, &contract).map(Some);
    }

    if !request.include_tombstones {
        return Ok(None);
    }

    let mut executor = backend;
    let tombstone = load_exact_tracked_tombstone_with_executor(
        &mut executor,
        &ExactTrackedRowRequest {
            schema_key: request.schema_key.clone(),
            version_id: request.version_id.clone(),
            entity_id: request.entity_id.clone(),
            file_id: Some(request.file_id.clone()),
        },
    )
    .await?;
    Ok(tombstone.map(tracked_tombstone_to_row))
}

async fn load_exact_untracked_row(
    backend: &dyn LixBackend,
    request: &ExactRowQuery,
) -> Result<Option<Row>, LixError> {
    let contract = load_live_read_contract_with_backend(backend, &request.schema_key).await?;
    let row = load_exact_untracked_row_with_backend(
        backend,
        &ExactUntrackedRowRequest {
            schema_key: request.schema_key.clone(),
            version_id: request.version_id.clone(),
            entity_id: request.entity_id.clone(),
            file_id: Some(request.file_id.clone()),
        },
    )
    .await?;
    row.map(|row| untracked_row_to_row(row, &contract))
        .transpose()
}

async fn scan_effective_rows(
    backend: &dyn LixBackend,
    request: &RowQuery,
) -> Result<Vec<Row>, LixError> {
    let mut resolved = BTreeMap::<(String, String), Row>::new();
    let mut hidden = BTreeSet::<(String, String)>::new();
    let lanes = effective_lanes(&request.version_id);

    for lane in lanes {
        for row in scan_lane_rows(backend, request, lane).await? {
            let key = (row.entity_id.clone(), row.file_id.clone());
            if resolved.contains_key(&key) || hidden.contains(&key) {
                continue;
            }

            if row.tombstone {
                if request.include_tombstones {
                    resolved.insert(key.clone(), row);
                }
                hidden.insert(key);
            } else {
                resolved.insert(key, row);
            }
        }
    }

    Ok(resolved.into_values().collect())
}

async fn load_exact_effective_row(
    backend: &dyn LixBackend,
    request: &ExactRowQuery,
) -> Result<Option<Row>, LixError> {
    let query = RowQuery {
        schema_key: request.schema_key.clone(),
        version_id: request.version_id.clone(),
        mode: RowReadMode::Effective,
        constraints: vec![
            ScanConstraint {
                field: super::ScanField::EntityId,
                operator: super::ScanOperator::Eq(Value::Text(request.entity_id.clone())),
            },
            ScanConstraint {
                field: super::ScanField::FileId,
                operator: super::ScanOperator::Eq(Value::Text(request.file_id.clone())),
            },
        ],
        include_tombstones: request.include_tombstones,
    };
    scan_effective_rows(backend, &query)
        .await
        .map(|rows| rows.into_iter().next())
}

#[derive(Clone, Copy)]
enum EffectiveLane {
    LocalTracked,
    LocalUntracked,
    GlobalTracked,
    GlobalUntracked,
}

impl EffectiveLane {
    fn is_global(self) -> bool {
        matches!(self, Self::GlobalTracked | Self::GlobalUntracked)
    }

    fn is_untracked(self) -> bool {
        matches!(self, Self::LocalUntracked | Self::GlobalUntracked)
    }
}

fn effective_lanes(version_id: &str) -> Vec<EffectiveLane> {
    let mut lanes = vec![EffectiveLane::LocalUntracked, EffectiveLane::LocalTracked];
    if version_id != GLOBAL_VERSION_ID {
        lanes.push(EffectiveLane::GlobalUntracked);
        lanes.push(EffectiveLane::GlobalTracked);
    }
    lanes
}

fn lane_version_id(requested_version_id: &str, lane: EffectiveLane) -> String {
    if lane.is_global() {
        GLOBAL_VERSION_ID.to_string()
    } else {
        requested_version_id.to_string()
    }
}

async fn scan_lane_rows(
    backend: &dyn LixBackend,
    request: &RowQuery,
    lane: EffectiveLane,
) -> Result<Vec<Row>, LixError> {
    if lane.is_untracked() {
        let contract = load_live_read_contract_with_backend(backend, &request.schema_key).await?;
        return scan_untracked_rows_with_backend(
            backend,
            &UntrackedScanRequest {
                schema_key: request.schema_key.clone(),
                version_id: lane_version_id(&request.version_id, lane),
                constraints: request.constraints.clone(),
                required_columns: Vec::new(),
            },
        )
        .await?
        .into_iter()
        .map(|row| {
            let mut row = untracked_row_to_row(row, &contract)?;
            row.global = lane.is_global() || row.global;
            Ok(row)
        })
        .collect();
    }

    let contract = load_live_read_contract_with_backend(backend, &request.schema_key).await?;
    let mut rows = scan_tracked_rows_with_backend(
        backend,
        &TrackedScanRequest {
            schema_key: request.schema_key.clone(),
            version_id: lane_version_id(&request.version_id, lane),
            constraints: request.constraints.clone(),
            required_columns: Vec::new(),
        },
    )
    .await?
    .into_iter()
    .map(|row| {
        let mut row = tracked_row_to_row(row, &contract)?;
        row.global = lane.is_global() || row.global;
        Ok(row)
    })
    .collect::<Result<Vec<_>, LixError>>()?;

    let mut executor = backend;
    let tombstones = scan_tracked_tombstones_with_executor(
        &mut executor,
        &TrackedScanRequest {
            schema_key: request.schema_key.clone(),
            version_id: lane_version_id(&request.version_id, lane),
            constraints: request.constraints.clone(),
            required_columns: Vec::new(),
        },
    )
    .await?;
    rows.extend(tombstones.into_iter().map(|tombstone| {
        let mut row = tracked_tombstone_to_row(tombstone);
        row.global = lane.is_global() || row.global;
        row
    }));

    Ok(rows)
}

fn tracked_row_to_row(
    row: super::TrackedRow,
    contract: &super::LiveReadContract,
) -> Result<Row, LixError> {
    let snapshot_content = Some(row_snapshot_text(&row.schema_key, &row.values, contract)?);
    Ok(Row {
        entity_id: row.entity_id,
        file_id: row.file_id,
        schema_key: row.schema_key,
        schema_version: row.schema_version,
        version_id: row.version_id,
        plugin_key: row.plugin_key,
        writer_key: row.writer_key,
        global: row.global,
        untracked: false,
        snapshot_content,
        values: row.values,
        tombstone: false,
    })
}

fn untracked_row_to_row(
    row: super::UntrackedRow,
    contract: &super::LiveReadContract,
) -> Result<Row, LixError> {
    let snapshot_content = Some(row_snapshot_text(&row.schema_key, &row.values, contract)?);
    Ok(Row {
        entity_id: row.entity_id,
        file_id: row.file_id,
        schema_key: row.schema_key,
        schema_version: row.schema_version,
        version_id: row.version_id,
        plugin_key: row.plugin_key,
        writer_key: row.writer_key,
        global: row.global,
        untracked: true,
        snapshot_content,
        values: row.values,
        tombstone: false,
    })
}

fn tracked_tombstone_to_row(tombstone: super::TrackedTombstoneMarker) -> Row {
    Row {
        entity_id: tombstone.entity_id,
        file_id: tombstone.file_id,
        schema_key: tombstone.schema_key,
        schema_version: tombstone.schema_version.unwrap_or_default(),
        version_id: tombstone.version_id,
        plugin_key: tombstone.plugin_key.unwrap_or_default(),
        writer_key: tombstone.writer_key,
        global: tombstone.global,
        untracked: false,
        snapshot_content: None,
        values: BTreeMap::new(),
        tombstone: true,
    }
}

fn row_snapshot_text(
    schema_key: &str,
    values: &BTreeMap<String, Value>,
    contract: &super::LiveReadContract,
) -> Result<String, LixError> {
    contract.snapshot_text_from_values(schema_key, values)
}

fn row_sort_key(left: &Row, right: &Row) -> std::cmp::Ordering {
    left.entity_id
        .cmp(&right.entity_id)
        .then_with(|| left.file_id.cmp(&right.file_id))
        .then_with(|| left.tombstone.cmp(&right.tombstone))
}

#[cfg(test)]
mod tests {
    use super::{decode_registered_schema_row, Row};
    use crate::schema::SchemaKey;
    use serde_json::Value as JsonValue;
    use std::collections::BTreeMap;

    fn registered_schema_row(snapshot_content: Option<&str>) -> Row {
        Row {
            entity_id: "users~1".to_string(),
            file_id: "users~1".to_string(),
            schema_key: "lix_registered_schema".to_string(),
            schema_version: "1".to_string(),
            version_id: "global".to_string(),
            plugin_key: "lix".to_string(),
            writer_key: None,
            global: true,
            untracked: false,
            snapshot_content: snapshot_content.map(ToString::to_string),
            values: BTreeMap::new(),
            tombstone: false,
        }
    }

    #[test]
    fn decode_registered_schema_row_extracts_key_and_schema() {
        let row = registered_schema_row(Some(
            r#"{"value":{"x-lix-key":"users","x-lix-version":"1","type":"object"}}"#,
        ));

        let decoded = decode_registered_schema_row(&row).expect("row should decode");
        let (key, schema) = decoded.expect("row should produce schema");
        assert_eq!(key, SchemaKey::new("users", "1"));
        assert_eq!(schema["type"], JsonValue::String("object".to_string()));
    }

    #[test]
    fn decode_registered_schema_row_returns_none_for_tombstones() {
        let mut row = registered_schema_row(Some(
            r#"{"value":{"x-lix-key":"users","x-lix-version":"1","type":"object"}}"#,
        ));
        row.tombstone = true;

        let decoded = decode_registered_schema_row(&row).expect("tombstone should be ignored");
        assert!(decoded.is_none());
    }
}
