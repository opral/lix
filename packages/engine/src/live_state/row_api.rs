use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value as JsonValue;

use crate::{LixBackend, LixBackendTransaction, LixError, Value};

use super::constraints::ScanConstraint;
use super::tracked::{ExactTrackedRowRequest, TrackedScanRequest};
use super::tracked::{TrackedWriteOperation, TrackedWriteRow};
use super::untracked::{ExactUntrackedRowRequest, UntrackedScanRequest};
use super::untracked::{UntrackedWriteOperation, UntrackedWriteRow};
use super::{
    apply_tracked_write_batch_in_transaction, apply_untracked_write_batch_in_transaction,
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
pub struct LiveRowQuery {
    pub schema_key: String,
    pub version_id: String,
    pub mode: RowReadMode,
    #[serde(default)]
    pub constraints: Vec<ScanConstraint>,
    #[serde(default)]
    pub include_tombstones: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ExactLiveRowQuery {
    pub schema_key: String,
    pub version_id: String,
    pub entity_id: String,
    pub file_id: String,
    pub mode: RowReadMode,
    #[serde(default)]
    pub include_tombstones: bool,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LiveRow {
    pub entity_id: String,
    pub file_id: String,
    pub schema_key: String,
    pub schema_version: String,
    pub version_id: String,
    pub plugin_key: String,
    pub metadata: Option<String>,
    pub change_id: Option<String>,
    pub writer_key: Option<String>,
    pub global: bool,
    pub untracked: bool,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
    pub snapshot_content: Option<String>,
}

pub fn decode_registered_schema_row(
    row: &LiveRow,
) -> Result<Option<(SchemaKey, JsonValue)>, LixError> {
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

pub async fn scan_live_rows(
    backend: &dyn LixBackend,
    request: &LiveRowQuery,
) -> Result<Vec<LiveRow>, LixError> {
    match request.mode {
        RowReadMode::Tracked => scan_tracked_rows(backend, request).await,
        RowReadMode::Untracked => scan_untracked_rows(backend, request).await,
        RowReadMode::Effective => scan_effective_rows(backend, request).await,
    }
}

pub async fn load_exact_live_row(
    backend: &dyn LixBackend,
    request: &ExactLiveRowQuery,
) -> Result<Option<LiveRow>, LixError> {
    match request.mode {
        RowReadMode::Tracked => load_exact_tracked_row(backend, request).await,
        RowReadMode::Untracked => load_exact_untracked_row(backend, request).await,
        RowReadMode::Effective => load_exact_effective_row(backend, request).await,
    }
}

pub async fn write_live_rows(
    transaction: &mut dyn LixBackendTransaction,
    rows: &[LiveRow],
) -> Result<(), LixError> {
    let (tracked, untracked) = partition_live_rows_for_write(rows)?;

    if !tracked.is_empty() {
        apply_tracked_write_batch_in_transaction(transaction, &tracked).await?;
    }
    if !untracked.is_empty() {
        apply_untracked_write_batch_in_transaction(transaction, &untracked).await?;
    }

    Ok(())
}

fn partition_live_rows_for_write(
    rows: &[LiveRow],
) -> Result<(Vec<TrackedWriteRow>, Vec<UntrackedWriteRow>), LixError> {
    let mut tracked = Vec::<TrackedWriteRow>::new();
    let mut untracked = Vec::<UntrackedWriteRow>::new();

    for row in rows {
        if row.untracked {
            untracked.push(untracked_write_from_live_row(row)?);
        } else {
            tracked.push(tracked_write_from_live_row(row)?);
        }
    }

    Ok((tracked, untracked))
}

async fn scan_tracked_rows(
    backend: &dyn LixBackend,
    request: &LiveRowQuery,
) -> Result<Vec<LiveRow>, LixError> {
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
    request: &LiveRowQuery,
) -> Result<Vec<LiveRow>, LixError> {
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
    request: &ExactLiveRowQuery,
) -> Result<Option<LiveRow>, LixError> {
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
    request: &ExactLiveRowQuery,
) -> Result<Option<LiveRow>, LixError> {
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
    request: &LiveRowQuery,
) -> Result<Vec<LiveRow>, LixError> {
    let mut resolved = BTreeMap::<(String, String), LiveRow>::new();
    let mut hidden = BTreeSet::<(String, String)>::new();
    let lanes = effective_lanes(&request.version_id);

    for lane in lanes {
        for row in scan_lane_rows(backend, request, lane).await? {
            let key = (row.entity_id.clone(), row.file_id.clone());
            if resolved.contains_key(&key) || hidden.contains(&key) {
                continue;
            }

            if row.snapshot_content.is_none() {
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
    request: &ExactLiveRowQuery,
) -> Result<Option<LiveRow>, LixError> {
    let query = LiveRowQuery {
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
    request: &LiveRowQuery,
    lane: EffectiveLane,
) -> Result<Vec<LiveRow>, LixError> {
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
) -> Result<LiveRow, LixError> {
    let snapshot_content = Some(row_snapshot_text(&row.schema_key, &row.values, contract)?);
    Ok(LiveRow {
        entity_id: row.entity_id,
        file_id: row.file_id,
        schema_key: row.schema_key,
        schema_version: row.schema_version,
        version_id: row.version_id,
        plugin_key: row.plugin_key,
        metadata: row.metadata,
        change_id: row.change_id,
        writer_key: row.writer_key,
        global: row.global,
        untracked: false,
        created_at: Some(row.created_at),
        updated_at: Some(row.updated_at),
        snapshot_content,
    })
}

fn untracked_row_to_row(
    row: super::UntrackedRow,
    contract: &super::LiveReadContract,
) -> Result<LiveRow, LixError> {
    let snapshot_content = Some(row_snapshot_text(&row.schema_key, &row.values, contract)?);
    Ok(LiveRow {
        entity_id: row.entity_id,
        file_id: row.file_id,
        schema_key: row.schema_key,
        schema_version: row.schema_version,
        version_id: row.version_id,
        plugin_key: row.plugin_key,
        metadata: row.metadata,
        change_id: None,
        writer_key: row.writer_key,
        global: row.global,
        untracked: true,
        created_at: Some(row.created_at),
        updated_at: Some(row.updated_at),
        snapshot_content,
    })
}

fn tracked_tombstone_to_row(tombstone: super::TrackedTombstoneMarker) -> LiveRow {
    LiveRow {
        entity_id: tombstone.entity_id,
        file_id: tombstone.file_id,
        schema_key: tombstone.schema_key,
        schema_version: tombstone.schema_version.unwrap_or_default(),
        version_id: tombstone.version_id,
        plugin_key: tombstone.plugin_key.unwrap_or_default(),
        metadata: tombstone.metadata,
        change_id: tombstone.change_id,
        writer_key: tombstone.writer_key,
        global: tombstone.global,
        untracked: false,
        created_at: tombstone.created_at,
        updated_at: tombstone.updated_at,
        snapshot_content: None,
    }
}

fn row_snapshot_text(
    schema_key: &str,
    values: &BTreeMap<String, Value>,
    contract: &super::LiveReadContract,
) -> Result<String, LixError> {
    contract.snapshot_text_from_values(schema_key, values)
}

fn row_sort_key(left: &LiveRow, right: &LiveRow) -> std::cmp::Ordering {
    left.entity_id
        .cmp(&right.entity_id)
        .then_with(|| left.file_id.cmp(&right.file_id))
        .then_with(|| {
            left.snapshot_content
                .is_none()
                .cmp(&right.snapshot_content.is_none())
        })
}

fn tracked_write_from_live_row(row: &LiveRow) -> Result<TrackedWriteRow, LixError> {
    let updated_at = row.updated_at.clone().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "tracked live_state write for '{}:{}' requires updated_at",
                row.schema_key, row.entity_id
            ),
        )
    })?;
    let change_id = row.change_id.clone().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "tracked live_state write for '{}:{}' requires change_id",
                row.schema_key, row.entity_id
            ),
        )
    })?;

    Ok(TrackedWriteRow {
        entity_id: row.entity_id.clone(),
        schema_key: row.schema_key.clone(),
        schema_version: row.schema_version.clone(),
        file_id: row.file_id.clone(),
        version_id: row.version_id.clone(),
        global: row.global,
        plugin_key: row.plugin_key.clone(),
        metadata: row.metadata.clone(),
        change_id,
        writer_key: row.writer_key.clone(),
        snapshot_content: row.snapshot_content.clone(),
        created_at: row.created_at.clone(),
        updated_at,
        operation: if row.snapshot_content.is_some() {
            TrackedWriteOperation::Upsert
        } else {
            TrackedWriteOperation::Tombstone
        },
    })
}

fn untracked_write_from_live_row(row: &LiveRow) -> Result<UntrackedWriteRow, LixError> {
    let updated_at = row.updated_at.clone().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "untracked live_state write for '{}:{}' requires updated_at",
                row.schema_key, row.entity_id
            ),
        )
    })?;

    Ok(UntrackedWriteRow {
        entity_id: row.entity_id.clone(),
        schema_key: row.schema_key.clone(),
        schema_version: row.schema_version.clone(),
        file_id: row.file_id.clone(),
        version_id: row.version_id.clone(),
        global: row.global,
        plugin_key: row.plugin_key.clone(),
        metadata: row.metadata.clone(),
        writer_key: row.writer_key.clone(),
        snapshot_content: row.snapshot_content.clone(),
        created_at: row.created_at.clone(),
        updated_at,
        operation: if row.snapshot_content.is_some() {
            UntrackedWriteOperation::Upsert
        } else {
            UntrackedWriteOperation::Delete
        },
    })
}

#[cfg(test)]
mod tests {
    use super::{
        decode_registered_schema_row, partition_live_rows_for_write, tracked_write_from_live_row,
        untracked_write_from_live_row, LiveRow,
    };
    use crate::live_state::{TrackedWriteOperation, UntrackedWriteOperation};
    use crate::schema::SchemaKey;
    use serde_json::Value as JsonValue;

    fn registered_schema_row(snapshot_content: Option<&str>) -> LiveRow {
        LiveRow {
            entity_id: "users~1".to_string(),
            file_id: "users~1".to_string(),
            schema_key: "lix_registered_schema".to_string(),
            schema_version: "1".to_string(),
            version_id: "global".to_string(),
            plugin_key: "lix".to_string(),
            metadata: None,
            change_id: None,
            writer_key: None,
            global: true,
            untracked: false,
            created_at: None,
            updated_at: None,
            snapshot_content: snapshot_content.map(ToString::to_string),
        }
    }

    fn writable_live_row(untracked: bool, snapshot_content: Option<&str>) -> LiveRow {
        LiveRow {
            entity_id: "settings".to_string(),
            file_id: "state".to_string(),
            schema_key: "lix_key_value".to_string(),
            schema_version: "1".to_string(),
            version_id: "main".to_string(),
            plugin_key: "lix".to_string(),
            metadata: Some("{\"kind\":\"state\"}".to_string()),
            change_id: Some("chg_123".to_string()),
            writer_key: Some("writer-a".to_string()),
            global: false,
            untracked,
            created_at: Some("2026-01-01T00:00:00Z".to_string()),
            updated_at: Some("2026-01-01T00:00:00Z".to_string()),
            snapshot_content: snapshot_content.map(ToString::to_string),
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
        let row = registered_schema_row(None);

        let decoded = decode_registered_schema_row(&row).expect("tombstone should be ignored");
        assert!(decoded.is_none());
    }

    #[test]
    fn tracked_write_uses_snapshot_none_as_tombstone() {
        let row = writable_live_row(false, None);

        let write = tracked_write_from_live_row(&row).expect("tracked write should build");

        assert_eq!(write.snapshot_content, None);
        assert_eq!(write.operation, TrackedWriteOperation::Tombstone);
    }

    #[test]
    fn tracked_write_uses_snapshot_some_as_upsert() {
        let row = writable_live_row(false, Some(r#"{"key":"theme","value":"dark"}"#));

        let write = tracked_write_from_live_row(&row).expect("tracked write should build");

        assert_eq!(
            write.snapshot_content,
            Some(r#"{"key":"theme","value":"dark"}"#.to_string())
        );
        assert_eq!(write.operation, TrackedWriteOperation::Upsert);
    }

    #[test]
    fn untracked_write_uses_snapshot_none_as_delete() {
        let row = writable_live_row(true, None);

        let write = untracked_write_from_live_row(&row).expect("untracked write should build");

        assert_eq!(write.snapshot_content, None);
        assert_eq!(write.operation, UntrackedWriteOperation::Delete);
    }

    #[test]
    fn untracked_write_uses_snapshot_some_as_upsert() {
        let row = writable_live_row(true, Some(r#"{"key":"theme","value":"dark"}"#));

        let write = untracked_write_from_live_row(&row).expect("untracked write should build");

        assert_eq!(
            write.snapshot_content,
            Some(r#"{"key":"theme","value":"dark"}"#.to_string())
        );
        assert_eq!(write.operation, UntrackedWriteOperation::Upsert);
    }

    #[test]
    fn partition_live_rows_for_write_fans_out_by_untracked_flag() {
        let tracked = writable_live_row(false, Some(r#"{"key":"theme","value":"dark"}"#));
        let untracked = writable_live_row(true, None);

        let (tracked_writes, untracked_writes) =
            partition_live_rows_for_write(&[tracked, untracked]).expect("partition should build");

        assert_eq!(tracked_writes.len(), 1);
        assert_eq!(untracked_writes.len(), 1);
        assert_eq!(tracked_writes[0].operation, TrackedWriteOperation::Upsert);
        assert_eq!(
            untracked_writes[0].operation,
            UntrackedWriteOperation::Delete
        );
    }
}
