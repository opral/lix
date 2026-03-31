use std::collections::BTreeMap;

use serde_json::Value as JsonValue;

use crate::{LixBackend, LixError, Value};

use super::effective::{EffectiveRow, EffectiveRowState, OverlayLane};
use super::schema_access::LiveReadContract;
use super::tracked::{ExactTrackedRowRequest, TrackedScanRequest, TrackedTombstoneMarker};
use super::untracked::{ExactUntrackedRowRequest, UntrackedRow};
use super::visible_rows::{scan_live_rows, LiveReadRow, LiveStorageLane};
use super::{
    live_storage_relation_exists_with_backend, load_exact_tracked_tombstone_with_executor,
    load_exact_untracked_row_with_executor, scan_tracked_tombstones_with_executor, ScanConstraint,
    ScanField, ScanOperator,
};
use crate::version::GLOBAL_VERSION_ID;

#[derive(Debug, Clone)]
pub(crate) struct LiveReadShape {
    contract: LiveReadContract,
}

impl LiveReadShape {
    pub(crate) fn property_names(&self) -> Vec<String> {
        self.contract
            .columns()
            .iter()
            .map(|column| column.property_name.clone())
            .collect()
    }

    pub(crate) fn normalized_projection_sql(&self, table_alias: Option<&str>) -> String {
        self.contract.normalized_projection_sql(table_alias)
    }

    pub(crate) fn normalized_values(
        &self,
        snapshot_content: Option<&str>,
    ) -> Result<BTreeMap<String, Value>, LixError> {
        self.contract.normalized_values(snapshot_content)
    }

    pub(crate) fn snapshot_from_projected_row(
        &self,
        schema_key: &str,
        row: &[Value],
        snapshot_index: usize,
        normalized_start_index: usize,
    ) -> Result<Option<JsonValue>, LixError> {
        super::schema_access::logical_snapshot_from_projected_row_with_contract(
            Some(&self.contract),
            schema_key,
            row,
            snapshot_index,
            normalized_start_index,
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LiveSnapshotStorage {
    Tracked,
    Untracked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum LiveFilterField {
    EntityId,
    FileId,
    PluginKey,
    SchemaVersion,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) enum LiveFilterOp {
    Eq(Value),
    In(Vec<Value>),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LiveFilter {
    pub(crate) field: LiveFilterField,
    pub(crate) operator: LiveFilterOp,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LiveSnapshotRow {
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) schema_version: String,
    pub(crate) file_id: String,
    pub(crate) version_id: String,
    pub(crate) plugin_key: String,
    pub(crate) metadata: Option<String>,
    pub(crate) source_change_id: Option<String>,
    pub(crate) snapshot: JsonValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExactUntrackedLookupRequest {
    pub(crate) schema_key: String,
    pub(crate) version_id: String,
    pub(crate) entity_id: String,
    pub(crate) file_id: Option<String>,
    pub(crate) plugin_key: Option<String>,
    pub(crate) schema_version: Option<String>,
    pub(crate) writer_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrackedTombstoneLookupRequest {
    pub(crate) schema_key: String,
    pub(crate) version_id: String,
    pub(crate) entity_id: String,
    pub(crate) file_id: Option<String>,
    pub(crate) plugin_key: Option<String>,
    pub(crate) schema_version: Option<String>,
}

pub(crate) async fn load_live_read_shape_with_backend(
    backend: &dyn LixBackend,
    schema_key: &str,
) -> Result<LiveReadShape, LixError> {
    super::load_live_read_contract_with_backend(backend, schema_key)
        .await
        .map(|contract| LiveReadShape { contract })
}

pub(crate) async fn load_live_read_shape_for_table_name(
    backend: &dyn LixBackend,
    table_name: &str,
) -> Result<Option<LiveReadShape>, LixError> {
    super::load_live_read_contract_for_table_name(backend, table_name)
        .await
        .map(|contract| contract.map(|contract| LiveReadShape { contract }))
}

pub(crate) async fn normalize_live_snapshot_values_with_backend(
    backend: &dyn LixBackend,
    schema_key: &str,
    snapshot_content: Option<&str>,
) -> Result<BTreeMap<String, Value>, LixError> {
    load_live_read_shape_with_backend(backend, schema_key)
        .await?
        .normalized_values(snapshot_content)
}

pub(crate) async fn load_live_snapshot_rows_with_backend(
    backend: &dyn LixBackend,
    storage: LiveSnapshotStorage,
    schema_key: &str,
    version_id: &str,
    filters: &[LiveFilter],
) -> Result<Vec<LiveSnapshotRow>, LixError> {
    if !live_storage_relation_exists_with_backend(backend, schema_key).await? {
        return Ok(Vec::new());
    }

    let shape = load_live_read_shape_with_backend(backend, schema_key).await?;
    let required_columns = shape.property_names();
    let constraints = filters
        .iter()
        .map(scan_constraint_from_filter)
        .collect::<Vec<_>>();
    let rows = scan_live_rows(
        backend,
        storage_lane(storage),
        schema_key,
        version_id,
        &constraints,
        &required_columns,
    )
    .await?;

    rows.into_iter()
        .map(|row| snapshot_row_from_live_row(&shape, row))
        .collect()
}

pub(crate) async fn load_exact_untracked_effective_row_with_backend(
    backend: &dyn LixBackend,
    request: &ExactUntrackedLookupRequest,
    requested_version_id: &str,
    overlay_lane: OverlayLane,
) -> Result<Option<EffectiveRow>, LixError> {
    let Some(file_id) = request.file_id.as_ref() else {
        return Ok(None);
    };
    let mut executor = backend;
    let row = load_exact_untracked_row_with_executor(
        &mut executor,
        &ExactUntrackedRowRequest {
            schema_key: request.schema_key.clone(),
            version_id: request.version_id.clone(),
            entity_id: request.entity_id.clone(),
            file_id: Some(file_id.clone()),
        },
    )
    .await?;

    Ok(row
        .filter(|row| untracked_row_matches_lookup(row, request))
        .map(|row| effective_row_from_untracked(row, requested_version_id, overlay_lane)))
}

pub(crate) async fn tracked_tombstone_shadows_exact_row_with_backend(
    backend: &dyn LixBackend,
    request: &TrackedTombstoneLookupRequest,
) -> Result<bool, LixError> {
    let exact_request = ExactTrackedRowRequest {
        schema_key: request.schema_key.clone(),
        version_id: request.version_id.clone(),
        entity_id: request.entity_id.clone(),
        file_id: request.file_id.clone(),
    };
    let mut executor = backend;
    if let Some(tombstone) =
        load_exact_tracked_tombstone_with_executor(&mut executor, &exact_request).await?
    {
        return Ok(tombstone_matches_lookup(&tombstone, request));
    }

    let tombstones = scan_tracked_tombstones_with_executor(
        &mut executor,
        &TrackedScanRequest {
            schema_key: request.schema_key.clone(),
            version_id: request.version_id.clone(),
            constraints: tracked_tombstone_constraints(request),
            required_columns: Vec::new(),
        },
    )
    .await?;
    Ok(tombstones
        .iter()
        .any(|tombstone| tombstone_matches_lookup(tombstone, request)))
}

fn storage_lane(storage: LiveSnapshotStorage) -> LiveStorageLane {
    match storage {
        LiveSnapshotStorage::Tracked => LiveStorageLane::Tracked,
        LiveSnapshotStorage::Untracked => LiveStorageLane::Untracked,
    }
}

fn scan_constraint_from_filter(filter: &LiveFilter) -> ScanConstraint {
    ScanConstraint {
        field: match filter.field {
            LiveFilterField::EntityId => ScanField::EntityId,
            LiveFilterField::FileId => ScanField::FileId,
            LiveFilterField::PluginKey => ScanField::PluginKey,
            LiveFilterField::SchemaVersion => ScanField::SchemaVersion,
        },
        operator: match &filter.operator {
            LiveFilterOp::Eq(value) => ScanOperator::Eq(value.clone()),
            LiveFilterOp::In(values) => ScanOperator::In(values.clone()),
        },
    }
}

fn snapshot_row_from_live_row(
    shape: &LiveReadShape,
    row: LiveReadRow,
) -> Result<LiveSnapshotRow, LixError> {
    Ok(LiveSnapshotRow {
        entity_id: row.entity_id().to_string(),
        schema_key: row.schema_key().to_string(),
        schema_version: row.schema_version().to_string(),
        file_id: row.file_id().to_string(),
        version_id: row.version_id().to_string(),
        plugin_key: row.plugin_key().to_string(),
        metadata: row.metadata().map(str::to_string),
        source_change_id: row.change_id().map(str::to_string),
        snapshot: row.snapshot_json(&shape.contract)?,
    })
}

fn tracked_tombstone_constraints(request: &TrackedTombstoneLookupRequest) -> Vec<ScanConstraint> {
    let mut constraints = vec![ScanConstraint {
        field: ScanField::EntityId,
        operator: ScanOperator::Eq(Value::Text(request.entity_id.clone())),
    }];
    if let Some(file_id) = request.file_id.as_ref() {
        constraints.push(ScanConstraint {
            field: ScanField::FileId,
            operator: ScanOperator::Eq(Value::Text(file_id.clone())),
        });
    }
    if let Some(plugin_key) = request.plugin_key.as_ref() {
        constraints.push(ScanConstraint {
            field: ScanField::PluginKey,
            operator: ScanOperator::Eq(Value::Text(plugin_key.clone())),
        });
    }
    if let Some(schema_version) = request.schema_version.as_ref() {
        constraints.push(ScanConstraint {
            field: ScanField::SchemaVersion,
            operator: ScanOperator::Eq(Value::Text(schema_version.clone())),
        });
    }
    constraints
}

fn tombstone_matches_lookup(
    row: &TrackedTombstoneMarker,
    request: &TrackedTombstoneLookupRequest,
) -> bool {
    request
        .plugin_key
        .as_deref()
        .is_none_or(|plugin_key| row.plugin_key.as_deref() == Some(plugin_key))
        && request
            .schema_version
            .as_deref()
            .is_none_or(|schema_version| row.schema_version.as_deref() == Some(schema_version))
}

fn untracked_row_matches_lookup(row: &UntrackedRow, request: &ExactUntrackedLookupRequest) -> bool {
    request
        .writer_key
        .as_deref()
        .is_none_or(|writer_key| row.writer_key.as_deref() == Some(writer_key))
        && request
            .plugin_key
            .as_deref()
            .is_none_or(|plugin_key| row.plugin_key == plugin_key)
        && request
            .schema_version
            .as_deref()
            .is_none_or(|schema_version| row.schema_version == schema_version)
}

fn effective_row_from_untracked(
    row: UntrackedRow,
    requested_version_id: &str,
    overlay_lane: OverlayLane,
) -> EffectiveRow {
    let source_version_id = row.version_id.clone();
    let version_id = projected_version_id(requested_version_id, overlay_lane, &source_version_id);
    EffectiveRow {
        entity_id: row.entity_id,
        schema_key: row.schema_key,
        schema_version: Some(row.schema_version),
        file_id: row.file_id,
        version_id,
        source_version_id,
        global: overlay_lane.is_global() || row.global,
        untracked: true,
        plugin_key: Some(row.plugin_key),
        metadata: row.metadata,
        writer_key: row.writer_key,
        created_at: Some(row.created_at),
        updated_at: Some(row.updated_at),
        source_change_id: None,
        overlay_lane,
        state: EffectiveRowState::Visible,
        values: row.values,
    }
}

fn projected_version_id(
    requested_version_id: &str,
    overlay_lane: OverlayLane,
    source_version_id: &str,
) -> String {
    if overlay_lane.is_global() && source_version_id == GLOBAL_VERSION_ID {
        requested_version_id.to_string()
    } else {
        source_version_id.to_string()
    }
}
