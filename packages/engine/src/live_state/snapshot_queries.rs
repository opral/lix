use std::collections::BTreeMap;

use async_trait::async_trait;
use serde_json::Value as JsonValue;

use crate::live_state::store::LiveStateBackendRef;
use crate::live_state::{
    LiveFilter, LiveFilterField, LiveFilterOp, LiveSnapshotRow, LiveSnapshotStorage,
};
use crate::{LixError, Value};

use super::schema_access::LiveRowShape;
use super::visible_rows::{scan_live_rows as scan_visible_live_rows, LiveReadRow, LiveStorageLane};
use super::{schema_access, ScanConstraint, ScanField, ScanOperator};

pub(crate) trait LiveRowShapeContract {
    fn normalized_projection_sql(&self, table_alias: Option<&str>) -> String;

    fn snapshot_from_projected_row(
        &self,
        schema_key: &str,
        row: &[Value],
        snapshot_index: usize,
        normalized_start_index: usize,
    ) -> Result<Option<JsonValue>, LixError>;
}

#[async_trait(?Send)]
pub(crate) trait LiveStateQueryBackend {
    async fn load_live_read_shape_for_table_name(
        &self,
        table_name: &str,
    ) -> Result<Option<Box<dyn LiveRowShapeContract>>, LixError>;

    async fn load_live_snapshot_rows(
        &self,
        storage: LiveSnapshotStorage,
        schema_key: &str,
        version_id: &str,
        filters: &[LiveFilter],
    ) -> Result<Vec<LiveSnapshotRow>, LixError>;

    async fn normalize_live_snapshot_values(
        &self,
        schema_key: &str,
        version_id: &str,
        snapshot_content: Option<&str>,
    ) -> Result<BTreeMap<String, Value>, LixError>;
}

#[derive(Debug, Clone)]
pub(crate) struct LiveRowShapeAdapter {
    contract: LiveRowShape,
}

impl LiveRowShapeAdapter {
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
        super::schema_access::logical_snapshot_from_projected_row_with_shape(
            Some(&self.contract),
            schema_key,
            row,
            snapshot_index,
            normalized_start_index,
        )
    }
}

impl LiveRowShapeContract for LiveRowShapeAdapter {
    fn normalized_projection_sql(&self, table_alias: Option<&str>) -> String {
        LiveRowShapeAdapter::normalized_projection_sql(self, table_alias)
    }

    fn snapshot_from_projected_row(
        &self,
        schema_key: &str,
        row: &[Value],
        snapshot_index: usize,
        normalized_start_index: usize,
    ) -> Result<Option<JsonValue>, LixError> {
        LiveRowShapeAdapter::snapshot_from_projected_row(
            self,
            schema_key,
            row,
            snapshot_index,
            normalized_start_index,
        )
    }
}

pub(crate) async fn load_live_read_shape_for_version_with_backend(
    backend: LiveStateBackendRef<'_>,
    schema_key: &str,
    version_id: &str,
) -> Result<LiveRowShapeAdapter, LixError> {
    schema_access::load_live_row_shape_for_version_with_backend(backend, schema_key, version_id)
        .await
        .map(|shape| LiveRowShapeAdapter { contract: shape })
}

pub(crate) async fn load_live_read_shape_for_table_name(
    backend: LiveStateBackendRef<'_>,
    table_name: &str,
) -> Result<Option<LiveRowShapeAdapter>, LixError> {
    schema_access::load_live_row_shape_for_table_name(backend, table_name)
        .await
        .map(|shape| shape.map(|shape| LiveRowShapeAdapter { contract: shape }))
}

pub(crate) async fn normalize_live_snapshot_values_for_version_with_backend(
    backend: LiveStateBackendRef<'_>,
    schema_key: &str,
    version_id: &str,
    snapshot_content: Option<&str>,
) -> Result<BTreeMap<String, Value>, LixError> {
    load_live_read_shape_for_version_with_backend(backend, schema_key, version_id)
        .await?
        .normalized_values(snapshot_content)
}

pub(crate) async fn load_live_snapshot_rows_with_backend(
    backend: LiveStateBackendRef<'_>,
    storage: LiveSnapshotStorage,
    schema_key: &str,
    version_id: &str,
    filters: &[LiveFilter],
) -> Result<Vec<LiveSnapshotRow>, LixError> {
    if !schema_access::live_storage_relation_exists_with_backend(backend, schema_key).await? {
        return Ok(Vec::new());
    }

    let shape = load_live_read_shape_for_version_with_backend(backend, schema_key, version_id).await?;
    let required_columns = shape.property_names();
    let constraints = filters
        .iter()
        .map(scan_constraint_from_filter)
        .collect::<Vec<_>>();
    let rows = scan_visible_live_rows(
        backend,
        storage_lane(storage),
        schema_key,
        version_id,
        &constraints,
        &required_columns,
        None,
    )
    .await?;

    rows.into_iter()
        .map(|row| snapshot_row_from_live_row(&shape, row))
        .collect()
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
    shape: &LiveRowShapeAdapter,
    row: LiveReadRow,
) -> Result<LiveSnapshotRow, LixError> {
    Ok(LiveSnapshotRow {
        entity_id: row.entity_id().to_string(),
        schema_key: row.schema_key().to_string(),
        schema_version: row.schema_version().to_string(),
        file_id: row.file_id().map(str::to_string),
        version_id: row.version_id().to_string(),
        plugin_key: row.plugin_key().map(str::to_string),
        metadata: row.metadata().map(str::to_string),
        source_change_id: row.change_id().map(str::to_string),
        snapshot: row.snapshot_json(&shape.contract)?,
    })
}
