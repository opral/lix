use std::collections::BTreeMap;

use async_trait::async_trait;
use serde_json::Value as JsonValue;

use crate::contracts::artifacts::{
    LiveFilter, LiveFilterField, LiveFilterOp, LiveSnapshotRow, LiveSnapshotStorage,
};
use crate::contracts::traits::{LiveReadShapeContract, LiveStateQueryBackend};
use crate::{LixBackend, LixError, Value};

use super::schema_access::LiveReadContract;
use super::visible_rows::{scan_live_rows as scan_visible_live_rows, LiveReadRow, LiveStorageLane};
use super::{schema_access, ScanConstraint, ScanField, ScanOperator};

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

impl LiveReadShapeContract for LiveReadShape {
    fn normalized_projection_sql(&self, table_alias: Option<&str>) -> String {
        LiveReadShape::normalized_projection_sql(self, table_alias)
    }

    fn snapshot_from_projected_row(
        &self,
        schema_key: &str,
        row: &[Value],
        snapshot_index: usize,
        normalized_start_index: usize,
    ) -> Result<Option<JsonValue>, LixError> {
        LiveReadShape::snapshot_from_projected_row(
            self,
            schema_key,
            row,
            snapshot_index,
            normalized_start_index,
        )
    }
}

pub(crate) async fn load_live_read_shape_with_backend(
    backend: &dyn LixBackend,
    schema_key: &str,
) -> Result<LiveReadShape, LixError> {
    schema_access::load_schema_read_contract_with_backend(backend, schema_key)
        .await
        .map(|contract| LiveReadShape { contract })
}

pub(crate) async fn load_live_read_shape_for_table_name(
    backend: &dyn LixBackend,
    table_name: &str,
) -> Result<Option<LiveReadShape>, LixError> {
    schema_access::load_schema_read_contract_for_table_name(backend, table_name)
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
    if !schema_access::live_storage_relation_exists_with_backend(backend, schema_key).await? {
        return Ok(Vec::new());
    }

    let shape = load_live_read_shape_with_backend(backend, schema_key).await?;
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

#[async_trait(?Send)]
impl LiveStateQueryBackend for dyn LixBackend + '_ {
    async fn load_live_read_shape_for_table_name(
        &self,
        table_name: &str,
    ) -> Result<Option<Box<dyn LiveReadShapeContract>>, LixError> {
        load_live_read_shape_for_table_name(self, table_name)
            .await
            .map(|shape| shape.map(|shape| Box::new(shape) as Box<dyn LiveReadShapeContract>))
    }

    async fn load_live_snapshot_rows(
        &self,
        storage: LiveSnapshotStorage,
        schema_key: &str,
        version_id: &str,
        filters: &[LiveFilter],
    ) -> Result<Vec<LiveSnapshotRow>, LixError> {
        load_live_snapshot_rows_with_backend(self, storage, schema_key, version_id, filters).await
    }

    async fn normalize_live_snapshot_values(
        &self,
        schema_key: &str,
        snapshot_content: Option<&str>,
    ) -> Result<BTreeMap<String, Value>, LixError> {
        normalize_live_snapshot_values_with_backend(self, schema_key, snapshot_content).await
    }

    async fn load_live_state_projection_status(
        &self,
    ) -> Result<crate::contracts::artifacts::LiveStateProjectionStatus, LixError> {
        super::projection::status::load_live_state_projection_status_with_backend(self).await
    }
}
