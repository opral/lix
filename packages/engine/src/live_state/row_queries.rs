use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value as JsonValue;

use crate::canonical::{
    load_change, load_visible_state, CanonicalContentMode, CanonicalTombstoneMode,
    CanonicalVisibility, CanonicalVisibleStateFilter, CanonicalVisibleStateRequest,
    CanonicalVisibleStateRow,
};
use crate::live_state::commit_derived::{is_lazy_commit_derived_surface, scan_commit_derived_rows};
use crate::live_state::store::{
    LiveStateBackendRef, LiveStateExecutorRef, LiveStateTransactionRef,
};
use crate::{LixError, NullableKeyFilter, Value};

use super::constraints::ScanConstraint;
use super::schema_access::load_live_row_shape_for_version_with_backend;
use super::tracked::{
    apply_write_batch_in_transaction as apply_tracked_write_batch_in_transaction,
    ExactTrackedRowRequest, TrackedScanRequest,
};
use super::untracked::{
    apply_write_batch_in_transaction as apply_untracked_write_batch_in_transaction,
    load_exact_row_with_backend as load_exact_untracked_row_with_backend,
    scan_rows_with_backend as scan_untracked_rows_with_backend, ExactUntrackedRowRequest,
    UntrackedScanRequest,
};
use super::{
    load_exact_tracked_row_with_backend, load_exact_tracked_tombstone_with_executor,
    load_exact_untracked_row_with_executor, scan_tracked_rows_with_backend,
    scan_tracked_tombstones_with_executor, LiveWriteOperation, LiveWriteRow,
};
use crate::schema::{schema_key_from_definition, SchemaKey};
use crate::version::GLOBAL_VERSION_ID;
use crate::version::{version_ref_schema_key, version_ref_storage_version_id};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum LiveRowSource {
    Tracked,
    Untracked,
    Effective,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LiveRowQuery {
    pub schema_key: String,
    pub version_id: String,
    #[serde(alias = "mode")]
    pub source: LiveRowSource,
    #[serde(default)]
    pub constraints: Vec<ScanConstraint>,
    #[serde(default)]
    pub include_tombstones: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ExactLiveRowQuery {
    #[serde(alias = "semantics")]
    pub source: LiveRowSource,
    pub schema_key: String,
    pub version_id: String,
    pub entity_id: String,
    #[serde(default)]
    pub file_id: NullableKeyFilter<String>,
    #[serde(default)]
    pub schema_version: Option<String>,
    #[serde(default)]
    pub plugin_key: NullableKeyFilter<String>,
    #[serde(default)]
    pub global: Option<bool>,
    #[serde(default)]
    pub untracked: Option<bool>,
    #[serde(default)]
    pub include_tombstones: bool,
    #[serde(default = "default_true")]
    pub include_global_overlay: bool,
    #[serde(default = "default_true")]
    pub include_untracked_overlay: bool,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LiveRow {
    pub entity_id: String,
    pub file_id: Option<String>,
    pub schema_key: String,
    pub schema_version: String,
    pub version_id: String,
    pub plugin_key: Option<String>,
    pub metadata: Option<String>,
    pub change_id: Option<String>,
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

pub(crate) async fn scan_live_rows(
    backend: LiveStateBackendRef<'_>,
    request: &LiveRowQuery,
) -> Result<Vec<LiveRow>, LixError> {
    if is_lazy_commit_derived_surface(&request.schema_key) {
        return scan_lazy_commit_derived_rows(backend, request).await;
    }

    match request.source {
        LiveRowSource::Tracked => scan_tracked_rows(backend, request).await,
        LiveRowSource::Untracked => scan_untracked_rows(backend, request).await,
        LiveRowSource::Effective => scan_effective_rows(backend, request).await,
    }
}

pub(crate) async fn load_exact_live_row(
    backend: LiveStateBackendRef<'_>,
    request: &ExactLiveRowQuery,
) -> Result<Option<LiveRow>, LixError> {
    if is_lazy_commit_derived_surface(&request.schema_key) {
        return load_exact_lazy_commit_derived_row(backend, request).await;
    }

    match request.source {
        LiveRowSource::Tracked => load_exact_tracked_row(backend, request).await,
        LiveRowSource::Untracked => load_exact_untracked_row(backend, request).await,
        LiveRowSource::Effective => load_exact_effective_row(backend, request).await,
    }
}

async fn scan_lazy_commit_derived_rows(
    backend: LiveStateBackendRef<'_>,
    request: &LiveRowQuery,
) -> Result<Vec<LiveRow>, LixError> {
    scan_commit_derived_rows(backend, request, |backend, request| {
        let request = request.clone();
        Box::pin(async move { scan_live_rows(backend, &request).await })
    })
    .await
}

async fn load_exact_lazy_commit_derived_row(
    backend: LiveStateBackendRef<'_>,
    request: &ExactLiveRowQuery,
) -> Result<Option<LiveRow>, LixError> {
    let mut scan_request = LiveRowQuery {
        schema_key: request.schema_key.clone(),
        version_id: request.version_id.clone(),
        source: request.source,
        constraints: vec![ScanConstraint {
            field: super::ScanField::EntityId,
            operator: super::ScanOperator::Eq(Value::Text(request.entity_id.clone())),
        }],
        include_tombstones: request.include_tombstones,
    };
    if let NullableKeyFilter::Value(file_id) = &request.file_id {
        scan_request.constraints.push(ScanConstraint {
            field: super::ScanField::FileId,
            operator: super::ScanOperator::Eq(Value::Text(file_id.clone())),
        });
    }
    if let Some(schema_version) = &request.schema_version {
        scan_request.constraints.push(ScanConstraint {
            field: super::ScanField::SchemaVersion,
            operator: super::ScanOperator::Eq(Value::Text(schema_version.clone())),
        });
    }
    if let NullableKeyFilter::Value(plugin_key) = &request.plugin_key {
        scan_request.constraints.push(ScanConstraint {
            field: super::ScanField::PluginKey,
            operator: super::ScanOperator::Eq(Value::Text(plugin_key.clone())),
        });
    }

    scan_lazy_commit_derived_rows(backend, &scan_request)
        .await
        .map(|rows| {
            rows.into_iter()
                .find(|row| exact_live_row_matches_query(row, request))
        })
}

pub(crate) async fn write_live_rows(
    transaction: LiveStateTransactionRef<'_>,
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
) -> Result<(Vec<LiveWriteRow>, Vec<LiveWriteRow>), LixError> {
    let mut tracked = Vec::<LiveWriteRow>::new();
    let mut untracked = Vec::<LiveWriteRow>::new();

    for row in rows {
        if row.untracked {
            untracked.push(live_write_from_live_row(row)?);
        } else {
            tracked.push(live_write_from_live_row(row)?);
        }
    }

    Ok((tracked, untracked))
}

async fn scan_tracked_rows(
    backend: LiveStateBackendRef<'_>,
    request: &LiveRowQuery,
) -> Result<Vec<LiveRow>, LixError> {
    let contract =
        load_live_row_shape_for_version_with_backend(backend, &request.schema_key, &request.version_id)
            .await?;
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
    backend: LiveStateBackendRef<'_>,
    request: &LiveRowQuery,
) -> Result<Vec<LiveRow>, LixError> {
    let contract =
        load_live_row_shape_for_version_with_backend(backend, &request.schema_key, &request.version_id)
            .await?;
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
    backend: LiveStateBackendRef<'_>,
    request: &ExactLiveRowQuery,
) -> Result<Option<LiveRow>, LixError> {
    let contract =
        load_live_row_shape_for_version_with_backend(backend, &request.schema_key, &request.version_id)
            .await?;
    if let Some(row) = load_exact_tracked_row_with_backend(
        backend,
        &ExactTrackedRowRequest {
            schema_key: request.schema_key.clone(),
            version_id: request.version_id.clone(),
            entity_id: request.entity_id.clone(),
            file_id: request.file_id.clone(),
        },
    )
    .await?
    {
        let row = tracked_row_to_row(row, &contract)?;
        return Ok(exact_live_row_matches_query(&row, request).then_some(row));
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
            file_id: request.file_id.clone(),
        },
    )
    .await?;
    if let Some(tombstone) = tombstone {
        let row = tracked_tombstone_to_row(tombstone);
        return Ok(exact_live_row_matches_query(&row, request).then_some(row));
    }

    Ok(None)
}

async fn load_exact_untracked_row(
    backend: LiveStateBackendRef<'_>,
    request: &ExactLiveRowQuery,
) -> Result<Option<LiveRow>, LixError> {
    let contract =
        load_live_row_shape_for_version_with_backend(backend, &request.schema_key, &request.version_id)
            .await?;
    let row = load_exact_untracked_row_with_backend(
        backend,
        &ExactUntrackedRowRequest {
            schema_key: request.schema_key.clone(),
            version_id: request.version_id.clone(),
            entity_id: request.entity_id.clone(),
            file_id: request.file_id.clone(),
        },
    )
    .await?;
    let row = row
        .map(|row| untracked_row_to_row(row, &contract))
        .transpose()?;
    Ok(row.filter(|row| exact_live_row_matches_query(row, request)))
}

async fn scan_effective_rows(
    backend: LiveStateBackendRef<'_>,
    request: &LiveRowQuery,
) -> Result<Vec<LiveRow>, LixError> {
    scan_effective_rows_with_options(backend, request, true, true).await
}

async fn scan_effective_rows_with_options(
    backend: LiveStateBackendRef<'_>,
    request: &LiveRowQuery,
    include_global_overlay: bool,
    include_untracked_overlay: bool,
) -> Result<Vec<LiveRow>, LixError> {
    let mut resolved = BTreeMap::<(String, Option<String>), LiveRow>::new();
    let mut hidden = BTreeSet::<(String, Option<String>)>::new();
    let lanes = effective_lanes(
        &request.version_id,
        include_global_overlay,
        include_untracked_overlay,
    );

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
    backend: LiveStateBackendRef<'_>,
    request: &ExactLiveRowQuery,
) -> Result<Option<LiveRow>, LixError> {
    let live_state_is_ready =
        super::projection::status::load_live_state_projection_status_with_backend(backend)
            .await?
            .mode
            == super::LiveStateMode::Ready;
    if !live_state_is_ready {
        return load_exact_effective_row_with_canonical_fallback(backend, request).await;
    }

    let mut query = LiveRowQuery {
        schema_key: request.schema_key.clone(),
        version_id: request.version_id.clone(),
        source: LiveRowSource::Effective,
        constraints: vec![ScanConstraint {
            field: super::ScanField::EntityId,
            operator: super::ScanOperator::Eq(Value::Text(request.entity_id.clone())),
        }],
        include_tombstones: request.include_tombstones,
    };
    if let NullableKeyFilter::Value(file_id) = &request.file_id {
        query.constraints.push(ScanConstraint {
            field: super::ScanField::FileId,
            operator: super::ScanOperator::Eq(Value::Text(file_id.clone())),
        });
    }
    scan_effective_rows_with_options(
        backend,
        &query,
        request.include_global_overlay,
        request.include_untracked_overlay,
    )
    .await
    .map(|rows| {
        rows.into_iter()
            .find(|row| exact_live_row_matches_query(row, request))
    })
}

enum EffectiveLaneOutcome {
    Missing,
    Visible(LiveRow),
    Tombstone(LiveRow),
}

async fn load_exact_effective_row_with_canonical_fallback(
    backend: LiveStateBackendRef<'_>,
    request: &ExactLiveRowQuery,
) -> Result<Option<LiveRow>, LixError> {
    let lanes = effective_lanes(
        &request.version_id,
        request.include_global_overlay,
        request.include_untracked_overlay,
    );

    for lane in lanes {
        let outcome = if lane.is_untracked() {
            load_exact_untracked_row_for_lane(backend, request, lane).await?
        } else {
            load_exact_tracked_row_from_canonical_for_lane(backend, request, lane).await?
        };

        match outcome {
            EffectiveLaneOutcome::Missing => continue,
            EffectiveLaneOutcome::Visible(row) => {
                return Ok(exact_live_row_matches_query(&row, request).then_some(row));
            }
            EffectiveLaneOutcome::Tombstone(row) => {
                if request.include_tombstones && exact_live_row_matches_query(&row, request) {
                    return Ok(Some(row));
                }
                return Ok(None);
            }
        }
    }

    Ok(None)
}

async fn load_exact_untracked_row_for_lane(
    backend: LiveStateBackendRef<'_>,
    request: &ExactLiveRowQuery,
    lane: EffectiveLane,
) -> Result<EffectiveLaneOutcome, LixError> {
    let contract =
        load_live_row_shape_for_version_with_backend(backend, &request.schema_key, &request.version_id)
            .await?;
    let row = load_exact_untracked_row_with_backend(
        backend,
        &ExactUntrackedRowRequest {
            schema_key: request.schema_key.clone(),
            version_id: lane_version_id(&request.version_id, lane),
            entity_id: request.entity_id.clone(),
            file_id: request.file_id.clone(),
        },
    )
    .await?;

    let Some(row) = row else {
        return Ok(EffectiveLaneOutcome::Missing);
    };

    let mut row = untracked_row_to_row(row, &contract)?;
    row.global = lane.is_global() || row.global;
    Ok(EffectiveLaneOutcome::Visible(row))
}

async fn load_exact_tracked_row_from_canonical_for_lane(
    backend: LiveStateBackendRef<'_>,
    request: &ExactLiveRowQuery,
    lane: EffectiveLane,
) -> Result<EffectiveLaneOutcome, LixError> {
    let storage_version_id = lane_version_id(&request.version_id, lane);
    let mut executor = backend;
    let Some(head_commit_id) =
        load_version_head_commit_id_from_live_row(&mut executor, &storage_version_id).await?
    else {
        return Ok(EffectiveLaneOutcome::Missing);
    };

    let mut filter = CanonicalVisibleStateFilter::default();
    filter.schema_keys.insert(request.schema_key.clone());
    filter.entity_ids.insert(request.entity_id.clone());
    if let NullableKeyFilter::Value(file_id) = &request.file_id {
        filter.file_ids.insert(file_id.clone());
    }
    if let NullableKeyFilter::Value(plugin_key) = &request.plugin_key {
        filter.plugin_keys.insert(plugin_key.clone());
    }

    let rows = load_visible_state(
        &mut executor,
        &CanonicalVisibleStateRequest {
            commit_ids: vec![head_commit_id],
            filter,
            content_mode: CanonicalContentMode::IncludeSnapshotContent,
            tombstones: if request.include_tombstones {
                CanonicalTombstoneMode::IncludeTombstones
            } else {
                CanonicalTombstoneMode::ExcludeTombstones
            },
        },
    )
    .await?;
    let Some(row) = rows
        .into_iter()
        .find(|row| canonical_visible_state_row_matches_query(row, request))
    else {
        return Ok(EffectiveLaneOutcome::Missing);
    };

    canonical_effective_lane_outcome_from_visible_row(backend, storage_version_id, lane, row).await
}

async fn load_version_head_commit_id_from_live_row(
    executor: LiveStateExecutorRef<'_>,
    version_id: &str,
) -> Result<Option<String>, LixError> {
    let Some(row) = load_exact_untracked_row_with_executor(
        executor,
        &ExactUntrackedRowRequest {
            schema_key: version_ref_schema_key().to_string(),
            version_id: version_ref_storage_version_id().to_string(),
            entity_id: version_id.to_string(),
            file_id: NullableKeyFilter::Null,
        },
    )
    .await?
    else {
        return Ok(None);
    };

    let Some(commit_id) = row
        .property_text("commit_id")
        .filter(|value| !value.trim().is_empty())
    else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("local version head for '{version_id}' has empty commit_id"),
        ));
    };

    Ok(Some(commit_id))
}

fn canonical_visible_state_row_matches_query(
    row: &CanonicalVisibleStateRow,
    request: &ExactLiveRowQuery,
) -> bool {
    row.entity_id == request.entity_id
        && row.schema_key == request.schema_key
        && request.file_id.matches(row.file_id.as_ref())
        && request
            .schema_version
            .as_ref()
            .is_none_or(|schema_version| row.schema_version == *schema_version)
        && request.plugin_key.matches(row.plugin_key.as_ref())
}

async fn canonical_effective_lane_outcome_from_visible_row(
    backend: LiveStateBackendRef<'_>,
    storage_version_id: String,
    lane: EffectiveLane,
    row: CanonicalVisibleStateRow,
) -> Result<EffectiveLaneOutcome, LixError> {
    let visibility = row.visibility;
    let mut executor = backend;
    let change = load_change(&mut executor, &row.source_change_id)
        .await?
        .ok_or_else(|| {
            LixError::unknown(format!(
                "canonical visible-state row references missing change '{}'",
                row.source_change_id
            ))
        })?;
    let row = LiveRow {
        entity_id: row.entity_id,
        file_id: row.file_id,
        schema_key: row.schema_key,
        schema_version: row.schema_version,
        version_id: storage_version_id,
        plugin_key: row.plugin_key,
        metadata: row.metadata,
        change_id: Some(change.id),
        global: lane.is_global(),
        untracked: false,
        created_at: Some(change.created_at.clone()),
        updated_at: Some(change.created_at),
        snapshot_content: row.snapshot_content,
    };

    match visibility {
        CanonicalVisibility::Visible => Ok(EffectiveLaneOutcome::Visible(row)),
        CanonicalVisibility::Tombstone => Ok(EffectiveLaneOutcome::Tombstone(row)),
    }
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

fn effective_lanes(
    version_id: &str,
    include_global_overlay: bool,
    include_untracked_overlay: bool,
) -> Vec<EffectiveLane> {
    let mut lanes = vec![EffectiveLane::LocalTracked];
    if include_untracked_overlay {
        lanes.insert(0, EffectiveLane::LocalUntracked);
    }
    if include_global_overlay && version_id != GLOBAL_VERSION_ID {
        if include_untracked_overlay {
            lanes.push(EffectiveLane::GlobalUntracked);
        }
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
    backend: LiveStateBackendRef<'_>,
    request: &LiveRowQuery,
    lane: EffectiveLane,
) -> Result<Vec<LiveRow>, LixError> {
    if lane.is_untracked() {
        let contract = load_live_row_shape_for_version_with_backend(
            backend,
            &request.schema_key,
            &request.version_id,
        )
        .await?;
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

    let contract =
        load_live_row_shape_for_version_with_backend(backend, &request.schema_key, &request.version_id)
            .await?;
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
    contract: &super::LiveRowShape,
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
        global: row.global,
        untracked: false,
        created_at: Some(row.created_at),
        updated_at: Some(row.updated_at),
        snapshot_content,
    })
}

fn untracked_row_to_row(
    row: super::UntrackedRow,
    contract: &super::LiveRowShape,
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
        change_id: Some(row.change_id),
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
        plugin_key: tombstone.plugin_key,
        metadata: tombstone.metadata,
        change_id: tombstone.change_id,
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
    contract: &super::LiveRowShape,
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

fn exact_live_row_matches_query(row: &LiveRow, request: &ExactLiveRowQuery) -> bool {
    request.file_id.matches(row.file_id.as_ref())
        && request
            .schema_version
            .as_deref()
            .is_none_or(|schema_version| row.schema_version == schema_version)
        && request.plugin_key.matches(row.plugin_key.as_ref())
        && request.global.is_none_or(|global| row.global == global)
        && request
            .untracked
            .is_none_or(|untracked| row.untracked == untracked)
}

fn default_true() -> bool {
    true
}

fn live_write_from_live_row(row: &LiveRow) -> Result<LiveWriteRow, LixError> {
    let updated_at = row.updated_at.clone().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "live_state write for '{}:{}' requires updated_at",
                row.schema_key, row.entity_id
            ),
        )
    })?;
    let change_id = row.change_id.clone().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "live_state write for '{}:{}' requires change_id",
                row.schema_key, row.entity_id
            ),
        )
    })?;

    Ok(LiveWriteRow {
        entity_id: row.entity_id.clone(),
        schema_key: row.schema_key.clone(),
        schema_version: row.schema_version.clone(),
        file_id: row.file_id.clone(),
        version_id: row.version_id.clone(),
        global: row.global,
        untracked: row.untracked,
        plugin_key: row.plugin_key.clone(),
        metadata: row.metadata.clone(),
        change_id,
        snapshot_content: row.snapshot_content.clone(),
        created_at: row.created_at.clone(),
        updated_at,
        operation: if row.snapshot_content.is_some() {
            LiveWriteOperation::Upsert
        } else if row.untracked {
            LiveWriteOperation::Delete
        } else {
            LiveWriteOperation::Tombstone
        },
    })
}

#[cfg(test)]
mod tests {
    use super::{
        decode_registered_schema_row, exact_live_row_matches_query, live_write_from_live_row,
        load_exact_live_row, partition_live_rows_for_write, scan_live_rows, ExactLiveRowQuery,
        LiveRow, LiveRowQuery, LiveRowSource,
    };
    use crate::live_state::LiveWriteOperation;
    use crate::live_state::ReplayCursor;
    use crate::live_state::{
        write_live_rows, LiveStateMode, ScanConstraint, ScanField, ScanOperator,
    };
    use crate::schema::LixCommit;
    use crate::schema::SchemaKey;
    use crate::test_support::{
        init_test_backend_core, seed_canonical_change_row, seed_live_state_status_row,
        seed_local_version_head, CanonicalChangeSeed, TestSqliteBackend,
    };
    use crate::{CommittedVersionFrontier, NullableKeyFilter, Value};
    use serde_json::Value as JsonValue;

    fn registered_schema_row(snapshot_content: Option<&str>) -> LiveRow {
        LiveRow {
            entity_id: "users~1".to_string(),
            file_id: None,
            schema_key: "lix_registered_schema".to_string(),
            schema_version: "1".to_string(),
            version_id: "global".to_string(),
            plugin_key: None,
            metadata: None,
            change_id: Some("chg_schema".to_string()),
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
            file_id: None,
            schema_key: "lix_key_value".to_string(),
            schema_version: "1".to_string(),
            version_id: "main".to_string(),
            plugin_key: None,
            metadata: Some("{\"kind\":\"state\"}".to_string()),
            change_id: Some("chg_123".to_string()),
            global: false,
            untracked,
            created_at: Some("2026-01-01T00:00:00Z".to_string()),
            updated_at: Some("2026-01-01T00:00:00Z".to_string()),
            snapshot_content: snapshot_content.map(ToString::to_string),
        }
    }

    fn exact_live_row_query() -> ExactLiveRowQuery {
        ExactLiveRowQuery {
            source: LiveRowSource::Tracked,
            schema_key: "lix_key_value".to_string(),
            version_id: "main".to_string(),
            entity_id: "settings".to_string(),
            file_id: NullableKeyFilter::Null,
            schema_version: None,
            plugin_key: NullableKeyFilter::Any,
            global: None,
            untracked: None,
            include_tombstones: false,
            include_global_overlay: true,
            include_untracked_overlay: true,
        }
    }

    fn commit_live_row(snapshot: &LixCommit, version_id: &str) -> LiveRow {
        LiveRow {
            entity_id: snapshot.id.clone(),
            file_id: None,
            schema_key: "lix_commit".to_string(),
            schema_version: "1".to_string(),
            version_id: version_id.to_string(),
            plugin_key: None,
            metadata: Some("{\"kind\":\"commit\"}".to_string()),
            change_id: Some(format!("change-{}", snapshot.id)),
            global: version_id == "global",
            untracked: false,
            created_at: Some("2026-03-30T00:00:00Z".to_string()),
            updated_at: Some("2026-03-30T00:00:00Z".to_string()),
            snapshot_content: Some(
                serde_json::to_string(snapshot).expect("commit snapshot should serialize"),
            ),
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

        let write = live_write_from_live_row(&row).expect("tracked write should build");

        assert_eq!(write.snapshot_content, None);
        assert_eq!(write.operation, LiveWriteOperation::Tombstone);
    }

    #[test]
    fn tracked_write_uses_snapshot_some_as_upsert() {
        let row = writable_live_row(false, Some(r#"{"key":"theme","value":"dark"}"#));

        let write = live_write_from_live_row(&row).expect("tracked write should build");

        assert_eq!(
            write.snapshot_content,
            Some(r#"{"key":"theme","value":"dark"}"#.to_string())
        );
        assert_eq!(write.operation, LiveWriteOperation::Upsert);
    }

    #[test]
    fn untracked_write_uses_snapshot_none_as_delete() {
        let row = writable_live_row(true, None);

        let write = live_write_from_live_row(&row).expect("untracked write should build");

        assert_eq!(write.snapshot_content, None);
        assert_eq!(write.operation, LiveWriteOperation::Delete);
    }

    #[test]
    fn untracked_write_uses_snapshot_some_as_upsert() {
        let row = writable_live_row(true, Some(r#"{"key":"theme","value":"dark"}"#));

        let write = live_write_from_live_row(&row).expect("untracked write should build");

        assert_eq!(
            write.snapshot_content,
            Some(r#"{"key":"theme","value":"dark"}"#.to_string())
        );
        assert_eq!(write.operation, LiveWriteOperation::Upsert);
    }

    #[test]
    fn partition_live_rows_for_write_fans_out_by_untracked_flag() {
        let tracked = writable_live_row(false, Some(r#"{"key":"theme","value":"dark"}"#));
        let untracked = writable_live_row(true, None);

        let (tracked_writes, untracked_writes) =
            partition_live_rows_for_write(&[tracked, untracked]).expect("partition should build");

        assert_eq!(tracked_writes.len(), 1);
        assert_eq!(untracked_writes.len(), 1);
        assert_eq!(tracked_writes[0].operation, LiveWriteOperation::Upsert);
        assert_eq!(untracked_writes[0].operation, LiveWriteOperation::Delete);
    }

    #[test]
    fn untracked_write_requires_change_id() {
        let mut row = writable_live_row(true, Some(r#"{"key":"theme","value":"dark"}"#));
        row.change_id = None;

        let error = live_write_from_live_row(&row).expect_err("missing change_id should fail");

        assert!(error.description.contains("requires change_id"));
    }

    #[test]
    fn exact_live_row_query_matches_optional_schema_version_and_plugin_key() {
        let row = writable_live_row(false, Some(r#"{"key":"theme","value":"dark"}"#));
        let mut query = exact_live_row_query();
        query.schema_version = Some("1".to_string());
        query.plugin_key = NullableKeyFilter::Null;

        assert!(exact_live_row_matches_query(&row, &query));
    }

    #[tokio::test]
    async fn exact_effective_live_row_falls_back_to_canonical_when_live_state_is_stale() {
        let backend = TestSqliteBackend::new();
        init_test_backend_core(&backend)
            .await
            .expect("test backend init should succeed");
        seed_local_version_head(&backend, "main", "commit-1", "2026-03-30T00:00:00Z")
            .await
            .expect("local head should seed");
        seed_canonical_change_row(
            &backend,
            CanonicalChangeSeed {
                id: "change-1",
                entity_id: "settings",
                schema_key: "lix_key_value",
                schema_version: "1",
                file_id: None,
                plugin_key: None,
                snapshot_id: "snapshot-1",
                snapshot_content: Some(r#"{"key":"theme","value":"canonical"}"#),
                metadata: Some(r#"{"kind":"state"}"#),
                created_at: "2026-03-30T00:00:00Z",
            },
        )
        .await
        .expect("canonical row should seed");
        seed_canonical_change_row(
            &backend,
            CanonicalChangeSeed {
                id: "change-commit-1",
                entity_id: "commit-1",
                schema_key: "lix_commit",
                schema_version: "1",
                file_id: None,
                plugin_key: None,
                snapshot_id: "snapshot-commit-1",
                snapshot_content: Some(
                    r#"{"id":"commit-1","change_set_id":"cs-1","change_ids":["change-1"],"parent_commit_ids":[]}"#,
                ),
                metadata: None,
                created_at: "2026-03-30T00:00:00Z",
            },
        )
        .await
        .expect("canonical commit should seed");

        let mut transaction = backend
            .begin_write_transaction()
            .await
            .expect("write transaction should open");
        write_live_rows(
            transaction.as_mut(),
            &[LiveRow {
                snapshot_content: Some(r#"{"key":"theme","value":"stale-live-state"}"#.to_string()),
                ..writable_live_row(false, Some(r#"{"key":"theme","value":"stale-live-state"}"#))
            }],
        )
        .await
        .expect("stale live_state row should write");
        transaction
            .commit()
            .await
            .expect("write transaction should commit");

        seed_live_state_status_row(
            &backend,
            LiveStateMode::NeedsRebuild,
            Some(&ReplayCursor {
                change_id: "change-1".to_string(),
                created_at: "2026-03-30T00:00:00Z".to_string(),
            }),
            Some(&CommittedVersionFrontier {
                version_heads: std::collections::BTreeMap::from([(
                    "main".to_string(),
                    "commit-1".to_string(),
                )]),
            }),
            "2026-03-30T00:00:01Z",
        )
        .await
        .expect("live_state status should seed");

        let row = load_exact_live_row(
            &backend,
            &ExactLiveRowQuery {
                source: LiveRowSource::Effective,
                schema_key: "lix_key_value".to_string(),
                version_id: "main".to_string(),
                entity_id: "settings".to_string(),
                file_id: NullableKeyFilter::Null,
                schema_version: Some("1".to_string()),
                plugin_key: NullableKeyFilter::Null,
                global: Some(false),
                untracked: Some(false),
                include_tombstones: false,
                include_global_overlay: true,
                include_untracked_overlay: true,
            },
        )
        .await
        .expect("exact effective lookup should succeed")
        .expect("exact effective lookup should return a row");

        assert_eq!(
            row.snapshot_content.as_deref(),
            Some(r#"{"key":"theme","value":"canonical"}"#)
        );
        assert_eq!(row.change_id.as_deref(), Some("change-1"));
        assert!(!row.global);
        assert!(!row.untracked);
    }

    #[tokio::test]
    async fn scan_live_rows_expands_lazy_change_set_element_surface_from_visible_commit_rows() {
        let backend = TestSqliteBackend::new();
        init_test_backend_core(&backend)
            .await
            .expect("test backend init should succeed");
        seed_canonical_change_row(
            &backend,
            CanonicalChangeSeed {
                id: "change-1",
                entity_id: "entity-a",
                schema_key: "test_schema",
                schema_version: "1",
                file_id: Some("file-a"),
                plugin_key: None,
                snapshot_id: "snapshot-1",
                snapshot_content: Some(r#"{"key":"a"}"#),
                metadata: Some(r#"{"member":true}"#),
                created_at: "2026-03-30T00:00:00Z",
            },
        )
        .await
        .expect("canonical member change should seed");

        let mut transaction = backend
            .begin_write_transaction()
            .await
            .expect("write transaction should open");
        write_live_rows(
            transaction.as_mut(),
            &[commit_live_row(
                &LixCommit {
                    id: "commit-1".to_string(),
                    change_set_id: Some("cs-1".to_string()),
                    change_ids: vec!["change-1".to_string()],
                    author_account_ids: vec![],
                    parent_commit_ids: vec![],
                },
                "main",
            )],
        )
        .await
        .expect("commit live row should write");
        transaction
            .commit()
            .await
            .expect("write transaction should commit");

        let rows = scan_live_rows(
            &backend,
            &LiveRowQuery {
                schema_key: "lix_change_set_element".to_string(),
                version_id: "main".to_string(),
                source: LiveRowSource::Effective,
                constraints: vec![ScanConstraint {
                    field: ScanField::EntityId,
                    operator: ScanOperator::Eq(Value::Text("cs-1~change-1".to_string())),
                }],
                include_tombstones: false,
            },
        )
        .await
        .expect("lazy derived scan should succeed");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].schema_key, "lix_change_set_element");
        assert_eq!(rows[0].entity_id, "cs-1~change-1");
        assert_eq!(rows[0].change_id.as_deref(), Some("change-1"));
    }

    #[tokio::test]
    async fn load_exact_live_row_matches_lazy_commit_edge_surface() {
        let backend = TestSqliteBackend::new();
        init_test_backend_core(&backend)
            .await
            .expect("test backend init should succeed");

        let mut transaction = backend
            .begin_write_transaction()
            .await
            .expect("write transaction should open");
        write_live_rows(
            transaction.as_mut(),
            &[commit_live_row(
                &LixCommit {
                    id: "commit-child".to_string(),
                    change_set_id: Some("cs-1".to_string()),
                    change_ids: vec![],
                    author_account_ids: vec![],
                    parent_commit_ids: vec!["commit-parent".to_string()],
                },
                "main",
            )],
        )
        .await
        .expect("commit live row should write");
        transaction
            .commit()
            .await
            .expect("write transaction should commit");

        let row = load_exact_live_row(
            &backend,
            &ExactLiveRowQuery {
                source: LiveRowSource::Effective,
                schema_key: "lix_commit_edge".to_string(),
                version_id: "main".to_string(),
                entity_id: "commit-parent~commit-child".to_string(),
                file_id: NullableKeyFilter::Null,
                schema_version: Some("1".to_string()),
                plugin_key: NullableKeyFilter::Null,
                global: Some(false),
                untracked: Some(false),
                include_tombstones: false,
                include_global_overlay: true,
                include_untracked_overlay: true,
            },
        )
        .await
        .expect("exact lazy derived lookup should succeed")
        .expect("exact lazy derived lookup should return a row");

        assert_eq!(row.schema_key, "lix_commit_edge");
        assert_eq!(row.entity_id, "commit-parent~commit-child");
        assert_eq!(row.change_id.as_deref(), Some("change-commit-child"));
    }
}
