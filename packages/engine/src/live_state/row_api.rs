use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value as JsonValue;

use crate::canonical::{
    load_change, load_visible_state, CanonicalContentMode, CanonicalTombstoneMode,
    CanonicalVisibility, CanonicalVisibleStateFilter, CanonicalVisibleStateRequest,
    CanonicalVisibleStateRow,
};
use crate::{LixBackend, LixBackendTransaction, LixError, Value};

use super::constraints::ScanConstraint;
use super::schema_access::load_schema_read_contract_with_backend;
use super::tracked::{
    apply_write_batch_in_transaction as apply_tracked_write_batch_in_transaction,
    ExactTrackedRowRequest, TrackedScanRequest,
};
use super::tracked::{TrackedWriteOperation, TrackedWriteRow};
use super::untracked::{
    apply_write_batch_in_transaction as apply_untracked_write_batch_in_transaction,
    load_exact_row_with_backend as load_exact_untracked_row_with_backend,
    scan_rows_with_backend as scan_untracked_rows_with_backend, ExactUntrackedRowRequest,
    UntrackedScanRequest,
};
use super::untracked::{UntrackedWriteOperation, UntrackedWriteRow};
use super::{
    load_exact_tracked_row_with_backend, load_exact_tracked_tombstone_with_executor,
    load_exact_untracked_row_with_executor, scan_tracked_rows_with_backend,
    scan_tracked_tombstones_with_executor, RowIdentity,
};
use crate::contracts::GLOBAL_VERSION_ID;
use crate::contracts::{
    version_ref_file_id, version_ref_schema_key, version_ref_storage_version_id,
};
use crate::schema::{schema_key_from_definition, SchemaKey};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RowReadMode {
    Tracked,
    Untracked,
    Effective,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum LiveRowSemantics {
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
    pub semantics: LiveRowSemantics,
    pub schema_key: String,
    pub version_id: String,
    pub entity_id: String,
    #[serde(default)]
    pub file_id: Option<String>,
    #[serde(default)]
    pub schema_version: Option<String>,
    #[serde(default)]
    pub plugin_key: Option<String>,
    #[serde(default)]
    pub writer_key: Option<String>,
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
    match request.semantics {
        LiveRowSemantics::Tracked => load_exact_tracked_row(backend, request).await,
        LiveRowSemantics::Untracked => load_exact_untracked_row(backend, request).await,
        LiveRowSemantics::Effective => load_exact_effective_row(backend, request).await,
    }
}

pub async fn write_live_rows(
    transaction: &mut dyn LixBackendTransaction,
    rows: &[LiveRow],
) -> Result<(), LixError> {
    if !rows.is_empty() {
        let annotations = rows
            .iter()
            .map(|row| {
                (
                    RowIdentity {
                        schema_key: row.schema_key.clone(),
                        version_id: row.version_id.clone(),
                        entity_id: row.entity_id.clone(),
                        file_id: row.file_id.clone(),
                    },
                    row.writer_key.clone(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut executor = &mut *transaction;
        super::writer_key::apply_writer_key_annotations_with_executor(&mut executor, &annotations)
            .await?;
    }

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
    let contract = load_schema_read_contract_with_backend(backend, &request.schema_key).await?;
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

    overlay_writer_key_annotations_on_tracked_live_rows(backend, &mut rows).await?;
    rows.sort_by(row_sort_key);
    Ok(rows)
}

async fn scan_untracked_rows(
    backend: &dyn LixBackend,
    request: &LiveRowQuery,
) -> Result<Vec<LiveRow>, LixError> {
    let contract = load_schema_read_contract_with_backend(backend, &request.schema_key).await?;
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
    let contract = load_schema_read_contract_with_backend(backend, &request.schema_key).await?;
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
        let mut rows = vec![tracked_row_to_row(row, &contract)?];
        overlay_writer_key_annotations_on_tracked_live_rows(backend, &mut rows).await?;
        let row = rows
            .pop()
            .expect("tracked exact read overlay should preserve single row");
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
        let mut rows = vec![tracked_tombstone_to_row(tombstone)];
        overlay_writer_key_annotations_on_tracked_live_rows(backend, &mut rows).await?;
        let row = rows
            .pop()
            .expect("tracked tombstone overlay should preserve single row");
        return Ok(exact_live_row_matches_query(&row, request).then_some(row));
    }

    Ok(None)
}

async fn load_exact_untracked_row(
    backend: &dyn LixBackend,
    request: &ExactLiveRowQuery,
) -> Result<Option<LiveRow>, LixError> {
    let contract = load_schema_read_contract_with_backend(backend, &request.schema_key).await?;
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
    backend: &dyn LixBackend,
    request: &LiveRowQuery,
) -> Result<Vec<LiveRow>, LixError> {
    scan_effective_rows_with_options(backend, request, true, true).await
}

async fn scan_effective_rows_with_options(
    backend: &dyn LixBackend,
    request: &LiveRowQuery,
    include_global_overlay: bool,
    include_untracked_overlay: bool,
) -> Result<Vec<LiveRow>, LixError> {
    let mut resolved = BTreeMap::<(String, String), LiveRow>::new();
    let mut hidden = BTreeSet::<(String, String)>::new();
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
    backend: &dyn LixBackend,
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
        mode: RowReadMode::Effective,
        constraints: vec![ScanConstraint {
            field: super::ScanField::EntityId,
            operator: super::ScanOperator::Eq(Value::Text(request.entity_id.clone())),
        }],
        include_tombstones: request.include_tombstones,
    };
    if let Some(file_id) = request.file_id.as_ref() {
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
    backend: &dyn LixBackend,
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
    backend: &dyn LixBackend,
    request: &ExactLiveRowQuery,
    lane: EffectiveLane,
) -> Result<EffectiveLaneOutcome, LixError> {
    let contract = load_schema_read_contract_with_backend(backend, &request.schema_key).await?;
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
    backend: &dyn LixBackend,
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
    if let Some(file_id) = request.file_id.as_ref() {
        filter.file_ids.insert(file_id.clone());
    }
    if let Some(plugin_key) = request.plugin_key.as_ref() {
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
    executor: &mut dyn crate::backend::QueryExecutor,
    version_id: &str,
) -> Result<Option<String>, LixError> {
    let Some(row) = load_exact_untracked_row_with_executor(
        executor,
        &ExactUntrackedRowRequest {
            schema_key: version_ref_schema_key().to_string(),
            version_id: version_ref_storage_version_id().to_string(),
            entity_id: version_id.to_string(),
            file_id: Some(version_ref_file_id().to_string()),
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
        && request
            .file_id
            .as_ref()
            .is_none_or(|file_id| row.file_id == *file_id)
        && request
            .schema_version
            .as_ref()
            .is_none_or(|schema_version| row.schema_version == *schema_version)
        && request
            .plugin_key
            .as_ref()
            .is_none_or(|plugin_key| row.plugin_key == *plugin_key)
}

async fn canonical_effective_lane_outcome_from_visible_row(
    backend: &dyn LixBackend,
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
    let writer_key = super::writer_key::load_writer_key_annotation_for_state_row(
        backend,
        &storage_version_id,
        &row.schema_key,
        &row.entity_id,
        &row.file_id,
    )
    .await?;

    let row = LiveRow {
        entity_id: row.entity_id,
        file_id: row.file_id,
        schema_key: row.schema_key,
        schema_version: row.schema_version,
        version_id: storage_version_id,
        plugin_key: row.plugin_key,
        metadata: row.metadata,
        change_id: Some(change.id),
        writer_key,
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
    backend: &dyn LixBackend,
    request: &LiveRowQuery,
    lane: EffectiveLane,
) -> Result<Vec<LiveRow>, LixError> {
    if lane.is_untracked() {
        let contract = load_schema_read_contract_with_backend(backend, &request.schema_key).await?;
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

    let contract = load_schema_read_contract_with_backend(backend, &request.schema_key).await?;
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

    overlay_writer_key_annotations_on_tracked_live_rows(backend, &mut rows).await?;
    Ok(rows)
}

async fn overlay_writer_key_annotations_on_tracked_live_rows(
    backend: &dyn LixBackend,
    rows: &mut [LiveRow],
) -> Result<(), LixError> {
    if rows.is_empty() {
        return Ok(());
    }

    let row_identities = rows
        .iter()
        .map(|row| RowIdentity {
            schema_key: row.schema_key.clone(),
            version_id: row.version_id.clone(),
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
        })
        .collect::<BTreeSet<_>>();

    if row_identities.is_empty() {
        return Ok(());
    }

    let annotations =
        super::writer_key::load_writer_key_annotations(backend, &row_identities).await?;
    for row in rows.iter_mut() {
        row.writer_key = annotations
            .get(&RowIdentity {
                schema_key: row.schema_key.clone(),
                version_id: row.version_id.clone(),
                entity_id: row.entity_id.clone(),
                file_id: row.file_id.clone(),
            })
            .cloned()
            .unwrap_or(None);
    }

    Ok(())
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

fn exact_live_row_matches_query(row: &LiveRow, request: &ExactLiveRowQuery) -> bool {
    request
        .schema_version
        .as_deref()
        .is_none_or(|schema_version| row.schema_version == schema_version)
        && request
            .plugin_key
            .as_deref()
            .is_none_or(|plugin_key| row.plugin_key == plugin_key)
        && request
            .writer_key
            .as_deref()
            .is_none_or(|writer_key| row.writer_key.as_deref() == Some(writer_key))
        && request.global.is_none_or(|global| row.global == global)
        && request
            .untracked
            .is_none_or(|untracked| row.untracked == untracked)
}

fn default_true() -> bool {
    true
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
        decode_registered_schema_row, exact_live_row_matches_query, load_exact_live_row,
        partition_live_rows_for_write, tracked_write_from_live_row, untracked_write_from_live_row,
        ExactLiveRowQuery, LiveRow, LiveRowSemantics,
    };
    use crate::live_state::tracked::TrackedWriteOperation;
    use crate::live_state::untracked::UntrackedWriteOperation;
    use crate::live_state::{write_live_rows, LiveStateMode};
    use crate::schema::SchemaKey;
    use crate::test_support::{
        init_test_backend_core, seed_canonical_change_row, seed_live_state_status_row,
        seed_local_version_head, CanonicalChangeSeed, TestSqliteBackend,
    };
    use crate::{CommittedVersionFrontier, LixBackend, ReplayCursor, TransactionMode};
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

    fn exact_live_row_query() -> ExactLiveRowQuery {
        ExactLiveRowQuery {
            semantics: LiveRowSemantics::Tracked,
            schema_key: "lix_key_value".to_string(),
            version_id: "main".to_string(),
            entity_id: "settings".to_string(),
            file_id: Some("state".to_string()),
            schema_version: None,
            plugin_key: None,
            writer_key: None,
            global: None,
            untracked: None,
            include_tombstones: false,
            include_global_overlay: true,
            include_untracked_overlay: true,
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

    #[test]
    fn exact_live_row_query_matches_optional_schema_version_plugin_key_and_writer_key() {
        let row = writable_live_row(false, Some(r#"{"key":"theme","value":"dark"}"#));
        let mut query = exact_live_row_query();
        query.schema_version = Some("1".to_string());
        query.plugin_key = Some("lix".to_string());
        query.writer_key = Some("writer-a".to_string());

        assert!(exact_live_row_matches_query(&row, &query));
    }

    #[test]
    fn exact_live_row_query_rejects_writer_key_mismatch() {
        let row = writable_live_row(false, Some(r#"{"key":"theme","value":"dark"}"#));
        let mut query = exact_live_row_query();
        query.writer_key = Some("writer-b".to_string());

        assert!(!exact_live_row_matches_query(&row, &query));
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
                file_id: "state",
                plugin_key: "lix",
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
                file_id: "lix",
                plugin_key: "lix",
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
            .begin_transaction(TransactionMode::Write)
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
                semantics: LiveRowSemantics::Effective,
                schema_key: "lix_key_value".to_string(),
                version_id: "main".to_string(),
                entity_id: "settings".to_string(),
                file_id: Some("state".to_string()),
                schema_version: Some("1".to_string()),
                plugin_key: Some("lix".to_string()),
                writer_key: None,
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
}
