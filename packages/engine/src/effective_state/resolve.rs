use std::collections::BTreeMap;

use crate::effective_state::contracts::{
    EffectiveRow, EffectiveRowIdentity, EffectiveRowRequest, EffectiveRowSet, EffectiveRowState,
    EffectiveRowsRequest, LaneResult, OverlayLane, ReadContext,
};
use crate::live_tracked_state::{
    BatchTrackedRowRequest, ExactTrackedRowRequest, TrackedRow, TrackedScanRequest,
    TrackedTombstoneMarker,
};
use crate::live_untracked_state::{
    BatchUntrackedRowRequest, ExactUntrackedRowRequest, UntrackedRow, UntrackedScanRequest,
};
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixError, Value};

pub fn overlay_lanes(include_global: bool, include_untracked: bool) -> Vec<OverlayLane> {
    let mut lanes = vec![OverlayLane::LocalTracked];
    if include_untracked {
        lanes.insert(0, OverlayLane::LocalUntracked);
    }
    if include_global {
        if include_untracked {
            lanes.push(OverlayLane::GlobalUntracked);
        }
        lanes.push(OverlayLane::GlobalTracked);
    }
    lanes
}

pub fn overlay_lanes_for_version(
    version_id: &str,
    include_global: bool,
    include_untracked: bool,
) -> Vec<OverlayLane> {
    overlay_lanes(
        include_global && version_id != GLOBAL_VERSION_ID,
        include_untracked,
    )
}

pub async fn resolve_effective_row(
    request: &EffectiveRowRequest,
    context: &ReadContext<'_>,
) -> Result<Option<EffectiveRow>, LixError> {
    let lanes = overlay_lanes_for_version(
        &request.version_id,
        request.include_global,
        request.include_untracked,
    );

    for lane in lanes {
        let storage_version_id = storage_version_id(&request.version_id, lane);

        match lane {
            OverlayLane::LocalTracked | OverlayLane::GlobalTracked => {
                let exact_request = ExactTrackedRowRequest {
                    schema_key: request.schema_key.clone(),
                    version_id: storage_version_id.clone(),
                    entity_id: request.entity_id.clone(),
                    file_id: request.file_id.clone(),
                };
                if let Some(row) = context.tracked.load_exact_row(&exact_request).await? {
                    return Ok(Some(effective_row_from_tracked(
                        row,
                        &request.version_id,
                        lane,
                    )));
                }
                if let Some(tombstone_view) = context.tracked_tombstones {
                    if tombstone_view
                        .load_exact_tombstone(&exact_request)
                        .await?
                        .is_some()
                    {
                        return Ok(None);
                    }
                }
            }
            OverlayLane::LocalUntracked | OverlayLane::GlobalUntracked => {
                let exact_request = ExactUntrackedRowRequest {
                    schema_key: request.schema_key.clone(),
                    version_id: storage_version_id,
                    entity_id: request.entity_id.clone(),
                    file_id: request.file_id.clone(),
                };
                if let Some(row) = context.untracked.load_exact_row(&exact_request).await? {
                    return Ok(Some(effective_row_from_untracked(
                        row,
                        &request.version_id,
                        lane,
                    )));
                }
            }
        }
    }

    Ok(None)
}

pub async fn resolve_effective_rows(
    request: &EffectiveRowsRequest,
    context: &ReadContext<'_>,
) -> Result<EffectiveRowSet, LixError> {
    let lanes = overlay_lanes_for_version(
        &request.version_id,
        request.include_global,
        request.include_untracked,
    );
    let mut visible = BTreeMap::<EffectiveRowIdentity, EffectiveRow>::new();
    let mut hidden = BTreeMap::<EffectiveRowIdentity, EffectiveRow>::new();

    for lane in lanes {
        let lane_rows = match lane {
            OverlayLane::LocalTracked | OverlayLane::GlobalTracked => {
                scan_tracked_lane(request, context, lane).await?
            }
            OverlayLane::LocalUntracked | OverlayLane::GlobalUntracked => {
                scan_untracked_lane(request, context, lane).await?
            }
        };

        for (identity, result) in lane_rows {
            if visible.contains_key(&identity) || hidden.contains_key(&identity) {
                continue;
            }

            match result {
                LaneResult::Found(row) => {
                    visible.insert(identity, row);
                }
                LaneResult::Tombstone => {
                    hidden.insert(
                        identity.clone(),
                        tombstone_placeholder_row(
                            &request.schema_key,
                            &request.version_id,
                            lane,
                            identity,
                        ),
                    );
                }
                LaneResult::Missing | LaneResult::Unavailable => {}
            }
        }
    }

    let mut rows = visible.into_values().collect::<Vec<_>>();
    if request.include_tombstones {
        rows.extend(hidden.into_values());
    }
    rows.sort_by(|left, right| {
        left.identity()
            .cmp(&right.identity())
            .then_with(|| left.overlay_lane.cmp(&right.overlay_lane))
    });

    Ok(EffectiveRowSet { rows })
}

async fn scan_tracked_lane(
    request: &EffectiveRowsRequest,
    context: &ReadContext<'_>,
    lane: OverlayLane,
) -> Result<BTreeMap<EffectiveRowIdentity, LaneResult<EffectiveRow>>, LixError> {
    let storage_version_id = storage_version_id(&request.version_id, lane);
    let mut rows_by_identity = BTreeMap::new();

    let rows = context
        .tracked
        .scan_rows(&TrackedScanRequest {
            schema_key: request.schema_key.clone(),
            version_id: storage_version_id.clone(),
            constraints: request.constraints.clone(),
            required_columns: request.required_columns.clone(),
        })
        .await?;

    for row in rows {
        let effective = effective_row_from_tracked(row, &request.version_id, lane);
        rows_by_identity.insert(effective.identity(), LaneResult::Found(effective));
    }

    if let Some(tombstone_view) = context.tracked_tombstones {
        let tombstones = tombstone_view
            .scan_tombstones(&TrackedScanRequest {
                schema_key: request.schema_key.clone(),
                version_id: storage_version_id,
                constraints: request.constraints.clone(),
                required_columns: Vec::new(),
            })
            .await?;
        for tombstone in tombstones {
            let tombstone_row = effective_row_from_tombstone(tombstone, &request.version_id, lane);
            rows_by_identity
                .entry(tombstone_row.identity())
                .or_insert(LaneResult::Tombstone);
        }
    }

    Ok(rows_by_identity)
}

async fn scan_untracked_lane(
    request: &EffectiveRowsRequest,
    context: &ReadContext<'_>,
    lane: OverlayLane,
) -> Result<BTreeMap<EffectiveRowIdentity, LaneResult<EffectiveRow>>, LixError> {
    let storage_version_id = storage_version_id(&request.version_id, lane);
    let mut rows_by_identity = BTreeMap::new();
    let rows = context
        .untracked
        .scan_rows(&UntrackedScanRequest {
            schema_key: request.schema_key.clone(),
            version_id: storage_version_id,
            constraints: request.constraints.clone(),
            required_columns: request.required_columns.clone(),
        })
        .await?;

    for row in rows {
        let effective = effective_row_from_untracked(row, &request.version_id, lane);
        rows_by_identity.insert(effective.identity(), LaneResult::Found(effective));
    }

    Ok(rows_by_identity)
}

fn effective_row_from_tracked(
    row: TrackedRow,
    requested_version_id: &str,
    lane: OverlayLane,
) -> EffectiveRow {
    let source_version_id = row.version_id.clone();
    let version_id = projected_version_id(requested_version_id, lane, &source_version_id);
    EffectiveRow {
        entity_id: row.entity_id,
        schema_key: row.schema_key,
        schema_version: Some(row.schema_version),
        file_id: row.file_id,
        version_id,
        source_version_id,
        global: lane.is_global() || row.global,
        untracked: false,
        plugin_key: Some(row.plugin_key),
        metadata: row.metadata,
        writer_key: row.writer_key,
        created_at: Some(row.created_at),
        updated_at: Some(row.updated_at),
        source_change_id: row.change_id,
        overlay_lane: lane,
        state: EffectiveRowState::Visible,
        values: row.values,
    }
}

fn effective_row_from_untracked(
    row: UntrackedRow,
    requested_version_id: &str,
    lane: OverlayLane,
) -> EffectiveRow {
    let source_version_id = row.version_id.clone();
    let version_id = projected_version_id(requested_version_id, lane, &source_version_id);
    EffectiveRow {
        entity_id: row.entity_id,
        schema_key: row.schema_key,
        schema_version: Some(row.schema_version),
        file_id: row.file_id,
        version_id,
        source_version_id,
        global: lane.is_global() || row.global,
        untracked: true,
        plugin_key: Some(row.plugin_key),
        metadata: row.metadata,
        writer_key: row.writer_key,
        created_at: Some(row.created_at),
        updated_at: Some(row.updated_at),
        source_change_id: None,
        overlay_lane: lane,
        state: EffectiveRowState::Visible,
        values: row.values,
    }
}

fn effective_row_from_tombstone(
    row: TrackedTombstoneMarker,
    requested_version_id: &str,
    lane: OverlayLane,
) -> EffectiveRow {
    let source_version_id = row.version_id.clone();
    let version_id = projected_version_id(requested_version_id, lane, &source_version_id);
    EffectiveRow {
        entity_id: row.entity_id,
        schema_key: row.schema_key,
        schema_version: row.schema_version,
        file_id: row.file_id,
        version_id,
        source_version_id,
        global: lane.is_global() || row.global,
        untracked: false,
        plugin_key: row.plugin_key,
        metadata: row.metadata,
        writer_key: row.writer_key,
        created_at: row.created_at,
        updated_at: row.updated_at,
        source_change_id: row.change_id,
        overlay_lane: lane,
        state: EffectiveRowState::Tombstone,
        values: BTreeMap::new(),
    }
}

fn tombstone_placeholder_row(
    schema_key: &str,
    requested_version_id: &str,
    lane: OverlayLane,
    identity: EffectiveRowIdentity,
) -> EffectiveRow {
    EffectiveRow {
        entity_id: identity.entity_id,
        schema_key: schema_key.to_string(),
        schema_version: None,
        file_id: identity.file_id,
        version_id: requested_version_id.to_string(),
        source_version_id: storage_version_id(requested_version_id, lane),
        global: lane.is_global(),
        untracked: false,
        plugin_key: None,
        metadata: None,
        writer_key: None,
        created_at: None,
        updated_at: None,
        source_change_id: None,
        overlay_lane: lane,
        state: EffectiveRowState::Tombstone,
        values: BTreeMap::new(),
    }
}

fn storage_version_id(requested_version_id: &str, lane: OverlayLane) -> String {
    if lane.is_global() {
        GLOBAL_VERSION_ID.to_string()
    } else {
        requested_version_id.to_string()
    }
}

fn projected_version_id(
    requested_version_id: &str,
    lane: OverlayLane,
    source_version_id: &str,
) -> String {
    if lane.is_global() && source_version_id == GLOBAL_VERSION_ID {
        requested_version_id.to_string()
    } else {
        source_version_id.to_string()
    }
}

#[allow(dead_code)]
async fn load_effective_rows_exact_batch(
    request: &EffectiveRowsRequest,
    context: &ReadContext<'_>,
    lane: OverlayLane,
    identities: &[EffectiveRowIdentity],
) -> Result<BTreeMap<EffectiveRowIdentity, EffectiveRow>, LixError> {
    let entity_ids = identities
        .iter()
        .map(|identity| identity.entity_id.clone())
        .collect::<Vec<_>>();
    let storage_version_id = storage_version_id(&request.version_id, lane);
    match lane {
        OverlayLane::LocalTracked | OverlayLane::GlobalTracked => context
            .tracked
            .load_exact_rows(&BatchTrackedRowRequest {
                schema_key: request.schema_key.clone(),
                version_id: storage_version_id,
                entity_ids,
                file_id: None,
            })
            .await
            .map(|rows| {
                rows.into_iter()
                    .map(|row| {
                        let effective = effective_row_from_tracked(row, &request.version_id, lane);
                        (effective.identity(), effective)
                    })
                    .collect()
            }),
        OverlayLane::LocalUntracked | OverlayLane::GlobalUntracked => context
            .untracked
            .load_exact_rows(&BatchUntrackedRowRequest {
                schema_key: request.schema_key.clone(),
                version_id: storage_version_id,
                entity_ids,
                file_id: None,
            })
            .await
            .map(|rows| {
                rows.into_iter()
                    .map(|row| {
                        let effective =
                            effective_row_from_untracked(row, &request.version_id, lane);
                        (effective.identity(), effective)
                    })
                    .collect()
            }),
    }
}

#[allow(dead_code)]
fn _preserve_value_type(_value: &Value) {}
