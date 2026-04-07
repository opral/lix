use crate::schema::annotations::writer_key::load_workspace_writer_key_annotation_for_state_row;
use crate::canonical::read::{
    load_exact_committed_state_row_at_version_head as load_exact_committed_state_row,
    ExactCommittedStateRow, ExactCommittedStateRowRequest,
};
use crate::contracts::artifacts::{
    ExactUntrackedLookupRequest, LiveQueryEffectiveRow, LiveQueryOverlayLane,
    TrackedTombstoneLookupRequest,
};
use crate::contracts::traits::{
    LiveStateQueryBackend, PendingSemanticRow, PendingSemanticStorage, PendingStateOverlay,
};
use crate::prepared_write_artifacts::{
    overlay_lanes_for_version, CanonicalStateRowKey, ExactEffectiveStateRow,
    ExactEffectiveStateRowRequest, OverlayLane,
};
use crate::version_artifacts::GLOBAL_VERSION_ID;
use crate::{LixBackend, LixError, Value};
enum TrackedExactEffectiveRowLookup {
    Matched(ExactEffectiveStateRow),
    Shadowed,
    Missing,
}

#[derive(Debug, Clone, PartialEq)]
struct WorkspaceAnnotatedTrackedExactRow {
    committed: ExactCommittedStateRow,
    writer_key: Option<String>,
}

pub(crate) async fn resolve_exact_effective_state_row_with_pending_overlay(
    backend: &dyn LixBackend,
    request: &ExactEffectiveStateRowRequest,
    pending_state_overlay: Option<&dyn PendingStateOverlay>,
) -> Result<Option<ExactEffectiveStateRow>, LixError> {
    let requested_untracked = request.row_key.untracked;
    let mut requested_global = request.row_key.global;
    if request.version_id == GLOBAL_VERSION_ID {
        if requested_global == Some(false) {
            return Ok(None);
        }
        requested_global = None;
    }

    let lanes = overlay_lanes_for_version(
        &request.version_id,
        request.include_global_overlay,
        request.include_untracked_overlay,
    );

    for lane in lanes {
        if !lane_matches_global_filter(lane, requested_global)
            || !lane_matches_untracked_filter(lane, requested_untracked)
        {
            continue;
        }

        let internal_version_id = if matches!(
            lane,
            OverlayLane::GlobalTracked | OverlayLane::GlobalUntracked
        ) {
            GLOBAL_VERSION_ID.to_string()
        } else {
            request.version_id.clone()
        };

        if let Some(row) = load_exact_pending_effective_row(
            backend,
            pending_state_overlay,
            request,
            &internal_version_id,
            lane,
        )
        .await?
        {
            return Ok(row);
        }

        match lane {
            OverlayLane::LocalTracked | OverlayLane::GlobalTracked => {
                match load_exact_tracked_effective_row(
                    backend,
                    pending_state_overlay,
                    request,
                    &internal_version_id,
                    lane,
                )
                .await?
                {
                    TrackedExactEffectiveRowLookup::Matched(row) => return Ok(Some(row)),
                    TrackedExactEffectiveRowLookup::Shadowed => return Ok(None),
                    TrackedExactEffectiveRowLookup::Missing => {}
                }
            }
            OverlayLane::LocalUntracked | OverlayLane::GlobalUntracked => {
                if let Some(row) =
                    load_exact_untracked_effective_row(backend, request, &internal_version_id, lane)
                        .await?
                {
                    return Ok(Some(row));
                }
            }
        }
    }

    Ok(None)
}

async fn load_exact_tracked_effective_row(
    backend: &dyn LixBackend,
    pending_state_overlay: Option<&dyn PendingStateOverlay>,
    request: &ExactEffectiveStateRowRequest,
    internal_version_id: &str,
    overlay_lane: OverlayLane,
) -> Result<TrackedExactEffectiveRowLookup, LixError> {
    let row = load_exact_committed_state_row(
        backend,
        &ExactCommittedStateRowRequest {
            entity_id: request.row_key.entity_id.clone(),
            schema_key: request.schema_key.clone(),
            version_id: internal_version_id.to_string(),
            exact_filters: request.row_key.committed_exact_filters(),
        },
    )
    .await?;

    if let Some(row) = row {
        let row = annotate_tracked_exact_row_with_workspace_writer_key(
            backend,
            pending_state_overlay,
            row,
        )
        .await?;
        if tracked_exact_row_matches_row_key(&row, &request.row_key) {
            return Ok(TrackedExactEffectiveRowLookup::Matched(
                exact_effective_state_row_from_tracked(row, &request.version_id, overlay_lane),
            ));
        }
        return Ok(TrackedExactEffectiveRowLookup::Shadowed);
    }

    if backend
        .tracked_tombstone_shadows_exact_row(&TrackedTombstoneLookupRequest {
            schema_key: request.schema_key.clone(),
            version_id: internal_version_id.to_string(),
            entity_id: request.row_key.entity_id.clone(),
            file_id: request.row_key.file_id.clone(),
            plugin_key: request.row_key.plugin_key.clone(),
            schema_version: request.row_key.schema_version.clone(),
        })
        .await?
    {
        return Ok(TrackedExactEffectiveRowLookup::Shadowed);
    }

    Ok(TrackedExactEffectiveRowLookup::Missing)
}

async fn load_exact_untracked_effective_row(
    backend: &dyn LixBackend,
    request: &ExactEffectiveStateRowRequest,
    version_id: &str,
    overlay_lane: OverlayLane,
) -> Result<Option<ExactEffectiveStateRow>, LixError> {
    backend
        .load_exact_untracked_effective_row(
            &ExactUntrackedLookupRequest {
                schema_key: request.schema_key.clone(),
                version_id: version_id.to_string(),
                entity_id: request.row_key.entity_id.clone(),
                file_id: request.row_key.file_id.clone(),
                plugin_key: request.row_key.plugin_key.clone(),
                schema_version: request.row_key.schema_version.clone(),
                writer_key: request.row_key.writer_key.clone(),
            },
            &request.version_id,
            live_state_overlay_lane(overlay_lane),
        )
        .await
        .map(|row| {
            row.map(|row| exact_effective_state_row_from_effective_untracked(row, overlay_lane))
        })
}

fn exact_effective_state_row_from_tracked(
    row: WorkspaceAnnotatedTrackedExactRow,
    requested_version_id: &str,
    overlay_lane: OverlayLane,
) -> ExactEffectiveStateRow {
    let WorkspaceAnnotatedTrackedExactRow {
        committed: row,
        writer_key,
    } = row;
    let projected_version_id = if matches!(overlay_lane, OverlayLane::GlobalTracked)
        && row.version_id == GLOBAL_VERSION_ID
    {
        requested_version_id.to_string()
    } else {
        row.version_id.clone()
    };
    let mut values = row.values;
    values.insert(
        "version_id".to_string(),
        Value::Text(projected_version_id.clone()),
    );
    values.insert(
        "writer_key".to_string(),
        writer_key.clone().map(Value::Text).unwrap_or(Value::Null),
    );
    ExactEffectiveStateRow {
        entity_id: row.entity_id.clone(),
        schema_key: row.schema_key.clone(),
        file_id: row.file_id.clone(),
        version_id: projected_version_id,
        values,
        source_change_id: row.source_change_id,
        overlay_lane,
    }
}

fn exact_effective_state_row_from_effective_untracked(
    row: LiveQueryEffectiveRow,
    overlay_lane: OverlayLane,
) -> ExactEffectiveStateRow {
    let mut values = row.values;
    values.insert("entity_id".to_string(), Value::Text(row.entity_id.clone()));
    values.insert(
        "schema_key".to_string(),
        Value::Text(row.schema_key.clone()),
    );
    values.insert("file_id".to_string(), Value::Text(row.file_id.clone()));
    values.insert(
        "version_id".to_string(),
        Value::Text(row.version_id.clone()),
    );
    if let Some(schema_version) = row.schema_version.as_ref() {
        values.insert(
            "schema_version".to_string(),
            Value::Text(schema_version.clone()),
        );
    }
    if let Some(plugin_key) = row.plugin_key.as_ref() {
        values.insert("plugin_key".to_string(), Value::Text(plugin_key.clone()));
    }
    values.insert(
        "metadata".to_string(),
        row.metadata.clone().map(Value::Text).unwrap_or(Value::Null),
    );
    values.insert(
        "writer_key".to_string(),
        row.writer_key
            .clone()
            .map(Value::Text)
            .unwrap_or(Value::Null),
    );
    values.insert("global".to_string(), Value::Boolean(row.global));
    values.insert("untracked".to_string(), Value::Boolean(row.untracked));
    ExactEffectiveStateRow {
        entity_id: row.entity_id,
        schema_key: row.schema_key,
        file_id: row.file_id,
        version_id: row.version_id,
        values,
        source_change_id: row.source_change_id,
        overlay_lane,
    }
}

fn live_state_overlay_lane(lane: OverlayLane) -> LiveQueryOverlayLane {
    match lane {
        OverlayLane::GlobalTracked => LiveQueryOverlayLane::GlobalTracked,
        OverlayLane::LocalTracked => LiveQueryOverlayLane::LocalTracked,
        OverlayLane::GlobalUntracked => LiveQueryOverlayLane::GlobalUntracked,
        OverlayLane::LocalUntracked => LiveQueryOverlayLane::LocalUntracked,
    }
}

async fn annotate_tracked_exact_row_with_workspace_writer_key(
    backend: &dyn LixBackend,
    pending_state_overlay: Option<&dyn PendingStateOverlay>,
    row: ExactCommittedStateRow,
) -> Result<WorkspaceAnnotatedTrackedExactRow, LixError> {
    let writer_key = if let Some(writer_key) = pending_workspace_writer_key_annotation(
        pending_state_overlay,
        &row.version_id,
        &row.schema_key,
        &row.entity_id,
        &row.file_id,
    ) {
        writer_key
    } else {
        load_workspace_writer_key_annotation_for_state_row(
            backend,
            &row.version_id,
            &row.schema_key,
            &row.entity_id,
            &row.file_id,
        )
        .await?
    };
    Ok(WorkspaceAnnotatedTrackedExactRow {
        committed: row,
        writer_key,
    })
}

fn pending_workspace_writer_key_annotation(
    pending_state_overlay: Option<&dyn PendingStateOverlay>,
    version_id: &str,
    schema_key: &str,
    entity_id: &str,
    file_id: &str,
) -> Option<Option<String>> {
    pending_state_overlay.and_then(|view| {
        view.workspace_writer_key_annotation_for_state_row(
            version_id, schema_key, entity_id, file_id,
        )
    })
}

fn tracked_exact_row_matches_row_key(
    row: &WorkspaceAnnotatedTrackedExactRow,
    row_key: &CanonicalStateRowKey,
) -> bool {
    row_key
        .writer_key
        .as_deref()
        .is_none_or(|writer_key| row.writer_key.as_deref() == Some(writer_key))
}

fn lane_matches_global_filter(lane: OverlayLane, requested_global: Option<bool>) -> bool {
    match requested_global {
        Some(true) => matches!(
            lane,
            OverlayLane::GlobalTracked | OverlayLane::GlobalUntracked
        ),
        Some(false) => matches!(
            lane,
            OverlayLane::LocalTracked | OverlayLane::LocalUntracked
        ),
        None => true,
    }
}

fn lane_matches_untracked_filter(lane: OverlayLane, requested_untracked: Option<bool>) -> bool {
    match requested_untracked {
        Some(true) => matches!(
            lane,
            OverlayLane::LocalUntracked | OverlayLane::GlobalUntracked
        ),
        Some(false) => matches!(lane, OverlayLane::LocalTracked | OverlayLane::GlobalTracked),
        None => true,
    }
}

fn row_key_text_value<'a>(row_key: &'a CanonicalStateRowKey, column: &str) -> Option<&'a str> {
    match column {
        "entity_id" => Some(row_key.entity_id.as_str()),
        "file_id" => row_key.file_id.as_deref(),
        "plugin_key" => row_key.plugin_key.as_deref(),
        "schema_version" => row_key.schema_version.as_deref(),
        "writer_key" => row_key.writer_key.as_deref(),
        _ => None,
    }
}

async fn load_exact_pending_effective_row(
    backend: &dyn LixBackend,
    pending_state_overlay: Option<&dyn PendingStateOverlay>,
    request: &ExactEffectiveStateRowRequest,
    internal_version_id: &str,
    overlay_lane: OverlayLane,
) -> Result<Option<Option<ExactEffectiveStateRow>>, LixError> {
    let storage = match overlay_lane {
        OverlayLane::LocalTracked | OverlayLane::GlobalTracked => PendingSemanticStorage::Tracked,
        OverlayLane::LocalUntracked | OverlayLane::GlobalUntracked => {
            PendingSemanticStorage::Untracked
        }
    };
    let Some(pending) = pending_state_overlay.and_then(|view| {
        view.visible_semantic_rows(storage, &request.schema_key)
            .into_iter()
            .find(|row| pending_row_matches_exact_request(row, request, internal_version_id))
    }) else {
        return Ok(None);
    };

    if pending.tombstone && matches!(storage, PendingSemanticStorage::Tracked) {
        return Ok(Some(None));
    }

    let row = exact_effective_state_row_from_pending(
        backend,
        pending_state_overlay,
        &pending,
        &request.version_id,
        overlay_lane,
    )
    .await?;
    if !pending_effective_row_matches_row_key(&row, &request.row_key) {
        return Ok(Some(None));
    }

    Ok(Some(Some(row)))
}

fn pending_row_matches_exact_request(
    row: &PendingSemanticRow,
    request: &ExactEffectiveStateRowRequest,
    internal_version_id: &str,
) -> bool {
    row.entity_id == request.row_key.entity_id
        && row.version_id == internal_version_id
        && row.file_id == row_key_text_value(&request.row_key, "file_id").unwrap_or(&row.file_id)
        && row.plugin_key
            == row_key_text_value(&request.row_key, "plugin_key").unwrap_or(&row.plugin_key)
        && row.schema_version
            == row_key_text_value(&request.row_key, "schema_version").unwrap_or(&row.schema_version)
}

async fn exact_effective_state_row_from_pending(
    backend: &dyn LixBackend,
    pending_state_overlay: Option<&dyn PendingStateOverlay>,
    row: &PendingSemanticRow,
    requested_version_id: &str,
    overlay_lane: OverlayLane,
) -> Result<ExactEffectiveStateRow, LixError> {
    let projected_version_id = if matches!(
        overlay_lane,
        OverlayLane::GlobalTracked | OverlayLane::GlobalUntracked
    ) && row.version_id == GLOBAL_VERSION_ID
    {
        requested_version_id.to_string()
    } else {
        row.version_id.clone()
    };
    let mut values = backend
        .normalize_live_snapshot_values(&row.schema_key, row.snapshot_content.as_deref())
        .await?;
    values.insert("entity_id".to_string(), Value::Text(row.entity_id.clone()));
    values.insert(
        "schema_key".to_string(),
        Value::Text(row.schema_key.clone()),
    );
    values.insert(
        "schema_version".to_string(),
        Value::Text(row.schema_version.clone()),
    );
    values.insert("file_id".to_string(), Value::Text(row.file_id.clone()));
    values.insert(
        "version_id".to_string(),
        Value::Text(projected_version_id.clone()),
    );
    values.insert(
        "plugin_key".to_string(),
        Value::Text(row.plugin_key.clone()),
    );
    values.insert(
        "metadata".to_string(),
        row.metadata.clone().map(Value::Text).unwrap_or(Value::Null),
    );
    let writer_key = pending_workspace_writer_key_annotation(
        pending_state_overlay,
        &row.version_id,
        &row.schema_key,
        &row.entity_id,
        &row.file_id,
    )
    .flatten();
    values.insert(
        "writer_key".to_string(),
        writer_key.map(Value::Text).unwrap_or(Value::Null),
    );

    Ok(ExactEffectiveStateRow {
        entity_id: row.entity_id.clone(),
        schema_key: row.schema_key.clone(),
        file_id: row.file_id.clone(),
        version_id: projected_version_id,
        values,
        source_change_id: Some("pending".to_string()),
        overlay_lane,
    })
}

fn pending_effective_row_matches_row_key(
    row: &ExactEffectiveStateRow,
    row_key: &CanonicalStateRowKey,
) -> bool {
    row_key.writer_key.as_deref().is_none_or(|writer_key| {
        matches!(
            row.values.get("writer_key"),
            Some(Value::Text(actual)) if actual == writer_key
        )
    })
}
