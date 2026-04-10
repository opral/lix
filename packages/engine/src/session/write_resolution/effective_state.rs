use crate::contracts::traits::{
    LiveStateQueryBackend, PendingSemanticRow, PendingSemanticStorage, PendingStateOverlay,
};
use crate::contracts::GLOBAL_VERSION_ID;
use crate::live_state::{load_exact_live_row, ExactLiveRowQuery, LiveRow, LiveRowSemantics};
use crate::session::write_resolution::prepared_artifacts::{
    overlay_lanes_for_version, CanonicalStateRowKey, ExactEffectiveStateRow,
    ExactEffectiveStateRowRequest, OverlayLane,
};
use crate::{LixBackend, LixError, Value};

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

    if let Some(row) = load_exact_pending_effective_row(
        backend,
        pending_state_overlay,
        request,
        requested_global,
        requested_untracked,
    )
    .await?
    {
        return Ok(row);
    }

    let Some(row) = load_exact_live_row(
        backend,
        &ExactLiveRowQuery {
            semantics: LiveRowSemantics::Effective,
            schema_key: request.schema_key.clone(),
            version_id: request.version_id.clone(),
            entity_id: request.row_key.entity_id.clone(),
            file_id: request.row_key.file_id.clone(),
            schema_version: request.row_key.schema_version.clone(),
            plugin_key: request.row_key.plugin_key.clone(),
            writer_key: request.row_key.writer_key.clone(),
            global: requested_global,
            untracked: requested_untracked,
            include_tombstones: false,
            include_global_overlay: request.include_global_overlay,
            include_untracked_overlay: request.include_untracked_overlay,
        },
    )
    .await?
    else {
        return Ok(None);
    };

    exact_effective_state_row_from_live_row(backend, row, &request.version_id)
        .await
        .map(Some)
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
    requested_global: Option<bool>,
    requested_untracked: Option<bool>,
) -> Result<Option<Option<ExactEffectiveStateRow>>, LixError> {
    let lanes = overlay_lanes_for_version(
        &request.version_id,
        request.include_global_overlay,
        request.include_untracked_overlay,
    );

    for overlay_lane in lanes {
        if !lane_matches_global_filter(overlay_lane, requested_global)
            || !lane_matches_untracked_filter(overlay_lane, requested_untracked)
        {
            continue;
        }

        let internal_version_id = if matches!(
            overlay_lane,
            OverlayLane::GlobalTracked | OverlayLane::GlobalUntracked
        ) {
            GLOBAL_VERSION_ID.to_string()
        } else {
            request.version_id.clone()
        };

        let storage = match overlay_lane {
            OverlayLane::LocalTracked | OverlayLane::GlobalTracked => {
                PendingSemanticStorage::Tracked
            }
            OverlayLane::LocalUntracked | OverlayLane::GlobalUntracked => {
                PendingSemanticStorage::Untracked
            }
        };
        let Some(pending) = pending_state_overlay.and_then(|view| {
            view.visible_semantic_rows(storage, &request.schema_key)
                .into_iter()
                .find(|row| pending_row_matches_exact_request(row, request, &internal_version_id))
        }) else {
            continue;
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

        return Ok(Some(Some(row)));
    }

    Ok(None)
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
        "snapshot_content".to_string(),
        row.snapshot_content
            .clone()
            .map(Value::Text)
            .unwrap_or(Value::Null),
    );
    values.insert(
        "metadata".to_string(),
        row.metadata.clone().map(Value::Text).unwrap_or(Value::Null),
    );
    let writer_key = pending_writer_key_annotation(
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

fn pending_writer_key_annotation(
    pending_state_overlay: Option<&dyn PendingStateOverlay>,
    version_id: &str,
    schema_key: &str,
    entity_id: &str,
    file_id: &str,
) -> Option<Option<String>> {
    pending_state_overlay.and_then(|view| {
        view.writer_key_annotation_for_state_row(version_id, schema_key, entity_id, file_id)
    })
}

async fn exact_effective_state_row_from_live_row(
    backend: &dyn LixBackend,
    row: LiveRow,
    requested_version_id: &str,
) -> Result<ExactEffectiveStateRow, LixError> {
    let overlay_lane = overlay_lane_from_live_row(&row);
    let projected_version_id = if row.global && row.version_id == GLOBAL_VERSION_ID {
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
        "snapshot_content".to_string(),
        row.snapshot_content
            .clone()
            .map(Value::Text)
            .unwrap_or(Value::Null),
    );
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

    Ok(ExactEffectiveStateRow {
        entity_id: row.entity_id,
        schema_key: row.schema_key,
        file_id: row.file_id,
        version_id: projected_version_id,
        values,
        source_change_id: row.change_id,
        overlay_lane,
    })
}

fn overlay_lane_from_live_row(row: &LiveRow) -> OverlayLane {
    match (row.global, row.untracked) {
        (true, true) => OverlayLane::GlobalUntracked,
        (true, false) => OverlayLane::GlobalTracked,
        (false, true) => OverlayLane::LocalUntracked,
        (false, false) => OverlayLane::LocalTracked,
    }
}
