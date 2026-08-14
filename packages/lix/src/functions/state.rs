use crate::LixError;
use crate::functions::{DeterministicMode, DeterministicSequence};
use crate::state::{ForkTreeStateView, StateRow};
use crate::storage_adapter::StorageAdapterRead;
use serde_json::Value as JsonValue;

pub(crate) const DETERMINISTIC_MODE_KEY: &str = "lix_deterministic_mode";
pub(crate) const DETERMINISTIC_SEQUENCE_KEY: &str = "lix_deterministic_sequence_number";
pub(crate) const DETERMINISTIC_SEQUENCE_INITIALIZED_KEY: &str =
    "lix_deterministic_sequence_initialized";

const KEY_VALUE_SCHEMA_KEY: &str = "lix_key_value";

/// Loads deterministic-mode settings from the authenticated global state.
///
/// Missing mode means deterministic execution is disabled. Malformed mode rows
/// are errors because they would make runtime function behavior ambiguous. This
/// is engine-owned global state and has no changelog or commit history.
pub(crate) async fn load_mode<R>(
    state: &ForkTreeStateView<R>,
) -> Result<DeterministicMode, LixError>
where
    R: StorageAdapterRead,
{
    let rows = load_key_value_rows(state).await?;
    let Some(row) = rows.iter().find(|row| {
        !row.value.cell.deleted()
            && state_row_key(row).ok().is_some_and(|key| {
                key.row_pk
                    .as_single_string()
                    .ok()
                    .is_some_and(|value| value == DETERMINISTIC_MODE_KEY)
            })
    }) else {
        return Ok(DeterministicMode::disabled());
    };
    let value = key_value_payload(&row, DETERMINISTIC_MODE_KEY)?;
    parse_mode_value(value)
}

/// Loads the persisted deterministic sequence position.
///
/// Missing sequence means no deterministic values have been produced yet, so
/// execution starts at sequence zero.
pub(crate) async fn load_sequence<R>(
    state: &ForkTreeStateView<R>,
) -> Result<DeterministicSequence, LixError>
where
    R: StorageAdapterRead,
{
    let rows = load_key_value_rows(state).await?;
    let sequence = rows.iter().find(|row| {
        !row.value.cell.deleted()
            && state_row_key(row).ok().is_some_and(|key| {
                key.row_pk
                    .as_single_string()
                    .ok()
                    .is_some_and(|value| value == DETERMINISTIC_SEQUENCE_KEY)
            })
    });
    if let Some(row) = sequence {
        return parse_sequence_value(key_value_payload(row, DETERMINISTIC_SEQUENCE_KEY)?);
    }

    let initialized = rows.iter().find(|row| {
        state_row_key(row).ok().is_some_and(|key| {
            key.row_pk
                .as_single_string()
                .ok()
                .is_some_and(|value| value == DETERMINISTIC_SEQUENCE_INITIALIZED_KEY)
        })
    });
    let Some(initialized) = initialized else {
        return Ok(DeterministicSequence::uninitialized());
    };
    if initialized.value.cell.deleted() {
        return Err(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            "deterministic sequence initialization marker was deleted",
        ));
    }
    let marker = key_value_payload(initialized, DETERMINISTIC_SEQUENCE_INITIALIZED_KEY)?;
    if marker != JsonValue::Bool(true) {
        return Err(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            "deterministic sequence initialization marker is invalid",
        ));
    }
    Err(LixError::new(
        LixError::CODE_STORAGE_ERROR,
        "deterministic sequence member is missing after initialization",
    ))
}

async fn load_key_value_rows<R>(state: &ForkTreeStateView<R>) -> Result<Vec<StateRow>, LixError>
where
    R: StorageAdapterRead,
{
    let empty_row_pk = crate::row_pk::RowPk {
        components: crate::row_pk::RowPkComponents::Empty,
    };
    let lower = crate::forktree::encode_state_row_prefix(KEY_VALUE_SCHEMA_KEY, &empty_row_pk);
    let upper = crate::forktree::exclusive_prefix_upper_bound(&lower);
    let rows = state
        .branch_range(
            crate::GLOBAL_BRANCH_ID,
            Some(&lower),
            upper.as_deref(),
            None,
            true,
        )
        .await
        .map_err(LixError::from)?;
    let mut identities = std::collections::BTreeSet::new();
    for row in &rows {
        let state_key = state_row_key(row)?;
        if state_key.schema_key != KEY_VALUE_SCHEMA_KEY || state_key.file_id.is_some() {
            return Err(LixError::new(
                LixError::CODE_STORAGE_ERROR,
                "deterministic key-value scan returned a row outside its authenticated owner",
            ));
        }
        let key = state_key.row_pk.as_single_string().map_err(|error| {
            LixError::new(
                LixError::CODE_STORAGE_ERROR,
                format!("deterministic key-value row has an invalid identity: {error}"),
            )
        })?;
        if !identities.insert(key.to_owned()) {
            return Err(LixError::new(
                LixError::CODE_STORAGE_ERROR,
                format!("deterministic key-value row '{key}' has a duplicate identity"),
            ));
        }
        let snapshot_content = row.seed_logical_snapshot(crate::GLOBAL_BRANCH_ID)?;
        if row.value.cell.deleted() {
            if snapshot_content.is_some()
                && matches!(
                    key,
                    DETERMINISTIC_MODE_KEY
                        | DETERMINISTIC_SEQUENCE_KEY
                        | DETERMINISTIC_SEQUENCE_INITIALIZED_KEY
                )
            {
                return Err(LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    format!("deterministic key-value tombstone '{key}' carries a payload"),
                ));
            }
        } else if let Some(snapshot_content) = snapshot_content {
            let snapshot =
                serde_json::from_str::<JsonValue>(&snapshot_content).map_err(|error| {
                    LixError::new(
                        LixError::CODE_STORAGE_ERROR,
                        format!("deterministic key-value row '{key}' has invalid JSON: {error}"),
                    )
                })?;
            let stored_key = snapshot.get("key").and_then(JsonValue::as_str);
            if matches!(
                key,
                DETERMINISTIC_MODE_KEY
                    | DETERMINISTIC_SEQUENCE_KEY
                    | DETERMINISTIC_SEQUENCE_INITIALIZED_KEY
            ) || matches!(
                stored_key,
                Some(
                    DETERMINISTIC_MODE_KEY
                        | DETERMINISTIC_SEQUENCE_KEY
                        | DETERMINISTIC_SEQUENCE_INITIALIZED_KEY
                )
            ) {
                if stored_key != Some(key) {
                    return Err(LixError::new(
                        LixError::CODE_STORAGE_ERROR,
                        format!("deterministic key-value row '{key}' has mismatched key field"),
                    ));
                }
                key_value_payload(row, key)?;
            }
        }
    }
    Ok(rows)
}

fn state_row_key(row: &StateRow) -> Result<crate::forktree::StateKey, LixError> {
    crate::forktree::decode_state_key(&row.key).map_err(LixError::from)
}

fn key_value_payload(row: &StateRow, key: &str) -> Result<JsonValue, LixError> {
    let snapshot_content = row
        .seed_logical_snapshot(crate::GLOBAL_BRANCH_ID)?
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("deterministic key-value row '{key}' is missing snapshot_content"),
            )
        })?;
    let snapshot = serde_json::from_str::<JsonValue>(&snapshot_content).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("deterministic key-value row '{key}' has invalid JSON: {error}"),
        )
    })?;
    let stored_key = snapshot.get("key").and_then(JsonValue::as_str);
    if stored_key != Some(key) {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("deterministic key-value row '{key}' has mismatched key field"),
        ));
    }
    snapshot.get("value").cloned().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("deterministic key-value row '{key}' is missing value"),
        )
    })
}

fn parse_mode_value(value: JsonValue) -> Result<DeterministicMode, LixError> {
    let Some(object) = value.as_object() else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "deterministic mode value must be an object",
        ));
    };

    let enabled = object
        .get("enabled")
        .and_then(JsonValue::as_bool)
        .unwrap_or(false);
    if !enabled {
        return Ok(DeterministicMode::disabled());
    }
    let timestamp_shuffle = object
        .get("timestamp_shuffle")
        .and_then(JsonValue::as_bool)
        .unwrap_or(false);
    Ok(DeterministicMode {
        enabled,
        timestamp_shuffle,
    })
}

fn parse_sequence_value(value: JsonValue) -> Result<DeterministicSequence, LixError> {
    let Some(highest_seen) = value.as_i64() else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "deterministic sequence value must be an integer",
        ));
    };
    Ok(DeterministicSequence { highest_seen })
}
