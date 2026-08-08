use crate::LixError;
use crate::NullableKeyFilter;
use crate::commit_graph::CommitGraphContext;
use crate::functions::{DeterministicMode, DeterministicSequence};
use crate::live_state::{LiveStateContext, LiveStateRowRequest, MaterializedLiveStateRow};
use crate::storage_adapter::{StorageAdapterRead, StoragePrecondition, StorageWriteSet};
use crate::tracked_state::TrackedStateContext;
use bytes::Bytes;
use serde_json::Value as JsonValue;

pub(crate) const DETERMINISTIC_MODE_KEY: &str = "lix_deterministic_mode";
pub(crate) const DETERMINISTIC_SEQUENCE_KEY: &str = "lix_deterministic_sequence_number";

const KEY_VALUE_SCHEMA_KEY: &str = "lix_key_value";

/// Loads deterministic-mode settings from the canonical untracked current
/// state member.
///
/// Missing mode means deterministic execution is disabled. Malformed mode rows
/// are errors because they would make runtime function behavior ambiguous. This
/// is engine-owned global state and has no changelog or commit history.
pub(crate) async fn load_mode(
    read: &(impl StorageAdapterRead + ?Sized),
) -> Result<DeterministicMode, LixError> {
    let Some(row) = load_key_value_row(read, DETERMINISTIC_MODE_KEY).await? else {
        return Ok(DeterministicMode::disabled());
    };
    let value = key_value_payload(&row, DETERMINISTIC_MODE_KEY)?;
    parse_mode_value(value)
}

/// Loads the persisted deterministic sequence position.
///
/// Missing sequence means no deterministic values have been produced yet, so
/// execution starts at sequence zero.
pub(crate) async fn load_sequence(
    read: &(impl StorageAdapterRead + ?Sized),
) -> Result<DeterministicSequence, LixError> {
    let Some(row) = load_key_value_row(read, DETERMINISTIC_SEQUENCE_KEY).await? else {
        return Ok(DeterministicSequence::uninitialized());
    };
    let value = key_value_payload(&row, DETERMINISTIC_SEQUENCE_KEY)?;
    parse_sequence_value(value)
}

/// Persists the highest deterministic sequence value used by an execution.
///
/// The row is untracked global `lix_key_value` current state. It never enters
/// the changelog or commit graph.
pub(crate) async fn stage_sequence(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    sequence: DeterministicSequence,
    timestamp: crate::common::LixTimestamp,
    _change_id: crate::changelog::ChangeId,
) -> Result<StoragePrecondition, LixError> {
    let entity_pk = crate::entity_pk::EntityPk::single(DETERMINISTIC_SEQUENCE_KEY);
    let key = crate::forktree::encode_untracked_key(
        crate::forktree::CanonicalBranchId::from_bytes(
            *uuid::Uuid::parse_str(crate::GLOBAL_BRANCH_ID)
                .expect("global branch ID is canonical")
                .as_bytes(),
        ),
        crate::forktree::StateKeyRef {
            schema_key: KEY_VALUE_SCHEMA_KEY,
            file_id: None,
            entity_pk: &entity_pk,
        },
    );
    let snapshot = serde_json::to_string(&serde_json::json!({
        "key": DETERMINISTIC_SEQUENCE_KEY,
        "value": sequence.highest_seen,
    }))
    .expect("deterministic sequence snapshot is serializable");
    let value = crate::forktree::encode_untracked_value(crate::forktree::UntrackedValueRef {
        created_at: timestamp,
        updated_at: timestamp,
        cell: crate::forktree::StateCellRef::Value(&snapshot),
        metadata: None,
        origin_key: None,
        blob_manifest_object_ids: &[],
    })?;
    let storage_key = crate::storage_adapter::StorageKey(Bytes::from(key));
    let current = crate::storage_adapter::PointReadPlan::new(
        crate::forktree::UNTRACKED_ROW_SPACE,
        std::slice::from_ref(&storage_key),
    )
    .materialize(read, crate::storage_adapter::StorageGetOptions::default())
    .await?
    .value
    .into_iter()
    .next()
    .flatten();
    let precondition = match current {
        Some(crate::storage_adapter::StorageProjectedValue::FullValue(expected)) => {
            StoragePrecondition::KeyValueEquals {
                space: crate::forktree::UNTRACKED_ROW_SPACE,
                key: storage_key.clone(),
                expected,
            }
        }
        Some(crate::storage_adapter::StorageProjectedValue::KeyOnly) => {
            return Err(LixError::new(
                LixError::CODE_STORAGE_ERROR,
                "deterministic sequence owner read returned key-only data",
            ));
        }
        None => StoragePrecondition::KeyAbsent {
            space: crate::forktree::UNTRACKED_ROW_SPACE,
            key: storage_key.clone(),
        },
    };
    writes.put(crate::forktree::UNTRACKED_ROW_SPACE, storage_key, value);
    Ok(precondition)
}

async fn load_key_value_row(
    read: &(impl StorageAdapterRead + ?Sized),
    key: &str,
) -> Result<Option<MaterializedLiveStateRow>, LixError> {
    LiveStateContext::new(TrackedStateContext::new(), CommitGraphContext::new())
        .reader(read)
        .load_row(&LiveStateRowRequest {
            schema_key: KEY_VALUE_SCHEMA_KEY.to_string(),
            branch_id: crate::GLOBAL_BRANCH_ID.to_string(),
            entity_pk: crate::entity_pk::EntityPk::single(key),
            file_id: NullableKeyFilter::Null,
        })
        .await
}

fn key_value_payload(row: &MaterializedLiveStateRow, key: &str) -> Result<JsonValue, LixError> {
    let snapshot_content = row.snapshot_content.as_deref().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("deterministic key-value row '{key}' is missing snapshot_content"),
        )
    })?;
    let snapshot = serde_json::from_str::<JsonValue>(snapshot_content).map_err(|error| {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_mode_payload_defaults_to_disabled_without_a_legacy_state_owner() {
        assert_eq!(
            parse_mode_value(serde_json::json!({})).expect("missing mode should decode"),
            DeterministicMode::disabled()
        );
    }

    #[test]
    fn deterministic_sequence_rejects_non_integer_payloads_fail_closed() {
        for value in [
            JsonValue::Null,
            JsonValue::String("7".to_owned()),
            serde_json::json!({ "highest_seen": 7 }),
        ] {
            let error = parse_sequence_value(value)
                .expect_err("a non-integer sequence payload must fail closed");
            assert_eq!(error.code, LixError::CODE_UNKNOWN);
            assert!(error.message.contains("must be an integer"));
        }
    }

    #[test]
    fn deterministic_mode_rejects_non_object_payloads_fail_closed() {
        for value in [
            JsonValue::Null,
            JsonValue::Bool(true),
            JsonValue::String("on".to_owned()),
        ] {
            let error =
                parse_mode_value(value).expect_err("a non-object mode payload must fail closed");
            assert_eq!(error.code, LixError::CODE_UNKNOWN);
            assert!(error.message.contains("must be an object"));
        }
    }
}
