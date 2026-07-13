use serde_json::Value as JsonValue;
use std::sync::Arc;

use crate::GLOBAL_BRANCH_ID;
use crate::changelog::{
    ChangeId, ChangeRecord, ChangelogAppend, ChangelogContext, ChangelogWriter,
};
use crate::common::LixTimestamp;
use crate::entity_pk::EntityPk;
use crate::functions::{DeterministicMode, DeterministicSequence};
use crate::json_store::NormalizedJson;
use crate::live_state::index::{
    LiveStateIndexContext, LiveStateIndexDeltaRef, LiveStateIndexRow, LiveStateIndexRowRequest,
};
use crate::live_state::{LiveStateReader, LiveStateRowRequest, MaterializedLiveStateRow};
use crate::storage::StorageRead;
use crate::storage::StorageWriteSet;
use crate::{LixError, NullableKeyFilter};

pub(crate) const DETERMINISTIC_MODE_KEY: &str = "lix_deterministic_mode";
pub(crate) const DETERMINISTIC_SEQUENCE_KEY: &str = "lix_deterministic_sequence_number";

const KEY_VALUE_SCHEMA_KEY: &str = "lix_key_value";

/// Loads deterministic-mode settings from visible live state.
///
/// Missing mode means deterministic execution is disabled. Malformed mode rows
/// are errors because they would make runtime function behavior ambiguous.
pub(crate) async fn load_mode(
    live_state: &dyn LiveStateReader,
) -> Result<DeterministicMode, LixError> {
    let Some(row) = load_key_value_row(live_state, DETERMINISTIC_MODE_KEY).await? else {
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
    live_state: &dyn LiveStateReader,
) -> Result<DeterministicSequence, LixError> {
    let Some(row) = load_key_value_row(live_state, DETERMINISTIC_SEQUENCE_KEY).await? else {
        return Ok(DeterministicSequence::uninitialized());
    };
    let value = key_value_payload(&row, DETERMINISTIC_SEQUENCE_KEY)?;
    parse_sequence_value(value)
}

/// Persists the highest deterministic sequence value used by an execution.
///
/// The row is untracked global `lix_key_value` state: it is a real changelog
/// fact, but is not retained by commit membership.
pub(crate) async fn stage_sequence(
    read: &(impl StorageRead + Send + Sync + ?Sized),
    writes: &mut StorageWriteSet,
    sequence: DeterministicSequence,
    timestamp: LixTimestamp,
    change_id: ChangeId,
) -> Result<(), LixError> {
    let snapshot_content = serde_json::to_string(&serde_json::json!({
        "key": DETERMINISTIC_SEQUENCE_KEY,
        "value": sequence.highest_seen,
    }))
    .map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("deterministic sequence snapshot serialization failed: {error}"),
        )
    })?;
    let snapshot = NormalizedJson::from_arc_unchecked(Arc::from(snapshot_content.as_str()));
    let entity_pk = EntityPk::single(DETERMINISTIC_SEQUENCE_KEY);
    let request = LiveStateIndexRowRequest {
        branch_id: GLOBAL_BRANCH_ID.to_string(),
        schema_key: KEY_VALUE_SCHEMA_KEY.to_string(),
        entity_pk: entity_pk.clone(),
        file_id: None,
    };
    let previous = LiveStateIndexContext::new()
        .reader(read)
        .load_index_row(&request)
        .await?;
    if previous
        .as_ref()
        .is_some_and(|row| row.change_id == change_id)
    {
        return Ok(());
    }
    let change = ChangeRecord {
        format_version: 2,
        change_id,
        schema_key: KEY_VALUE_SCHEMA_KEY.to_string(),
        entity_pk: entity_pk.clone(),
        file_id: None,
        snapshot: crate::json_store::JsonSlot::from_json(snapshot.as_str()),
        metadata: crate::json_store::JsonSlot::None,
        created_at: timestamp,
        origin_key: None,
    };
    {
        let mut changelog_read = read;
        let mut changelog = ChangelogContext::new().writer(&mut changelog_read, writes);
        if let Some(previous) = previous.filter(LiveStateIndexRow::untracked) {
            changelog
                .stage_delete_standalone_changes(&[previous.change_id])
                .await?;
        }
        changelog
            .stage_append(ChangelogAppend {
                changes: vec![change],
                ..ChangelogAppend::default()
            })
            .await?;
    }
    LiveStateIndexContext::new()
        .writer(read, writes)
        .stage_branch_rows(
            GLOBAL_BRANCH_ID,
            [LiveStateIndexDeltaRef {
                schema_key: KEY_VALUE_SCHEMA_KEY,
                file_id: None,
                entity_pk: &entity_pk,
                change_id,
                commit_id: None,
                deleted: false,
                created_at: timestamp,
                updated_at: timestamp,
            }],
        )
        .await?;
    Ok(())
}

async fn load_key_value_row(
    live_state: &dyn LiveStateReader,
    key: &str,
) -> Result<Option<MaterializedLiveStateRow>, LixError> {
    live_state
        .load_row(&LiveStateRowRequest {
            schema_key: KEY_VALUE_SCHEMA_KEY.to_string(),
            branch_id: GLOBAL_BRANCH_ID.to_string(),
            entity_pk: EntityPk::single(key),
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
    use crate::live_state::index::LiveStateIndexContext;
    use crate::live_state::{LiveStateContext, LiveStateRowRequest};
    use crate::storage::StorageContext;
    use crate::storage::{InMemoryStorageBackend, StorageReadOptions, StorageWriteOptions};

    use super::*;

    fn live_state_context() -> LiveStateContext {
        LiveStateContext::new(
            crate::tracked_state::TrackedStateContext::new(),
            LiveStateIndexContext::new(),
            crate::commit_graph::CommitGraphContext::new(),
        )
    }

    #[tokio::test]
    async fn missing_mode_is_disabled() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let live_state = live_state_context();
        let reader = live_state.reader(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("read should open"),
        );

        let mode = load_mode(&reader)
            .await
            .expect("missing mode should decode");

        assert_eq!(mode, DeterministicMode::disabled());
    }

    #[tokio::test]
    async fn valid_mode_decodes_flags() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let live_state = live_state_context();
        crate::test_support::seed_global_branch_head(storage.clone()).await;
        write_test_key_value(
            storage.clone(),
            DETERMINISTIC_MODE_KEY,
            serde_json::json!({
                "enabled": true,
                "timestamp_shuffle": true,
            }),
        )
        .await;

        let reader = live_state.reader(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("read should open"),
        );
        let mode = load_mode(&reader).await.expect("valid mode should decode");

        assert_eq!(
            mode,
            DeterministicMode {
                enabled: true,
                timestamp_shuffle: true,
            }
        );
    }

    #[tokio::test]
    async fn missing_sequence_is_uninitialized() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let live_state = live_state_context();
        let reader = live_state.reader(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("read should open"),
        );

        let sequence = load_sequence(&reader)
            .await
            .expect("missing sequence should decode");

        assert_eq!(sequence, DeterministicSequence::uninitialized());
    }

    #[tokio::test]
    async fn valid_sequence_decodes_highest_seen() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let live_state = live_state_context();
        crate::test_support::seed_global_branch_head(storage.clone()).await;
        write_test_key_value(
            storage.clone(),
            DETERMINISTIC_SEQUENCE_KEY,
            serde_json::json!(41),
        )
        .await;

        let reader = live_state.reader(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("read should open"),
        );
        let sequence = load_sequence(&reader)
            .await
            .expect("valid sequence should decode");

        assert_eq!(sequence, DeterministicSequence { highest_seen: 41 });
        assert_eq!(sequence.next_sequence(), 42);
    }

    #[tokio::test]
    async fn write_sequence_persists_untracked_global_key_value() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let live_state = live_state_context();
        crate::test_support::seed_global_branch_head(storage.clone()).await;

        let mut writes = storage.new_write_set();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        stage_sequence(
            &read,
            &mut writes,
            DeterministicSequence { highest_seen: 7 },
            test_timestamp(),
            ChangeId::for_test_label("sequence-change-7"),
        )
        .await
        .expect("sequence should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("sequence should commit");

        let reader = live_state.reader(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("read should open"),
        );
        let row = reader
            .load_row(&LiveStateRowRequest {
                schema_key: KEY_VALUE_SCHEMA_KEY.to_string(),
                branch_id: GLOBAL_BRANCH_ID.to_string(),
                entity_pk: EntityPk::single(DETERMINISTIC_SEQUENCE_KEY),
                file_id: NullableKeyFilter::Null,
            })
            .await
            .expect("sequence row should load")
            .expect("sequence row should exist");
        assert!(row.untracked);
        assert!(row.global);
        assert_eq!(
            row.change_id,
            Some(ChangeId::for_test_label("sequence-change-7"))
        );
        assert_eq!(row.commit_id, None);
        assert_eq!(
            row.snapshot_content.as_deref(),
            Some("{\"key\":\"lix_deterministic_sequence_number\",\"value\":7}")
        );
    }

    async fn write_test_key_value(storage: StorageContext, key: &str, value: JsonValue) {
        let snapshot_content = serde_json::to_string(&serde_json::json!({
            "key": key,
            "value": value,
        }))
        .expect("snapshot should serialize");
        let entity_pk = EntityPk::single(key);
        let change_id = ChangeId::for_test_label(&format!("test-key-value-{key}"));
        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut writes = storage.new_write_set();
        {
            let mut changelog_read = &read;
            ChangelogContext::new()
                .writer(&mut changelog_read, &mut writes)
                .stage_append(ChangelogAppend {
                    changes: vec![ChangeRecord {
                        format_version: 2,
                        change_id,
                        schema_key: KEY_VALUE_SCHEMA_KEY.to_string(),
                        entity_pk: entity_pk.clone(),
                        file_id: None,
                        snapshot: crate::json_store::JsonSlot::from_json(&snapshot_content),
                        metadata: crate::json_store::JsonSlot::None,
                        created_at: test_timestamp(),
                        origin_key: None,
                    }],
                    ..ChangelogAppend::default()
                })
                .await
                .expect("test key-value change should stage");
        }
        LiveStateIndexContext::new()
            .writer(&read, &mut writes)
            .stage_branch_rows(
                GLOBAL_BRANCH_ID,
                [LiveStateIndexDeltaRef {
                    schema_key: KEY_VALUE_SCHEMA_KEY,
                    file_id: None,
                    entity_pk: &entity_pk,
                    change_id,
                    commit_id: None,
                    deleted: false,
                    created_at: test_timestamp(),
                    updated_at: test_timestamp(),
                }],
            )
            .await
            .expect("test key-value current row should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("test key-value should commit");
    }

    fn test_timestamp() -> LixTimestamp {
        LixTimestamp::expect_parse("timestamp", "1970-01-01T00:00:00.000Z")
    }
}
