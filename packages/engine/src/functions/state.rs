use serde_json::Value as JsonValue;
use std::sync::Arc;

use crate::GLOBAL_BRANCH_ID;
use crate::LixError;
use crate::branch::{
    BranchHeadControlContext, branch_head_control_precondition, stage_branch_head_control,
};
use crate::changelog::ChangeId;
use crate::common::LixTimestamp;
use crate::entity_pk::EntityPk;
use crate::functions::{DeterministicMode, DeterministicSequence};
use crate::json_store::{
    JsonSlot, JsonStoreContext, JsonWritePlacementRef, NormalizedJson, NormalizedJsonRef,
};
use crate::live_state::{CurrentStateDeltaRef, MaterializedLiveStateRow};
use crate::storage_adapter::{StorageAdapterRead, StoragePrecondition, StorageWriteSet};

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
    timestamp: LixTimestamp,
    _change_id: ChangeId,
) -> Result<StoragePrecondition, LixError> {
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
    let mut observations = BranchHeadControlContext::new()
        .reader(read)
        .load_observed(&[GLOBAL_BRANCH_ID.to_string()])
        .await?;
    let observation = observations.pop().expect("one global control observation");
    let control = observation.control.ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "global branch control is missing while staging deterministic state",
        )
    })?;
    JsonStoreContext::new().writer().stage_batch(
        writes,
        JsonWritePlacementRef::OutOfBand,
        [NormalizedJsonRef::from(&snapshot)],
    )?;
    let snapshot_slot = JsonSlot::from_json(snapshot.as_str());
    let control = crate::live_state::stage_untracked_deltas(
        read,
        writes,
        GLOBAL_BRANCH_ID,
        control,
        &[CurrentStateDeltaRef {
            schema_key: KEY_VALUE_SCHEMA_KEY,
            file_id: None,
            entity_pk: &entity_pk,
            change_id: None,
            commit_id: None,
            untracked: true,
            deleted: false,
            created_at: timestamp,
            updated_at: timestamp,
            snapshot: snapshot_slot.as_ref_slot(),
            metadata: crate::json_store::JsonSlotRef::None,
            columnar_base_coordinate: None,
        }],
        &[false],
    )
    .await?;
    // The hot-state mutation is fenced by an actual control-byte
    // change. Merely restaging the old control would let two writers both
    // satisfy the same CAS after the first write, losing one group update.
    stage_branch_head_control(writes, GLOBAL_BRANCH_ID, control)?;
    branch_head_control_precondition(GLOBAL_BRANCH_ID, observation.raw_token)
}

async fn load_key_value_row(
    read: &(impl StorageAdapterRead + ?Sized),
    key: &str,
) -> Result<Option<MaterializedLiveStateRow>, LixError> {
    let request = crate::live_state::LiveStateExactBatchRequest {
        rows: vec![crate::live_state::LiveStateExactRowRequest {
            schema_key: KEY_VALUE_SCHEMA_KEY.to_string(),
            branch_id: GLOBAL_BRANCH_ID.to_string(),
            entity_pk: EntityPk::single(key),
            file_id: None,
        }],
        projection: crate::live_state::LiveStateProjection {
            columns: vec!["snapshot_content".to_string()],
        },
        untracked: Some(true),
        include_tombstones: false,
    };
    let visible_branch_ids = std::collections::BTreeSet::from([GLOBAL_BRANCH_ID.to_string()]);
    let rows =
        crate::live_state::load_untracked_exact_batch(read, &request, &visible_branch_ids).await?;
    Ok(rows.row(0).map(|row| row.to_owned()))
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
    use crate::NullableKeyFilter;
    use crate::live_state::{LiveStateContext, LiveStateRowRequest};
    use crate::storage_adapter::StorageAdapter;
    use crate::storage_adapter::{Memory, StorageReadOptions, StorageWriteOptions};

    use super::*;

    fn live_state_context() -> LiveStateContext {
        LiveStateContext::new(
            crate::tracked_state::TrackedStateContext::new(),
            crate::commit_graph::CommitGraphContext::new(),
        )
    }

    #[tokio::test]
    async fn missing_mode_is_disabled() {
        let storage = StorageAdapter::new(Memory::new());
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");

        let mode = load_mode(&read).await.expect("missing mode should decode");

        assert_eq!(mode, DeterministicMode::disabled());
    }

    #[tokio::test]
    async fn valid_mode_decodes_flags() {
        let storage = StorageAdapter::new(Memory::new());
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

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mode = load_mode(&read).await.expect("valid mode should decode");

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
        let storage = StorageAdapter::new(Memory::new());
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");

        let sequence = load_sequence(&read)
            .await
            .expect("missing sequence should decode");

        assert_eq!(sequence, DeterministicSequence::uninitialized());
    }

    #[tokio::test]
    async fn valid_sequence_decodes_highest_seen() {
        let storage = StorageAdapter::new(Memory::new());
        crate::test_support::seed_global_branch_head(storage.clone()).await;
        write_test_key_value(
            storage.clone(),
            DETERMINISTIC_SEQUENCE_KEY,
            serde_json::json!(41),
        )
        .await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let sequence = load_sequence(&read)
            .await
            .expect("valid sequence should decode");

        assert_eq!(sequence, DeterministicSequence { highest_seen: 41 });
        assert_eq!(sequence.next_sequence(), 42);
    }

    #[tokio::test]
    async fn write_sequence_persists_untracked_global_key_value() {
        let storage = StorageAdapter::new(Memory::new());
        let live_state = live_state_context();
        crate::test_support::seed_global_branch_head(storage.clone()).await;

        let mut writes = storage.new_write_set();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
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

        let updated_at = LixTimestamp::expect_parse("timestamp", "1970-01-01T00:00:01.000Z");
        let mut writes = storage.new_write_set();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("update read should open");
        stage_sequence(
            &read,
            &mut writes,
            DeterministicSequence { highest_seen: 8 },
            updated_at,
            ChangeId::for_test_label("sequence-change-8"),
        )
        .await
        .expect("sequence update should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("sequence update should commit");

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
        assert_eq!(row.change_id, None);
        assert_eq!(row.commit_id, None);
        assert_eq!(row.created_at, test_timestamp());
        assert_eq!(row.updated_at, updated_at);
        assert_eq!(
            row.snapshot_content.as_deref(),
            Some("{\"key\":\"lix_deterministic_sequence_number\",\"value\":8}")
        );
    }

    async fn write_test_key_value(storage: StorageAdapter, key: &str, value: JsonValue) {
        let snapshot_content = serde_json::to_string(&serde_json::json!({
            "key": key,
            "value": value,
        }))
        .expect("snapshot should serialize");
        let entity_pk = EntityPk::single(key);
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut writes = storage.new_write_set();
        let control = BranchHeadControlContext::new()
            .reader(&read)
            .load(GLOBAL_BRANCH_ID)
            .await
            .expect("global control should load")
            .expect("global control should exist");
        let snapshot = JsonSlot::from_json(&snapshot_content);
        let control = crate::live_state::stage_untracked_deltas(
            &read,
            &mut writes,
            GLOBAL_BRANCH_ID,
            control,
            &[CurrentStateDeltaRef {
                schema_key: KEY_VALUE_SCHEMA_KEY,
                file_id: None,
                entity_pk: &entity_pk,
                change_id: None,
                commit_id: None,
                untracked: true,
                deleted: false,
                created_at: test_timestamp(),
                updated_at: test_timestamp(),
                snapshot: snapshot.as_ref_slot(),
                metadata: crate::json_store::JsonSlotRef::None,
                columnar_base_coordinate: None,
            }],
            &[false],
        )
        .await
        .expect("test key-value current row should stage");
        stage_branch_head_control(&mut writes, GLOBAL_BRANCH_ID, control)
            .expect("test global control should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("test key-value should commit");
    }

    fn test_timestamp() -> LixTimestamp {
        LixTimestamp::expect_parse("timestamp", "1970-01-01T00:00:00.000Z")
    }
}
