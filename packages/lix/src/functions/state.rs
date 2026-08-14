use serde_json::Value as JsonValue;

use crate::GLOBAL_BRANCH_ID;
use crate::LixError;
use crate::branch::{
    BranchHeadControlContext, branch_head_control_precondition, stage_branch_head_control,
};
use crate::changelog::{ChangeId, ChangeRecordProjection};
use crate::common::LixTimestamp;
use crate::entity_pk::EntityPk;
use crate::functions::{DeterministicMode, DeterministicSequence};
use crate::hot_state::{
    CurrentStateDeltaRef, GlobalKeyValueRowCache, HotStateReadDomain,
    MaterializedHotStateExactBatch, TrackedHeadContext,
};
use crate::json_store::JsonSlot;
use crate::storage_adapter::{StorageAdapterRead, StoragePrecondition, StorageWriteSet};
use crate::tracked_state::{TrackedStateKey, TrackedStateKeyRef};

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
    cache: Option<&GlobalKeyValueRowCache>,
) -> Result<DeterministicMode, LixError> {
    let Some(row) = load_key_value_row(read, cache, DETERMINISTIC_MODE_KEY).await? else {
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
    cache: Option<&GlobalKeyValueRowCache>,
) -> Result<DeterministicSequence, LixError> {
    let Some(row) = load_key_value_row(read, cache, DETERMINISTIC_SEQUENCE_KEY).await? else {
        return Ok(DeterministicSequence::uninitialized());
    };
    let value = key_value_payload(&row, DETERMINISTIC_SEQUENCE_KEY)?;
    parse_sequence_value(value)
}

/// Reads one global untracked `lix_key_value` cell from its authenticated
/// native tuple. Public/system callers share this owner instead of rebuilding
/// a JSON snapshot reader.
pub(crate) async fn load_untracked_key_value(
    read: &(impl StorageAdapterRead + ?Sized),
    key: &str,
) -> Result<Option<JsonValue>, LixError> {
    load_key_value_row(read, None, key)
        .await?
        .map(|rows| key_value_payload(&rows, key))
        .transpose()
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
    change_id: ChangeId,
) -> Result<Vec<StoragePrecondition>, LixError> {
    let snapshot = serde_json::json!({
        "key": DETERMINISTIC_SEQUENCE_KEY,
        "value": sequence.highest_seen,
    });
    let entity_pk = EntityPk::single(DETERMINISTIC_SEQUENCE_KEY);
    let native_snapshot = crate::native_row::encode(
        key_value_schema(),
        &entity_pk,
        GLOBAL_BRANCH_ID,
        None,
        true,
        &snapshot,
    )?;
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
    let mut preconditions = Vec::with_capacity(1);
    let next_revision = control
        .next_current_state_revision()?
        .current_state_revision;
    TrackedHeadContext::new()
        .writer(read, writes)
        .stage_untracked_current_state(
            GLOBAL_BRANCH_ID,
            control.tracked_generation,
            &[CurrentStateDeltaRef {
                schema_key: KEY_VALUE_SCHEMA_KEY,
                file_id: None,
                entity_pk: &entity_pk,
                // The caller already minted this id for the sequence row; this
                // lane stages the head directly, so it must carry it rather
                // than relying on the prepared-row path to supply one.
                change_id: Some(change_id),
                commit_id: None,
                untracked: true,
                deleted: false,
                created_at: timestamp,
                updated_at: timestamp,
                snapshot: crate::json_store::JsonSlotRef::None,
                native_snapshot: Some(&native_snapshot),
                metadata: crate::json_store::JsonSlotRef::None,
                columnar_base_coordinate: None,
            }],
            &std::collections::BTreeSet::new(),
        )
        .await?;
    // The hot-state mutation is fenced by an actual control-byte
    // change. Merely restaging the old control would let two writers both
    // satisfy the same CAS after the first write, losing one group update.
    let mut next_control = control;
    next_control.current_state_revision = next_revision;
    next_control.note_schema(KEY_VALUE_SCHEMA_KEY);
    stage_branch_head_control(writes, GLOBAL_BRANCH_ID, next_control)?;
    preconditions.push(branch_head_control_precondition(
        GLOBAL_BRANCH_ID,
        observation.raw_token,
    )?);
    Ok(preconditions)
}

async fn load_key_value_row(
    read: &(impl StorageAdapterRead + ?Sized),
    cache: Option<&GlobalKeyValueRowCache>,
    key: &str,
) -> Result<Option<MaterializedHotStateExactBatch>, LixError> {
    let Some(control) = BranchHeadControlContext::new()
        .reader(read)
        .load(GLOBAL_BRANCH_ID)
        .await?
    else {
        return Ok(None);
    };
    // The control is the fence, not just the read's input: it is republished
    // under a CAS by every write to this plane, so an unchanged control means
    // the row below — and the collection closure validated with it — cannot
    // have moved.
    if let Some(cache) = cache
        && let Some(row) = cache.get(control, key)
    {
        return Ok(row);
    }
    // Deliberately one function, not two. Extracting the canonical read into
    // its own `async fn` would add a poll frame to a path that runs inside
    // `Transaction::open`, and this codebase's async future-size budget is
    // tight enough that the sign of such a change cannot be assumed. The
    // labelled block gives the same early-exit shape with no extra future.
    let row = 'resolved: {
        let keys = [TrackedStateKey {
            schema_key: KEY_VALUE_SCHEMA_KEY.to_string(),
            entity_pk: EntityPk::single(key),
            file_id: None,
        }];
        let projection = ChangeRecordProjection {
            snapshot_content: false,
            metadata: false,
        };
        let reader = TrackedHeadContext::new().reader(read);
        let key_refs = keys
            .iter()
            .map(|key| TrackedStateKeyRef {
                schema_key: key.schema_key.as_str(),
                entity_pk: &key.entity_pk,
                file_id: key.file_id.as_deref(),
            })
            .collect::<Vec<_>>();
        let rows = reader
            .load_projected_live_batch_refs_for_domain(
                GLOBAL_BRANCH_ID,
                control,
                &key_refs,
                &projection,
                HotStateReadDomain::Untracked,
            )
            .await?;
        let Some(row) = rows.row(0) else {
            reader
                .validate_exact_collection_closure(
                    GLOBAL_BRANCH_ID,
                    control.tracked_generation,
                    crate::collection_generation::CollectionScopeRef {
                        schema_key: KEY_VALUE_SCHEMA_KEY,
                        file_id: None,
                    },
                    key_refs[0],
                    HotStateReadDomain::Untracked,
                    control.current_state_revision == 0,
                )
                .await?;
            break 'resolved None;
        };
        if !row.untracked() || row.deleted() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "deterministic key-value row '{key}' is not a live untracked authority member"
                ),
            ));
        }
        Some(rows)
    };
    if let Some(cache) = cache {
        cache.insert(control, key, row.clone());
    }
    Ok(row)
}

fn key_value_schema() -> &'static lix_schema::Schema {
    crate::native_row::seed_schema(KEY_VALUE_SCHEMA_KEY)
        .expect("compile-time lix_key_value Schema v1 definition must be valid")
}

fn key_value_payload(
    rows: &MaterializedHotStateExactBatch,
    key: &str,
) -> Result<JsonValue, LixError> {
    if rows.len() != 1 {
        return Err(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            format!("deterministic key-value lookup '{key}' returned {} rows", rows.len()),
        ));
    }
    let row = rows.row(0).ok_or_else(|| {
        LixError::new(
            LixError::CODE_STORAGE_ERROR,
            format!("deterministic key-value lookup '{key}' returned an empty slot"),
        )
    })?;
    if row.schema_key() != KEY_VALUE_SCHEMA_KEY
        || row.entity_pk().as_single_string()? != key
        || row.file_id().is_some()
        || row.branch_id() != GLOBAL_BRANCH_ID
        || !row.global()
        || !row.untracked()
        || row.deleted()
    {
        return Err(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            format!("deterministic key-value row '{key}' has mismatched state identity"),
        ));
    }
    let native = row.native_snapshot().ok_or_else(|| {
        LixError::new(
            LixError::CODE_STORAGE_ERROR,
            format!("deterministic key-value row '{key}' is missing its native scalar tuple"),
        )
    })?;
    let values = crate::native_row::decode(key_value_schema(), native)?;
    let [lix_schema::value_layout::BodyValue::Jsonb(value)] = values.as_slice() else {
        return Err(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            format!("deterministic key-value row '{key}' has an invalid native value tuple"),
        ));
    };
    Ok(value.clone())
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
    use crate::hot_state::{HotStateContext, HotStateRowRequest};
    use crate::storage_adapter::StorageAdapter;
    use crate::storage_adapter::{
        Memory, StorageBeginScanOptions, StorageKey, StorageProjectedValue, StorageReadOptions,
        StorageValue, StorageWriteOptions,
    };

    use super::*;

    fn hot_state_context() -> HotStateContext {
        HotStateContext::new(
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

        let mode = load_mode(&read, None).await.expect("missing mode should decode");

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
        let mode = load_mode(&read, None).await.expect("valid mode should decode");

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
        crate::test_support::seed_global_branch_head(storage.clone()).await;
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");

        let sequence = load_sequence(&read, None)
            .await
            .expect("missing sequence should decode");

        assert_eq!(sequence, DeterministicSequence::uninitialized());
    }

    #[tokio::test]
    async fn same_count_sequence_substitution_fails_identity_closure() {
        let memory = Memory::new();
        let storage = StorageAdapter::new(memory.clone());
        crate::test_support::seed_global_branch_head(storage.clone()).await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("sequence publication read should open");
        let mut writes = storage.new_write_set();
        stage_sequence(
            &read,
            &mut writes,
            DeterministicSequence { highest_seen: 7 },
            test_timestamp(),
            ChangeId::for_test_label("sequence-corruption-change"),
        )
        .await
        .expect("sequence should stage");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("sequence should publish");

        let snapshot = memory
            .export_snapshot()
            .expect("published sequence storage should snapshot");
        drop(storage);
        drop(memory);
        let storage = StorageAdapter::new(
            Memory::from_snapshot(&snapshot).expect("published sequence storage should reopen"),
        );

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("reopened sequence storage should read");
        let range = crate::storage_adapter::StoragePrefix {
            bytes: bytes::Bytes::new(),
        }
        .to_range()
        .expect("valid empty prefix");
        let mut cursor = read
            .begin_scan(
                crate::hot_state::ROW_SPACE,
                range,
                StorageBeginScanOptions::default(),
            )
            .await
            .expect("selected HOT member scan should begin");
        let mut rows = cursor
            .collect_all()
            .await
            .expect("selected HOT members should scan");
        assert_eq!(
            rows.len(),
            1,
            "fixture should publish only the sequence member"
        );
        let sequence_member = rows.pop().expect("one sequence member");
        let StorageProjectedValue::FullValue(sequence_value) = sequence_member.value else {
            panic!("sequence fixture should scan the full HOT row value");
        };
        let sequence_identity = DETERMINISTIC_SEQUENCE_KEY.as_bytes();
        let unrelated_identity = b"lix_unrelated_sequence_substitute";
        assert_eq!(sequence_identity.len(), unrelated_identity.len());
        let identity_offset = sequence_member
            .key
            .0
            .windows(sequence_identity.len())
            .position(|candidate| candidate == sequence_identity)
            .expect("sequence identity should be encoded in its HOT key");
        let mut unrelated_key = sequence_member.key.0.to_vec();
        unrelated_key[identity_offset..identity_offset + sequence_identity.len()]
            .copy_from_slice(unrelated_identity);
        drop(cursor);
        drop(read);

        let mut corrupt = storage.new_write_set();
        corrupt.delete(crate::hot_state::ROW_SPACE, sequence_member.key);
        corrupt.put(
            crate::hot_state::ROW_SPACE,
            StorageKey(bytes::Bytes::from(unrelated_key)),
            StorageValue {
                bytes: sequence_value,
            },
        );
        storage
            .commit_write_set(corrupt, StorageWriteOptions::default())
            .await
            .expect("same-count sequence member substitution should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("corrupt sequence storage should read");
        let error = load_sequence(&read, None)
            .await
            .expect_err("missing selected sequence member must fail closed");
        assert!(
            error
                .message
                .contains("identity digest does not match its canonical members"),
            "unexpected closure error: {error:?}"
        );
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
        let sequence = load_sequence(&read, None)
            .await
            .expect("valid sequence should decode");

        assert_eq!(sequence, DeterministicSequence { highest_seen: 41 });
        assert_eq!(sequence.next_sequence(), 42);
    }

    #[tokio::test]
    async fn write_sequence_persists_untracked_global_key_value() {
        let storage = StorageAdapter::new(Memory::new());
        let hot_state = hot_state_context();
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

        let reader = hot_state.reader(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("read should open"),
        );
        let row = reader
            .load_row(&HotStateRowRequest {
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
        // The id the caller handed `stage_sequence` must survive to the head.
        // Asserting the exact supplied label — rather than merely `is_some()` —
        // is what proves this lane carries the caller's id instead of inventing
        // one of its own.
        assert_eq!(
            row.change_id,
            Some(ChangeId::for_test_label("sequence-change-7"))
        );
        assert_eq!(row.commit_id, None);
        assert_eq!(row.snapshot_content, None);
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("native sequence read should open");
        assert_eq!(
            load_sequence(&read, None)
                .await
                .expect("native sequence should decode"),
            DeterministicSequence { highest_seen: 7 }
        );
    }

    async fn write_test_key_value(storage: StorageAdapter, key: &str, value: JsonValue) {
        let snapshot_value = serde_json::json!({
            "key": key,
            "value": value,
        });
        let snapshot_content =
            serde_json::to_string(&snapshot_value).expect("snapshot should serialize");
        let entity_pk = EntityPk::single(key);
        let native_snapshot = crate::native_row::encode(
            key_value_schema(),
            &entity_pk,
            GLOBAL_BRANCH_ID,
            None,
            true,
            &snapshot_value,
        )
        .expect("test key-value tuple should encode");
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
        let mut next_control = control
            .next_current_state_revision()
            .expect("global control revision should advance");
        TrackedHeadContext::new()
            .writer(&read, &mut writes)
            .stage_untracked_current_state(
                GLOBAL_BRANCH_ID,
                control.tracked_generation,
                &[CurrentStateDeltaRef {
                    schema_key: KEY_VALUE_SCHEMA_KEY,
                    file_id: None,
                    entity_pk: &entity_pk,
                    change_id: Some(ChangeId::for_test_label("functions-state-sequence")),
                    commit_id: None,
                    untracked: true,
                    deleted: false,
                    created_at: test_timestamp(),
                    updated_at: test_timestamp(),
                    snapshot: snapshot.as_ref_slot(),
                    native_snapshot: Some(&native_snapshot),
                    metadata: crate::json_store::JsonSlotRef::None,
                    columnar_base_coordinate: None,
                }],
                &std::collections::BTreeSet::new(),
            )
            .await
            .expect("test key-value current row should stage");
        next_control.note_schema(KEY_VALUE_SCHEMA_KEY);
        stage_branch_head_control(&mut writes, GLOBAL_BRANCH_ID, next_control)
            .expect("global control should publish current state");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("test key-value should commit");
    }

    fn test_timestamp() -> LixTimestamp {
        LixTimestamp::expect_parse("timestamp", "1970-01-01T00:00:00.000Z")
    }
}
