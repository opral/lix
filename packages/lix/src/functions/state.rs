use crate::LixError;
use crate::NullableKeyFilter;
use crate::commit_graph::CommitGraphContext;
use crate::functions::{DeterministicMode, DeterministicSequence};
use crate::live_state::{LiveStateContext, LiveStateRowRequest, MaterializedLiveStateRow};
use crate::storage_adapter::{StorageAdapterRead, StoragePrecondition, StorageWriteSet};
use crate::tracked_state::TrackedStateContext;
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
    _read: &(impl StorageAdapterRead + ?Sized),
    _writes: &mut StorageWriteSet,
    _sequence: DeterministicSequence,
    _timestamp: crate::common::LixTimestamp,
    _change_id: crate::changelog::ChangeId,
) -> Result<StoragePrecondition, LixError> {
    Err(LixError::new(
        LixError::CODE_UNSUPPORTED_SQL,
        "deterministic sequence publication is deferred until its ForkTree owner is lowered",
    ))
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
    use std::ops::Bound;

    use bytes::Bytes;

    use crate::engine::Engine;
    use crate::storage::Memory;
    use crate::storage_adapter::{
        MAX_SCAN_PAGE_ROWS, StorageAdapter, StorageBeginScanOptions, StorageCoreProjection,
        StorageKey, StorageProjectedValue, StorageReadOptions, StorageValue, StorageWriteOptions,
    };

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

    #[tokio::test]
    async fn public_deterministic_function_rejects_same_count_member_substitution_after_reopen() {
        let storage = Memory::new();
        let receipt = Engine::initialize(storage.clone())
            .await
            .expect("test repository should initialize");
        let engine = Engine::new(storage.clone())
            .await
            .expect("test repository should open");
        let session = engine
            .open_session(receipt.main_branch_id.clone())
            .await
            .expect("workspace session should open");
        session
            .execute(
                "INSERT INTO lix_key_value (key, value, lixcol_global, lixcol_untracked) \
                 VALUES ('lix_deterministic_mode', lix_json('{\"enabled\":true}'), true, true)",
                &[],
            )
            .await
            .expect("deterministic mode should publish");
        session
            .execute("SELECT lix_uuid_v7()", &[])
            .await
            .expect("the initial deterministic function call should publish its member");
        drop(session);
        drop(engine);

        let published = storage
            .export_snapshot()
            .expect("published deterministic state should snapshot");
        let reopened = Memory::from_snapshot(&published).expect("state should cold reopen");
        let reopened_engine = Engine::new(reopened.clone())
            .await
            .expect("reopened state should open");
        let reopened_session = reopened_engine
            .open_session(crate::GLOBAL_BRANCH_ID.to_owned())
            .await
            .expect("reopened workspace should open");
        reopened_session
            .execute("SELECT lix_uuid_v7()", &[])
            .await
            .expect("an authenticated selected member should remain usable after reopen");
        drop(reopened_session);
        drop(reopened_engine);

        let corrupt = Memory::from_snapshot(&published).expect("corruption fixture should reopen");
        let storage = StorageAdapter::new(corrupt.clone());
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("current untracked state should be readable");
        let range = crate::storage_adapter::StorageKeyRange {
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
        };
        let mut cursor = read
            .begin_scan(
                crate::forktree::UNTRACKED_ROW_SPACE,
                range,
                StorageBeginScanOptions {
                    projection: StorageCoreProjection::FullValue,
                    ..StorageBeginScanOptions::default()
                },
            )
            .await
            .expect("untracked state scan should begin");
        let mut selected = Vec::new();
        loop {
            let page = cursor
                .next_page(MAX_SCAN_PAGE_ROWS)
                .await
                .expect("untracked state page should read");
            selected.extend(page.entries.into_iter().filter(|entry| {
                entry
                    .key
                    .0
                    .windows(DETERMINISTIC_SEQUENCE_KEY.len())
                    .any(|window| window == DETERMINISTIC_SEQUENCE_KEY.as_bytes())
            }));
            if !page.has_more {
                break;
            }
        }
        drop(cursor);
        drop(read);
        assert_eq!(selected.len(), 1, "fixture must select one sequence member");
        let selected = selected.pop().expect("selected member");
        let StorageProjectedValue::FullValue(value) = selected.value else {
            panic!("selected deterministic member must include its value");
        };
        let replacement = b"lix_unrelated_sequence_substitute";
        assert_eq!(replacement.len(), DETERMINISTIC_SEQUENCE_KEY.len());
        let replacement_key = StorageKey(Bytes::from(replace_subslice(
            selected.key.0.as_ref(),
            DETERMINISTIC_SEQUENCE_KEY.as_bytes(),
            replacement,
        )));
        let mut writes = storage.new_write_set();
        writes.delete(crate::forktree::UNTRACKED_ROW_SPACE, selected.key);
        writes.put(
            crate::forktree::UNTRACKED_ROW_SPACE,
            replacement_key,
            StorageValue { bytes: value },
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("same-count current-owner substitution should commit");

        let corrupted_snapshot = corrupt
            .export_snapshot()
            .expect("corrupted current-owner state should snapshot");
        let corrupted = Memory::from_snapshot(&corrupted_snapshot)
            .expect("corrupted current-owner state should cold reopen");
        let corrupted_engine = Engine::new(corrupted)
            .await
            .expect("structurally corrupt state should open before selected read");
        let corrupted_session = corrupted_engine
            .open_workspace_session()
            .await
            .expect("corrupted workspace should open before selected read");
        let error = corrupted_session
            .execute("SELECT lix_uuid_v7()", &[])
            .await
            .expect_err("missing selected member must fail closed through the public function");
        assert!(
            error.message.contains("identity") || error.message.contains("sequence"),
            "unexpected selected-member closure error: {error:?}"
        );
    }

    fn replace_subslice(haystack: &[u8], needle: &[u8], replacement: &[u8]) -> Vec<u8> {
        assert_eq!(needle.len(), replacement.len());
        let position = haystack
            .windows(needle.len())
            .position(|candidate| candidate == needle)
            .expect("selected key must contain its canonical identity");
        let mut replaced = haystack.to_vec();
        replaced[position..position + needle.len()].copy_from_slice(replacement);
        replaced
    }
}
