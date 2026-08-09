use crate::LixError;
use crate::NullableKeyFilter;
use crate::forktree::ForkTreeReadFacade;
use crate::functions::{DeterministicMode, DeterministicSequence};
use crate::live_state::{LiveStateFilter, LiveStateScanRequest, MaterializedLiveStateRow};
use crate::storage_adapter::{StorageAdapterRead, StoragePrecondition, StorageWriteSet};
use bytes::Bytes;
use serde_json::Value as JsonValue;
use std::collections::BTreeSet;

pub(crate) const DETERMINISTIC_MODE_KEY: &str = "lix_deterministic_mode";
pub(crate) const DETERMINISTIC_SEQUENCE_KEY: &str = "lix_deterministic_sequence_number";
pub(crate) const DETERMINISTIC_SEQUENCE_INITIALIZED_KEY: &str =
    "lix_deterministic_sequence_initialized";

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
    let rows = load_key_value_rows(read).await?;
    let Some(row) = rows.iter().find(|row| {
        !row.deleted
            && row
                .entity_pk
                .as_single_string()
                .is_ok_and(|key| key == DETERMINISTIC_MODE_KEY)
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
pub(crate) async fn load_sequence(
    read: &(impl StorageAdapterRead + ?Sized),
) -> Result<DeterministicSequence, LixError> {
    let rows = load_key_value_rows(read).await?;
    let sequence = rows.iter().find(|row| {
        !row.deleted
            && row
                .entity_pk
                .as_single_string()
                .is_ok_and(|key| key == DETERMINISTIC_SEQUENCE_KEY)
    });
    if let Some(row) = sequence {
        return parse_sequence_value(key_value_payload(row, DETERMINISTIC_SEQUENCE_KEY)?);
    }

    let initialized = rows.iter().find(|row| {
        row.entity_pk
            .as_single_string()
            .is_ok_and(|key| key == DETERMINISTIC_SEQUENCE_INITIALIZED_KEY)
    });
    let Some(initialized) = initialized else {
        return Ok(DeterministicSequence::uninitialized());
    };
    if initialized.deleted {
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
) -> Result<Vec<StoragePrecondition>, LixError> {
    let sequence_key = deterministic_untracked_storage_key(DETERMINISTIC_SEQUENCE_KEY);
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
    let sequence_precondition = untracked_precondition(read, &sequence_key).await?;
    writes.put(crate::forktree::UNTRACKED_ROW_SPACE, sequence_key, value);

    let marker_key = deterministic_untracked_storage_key(DETERMINISTIC_SEQUENCE_INITIALIZED_KEY);
    let marker_snapshot = serde_json::to_string(&serde_json::json!({
        "key": DETERMINISTIC_SEQUENCE_INITIALIZED_KEY,
        "value": true,
    }))
    .expect("deterministic sequence initialization marker is serializable");
    let marker_value =
        crate::forktree::encode_untracked_value(crate::forktree::UntrackedValueRef {
            created_at: timestamp,
            updated_at: timestamp,
            cell: crate::forktree::StateCellRef::Value(&marker_snapshot),
            metadata: None,
            origin_key: None,
            blob_manifest_object_ids: &[],
        })?;
    let marker_precondition = untracked_precondition(read, &marker_key).await?;
    writes.put(
        crate::forktree::UNTRACKED_ROW_SPACE,
        marker_key,
        marker_value,
    );
    Ok(vec![sequence_precondition, marker_precondition])
}

fn deterministic_untracked_storage_key(key: &str) -> crate::storage_adapter::StorageKey {
    let entity_pk = crate::entity_pk::EntityPk::single(key);
    let encoded = crate::forktree::encode_untracked_key(
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
    crate::storage_adapter::StorageKey(Bytes::from(encoded))
}

async fn untracked_precondition(
    read: &(impl StorageAdapterRead + ?Sized),
    storage_key: &crate::storage_adapter::StorageKey,
) -> Result<StoragePrecondition, LixError> {
    let current = crate::storage_adapter::PointReadPlan::new(
        crate::forktree::UNTRACKED_ROW_SPACE,
        std::slice::from_ref(storage_key),
    )
    .materialize(read, crate::storage_adapter::StorageGetOptions::default())
    .await?
    .value
    .into_iter()
    .next()
    .flatten();
    match current {
        Some(crate::storage_adapter::StorageProjectedValue::FullValue(expected)) => {
            Ok(StoragePrecondition::KeyValueEquals {
                space: crate::forktree::UNTRACKED_ROW_SPACE,
                key: storage_key.clone(),
                expected,
            })
        }
        Some(crate::storage_adapter::StorageProjectedValue::KeyOnly) => Err(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            "deterministic sequence owner read returned key-only data",
        )),
        None => Ok(StoragePrecondition::KeyAbsent {
            space: crate::forktree::UNTRACKED_ROW_SPACE,
            key: storage_key.clone(),
        }),
    }
}

async fn load_key_value_rows(
    read: &(impl StorageAdapterRead + ?Sized),
) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
    let forktree = ForkTreeReadFacade::new(read);
    let rows = crate::live_state::scan_forktree_facade(
        &forktree,
        &LiveStateScanRequest {
            filter: LiveStateFilter {
                schema_keys: vec![KEY_VALUE_SCHEMA_KEY.to_owned()],
                branch_ids: vec![crate::GLOBAL_BRANCH_ID.to_owned()],
                file_ids: vec![NullableKeyFilter::Null],
                untracked: Some(true),
                include_tombstones: true,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await?
    .into_rows();
    let mut identities = BTreeSet::new();
    for row in &rows {
        if row.schema_key != KEY_VALUE_SCHEMA_KEY
            || row.file_id.is_some()
            || !row.untracked
            || row.branch_id.as_ref() != crate::GLOBAL_BRANCH_ID
        {
            return Err(LixError::new(
                LixError::CODE_STORAGE_ERROR,
                "deterministic key-value scan returned a row outside its authenticated owner",
            ));
        }
        let key = row.entity_pk.as_single_string().map_err(|error| {
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
        if row.deleted {
            if row.snapshot_content.is_some()
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
        } else if let Some(snapshot_content) = row.snapshot_content.as_deref() {
            let snapshot =
                serde_json::from_str::<JsonValue>(snapshot_content).map_err(|error| {
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

    #[tokio::test]
    async fn public_deterministic_function_rejects_missing_initialized_member_after_reopen() {
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

        let corrupted = Memory::from_snapshot(
            &storage
                .export_snapshot()
                .expect("published deterministic state should snapshot"),
        )
        .expect("corruption fixture should reopen");
        let storage_adapter = StorageAdapter::new(corrupted.clone());
        let read = storage_adapter
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
        let mut sequence_key = None;
        loop {
            let page = cursor
                .next_page(MAX_SCAN_PAGE_ROWS)
                .await
                .expect("untracked state page should read");
            if sequence_key.is_none() {
                sequence_key = page.entries.into_iter().find_map(|entry| {
                    entry
                        .key
                        .0
                        .windows(DETERMINISTIC_SEQUENCE_KEY.len())
                        .any(|window| window == DETERMINISTIC_SEQUENCE_KEY.as_bytes())
                        .then_some(entry.key)
                });
            }
            if sequence_key.is_some() || !page.has_more {
                break;
            }
        }
        drop(cursor);
        drop(read);
        let sequence_key = sequence_key.expect("fixture must contain one sequence member");
        let mut writes = storage_adapter.new_write_set();
        writes.delete(crate::forktree::UNTRACKED_ROW_SPACE, sequence_key);
        storage_adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("missing-member corruption should commit");

        let corrupted_snapshot = corrupted
            .export_snapshot()
            .expect("missing-member state should snapshot");
        let corrupted = Memory::from_snapshot(&corrupted_snapshot)
            .expect("missing-member state should cold reopen");
        let corrupted_engine = Engine::new(corrupted)
            .await
            .expect("corrupt state should open before selected read");
        let corrupted_session = corrupted_engine
            .open_workspace_session()
            .await
            .expect("corrupt workspace should open before selected read");
        let error = corrupted_session
            .execute("SELECT lix_uuid_v7()", &[])
            .await
            .expect_err("an initialized but missing sequence member must fail closed");
        assert!(
            error.message.contains("missing") || error.message.contains("sequence"),
            "unexpected missing-member closure error: {error:?}"
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
