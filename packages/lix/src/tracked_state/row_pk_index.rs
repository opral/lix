//! Secondary commit-root identities ordered by `(schema_key, row_pk, file_id)`.
//!
//! The primary tracked-state tree is ordered by `(schema_key, file_id, row_pk)`.
//! Re-encoding an identity as an otherwise ordinary tracked-state key lets the
//! same persistent tree implementation serve row-primary-key prefix lookups.

use crate::row_pk::RowPk;
use crate::storage_adapter::{StorageAdapterRead, StorageWriteSet};
use crate::tracked_state::codec::{decode_key, encode_key_ref, encode_value_ref};
use crate::tracked_state::types::{
    CommitStateManifest, TrackedStateIndexValueRef, TrackedStateKey, TrackedStateKeyRef,
    TrackedStateMutation, TrackedStateMutationBatch, TrackedStateRootId,
    TrackedStateTreeScanRequest,
};
use crate::{LixError, NullableKeyFilter};

const NULL_FILE_ID_TAG: &str = "n";
const VALUE_FILE_ID_TAG: &str = "v";

/// Re-encodes a primary-tree identity for the row-PK secondary tree.
pub(crate) fn encode_row_pk_index_key(key: TrackedStateKeyRef<'_>) -> Result<Vec<u8>, LixError> {
    let row_pk = typed_row_pk_text(key.row_pk)?;
    let file_identity = match key.file_id {
        Some(file_id) => RowPk::from_shared_parts(
            [
                VALUE_FILE_ID_TAG.into(),
                serde_json::to_string(file_id)
                    .map_err(|error| {
                        row_pk_index_error(format!("failed to encode file ID: {error}"))
                    })?
                    .into(),
            ]
            .into_iter(),
        ),
        None => RowPk::from_shared_parts([NULL_FILE_ID_TAG.into()].into_iter()),
    }
    .map_err(row_pk_index_error)?;
    Ok(encode_key_ref(TrackedStateKeyRef {
        schema_key: key.schema_key,
        file_id: Some(&row_pk),
        row_pk: &file_identity,
    }))
}

/// Decodes one row-PK secondary-tree identity back to its primary-tree form.
pub(crate) fn decode_row_pk_index_key(encoded: &[u8]) -> Result<TrackedStateKey, LixError> {
    let encoded = decode_key(encoded)?;
    let row_pk = encoded
        .file_id
        .as_deref()
        .ok_or_else(|| row_pk_index_error("secondary identity is missing its row-PK prefix"))?;
    let row_pk_value = serde_json::from_str(row_pk)
        .map_err(|_| row_pk_index_error("secondary identity has an invalid row-PK prefix"))?;
    let row_pk = RowPk::from_typed_json_array_value(&row_pk_value).map_err(row_pk_index_error)?;
    let parts = encoded.row_pk.into_parts();
    let file_id = match parts.as_slice() {
        [tag] if tag == NULL_FILE_ID_TAG => None,
        [tag, file_id] if tag == VALUE_FILE_ID_TAG => Some(
            serde_json::from_str(file_id)
                .map_err(|_| row_pk_index_error("secondary identity has an invalid file ID"))?,
        ),
        _ => {
            return Err(row_pk_index_error(
                "secondary identity has an invalid file-id suffix",
            ));
        }
    };
    Ok(TrackedStateKey {
        schema_key: encoded.schema_key,
        file_id,
        row_pk,
    })
}

/// Builds the bounded tree request for one schema and an exact row-PK set.
pub(crate) fn row_pk_index_scan_request(
    schema_key: &str,
    row_pks: &[RowPk],
    include_tombstones: bool,
) -> Result<TrackedStateTreeScanRequest, LixError> {
    Ok(TrackedStateTreeScanRequest {
        schema_keys: vec![schema_key.to_owned()],
        file_ids: row_pks
            .iter()
            .map(typed_row_pk_text)
            .map(|value| value.map(NullableKeyFilter::Value))
            .collect::<Result<Vec<_>, _>>()?,
        include_tombstones,
        ..TrackedStateTreeScanRequest::default()
    })
}

/// Retains the primary batch and derives its row-PK-index counterpart.
///
/// Values are immutable byte slices, so the secondary batch shares them rather
/// than decoding and re-encoding lifecycle metadata.
pub(crate) fn with_row_pk_index_mutations(
    primary: TrackedStateMutationBatch,
) -> Result<(TrackedStateMutationBatch, TrackedStateMutationBatch), LixError> {
    let primary = primary.into_mutations();
    let mut secondary = Vec::with_capacity(primary.len());
    for mutation in &primary {
        let key = decode_key(&mutation.encoded_key)?;
        let encoded_key = encode_row_pk_index_key(TrackedStateKeyRef {
            schema_key: &key.schema_key,
            file_id: key.file_id.as_deref(),
            row_pk: &key.row_pk,
        })?;
        secondary.push(TrackedStateMutation::from_shared(
            encoded_key.into(),
            mutation.encoded_value.clone(),
        ));
    }
    Ok((
        TrackedStateMutationBatch::from_shared(primary),
        TrackedStateMutationBatch::from_shared(secondary),
    ))
}

/// Stages the monotonic identity catalog transition for one commit.
///
/// Values only keep catalog entries visible to the shared tree scanner; readers
/// resolve current values through canonical state. Preserve an existing entry's
/// bytes across updates, deletes, and recreates instead of rewriting its leaf.
/// An unchanged identity set can therefore share the parent's catalog root.
pub(crate) async fn stage_row_pk_index_from_members(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    overlay: &mut super::storage::TrackedStateChunkOverlay,
    base_root: Option<&TrackedStateRootId>,
    members: &[super::storage::CommitDeltaMember],
    commit_id: crate::changelog::CommitId,
) -> Result<Option<TrackedStateRootId>, LixError> {
    if members.is_empty() && base_root.is_some() {
        return Ok(base_root.cloned());
    }
    let keys = members
        .iter()
        .map(|member| {
            encode_row_pk_index_key(TrackedStateKeyRef {
                schema_key: &member.key.schema_key,
                file_id: member.key.file_id.as_deref(),
                row_pk: &member.key.row_pk,
            })
            .map(bytes::Bytes::from)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let tree = super::tree::TrackedStateTree::new();
    let existing = if let Some(root) = base_root {
        // A preceding commit in this publication may only exist in the chunk
        // overlay. Probe the same coherent staged view used by tree mutation.
        let staged_read = super::storage::TrackedStateStagedRead::new(store, overlay);
        tree.get_many_encoded(&staged_read, root, &keys).await?
    } else {
        vec![None; keys.len()]
    };
    if existing.iter().flatten().any(|value| value.deleted()) {
        return Err(row_pk_index_error("identity catalog contains a tombstone"));
    }
    let mut secondary = Vec::new();
    for ((member, key), existing) in members.iter().zip(keys).zip(existing) {
        if existing.is_some() {
            continue;
        }
        secondary.push(TrackedStateMutation::from_shared(
            key,
            encode_value_ref(TrackedStateIndexValueRef {
                change_id: member.value.change_id,
                commit_id: member.value.commit_id,
                // This tree is an identity catalog, not current-state
                // membership. Preserve deleted identities for later exact
                // canonical resolution.
                deleted: false,
                created_at: member.value.created_at,
                updated_at: member.value.updated_at,
            })
            .into(),
        ));
    }
    if secondary.is_empty() && base_root.is_some() {
        return Ok(base_root.cloned());
    }
    let result = tree
        .apply_mutations_with_overlay(
            store,
            writes,
            overlay,
            base_root,
            TrackedStateMutationBatch::from_shared(secondary),
            Some(&commit_id.to_string()),
        )
        .await?;
    Ok(Some(result.root_id))
}

/// Stages a complete identity catalog from an already materialized state.
pub(crate) async fn stage_row_pk_index_from_deltas<'a>(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    overlay: &mut super::storage::TrackedStateChunkOverlay,
    deltas: impl IntoIterator<Item = crate::tracked_state::TrackedStateDeltaRef<'a>>,
    commit_id: crate::changelog::CommitId,
) -> Result<Option<TrackedStateRootId>, LixError> {
    let deltas = deltas.into_iter().collect::<Vec<_>>();
    let mut primary = crate::tracked_state::codec::TrackedStateMutationBatchBuilder::with_row_capacity(
        deltas.len(),
    );
    for delta in deltas {
        primary.push(
            TrackedStateKeyRef {
                schema_key: delta.schema_key,
                file_id: delta.file_id,
                row_pk: delta.row_pk,
            },
            TrackedStateIndexValueRef {
                change_id: delta.change_id,
                commit_id: delta.commit_id,
                deleted: false,
                created_at: delta.created_at,
                updated_at: delta.updated_at,
            },
        );
    }
    let (_, secondary) = with_row_pk_index_mutations(primary.finish())?;
    let result = super::tree::TrackedStateTree::new()
        .apply_mutations_with_overlay(
            store,
            writes,
            overlay,
            None,
            secondary,
            Some(&commit_id.to_string()),
        )
        .await?;
    Ok(Some(result.root_id))
}

/// Offline v73 backfill for one immutable commit authority.
pub(crate) async fn backfill_row_pk_index_for_commit(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    manifest: &CommitStateManifest,
    max_rows: usize,
) -> Result<(Option<TrackedStateRootId>, usize), LixError> {
    let mut reader = crate::tracked_state::TrackedStateContext::new().reader(store);
    let mut rows = Vec::new();
    if manifest.snapshot_root.is_none() {
        rows = reader
            .scan_batch_at_commit(
                &manifest.commit_id.to_string(),
                &crate::tracked_state::TrackedStateScanRequest {
                    limit: Some(max_rows.saturating_add(1)),
                    ..Default::default()
                },
            )
            .await?
            .into_rows();
    } else {
        let mut exclusive_after = None;
        loop {
            let remaining = max_rows.saturating_sub(rows.len());
            let request = crate::tracked_state::TrackedStateScanRequest {
                limit: Some(remaining.saturating_add(1).min(4096)),
                ..Default::default()
            };
            let page = reader
                .scan_batch_at_commit_page(
                    &manifest.commit_id.to_string(),
                    &request,
                    exclusive_after.as_ref(),
                )
                .await?;
            if page.len() == 0 {
                break;
            }
            let page_rows = page.into_rows();
            let last = page_rows.last().expect("non-empty page has a final row");
            exclusive_after = Some(TrackedStateKey {
                schema_key: last.schema_key.clone(),
                file_id: last.file_id.clone(),
                row_pk: last.row_pk.clone(),
            });
            rows.extend(page_rows);
            if rows.len() > max_rows {
                break;
            }
        }
    }
    if rows.len() > max_rows {
        return Err(LixError::new(
            "LIX_ERROR_MIGRATION_LIMIT_EXCEEDED",
            format!(
                "v73 row-PK-index migration exceeds configured row bound while scanning commit '{}'",
                manifest.commit_id
            ),
        ));
    }
    let mut primary = crate::tracked_state::codec::TrackedStateMutationBatchBuilder::with_row_capacity(
        rows.len(),
    );
    for row in &rows {
        primary.push(
            TrackedStateKeyRef {
                schema_key: &row.schema_key,
                file_id: row.file_id.as_deref(),
                row_pk: &row.row_pk,
            },
            TrackedStateIndexValueRef {
                change_id: row.change_id,
                commit_id: row.commit_id,
                deleted: false,
                created_at: crate::common::LixTimestamp::parse(&row.created_at)
                    .map_err(row_pk_index_error)?,
                updated_at: crate::common::LixTimestamp::parse(&row.updated_at)
                    .map_err(row_pk_index_error)?,
            },
        );
    }
    let (_, secondary) = with_row_pk_index_mutations(primary.finish())?;
    let mut overlay = super::storage::TrackedStateChunkOverlay::new();
    let result = super::tree::TrackedStateTree::new()
        .apply_mutations_with_overlay(
            store,
            writes,
            &mut overlay,
            None,
            secondary,
            Some(&manifest.commit_id.to_string()),
        )
        .await?;
    Ok((Some(result.root_id), rows.len()))
}

fn typed_row_pk_text(row_pk: &RowPk) -> Result<String, LixError> {
    serde_json::to_string(&row_pk.as_typed_json_array_value()?)
        .map_err(|error| row_pk_index_error(format!("failed to encode row PK: {error}")))
}

fn row_pk_index_error(message: impl std::fmt::Display) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("tracked-state row-PK index {message}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changelog::{ChangeId, CommitId};
    use crate::common::LixTimestamp;
    use crate::storage_adapter::{Memory, StorageAdapter, StorageReadOptions, StorageWriteOptions};
    use crate::tracked_state::codec::{TrackedStateMutationBatchBuilder, encode_value_ref};
    use crate::tracked_state::storage::{
        CommitDeltaMember, TrackedStateChunkOverlay, TrackedStateStagedRead,
    };
    use crate::tracked_state::types::TrackedStateIndexValueRef;

    fn identities() -> Vec<TrackedStateKey> {
        vec![
            TrackedStateKey {
                schema_key: "schema".to_owned(),
                file_id: None,
                row_pk: RowPk::single("row"),
            },
            TrackedStateKey {
                schema_key: "schema".to_owned(),
                file_id: Some("01920000-0000-7000-8000-000000000001".to_owned()),
                row_pk: RowPk::tuple(vec!["row".to_owned(), "part".to_owned()]).unwrap(),
            },
            TrackedStateKey {
                schema_key: "schema\0escaped".to_owned(),
                file_id: Some("file\0escaped".to_owned()),
                row_pk: RowPk::uuid_from_canonical("01920000-0000-7000-8000-000000000002").unwrap(),
            },
        ]
    }

    // Construct a real decoded member through the canonical delta codec rather
    // than exposing CommitDeltaMember's private authority fields to tests.
    async fn catalog_member(store: &impl StorageAdapterRead) -> CommitDeltaMember {
        let row_pk = RowPk::single("row");
        let commit_id = CommitId::for_test_label("catalog-original");
        let snapshot = crate::plugin::runtime::WasmTypedRow {
            schema_fingerprint: [9; 32],
            row_pk: vec![lix_schema::Value::Text("row".to_owned())].into(),
            row: lix_schema::Row::from([("value".to_owned(), lix_schema::Value::Int8(1))]),
            native_payload: std::sync::OnceLock::new(),
            boundary_create_validation: std::sync::OnceLock::new(),
        };
        let deltas = [crate::tracked_state::types::TrackedStateCommitDeltaRef {
            delta: crate::tracked_state::TrackedStateDeltaRef {
                schema_key: "schema",
                file_id: None,
                row_pk: &row_pk,
                change_id: ChangeId::for_test_label("catalog-original-change"),
                commit_id,
                deleted: false,
                created_at: LixTimestamp::from_unix_millis_utc_lossy(1),
                updated_at: LixTimestamp::from_unix_millis_utc_lossy(2),
            },
            metadata: None,
            snapshot: Some(snapshot.durable_payload_ref().unwrap()),
            origin_key: None,
            base_coordinate: None,
            authored: true,
        }];
        let mut writes = StorageWriteSet::default();
        let staged =
            super::super::storage::stage_addressable_commit_deltas(&mut writes, &deltas, &[false])
                .unwrap();
        let inventory = staged.mutation_inventory();
        let segments =
            super::super::storage::staged_commit_delta_segment_bytes(&writes, commit_id, inventory)
                .unwrap();
        super::super::storage::staged_commit_delta_members(
            store,
            commit_id,
            "catalog-account",
            inventory,
            segments,
        )
        .await
        .unwrap()
        .pop()
        .unwrap()
    }

    fn member_key(member: &CommitDeltaMember) -> bytes::Bytes {
        encode_row_pk_index_key(TrackedStateKeyRef {
            schema_key: &member.key.schema_key,
            file_id: member.key.file_id.as_deref(),
            row_pk: &member.key.row_pk,
        })
        .unwrap()
        .into()
    }

    #[tokio::test]
    async fn identity_catalog_retains_durable_root_and_values_through_row_lifecycle() {
        let storage = StorageAdapter::new(Memory::new());
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        let original = catalog_member(&read).await;
        let mut writes = storage.new_write_set();
        let mut overlay = TrackedStateChunkOverlay::new();
        // A snapshot/backfill-style base is compatible with incremental catalog maintenance.
        let root = stage_row_pk_index_from_deltas(
            &read,
            &mut writes,
            &mut overlay,
            [crate::tracked_state::TrackedStateDeltaRef {
                schema_key: &original.key.schema_key,
                file_id: original.key.file_id.as_deref(),
                row_pk: &original.key.row_pk,
                change_id: original.value.change_id,
                commit_id: original.value.commit_id,
                deleted: false,
                created_at: original.value.created_at,
                updated_at: original.value.updated_at,
            }],
            original.value.commit_id,
        )
        .await
        .unwrap()
        .unwrap();
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .unwrap();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        let mut overlay = TrackedStateChunkOverlay::new();
        let mut writes = storage.new_write_set();
        for (label, deleted) in [("update", false), ("delete", true), ("recreate", false)] {
            let mut member = original.clone();
            member.value.change_id = ChangeId::for_test_label(label);
            member.value.commit_id = CommitId::for_test_label(label);
            member.value.created_at = LixTimestamp::from_unix_millis_utc_lossy(30);
            member.value.updated_at = LixTimestamp::from_unix_millis_utc_lossy(40);
            member.value.deleted = deleted;
            let before = writes.stats();
            let next = stage_row_pk_index_from_members(
                &read,
                &mut writes,
                &mut overlay,
                Some(&root),
                &[member],
                CommitId::for_test_label(label),
            )
            .await
            .unwrap();
            assert_eq!(next.as_ref(), Some(&root), "{label} must retain the root");
            assert_eq!(writes.stats(), before, "{label} must stage no chunks");
        }
        let values = super::super::tree::TrackedStateTree::new()
            .get_many_encoded(&read, &root, &[member_key(&original)])
            .await
            .unwrap();
        assert_eq!(values, vec![Some(original.value)]);
        assert_eq!(
            stage_row_pk_index_from_members(
                &read,
                &mut writes,
                &mut overlay,
                Some(&root),
                &[],
                CommitId::for_test_label("empty-child"),
            )
            .await
            .unwrap(),
            Some(root)
        );
        assert_eq!(writes.stats().staged_puts, 0);
    }

    #[tokio::test]
    async fn identity_catalog_scopes_and_staged_parent_child_are_exact() {
        let storage = StorageAdapter::new(Memory::new());
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        let original = catalog_member(&read).await;
        let mut writes = storage.new_write_set();
        let mut overlay = TrackedStateChunkOverlay::new();
        let root = stage_row_pk_index_from_members(
            &read,
            &mut writes,
            &mut overlay,
            None,
            std::slice::from_ref(&original),
            original.value.commit_id,
        )
        .await
        .unwrap()
        .unwrap();
        let mut members = vec![original.clone(); 4];
        members[0].value.updated_at = LixTimestamp::from_unix_millis_utc_lossy(100);
        members[1].key.file_id = Some("first-file".into());
        members[2].key.file_id = Some("second-file".into());
        members[2].value.deleted = true; // New deleted identities must still enter the catalog.
        members[3].key.schema_key = "other-schema".into();
        let child = stage_row_pk_index_from_members(
            &read,
            &mut writes,
            &mut overlay,
            Some(&root),
            &members,
            CommitId::for_test_label("scoped-child"),
        )
        .await
        .unwrap()
        .unwrap();
        assert_ne!(child, root);
        let staged_read = TrackedStateStagedRead::new(&read, &overlay);
        let keys = members.iter().map(member_key).collect::<Vec<_>>();
        let values = super::super::tree::TrackedStateTree::new()
            .get_many_encoded(&staged_read, &child, &keys)
            .await
            .unwrap();
        assert_eq!(values[0], Some(original.value.clone()));
        assert!(
            values
                .iter()
                .all(|value| value.as_ref().is_some_and(|value| !value.deleted()))
        );
        drop(staged_read);
        let before = writes.stats();
        let grandchild = stage_row_pk_index_from_members(
            &read,
            &mut writes,
            &mut overlay,
            Some(&child),
            &members,
            CommitId::for_test_label("scoped-grandchild"),
        )
        .await
        .unwrap();
        assert_eq!(grandchild, Some(child));
        assert_eq!(writes.stats(), before);
    }

    #[tokio::test]
    async fn identity_catalog_empty_base_still_publishes_a_readable_root() {
        let storage = StorageAdapter::new(Memory::new());
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        let mut writes = storage.new_write_set();
        let mut overlay = TrackedStateChunkOverlay::new();
        let root = stage_row_pk_index_from_members(
            &read,
            &mut writes,
            &mut overlay,
            None,
            &[],
            CommitId::for_test_label("empty-catalog"),
        )
        .await
        .unwrap()
        .unwrap();
        let staged_read = TrackedStateStagedRead::new(&read, &overlay);
        let rows = super::super::tree::TrackedStateTree::new()
            .scan(&staged_read, &root, &TrackedStateTreeScanRequest::default())
            .await
            .unwrap();
        assert!(rows.is_empty());
        drop(staged_read);
        let before = writes.stats();
        assert_eq!(
            stage_row_pk_index_from_members(
                &read,
                &mut writes,
                &mut overlay,
                Some(&root),
                &[],
                CommitId::for_test_label("empty-catalog-child"),
            )
            .await
            .unwrap(),
            Some(root)
        );
        assert_eq!(writes.stats(), before);
    }

    #[tokio::test]
    async fn identity_catalog_rejects_secondary_tombstones_without_staging() {
        let storage = StorageAdapter::new(Memory::new());
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        let original = catalog_member(&read).await;
        let mutation = TrackedStateMutation::from_shared(
            member_key(&original),
            encode_value_ref(TrackedStateIndexValueRef {
                change_id: original.value.change_id,
                commit_id: original.value.commit_id,
                deleted: true,
                created_at: original.value.created_at,
                updated_at: original.value.updated_at,
            })
            .into(),
        );
        let mut writes = storage.new_write_set();
        let mut overlay = TrackedStateChunkOverlay::new();
        let root = super::super::tree::TrackedStateTree::new()
            .apply_mutations_with_overlay(
                &read,
                &mut writes,
                &mut overlay,
                None,
                TrackedStateMutationBatch::from_shared(vec![mutation]),
                None,
            )
            .await
            .unwrap()
            .root_id;
        let before = writes.stats();
        let mut missing = original.clone();
        missing.key.row_pk = RowPk::single("new-row");
        let error = stage_row_pk_index_from_members(
            &read,
            &mut writes,
            &mut overlay,
            Some(&root),
            &[missing, original],
            CommitId::for_test_label("invalid-child"),
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, LixError::CODE_INTERNAL_ERROR);
        assert!(
            error
                .message
                .contains("identity catalog contains a tombstone")
        );
        assert_eq!(writes.stats(), before);
    }

    #[test]
    fn synthetic_identity_round_trips_null_values_composites_and_escapes() {
        for expected in identities() {
            let encoded = encode_row_pk_index_key(TrackedStateKeyRef {
                schema_key: &expected.schema_key,
                file_id: expected.file_id.as_deref(),
                row_pk: &expected.row_pk,
            })
            .unwrap();
            assert_eq!(decode_row_pk_index_key(&encoded).unwrap(), expected);
        }
    }

    #[test]
    fn typed_row_pk_prefix_does_not_alias_uuid_and_text() {
        let uuid = RowPk::uuid_from_canonical("01920000-0000-7000-8000-000000000002").unwrap();
        let text = RowPk::single("01920000-0000-7000-8000-000000000002");
        assert_ne!(typed_row_pk_text(&uuid).unwrap(), typed_row_pk_text(&text).unwrap());
    }

    #[test]
    fn scan_request_binds_row_pk_as_the_tree_file_prefix() {
        let row_pks = vec![RowPk::single("a"), RowPk::single("b")];
        let request = row_pk_index_scan_request("schema", &row_pks, false).unwrap();
        assert_eq!(request.schema_keys, ["schema"]);
        assert!(request.row_pks.is_empty());
        assert!(!request.include_tombstones);
        assert_eq!(
            request.file_ids,
            row_pks
                .iter()
                .map(|row_pk| NullableKeyFilter::Value(typed_row_pk_text(row_pk).unwrap()))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn mutation_transform_preserves_primary_and_shares_values() {
        let key = identities().remove(1);
        let value = TrackedStateIndexValueRef {
            change_id: ChangeId::for_test_label("row-pk-index-change"),
            commit_id: CommitId::for_test_label("row-pk-index-commit"),
            deleted: false,
            created_at: LixTimestamp::from_unix_millis_utc_lossy(1),
            updated_at: LixTimestamp::from_unix_millis_utc_lossy(2),
        };
        let mut builder = TrackedStateMutationBatchBuilder::with_row_capacity(1);
        builder.push(
            TrackedStateKeyRef {
                schema_key: &key.schema_key,
                file_id: key.file_id.as_deref(),
                row_pk: &key.row_pk,
            },
            value,
        );
        let original = builder.finish();
        let original_key = original.as_slice()[0].encoded_key.clone();
        let original_value = original.as_slice()[0].encoded_value.clone();
        let (primary, secondary) = with_row_pk_index_mutations(original).unwrap();
        assert_eq!(primary.as_slice()[0].encoded_key, original_key);
        assert_eq!(secondary.as_slice()[0].encoded_value, original_value);
        assert_eq!(
            decode_row_pk_index_key(&secondary.as_slice()[0].encoded_key).unwrap(),
            key
        );
        assert_eq!(secondary.as_slice()[0].encoded_value, encode_value_ref(value));
    }
}
