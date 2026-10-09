//! Canonical conversation pointer for a checkpoint commit.
//!
//! This tiny commit-keyed metadata record is published in the same storage
//! transaction as the checkpoint. Older checkpoints have no record and expose
//! a NULL `lix_log().conversation_id`.

use bytes::Bytes;

use crate::LixError;
use crate::changelog::CommitId;
use crate::storage_adapter::{
    StorageAdapterRead, StorageGetManyRequest, StorageGetOptions, StorageKey, StoragePrecondition,
    StorageProjectedValue, StorageSpace, StorageSpaceId, StorageValue, StorageWriteSet,
};

pub(crate) const CHECKPOINT_CONVERSATION_SPACE: StorageSpace = StorageSpace::mutable(
    StorageSpaceId(0x0008_000d),
    "checkpoint.conversation.v1",
);

/// Partial replicas persist authenticated negative knowledge separately from
/// the canonical UUID pointer space. The key is the commit UUID; the value
/// binds the proof to the source epoch so a rebase cannot reuse stale NULLs.
pub(crate) const PARTIAL_CHECKPOINT_CONVERSATION_COVERAGE_SPACE: StorageSpace =
    StorageSpace::mutable(
        StorageSpaceId(0x0008_000e),
        "sync.partial_checkpoint_conversation_coverage.v1",
    );

const NULL_COVERAGE_MAGIC: &[u8] = b"LIXCCN1";
const MAX_CONVERSATION_POINT_READ_BYTES: usize = 256 * 1024;

pub(crate) fn stage_checkpoint_conversation(
    writes: &mut StorageWriteSet,
    commit_id: CommitId,
    conversation_id: &str,
) -> Result<(), LixError> {
    let id = uuid::Uuid::parse_str(conversation_id).map_err(|_| {
        LixError::new(LixError::CODE_INVALID_PARAM, "checkpoint conversation ID must be a UUID")
    })?;
    if id.to_string() != conversation_id {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "checkpoint conversation ID must be a canonical lowercase UUID",
        ));
    }
    writes.put(
        CHECKPOINT_CONVERSATION_SPACE,
        StorageKey(Bytes::copy_from_slice(commit_id.as_uuid().as_bytes())),
        StorageValue { bytes: Bytes::copy_from_slice(id.as_bytes()) },
    );
    Ok(())
}

pub(crate) fn stage_delete_checkpoint_conversation(
    writes: &mut StorageWriteSet,
    commit_id: CommitId,
) {
    writes.delete(
        CHECKPOINT_CONVERSATION_SPACE,
        StorageKey(Bytes::copy_from_slice(commit_id.as_uuid().as_bytes())),
    );
    stage_delete_partial_null_coverage(writes, commit_id);
}

pub(crate) async fn load_checkpoint_conversation(
    read: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
) -> Result<Option<String>, LixError> {
    Ok(load_checkpoint_conversations(read, &[commit_id])
        .await?
        .into_iter()
        .next()
        .flatten())
}

pub(crate) fn partial_null_coverage_key(commit_id: CommitId) -> StorageKey {
    StorageKey(Bytes::copy_from_slice(commit_id.as_uuid().as_bytes()))
}

pub(crate) fn partial_null_coverage_bytes(epoch_id: &str) -> Result<Bytes, LixError> {
    let epoch = uuid::Uuid::parse_str(epoch_id).map_err(|_| {
        LixError::new(
            LixError::CODE_INVALID_PARAM,
            "partial checkpoint coverage epoch must be a UUID",
        )
    })?;
    if epoch.to_string() != epoch_id {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "partial checkpoint coverage epoch must be canonical",
        ));
    }
    let mut bytes = Vec::with_capacity(NULL_COVERAGE_MAGIC.len() + 16);
    bytes.extend_from_slice(NULL_COVERAGE_MAGIC);
    bytes.extend_from_slice(epoch.as_bytes());
    Ok(Bytes::from(bytes))
}

pub(crate) fn validate_partial_null_coverage(
    bytes: &[u8],
    epoch_id: &str,
) -> Result<bool, LixError> {
    let expected = partial_null_coverage_bytes(epoch_id)?;
    if bytes == expected.as_ref() {
        return Ok(true);
    }
    if bytes.len() != NULL_COVERAGE_MAGIC.len() + 16
        || !bytes.starts_with(NULL_COVERAGE_MAGIC)
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "malformed partial checkpoint conversation coverage",
        ));
    }
    Ok(false)
}

pub(crate) async fn load_checkpoint_conversations(
    read: &(impl StorageAdapterRead + ?Sized),
    commit_ids: &[CommitId],
) -> Result<Vec<Option<String>>, LixError> {
    if commit_ids.is_empty() {
        return Ok(Vec::new());
    }
    let source_epoch_id = crate::sync::load_partial_replica_state(read)
        .await?
        .map(|(state, _)| state.epoch_id().to_owned());
    let pointer_keys = commit_ids
        .iter()
        .map(|commit_id| StorageKey(Bytes::copy_from_slice(commit_id.as_uuid().as_bytes())))
        .collect::<Vec<_>>();
    let pointer_requests = pointer_keys
        .iter()
        .map(|key| StorageGetManyRequest {
            space: CHECKPOINT_CONVERSATION_SPACE,
            keys: std::slice::from_ref(key),
            opts: StorageGetOptions::default(),
        })
        .collect::<Vec<_>>();
    let (pointer_values, _, _) = crate::storage_adapter::collect_bounded_point_pages(
        read,
        &pointer_requests,
        crate::storage_adapter::ReadBudget {
            max_result_bytes: MAX_CONVERSATION_POINT_READ_BYTES,
            max_single_value_bytes: MAX_CONVERSATION_POINT_READ_BYTES,
        },
        MAX_CONVERSATION_POINT_READ_BYTES,
        32,
    )
    .await?;
    let pointer_values = pointer_values.values;
    if pointer_values.len() != commit_ids.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "checkpoint conversation pointer batch cardinality mismatch",
        ));
    }
    let mut values = Vec::with_capacity(commit_ids.len());
    let mut missing = Vec::new();
    for (index, value) in pointer_values.into_iter().enumerate() {
        match value {
            Some(StorageProjectedValue::FullValue(bytes)) => {
                let id = uuid::Uuid::from_slice(&bytes).map_err(|_| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "checkpoint conversation pointer is not a UUID",
                    )
                })?;
                if bytes.len() != 16 {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "checkpoint conversation pointer has invalid length",
                    ));
                }
                values.push(Some(id.to_string()));
            }
            Some(StorageProjectedValue::KeyOnly) => {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "checkpoint conversation read omitted its pointer value",
                ));
            }
            None => {
                values.push(None);
                if source_epoch_id.is_some() {
                    missing.push(index);
                }
            }
        }
    }
    let Some(source_epoch_id) = source_epoch_id.as_deref() else {
        return Ok(values);
    };
    let coverage_keys = commit_ids
        .iter()
        .map(|id| partial_null_coverage_key(*id))
        .collect::<Vec<_>>();
    let coverage_requests = coverage_keys
        .iter()
        .map(|key| StorageGetManyRequest {
            space: PARTIAL_CHECKPOINT_CONVERSATION_COVERAGE_SPACE,
            keys: std::slice::from_ref(key),
            opts: StorageGetOptions::default(),
        })
        .collect::<Vec<_>>();
    let (coverage_values, _, _) = crate::storage_adapter::collect_bounded_point_pages(
        read,
        &coverage_requests,
        crate::storage_adapter::ReadBudget {
            max_result_bytes: MAX_CONVERSATION_POINT_READ_BYTES,
            max_single_value_bytes: MAX_CONVERSATION_POINT_READ_BYTES,
        },
        MAX_CONVERSATION_POINT_READ_BYTES,
        32,
    )
    .await?;
    let coverage_values = coverage_values.values;
    if coverage_values.len() != commit_ids.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "checkpoint conversation coverage batch cardinality mismatch",
        ));
    }
    let mut unresolved = Vec::new();
    let missing = missing.into_iter().collect::<std::collections::BTreeSet<_>>();
    for (index, coverage) in coverage_values.into_iter().enumerate() {
        let covered = match coverage {
            Some(StorageProjectedValue::FullValue(bytes)) => {
                validate_partial_null_coverage(&bytes, source_epoch_id)?
            }
            Some(StorageProjectedValue::KeyOnly) => {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "checkpoint conversation coverage read omitted its value",
                ));
            }
            None => false,
        };
        if !missing.contains(&index) {
            if covered {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "checkpoint conversation pointer conflicts with NULL coverage",
                ));
            }
            continue;
        }
        if covered {
            values[index] = None;
        } else {
            if unresolved.len() < crate::tracked_state::NativeMetadataRef::MAX_MISSING_BATCH {
                unresolved.push(
                crate::tracked_state::NativeMetadataRef::CheckpointConversation(
                    commit_ids[index].to_string(),
                ),
                );
            }
        }
    }
    if !unresolved.is_empty() {
        return Err(crate::tracked_state::NativeMetadataRef::annotate_missing_batch(
            unresolved,
            LixError::new(
                "LIX_NATIVE_METADATA_UNAVAILABLE",
                "checkpoint conversation metadata is not resident",
            ),
        ));
    }
    Ok(values)
}

pub(crate) async fn partial_checkpoint_conversation_residency(
    read: &(impl StorageAdapterRead + ?Sized),
    epoch_id: &str,
    commit_ids: &[CommitId],
) -> Result<Vec<bool>, LixError> {
    if commit_ids.len() > crate::tracked_state::NativeMetadataRef::MAX_MISSING_BATCH {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "checkpoint conversation residency exceeds the native metadata batch bound",
        ));
    }
    let keys = commit_ids
        .iter()
        .map(|id| partial_null_coverage_key(*id))
        .collect::<Vec<_>>();
    let pointer_requests = commit_ids
        .iter()
        .zip(&keys)
        .map(|(_, key)| StorageGetManyRequest {
            space: CHECKPOINT_CONVERSATION_SPACE,
            keys: std::slice::from_ref(key),
            opts: StorageGetOptions::default(),
        })
        .collect::<Vec<_>>();
    let coverage_requests = commit_ids
        .iter()
        .zip(&keys)
        .map(|(_, key)| StorageGetManyRequest {
            space: PARTIAL_CHECKPOINT_CONVERSATION_COVERAGE_SPACE,
            keys: std::slice::from_ref(key),
            opts: StorageGetOptions::default(),
        })
        .collect::<Vec<_>>();
    let mut retained_bytes = 0usize;
    let pointers =
        read_bounded_checkpoint_point_values(read, &pointer_requests, &mut retained_bytes).await?;
    let coverage =
        read_bounded_checkpoint_point_values(read, &coverage_requests, &mut retained_bytes).await?;
    if pointers.len() != commit_ids.len() || coverage.len() != commit_ids.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "checkpoint conversation residency cardinality mismatch",
        ));
    }
    pointers
        .into_iter()
        .zip(coverage)
        .map(|(pointer, coverage)| {
            let pointer_present = match pointer {
                Some(StorageProjectedValue::FullValue(bytes)) => {
                    if bytes.len() != 16 || uuid::Uuid::from_slice(&bytes).is_err() {
                        return Err(LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "checkpoint conversation pointer is malformed",
                        ));
                    }
                    true
                }
                Some(StorageProjectedValue::KeyOnly) => {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "checkpoint conversation residency omitted pointer value",
                    ));
                }
                None => false,
            };
            let null_proven = match coverage {
                Some(StorageProjectedValue::FullValue(bytes)) => {
                    validate_partial_null_coverage(&bytes, epoch_id)?
                }
                Some(StorageProjectedValue::KeyOnly) => {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "checkpoint conversation residency omitted coverage value",
                    ));
                }
                None => false,
            };
            if pointer_present && null_proven {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "checkpoint conversation pointer conflicts with NULL coverage",
                ));
            }
            Ok(pointer_present || null_proven)
        })
        .collect()
}

async fn read_bounded_checkpoint_point_values(
    read: &(impl StorageAdapterRead + ?Sized),
    requests: &[StorageGetManyRequest<'_>],
    retained_bytes: &mut usize,
) -> Result<Vec<Option<StorageProjectedValue>>, LixError> {
    let mut values = Vec::with_capacity(requests.len());
    let mut offset = 0usize;
    while offset < requests.len() {
        let remaining = MAX_CONVERSATION_POINT_READ_BYTES.saturating_sub(*retained_bytes);
        let budget = crate::storage_adapter::ReadBudget {
            max_result_bytes: remaining,
            max_single_value_bytes: remaining,
        };
        let (page, bytes) =
            crate::storage_adapter::read_bounded_point_page(read, requests, offset, 32, budget)
                .await?;
        offset = page.next_offset.unwrap_or(requests.len());
        *retained_bytes = (*retained_bytes)
            .checked_add(bytes)
            .filter(|total| *total <= MAX_CONVERSATION_POINT_READ_BYTES)
            .ok_or(crate::storage_adapter::StorageError::ReadBudgetExceeded {
                singleton: false,
            })?;
        values.extend(page.values);
    }
    Ok(values)
}

pub(crate) fn stage_partial_null_coverage(
    writes: &mut StorageWriteSet,
    epoch_id: &str,
    commit_id: CommitId,
) -> Result<(StorageSpace, StorageKey, StorageValue), LixError> {
    let key = partial_null_coverage_key(commit_id);
    let value = StorageValue {
        bytes: partial_null_coverage_bytes(epoch_id)?,
    };
    writes.put(PARTIAL_CHECKPOINT_CONVERSATION_COVERAGE_SPACE, key.clone(), value.clone());
    Ok((PARTIAL_CHECKPOINT_CONVERSATION_COVERAGE_SPACE, key, value))
}

pub(crate) fn stage_delete_partial_null_coverage(
    writes: &mut StorageWriteSet,
    commit_id: CommitId,
) {
    writes.delete(
        PARTIAL_CHECKPOINT_CONVERSATION_COVERAGE_SPACE,
        partial_null_coverage_key(commit_id),
    );
}

/// Stage an authenticated import fact for an existing or new checkpoint. A
/// partial NULL is publishable only with the exact admission receipt; full
/// storage uses canonical pointer absence as complete knowledge.
pub(crate) async fn stage_checkpoint_conversation_fact(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    preconditions: &mut Vec<StoragePrecondition>,
    commit_id: CommitId,
    record_exists: bool,
    conversation_id: Option<&str>,
    partial_admission: Option<(&str, &Bytes)>,
) -> Result<bool, LixError> {
    let expected_id = conversation_id
        .map(|id| {
            let parsed = uuid::Uuid::parse_str(id).map_err(|_| {
                LixError::new(LixError::CODE_INVALID_PARAM, "checkpoint conversation ID must be a UUID")
            })?;
            if parsed.to_string() != id {
                return Err(LixError::new(LixError::CODE_INVALID_PARAM, "checkpoint conversation ID must be canonical"));
            }
            Ok(parsed)
        })
        .transpose()?;
    let pointer_key = partial_null_coverage_key(commit_id);
    let coverage_key = partial_null_coverage_key(commit_id);
    let pointer_key_bytes = pointer_key.0.clone();
    let coverage_key_bytes = coverage_key.0.clone();
    let pointer_before = writes.staged_value(CHECKPOINT_CONVERSATION_SPACE, &pointer_key_bytes);
    let coverage_before = writes.staged_value(PARTIAL_CHECKPOINT_CONVERSATION_COVERAGE_SPACE, &coverage_key_bytes);
    let pointer_request = [StorageGetManyRequest {
        space: CHECKPOINT_CONVERSATION_SPACE,
        keys: std::slice::from_ref(&pointer_key),
        opts: StorageGetOptions::default(),
    }];
    let coverage_request = [StorageGetManyRequest {
        space: PARTIAL_CHECKPOINT_CONVERSATION_COVERAGE_SPACE,
        keys: std::slice::from_ref(&coverage_key),
        opts: StorageGetOptions::default(),
    }];
    let mut pointer_values = read.get_many(&pointer_request).await?.values;
    if pointer_values.len() != 1 {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "checkpoint conversation pointer cardinality mismatch",
        ));
    }
    let pointer = pointer_values.pop().flatten();
    let coverage = if partial_admission.is_some() {
        let mut values = read.get_many(&coverage_request).await?.values;
        if values.len() != 1 {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "checkpoint conversation coverage cardinality mismatch",
            ));
        }
        values.pop().flatten()
    } else {
        None
    };
    let pointer_bytes = match pointer {
        Some(StorageProjectedValue::FullValue(bytes)) => {
            if bytes.len() != 16 {
                return Err(LixError::new(LixError::CODE_INTERNAL_ERROR, "checkpoint conversation pointer has invalid length"));
            }
            uuid::Uuid::from_slice(&bytes).map_err(|_| LixError::new(LixError::CODE_INTERNAL_ERROR, "checkpoint conversation pointer is malformed"))?;
            Some(bytes)
        }
        Some(StorageProjectedValue::KeyOnly) => return Err(LixError::new(LixError::CODE_INTERNAL_ERROR, "checkpoint conversation pointer omitted its value")),
        None => None,
    };
    let current_coverage = if let Some((epoch_id, _)) = partial_admission {
        match coverage {
            Some(StorageProjectedValue::FullValue(bytes)) => {
                let current = validate_partial_null_coverage(&bytes, epoch_id)?;
                Some((bytes, current))
            }
            Some(StorageProjectedValue::KeyOnly) => return Err(LixError::new(LixError::CODE_INTERNAL_ERROR, "checkpoint conversation coverage omitted its value")),
            None => None,
        }
    } else {
        None
    };
    if let Some(staged) = writes.staged_value(CHECKPOINT_CONVERSATION_SPACE, &pointer_key.0)
        && !expected_id.as_ref().is_some_and(|expected| staged.as_ref() == expected.as_bytes())
    {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "staged checkpoint conversation pointer conflicts with imported fact",
        ));
    }
    if let (Some(staged), Some((epoch_id, _))) = (
        writes.staged_value(PARTIAL_CHECKPOINT_CONVERSATION_COVERAGE_SPACE, &coverage_key.0),
        partial_admission,
    ) {
        let staged_is_current = validate_partial_null_coverage(&staged, epoch_id)?;
        if expected_id.is_some() && staged_is_current {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "staged checkpoint NULL proof conflicts with imported pointer",
            ));
        }
    }
    if pointer_bytes.as_ref().is_some_and(|bytes| expected_id.as_ref().is_none_or(|expected| bytes.as_ref() != expected.as_bytes())) {
        return Err(LixError::new(LixError::CODE_INVALID_PARAM, "checkpoint conversation import conflicts with resident pointer"));
    }
    if record_exists && partial_admission.is_none() && expected_id.is_some() && pointer_bytes.is_none() {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "existing full checkpoint has no canonical conversation pointer",
        ));
    }
    if expected_id.is_some() && current_coverage.as_ref().is_some_and(|(_, current)| *current) {
        return Err(LixError::new(LixError::CODE_INVALID_PARAM, "checkpoint conversation import conflicts with resident NULL proof"));
    }
    preconditions.push(match &pointer_bytes {
        Some(bytes) => StoragePrecondition::KeyValueEquals {
            space: CHECKPOINT_CONVERSATION_SPACE,
            key: pointer_key.clone(),
            expected: bytes.clone(),
        },
        None => StoragePrecondition::KeyAbsent {
            space: CHECKPOINT_CONVERSATION_SPACE,
            key: pointer_key.clone(),
        },
    });
    match expected_id {
        Some(id) => {
            if pointer_bytes.is_none() {
                writes.put(CHECKPOINT_CONVERSATION_SPACE, pointer_key, StorageValue { bytes: Bytes::copy_from_slice(id.as_bytes()) });
            }
            if let Some((bytes, _)) = current_coverage {
                preconditions.push(StoragePrecondition::KeyValueEquals {
                    space: PARTIAL_CHECKPOINT_CONVERSATION_COVERAGE_SPACE,
                    key: coverage_key.clone(),
                    expected: bytes,
                });
                writes.delete(PARTIAL_CHECKPOINT_CONVERSATION_COVERAGE_SPACE, coverage_key);
            } else if partial_admission.is_some() {
                preconditions.push(StoragePrecondition::KeyAbsent {
                    space: PARTIAL_CHECKPOINT_CONVERSATION_COVERAGE_SPACE,
                    key: coverage_key,
                });
            }
        }
        None => if let Some((epoch_id, _receipt)) = partial_admission {
            let coverage_value = partial_null_coverage_bytes(epoch_id)?;
            match current_coverage {
                Some((_bytes, true)) => {}
                Some((bytes, false)) => {
                    preconditions.push(StoragePrecondition::KeyValueEquals {
                        space: PARTIAL_CHECKPOINT_CONVERSATION_COVERAGE_SPACE,
                        key: coverage_key.clone(),
                        expected: bytes,
                    });
                    writes.put(PARTIAL_CHECKPOINT_CONVERSATION_COVERAGE_SPACE, coverage_key, StorageValue { bytes: coverage_value });
                }
                None => {
                    preconditions.push(StoragePrecondition::KeyAbsent {
                        space: PARTIAL_CHECKPOINT_CONVERSATION_COVERAGE_SPACE,
                        key: coverage_key.clone(),
                    });
                    writes.put(PARTIAL_CHECKPOINT_CONVERSATION_COVERAGE_SPACE, coverage_key, StorageValue { bytes: coverage_value });
                }
            }
        },
    }
    if let Some((_, receipt)) = partial_admission {
        let receipt_key = crate::sync::partial_replica_state_key();
        let receipt_space = crate::sync::PARTIAL_REPLICA_STATE_SPACE;
        if !preconditions.iter().any(|condition| {
            matches!(condition, StoragePrecondition::KeyValueEquals { space, key, .. }
                if *space == receipt_space && key == &receipt_key)
        }) {
            preconditions.push(StoragePrecondition::KeyValueEquals {
                space: receipt_space,
                key: receipt_key,
                expected: receipt.clone(),
            });
        }
    }
    Ok(pointer_before != writes.staged_value(CHECKPOINT_CONVERSATION_SPACE, &pointer_key_bytes)
        || coverage_before
            != writes.staged_value(PARTIAL_CHECKPOINT_CONVERSATION_COVERAGE_SPACE, &coverage_key_bytes))
}

#[cfg(test)]
mod tests;
