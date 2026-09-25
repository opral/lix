//! Canonical conversation pointer for a checkpoint commit.
//!
//! This tiny commit-keyed metadata record is published in the same storage
//! transaction as the checkpoint. Older checkpoints have no record and expose
//! a NULL `lix_log().conversation_id`.

use bytes::Bytes;

use crate::LixError;
use crate::changelog::CommitId;
use crate::storage_adapter::{
    PointReadPlan, StorageAdapterRead, StorageGetOptions, StorageKey, StorageProjectedValue,
    StorageSpace, StorageSpaceId, StorageValue, StorageWriteSet,
};

pub(crate) const CHECKPOINT_CONVERSATION_SPACE: StorageSpace = StorageSpace::mutable(
    StorageSpaceId(0x0008_000d),
    "checkpoint.conversation.v1",
);

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
}

pub(crate) async fn load_checkpoint_conversation(
    read: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
) -> Result<Option<String>, LixError> {
    let key = StorageKey(Bytes::copy_from_slice(commit_id.as_uuid().as_bytes()));
    let result = PointReadPlan::new(CHECKPOINT_CONVERSATION_SPACE, &[key])
        .materialize(read, StorageGetOptions::default())
        .await?;
    let Some(StorageProjectedValue::FullValue(bytes)) = result.value.into_iter().next().flatten() else {
        return Ok(None);
    };
    let id = uuid::Uuid::from_slice(&bytes).map_err(|_| {
        LixError::new(LixError::CODE_INTERNAL_ERROR, "checkpoint conversation pointer is not a UUID")
    })?;
    Ok(Some(id.to_string()))
}
