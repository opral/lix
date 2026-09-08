//! Bounded proof of ref requests prepared against one authority coordinate.
//! These records authorize recognizing an old acknowledgment after a restore;
//! they do not change commit ancestry or authorize a stale server CAS.

use std::collections::BTreeSet;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::LixError;
use crate::storage_adapter::{
    StorageAdapterRead, StorageGetManyRequest, StorageGetOptions, StorageKey, StoragePrecondition,
    StorageProjectedValue, StorageSpace, StorageSpaceId, StorageWriteSet, ValueSemantics,
    exact_get_many,
};

use super::protocol::SyncRefUpdate;

pub(crate) const SYNC_UPLOAD_PROOF_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0007_0018),
    "sync.prepared_ref_proof.v1",
    ValueSemantics::Mutable,
);
const MAX_TARGETS: usize = 64;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct PreparedProof {
    pub(super) expected_head: Option<String>,
    pub(super) expected_checkpoint: Option<String>,
    pub(super) targets: BTreeSet<(String, String)>,
}

fn proof_key(branch_id: &str) -> Result<StorageKey, LixError> {
    let branch = uuid::Uuid::parse_str(branch_id).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("invalid prepared-ref branch: {error}"),
        )
    })?;
    Ok(StorageKey(Bytes::copy_from_slice(branch.as_bytes())))
}

fn proof_precondition(key: StorageKey, raw: Option<Bytes>) -> StoragePrecondition {
    match raw {
        Some(expected) => StoragePrecondition::KeyValueEquals {
            space: SYNC_UPLOAD_PROOF_SPACE,
            key,
            expected,
        },
        None => StoragePrecondition::KeyAbsent {
            space: SYNC_UPLOAD_PROOF_SPACE,
            key,
        },
    }
}

pub(super) async fn load_proof(
    read: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
) -> Result<(Option<PreparedProof>, Option<Bytes>), LixError> {
    let key = proof_key(branch_id)?;
    let result = exact_get_many(
        read,
        &[StorageGetManyRequest {
            space: SYNC_UPLOAD_PROOF_SPACE,
            keys: std::slice::from_ref(&key),
            opts: StorageGetOptions::default(),
        }],
    )
    .await?;
    let Some(value) = result.values.into_iter().next().flatten() else {
        return Ok((None, None));
    };
    let StorageProjectedValue::FullValue(raw) = value else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "prepared-ref proof omitted its value",
        ));
    };
    let proof: PreparedProof = serde_json::from_slice(&raw).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("invalid prepared-ref proof: {error}"),
        )
    })?;
    if proof.targets.len() > MAX_TARGETS
        || proof.expected_head.is_some() != proof.expected_checkpoint.is_some()
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "prepared-ref proof has invalid bounds or source coordinate",
        ));
    }
    Ok((Some(proof), Some(raw)))
}

/// The caller also guards the replica receipt and destructive-state generation.
/// A full proof refuses another target instead of forgetting an in-flight one.
pub(super) async fn stage_merge_proof(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    preconditions: &mut Vec<StoragePrecondition>,
    update: &SyncRefUpdate,
) -> Result<(), LixError> {
    let (head, checkpoint) = match (&update.head_commit_id, &update.checkpoint_commit_id) {
        (None, None) => return Ok(()),
        (Some(head), Some(checkpoint)) => (head, checkpoint),
        _ => {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "prepared ref requires a complete target coordinate",
            ));
        }
    };
    if update.expected_head_commit_id.is_some() != update.expected_checkpoint_commit_id.is_some() {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "prepared ref requires a complete source coordinate",
        ));
    }
    let (previous, raw) = load_proof(read, &update.branch_id).await?;
    let mut proof = previous
        .filter(|proof| {
            proof.expected_head == update.expected_head_commit_id
                && proof.expected_checkpoint == update.expected_checkpoint_commit_id
        })
        .unwrap_or_else(|| PreparedProof {
            expected_head: update.expected_head_commit_id.clone(),
            expected_checkpoint: update.expected_checkpoint_commit_id.clone(),
            targets: BTreeSet::new(),
        });
    let target = (head.clone(), checkpoint.clone());
    if !proof.targets.contains(&target) && proof.targets.len() >= MAX_TARGETS {
        return Err(LixError::new(
            "LIX_SYNC_PREPARED_REF_LIMIT",
            "too many ref targets prepared before authority acknowledgment",
        ));
    }
    proof.targets.insert(target);
    let encoded = serde_json::to_vec(&proof).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("encode prepared-ref proof: {error}"),
        )
    })?;
    let key = proof_key(&update.branch_id)?;
    preconditions.push(proof_precondition(key.clone(), raw));
    writes.put(SYNC_UPLOAD_PROOF_SPACE, key, encoded);
    Ok(())
}

/// Restore copies only targets prepared against its exact expected source.
/// Guard absence too: preparation racing this read must invalidate the restore.
pub(super) async fn load_copy_targets(
    read: &(impl StorageAdapterRead + ?Sized),
    preconditions: &mut Vec<StoragePrecondition>,
    branch_id: &str,
    expected_head: &str,
    expected_checkpoint: &str,
) -> Result<BTreeSet<(String, String)>, LixError> {
    let (proof, raw) = load_proof(read, branch_id).await?;
    preconditions.push(proof_precondition(proof_key(branch_id)?, raw));
    Ok(proof
        .filter(|proof| {
            proof.expected_head.as_deref() == Some(expected_head)
                && proof.expected_checkpoint.as_deref() == Some(expected_checkpoint)
        })
        .map(|proof| proof.targets)
        .unwrap_or_default())
}
