//! Authority-owned terminal restart fencing for one exact expired KV attempt.
//! Compact immutable restart records are retained: UUID-only old requests have
//! no finite replay horizon, so deleting this fence would permit resurrection.
use super::{PartialMergeReceipt, PartialMergeRequest};
use crate::LixError;
use crate::storage_adapter::{
    PointReadPlan, StorageAdapterRead, StorageKey, StoragePrecondition, StorageProjectedValue,
    StorageSpace, StorageSpaceId, StorageValue, StorageWriteSet, ValueSemantics,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

// Reserve through registry owner before registering this module.
pub(crate) const PARTIAL_ATTEMPT_RESTART_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0007_001e),
    "sync.partial_attempt_restart.v1",
    ValueSemantics::Immutable,
);
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct PartialAttemptRestartRequest {
    pub old: PartialMergeRequest,
    pub next_attempt_id: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct PartialAttemptRestartReceipt {
    version: u32,
    pub repository_id: String,
    pub account_id: String,
    pub request: PartialAttemptRestartRequest,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "outcome",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub(crate) enum PartialAttemptRestartOutcome {
    Committed {
        repository_id: String,
        account_id: String,
        receipt: PartialMergeReceipt,
    },
    Restarted {
        receipt: PartialAttemptRestartReceipt,
    },
}
impl PartialAttemptRestartRequest {
    pub(crate) fn validate(&self) -> Result<(), LixError> {
        validate_request(self)
    }
}
impl PartialAttemptRestartReceipt {
    pub(crate) fn validate_for(
        &self,
        repository: &str,
        account: &str,
        request: &PartialAttemptRestartRequest,
    ) -> Result<(), LixError> {
        request.validate()?;
        key(repository, account, &request.old)?;
        if self.version != 1
            || self.repository_id != repository
            || self.account_id != account
            || &self.request != request
        {
            return Err(invalid(
                "restart receipt changed authenticated identity or exact request",
            ));
        }
        Ok(())
    }
}
impl PartialAttemptRestartOutcome {
    pub(crate) fn validate_for(
        &self,
        repository: &str,
        account: &str,
        request: &PartialAttemptRestartRequest,
    ) -> Result<(), LixError> {
        request.validate()?;
        key(repository, account, &request.old)?;
        match self {
            Self::Committed {
                repository_id,
                account_id,
                receipt,
            } => {
                if repository_id != repository || account_id != account {
                    return Err(invalid("committed outcome changed authenticated identity"));
                }
                receipt.validate_for(&request.old)
            }
            Self::Restarted { receipt } => receipt.validate_for(repository, account, request),
        }
    }
}
fn invalid(message: &str) -> LixError {
    LixError::new("LIX_PARTIAL_ATTEMPT_RESTART_INVALID", message)
}
fn key(repo: &str, account: &str, request: &PartialMergeRequest) -> Result<StorageKey, LixError> {
    request.validate()?;
    let mut bytes = Vec::with_capacity(64);
    for id in [repo, account, &request.branch_id, &request.attempt_id] {
        bytes.extend_from_slice(
            &crate::storage_codec::id_string::uuid_bytes_from_canonical(id)
                .ok_or_else(|| invalid("restart identity must be canonical UUID"))?,
        );
    }
    Ok(StorageKey(Bytes::from(bytes)))
}
pub(super) async fn load_restart(
    read: &(impl StorageAdapterRead + ?Sized),
    repo: &str,
    account: &str,
    old: &PartialMergeRequest,
) -> Result<Option<PartialAttemptRestartReceipt>, LixError> {
    let address = key(repo, account, old)?;
    let value = PointReadPlan::new(PARTIAL_ATTEMPT_RESTART_SPACE, &[address])
        .materialize(read, Default::default())
        .await?
        .value
        .pop()
        .flatten();
    let Some(value) = value else { return Ok(None) };
    let StorageProjectedValue::FullValue(bytes) = value else {
        return Err(invalid("restart record projection omitted"));
    };
    if bytes.len() > 4096 {
        return Err(invalid("restart record exceeds bound"));
    }
    let receipt: PartialAttemptRestartReceipt =
        serde_json::from_slice(&bytes).map_err(|_| invalid("restart record malformed"))?;
    if receipt.version != 1
        || receipt.repository_id != repo
        || receipt.account_id != account
        || &receipt.request.old != old
    {
        return Err(invalid("restart record binding mismatch"));
    }
    validate_request(&receipt.request)?;
    Ok(Some(receipt))
}
fn validate_request(request: &PartialAttemptRestartRequest) -> Result<(), LixError> {
    request.old.validate()?;
    if request.next_attempt_id == request.old.attempt_id
        || crate::storage_codec::id_string::uuid_bytes_from_canonical(&request.next_attempt_id)
            .is_none()
    {
        return Err(invalid("restart must select a new canonical attempt UUID"));
    }
    Ok(())
}
/// REQUIRED in every body import, merge transaction and attempt renewal before
/// mutation. The returned KeyAbsent guard must accompany their atomic commit.
/// It fences requests prepared before restart and rejects those arriving later.
pub(crate) async fn require_unrestarted_identity(
    read: &(impl StorageAdapterRead + ?Sized),
    repo: &str,
    account: &str,
    branch: &str,
    attempt: &str,
) -> Result<StoragePrecondition, LixError> {
    let mut bytes = Vec::with_capacity(64);
    for id in [repo, account, branch, attempt] {
        bytes.extend_from_slice(
            &crate::storage_codec::id_string::uuid_bytes_from_canonical(id)
                .ok_or_else(|| invalid("restart fence identity malformed"))?,
        );
    }
    let address = StorageKey(Bytes::from(bytes));
    let value = PointReadPlan::new(
        PARTIAL_ATTEMPT_RESTART_SPACE,
        std::slice::from_ref(&address),
    )
    .materialize(read, Default::default())
    .await?
    .value
    .pop()
    .flatten();
    if let Some(value) = value {
        let StorageProjectedValue::FullValue(bytes) = value else {
            return Err(invalid("restart fence projection omitted"));
        };
        if bytes.len() > 4096 {
            return Err(invalid("restart fence exceeds bound"));
        }
        let receipt: PartialAttemptRestartReceipt =
            serde_json::from_slice(&bytes).map_err(|_| invalid("restart fence malformed"))?;
        receipt.request.validate()?;
        if receipt.version != 1
            || receipt.repository_id != repo
            || receipt.account_id != account
            || receipt.request.old.branch_id != branch
            || receipt.request.old.attempt_id != attempt
        {
            return Err(invalid("restart fence identity mismatch"));
        }
        return Err(LixError::new(
            "LIX_PARTIAL_ATTEMPT_RESTARTED",
            "attempt was terminally restarted; recover its immutable restart receipt",
        ));
    }
    Ok(StoragePrecondition::KeyAbsent {
        space: PARTIAL_ATTEMPT_RESTART_SPACE,
        key: address,
    })
}
pub(crate) async fn require_unrestarted_attempt(
    read: &(impl StorageAdapterRead + ?Sized),
    repo: &str,
    account: &str,
    request: &PartialMergeRequest,
) -> Result<StoragePrecondition, LixError> {
    request.validate()?;
    require_unrestarted_identity(read, repo, account, &request.branch_id, &request.attempt_id).await
}
/// Caller supplies authenticated repository/account and commits returned writes
/// with ALL guards durably. A response is not sent before that commit succeeds.
/// CAS failure requires rereading exact terminal receipt; never an inferred ACK.
pub(crate) async fn stage_restart_expired_attempt(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    repo: &str,
    account: &str,
    request: &PartialAttemptRestartRequest,
    now_ms: u64,
) -> Result<(PartialAttemptRestartOutcome, Vec<StoragePrecondition>), LixError> {
    validate_request(request)?;
    // Exact old M receipt wins over every pin/expiry/restart consideration.
    if let Some(receipt) =
        super::load_authority_merge_receipt(read, repo, account, &request.old).await?
    {
        return Ok((
            PartialAttemptRestartOutcome::Committed {
                repository_id: repo.into(),
                account_id: account.into(),
                receipt,
            },
            vec![],
        ));
    }
    if let Some(receipt) = load_restart(read, repo, account, &request.old).await? {
        if receipt.request != *request {
            return Err(invalid("restart ID reused with changed successor"));
        }
        return Ok((PartialAttemptRestartOutcome::Restarted { receipt }, vec![]));
    }
    let mut guards = vec![require_unrestarted_attempt(read, repo, account, &request.old).await?];
    // Narrow receipt-owner helper returns KeyAbsent after validating canonical key.
    guards.push(
        super::partial_authority_merge_receipt::absent_receipt_guard(repo, account, &request.old)?,
    );
    // GC owner checks exact binding digest if record remains, requires expiration,
    // stages deletion under exact record CAS (or KeyAbsent if already collected),
    // and adds repository mutation revision guard. No closure transfer: new
    // attempt starts at B and reuploads its captured locally retained bodies.
    guards.extend(
        crate::gc::stage_revoke_expired_upload_attempt(
            read,
            writes,
            repo,
            account,
            &request.old,
            now_ms,
        )
        .await?,
    );
    let receipt = PartialAttemptRestartReceipt {
        version: 1,
        repository_id: repo.into(),
        account_id: account.into(),
        request: request.clone(),
    };
    let bytes =
        serde_json::to_vec(&receipt).map_err(|_| invalid("restart receipt encode failed"))?;
    if bytes.len() > 4096 {
        return Err(invalid("restart receipt exceeds fixed bound"));
    }
    writes.put(
        PARTIAL_ATTEMPT_RESTART_SPACE,
        key(repo, account, &request.old)?,
        StorageValue {
            bytes: Bytes::from(bytes),
        },
    );
    Ok((PartialAttemptRestartOutcome::Restarted { receipt }, guards))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn request() -> PartialAttemptRestartRequest {
        let id = || uuid::Uuid::now_v7().to_string();
        let checkpoint = id();
        PartialAttemptRestartRequest {
            old: PartialMergeRequest {
                attempt_id: id(),
                branch_id: id(),
                base_commit_id: id(),
                expected_authority_head_commit_id: id(),
                captured_local_head_commit_id: id(),
                expected_authority_checkpoint_commit_id: checkpoint.clone(),
                captured_local_checkpoint_commit_id: checkpoint.clone(),
                checkpoint_commit_id: checkpoint,
                global_head_commit_id: id(),
                global_checkpoint_commit_id: id(),
            },
            next_attempt_id: id(),
        }
    }
    #[test]
    fn restart_outcomes_bind_identity_and_exact_successor() {
        let request = request();
        let repo = uuid::Uuid::now_v7().to_string();
        let account = uuid::Uuid::now_v7().to_string();
        let receipt = PartialAttemptRestartReceipt {
            version: 1,
            repository_id: repo.clone(),
            account_id: account.clone(),
            request: request.clone(),
        };
        let outcome = PartialAttemptRestartOutcome::Restarted {
            receipt: receipt.clone(),
        };
        outcome.validate_for(&repo, &account, &request).unwrap();
        assert!(
            outcome
                .validate_for(&repo, &uuid::Uuid::now_v7().to_string(), &request)
                .is_err()
        );
        let mut changed = request.clone();
        changed.next_attempt_id = uuid::Uuid::now_v7().to_string();
        assert!(outcome.validate_for(&repo, &account, &changed).is_err());
        let committed = PartialAttemptRestartOutcome::Committed {
            repository_id: repo.clone(),
            account_id: account.clone(),
            receipt: PartialMergeReceipt {
                request: request.old.clone(),
                merge_commit_id: uuid::Uuid::now_v7().to_string(),
            },
        };
        committed.validate_for(&repo, &account, &request).unwrap();
        assert!(
            committed
                .validate_for(&uuid::Uuid::now_v7().to_string(), &account, &request)
                .is_err()
        );
        changed.old.captured_local_head_commit_id = uuid::Uuid::now_v7().to_string();
        assert!(committed.validate_for(&repo, &account, &changed).is_err());
    }
    #[test]
    fn restart_wire_rejects_unknown_fields_and_reused_old_attempt() {
        let request = request();
        let mut encoded = serde_json::to_value(&request).unwrap();
        encoded["ignored"] = true.into();
        assert!(serde_json::from_value::<PartialAttemptRestartRequest>(encoded).is_err());
        let mut invalid = request.clone();
        invalid.next_attempt_id = invalid.old.attempt_id.clone();
        assert!(invalid.validate().is_err());
        let receipt = PartialAttemptRestartReceipt {
            version: 1,
            repository_id: uuid::Uuid::now_v7().to_string(),
            account_id: uuid::Uuid::now_v7().to_string(),
            request,
        };
        let mut encoded =
            serde_json::to_value(PartialAttemptRestartOutcome::Restarted { receipt }).unwrap();
        encoded["ignored"] = true.into();
        assert!(serde_json::from_value::<PartialAttemptRestartOutcome>(encoded).is_err());
    }
}
