//! Authority retention for accepted, complete partial-replica upload waves.
//! No body is acknowledged without its root pin in the same storage commit.
use crate::storage_adapter::{
    PointReadPlan, StorageAdapterRead, StorageKey, StoragePrecondition, StorageProjectedValue,
    StorageSpace, StorageSpaceId, StorageValue, StorageWriteSet,
};
use crate::{LixError, changelog::CommitId};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub(crate) const NATIVE_UPLOAD_ATTEMPT_SPACE: StorageSpace =
    StorageSpace::mutable(StorageSpaceId(0x0008_000b), "gc.native_upload_attempt.v1");
const TTL_MS: u64 = 300_000;
const MAX_BYTES: usize = 2048;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct NativeUploadAttemptIdentity {
    pub repository_id: String,
    pub account_id: String,
    pub branch_id: String,
    pub attempt_id: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct NativeUploadAttempt {
    version: u32,
    identity: NativeUploadAttemptIdentity,
    binding_digest: [u8; 32],
    accepted_tip: String,
    accepted_commits: usize,
    anchors: Vec<String>,
    merge_commit_id: Option<String>,
    pub expires_at_ms: u64,
}
fn invalid(message: &str) -> LixError {
    LixError::new("LIX_NATIVE_UPLOAD_ATTEMPT_INVALID", message)
}
fn expired() -> LixError {
    LixError::new(
        "LIX_NATIVE_UPLOAD_ATTEMPT_EXPIRED",
        "upload attempt retention expired; preserve local commits and restart reconciliation",
    )
}
fn key(identity: &NativeUploadAttemptIdentity) -> Result<StorageKey, LixError> {
    let mut bytes = Vec::with_capacity(64);
    for value in [
        &identity.repository_id,
        &identity.account_id,
        &identity.branch_id,
        &identity.attempt_id,
    ] {
        let id = crate::storage_codec::id_string::uuid_bytes_from_canonical(value)
            .ok_or_else(|| invalid("attempt identity must use canonical UUIDs"))?;
        bytes.extend_from_slice(&id);
    }
    Ok(StorageKey(Bytes::from(bytes)))
}
impl NativeUploadAttempt {
    fn validate(&self) -> Result<(), LixError> {
        key(&self.identity)?;
        if self.version != 1
            || self.anchors.is_empty()
            || self.anchors.len() > 4
            || self.expires_at_ms == 0
            || self.accepted_commits == 0
            || self.accepted_commits > 1024
        {
            return Err(invalid("invalid upload attempt retention shape"));
        }
        CommitId::parse_lix(&self.accepted_tip, "accepted upload tip")?;
        if let Some(merge) = &self.merge_commit_id {
            CommitId::parse_lix(merge, "retained authority merge")?;
        }
        let roots = self
            .anchors
            .iter()
            .map(|id| CommitId::parse_lix(id, "retained upload anchor"))
            .collect::<Result<BTreeSet<_>, _>>()?;
        if roots.len() != self.anchors.len() {
            return Err(invalid("duplicate upload retention anchors"));
        }
        Ok(())
    }
    pub(crate) fn accepted_tip(&self) -> Result<CommitId, LixError> {
        CommitId::parse_lix(&self.accepted_tip, "accepted upload tip")
    }
    pub(crate) fn is_terminal(&self) -> bool {
        self.merge_commit_id.is_some()
    }
    fn roots(&self) -> Result<BTreeSet<CommitId>, LixError> {
        self.anchors
            .iter()
            .chain(std::iter::once(
                self.merge_commit_id.as_ref().unwrap_or(&self.accepted_tip),
            ))
            .map(|id| CommitId::parse_lix(id, "retained upload root"))
            .collect()
    }
}
pub(crate) async fn load_native_upload_attempt(
    read: &(impl StorageAdapterRead + ?Sized),
    identity: &NativeUploadAttemptIdentity,
) -> Result<Option<(NativeUploadAttempt, Bytes)>, LixError> {
    let values = PointReadPlan::new(NATIVE_UPLOAD_ATTEMPT_SPACE, &[key(identity)?])
        .materialize(read, Default::default())
        .await?
        .value;
    let Some(value) = values.into_iter().next().flatten() else {
        return Ok(None);
    };
    let StorageProjectedValue::FullValue(bytes) = value else {
        return Err(invalid("attempt read omitted payload"));
    };
    if bytes.len() > MAX_BYTES {
        return Err(invalid("attempt exceeds encoded bound"));
    }
    let state: NativeUploadAttempt =
        serde_json::from_slice(&bytes).map_err(|_| invalid("malformed attempt record"))?;
    state.validate()?;
    if &state.identity != identity {
        return Err(invalid("attempt identity disagrees with key"));
    }
    Ok(Some((state, bytes)))
}
fn stage(writes: &mut StorageWriteSet, state: &NativeUploadAttempt) -> Result<(), LixError> {
    state.validate()?;
    let bytes = serde_json::to_vec(state).map_err(|_| invalid("attempt serialization failed"))?;
    if bytes.len() > MAX_BYTES {
        return Err(invalid("attempt exceeds encoded bound"));
    }
    writes.put(
        NATIVE_UPLOAD_ATTEMPT_SPACE,
        key(&state.identity)?,
        StorageValue {
            bytes: bytes.into(),
        },
    );
    Ok(())
}
async fn guards(
    read: &(impl StorageAdapterRead + ?Sized),
    identity: &NativeUploadAttemptIdentity,
    previous: Option<Bytes>,
) -> Result<Vec<StoragePrecondition>, LixError> {
    let revision = crate::storage_adapter::load_repository_mutation_revision(read).await?;
    Ok(vec![
        match previous {
            Some(expected) => StoragePrecondition::KeyValueEquals {
                space: NATIVE_UPLOAD_ATTEMPT_SPACE,
                key: key(identity)?,
                expected,
            },
            None => StoragePrecondition::KeyAbsent {
                space: NATIVE_UPLOAD_ATTEMPT_SPACE,
                key: key(identity)?,
            },
        },
        crate::storage_adapter::repository_mutation_revision_precondition(revision),
        crate::sync::require_unrestarted_identity(
            read,
            &identity.repository_id,
            &identity.account_id,
            &identity.branch_id,
            &identity.attempt_id,
        )
        .await?,
    ])
}

/// Initial ownership anchors must already be protected by observed authority
/// controls and a proved B→R ancestry; importer supplies those control CASs in
/// its atomic options. The private native importer token certifies the new tip's
/// complete body closure in THIS write set, so GC never reads it from old read.
pub(crate) async fn stage_accepted_native_upload_wave(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    identity: &NativeUploadAttemptIdentity,
    proof: &crate::sync::VerifiedRetainedBodyWave,
    now_ms: u64,
) -> Result<(NativeUploadAttempt, Vec<StoragePrecondition>), LixError> {
    if !proof.matches_identity(
        &identity.repository_id,
        &identity.account_id,
        &identity.branch_id,
        &identity.attempt_id,
    ) {
        return Err(invalid(
            "body proof belongs to another authenticated attempt",
        ));
    }
    let loaded = load_native_upload_attempt(read, identity).await?;
    if proof.initial() != loaded.is_none() {
        return Err(invalid(
            "wave admission snapshot differs from attempt state",
        ));
    }
    let (mut state, previous) = if let Some((state, raw)) = loaded {
        if state.expires_at_ms <= now_ms {
            return Err(expired());
        }
        if state.binding_digest != proof.binding_digest()
            || state
                .anchors
                .iter()
                .map(|id| CommitId::parse_lix(id, "upload anchor"))
                .collect::<Result<BTreeSet<_>, _>>()?
                != *proof.anchors()
            || state.merge_commit_id.is_some()
        {
            return Err(invalid("upload attempt binding changed or is terminal"));
        }
        if state.accepted_tip()? != proof.previous() && state.accepted_tip()? != proof.tip() {
            return Err(invalid(
                "upload wave does not continue durable accepted frontier",
            ));
        }
        (state, Some(raw))
    } else {
        if proof.previous() != proof.base() {
            return Err(invalid("initial wave must continue merge base"));
        }
        (
            NativeUploadAttempt {
                version: 1,
                identity: identity.clone(),
                binding_digest: proof.binding_digest(),
                accepted_tip: proof.previous().to_string(),
                accepted_commits: 0,
                anchors: proof.anchors().iter().map(ToString::to_string).collect(),
                merge_commit_id: None,
                expires_at_ms: 0,
            },
            None,
        )
    };
    if state.accepted_tip != proof.tip().to_string() {
        state.accepted_commits = state
            .accepted_commits
            .checked_add(proof.commit_count())
            .ok_or_else(|| invalid("attempt commit count overflow"))?;
        if state.accepted_commits > 1024 {
            return Err(invalid("attempt exceeds bounded local suffix"));
        }
    }
    state.accepted_tip = proof.tip().to_string();
    state.expires_at_ms = state.expires_at_ms.max(
        now_ms
            .checked_add(TTL_MS)
            .ok_or_else(|| invalid("attempt clock overflow"))?,
    );
    let mut preconditions = guards(read, identity, previous).await?;
    preconditions.extend(proof.anchor_guards().iter().cloned());
    stage(writes, &state)?;
    Ok((state, preconditions))
}

pub(crate) async fn require_native_upload_attempt(
    read: &(impl StorageAdapterRead + ?Sized),
    identity: &NativeUploadAttemptIdentity,
    digest: [u8; 32],
    expected_tip: CommitId,
    now_ms: u64,
) -> Result<NativeUploadAttempt, LixError> {
    let (state, _) = load_native_upload_attempt(read, identity)
        .await?
        .ok_or_else(expired)?;
    if state.expires_at_ms <= now_ms {
        return Err(expired());
    }
    if state.binding_digest != digest
        || state.accepted_tip()? != expected_tip
        || state.is_terminal()
    {
        return Err(invalid(
            "merge does not match a fully accepted live upload attempt",
        ));
    }
    Ok(state)
}

pub(crate) async fn stage_renew_native_upload_attempt(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    identity: &NativeUploadAttemptIdentity,
    binding_digest: [u8; 32],
    now_ms: u64,
) -> Result<(NativeUploadAttempt, Vec<StoragePrecondition>), LixError> {
    let (mut state, raw) = load_native_upload_attempt(read, identity)
        .await?
        .ok_or_else(expired)?;
    if state.binding_digest != binding_digest {
        return Err(invalid("attempt renewal binding mismatch"));
    }
    if state.expires_at_ms <= now_ms {
        return Err(expired());
    }
    state.expires_at_ms = state.expires_at_ms.max(
        now_ms
            .checked_add(TTL_MS)
            .ok_or_else(|| invalid("attempt clock overflow"))?,
    );
    let preconditions = guards(read, identity, Some(raw)).await?;
    stage(writes, &state)?;
    Ok((state, preconditions))
}

/// Finalization must be called by the same authority transaction that staged M
/// and its immutable receipt. The typed prepared receipt certifies the staged
/// native root address; M is intentionally absent from the old read snapshot.
pub(crate) async fn stage_finalize_native_upload_attempt(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    identity: &NativeUploadAttemptIdentity,
    proof: &crate::sync::PreparedAuthorityMergeReceipt,
    now_ms: u64,
) -> Result<Vec<StoragePrecondition>, LixError> {
    let (mut state, raw) = load_native_upload_attempt(read, identity)
        .await?
        .ok_or_else(expired)?;
    let receipt = proof.receipt();
    let request = &receipt.request;
    let digest = *blake3::hash(
        &serde_json::to_vec(request).map_err(|_| invalid("merge receipt encoding failed"))?,
    )
    .as_bytes();
    if state.expires_at_ms <= now_ms {
        return Err(expired());
    }
    if proof.repository_id() != identity.repository_id
        || proof.account_id() != identity.account_id
        || request.branch_id != identity.branch_id
        || request.attempt_id != identity.attempt_id
        || state.binding_digest != digest
        || state.accepted_tip != request.captured_local_head_commit_id
        || state.merge_commit_id.is_some()
    {
        return Err(invalid(
            "terminal merge does not match the fully accepted attempt",
        ));
    }
    state.merge_commit_id = Some(receipt.merge_commit_id.clone());
    state.expires_at_ms = state.expires_at_ms.max(
        now_ms
            .checked_add(TTL_MS)
            .ok_or_else(|| invalid("attempt clock overflow"))?,
    );
    let preconditions = guards(read, identity, Some(raw)).await?;
    stage(writes, &state)?;
    Ok(preconditions)
}

pub(super) struct NativeUploadRetention {
    pub roots: BTreeSet<CommitId>,
    pub expired_keys: Vec<StorageKey>,
    pub more_expired: bool,
}
pub(super) async fn load_native_upload_retention(
    read: &(impl StorageAdapterRead + ?Sized),
    now_ms: u64,
) -> Result<NativeUploadRetention, LixError> {
    let mut cursor = read
        .begin_scan(
            NATIVE_UPLOAD_ATTEMPT_SPACE,
            crate::storage_adapter::StoragePrefix {
                bytes: Bytes::new(),
            }
            .to_range()?,
            Default::default(),
        )
        .await?;
    let mut retained = NativeUploadRetention {
        roots: BTreeSet::new(),
        expired_keys: vec![],
        more_expired: false,
    };
    while let Some(entries) = cursor.next_chunk().await? {
        for entry in entries {
            let StorageProjectedValue::FullValue(bytes) = entry.value else {
                return Err(invalid("GC attempt omitted payload"));
            };
            if bytes.len() > MAX_BYTES {
                return Err(invalid("GC attempt exceeds encoded bound"));
            }
            let state: NativeUploadAttempt =
                serde_json::from_slice(&bytes).map_err(|_| invalid("GC attempt malformed"))?;
            state.validate()?;
            if entry.key != key(&state.identity)? {
                return Err(invalid("GC attempt key mismatch"));
            }
            if state.expires_at_ms > now_ms {
                retained.roots.extend(state.roots()?);
            } else if retained.expired_keys.len() < 128 {
                retained.expired_keys.push(entry.key);
            } else {
                retained.more_expired = true;
            }
        }
    }
    Ok(retained)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_adapter::{SharedStorageAdapterRead, StorageAdapter, StorageWriteOptions};
    fn uuid(n: u128) -> String {
        uuid::Uuid::from_u128(n).to_string()
    }
    fn record() -> NativeUploadAttempt {
        NativeUploadAttempt {
            version: 1,
            identity: NativeUploadAttemptIdentity {
                repository_id: uuid(1),
                account_id: uuid(2),
                branch_id: uuid(3),
                attempt_id: uuid(4),
            },
            binding_digest: [9; 32],
            accepted_tip: uuid(5),
            accepted_commits: 1,
            anchors: vec![uuid(6)],
            merge_commit_id: None,
            expires_at_ms: 400_000,
        }
    }
    #[tokio::test]
    async fn expired_restart_fences_delayed_renewal_and_cannot_resurrect_old_attempt() {
        let adapter = StorageAdapter::new(crate::Memory::new());
        let mut state = record();
        let request = crate::sync::PartialMergeRequest {
            attempt_id: state.identity.attempt_id.clone(),
            branch_id: state.identity.branch_id.clone(),
            base_commit_id: uuid(10),
            expected_authority_head_commit_id: uuid(11),
            captured_local_head_commit_id: state.accepted_tip.clone(),
            checkpoint_commit_id: uuid(12),
            global_head_commit_id: uuid(13),
            global_checkpoint_commit_id: uuid(14),
        };
        state.binding_digest = *blake3::hash(&serde_json::to_vec(&request).unwrap()).as_bytes();
        let mut writes = adapter.new_write_set();
        stage(&mut writes, &state).unwrap();
        adapter
            .commit_write_set(writes, Default::default())
            .await
            .unwrap();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let mut delayed = adapter.new_write_set();
        let (_, preconditions) = stage_renew_native_upload_attempt(
            &read,
            &mut delayed,
            &state.identity,
            state.binding_digest,
            100_000,
        )
        .await
        .unwrap();
        drop(read);
        let intent = crate::sync::PartialAttemptRestartRequest {
            old: request.clone(),
            next_attempt_id: uuid(15),
        };
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let mut writes = adapter.new_write_set();
        let (outcome, restart_guards) = crate::sync::stage_restart_expired_attempt(
            &read,
            &mut writes,
            &state.identity.repository_id,
            &state.identity.account_id,
            &intent,
            state.expires_at_ms,
        )
        .await
        .unwrap();
        drop(read);
        adapter
            .commit_write_set(
                writes,
                StorageWriteOptions {
                    preconditions: restart_guards,
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(
            adapter
                .commit_write_set(
                    delayed,
                    StorageWriteOptions {
                        preconditions,
                        ..Default::default()
                    }
                )
                .await
                .is_err()
        );
        let read = adapter.begin_read(Default::default()).await.unwrap();
        assert!(
            load_native_upload_attempt(&read, &state.identity)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            crate::sync::require_unrestarted_identity(
                &read,
                &state.identity.repository_id,
                &state.identity.account_id,
                &state.identity.branch_id,
                &state.identity.attempt_id
            )
            .await
            .unwrap_err()
            .code,
            "LIX_PARTIAL_ATTEMPT_RESTARTED"
        );
        // Exact retry after pin deletion returns the immutable same receipt.
        let (again, _) = crate::sync::stage_restart_expired_attempt(
            &read,
            &mut adapter.new_write_set(),
            &state.identity.repository_id,
            &state.identity.account_id,
            &intent,
            state.expires_at_ms + 1,
        )
        .await
        .unwrap();
        assert_eq!(again, outcome);
        let changed = crate::sync::PartialAttemptRestartRequest {
            next_attempt_id: uuid(16),
            ..intent
        };
        assert!(
            crate::sync::stage_restart_expired_attempt(
                &read,
                &mut adapter.new_write_set(),
                &state.identity.repository_id,
                &state.identity.account_id,
                &changed,
                state.expires_at_ms + 1
            )
            .await
            .is_err()
        );
    }
    #[tokio::test]
    async fn delayed_renewal_is_revision_fenced_and_account_isolated() {
        let adapter = StorageAdapter::new(crate::Memory::new());
        let state = record();
        let mut writes = adapter.new_write_set();
        stage(&mut writes, &state).unwrap();
        adapter
            .commit_write_set(writes, Default::default())
            .await
            .unwrap();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let mut renewal = adapter.new_write_set();
        let (_next, preconditions) = stage_renew_native_upload_attempt(
            &read,
            &mut renewal,
            &state.identity,
            state.binding_digest,
            100_001,
        )
        .await
        .unwrap();
        let mut other = state.identity.clone();
        other.account_id = uuid(7);
        assert!(
            load_native_upload_attempt(&read, &other)
                .await
                .unwrap()
                .is_none()
        );
        let mut changed = state.clone();
        // Keep the attempted record byte-identical: only the shared repository
        // mutation revision changes, as when a concurrent GC slice commits.
        changed.identity.attempt_id = uuid(8);
        let mut writes = adapter.new_write_set();
        stage(&mut writes, &changed).unwrap();
        drop(read);
        adapter
            .commit_write_set(writes, Default::default())
            .await
            .unwrap();
        assert!(
            adapter
                .commit_write_set(
                    renewal,
                    StorageWriteOptions {
                        preconditions,
                        ..Default::default()
                    }
                )
                .await
                .is_err()
        );
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let mut writes = adapter.new_write_set();
        assert_eq!(
            stage_renew_native_upload_attempt(
                &read,
                &mut writes,
                &state.identity,
                state.binding_digest,
                changed.expires_at_ms
            )
            .await
            .unwrap_err()
            .code,
            "LIX_NATIVE_UPLOAD_ATTEMPT_EXPIRED"
        );
    }

    // Retention-owner test uses a genuine immutable native commit and removes
    // its only branch ref. Native importer proof tests separately reject an
    // incomplete accepted wave; this test does not pretend to exercise HTTP.
    #[tokio::test]
    async fn attempt_pin_preserves_unreferenced_native_tip_until_expiry() {
        let lix = crate::open_lix().await.unwrap();
        let branch = lix
            .create_branch(crate::CreateBranchOptions {
                id: None,
                name: "attempt-only".into(),
                from_commit_id: None,
            })
            .await
            .unwrap();
        let session = lix
            .open_another_session()
            .with_branch(branch.id.clone())
            .await
            .unwrap();
        session
            .execute(
                "INSERT INTO lix_key_value (key,value) VALUES ('attempt-owned','kept')",
                &[],
            )
            .await
            .unwrap();
        let descriptor = lix
            .partial_replica_descriptor(Some(&branch.id))
            .await
            .unwrap();
        let tip = CommitId::parse_lix(
            &descriptor.selected_branch.head.commit_id,
            "test upload tip",
        )
        .unwrap();
        let mut state = NativeUploadAttempt {
            version: 1,
            identity: NativeUploadAttemptIdentity {
                repository_id: lix.lix_id().into(),
                account_id: lix.active_account_id().into(),
                branch_id: branch.id.clone(),
                attempt_id: uuid::Uuid::now_v7().to_string(),
            },
            binding_digest: [3; 32],
            accepted_tip: tip.to_string(),
            accepted_commits: 1,
            anchors: vec![descriptor.global_branch.head.commit_id],
            merge_commit_id: None,
            expires_at_ms: crate::telemetry::unix_time_ms() + TTL_MS,
        };
        let adapter = lix.storage_adapter();
        let mut writes = adapter.new_write_set();
        stage(&mut writes, &state).unwrap();
        adapter
            .commit_write_set(writes, Default::default())
            .await
            .unwrap();
        session.close().await.unwrap();
        drop(session);
        lix.execute(
            "DELETE FROM lix_branch WHERE id=$1",
            &[crate::Value::Text(branch.id)],
        )
        .await
        .unwrap();
        for expired in [false, true] {
            if expired {
                state.expires_at_ms = 1;
                let mut writes = adapter.new_write_set();
                stage(&mut writes, &state).unwrap();
                adapter
                    .commit_write_set(writes, Default::default())
                    .await
                    .unwrap();
            }
            for _ in 0..3 {
                let read = SharedStorageAdapterRead::new(
                    adapter.begin_read(Default::default()).await.unwrap(),
                );
                let mut writes = adapter.new_write_set();
                let mut preconditions = Vec::new();
                super::super::stage_repository_gc_with_preconditions(
                    read,
                    &mut writes,
                    &mut preconditions,
                )
                .await
                .unwrap();
                adapter
                    .commit_write_set(
                        writes,
                        StorageWriteOptions {
                            preconditions,
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap();
            }
            let read = adapter.begin_read(Default::default()).await.unwrap();
            let native = crate::tracked_state::load_commit_state_authority_ids(&read, &[tip])
                .await
                .unwrap();
            assert_eq!(native[0].is_some(), !expired);
        }
    }
}

// Append inside gc/native_upload_attempt.rs (native owner).
pub(crate) async fn stage_revoke_expired_upload_attempt(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    repository: &str,
    account: &str,
    request: &crate::sync::PartialMergeRequest,
    now_ms: u64,
) -> Result<Vec<StoragePrecondition>, LixError> {
    request.validate()?;
    let identity = NativeUploadAttemptIdentity {
        repository_id: repository.into(),
        account_id: account.into(),
        branch_id: request.branch_id.clone(),
        attempt_id: request.attempt_id.clone(),
    };
    let loaded = load_native_upload_attempt(read, &identity).await?;
    let previous = match loaded {
        Some((state, raw)) => {
            let digest = *blake3::hash(
                &serde_json::to_vec(request)
                    .map_err(|_| invalid("restart request encoding failed"))?,
            )
            .as_bytes();
            if state.binding_digest != digest || state.merge_commit_id.is_some() {
                return Err(invalid(
                    "restart disagrees with attempt binding or missing terminal merge receipt",
                ));
            }
            if state.expires_at_ms > now_ms {
                return Err(invalid("live attempt must not be restarted"));
            }
            writes.delete(NATIVE_UPLOAD_ATTEMPT_SPACE, key(&identity)?);
            Some(raw)
        }
        None => None,
    };
    // Includes exact previous/keyabsence plus GC mutation revision. Late renew,
    // wave, merge or GC cannot cross this atomic immutable terminal transition.
    guards(read, &identity, previous).await
}
