//! Authority-owned native baseline retention. Lease acquisition/renewal uses
//! point reads and writes; only GC enumerates these explicit ownership roots.
use crate::storage_adapter::{
    PointReadPlan, StorageAdapterRead, StorageKey, StoragePrecondition, StorageProjectedValue,
    StorageSpace, StorageSpaceId, StorageValue, StorageWriteSet,
};
use crate::{LixError, changelog::CommitId};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
pub(crate) const NATIVE_BASELINE_LEASE_SPACE: StorageSpace =
    StorageSpace::mutable(StorageSpaceId(0x0008_000a), "gc.native_baseline_lease.v1");
pub(crate) const NATIVE_BASELINE_LEASE_TTL_MS: u64 = 300_000;
const MAX_LEASE_BYTES: usize = 1024;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct NativeBaselineLease {
    version: u32,
    pub(crate) lease_id: String,
    pub(crate) account_id: String,
    roots: Vec<String>,
    pub(crate) expires_at_ms: u64,
}
fn invalid(message: &str) -> LixError {
    LixError::new("LIX_NATIVE_BASELINE_LEASE_INVALID", message)
}
fn expired() -> LixError {
    LixError::new(
        "LIX_PARTIAL_BASELINE_EXPIRED",
        "native baseline lease expired; preserve pending edits and reconcile against a new authority baseline",
    )
}
fn key(id: &str) -> Result<StorageKey, LixError> {
    crate::storage_codec::id_string::uuid_bytes_from_canonical(id)
        .map(|bytes| StorageKey(Bytes::copy_from_slice(&bytes)))
        .ok_or_else(|| invalid("lease ID must be a canonical UUID"))
}
impl NativeBaselineLease {
    pub(crate) fn validate(&self) -> Result<(), LixError> {
        key(&self.lease_id)?;
        key(&self.account_id)?;
        if self.version != 1
            || self.roots.is_empty()
            || self.roots.len() > 4
            || self.expires_at_ms == 0
        {
            return Err(invalid("invalid native baseline lease shape"));
        }
        let roots = self
            .roots
            .iter()
            .map(|root| CommitId::parse_lix(root, "baseline lease root"))
            .collect::<Result<BTreeSet<_>, _>>()?;
        if roots.len() != self.roots.len() {
            return Err(invalid("duplicate lease roots"));
        }
        Ok(())
    }
}
async fn load(
    read: &(impl StorageAdapterRead + ?Sized),
    lease_id: &str,
) -> Result<Option<(NativeBaselineLease, Bytes)>, LixError> {
    let values = PointReadPlan::new(NATIVE_BASELINE_LEASE_SPACE, &[key(lease_id)?])
        .materialize(read, Default::default())
        .await?
        .value;
    let Some(value) = values.into_iter().next().flatten() else {
        return Ok(None);
    };
    let StorageProjectedValue::FullValue(bytes) = value else {
        return Err(invalid("lease read omitted payload"));
    };
    if bytes.len() > MAX_LEASE_BYTES {
        return Err(invalid("lease payload exceeds bound"));
    }
    let lease: NativeBaselineLease =
        serde_json::from_slice(&bytes).map_err(|_| invalid("malformed lease payload"))?;
    lease.validate()?;
    if lease.lease_id != lease_id {
        return Err(invalid("lease key identity mismatch"));
    }
    Ok(Some((lease, bytes)))
}
fn stage(writes: &mut StorageWriteSet, lease: &NativeBaselineLease) -> Result<(), LixError> {
    lease.validate()?;
    let bytes = serde_json::to_vec(lease).map_err(|_| invalid("lease serialization failed"))?;
    if bytes.len() > MAX_LEASE_BYTES {
        return Err(invalid("lease exceeds encoded bound"));
    }
    writes.put(
        NATIVE_BASELINE_LEASE_SPACE,
        key(&lease.lease_id)?,
        StorageValue {
            bytes: bytes.into(),
        },
    );
    Ok(())
}

/// Descriptor caller supplies the exact four roots it read, plus selected/global
/// IDs. We derive them again from control observations and fence those controls;
/// a checkpoint/GC between descriptor read and pin commit cannot orphan a lease.
pub(crate) async fn stage_acquire_native_baseline_lease(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    lease_id: &str,
    account_id: &str,
    branch_ids: &[String],
    expected_roots: &BTreeSet<CommitId>,
    now_ms: u64,
) -> Result<(NativeBaselineLease, Vec<StoragePrecondition>), LixError> {
    if branch_ids.is_empty() || branch_ids.len() > 2 {
        return Err(invalid("baseline lease requires selected/global branches"));
    }
    key(account_id)?;
    let observations = crate::branch::BranchHeadControlContext::new()
        .reader(read)
        .load_observed(branch_ids)
        .await?;
    let mut roots = BTreeSet::new();
    let mut guards = Vec::new();
    for (branch, observation) in branch_ids.iter().zip(observations) {
        let control = observation
            .control
            .ok_or_else(|| invalid("baseline branch disappeared"))?;
        roots.insert(control.head_commit_id);
        roots.insert(
            control
                .working_diff_checkpoint_commit_id
                .ok_or_else(|| invalid("baseline branch has no checkpoint"))?,
        );
        guards.push(crate::branch::branch_head_control_precondition(
            branch,
            observation.raw_token,
        )?);
    }
    if &roots != expected_roots {
        return Err(LixError::new(
            LixError::CODE_TRANSACTION_CONFLICT,
            "descriptor roots changed before baseline lease acquisition",
        ));
    }
    let lease = NativeBaselineLease {
        version: 1,
        lease_id: lease_id.into(),
        account_id: account_id.into(),
        roots: roots.into_iter().map(|root| root.to_string()).collect(),
        expires_at_ms: now_ms
            .checked_add(NATIVE_BASELINE_LEASE_TTL_MS)
            .ok_or_else(|| invalid("lease clock overflow"))?,
    };
    guards.push(StoragePrecondition::KeyAbsent {
        space: NATIVE_BASELINE_LEASE_SPACE,
        key: key(lease_id)?,
    });
    stage(writes, &lease)?;
    Ok((lease, guards))
}
pub(crate) async fn require_native_baseline_lease(
    read: &(impl StorageAdapterRead + ?Sized),
    lease_id: &str,
    account_id: &str,
    now_ms: u64,
) -> Result<NativeBaselineLease, LixError> {
    let (lease, _) = load(read, lease_id).await?.ok_or_else(expired)?;
    if lease.account_id != account_id {
        return Err(invalid("baseline lease belongs to another account"));
    }
    if lease.expires_at_ms <= now_ms {
        return Err(expired());
    }
    Ok(lease)
}
pub(crate) async fn stage_renew_native_baseline_lease(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    lease_id: &str,
    account_id: &str,
    now_ms: u64,
) -> Result<(NativeBaselineLease, Vec<StoragePrecondition>), LixError> {
    let (mut lease, raw) = load(read, lease_id).await?.ok_or_else(expired)?;
    if lease.account_id != account_id {
        return Err(invalid("baseline lease belongs to another account"));
    }
    if lease.expires_at_ms <= now_ms {
        return Err(expired());
    }
    lease.expires_at_ms = lease.expires_at_ms.max(
        now_ms
            .checked_add(NATIVE_BASELINE_LEASE_TTL_MS)
            .ok_or_else(|| invalid("lease clock overflow"))?,
    );
    let revision = crate::storage_adapter::load_repository_mutation_revision(read).await?;
    stage(writes, &lease)?;
    Ok((
        lease,
        vec![
            StoragePrecondition::KeyValueEquals {
                space: NATIVE_BASELINE_LEASE_SPACE,
                key: key(lease_id)?,
                expected: raw,
            },
            crate::storage_adapter::repository_mutation_revision_precondition(revision),
        ],
    ))
}
pub(super) struct NativeBaselineRetention {
    pub(super) roots: BTreeSet<CommitId>,
    pub(super) expired_keys: Vec<StorageKey>,
    pub(super) more_expired: bool,
}
pub(super) async fn load_native_baseline_retention(
    read: &(impl StorageAdapterRead + ?Sized),
    now_ms: u64,
) -> Result<NativeBaselineRetention, LixError> {
    let mut cursor = read
        .begin_scan(
            NATIVE_BASELINE_LEASE_SPACE,
            crate::storage_adapter::StoragePrefix {
                bytes: Bytes::new(),
            }
            .to_range()?,
            Default::default(),
        )
        .await?;
    let mut roots = BTreeSet::new();
    let mut expired_keys = Vec::new();
    let mut more_expired = false;
    while let Some(entries) = cursor.next_chunk().await? {
        for entry in entries {
            let StorageProjectedValue::FullValue(bytes) = entry.value else {
                return Err(invalid("GC lease scan omitted payload"));
            };
            if bytes.len() > MAX_LEASE_BYTES {
                return Err(invalid("GC lease exceeds bound"));
            }
            let lease: NativeBaselineLease =
                serde_json::from_slice(&bytes).map_err(|_| invalid("GC lease is malformed"))?;
            lease.validate()?;
            if entry.key != key(&lease.lease_id)? {
                return Err(invalid("GC lease key identity mismatch"));
            }
            if lease.expires_at_ms > now_ms {
                for root in lease.roots {
                    roots.insert(CommitId::parse_lix(&root, "GC baseline lease")?);
                }
            } else if expired_keys.len() < 128 {
                expired_keys.push(entry.key);
            } else {
                more_expired = true;
            }
        }
    }
    Ok(NativeBaselineRetention {
        roots,
        expired_keys,
        more_expired,
    })
}

#[cfg(test)]
pub(super) async fn live_native_baseline_roots(
    read: &(impl StorageAdapterRead + ?Sized),
    now_ms: u64,
) -> Result<BTreeSet<CommitId>, LixError> {
    Ok(load_native_baseline_retention(read, now_ms).await?.roots)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn baseline_lease_retains_roots_and_cannot_renew_after_expiry() {
        let lix = crate::open_lix().await.unwrap();
        let descriptor = lix.partial_replica_descriptor(None).await.unwrap();
        let roots = [
            &descriptor.selected_branch.head,
            &descriptor.selected_branch.checkpoint,
            &descriptor.global_branch.head,
            &descriptor.global_branch.checkpoint,
        ]
        .into_iter()
        .map(|root| CommitId::parse_lix(&root.commit_id, "test lease root").unwrap())
        .collect::<BTreeSet<_>>();
        let branches = vec![
            descriptor.selected_branch.branch_id.clone(),
            descriptor.global_branch.branch_id.clone(),
        ];
        let adapter = lix.storage_adapter();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let mut writes = adapter.new_write_set();
        let now = crate::telemetry::unix_time_ms();
        let (lease, guards) = stage_acquire_native_baseline_lease(
            &read,
            &mut writes,
            &uuid::Uuid::now_v7().to_string(),
            lix.active_account_id(),
            &branches,
            &roots,
            now,
        )
        .await
        .unwrap();
        drop(read);
        adapter
            .commit_write_set(
                writes,
                crate::storage_adapter::StorageWriteOptions {
                    preconditions: guards,
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        assert_eq!(live_native_baseline_roots(&read, now).await.unwrap(), roots);
        assert!(
            require_native_baseline_lease(&read, &lease.lease_id, crate::SYSTEM_ACCOUNT_ID, now)
                .await
                .is_err()
        );
        assert!(
            require_native_baseline_lease(
                &read,
                &lease.lease_id,
                lix.active_account_id(),
                lease.expires_at_ms
            )
            .await
            .is_err()
        );
        let mut writes = adapter.new_write_set();
        assert_eq!(
            stage_renew_native_baseline_lease(
                &read,
                &mut writes,
                &lease.lease_id,
                lix.active_account_id(),
                lease.expires_at_ms
            )
            .await
            .unwrap_err()
            .code,
            "LIX_PARTIAL_BASELINE_EXPIRED"
        );
        let mut clock_rollback = adapter.new_write_set();
        let (not_shortened, _) = stage_renew_native_baseline_lease(
            &read,
            &mut clock_rollback,
            &lease.lease_id,
            lix.active_account_id(),
            now.saturating_sub(60_000),
        )
        .await
        .unwrap();
        assert_eq!(
            not_shortened.expires_at_ms, lease.expires_at_ms,
            "clock rollback cannot shorten an existing lease"
        );
        let (renewed, guard) = stage_renew_native_baseline_lease(
            &read,
            &mut writes,
            &lease.lease_id,
            lix.active_account_id(),
            now + 1,
        )
        .await
        .unwrap();
        assert_eq!(renewed.lease_id, lease.lease_id);
        assert_eq!(renewed.roots, lease.roots);
        assert!(renewed.expires_at_ms > lease.expires_at_ms);
        drop(read);
        adapter
            .commit_write_set(
                writes,
                crate::storage_adapter::StorageWriteOptions {
                    preconditions: guard,
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let controls = crate::branch::BranchHeadControlContext::new()
            .reader(&read)
            .scan()
            .await
            .unwrap();
        let retained = super::super::load_authenticated_repository_retention(&&read, &controls)
            .await
            .unwrap();
        assert!(roots.is_subset(&retained.chronology_roots));
        assert!(
            live_native_baseline_roots(&read, renewed.expires_at_ms)
                .await
                .unwrap()
                .is_empty()
        );
    }
    #[tokio::test]
    async fn baseline_lease_alone_retains_deleted_branch_native_authority_until_expiry() {
        let lix = crate::open_lix().await.unwrap();
        let branch = lix
            .create_branch(crate::CreateBranchOptions {
                id: None,
                name: "leased-disposable".into(),
                from_commit_id: None,
            })
            .await
            .unwrap();
        let writer = lix
            .open_another_session()
            .with_branch(branch.id.clone())
            .await
            .unwrap();
        writer
            .execute(
                "INSERT INTO lix_key_value (key,value) VALUES ('leased-native-only','kept')",
                &[],
            )
            .await
            .unwrap();
        let descriptor = lix
            .partial_replica_descriptor(Some(&branch.id))
            .await
            .unwrap();
        let leased_head =
            CommitId::parse_lix(&descriptor.selected_branch.head.commit_id, "leased head").unwrap();
        let roots = [
            &descriptor.selected_branch.head,
            &descriptor.selected_branch.checkpoint,
            &descriptor.global_branch.head,
            &descriptor.global_branch.checkpoint,
        ]
        .into_iter()
        .map(|root| CommitId::parse_lix(&root.commit_id, "lease root").unwrap())
        .collect::<BTreeSet<_>>();
        let adapter = lix.storage_adapter();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let mut writes = adapter.new_write_set();
        let (mut lease, guards) = stage_acquire_native_baseline_lease(
            &read,
            &mut writes,
            &uuid::Uuid::now_v7().to_string(),
            lix.active_account_id(),
            &[branch.id.clone(), crate::GLOBAL_BRANCH_ID.into()],
            &roots,
            crate::telemetry::unix_time_ms(),
        )
        .await
        .unwrap();
        drop(read);
        adapter
            .commit_write_set(
                writes,
                crate::storage_adapter::StorageWriteOptions {
                    preconditions: guards,
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        writer.close().await.unwrap();
        drop(writer);
        lix.execute(
            "DELETE FROM lix_branch WHERE id=$1",
            &[crate::Value::Text(branch.id)],
        )
        .await
        .unwrap();
        for expired in [false, true] {
            if expired {
                lease.expires_at_ms = 1;
                let mut writes = adapter.new_write_set();
                stage(&mut writes, &lease).unwrap();
                adapter
                    .commit_write_set(writes, Default::default())
                    .await
                    .unwrap();
            }
            for _ in 0..3 {
                let read = crate::storage_adapter::SharedStorageAdapterRead::new(
                    adapter.begin_read(Default::default()).await.unwrap(),
                );
                let controls = crate::branch::BranchHeadControlContext::new()
                    .reader(&read)
                    .scan()
                    .await
                    .unwrap();
                let closure =
                    super::super::load_authenticated_repository_retention(&read, &controls)
                        .await
                        .unwrap();
                assert_eq!(
                    closure.chronology_roots.contains(&leased_head),
                    !expired,
                    "lease must be the deleted ordinary head's sole root"
                );
                let mut writes = adapter.new_write_set();
                let mut guards = Vec::new();
                super::super::stage_repository_gc_with_preconditions(
                    read,
                    &mut writes,
                    &mut guards,
                )
                .await
                .unwrap();
                adapter
                    .commit_write_set(
                        writes,
                        crate::storage_adapter::StorageWriteOptions {
                            preconditions: guards,
                            await_durable: true,
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap();
            }
            let read = adapter.begin_read(Default::default()).await.unwrap();
            let authority =
                crate::tracked_state::load_commit_state_authority_ids(&read, &[leased_head])
                    .await
                    .unwrap();
            assert_eq!(
                authority[0].is_some(),
                !expired,
                "GC must retain only the unexpired leased native authority"
            );
        }
    }

    #[tokio::test]
    async fn baseline_lease_retains_deleted_branch_file_manifest_and_chunks_until_expiry() {
        let lix = crate::open_lix().await.unwrap();
        let branch = lix
            .create_branch(crate::CreateBranchOptions {
                id: None,
                name: "leased-file-disposable".into(),
                from_commit_id: None,
            })
            .await
            .unwrap();
        let writer = lix
            .open_another_session()
            .with_branch(branch.id.clone())
            .await
            .unwrap();
        let content = (0..2 * 1024 * 1024 + 37)
            .map(|index| ((index * 29 + 17) % 251) as u8)
            .collect::<Vec<_>>();
        writer
            .execute(
                "INSERT INTO lix_file (path,content) VALUES ('/leased-only.bin',$1)",
                &[crate::Value::Blob(content.clone().into())],
            )
            .await
            .unwrap();
        let blob = crate::binary_cas::BlobId::from_content(&content);
        let manifest = lix
            .get_sync_blob_manifest(&blob.to_hex())
            .await
            .unwrap()
            .unwrap();
        let chunks = manifest
            .chunks
            .iter()
            .map(|chunk| crate::binary_cas::ChunkHash::from_hex(&chunk.chunk_id).unwrap())
            .collect::<Vec<_>>();
        assert!(!chunks.is_empty());
        let descriptor = lix
            .partial_replica_descriptor(Some(&branch.id))
            .await
            .unwrap();
        let leased_head =
            CommitId::parse_lix(&descriptor.selected_branch.head.commit_id, "leased head").unwrap();
        let roots = [
            &descriptor.selected_branch.head,
            &descriptor.selected_branch.checkpoint,
            &descriptor.global_branch.head,
            &descriptor.global_branch.checkpoint,
        ]
        .into_iter()
        .map(|root| CommitId::parse_lix(&root.commit_id, "lease root").unwrap())
        .collect::<BTreeSet<_>>();
        let adapter = lix.storage_adapter();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let mut writes = adapter.new_write_set();
        let (mut lease, guards) = stage_acquire_native_baseline_lease(
            &read,
            &mut writes,
            &uuid::Uuid::now_v7().to_string(),
            lix.active_account_id(),
            &[branch.id.clone(), crate::GLOBAL_BRANCH_ID.into()],
            &roots,
            crate::telemetry::unix_time_ms(),
        )
        .await
        .unwrap();
        drop(read);
        adapter
            .commit_write_set(
                writes,
                crate::storage_adapter::StorageWriteOptions {
                    preconditions: guards,
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        writer.close().await.unwrap();
        drop(writer);
        lix.execute(
            "DELETE FROM lix_branch WHERE id=$1",
            &[crate::Value::Text(branch.id)],
        )
        .await
        .unwrap();
        for expired in [false, true] {
            if expired {
                lease.expires_at_ms = 1;
                let mut writes = adapter.new_write_set();
                stage(&mut writes, &lease).unwrap();
                adapter
                    .commit_write_set(writes, Default::default())
                    .await
                    .unwrap();
            }
            for _ in 0..3 {
                let read = crate::storage_adapter::SharedStorageAdapterRead::new(
                    adapter.begin_read(Default::default()).await.unwrap(),
                );
                let controls = crate::branch::BranchHeadControlContext::new()
                    .reader(&read)
                    .scan()
                    .await
                    .unwrap();
                let closure =
                    super::super::load_authenticated_repository_retention(&read, &controls)
                        .await
                        .unwrap();
                assert_eq!(
                    closure.chronology_roots.contains(&leased_head),
                    !expired,
                    "lease must be the deleted ordinary head's sole root"
                );
                let mut writes = adapter.new_write_set();
                let mut guards = Vec::new();
                super::super::stage_repository_gc_with_preconditions(
                    read,
                    &mut writes,
                    &mut guards,
                )
                .await
                .unwrap();
                adapter
                    .commit_write_set(
                        writes,
                        crate::storage_adapter::StorageWriteOptions {
                            preconditions: guards,
                            await_durable: true,
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap();
            }
            let read = adapter.begin_read(Default::default()).await.unwrap();
            let authority =
                crate::tracked_state::load_commit_state_authority_ids(&read, &[leased_head])
                    .await
                    .unwrap();
            assert_eq!(
                authority[0].is_some(),
                !expired,
                "GC must retain only the unexpired leased native authority"
            );
            assert_eq!(
                crate::binary_cas::load_metadata_many(&read, &[blob])
                    .await
                    .unwrap()
                    .into_vec()[0]
                    .is_some(),
                !expired,
                "lease must preserve the deleted file's manifest"
            );
            for chunk in &chunks {
                assert_eq!(
                    crate::binary_cas::load_verified_chunk(&read, *chunk)
                        .await
                        .unwrap()
                        .is_some(),
                    !expired,
                    "lease must preserve each deleted file chunk"
                );
            }
        }
    }

    #[tokio::test]
    async fn delayed_renewal_cannot_resurrect_a_baseline_after_gc() {
        let lix = crate::open_lix().await.unwrap();
        let branch = lix
            .create_branch(crate::CreateBranchOptions {
                id: None,
                name: "expired-renew-race".into(),
                from_commit_id: None,
            })
            .await
            .unwrap();
        let writer = lix
            .open_another_session()
            .with_branch(branch.id.clone())
            .await
            .unwrap();
        writer
            .execute(
                "INSERT INTO lix_key_value (key,value) VALUES ('expired-lease-only','row')",
                &[],
            )
            .await
            .unwrap();
        let descriptor = lix
            .partial_replica_descriptor(Some(&branch.id))
            .await
            .unwrap();
        let head =
            CommitId::parse_lix(&descriptor.selected_branch.head.commit_id, "race head").unwrap();
        let roots = [
            &descriptor.selected_branch.head,
            &descriptor.selected_branch.checkpoint,
            &descriptor.global_branch.head,
            &descriptor.global_branch.checkpoint,
        ]
        .into_iter()
        .map(|root| CommitId::parse_lix(&root.commit_id, "race root").unwrap())
        .collect::<BTreeSet<_>>();
        let adapter = lix.storage_adapter();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let mut writes = adapter.new_write_set();
        // The explicit clock models a renewal begun before expiration and delayed
        // until after GC. The durable lease bytes remain identical throughout.
        let (lease, guards) = stage_acquire_native_baseline_lease(
            &read,
            &mut writes,
            "ffffffff-ffff-7fff-bfff-ffffffffffff",
            lix.active_account_id(),
            &[branch.id.clone(), crate::GLOBAL_BRANCH_ID.into()],
            &roots,
            1,
        )
        .await
        .unwrap();
        drop(read);
        adapter
            .commit_write_set(
                writes,
                crate::storage_adapter::StorageWriteOptions {
                    preconditions: guards,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        writer.close().await.unwrap();
        drop(writer);
        lix.execute(
            "DELETE FROM lix_branch WHERE id=$1",
            &[crate::Value::Text(branch.id)],
        )
        .await
        .unwrap();
        // Three bounded GC slices delete these 384 earlier lease keys first.
        // The target token remains byte-for-byte present after native GC, so
        // its own key CAS cannot detect the destructive intervening sweep.
        let mut preceding = adapter.new_write_set();
        for _ in 0..384 {
            let mut earlier = lease.clone();
            earlier.lease_id = uuid::Uuid::now_v7().to_string();
            stage(&mut preceding, &earlier).unwrap();
        }
        adapter
            .commit_write_set(preceding, Default::default())
            .await
            .unwrap();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let mut renewal = adapter.new_write_set();
        let (_, renew_guards) = stage_renew_native_baseline_lease(
            &read,
            &mut renewal,
            &lease.lease_id,
            lix.active_account_id(),
            2,
        )
        .await
        .unwrap();
        let original = load(&read, &lease.lease_id).await.unwrap().unwrap().1;
        drop(read);
        for _ in 0..3 {
            let read = crate::storage_adapter::SharedStorageAdapterRead::new(
                adapter.begin_read(Default::default()).await.unwrap(),
            );
            let mut gc = adapter.new_write_set();
            let mut guards = Vec::new();
            super::super::stage_repository_gc_with_preconditions(read, &mut gc, &mut guards)
                .await
                .unwrap();
            adapter
                .commit_write_set(
                    gc,
                    crate::storage_adapter::StorageWriteOptions {
                        preconditions: guards,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
        }
        let read = adapter.begin_read(Default::default()).await.unwrap();
        assert_eq!(
            load(&read, &lease.lease_id).await.unwrap().unwrap().1,
            original,
            "lease CAS alone would still succeed"
        );
        let remaining = load_native_baseline_retention(&read, crate::telemetry::unix_time_ms())
            .await
            .unwrap();
        assert_eq!(
            remaining.expired_keys.len(),
            1,
            "three bounded slices must reclaim 384 earlier expired lease records"
        );
        assert!(!remaining.more_expired);
        assert!(
            crate::tracked_state::load_commit_state_authority_ids(&read, &[head])
                .await
                .unwrap()[0]
                .is_none()
        );
        drop(read);
        assert!(
            adapter
                .commit_write_set(
                    renewal,
                    crate::storage_adapter::StorageWriteOptions {
                        preconditions: renew_guards,
                        ..Default::default()
                    }
                )
                .await
                .is_err(),
            "mutation revision must reject renewal after GC"
        );
    }
}

impl NativeBaselineLease {
    pub(crate) fn validate_for_roots(
        &self,
        account_id: &str,
        expected: &BTreeSet<CommitId>,
    ) -> Result<(), LixError> {
        self.validate()?;
        let roots = self
            .roots
            .iter()
            .map(|root| CommitId::parse_lix(root, "baseline lease root"))
            .collect::<Result<BTreeSet<_>, _>>()?;
        if self.account_id != account_id || &roots != expected {
            return Err(invalid(
                "baseline lease disagrees with authenticated account or descriptor roots",
            ));
        }
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn for_test(account_id: &str, roots: &BTreeSet<CommitId>) -> Self {
        Self {
            version: 1,
            lease_id: uuid::Uuid::now_v7().to_string(),
            account_id: account_id.into(),
            roots: roots.iter().map(ToString::to_string).collect(),
            expires_at_ms: u64::MAX,
        }
    }
}
