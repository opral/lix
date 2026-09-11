//! Durable opening coordinates for a partial replica with on-demand sync.
//!
//! This receipt identifies the local epoch and its authority context. It never
//! certifies a complete HOT generation or claims that referenced objects are
//! resident. Coverage, native object ownership and pending commits belong in
//! separately keyed records so reopening does not enumerate the working set.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::LixError;
use crate::storage_adapter::{
    PointReadPlan, StorageAdapterRead, StorageGetOptions, StorageKey, StoragePrecondition,
    StorageProjectedValue, StorageSpace, StorageSpaceId, StorageValue, StorageWriteSet,
    ValueSemantics,
};

use super::partial_replica::PartialReplicaDescriptor;

pub(crate) const PARTIAL_REPLICA_STATE_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0007_0019),
    "sync.partial_replica_state.v1",
    ValueSemantics::Mutable,
);
const STATE_KEY: &[u8] = b"current";
const STATE_VERSION: u32 = 1;
const MAX_STATE_BYTES: usize = 16 * 1024;

pub(crate) fn partial_replica_state_key() -> StorageKey {
    StorageKey(Bytes::from_static(STATE_KEY))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct PartialReplicaState {
    version: u32,
    remote_id: String,
    active_account_id: String,
    epoch_id: String,
    descriptor: PartialReplicaDescriptor,
    baseline_lease: crate::gc::NativeBaselineLease,
    selected_serving_generation: String,
    global_serving_generation: String,
}

impl PartialReplicaState {
    #[cfg(test)]
    pub(crate) fn new(
        remote_id: String,
        active_account_id: String,
        epoch_id: String,
        descriptor: PartialReplicaDescriptor,
    ) -> Result<Self, LixError> {
        let lease = crate::gc::NativeBaselineLease::for_test(
            &active_account_id,
            &super::leased_descriptor::descriptor_roots(&descriptor)?,
        );
        Self::from_leased(
            remote_id,
            active_account_id,
            epoch_id,
            super::LeasedPartialReplicaDescriptor { descriptor, lease },
        )
    }

    pub(crate) fn from_leased(
        remote_id: String,
        active_account_id: String,
        epoch_id: String,
        leased: super::LeasedPartialReplicaDescriptor,
    ) -> Result<Self, LixError> {
        leased.validate(
            &leased.descriptor.lix_id,
            &active_account_id,
            Some(&leased.descriptor.selected_branch.branch_id),
        )?;
        let descriptor = leased.descriptor;
        let state = Self {
            baseline_lease: leased.lease,
            version: STATE_VERSION,
            remote_id,
            active_account_id,
            epoch_id,
            selected_serving_generation: descriptor.selected_branch.head.commit_id.clone(),
            global_serving_generation: descriptor.global_branch.head.commit_id.clone(),
            descriptor,
        };
        state.validate()?;
        Ok(state)
    }

    fn validate(&self) -> Result<(), LixError> {
        if self.version != STATE_VERSION {
            return Err(invalid("unsupported partial replica state version"));
        }
        super::validate_sync_remote_id(&self.remote_id)?;
        for id in [
            &self.active_account_id,
            &self.epoch_id,
            &self.selected_serving_generation,
            &self.global_serving_generation,
        ] {
            if crate::storage_codec::id_string::uuid_bytes_from_canonical(id).is_none() {
                return Err(invalid(
                    "partial replica account and epoch must be canonical UUIDs",
                ));
            }
        }
        if self.descriptor.selected_branch.branch_id == self.descriptor.global_branch.branch_id
            && self.selected_serving_generation != self.global_serving_generation
        {
            return Err(invalid(
                "one branch cannot have conflicting serving generations",
            ));
        }
        self.baseline_lease.validate_for_roots(
            &self.active_account_id,
            &super::leased_descriptor::descriptor_roots(&self.descriptor)?,
        )?;
        self.descriptor.validate(
            &self.descriptor.lix_id,
            Some(&self.descriptor.selected_branch.branch_id),
        )
    }

    pub(crate) fn serving_generation(
        &self,
        branch_id: &str,
    ) -> Result<crate::changelog::CommitId, LixError> {
        let generation = if branch_id == self.descriptor.selected_branch.branch_id {
            &self.selected_serving_generation
        } else if branch_id == self.descriptor.global_branch.branch_id {
            &self.global_serving_generation
        } else {
            return Err(invalid("branch has no admitted serving generation"));
        };
        crate::changelog::CommitId::parse_lix(generation, "partial serving generation")
    }

    /// A new baseline never reuses a former mutable HOT generation, even if
    /// the authority returns to a previously observed commit.
    #[cfg(test)]
    pub(crate) fn with_descriptor_and_fresh_generations(
        &self,
        descriptor: PartialReplicaDescriptor,
    ) -> Result<Self, LixError> {
        let lease = crate::gc::NativeBaselineLease::for_test(
            self.active_account_id(),
            &super::leased_descriptor::descriptor_roots(&descriptor)?,
        );
        self.with_leased_descriptor_and_fresh_generations(super::LeasedPartialReplicaDescriptor {
            descriptor,
            lease,
        })
    }

    pub(crate) fn with_renewed_baseline_lease(
        &self,
        lease: crate::gc::NativeBaselineLease,
    ) -> Result<Self, LixError> {
        let mut expected = self.baseline_lease.clone();
        expected.expires_at_ms = lease.expires_at_ms;
        if lease != expected || lease.expires_at_ms < self.baseline_lease.expires_at_ms {
            return Err(invalid("renewed baseline changed its immutable admission"));
        }
        let mut next = self.clone();
        next.baseline_lease = lease;
        next.validate()?;
        Ok(next)
    }

    /// Replace only retention identity after fresh authenticated acquisition.
    /// The caller supplies a fresh HTTP deadline and publishes via exact CAS.
    pub(crate) fn with_reacquired_baseline_lease(
        &self,
        lease: crate::gc::NativeBaselineLease,
    ) -> Result<Self, LixError> {
        lease.validate_for_roots(
            self.active_account_id(),
            &super::leased_descriptor::descriptor_roots(self.descriptor())?,
        )?;
        let mut next = self.clone();
        next.baseline_lease = lease;
        next.validate()?;
        Ok(next)
    }

    pub(crate) fn baseline_lease(&self) -> &crate::gc::NativeBaselineLease {
        &self.baseline_lease
    }

    pub(crate) fn with_leased_descriptor_and_fresh_generations(
        &self,
        leased: super::LeasedPartialReplicaDescriptor,
    ) -> Result<Self, LixError> {
        leased.validate(
            self.repository_id(),
            self.active_account_id(),
            Some(&self.descriptor.selected_branch.branch_id),
        )?;
        let descriptor = leased.descriptor;
        descriptor.validate(
            self.repository_id(),
            Some(&self.descriptor.selected_branch.branch_id),
        )?;
        let mut next = self.clone();
        next.selected_serving_generation = uuid::Uuid::now_v7().to_string();
        next.global_serving_generation =
            if descriptor.selected_branch.branch_id == descriptor.global_branch.branch_id {
                next.selected_serving_generation.clone()
            } else {
                uuid::Uuid::now_v7().to_string()
            };
        next.descriptor = descriptor;
        next.baseline_lease = leased.lease;
        next.validate()?;
        Ok(next)
    }

    pub(crate) fn repository_id(&self) -> &str {
        &self.descriptor.lix_id
    }

    pub(crate) fn active_account_id(&self) -> &str {
        &self.active_account_id
    }

    pub(crate) fn epoch_id(&self) -> &str {
        &self.epoch_id
    }

    pub(crate) fn descriptor(&self) -> &PartialReplicaDescriptor {
        &self.descriptor
    }

    pub(crate) fn remote_id(&self) -> &str {
        &self.remote_id
    }
}

fn invalid(message: &str) -> LixError {
    LixError::new("LIX_PARTIAL_REPLICA_STATE_INVALID", message)
}

/// One point read. The returned bytes fence a caller's later atomic update.
pub(crate) async fn load_partial_replica_state(
    read: &(impl StorageAdapterRead + ?Sized),
) -> Result<Option<(PartialReplicaState, Bytes)>, LixError> {
    let values = PointReadPlan::new(
        PARTIAL_REPLICA_STATE_SPACE,
        &[StorageKey(Bytes::from_static(STATE_KEY))],
    )
    .materialize(read, StorageGetOptions::default())
    .await?;
    let Some(value) = values.value.into_iter().next().flatten() else {
        return Ok(None);
    };
    let StorageProjectedValue::FullValue(bytes) = value else {
        return Err(invalid("partial replica state read omitted its value"));
    };
    if bytes.len() > MAX_STATE_BYTES {
        return Err(invalid("partial replica state exceeds its metadata bound"));
    }
    let state: PartialReplicaState = serde_json::from_slice(&bytes)
        .map_err(|_| invalid("partial replica state is malformed"))?;
    state.validate()?;
    Ok(Some((state, bytes)))
}

/// Caller publishes this with the selected controls/root markers, and uses
/// the returned precondition in its storage commit. No standalone receipt
/// installation may expose half-installed opening coordinates.
pub(super) fn stage_partial_replica_state(
    writes: &mut StorageWriteSet,
    state: &PartialReplicaState,
    previous: Option<Bytes>,
) -> Result<StoragePrecondition, LixError> {
    state.validate()?;
    let bytes =
        serde_json::to_vec(state).map_err(|_| invalid("partial replica state encoding failed"))?;
    if bytes.len() > MAX_STATE_BYTES {
        return Err(invalid("partial replica state exceeds its metadata bound"));
    }
    let key = StorageKey(Bytes::from_static(STATE_KEY));
    let precondition = match previous {
        Some(expected) => StoragePrecondition::KeyValueEquals {
            space: PARTIAL_REPLICA_STATE_SPACE,
            key: key.clone(),
            expected,
        },
        None => StoragePrecondition::KeyAbsent {
            space: PARTIAL_REPLICA_STATE_SPACE,
            key: key.clone(),
        },
    };
    writes.put(
        PARTIAL_REPLICA_STATE_SPACE,
        key,
        StorageValue {
            bytes: bytes.into(),
        },
    );
    Ok(precondition)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_adapter::{StorageAdapter, StorageReadOptions, StorageWriteOptions};
    use crate::{Memory, open_lix};

    async fn state() -> PartialReplicaState {
        let authority = open_lix().await.unwrap();
        PartialReplicaState::new(
            format!("https://example.test/lix/{}", authority.lix_id()),
            crate::ANONYMOUS_ACCOUNT_ID.to_owned(),
            "00000000-0000-7000-8000-000000000099".to_owned(),
            authority.partial_replica_descriptor(None).await.unwrap(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn partial_opening_receipt_roundtrips_and_fences_concurrent_installation() {
        let state = state().await;
        let adapter = StorageAdapter::new(Memory::new());
        let mut writes = adapter.new_write_set();
        let precondition = stage_partial_replica_state(&mut writes, &state, None).unwrap();
        adapter
            .commit_write_set(
                writes,
                StorageWriteOptions {
                    preconditions: vec![precondition],
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        let (loaded, previous) = load_partial_replica_state(&read).await.unwrap().unwrap();
        assert_eq!(loaded, state);
        assert!(previous.len() <= MAX_STATE_BYTES);
        drop(read);

        let mut conflicting = adapter.new_write_set();
        let precondition = stage_partial_replica_state(&mut conflicting, &state, None).unwrap();
        assert!(
            adapter
                .commit_write_set(
                    conflicting,
                    StorageWriteOptions {
                        preconditions: vec![precondition],
                        ..Default::default()
                    }
                )
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn partial_opening_receipt_rejects_unknown_versions_before_staging() {
        let mut state = state().await;
        state.version += 1;
        let adapter = StorageAdapter::new(Memory::new());
        let mut writes = adapter.new_write_set();
        assert_eq!(
            stage_partial_replica_state(&mut writes, &state, None)
                .unwrap_err()
                .code,
            "LIX_PARTIAL_REPLICA_STATE_INVALID"
        );
        assert!(
            writes
                .staged_value(PARTIAL_REPLICA_STATE_SPACE, STATE_KEY)
                .is_none()
        );
        // The decoder must apply the same validation to already persisted data.
        writes.put(
            PARTIAL_REPLICA_STATE_SPACE,
            StorageKey(Bytes::from_static(STATE_KEY)),
            StorageValue {
                bytes: serde_json::to_vec(&state).unwrap().into(),
            },
        );
        adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .unwrap();
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        assert_eq!(
            load_partial_replica_state(&read).await.unwrap_err().code,
            "LIX_PARTIAL_REPLICA_STATE_INVALID"
        );
    }
}
