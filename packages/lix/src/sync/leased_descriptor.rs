//! Required authenticated network envelope. The coordinate descriptor remains
//! a read-only primitive; this envelope proves a durable authority retention
//! lease was acquired before those coordinates were transmitted.
use super::PartialReplicaDescriptor;
use crate::{LixError, changelog::CommitId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LeasedPartialReplicaDescriptor {
    pub(crate) descriptor: PartialReplicaDescriptor,
    pub(crate) lease: crate::gc::NativeBaselineLease,
}
impl LeasedPartialReplicaDescriptor {
    pub(crate) fn validate(
        &self,
        repository_id: &str,
        account_id: &str,
        branch_id: Option<&str>,
    ) -> Result<(), LixError> {
        self.descriptor.validate(repository_id, branch_id)?;
        self.lease
            .validate_for_roots(account_id, &descriptor_roots(&self.descriptor)?)
    }
}
pub(super) fn descriptor_roots(
    descriptor: &PartialReplicaDescriptor,
) -> Result<BTreeSet<CommitId>, LixError> {
    [
        &descriptor.selected_branch.head,
        &descriptor.selected_branch.checkpoint,
        &descriptor.global_branch.head,
        &descriptor.global_branch.checkpoint,
    ]
    .into_iter()
    .map(|roots| CommitId::parse_lix(&roots.commit_id, "leased descriptor root"))
    .collect()
}
impl<S> crate::Lix<S>
where
    S: crate::storage_adapter::Storage + Clone + Send + Sync + 'static,
{
    /// One coherent point-read descriptor + conditional durable root pin.
    /// A publication racing the final commit produces a retryable conflict.
    pub(crate) async fn leased_partial_replica_descriptor(
        &self,
        branch_id: Option<&str>,
    ) -> Result<LeasedPartialReplicaDescriptor, LixError> {
        if self.sync_mode_state().role() != super::SyncRole::Authority {
            return Err(LixError::new(
                super::SYNC_PROTOCOL_MISMATCH_CODE,
                "only a lease-aware authority can issue baseline pins",
            ));
        }
        if let Some(branch) = branch_id {
            super::validate_sync_branch_id(branch)?;
        }
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(Default::default()).await?;
        let descriptor = self
            .partial_replica_descriptor_with_read(&read, branch_id)
            .await?;
        let mut writes = adapter.new_write_set();
        let branches = vec![
            descriptor.selected_branch.branch_id.clone(),
            descriptor.global_branch.branch_id.clone(),
        ];
        let roots = descriptor_roots(&descriptor)?;
        let (lease, preconditions) = crate::gc::stage_acquire_native_baseline_lease(
            &read,
            &mut writes,
            &uuid::Uuid::now_v7().to_string(),
            self.active_account_id(),
            &branches,
            &roots,
            crate::telemetry::unix_time_ms(),
        )
        .await?;
        drop(read);
        adapter
            .commit_write_set(
                writes,
                crate::storage_adapter::StorageWriteOptions {
                    preconditions,
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await?;
        Ok(LeasedPartialReplicaDescriptor { descriptor, lease })
    }
}

pub(crate) const MAX_LEASED_DESCRIPTOR_BYTES: usize = 6144;

impl<S: crate::storage_adapter::Storage + Clone + Send + Sync + 'static> crate::Lix<S> {
    pub(crate) async fn renew_sync_native_baseline_lease(
        &self,
        lease_id: &str,
    ) -> Result<crate::gc::NativeBaselineLease, LixError> {
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(Default::default()).await?;
        let mut writes = adapter.new_write_set();
        let (lease, preconditions) = crate::gc::stage_renew_native_baseline_lease(
            &read,
            &mut writes,
            lease_id,
            self.active_account_id(),
            crate::telemetry::unix_time_ms(),
        )
        .await?;
        drop(read);
        adapter
            .commit_write_set(
                writes,
                crate::storage_adapter::StorageWriteOptions {
                    preconditions,
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await?;
        Ok(lease)
    }
}

#[cfg(test)]
impl LeasedPartialReplicaDescriptor {
    pub(crate) fn for_test(descriptor: PartialReplicaDescriptor, account: &str) -> Self {
        let lease = crate::gc::NativeBaselineLease::for_test(
            account,
            &descriptor_roots(&descriptor).unwrap_or_default(),
        );
        Self { descriptor, lease }
    }
}
