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

use super::partial_replica::{
    PARTIAL_REPLICA_DESCRIPTOR_VERSION, PartialReplicaBranch, PartialReplicaCommitRoots,
    PartialReplicaDescriptor,
};

pub(crate) const PARTIAL_REPLICA_STATE_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0007_0019),
    "sync.partial_replica_state.v1",
    ValueSemantics::Mutable,
);
const STATE_KEY: &[u8] = b"current";
const STATE_VERSION: u32 = 3;
const MAX_STATE_BYTES: usize = 16 * 1024;

pub(crate) fn partial_replica_state_key() -> StorageKey {
    StorageKey(Bytes::from_static(STATE_KEY))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct PartialReplicaState {
    version: u32,
    archived_branch_ids: Vec<String>,
    remote_id: String,
    active_account_id: String,
    epoch_id: String,
    descriptor: PartialReplicaDescriptor,
    baseline_lease: crate::gc::NativeBaselineLease,
    selected_serving_generation: String,
    global_serving_generation: String,
}

impl PartialReplicaState {
    /// Isolate authority recipe evaluation from every mutable serving generation.
    pub(crate) fn for_read_fulfillment(&self) -> Self {
        let mut state = self.clone();
        state.selected_serving_generation = uuid::Uuid::now_v7().to_string();
        state.global_serving_generation =
            if state.descriptor.selected_branch.branch_id == state.descriptor.global_branch.branch_id {
                state.selected_serving_generation.clone()
            } else {
                uuid::Uuid::now_v7().to_string()
            };
        state
    }

    // Method in PartialReplicaState; only the private switch owner calls this.
    pub(super) fn with_selected_branch(
        &self,
        leased: super::LeasedPartialReplicaDescriptor,
        target: &str,
    ) -> Result<Self, LixError> {
        leased.validate(self.repository_id(), self.active_account_id(), Some(target))?;
        if leased.descriptor.cursor < self.descriptor.cursor {
            return Err(invalid("branch admission cursor regressed"));
        }
        let mut next = Self::from_leased(
            self.remote_id.clone(),
            self.active_account_id.clone(),
            self.epoch_id.clone(),
            leased,
        )?;
        let mut archived = self
            .archived_branch_ids
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        if self.descriptor.selected_branch.branch_id != crate::GLOBAL_BRANCH_ID {
            archived.insert(self.descriptor.selected_branch.branch_id.clone());
        }
        archived.remove(target);
        next.archived_branch_ids = archived.into_iter().collect();
        next.selected_serving_generation = uuid::Uuid::now_v7().to_string();
        next.global_serving_generation = if target == crate::GLOBAL_BRANCH_ID {
            next.selected_serving_generation.clone()
        } else {
            uuid::Uuid::now_v7().to_string()
        };
        next.validate()?;
        Ok(next)
    }

    // Method to insert in PartialReplicaState's owner implementation.
    pub(crate) fn read_scope_source(&self) -> crate::hot_state::PartialReadScopeSource {
        let expected_repository = self.repository_id().to_owned();
        let expected_remote = self.remote_id().to_owned();
        let expected_account = self.active_account_id().to_owned();
        let expected_epoch = self.epoch_id().to_owned();
        crate::hot_state::PartialReadScopeSource::new(
            PARTIAL_REPLICA_STATE_SPACE,
            partial_replica_state_key(),
            MAX_STATE_BYTES,
            move |bytes| {
                let version =
                    partial_receipt_version(bytes, "partial read admission version is malformed")?;
                let state = decode_current_partial_receipt(
                    bytes,
                    version,
                    "partial read admission is malformed",
                )?;
                if state.repository_id() != expected_repository
                    || state.remote_id() != expected_remote
                    || state.active_account_id() != expected_account
                    || state.epoch_id() != expected_epoch
                {
                    return Err(LixError::new(
                        "LIX_PARTIAL_REPLICA_ADMISSION_MISMATCH",
                        "partial read admission belongs to another storage owner",
                    ));
                }
                let mut policy = crate::hot_state::PartialReadScopePolicy::new(
                    &state.descriptor().selected_branch.branch_id,
                    &state.descriptor().global_branch.branch_id,
                );
                policy.set_preparation_epoch(state.epoch_id());
                Ok(policy)
            },
        )
    }

    pub(crate) fn archived_branch_ids(&self) -> &[String] {
        &self.archived_branch_ids
    }

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
            archived_branch_ids: Vec::new(),
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
        if self.archived_branch_ids.len() > 128
            || self
                .archived_branch_ids
                .windows(2)
                .any(|ids| ids[0] >= ids[1])
            || self.archived_branch_ids.iter().any(|id| {
                crate::storage_codec::id_string::uuid_bytes_from_canonical(id).is_none()
                    || id == &self.descriptor.selected_branch.branch_id
                    || id == &self.descriptor.global_branch.branch_id
            })
        {
            return Err(invalid(
                "partial branch archive is malformed or exceeds bound",
            ));
        }
        if self.version != STATE_VERSION {
            return Err(receipt_version_error("unsupported partial replica state version", self.version));
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

fn receipt_failure(
    error: LixError,
    version: Option<u32>,
    failure_reason: &'static str,
    failure_path: &'static str,
) -> LixError {
    let mut details = serde_json::json!({
        "expectedReceiptVersion": STATE_VERSION,
        "migrationPhase": "partial_receipt",
        "failureReason": failure_reason,
        "failurePath": failure_path,
    });
    if let Some(version) = version {
        details["receiptVersion"] = serde_json::json!(version);
    }
    error.with_details(details)
}

fn receipt_version_error(message: &str, version: u32) -> LixError {
    receipt_failure(
        invalid(message),
        Some(version),
        "unsupported_receipt_version",
        "$",
    )
}

fn partial_receipt_version(bytes: &[u8], message: &str) -> Result<u32, LixError> {
    let probe: PartialReplicaReceiptVersion = serde_json::from_slice(bytes)
        .map_err(|error| receipt_json_parse_error(error, None, message))?;
    Ok(probe.version)
}

fn decode_current_partial_receipt(
    bytes: &[u8],
    version: u32,
    message: &str,
) -> Result<PartialReplicaState, LixError> {
    if version != STATE_VERSION {
        return Err(receipt_version_error(
            "partial receipt requires owned migration",
            version,
        ));
    }
    let state: PartialReplicaState = serde_json::from_slice(bytes)
        .map_err(|error| receipt_json_parse_error(error, Some(version), message))?;
    validate_partial_receipt_state(&state, version)?;
    Ok(state)
}

fn validate_partial_receipt_state(
    state: &PartialReplicaState,
    version: u32,
) -> Result<(), LixError> {
    let roots = super::leased_descriptor::descriptor_roots(&state.descriptor)
        .map_err(|error| receipt_failure(error, Some(version), "receipt_validation_failed", "$"))?;
    state
        .baseline_lease
        .validate_for_roots(&state.active_account_id, &roots)
        .map_err(|error| {
            receipt_failure(
                error,
                Some(version),
                "baseline_lease_invalid",
                "$.baselineLease",
            )
        })?;
    state
        .validate()
        .map_err(|error| receipt_failure(error, Some(version), "receipt_validation_failed", "$"))
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
        return Err(receipt_failure(
            invalid("partial replica state read omitted its value"),
            None,
            "receipt_validation_failed",
            "$",
        ));
    };
    if bytes.len() > MAX_STATE_BYTES {
        return Err(receipt_failure(
            invalid("partial replica state exceeds its metadata bound"),
            None,
            "receipt_validation_failed",
            "$",
        ));
    }
    let version = partial_receipt_version(&bytes, "partial replica state version is malformed")?;
    let state =
        decode_current_partial_receipt(&bytes, version, "partial replica state is malformed")?;
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
pub(crate) mod tests {
    use super::*;
    use crate::storage::StorageWrite;
    use crate::storage_adapter::{StorageAdapter, StorageReadOptions, StorageWriteOptions};
    use crate::{Memory, open_lix};

    async fn commit_raw_fixture(
        adapter: &StorageAdapter<Memory>,
        writes: StorageWriteSet,
        options: StorageWriteOptions,
    ) -> Result<(), ()> {
        let mut raw = adapter
            .begin_migration_write(options)
            .await
            .map_err(|_| ())?;
        writes.lower_into(&mut raw).await.map_err(|_| ())?;
        raw.commit().await.map(|_| ()).map_err(|_| ())
    }

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

    async fn state_with_global_selected() -> PartialReplicaState {
        let authority = open_lix().await.unwrap();
        let descriptor = authority
            .partial_replica_descriptor(Some(crate::GLOBAL_BRANCH_ID))
            .await
            .unwrap();
        let state = PartialReplicaState::new(
            format!("https://example.test/lix/{}", authority.lix_id()),
            crate::ANONYMOUS_ACCOUNT_ID.to_owned(),
            "00000000-0000-7000-8000-000000000099".to_owned(),
            descriptor,
        )
        .unwrap();
        authority.close().await.unwrap();
        state
    }

    async fn state_with_distinct_selected_and_global_branches() -> PartialReplicaState {
        let authority = open_lix().await.unwrap();
        let selected = authority
            .create_branch(crate::CreateBranchOptions {
                id: None,
                name: "receipt-migration-selected".into(),
                from_commit_id: None,
            })
            .await
            .unwrap();
        let descriptor = authority
            .partial_replica_descriptor(Some(&selected.id))
            .await
            .unwrap();
        let state = PartialReplicaState::new(
            format!("https://example.test/lix/{}", authority.lix_id()),
            crate::ANONYMOUS_ACCOUNT_ID.to_owned(),
            "00000000-0000-7000-8000-000000000099".to_owned(),
            descriptor,
        )
        .unwrap();
        authority.close().await.unwrap();
        state
    }

    /// Wire shape written by the released v2 receipt writer at
    /// 62713991a. That writer embeds descriptor v1, whose branch coordinates
    /// predate authorId, and a v1 native-baseline lease.
    pub(crate) fn released_v2_receipt_bytes_for_test(
        state: &PartialReplicaState,
        lease_version: u32,
    ) -> Bytes {
        assert!(matches!(lease_version, 1 | 2));
        let encode_roots = |roots: &PartialReplicaCommitRoots| {
            serde_json::json!({
                "commitId": roots.commit_id,
                "scopedRangeRootId": roots.scoped_range_root_id,
                "scopedRangeRootDigest": roots.scoped_range_root_digest,
                "rowPkIndexRootId": roots.row_pk_index_root_id,
            })
        };
        let encode_branch = |branch: &PartialReplicaBranch| {
            serde_json::json!({
                "branchId": branch.branch_id,
                "createdAt": branch.created_at,
                "updatedAt": branch.updated_at,
                "refChangeId": branch.ref_change_id,
                "head": encode_roots(&branch.head),
                "checkpoint": encode_roots(&branch.checkpoint),
            })
        };
        let descriptor = serde_json::json!({
            "descriptorVersion": 1,
            "lixId": state.descriptor.lix_id,
            "defaultBranchId": state.descriptor.default_branch_id,
            "cursor": state.descriptor.cursor,
            "selectedBranch": encode_branch(&state.descriptor.selected_branch),
            "globalBranch": encode_branch(&state.descriptor.global_branch),
        });
        // Explicitly name the released lease-v1/v2 keys. This fixture must not
        // inherit future fields from the current NativeBaselineLease serializer.
        let current_lease = serde_json::to_value(&state.baseline_lease).unwrap();
        let baseline_lease = serde_json::json!({
            "version": lease_version,
            "leaseId": current_lease["leaseId"],
            "accountId": current_lease["accountId"],
            "roots": current_lease["roots"],
            "expiresAtMs": current_lease["expiresAtMs"],
        });
        let value = serde_json::json!({
            "version": 2,
            "archivedBranchIds": state.archived_branch_ids,
            "remoteId": state.remote_id,
            "activeAccountId": state.active_account_id,
            "epochId": state.epoch_id,
            "descriptor": descriptor,
            "baselineLease": baseline_lease,
            "selectedServingGeneration": state.selected_serving_generation,
            "globalServingGeneration": state.global_serving_generation,
        });
        let exact_keys = |value: &serde_json::Value, expected: &[&str]| {
            let actual = value
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect::<std::collections::BTreeSet<_>>();
            let expected = expected
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>();
            assert_eq!(actual, expected);
        };
        exact_keys(
            &value,
            &[
                "version",
                "archivedBranchIds",
                "remoteId",
                "activeAccountId",
                "epochId",
                "descriptor",
                "baselineLease",
                "selectedServingGeneration",
                "globalServingGeneration",
            ],
        );
        exact_keys(
            &value["descriptor"],
            &[
                "descriptorVersion",
                "lixId",
                "defaultBranchId",
                "cursor",
                "selectedBranch",
                "globalBranch",
            ],
        );
        for branch in ["selectedBranch", "globalBranch"] {
            exact_keys(
                &value["descriptor"][branch],
                &[
                    "branchId",
                    "createdAt",
                    "updatedAt",
                    "refChangeId",
                    "head",
                    "checkpoint",
                ],
            );
            for roots in ["head", "checkpoint"] {
                exact_keys(
                    &value["descriptor"][branch][roots],
                    &[
                        "commitId",
                        "scopedRangeRootId",
                        "scopedRangeRootDigest",
                        "rowPkIndexRootId",
                    ],
                );
            }
        }
        exact_keys(
            &value["baselineLease"],
            &["version", "leaseId", "accountId", "roots", "expiresAtMs"],
        );
        Bytes::from(serde_json::to_vec(&value).unwrap())
    }

    async fn install_legacy_receipt_fixture(
        state: &PartialReplicaState,
        receipt: Bytes,
    ) -> StorageAdapter<Memory> {
        let adapter = StorageAdapter::new(Memory::new());
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let mut bootstrap = adapter.new_write_set();
        crate::init::stage_partial_repository_protocol(&mut bootstrap);
        let preconditions =
            crate::sync::partial_bootstrap::stage_partial_bootstrap(&read, &mut bootstrap, state)
                .unwrap();
        drop(read);
        commit_raw_fixture(
            &adapter,
            bootstrap,
            StorageWriteOptions {
                preconditions,
                await_durable: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let mut fixture = adapter.new_write_set();
        fixture.put(
            PARTIAL_REPLICA_STATE_SPACE,
            partial_replica_state_key(),
            receipt.to_vec(),
        );
        commit_raw_fixture(&adapter, fixture, Default::default())
            .await
            .unwrap();
        adapter
    }

    #[tokio::test]
    async fn read_scope_distinguishes_valid_owner_mismatch_from_corrupt_admission() {
        let original = state().await;
        for axis in ["epoch", "account", "remote", "repository", "malformed"] {
            let mut descriptor = original.descriptor().clone();
            if axis == "repository" {
                descriptor.lix_id = uuid::Uuid::now_v7().to_string();
            }
            let replacement = PartialReplicaState::new(
                if axis == "remote" {
                    "https://mirror.example.test/lix/other".into()
                } else {
                    original.remote_id().into()
                },
                if axis == "account" {
                    uuid::Uuid::now_v7().to_string()
                } else {
                    original.active_account_id().into()
                },
                if axis == "epoch" {
                    uuid::Uuid::now_v7().to_string()
                } else {
                    original.epoch_id().into()
                },
                descriptor,
            )
            .unwrap();
            let adapter = StorageAdapter::new(Memory::new());
            let mut writes = adapter.new_write_set();
            let bytes = if axis == "malformed" {
                b"{}".to_vec()
            } else {
                serde_json::to_vec(&replacement).unwrap()
            };
            writes.put(
                PARTIAL_REPLICA_STATE_SPACE,
                partial_replica_state_key(),
                bytes,
            );
            commit_raw_fixture(&adapter, writes, Default::default())
                .await
                .unwrap();
            let read = adapter.begin_read(Default::default()).await.unwrap();
            let error = original.read_scope_source().load(&read).await.unwrap_err();
            assert_eq!(
                error.code,
                if axis == "malformed" {
                    "LIX_PARTIAL_REPLICA_STATE_INVALID"
                } else {
                    "LIX_PARTIAL_REPLICA_ADMISSION_MISMATCH"
                },
                "{axis}"
            );
        }
    }

    #[tokio::test]
    async fn partial_opening_receipt_roundtrips_and_fences_concurrent_installation() {
        let state = state().await;
        let adapter = StorageAdapter::new(Memory::new());
        let mut writes = adapter.new_write_set();
        let precondition = stage_partial_replica_state(&mut writes, &state, None).unwrap();
        commit_raw_fixture(
            &adapter,
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
            commit_raw_fixture(
                &adapter,
                conflicting,
                StorageWriteOptions {
                    preconditions: vec![precondition],
                    ..Default::default()
                },
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn owned_v2_receipt_upgrade_preserves_archives_and_exact_source_guard() {
        let mut expected = state_with_global_selected().await;
        assert_eq!(
            expected.descriptor.selected_branch.branch_id,
            expected.descriptor.global_branch.branch_id
        );
        expected.descriptor.selected_branch.author_id =
            "00000000-0000-7000-8000-000000000111".to_owned();
        expected.descriptor.global_branch.author_id =
            "00000000-0000-7000-8000-000000000111".to_owned();
        expected
            .archived_branch_ids
            .push(uuid::Uuid::now_v7().to_string());
        expected.validate().unwrap();
        let bytes = released_v2_receipt_bytes_for_test(&expected, 2);
        let adapter = install_legacy_receipt_fixture(&expected, bytes.clone()).await;
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let mut writes = StorageWriteSet::new();
        let (upgraded, changed, guards) = prepare_owned_partial_receipt_upgrade(&read, &mut writes)
            .await
            .unwrap()
            .unwrap();
        assert!(changed);
        assert_eq!(upgraded, expected);
        assert!(guards.iter().any(|guard| matches!(
            guard,
            StoragePrecondition::KeyValueEquals { space, key, expected: source }
                if *space == PARTIAL_REPLICA_STATE_SPACE
                    && key == &partial_replica_state_key()
                    && source == &bytes
        )));
        let unique_branches = [
            upgraded.descriptor.selected_branch.branch_id.as_str(),
            upgraded.descriptor.global_branch.branch_id.as_str(),
        ]
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>()
        .len();
        assert_eq!(
            guards.len(),
            1 + unique_branches,
            "receipt CAS and one exact source-control guard per recovered author"
        );
        assert_eq!(
            unique_branches, 1,
            "selected/global share one source control"
        );
        assert_eq!(
            load_partial_replica_state(&read)
                .await
                .unwrap_err()
                .details
                .as_deref()
                .unwrap()["receiptVersion"],
            2
        );
        drop(read);
        adapter
            .commit_migration_write_set(
                writes,
                StorageWriteOptions {
                    preconditions: guards,
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        assert_eq!(
            load_partial_replica_state(&read).await.unwrap().unwrap().0,
            expected,
            "the guarded v2 promotion should publish the current receipt"
        );
    }

    #[tokio::test]
    async fn owned_v2_receipt_upgrade_rejects_a_stale_source_control() {
        let mut expected = state_with_distinct_selected_and_global_branches().await;
        assert_ne!(
            expected.descriptor.selected_branch.branch_id,
            expected.descriptor.global_branch.branch_id
        );
        expected.descriptor.selected_branch.author_id =
            "00000000-0000-7000-8000-000000000111".to_owned();
        expected.descriptor.global_branch.author_id =
            "00000000-0000-7000-8000-000000000111".to_owned();
        expected
            .archived_branch_ids
            .push(uuid::Uuid::now_v7().to_string());
        expected.validate().unwrap();
        let adapter = install_legacy_receipt_fixture(
            &expected,
            released_v2_receipt_bytes_for_test(&expected, 1),
        )
        .await;
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let mut writes = StorageWriteSet::new();
        let (_, changed, guards) = prepare_owned_partial_receipt_upgrade(&read, &mut writes)
            .await
            .unwrap()
            .unwrap();
        assert!(changed);
        assert_eq!(
            guards.len(),
            3,
            "receipt CAS plus distinct selected/global controls"
        );
        let global_control_key = StorageKey(Bytes::from(
            crate::branch::branch_head_control_key(&expected.descriptor.global_branch.branch_id)
                .unwrap(),
        ));
        assert!(guards.iter().any(|guard| matches!(
            guard,
            StoragePrecondition::KeyValueEquals { space, key, .. }
                if *space == crate::branch::BRANCH_HEAD_CONTROL_SPACE
                    && key == &global_control_key
        )));
        let source_control = crate::branch::observe_branch_control_coordinate(
            &read,
            &expected.descriptor.global_branch.branch_id,
        )
        .await
        .unwrap()
        .control
        .unwrap();
        drop(read);

        let mut changed_control = source_control;
        changed_control.current_state_revision += 1;
        let mut concurrent = adapter.new_write_set();
        crate::branch::stage_branch_head_control(
            &mut concurrent,
            &expected.descriptor.global_branch.branch_id,
            changed_control,
        )
        .unwrap();
        adapter
            .commit_migration_write_set(concurrent, Default::default())
            .await
            .unwrap();
        let error = adapter
            .commit_migration_write_set(
                writes,
                StorageWriteOptions {
                    preconditions: guards,
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            crate::storage_adapter::StorageWriteSetError::Storage(
                crate::storage_adapter::StorageError::PreconditionFailed(_)
            )
        ));
        let read = adapter.begin_read(Default::default()).await.unwrap();
        assert_eq!(
            load_partial_replica_state(&read)
                .await
                .unwrap_err()
                .details
                .as_deref()
                .unwrap()["receiptVersion"],
            2,
            "a source race must leave the old receipt unpublished"
        );
    }

    #[tokio::test]
    async fn v2_migration_rejects_unsupported_descriptor_and_future_receipt_versions() {
        let expected = state().await;
        let mut missing_field: serde_json::Value =
            serde_json::from_slice(&released_v2_receipt_bytes_for_test(&expected, 2)).unwrap();
        missing_field
            .as_object_mut()
            .unwrap()
            .remove("archivedBranchIds");
        let adapter = install_legacy_receipt_fixture(
            &expected,
            Bytes::from(serde_json::to_vec(&missing_field).unwrap()),
        )
        .await;
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let mut writes = StorageWriteSet::new();
        let error = prepare_owned_partial_receipt_upgrade(&read, &mut writes)
            .await
            .unwrap_err();
        let details = error.details.as_deref().unwrap();
        assert_eq!(details["failureReason"], "receipt_json_data");
        assert_eq!(details["failurePath"], "$");
        assert_eq!(details["missingField"], "archivedBranchIds");
        assert!((1..=MAX_STATE_BYTES as u64).contains(&details["jsonLine"].as_u64().unwrap()));
        assert!((1..=MAX_STATE_BYTES as u64).contains(&details["jsonColumn"].as_u64().unwrap()));
        assert!(
            writes.is_empty(),
            "rejected migration must stage no changes"
        );

        let mut unexpected_field: serde_json::Value =
            serde_json::from_slice(&released_v2_receipt_bytes_for_test(&expected, 2)).unwrap();
        unexpected_field["descriptor"]["selectedBranch"]["authorId"] =
            serde_json::json!("00000000-0000-7000-8000-000000000111");
        let adapter = install_legacy_receipt_fixture(
            &expected,
            Bytes::from(serde_json::to_vec(&unexpected_field).unwrap()),
        )
        .await;
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let mut writes = StorageWriteSet::new();
        let error = prepare_owned_partial_receipt_upgrade(&read, &mut writes)
            .await
            .unwrap_err();
        let details = error.details.as_deref().unwrap();
        assert_eq!(details["failureReason"], "receipt_json_data");
        assert_eq!(details["failurePath"], "$");
        assert!(details.get("missingField").is_none());
        assert!(
            writes.is_empty(),
            "unexpected current-only fields stay invalid"
        );

        let mut malformed: serde_json::Value =
            serde_json::from_slice(&released_v2_receipt_bytes_for_test(&expected, 2)).unwrap();
        malformed["descriptor"]["descriptorVersion"] = serde_json::json!(2);
        let adapter = install_legacy_receipt_fixture(
            &expected,
            Bytes::from(serde_json::to_vec(&malformed).unwrap()),
        )
        .await;
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let mut writes = StorageWriteSet::new();
        let error = prepare_owned_partial_receipt_upgrade(&read, &mut writes)
            .await
            .unwrap_err();
        let details = error.details.as_deref().unwrap();
        assert_eq!(details["failureReason"], "descriptor_version_invalid");
        assert_eq!(details["failurePath"], "$.descriptor.descriptorVersion");
        assert_eq!(details["receiptVersion"], 2);
        assert_eq!(details["expectedReceiptVersion"], STATE_VERSION);
        assert!(
            writes.is_empty(),
            "rejected migration must stage no changes"
        );

        let mut future: serde_json::Value =
            serde_json::from_slice(&released_v2_receipt_bytes_for_test(&expected, 2)).unwrap();
        future["version"] = serde_json::json!(99);
        let adapter = install_legacy_receipt_fixture(
            &expected,
            Bytes::from(serde_json::to_vec(&future).unwrap()),
        )
        .await;
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let mut writes = StorageWriteSet::new();
        let error = prepare_owned_partial_receipt_upgrade(&read, &mut writes)
            .await
            .unwrap_err();
        let details = error.details.as_deref().unwrap();
        assert_eq!(details["receiptVersion"], 99);
        assert_eq!(details["expectedReceiptVersion"], STATE_VERSION);
        assert_eq!(details["failureReason"], "unsupported_receipt_version");
        assert_eq!(details["failurePath"], "$");
        assert!(writes.is_empty(), "unknown versions must remain invalid");
    }

    #[tokio::test]
    async fn partial_receipt_diagnostics_cover_probe_v1_v3_and_baseline_lease_failures() {
        let expected = state().await;

        let adapter = install_legacy_receipt_fixture(&expected, Bytes::from_static(b"{}")).await;
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let error = load_partial_replica_state(&read).await.unwrap_err();
        let details = error.details.as_deref().unwrap();
        assert_eq!(details["failureReason"], "receipt_json_data");
        assert_eq!(details["failurePath"], "$");
        assert_eq!(details["missingField"], "version");
        assert!(details.get("receiptVersion").is_none());
        drop(read);

        let mut current: serde_json::Value = serde_json::to_value(&expected).unwrap();
        current["descriptor"]["selectedBranch"]
            .as_object_mut()
            .unwrap()
            .remove("authorId");
        let adapter = install_legacy_receipt_fixture(
            &expected,
            Bytes::from(serde_json::to_vec(&current).unwrap()),
        )
        .await;
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let error = load_partial_replica_state(&read).await.unwrap_err();
        let details = error.details.as_deref().unwrap();
        assert_eq!(details["failureReason"], "receipt_json_data");
        assert_eq!(details["receiptVersion"], STATE_VERSION);
        assert_eq!(details["missingField"], "authorId");
        drop(read);

        let mut v1: serde_json::Value =
            serde_json::from_slice(&released_v2_receipt_bytes_for_test(&expected, 1)).unwrap();
        v1["version"] = serde_json::json!(1);
        v1.as_object_mut().unwrap().remove("archivedBranchIds");
        v1["baselineLease"]
            .as_object_mut()
            .unwrap()
            .remove("expiresAtMs");
        let adapter = install_legacy_receipt_fixture(
            &expected,
            Bytes::from(serde_json::to_vec(&v1).unwrap()),
        )
        .await;
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let mut writes = StorageWriteSet::new();
        let error = prepare_owned_partial_receipt_upgrade(&read, &mut writes)
            .await
            .unwrap_err();
        let details = error.details.as_deref().unwrap();
        assert_eq!(details["failureReason"], "receipt_json_data");
        assert_eq!(details["receiptVersion"], 1);
        assert_eq!(details["missingField"], "expiresAtMs");
        assert!(writes.is_empty());
        drop(read);

        let mut invalid_lease: serde_json::Value =
            serde_json::from_slice(&released_v2_receipt_bytes_for_test(&expected, 1)).unwrap();
        invalid_lease["version"] = serde_json::json!(1);
        invalid_lease
            .as_object_mut()
            .unwrap()
            .remove("archivedBranchIds");
        invalid_lease["baselineLease"]["version"] = serde_json::json!(99);
        let adapter = install_legacy_receipt_fixture(
            &expected,
            Bytes::from(serde_json::to_vec(&invalid_lease).unwrap()),
        )
        .await;
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let mut writes = StorageWriteSet::new();
        let error = prepare_owned_partial_receipt_upgrade(&read, &mut writes)
            .await
            .unwrap_err();
        let details = error.details.as_deref().unwrap();
        assert_eq!(details["failureReason"], "baseline_lease_invalid");
        assert_eq!(details["failurePath"], "$.baselineLease");
        assert_eq!(details["receiptVersion"], 1);
        assert!(writes.is_empty());
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
        commit_raw_fixture(&adapter, writes, StorageWriteOptions::default())
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

// Private to the one-way owned-open migration. Ordinary readers and HOT policy
// decoders accept only the current receipt version.
// Frozen on-disk shapes from the released v1/v2 receipt writers. These must
// stay independent of the current descriptor: v2 serialized descriptor v1,
// whose branch coordinates predate the required authorId field.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PartialReplicaStateV1 {
    version: u32,
    remote_id: String,
    active_account_id: String,
    epoch_id: String,
    descriptor: PartialReplicaDescriptorV1,
    baseline_lease: PartialBaselineLeaseV1V2,
    selected_serving_generation: String,
    global_serving_generation: String,
}

#[derive(Deserialize)]
struct PartialReplicaReceiptVersion {
    version: u32,
}

// Baseline-lease wire shape embedded in released partial receipts. Versions 1
// and 2 use the same five coordinates; keep this decoder independent from the
// current GC lease type so future fields cannot silently alter receipt parsing.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PartialBaselineLeaseV1V2 {
    version: u32,
    lease_id: String,
    account_id: String,
    roots: Vec<String>,
    expires_at_ms: u64,
}

impl PartialBaselineLeaseV1V2 {
    fn into_native(self) -> Result<crate::gc::NativeBaselineLease, LixError> {
        crate::gc::NativeBaselineLease::from_partial_receipt_fields(
            self.version,
            self.lease_id,
            self.account_id,
            self.roots,
            self.expires_at_ms,
        )
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PartialReplicaStateV2 {
    version: u32,
    archived_branch_ids: Vec<String>,
    remote_id: String,
    active_account_id: String,
    epoch_id: String,
    descriptor: PartialReplicaDescriptorV1,
    baseline_lease: PartialBaselineLeaseV1V2,
    selected_serving_generation: String,
    global_serving_generation: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PartialReplicaDescriptorV1 {
    descriptor_version: u32,
    lix_id: String,
    default_branch_id: String,
    cursor: u64,
    selected_branch: PartialReplicaBranchV1,
    global_branch: PartialReplicaBranchV1,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PartialReplicaBranchV1 {
    branch_id: String,
    created_at: String,
    updated_at: String,
    ref_change_id: String,
    head: PartialReplicaCommitRootsV1,
    checkpoint: PartialReplicaCommitRootsV1,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PartialReplicaCommitRootsV1 {
    commit_id: String,
    scoped_range_root_id: Option<[u8; 32]>,
    scoped_range_root_digest: Option<[u8; 32]>,
    row_pk_index_root_id: Option<[u8; 32]>,
}

impl From<PartialReplicaCommitRootsV1> for PartialReplicaCommitRoots {
    fn from(roots: PartialReplicaCommitRootsV1) -> Self {
        Self {
            commit_id: roots.commit_id,
            scoped_range_root_id: roots.scoped_range_root_id,
            scoped_range_root_digest: roots.scoped_range_root_digest,
            row_pk_index_root_id: roots.row_pk_index_root_id,
        }
    }
}

impl PartialReplicaBranchV1 {
    fn promote(self, author_id: String) -> PartialReplicaBranch {
        PartialReplicaBranch {
            branch_id: self.branch_id,
            created_at: self.created_at,
            updated_at: self.updated_at,
            ref_change_id: self.ref_change_id,
            author_id,
            head: self.head.into(),
            checkpoint: self.checkpoint.into(),
        }
    }
}

fn legacy_receipt_error(
    version: u32,
    message: &str,
    failure: Option<(&'static str, &'static str)>,
) -> LixError {
    let (reason, path) = failure.unwrap_or(("receipt_validation_failed", "$"));
    receipt_failure(invalid(message), Some(version), reason, path)
}

fn receipt_json_parse_error(
    error: serde_json::Error,
    version: Option<u32>,
    message: &str,
) -> LixError {
    let failure_reason = match error.classify() {
        serde_json::error::Category::Syntax => "receipt_json_syntax",
        serde_json::error::Category::Data => "receipt_json_data",
        serde_json::error::Category::Eof => "receipt_json_eof",
        serde_json::error::Category::Io => "receipt_json_other",
    };
    let mut details = serde_json::json!({});
    if let Some(version) = version {
        details["receiptVersion"] = serde_json::json!(version);
    }
    details["expectedReceiptVersion"] = serde_json::json!(STATE_VERSION);
    details["migrationPhase"] = serde_json::json!("partial_receipt");
    details["failureReason"] = serde_json::json!(failure_reason);
    details["failurePath"] = serde_json::json!("$");
    let line = error.line().min(MAX_STATE_BYTES);
    let column = error.column().min(MAX_STATE_BYTES);
    if line > 0 {
        details["jsonLine"] = serde_json::json!(line);
    }
    if column > 0 {
        details["jsonColumn"] = serde_json::json!(column);
    }
    if let Some(field) = known_missing_receipt_field(&error, version) {
        details["missingField"] = serde_json::json!(field);
    }
    invalid(message).with_details(details)
}

fn known_missing_receipt_field(
    error: &serde_json::Error,
    version: Option<u32>,
) -> Option<&'static str> {
    if error.classify() != serde_json::error::Category::Data {
        return None;
    }
    let message = error.to_string();
    let missing = message.strip_prefix("missing field `")?.split_once('`')?.0;
    // These lists are frozen receipt wire fields. The raw parser string and
    // any unrecognized field name are never included in diagnostics.
    const V1_FIELDS: &[&str] = &[
        "version",
        "remoteId",
        "activeAccountId",
        "epochId",
        "descriptor",
        "baselineLease",
        "selectedServingGeneration",
        "globalServingGeneration",
        "descriptorVersion",
        "lixId",
        "defaultBranchId",
        "cursor",
        "selectedBranch",
        "globalBranch",
        "branchId",
        "createdAt",
        "updatedAt",
        "refChangeId",
        "head",
        "checkpoint",
        "commitId",
        "scopedRangeRootId",
        "scopedRangeRootDigest",
        "rowPkIndexRootId",
        "leaseId",
        "accountId",
        "roots",
        "expiresAtMs",
    ];
    const V2_FIELDS: &[&str] = &[
        "version",
        "archivedBranchIds",
        "remoteId",
        "activeAccountId",
        "epochId",
        "descriptor",
        "baselineLease",
        "selectedServingGeneration",
        "globalServingGeneration",
        "descriptorVersion",
        "lixId",
        "defaultBranchId",
        "cursor",
        "selectedBranch",
        "globalBranch",
        "branchId",
        "createdAt",
        "updatedAt",
        "refChangeId",
        "head",
        "checkpoint",
        "commitId",
        "scopedRangeRootId",
        "scopedRangeRootDigest",
        "rowPkIndexRootId",
        "leaseId",
        "accountId",
        "roots",
        "expiresAtMs",
    ];
    const V3_FIELDS: &[&str] = &[
        "version",
        "archivedBranchIds",
        "remoteId",
        "activeAccountId",
        "epochId",
        "descriptor",
        "baselineLease",
        "selectedServingGeneration",
        "globalServingGeneration",
        "descriptorVersion",
        "lixId",
        "defaultBranchId",
        "cursor",
        "selectedBranch",
        "globalBranch",
        "branchId",
        "createdAt",
        "updatedAt",
        "refChangeId",
        "authorId",
        "head",
        "checkpoint",
        "commitId",
        "scopedRangeRootId",
        "scopedRangeRootDigest",
        "rowPkIndexRootId",
        "leaseId",
        "accountId",
        "roots",
        "expiresAtMs",
    ];
    const VERSION_PROBE_FIELDS: &[&str] = &["version"];
    let fields = match version {
        Some(1) => V1_FIELDS,
        Some(2) => V2_FIELDS,
        Some(STATE_VERSION) => V3_FIELDS,
        Some(_) => return None,
        None => VERSION_PROBE_FIELDS,
    };
    fields.iter().copied().find(|field| *field == missing)
}

async fn promote_legacy_descriptor(
    read: &(impl StorageAdapterRead + ?Sized),
    descriptor: PartialReplicaDescriptorV1,
    receipt_version: u32,
) -> Result<(PartialReplicaDescriptor, Vec<StoragePrecondition>), LixError> {
    if descriptor.descriptor_version != 1 {
        return Err(legacy_receipt_error(
            receipt_version,
            "partial legacy descriptor version is unsupported",
            Some((
                "descriptor_version_invalid",
                "$.descriptor.descriptorVersion",
            )),
        ));
    }

    async fn author_from_source_control(
        read: &(impl StorageAdapterRead + ?Sized),
        branch: &PartialReplicaBranchV1,
        receipt_version: u32,
        branch_path: &'static str,
    ) -> Result<(String, StoragePrecondition), LixError> {
        let observation = crate::branch::observe_branch_control_coordinate(read, &branch.branch_id)
            .await
            .map_err(|error| {
                receipt_failure(
                    error,
                    Some(receipt_version),
                    "receipt_validation_failed",
                    branch_path,
                )
            })?;
        let Some(control) = observation.control else {
            return Err(legacy_receipt_error(
                receipt_version,
                "legacy partial admission lost its local branch control",
                Some(("branch_control_missing", branch_path)),
            ));
        };
        if crate::common::LixTimestamp::parse(&branch.created_at).ok() != Some(control.created_at) {
            let path = if branch_path == "$.descriptor.selectedBranch" {
                "$.descriptor.selectedBranch.createdAt"
            } else {
                "$.descriptor.globalBranch.createdAt"
            };
            return Err(legacy_receipt_error(
                receipt_version,
                "legacy partial branch incarnation differs from its local control",
                Some(("branch_incarnation_mismatch", path)),
            ));
        }
        let guard = crate::branch::branch_head_control_precondition(
            &branch.branch_id,
            observation.raw_token,
        )
        .map_err(|error| {
            receipt_failure(
                error,
                Some(receipt_version),
                "receipt_validation_failed",
                branch_path,
            )
        })?;
        Ok((control.author_id_string(), guard))
    }

    let (selected_author, selected_guard) = author_from_source_control(
        read,
        &descriptor.selected_branch,
        receipt_version,
        "$.descriptor.selectedBranch",
    )
    .await?;
    let (global_author, global_guard) =
        if descriptor.global_branch.branch_id == descriptor.selected_branch.branch_id {
            (selected_author.clone(), None)
        } else {
            let (author, guard) = author_from_source_control(
                read,
                &descriptor.global_branch,
                receipt_version,
                "$.descriptor.globalBranch",
            )
            .await?;
            (author, Some(guard))
        };
    let mut guards = vec![selected_guard];
    if let Some(guard) = global_guard {
        guards.push(guard);
    }
    Ok((
        PartialReplicaDescriptor {
            descriptor_version: PARTIAL_REPLICA_DESCRIPTOR_VERSION,
            lix_id: descriptor.lix_id,
            default_branch_id: descriptor.default_branch_id,
            cursor: descriptor.cursor,
            selected_branch: descriptor.selected_branch.promote(selected_author),
            global_branch: descriptor.global_branch.promote(global_author),
        },
        guards,
    ))
}

/// Owned epoch opening upgrades only bounded admission/upload metadata.
/// Local controls and native data remain intact; pending attempts preserve
/// their exact previously prepared wire requests without authority access.
/// This uses the raw migration writer because a legacy receipt cannot pass the
/// current partial-serving validator until its format has been upgraded. The
/// detached candidate derives and validates its serving witness before it can
/// be activated.
pub(crate) async fn upgrade_owned_partial_receipt<S>(
    adapter: &crate::storage_adapter::StorageAdapter<S>,
) -> Result<Option<PartialReplicaState>, LixError>
where
    S: crate::storage_adapter::Storage + Clone + Send + Sync + 'static,
{
    let read = adapter
        .begin_read(crate::storage_adapter::StorageReadOptions {
            durability: crate::storage_adapter::StorageReadDurability::Durable,
            ..Default::default()
        })
        .await?;
    let Some((state, writes, preconditions)) =
        prepare_owned_partial_metadata_upgrade(&read).await?
    else {
        return Ok(None);
    };
    drop(read);
    if !writes.is_empty() {
        adapter
            .commit_migration_write_set(
                writes,
                crate::storage_adapter::StorageWriteOptions {
                    await_durable: true,
                    preconditions,
                    ..Default::default()
                },
            )
            .await?;
    }
    Ok(Some(state))
}

/// The exact bounded metadata plan is also used by epoch preservation checks.
pub(crate) async fn prepare_owned_partial_metadata_upgrade(
    read: &(impl StorageAdapterRead + ?Sized),
) -> Result<
    Option<(
        PartialReplicaState,
        StorageWriteSet,
        Vec<StoragePrecondition>,
    )>,
    LixError,
> {
    let mut writes = StorageWriteSet::new();
    let Some((state, _upgraded, mut preconditions)) =
        prepare_owned_partial_receipt_upgrade(read, &mut writes).await?
    else {
        return Ok(None);
    };
    preconditions.extend(
        super::partial_push_state::prepare_owned_partial_push_upgrade(read, &mut writes, &state)
            .await?,
    );
    preconditions.extend(
        super::partial_merge_state::prepare_owned_partial_merge_upload_upgrade(
            read,
            &mut writes,
            &state,
        )
        .await?,
    );
    Ok(Some((state, writes, preconditions)))
}

pub(crate) async fn prepare_owned_partial_receipt_upgrade(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
) -> Result<Option<(PartialReplicaState, bool, Vec<StoragePrecondition>)>, LixError> {
    let value = PointReadPlan::new(PARTIAL_REPLICA_STATE_SPACE, &[partial_replica_state_key()])
        .materialize(read, Default::default())
        .await?
        .value
        .pop()
        .flatten();
    let bytes = match value {
        None => return Ok(None),
        Some(StorageProjectedValue::FullValue(bytes)) => bytes,
        Some(_) => {
            return Err(receipt_failure(
                invalid("partial receipt migration omitted its value"),
                None,
                "receipt_validation_failed",
                "$",
            ));
        }
    };
    if bytes.len() > MAX_STATE_BYTES {
        return Err(receipt_failure(
            invalid("partial receipt migration exceeds metadata bound"),
            None,
            "receipt_validation_failed",
            "$",
        ));
    }
    let version = partial_receipt_version(&bytes, "partial receipt version is malformed")?;
    if version == STATE_VERSION {
        let state =
            decode_current_partial_receipt(&bytes, version, "partial receipt is malformed")?;
        return Ok(Some((
            state,
            false,
            vec![StoragePrecondition::KeyValueEquals {
                space: PARTIAL_REPLICA_STATE_SPACE,
                key: partial_replica_state_key(),
                expected: bytes,
            }],
        )));
    }
    // v2 introduced archived branches, but it still embeds descriptor v1.
    // Descriptor v2 later added branch author IDs, so the current state type
    // cannot decode an actual v2 receipt. Decode the frozen v2 shape, recover
    // only the absent author coordinates from source-owned branch controls,
    // and preserve every other receipt field byte-for-byte in the CAS guard.
    // The detached epoch candidate proves local controls/native roots before
    // activation; an admission-base-only proof would reject valid v2 local undo.
    if version == 2 {
        let old: PartialReplicaStateV2 = serde_json::from_slice(&bytes).map_err(|error| {
            receipt_json_parse_error(
                error,
                Some(2),
                "partial v2 receipt does not match its released wire shape",
            )
        })?;
        if old.version != 2 {
            return Err(legacy_receipt_error(
                2,
                "partial v2 receipt version changed",
                Some(("receipt_validation_failed", "$")),
            ));
        }
        let (descriptor, mut guards) = promote_legacy_descriptor(read, old.descriptor, 2).await?;
        let baseline_lease = old.baseline_lease.into_native().map_err(|error| {
            receipt_failure(error, Some(2), "baseline_lease_invalid", "$.baselineLease")
        })?;
        let state = PartialReplicaState {
            version: STATE_VERSION,
            archived_branch_ids: old.archived_branch_ids,
            remote_id: old.remote_id,
            active_account_id: old.active_account_id,
            epoch_id: old.epoch_id,
            descriptor,
            baseline_lease,
            selected_serving_generation: old.selected_serving_generation,
            global_serving_generation: old.global_serving_generation,
        };
        validate_partial_receipt_state(&state, 2)?;
        guards.push(
            stage_partial_replica_state(writes, &state, Some(bytes)).map_err(|error| {
                receipt_failure(error, Some(2), "receipt_validation_failed", "$")
            })?,
        );
        return Ok(Some((state, true, guards)));
    }
    if version != 1 {
        return Err(receipt_version_error(
            "unsupported partial receipt migration source",
            version,
        ));
    }
    let old: PartialReplicaStateV1 = serde_json::from_slice(&bytes).map_err(|error| {
        receipt_json_parse_error(error, Some(1), "partial v1 receipt is malformed")
    })?;
    if old.version != 1 {
        return Err(legacy_receipt_error(
            1,
            "partial v1 receipt version changed",
            None,
        ));
    }
    let (descriptor, mut guards) = promote_legacy_descriptor(read, old.descriptor, 1).await?;
    let baseline_lease = old.baseline_lease.into_native().map_err(|error| {
        receipt_failure(error, Some(1), "baseline_lease_invalid", "$.baselineLease")
    })?;
    let state = PartialReplicaState {
        version: STATE_VERSION,
        archived_branch_ids: Vec::new(),
        remote_id: old.remote_id,
        active_account_id: old.active_account_id,
        epoch_id: old.epoch_id,
        descriptor,
        baseline_lease,
        selected_serving_generation: old.selected_serving_generation,
        global_serving_generation: old.global_serving_generation,
    };
    validate_partial_receipt_state(&state, 1)?;
    let mut visited = std::collections::BTreeSet::new();
    for branch in [
        &state.descriptor.selected_branch,
        &state.descriptor.global_branch,
    ] {
        if !visited.insert(branch.branch_id.clone()) {
            continue;
        }
        let branch_path = if branch.branch_id == state.descriptor.selected_branch.branch_id {
            "$.descriptor.selectedBranch"
        } else {
            "$.descriptor.globalBranch"
        };
        let observation = crate::branch::observe_branch_control_coordinate(read, &branch.branch_id)
            .await
            .map_err(|error| {
                receipt_failure(error, Some(1), "receipt_validation_failed", branch_path)
            })?;
        let control = observation.control.ok_or_else(|| {
            legacy_receipt_error(
                1,
                "legacy partial admission lost its local control",
                Some(("branch_control_missing", branch_path)),
            )
        })?;
        let serving_generation = state.serving_generation(&branch.branch_id).map_err(|error| {
            receipt_failure(
                error,
                Some(1),
                "receipt_validation_failed",
                branch_path,
            )
        })?;
        if control.tracked_generation != serving_generation {
            return Err(legacy_receipt_error(
                1,
                "legacy partial serving generation disagrees with its owner",
                Some(("receipt_validation_failed", branch_path)),
            ));
        }
        let marker_key = StorageKey(Bytes::from(crate::hot_state::hot_generation_scope_prefix(
            &branch.branch_id,
            control.tracked_generation,
        )));
        let marker = PointReadPlan::new(
            crate::hot_state::ROOT_CURRENT_BASE_SPACE,
            std::slice::from_ref(&marker_key),
        )
        .materialize(read, Default::default())
        .await
        .map_err(|error| {
            receipt_failure(error.into(), Some(1), "receipt_validation_failed", branch_path)
        })?
        .value
        .pop()
        .flatten();
        let Some(StorageProjectedValue::FullValue(marker)) = marker else {
            return Err(legacy_receipt_error(
                1,
                "legacy partial admission lost its native root marker",
                Some(("receipt_validation_failed", branch_path)),
            ));
        };
        let base = crate::changelog::CommitId::parse_lix(
            &branch.head.commit_id,
            "legacy partial base",
        )
        .map_err(|error| {
            receipt_failure(error, Some(1), "receipt_validation_failed", branch_path)
        })?;
        if marker.as_ref() != base.as_uuid().as_bytes() {
            return Err(legacy_receipt_error(
                1,
                "legacy partial native root disagrees with its owner",
                Some(("receipt_validation_failed", branch_path)),
            ));
        }
        guards.push(StoragePrecondition::KeyValueEquals {
            space: crate::hot_state::ROOT_CURRENT_BASE_SPACE,
            key: marker_key,
            expected: marker,
        });
    }
    guards.push(
        stage_partial_replica_state(writes, &state, Some(bytes)).map_err(|error| {
            receipt_failure(error, Some(1), "receipt_validation_failed", "$")
        })?,
    );
    Ok(Some((state, true, guards)))
}
