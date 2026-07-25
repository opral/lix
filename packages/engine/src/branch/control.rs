//! Durable v6 branch-head control records.
//!
//! A branch head is a tiny mutable control-plane record, not a user row.  It
//! therefore has its own space and exact-byte CAS token.  The current tracked
//! serving generation lives beside the head so readers can bind a v5 group
//! marker to the same atomic publication without consulting `lix_branch_ref`
//! through the mutable live-state index.

use bytes::Bytes;

use crate::LixError;
use crate::changelog::{ChangeId, CommitId};
use crate::common::LixTimestamp;
use crate::storage_adapter::{
    PointReadPlan, ScanPlan, StorageAdapterRead, StorageGetOptions, StorageKey,
    StoragePrecondition, StoragePrefix, StorageProjectedValue, StorageScanOptions, StorageSpace,
    StorageSpaceId, StorageValue, StorageWriteSet,
};
use crate::storage_codec;

pub(crate) const BRANCH_HEAD_CONTROL_NAMESPACE: &str = "branch.head_control.v6";
pub(crate) const BRANCH_HEAD_CONTROL_SPACE: StorageSpace =
    StorageSpace::new(StorageSpaceId(0x0004_0015), BRANCH_HEAD_CONTROL_NAMESPACE);

/// The one mutable publication record for a branch.
///
/// `generation` is the physical v5 tracked-head generation currently serving
/// `head_commit_id`. Serial normal commits retain it; a rewind, merge fence,
/// or bootstrap gets a fresh generation and takes the historical fallback
/// until a complete v5 projection is published.
#[derive(Debug, Clone, Copy, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct BranchHeadControl {
    pub(crate) head_commit_id: CommitId,
    pub(crate) generation: CommitId,
    /// Stable public `lixcol_created_at` for the synthesized branch-ref row.
    pub(crate) created_at: LixTimestamp,
    /// Public `lixcol_updated_at` for the last head publication.
    pub(crate) updated_at: LixTimestamp,
    /// Public `lixcol_change_id` for the last head publication.
    pub(crate) ref_change_id: ChangeId,
}

/// One coherent point-read observation used for both generation selection and
/// the final exact-byte CAS guard. Keeping the decoded control and original
/// bytes together prevents a materializer from issuing a second control read
/// merely to build its publication precondition.
#[derive(Debug, Clone)]
pub(crate) struct BranchHeadControlObservation {
    pub(crate) control: Option<BranchHeadControl>,
    pub(crate) raw_token: Option<Bytes>,
}

#[derive(musli::Encode, musli::Decode)]
#[musli(packed)]
struct BranchHeadControlKey {
    branch_id: String,
}

#[derive(musli::Encode)]
#[musli(packed)]
struct BranchHeadControlKeyRef<'a> {
    branch_id: &'a str,
}

/// Read-side access for direct branch-head control records.
pub(crate) struct BranchHeadControlReader<S> {
    store: S,
}

impl<S> BranchHeadControlReader<S>
where
    S: StorageAdapterRead,
{
    pub(crate) async fn load(
        &self,
        branch_id: &str,
    ) -> Result<Option<BranchHeadControl>, LixError> {
        let mut values = self.load_many(&[branch_id.to_string()]).await?;
        Ok(values.pop().flatten())
    }

    /// Preserves request cardinality and order, including duplicates.
    pub(crate) async fn load_many(
        &self,
        branch_ids: &[String],
    ) -> Result<Vec<Option<BranchHeadControl>>, LixError> {
        Ok(self
            .load_observed(branch_ids)
            .await?
            .into_iter()
            .map(|observation| observation.control)
            .collect())
    }

    /// One point batch that returns both the decoded control and its opaque
    /// exact persisted bytes. Publication callers must retain this result
    /// through their write-set construction rather than reading the control
    /// again for CAS.
    pub(crate) async fn load_observed(
        &self,
        branch_ids: &[String],
    ) -> Result<Vec<BranchHeadControlObservation>, LixError> {
        if branch_ids.is_empty() {
            return Ok(Vec::new());
        }
        let keys = branch_ids
            .iter()
            .map(|branch_id| Ok(StorageKey(Bytes::from(encode_key(branch_id)?))))
            .collect::<Result<Vec<_>, LixError>>()?;
        PointReadPlan::new(BRANCH_HEAD_CONTROL_SPACE, &keys)
            .materialize(&self.store, StorageGetOptions::default())
            .await?
            .value
            .into_iter()
            .map(|value| match value {
                None => Ok(BranchHeadControlObservation {
                    control: None,
                    raw_token: None,
                }),
                Some(StorageProjectedValue::FullValue(bytes)) => {
                    let control = storage_codec::decode("branch-head control", &bytes)?;
                    Ok(BranchHeadControlObservation {
                        control: Some(control),
                        raw_token: Some(bytes),
                    })
                }
                Some(StorageProjectedValue::KeyOnly) => Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "branch-head control point read unexpectedly omitted its value",
                )),
            })
            .collect()
    }

    /// Returns every durable branch control in deterministic branch-id order.
    pub(crate) async fn scan(&self) -> Result<Vec<(String, BranchHeadControl)>, LixError> {
        let plan = ScanPlan::prefix(
            BRANCH_HEAD_CONTROL_SPACE,
            StoragePrefix {
                bytes: Bytes::new(),
            },
        );
        let mut rows = Vec::new();
        let mut resume_after = None;
        loop {
            let page = plan
                .collect(
                    &self.store,
                    StorageScanOptions {
                        resume_after: resume_after.clone(),
                        ..StorageScanOptions::default()
                    },
                )
                .await?;
            resume_after = page.value.entries.last().map(|entry| entry.key.clone());
            for entry in page.value.entries {
                let key = storage_codec::decode::<BranchHeadControlKey>(
                    "branch-head control key",
                    entry.key.0.as_ref(),
                )?;
                let control = decode_projected_value(entry.value)?;
                rows.push((key.branch_id, control));
            }
            if !page.value.has_more || resume_after.is_none() {
                break;
            }
        }
        rows.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(rows)
    }
}

/// Storage factory for branch-head controls.
#[derive(Clone, Copy, Default)]
pub(crate) struct BranchHeadControlContext;

impl BranchHeadControlContext {
    pub(crate) fn new() -> Self {
        Self
    }

    #[expect(clippy::unused_self)]
    pub(crate) fn reader<S>(&self, store: S) -> BranchHeadControlReader<S>
    where
        S: StorageAdapterRead,
    {
        BranchHeadControlReader { store }
    }
}

pub(crate) fn stage_branch_head_control(
    writes: &mut StorageWriteSet,
    branch_id: &str,
    control: BranchHeadControl,
) -> Result<(), LixError> {
    writes.put(
        BRANCH_HEAD_CONTROL_SPACE,
        StorageKey(Bytes::from(encode_key(branch_id)?)),
        StorageValue {
            bytes: Bytes::from(storage_codec::encode("branch-head control", &control)?),
        },
    );
    Ok(())
}

pub(crate) fn stage_delete_branch_head_control(
    writes: &mut StorageWriteSet,
    branch_id: &str,
) -> Result<(), LixError> {
    writes.delete(
        BRANCH_HEAD_CONTROL_SPACE,
        StorageKey(Bytes::from(encode_key(branch_id)?)),
    );
    Ok(())
}

/// Converts an observed opaque value into the one backend-neutral publication
/// guard. A missing control is guarded as absent, so concurrent branch
/// creation cannot both succeed.
pub(crate) fn branch_head_control_precondition(
    branch_id: &str,
    expected: Option<Bytes>,
) -> Result<StoragePrecondition, LixError> {
    let key = StorageKey(Bytes::from(encode_key(branch_id)?));
    Ok(match expected {
        None => StoragePrecondition::KeyAbsent {
            space: BRANCH_HEAD_CONTROL_SPACE.id,
            key,
        },
        Some(expected) => StoragePrecondition::KeyValueEquals {
            space: BRANCH_HEAD_CONTROL_SPACE.id,
            key,
            expected,
        },
    })
}

fn encode_key(branch_id: &str) -> Result<Vec<u8>, LixError> {
    storage_codec::encode(
        "branch-head control key",
        &BranchHeadControlKeyRef { branch_id },
    )
}

fn decode_projected_value(value: StorageProjectedValue) -> Result<BranchHeadControl, LixError> {
    let StorageProjectedValue::FullValue(bytes) = value else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "branch-head control read unexpectedly omitted its value",
        ));
    };
    storage_codec::decode("branch-head control", &bytes)
}

#[cfg(test)]
mod tests {
    use crate::storage_adapter::{Memory, StorageAdapter, StorageReadOptions, StorageWriteOptions};

    use super::*;

    #[tokio::test]
    async fn point_reads_scans_and_exact_byte_cas_controls() {
        let storage = StorageAdapter::new(Memory::new());
        let first = BranchHeadControl {
            head_commit_id: CommitId::for_test_label("first-head"),
            generation: CommitId::for_test_label("first-generation"),
            created_at: LixTimestamp::expect_parse("first created_at", "2026-01-01T00:00:00Z"),
            updated_at: LixTimestamp::expect_parse("first updated_at", "2026-01-01T00:00:00Z"),
            ref_change_id: ChangeId::for_test_label("first-ref-change"),
        };
        let second = BranchHeadControl {
            head_commit_id: CommitId::for_test_label("second-head"),
            generation: CommitId::for_test_label("first-generation"),
            created_at: first.created_at,
            updated_at: LixTimestamp::expect_parse("second updated_at", "2026-01-02T00:00:00Z"),
            ref_change_id: ChangeId::for_test_label("second-ref-change"),
        };
        let branch_a = "branch-a".to_string();
        let branch_b = "branch-b".to_string();

        let mut writes = storage.new_write_set();
        stage_branch_head_control(&mut writes, &branch_b, first)
            .expect("branch b control should stage");
        stage_branch_head_control(&mut writes, &branch_a, first)
            .expect("branch a control should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("controls should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let reader = BranchHeadControlContext::new().reader(read);
        assert_eq!(
            reader
                .load_many(&[branch_b.clone(), branch_a.clone(), branch_a.clone()])
                .await
                .expect("point reads should load"),
            vec![Some(first), Some(first), Some(first)]
        );
        assert_eq!(
            reader.scan().await.expect("scan should load"),
            vec![(branch_a.clone(), first), (branch_b, first)]
        );
        let token = reader
            .load_observed(std::slice::from_ref(&branch_a))
            .await
            .expect("control observation should load")
            .pop()
            .and_then(|observation| observation.raw_token)
            .expect("stored control should have token");

        let mut winner = storage.new_write_set();
        stage_branch_head_control(&mut winner, &branch_a, second).expect("winner should stage");
        storage
            .commit_write_set(
                winner,
                StorageWriteOptions {
                    preconditions: vec![
                        branch_head_control_precondition(&branch_a, Some(token.clone()))
                            .expect("winner guard should encode"),
                    ],
                    ..StorageWriteOptions::default()
                },
            )
            .await
            .expect("winner should commit");

        let mut stale = storage.new_write_set();
        stage_branch_head_control(&mut stale, &branch_a, first).expect("stale write should stage");
        let error = storage
            .commit_write_set(
                stale,
                StorageWriteOptions {
                    preconditions: vec![
                        branch_head_control_precondition(&branch_a, Some(token))
                            .expect("stale guard should encode"),
                    ],
                    ..StorageWriteOptions::default()
                },
            )
            .await
            .expect_err("stale publish must fail");
        assert!(matches!(
            error,
            crate::storage_adapter::StorageWriteSetError::Storage(
                crate::storage_adapter::StorageError::PreconditionFailed(_)
            )
        ));
    }
}
