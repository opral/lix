//! Private compare-and-swap controls for tracked global proposals.
//!
//! `lix_change_proposal` is the durable, tracked global entity and therefore
//! the source of truth for proposal data. This module stores no duplicate
//! proposal payload. Its two tiny controls only make the happy-path lifecycle
//! atomic: an immutable id reservation and the one-open-proposal-per-ordered-
//! branch-pair compare-and-swap index.

use bytes::Bytes;
use serde::Deserialize;
use serde_json::{Value as JsonValue, json};

use crate::changelog::CommitId;
use crate::storage_adapter::{
    PointReadPlan, StorageAdapterRead, StorageGetOptions, StorageKey, StoragePrecondition,
    StorageProjectedValue, StorageSpace, StorageSpaceId, StorageValue, StorageWriteSet,
};
use crate::{LixError, storage_codec};

/// Immutable id reservation. It protects proposal creation from a concurrent
/// reuse of the same caller-provided id, without storing proposal data outside
/// the tracked entity.
pub(crate) const CHANGE_PROPOSAL_ID_SPACE: StorageSpace =
    StorageSpace::new(StorageSpaceId(0x0004_001b), "proposal.id.v1");

pub(crate) const CHANGE_PROPOSAL_SCHEMA_KEY: &str = "lix_change_proposal";

/// Enforces the happy-path invariant of one open proposal per ordered branch
/// pair. Resolved proposals keep their tracked entity but release this index.
pub(crate) const OPEN_CHANGE_PROPOSAL_BY_BRANCH_PAIR_SPACE: StorageSpace = StorageSpace::new(
    StorageSpaceId(0x0004_001c),
    "proposal.open_by_branch_pair.v1",
);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChangeProposalStateRecord {
    Open,
    Accepted,
    Rejected,
}

/// Domain payload materialized from the tracked `lix_change_proposal` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ChangeProposalRecord {
    pub(crate) id: String,
    pub(crate) source_branch_id: String,
    pub(crate) target_branch_id: String,
    /// Merge base pinned when the proposal was opened. This is the left side
    /// of the review diff, not an implementation cache.
    pub(crate) base_commit_id: CommitId,
    pub(crate) source_head_commit_id: CommitId,
    pub(crate) target_head_commit_id: CommitId,
    pub(crate) state: ChangeProposalStateRecord,
    pub(crate) accepted_target_head_commit_id: Option<CommitId>,
}

#[derive(Debug, Clone)]
pub(crate) enum ChangeProposalMutation {
    Create {
        proposal_id: String,
        source_branch_id: String,
        target_branch_id: String,
    },
    Resolve {
        proposal_id: String,
        source_branch_id: String,
        target_branch_id: String,
    },
}

#[derive(musli::Encode)]
#[musli(packed)]
struct ChangeProposalKeyRef<'a> {
    id: &'a str,
}

#[derive(musli::Encode)]
#[musli(packed)]
struct BranchPairKeyRef<'a> {
    source_branch_id: &'a str,
    target_branch_id: &'a str,
}

#[derive(musli::Encode, musli::Decode)]
#[musli(packed)]
struct OpenProposalPairValue {
    proposal_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ChangeProposalSnapshot {
    id: String,
    source_branch_id: String,
    target_branch_id: String,
    base_commit_id: String,
    source_head_commit_id: String,
    target_head_commit_id: String,
    state: String,
    accepted_target_head_commit_id: Option<String>,
}

impl ChangeProposalRecord {
    /// Serializes exactly the public tracked entity payload.
    pub(crate) fn snapshot_json(&self) -> JsonValue {
        json!({
            "id": self.id,
            "source_branch_id": self.source_branch_id,
            "target_branch_id": self.target_branch_id,
            "base_commit_id": self.base_commit_id.to_string(),
            "source_head_commit_id": self.source_head_commit_id.to_string(),
            "target_head_commit_id": self.target_head_commit_id.to_string(),
            "state": change_proposal_state_label(self.state),
            "accepted_target_head_commit_id": self
                .accepted_target_head_commit_id
                .map(|commit_id| commit_id.to_string()),
        })
    }

    /// Decodes and validates the public tracked entity payload. A corrupt row
    /// is an internal repository-integrity failure, never a reason to fall
    /// back to a private duplicate record.
    pub(crate) fn from_snapshot_content(snapshot_content: Option<&str>) -> Result<Self, LixError> {
        let snapshot_content = snapshot_content.ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked change proposal row omitted its snapshot content",
            )
        })?;
        let snapshot: ChangeProposalSnapshot =
            serde_json::from_str(snapshot_content).map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("tracked change proposal row has invalid JSON: {error}"),
                )
            })?;
        let state = match snapshot.state.as_str() {
            "open" => ChangeProposalStateRecord::Open,
            "accepted" => ChangeProposalStateRecord::Accepted,
            "rejected" => ChangeProposalStateRecord::Rejected,
            state => {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("tracked change proposal row has unknown state '{state}'"),
                ));
            }
        };
        let record = Self {
            id: snapshot.id,
            source_branch_id: snapshot.source_branch_id,
            target_branch_id: snapshot.target_branch_id,
            base_commit_id: CommitId::parse_lix(
                &snapshot.base_commit_id,
                "tracked change proposal base_commit_id",
            )?,
            source_head_commit_id: CommitId::parse_lix(
                &snapshot.source_head_commit_id,
                "tracked change proposal source_head_commit_id",
            )?,
            target_head_commit_id: CommitId::parse_lix(
                &snapshot.target_head_commit_id,
                "tracked change proposal target_head_commit_id",
            )?,
            state,
            accepted_target_head_commit_id: snapshot
                .accepted_target_head_commit_id
                .as_deref()
                .map(|value| {
                    CommitId::parse_lix(
                        value,
                        "tracked change proposal accepted_target_head_commit_id",
                    )
                })
                .transpose()?,
        };
        validate_record(&record)?;
        Ok(record)
    }
}

/// Read-side access to the private pair-index control only.
pub(crate) struct ChangeProposalControlReader<S> {
    store: S,
}

impl<S> ChangeProposalControlReader<S>
where
    S: StorageAdapterRead,
{
    pub(crate) fn new(store: S) -> Self {
        Self { store }
    }

    pub(crate) async fn load_open_for_branch_pair(
        &self,
        source_branch_id: &str,
        target_branch_id: &str,
    ) -> Result<Option<String>, LixError> {
        let key = branch_pair_key(source_branch_id, target_branch_id)?;
        let values = PointReadPlan::new(OPEN_CHANGE_PROPOSAL_BY_BRANCH_PAIR_SPACE, &[key])
            .materialize(&self.store, StorageGetOptions::default())
            .await?
            .value;
        let Some(value) = values.into_iter().next().flatten() else {
            return Ok(None);
        };
        let StorageProjectedValue::FullValue(bytes) = value else {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "change-proposal pair point read unexpectedly omitted its value",
            ));
        };
        let value: OpenProposalPairValue =
            storage_codec::decode("change-proposal open-pair value", &bytes)?;
        Ok(Some(value.proposal_id))
    }
}

/// Stages private lifecycle controls into the same storage write set as the
/// tracked proposal row and, for acceptance, the target branch merge.
pub(crate) fn stage_change_proposal_mutation(
    writes: &mut StorageWriteSet,
    preconditions: &mut Vec<StoragePrecondition>,
    mutation: ChangeProposalMutation,
) -> Result<(), LixError> {
    match mutation {
        ChangeProposalMutation::Create {
            proposal_id,
            source_branch_id,
            target_branch_id,
        } => {
            validate_control_fields(&proposal_id, &source_branch_id, &target_branch_id)?;
            let id_key = proposal_id_key(&proposal_id)?;
            let pair_key = branch_pair_key(&source_branch_id, &target_branch_id)?;
            writes.put(
                CHANGE_PROPOSAL_ID_SPACE,
                id_key.clone(),
                StorageValue {
                    bytes: Bytes::from_static(b"v1"),
                },
            );
            writes.put(
                OPEN_CHANGE_PROPOSAL_BY_BRANCH_PAIR_SPACE,
                pair_key.clone(),
                StorageValue {
                    bytes: Bytes::from(storage_codec::encode(
                        "change-proposal open-pair value",
                        &OpenProposalPairValue { proposal_id },
                    )?),
                },
            );
            preconditions.push(StoragePrecondition::KeyAbsent {
                space: CHANGE_PROPOSAL_ID_SPACE.id,
                key: id_key,
            });
            preconditions.push(StoragePrecondition::KeyAbsent {
                space: OPEN_CHANGE_PROPOSAL_BY_BRANCH_PAIR_SPACE.id,
                key: pair_key,
            });
        }
        ChangeProposalMutation::Resolve {
            proposal_id,
            source_branch_id,
            target_branch_id,
        } => {
            validate_control_fields(&proposal_id, &source_branch_id, &target_branch_id)?;
            let pair_key = branch_pair_key(&source_branch_id, &target_branch_id)?;
            let expected_pair_value = Bytes::from(storage_codec::encode(
                "change-proposal open-pair value",
                &OpenProposalPairValue { proposal_id },
            )?);
            writes.delete(OPEN_CHANGE_PROPOSAL_BY_BRANCH_PAIR_SPACE, pair_key.clone());
            preconditions.push(StoragePrecondition::KeyValueEquals {
                space: OPEN_CHANGE_PROPOSAL_BY_BRANCH_PAIR_SPACE.id,
                key: pair_key,
                expected: expected_pair_value,
            });
        }
    }
    Ok(())
}

pub(crate) fn change_proposal_state_label(state: ChangeProposalStateRecord) -> &'static str {
    match state {
        ChangeProposalStateRecord::Open => "open",
        ChangeProposalStateRecord::Accepted => "accepted",
        ChangeProposalStateRecord::Rejected => "rejected",
    }
}

fn validate_control_fields(
    proposal_id: &str,
    source_branch_id: &str,
    target_branch_id: &str,
) -> Result<(), LixError> {
    if proposal_id.is_empty() || source_branch_id.is_empty() || target_branch_id.is_empty() {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "change proposal id, source_branch_id, and target_branch_id must be non-empty",
        ));
    }
    if source_branch_id == target_branch_id {
        return Err(LixError::new(
            LixError::CODE_INVALID_MERGE,
            "a change proposal must target a different branch",
        ));
    }
    Ok(())
}

fn validate_record(record: &ChangeProposalRecord) -> Result<(), LixError> {
    validate_control_fields(
        &record.id,
        &record.source_branch_id,
        &record.target_branch_id,
    )?;
    match record.state {
        ChangeProposalStateRecord::Open if record.accepted_target_head_commit_id.is_none() => {
            Ok(())
        }
        ChangeProposalStateRecord::Accepted if record.accepted_target_head_commit_id.is_some() => {
            Ok(())
        }
        ChangeProposalStateRecord::Rejected if record.accepted_target_head_commit_id.is_none() => {
            Ok(())
        }
        ChangeProposalStateRecord::Open => Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "an open tracked change proposal must not record an accepted target head",
        )),
        ChangeProposalStateRecord::Accepted => Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "an accepted tracked change proposal must record its target head",
        )),
        ChangeProposalStateRecord::Rejected => Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "a rejected tracked change proposal must not record an accepted target head",
        )),
    }
}

fn proposal_id_key(id: &str) -> Result<StorageKey, LixError> {
    Ok(StorageKey(Bytes::from(storage_codec::encode(
        "change-proposal id key",
        &ChangeProposalKeyRef { id },
    )?)))
}

fn branch_pair_key(source_branch_id: &str, target_branch_id: &str) -> Result<StorageKey, LixError> {
    Ok(StorageKey(Bytes::from(storage_codec::encode(
        "change-proposal branch-pair key",
        &BranchPairKeyRef {
            source_branch_id,
            target_branch_id,
        },
    )?)))
}

#[cfg(test)]
mod tests {
    use crate::storage_adapter::{Memory, StorageAdapter, StorageReadOptions, StorageWriteOptions};

    use super::*;

    #[tokio::test]
    async fn controls_release_the_open_pair_without_storing_proposal_payload() {
        let storage = StorageAdapter::new(Memory::new());
        let mut writes = storage.new_write_set();
        let mut preconditions = Vec::new();
        stage_change_proposal_mutation(
            &mut writes,
            &mut preconditions,
            ChangeProposalMutation::Create {
                proposal_id: "proposal-1".to_string(),
                source_branch_id: "source".to_string(),
                target_branch_id: "target".to_string(),
            },
        )
        .expect("creation should stage");
        storage
            .commit_write_set(
                writes,
                StorageWriteOptions {
                    preconditions,
                    ..StorageWriteOptions::default()
                },
            )
            .await
            .expect("creation should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let reader = ChangeProposalControlReader::new(read);
        assert_eq!(
            reader
                .load_open_for_branch_pair("source", "target")
                .await
                .expect("pair should load"),
            Some("proposal-1".to_string())
        );
        drop(reader);

        let mut writes = storage.new_write_set();
        let mut preconditions = Vec::new();
        stage_change_proposal_mutation(
            &mut writes,
            &mut preconditions,
            ChangeProposalMutation::Resolve {
                proposal_id: "proposal-1".to_string(),
                source_branch_id: "source".to_string(),
                target_branch_id: "target".to_string(),
            },
        )
        .expect("resolution should stage");
        storage
            .commit_write_set(
                writes,
                StorageWriteOptions {
                    preconditions,
                    ..StorageWriteOptions::default()
                },
            )
            .await
            .expect("resolution should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("post-resolution read should open");
        let reader = ChangeProposalControlReader::new(read);
        assert_eq!(
            reader
                .load_open_for_branch_pair("source", "target")
                .await
                .expect("pair should load"),
            None
        );
    }
}
