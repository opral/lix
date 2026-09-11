//! Global migration intent persists in the unbanked epoch owner space.
//! Transition methods never change source digest or original native coordinates.
use crate::sync::{NativeGlobalMigrationReceipt, NativeGlobalMigrationRequest};
use crate::sync::{NativeGlobalRestartReceipt, NativeGlobalRestartRequest};
use crate::{GLOBAL_BRANCH_ID, LixError};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct GlobalBodyFrontier {
    pub accepted: String,
    pub prepared: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct GlobalConversionJournal {
    pub version: u32,
    pub source_bank: String,
    pub manifest_digest: [u8; 32],
    pub request: NativeGlobalMigrationRequest,
    pub frontiers: BTreeMap<String, GlobalBodyFrontier>,
    pub acknowledged_roots: BTreeSet<String>,
    pub restart_intent: Option<NativeGlobalRestartRequest>,
    pub restart_receipt: Option<NativeGlobalRestartReceipt>,
    pub previous_abort: Option<NativeGlobalRestartReceipt>,
    pub receipt: Option<NativeGlobalMigrationReceipt>,
    pub cleanup_complete: bool,
}
fn invalid() -> LixError {
    LixError::new(
        "LIX_MIGRATION_GLOBAL_JOURNAL_INVALID",
        "global migration transition disagrees with frozen source or exact prepared request",
    )
}
impl GlobalConversionJournal {
    pub(crate) fn validate(&self) -> Result<(), LixError> {
        self.request.validate()?;
        if self.version != 2
            || self.source_bank.is_empty()
            || self.frontiers.len() != self.request.new_branches.len() + 1
        {
            return Err(invalid());
        }
        for (branch, frontier) in &self.frontiers {
            if branch != GLOBAL_BRANCH_ID
                && !self
                    .request
                    .new_branches
                    .iter()
                    .any(|b| &b.branch_id == branch)
            {
                return Err(invalid());
            }
            for key in std::iter::once(&frontier.accepted).chain(frontier.prepared.iter()) {
                crate::changelog::CommitId::parse_lix(key, "global conversion frontier")?;
            }
        }
        if !self.frontiers.contains_key(GLOBAL_BRANCH_ID) {
            return Err(invalid());
        }
        if let Some(previous) = &self.previous_abort {
            let NativeGlobalRestartReceipt::Restarted { intent } = previous else {
                return Err(invalid());
            };
            intent.validate()?;
            if intent.next_attempt_id != self.request.attempt_id
                || intent.request.base_commit_id != self.request.base_commit_id
                || intent.request.captured_local_head_commit_id
                    != self.request.captured_local_head_commit_id
                || intent.request.checkpoint_commit_id != self.request.checkpoint_commit_id
                || intent.request.new_branches != self.request.new_branches
            {
                return Err(invalid());
            }
        }
        if self
            .acknowledged_roots
            .iter()
            .any(|branch| !self.frontiers.contains_key(branch))
        {
            return Err(invalid());
        }
        if let Some(intent) = &self.restart_intent {
            intent.validate()?;
            if intent.request != self.request || self.receipt.is_some() {
                return Err(invalid());
            }
            if let Some(outcome) = &self.restart_receipt {
                outcome.validate_for(intent)?;
            }
        } else if self.restart_receipt.is_some() {
            return Err(invalid());
        }
        if let Some(receipt) = &self.receipt {
            receipt.validate()?;
            if receipt.request != self.request
                || self.acknowledged_roots.len() != self.frontiers.len()
                || self.frontiers.values().any(|f| f.prepared.is_some())
                || self.frontiers[GLOBAL_BRANCH_ID].accepted
                    != self.request.captured_local_head_commit_id
            {
                return Err(invalid());
            }
            for branch in &self.request.new_branches {
                if self.frontiers[&branch.branch_id].accepted != branch.head_commit_id {
                    return Err(invalid());
                }
            }
        } else if self.cleanup_complete {
            return Err(invalid());
        }
        Ok(())
    }
    pub(crate) fn prepare_wave(
        &self,
        branch: &str,
        previous: &str,
        target: &str,
    ) -> Result<Self, LixError> {
        self.validate()?;
        if self.restart_intent.is_some() || self.receipt.is_some() {
            return Err(invalid());
        }
        crate::changelog::CommitId::parse_lix(target, "global prepared target")?;
        let mut next = self.clone();
        let frontier = next.frontiers.get_mut(branch).ok_or_else(invalid)?;
        if frontier.accepted != previous
            || frontier.prepared.as_ref().is_some_and(|old| old != target)
        {
            return Err(invalid());
        }
        frontier.prepared = Some(target.to_owned());
        next.validate()?;
        Ok(next)
    }
    pub(crate) fn acknowledge_wave(
        &self,
        branch: &str,
        previous: &str,
        target: &str,
    ) -> Result<Self, LixError> {
        self.validate()?;
        let mut next = self.clone();
        let frontier = next.frontiers.get_mut(branch).ok_or_else(invalid)?;
        if self.restart_intent.is_some()
            || self.receipt.is_some()
            || frontier.accepted != previous
            || frontier.prepared.as_deref() != Some(target)
        {
            return Err(invalid());
        }
        frontier.accepted = target.to_owned();
        frontier.prepared = None;
        next.acknowledged_roots.insert(branch.to_owned());
        next.validate()?;
        Ok(next)
    }
    pub(crate) fn acknowledge_merge(
        &self,
        receipt: NativeGlobalMigrationReceipt,
    ) -> Result<Self, LixError> {
        self.validate()?;
        if self.receipt.as_ref().is_some_and(|old| old != &receipt) {
            return Err(invalid());
        }
        let mut next = self.clone();
        next.restart_intent = None;
        next.restart_receipt = None;
        next.receipt = Some(receipt);
        next.validate()?;
        Ok(next)
    }
    pub(crate) fn prepare_restart(&self, next_attempt_id: String) -> Result<Self, LixError> {
        self.validate()?;
        if self.receipt.is_some() {
            return Err(invalid());
        }
        let intent = NativeGlobalRestartRequest {
            request: self.request.clone(),
            next_attempt_id,
        };
        intent.validate()?;
        if self
            .restart_intent
            .as_ref()
            .is_some_and(|old| old != &intent)
        {
            return Err(invalid());
        }
        let mut next = self.clone();
        next.restart_intent = Some(intent);
        next.validate()?;
        Ok(next)
    }
    pub(crate) fn acknowledge_restart(
        &self,
        outcome: NativeGlobalRestartReceipt,
    ) -> Result<Self, LixError> {
        let intent = self.restart_intent.as_ref().ok_or_else(invalid)?;
        outcome.validate_for(intent)?;
        if self
            .restart_receipt
            .as_ref()
            .is_some_and(|old| old != &outcome)
        {
            return Err(invalid());
        }
        match &outcome {
            NativeGlobalRestartReceipt::Committed { receipt } => {
                self.acknowledge_merge(receipt.clone())
            }
            NativeGlobalRestartReceipt::Restarted { .. } => {
                let mut next = self.clone();
                next.restart_receipt = Some(outcome);
                next.validate()?;
                Ok(next)
            }
        }
    }
    pub(crate) fn capture_successor(
        &self,
        request: NativeGlobalMigrationRequest,
        boundaries: BTreeMap<String, String>,
    ) -> Result<Self, LixError> {
        self.validate()?;
        request.validate()?;
        let Some(NativeGlobalRestartReceipt::Restarted { intent }) = &self.restart_receipt else {
            return Err(invalid());
        };
        if request.attempt_id != intent.next_attempt_id
            || request.base_commit_id != self.request.base_commit_id
            || request.captured_local_head_commit_id != self.request.captured_local_head_commit_id
            || request.checkpoint_commit_id != self.request.checkpoint_commit_id
            || request.new_branches != self.request.new_branches
        {
            return Err(invalid());
        }
        let mut next = self.clone();
        next.previous_abort = self.restart_receipt.clone();
        next.request = request;
        next.restart_intent = None;
        next.restart_receipt = None;
        next.acknowledged_roots.clear();
        next.frontiers = boundaries
            .into_iter()
            .map(|(branch, accepted)| {
                (
                    branch,
                    GlobalBodyFrontier {
                        accepted,
                        prepared: None,
                    },
                )
            })
            .collect();
        next.validate()?;
        Ok(next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn id(n: u128) -> String {
        uuid::Uuid::from_u128(n).to_string()
    }
    fn journal() -> GlobalConversionJournal {
        let request = NativeGlobalMigrationRequest {
            attempt_id: id(1),
            base_commit_id: id(2),
            expected_authority_head_commit_id: id(3),
            captured_local_head_commit_id: id(4),
            checkpoint_commit_id: id(5),
            new_branches: vec![crate::sync::NativeNewBranchCoordinate {
                branch_id: id(6),
                head_commit_id: id(7),
                checkpoint_commit_id: id(8),
            }],
        };
        GlobalConversionJournal {
            version: 2,
            acknowledged_roots: Default::default(),
            restart_intent: None,
            restart_receipt: None,
            previous_abort: None,
            source_bank: "source-bank".into(),
            manifest_digest: [9; 32],
            request,
            frontiers: BTreeMap::from([
                (
                    GLOBAL_BRANCH_ID.into(),
                    GlobalBodyFrontier {
                        accepted: id(2),
                        prepared: None,
                    },
                ),
                (
                    id(6),
                    GlobalBodyFrontier {
                        accepted: id(8),
                        prepared: None,
                    },
                ),
            ]),
            receipt: None,
            cleanup_complete: false,
        }
    }
    #[test]
    fn lost_wave_response_preserves_identical_prepared_request_after_reload() {
        let original = journal();
        let prepared = original
            .prepare_wave(GLOBAL_BRANCH_ID, &id(2), &id(4))
            .unwrap();
        let bytes = serde_json::to_vec(&prepared).unwrap();
        let reopened: GlobalConversionJournal = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            reopened
                .prepare_wave(GLOBAL_BRANCH_ID, &id(2), &id(4))
                .unwrap(),
            prepared
        );
        assert!(
            reopened
                .prepare_wave(GLOBAL_BRANCH_ID, &id(2), &id(11))
                .is_err()
        );
        assert_eq!(reopened.manifest_digest, original.manifest_digest);
        assert!(
            reopened
                .acknowledge_wave(GLOBAL_BRANCH_ID, &id(3), &id(4))
                .is_err()
        );
    }
    #[test]
    fn global_receipt_requires_every_exact_new_head_acknowledged() {
        let j = journal();
        let receipt = NativeGlobalMigrationReceipt {
            request: j.request.clone(),
            merge_commit_id: id(10),
        };
        assert!(j.acknowledge_merge(receipt.clone()).is_err());
        let j = j
            .prepare_wave(GLOBAL_BRANCH_ID, &id(2), &id(4))
            .unwrap()
            .acknowledge_wave(GLOBAL_BRANCH_ID, &id(2), &id(4))
            .unwrap();
        assert!(j.acknowledge_merge(receipt.clone()).is_err());
        let j = j
            .prepare_wave(&id(6), &id(8), &id(7))
            .unwrap()
            .acknowledge_wave(&id(6), &id(8), &id(7))
            .unwrap()
            .acknowledge_merge(receipt.clone())
            .unwrap();
        assert_eq!(j.acknowledge_merge(receipt).unwrap(), j);
        let mut changed = j.receipt.clone().unwrap();
        changed.request.new_branches[0].checkpoint_commit_id = id(12);
        assert!(j.acknowledge_merge(changed).is_err());
    }
}
