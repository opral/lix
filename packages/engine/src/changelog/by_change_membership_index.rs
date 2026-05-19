//! Rebuildable by_change_membership index behavior.

use super::truth::SegmentTruthSnapshot;
use super::types::{CommitId, Segment};

pub(super) struct ByChangeMembershipEntry {
    #[allow(dead_code)]
    pub(super) change_id: String,
    #[allow(dead_code)]
    pub(super) commit_id: CommitId,
}

pub(super) fn by_change_membership_entries_for_segments(
    segments: &[Segment],
) -> Vec<ByChangeMembershipEntry> {
    let mut entries = Vec::new();
    for segment in segments {
        for commit in &segment.commits {
            for membership in &commit.body.membership {
                entries.push(ByChangeMembershipEntry {
                    change_id: membership.member_change_id.clone(),
                    commit_id: commit.header.id.clone(),
                });
            }
        }
    }
    entries
}

pub(super) fn by_change_membership_entries_for_truth(
    truth: &SegmentTruthSnapshot,
) -> Vec<ByChangeMembershipEntry> {
    let mut entries = Vec::new();
    for (commit_id, _, commit) in truth.commits_in_segment_order() {
        for membership in &commit.body.membership {
            entries.push(ByChangeMembershipEntry {
                change_id: membership.member_change_id.clone(),
                commit_id: commit_id.to_string(),
            });
        }
    }
    entries
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changelog::{
        CommitBody, CommitHeader, MembershipRecord, MembershipRole, SegmentCommit,
        SegmentCommitDirectory, SegmentDirectory, SegmentHeader,
    };

    #[test]
    fn entries_include_authored_and_adopted_membership() {
        let segment = super::Segment {
            header: SegmentHeader {
                segment_id: "segment-1".to_string(),
                format_version: 1,
                commit_count: 2,
                change_count: 0,
                byte_count: 0,
                payload_count: 0,
                checksum: String::new(),
            },
            directory: SegmentDirectory::default(),
            commits: vec![
                commit("commit-authored", "change-1", MembershipRole::Authored),
                commit("commit-adopted", "change-1", MembershipRole::Adopted),
            ],
            changes: Vec::new(),
        };

        let entries = by_change_membership_entries_for_segments(&[segment]);

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].change_id, "change-1");
        assert_eq!(entries[0].commit_id, "commit-authored");
        assert_eq!(entries[1].change_id, "change-1");
        assert_eq!(entries[1].commit_id, "commit-adopted");
    }

    fn commit(commit_id: &str, change_id: &str, role: MembershipRole) -> SegmentCommit {
        SegmentCommit {
            header: CommitHeader {
                id: commit_id.to_string(),
                parent_commit_ids: Vec::new(),
                derivable_change_id: format!("{commit_id}-derivable"),
                author_account_ids: vec!["account-1".to_string()],
                created_at: "2026-05-12T00:00:00Z".to_string(),
                membership_count: 1,
            },
            body: CommitBody {
                membership: vec![MembershipRecord {
                    member_change_id: change_id.to_string(),
                    role,
                    source_parent_ordinal: match role {
                        MembershipRole::Authored => None,
                        MembershipRole::Adopted => Some(0),
                    },
                }],
            },
            directory: SegmentCommitDirectory::default(),
            checksum: format!("{commit_id}-checksum"),
        }
    }
}
