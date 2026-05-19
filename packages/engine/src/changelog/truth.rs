use std::collections::{HashMap, HashSet};

use super::context::ChangelogStorageRead;
use super::segment::{
    directory_change_location, directory_commit_location, validate_change_checksum,
    validate_commit_checksum, validate_segment_shape,
};
use super::store::SEGMENT_SPACE;
use super::types::{SegmentChange, SegmentCommit, SegmentObjectLocation};
use crate::LixError;
use crate::changelog::decode_segment;
use crate::storage::StorageCoreProjection;

pub(super) struct SegmentTruthIndex {
    pub(super) segment_ids: Vec<String>,
    pub(super) commits: HashMap<String, (SegmentObjectLocation, SegmentCommit)>,
    pub(super) changes: HashMap<String, (SegmentObjectLocation, SegmentChange)>,
}

pub(super) struct RetainedPrimaryClosure {
    pub(super) segments: HashSet<String>,
    pub(super) commits: HashSet<String>,
    pub(super) changes: HashSet<String>,
}

pub(super) fn compute_retained_primary_closure(
    truth: &SegmentTruthIndex,
    mut segments: HashSet<String>,
) -> Result<RetainedPrimaryClosure, LixError> {
    let mut commits = HashSet::new();
    let mut changes = HashSet::new();
    let mut changed = true;
    while changed {
        changed = false;
        for (commit_id, (location, commit)) in &truth.commits {
            if !segments.contains(&location.segment_id) {
                continue;
            }
            commits.insert(commit_id.clone());
            for parent_id in &commit.header.parent_commit_ids {
                let Some((parent_location, _)) = truth.commits.get(parent_id) else {
                    return Err(LixError::unknown(format!(
                        "changelog retained commit '{commit_id}' references missing parent commit '{parent_id}'"
                    )));
                };
                if segments.insert(parent_location.segment_id.clone()) {
                    changed = true;
                }
            }
            for membership in &commit.body.membership {
                let Some((change_location, _)) = truth.changes.get(&membership.member_change_id)
                else {
                    return Err(LixError::unknown(format!(
                        "changelog retained commit '{commit_id}' references missing change '{}'",
                        membership.member_change_id
                    )));
                };
                if segments.insert(change_location.segment_id.clone()) {
                    changed = true;
                }
            }
        }
        for (change_id, (location, _)) in &truth.changes {
            if segments.contains(&location.segment_id) {
                changes.insert(change_id.clone());
            }
        }
    }
    Ok(RetainedPrimaryClosure {
        segments,
        commits,
        changes,
    })
}

pub(super) async fn load_segment_truth_index<S>(
    store: &mut S,
) -> Result<SegmentTruthIndex, LixError>
where
    S: ChangelogStorageRead + ?Sized,
{
    let mut after = None;
    let mut segment_ids = Vec::new();
    let mut commits = HashMap::new();
    let mut changes = HashMap::new();
    loop {
        let page = store
            .changelog_scan(
                SEGMENT_SPACE,
                Vec::new(),
                after,
                64,
                StorageCoreProjection::FullValue,
            )
            .await?;
        for index in 0..page.len() {
            let Some(key) = page.key(index) else {
                continue;
            };
            let segment_id = std::str::from_utf8(key)
                .map_err(|error| {
                    LixError::unknown(format!(
                        "changelog found invalid UTF-8 segment key: {error}"
                    ))
                })?
                .to_string();
            let Some(bytes) = page.value(index) else {
                return Err(LixError::unknown(format!(
                    "changelog segment '{segment_id}' scan returned key without value"
                )));
            };
            let segment = decode_segment(bytes)?;
            validate_segment_shape(&segment)?;
            if segment.header.segment_id != segment_id {
                return Err(LixError::unknown(format!(
                    "changelog segment key '{segment_id}' contains segment '{}'",
                    segment.header.segment_id
                )));
            }
            if !segment_ids.iter().any(|existing| existing == &segment_id) {
                segment_ids.push(segment_id);
            }
            for commit in &segment.commits {
                let location = directory_commit_location(&segment, &commit.header.id)?;
                validate_commit_checksum(&location.checksum, &commit.header.id, commit)?;
                if commits
                    .insert(commit.header.id.clone(), (location, commit.clone()))
                    .is_some()
                {
                    return Err(LixError::unknown(format!(
                        "changelog found duplicate commit id '{}'",
                        commit.header.id
                    )));
                }
            }
            for change in &segment.changes {
                let location = directory_change_location(&segment, &change.id)?;
                validate_change_checksum(&location.checksum, &change.id, change)?;
                if changes
                    .insert(change.id.clone(), (location, change.clone()))
                    .is_some()
                {
                    return Err(LixError::unknown(format!(
                        "changelog found duplicate change id '{}'",
                        change.id
                    )));
                }
            }
        }
        let Some(next_after) = page.resume_after else {
            break;
        };
        after = Some(next_after);
    }
    Ok(SegmentTruthIndex {
        segment_ids,
        commits,
        changes,
    })
}
