use std::collections::{HashMap, HashSet};

use super::context::{ChangelogScanPage, ChangelogStorageRead};
use super::segment::{
    directory_change_location, directory_commit_location, validate_change_checksum,
    validate_commit_checksum, validate_segment_shape,
};
use super::store::{
    BY_CHANGE_INDEX_SPACE, BY_CHANGE_MEMBERSHIP_INDEX_SPACE, BY_COMMIT_INDEX_SPACE,
    COMMIT_VISIBILITY_SPACE, SEGMENT_SPACE, VISIBLE_CHANGE_PROOF_SPACE,
};
use super::types::{Segment, SegmentChange, SegmentCommit, SegmentObjectLocation};
use crate::LixError;
use crate::changelog::decode_segment;
use crate::storage::{StorageCoreProjection, StorageSpace};

pub(super) struct SegmentTruthIndex {
    pub(super) segment_ids: Vec<String>,
    pub(super) segments: Vec<SegmentTruthRecord>,
    pub(super) commits: HashMap<String, (SegmentObjectLocation, SegmentCommit)>,
    pub(super) changes: HashMap<String, (SegmentObjectLocation, SegmentChange)>,
}

pub(super) type SegmentTruthSnapshot = SegmentTruthIndex;

pub(super) struct SegmentTruthRecord {
    pub(super) segment_id: String,
    pub(super) commit_ids: Vec<String>,
    pub(super) change_ids: Vec<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SegmentTruthSource {
    Stored,
    Staged,
}

#[derive(Clone)]
pub(super) struct SegmentCommitTruthEntry {
    pub(super) source: SegmentTruthSource,
    pub(super) location: SegmentObjectLocation,
    pub(super) commit: SegmentCommit,
}

#[derive(Clone)]
pub(super) struct SegmentChangeTruthEntry {
    pub(super) source: SegmentTruthSource,
    pub(super) location: SegmentObjectLocation,
    pub(super) change: SegmentChange,
}

pub(super) struct SegmentTruthOverlay {
    pub(super) segment_ids: HashSet<String>,
    pub(super) commits: HashMap<String, SegmentCommitTruthEntry>,
    pub(super) changes: HashMap<String, SegmentChangeTruthEntry>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ChangelogSpaceAuthority {
    Primary,
    Publication,
    DerivedRepairable,
}

pub(super) fn changelog_space_authority(space: StorageSpace) -> Option<ChangelogSpaceAuthority> {
    if space == SEGMENT_SPACE {
        Some(ChangelogSpaceAuthority::Primary)
    } else if space == COMMIT_VISIBILITY_SPACE {
        Some(ChangelogSpaceAuthority::Publication)
    } else if space == BY_COMMIT_INDEX_SPACE
        || space == BY_CHANGE_INDEX_SPACE
        || space == BY_CHANGE_MEMBERSHIP_INDEX_SPACE
        || space == VISIBLE_CHANGE_PROOF_SPACE
    {
        Some(ChangelogSpaceAuthority::DerivedRepairable)
    } else {
        None
    }
}

impl SegmentTruthSnapshot {
    pub(super) fn commits_in_segment_order(
        &self,
    ) -> impl Iterator<Item = (&str, &SegmentObjectLocation, &SegmentCommit)> {
        self.segments.iter().flat_map(|segment| {
            segment.commit_ids.iter().map(|commit_id| {
                let (location, commit) = self
                    .commits
                    .get(commit_id)
                    .expect("segment truth record must reference known commit");
                (commit_id.as_str(), location, commit)
            })
        })
    }

    pub(super) fn changes_in_segment_order(
        &self,
    ) -> impl Iterator<Item = (&str, &SegmentObjectLocation, &SegmentChange)> {
        self.segments.iter().flat_map(|segment| {
            segment.change_ids.iter().map(|change_id| {
                let (location, change) = self
                    .changes
                    .get(change_id)
                    .expect("segment truth record must reference known change");
                (change_id.as_str(), location, change)
            })
        })
    }
}

impl SegmentTruthOverlay {
    pub(super) fn contains_segment(&self, segment_id: &str) -> bool {
        self.segment_ids.contains(segment_id)
    }

    pub(super) fn commit(&self, commit_id: &str) -> Option<&SegmentCommitTruthEntry> {
        self.commits.get(commit_id)
    }

    pub(super) fn change(&self, change_id: &str) -> Option<&SegmentChangeTruthEntry> {
        self.changes.get(change_id)
    }
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
) -> Result<SegmentTruthSnapshot, LixError>
where
    S: ChangelogStorageRead + ?Sized,
{
    let mut after = None;
    let mut segment_ids = Vec::new();
    let mut seen_segment_ids = HashSet::new();
    let mut segments = Vec::new();
    let mut commits = HashMap::new();
    let mut changes = HashMap::new();
    loop {
        let page = scan_segment_truth_page(store, after).await?;
        for index in 0..page.len() {
            let (segment_id, segment) =
                decode_validated_scanned_segment(page.key(index), page.value(index))?;
            let mut record = SegmentTruthRecord {
                segment_id: segment_id.clone(),
                commit_ids: Vec::with_capacity(segment.commits.len()),
                change_ids: Vec::with_capacity(segment.changes.len()),
            };
            if !seen_segment_ids.insert(segment_id.clone()) {
                return Err(LixError::unknown(format!(
                    "changelog found duplicate segment id '{segment_id}'"
                )));
            }
            segment_ids.push(segment_id.clone());
            for (commit_id, location, commit) in validated_segment_commit_entries(&segment)? {
                record.commit_ids.push(commit_id.to_string());
                if commits
                    .insert(commit_id.to_string(), (location, commit.clone()))
                    .is_some()
                {
                    return Err(LixError::unknown(format!(
                        "changelog found duplicate commit id '{commit_id}'"
                    )));
                }
            }
            for (change_id, location, change) in validated_segment_change_entries(&segment)? {
                record.change_ids.push(change_id.to_string());
                if changes
                    .insert(change_id.to_string(), (location, change.clone()))
                    .is_some()
                {
                    return Err(LixError::unknown(format!(
                        "changelog found duplicate change id '{change_id}'"
                    )));
                }
            }
            segments.push(record);
        }
        let Some(next_after) = page.resume_after else {
            break;
        };
        after = Some(next_after);
    }
    Ok(SegmentTruthIndex {
        segment_ids,
        segments,
        commits,
        changes,
    })
}

pub(super) async fn find_commit_by_exhaustive_segment_scan<S>(
    store: &mut S,
    commit_id: &str,
) -> Result<Option<(SegmentObjectLocation, SegmentCommit)>, LixError>
where
    S: ChangelogStorageRead + ?Sized,
{
    let mut results =
        find_commits_by_exhaustive_segment_scan(store, std::slice::from_ref(&commit_id)).await?;
    Ok(results.remove(commit_id))
}

pub(super) async fn find_change_by_exhaustive_segment_scan<S>(
    store: &mut S,
    change_id: &str,
) -> Result<Option<(SegmentObjectLocation, SegmentChange)>, LixError>
where
    S: ChangelogStorageRead + ?Sized,
{
    let mut results =
        find_changes_by_exhaustive_segment_scan(store, std::slice::from_ref(&change_id)).await?;
    Ok(results.remove(change_id))
}

pub(super) async fn find_segment_by_exhaustive_segment_scan<S>(
    store: &mut S,
    segment_id: &str,
) -> Result<Option<Segment>, LixError>
where
    S: ChangelogStorageRead + ?Sized,
{
    let mut found = None::<Segment>;
    scan_segment_truth(store, |loaded_segment_id, segment| {
        if loaded_segment_id != segment_id {
            return Ok(());
        }
        if found.is_some() {
            return Err(LixError::unknown(format!(
                "changelog segment '{segment_id}' appears multiple times"
            )));
        }
        found = Some(segment.clone());
        Ok(())
    })
    .await?;
    Ok(found)
}

pub(super) async fn find_commits_by_exhaustive_segment_scan<S>(
    store: &mut S,
    commit_ids: &[&str],
) -> Result<HashMap<String, (SegmentObjectLocation, SegmentCommit)>, LixError>
where
    S: ChangelogStorageRead + ?Sized,
{
    let requested = commit_ids.iter().copied().collect::<HashSet<_>>();
    let mut found = HashMap::<String, (SegmentObjectLocation, SegmentCommit)>::new();
    scan_segment_truth(store, |_, segment| {
        for (commit_id, location, commit) in validated_segment_commit_entries(segment)? {
            if !requested.contains(commit_id) {
                continue;
            }
            if let Some((existing, _)) =
                found.insert(commit_id.to_string(), (location.clone(), commit.clone()))
            {
                return Err(LixError::unknown(format!(
                    "changelog commit '{commit_id}' appears in multiple segments: '{}' and '{}'",
                    existing.segment_id, location.segment_id
                )));
            }
        }
        Ok(())
    })
    .await?;
    Ok(found)
}

pub(super) async fn find_changes_by_exhaustive_segment_scan<S>(
    store: &mut S,
    change_ids: &[&str],
) -> Result<HashMap<String, (SegmentObjectLocation, SegmentChange)>, LixError>
where
    S: ChangelogStorageRead + ?Sized,
{
    let requested = change_ids.iter().copied().collect::<HashSet<_>>();
    let mut found = HashMap::<String, (SegmentObjectLocation, SegmentChange)>::new();
    scan_segment_truth(store, |_, segment| {
        for (change_id, location, change) in validated_segment_change_entries(segment)? {
            if !requested.contains(change_id) {
                continue;
            }
            if let Some((existing, _)) =
                found.insert(change_id.to_string(), (location.clone(), change.clone()))
            {
                return Err(LixError::unknown(format!(
                    "changelog change '{change_id}' appears in multiple segments: '{}' and '{}'",
                    existing.segment_id, location.segment_id
                )));
            }
        }
        return Ok(());
    })
    .await?;
    Ok(found)
}

pub(super) fn build_segment_truth_overlay<'a>(
    stored: &SegmentTruthSnapshot,
    staged: impl IntoIterator<Item = &'a Segment>,
) -> Result<SegmentTruthOverlay, LixError> {
    let mut segment_ids = stored.segment_ids.iter().cloned().collect::<HashSet<_>>();
    let mut commits = stored
        .commits
        .iter()
        .map(|(id, (location, commit))| {
            (
                id.clone(),
                SegmentCommitTruthEntry {
                    source: SegmentTruthSource::Stored,
                    location: location.clone(),
                    commit: commit.clone(),
                },
            )
        })
        .collect::<HashMap<_, _>>();
    let mut changes = stored
        .changes
        .iter()
        .map(|(id, (location, change))| {
            (
                id.clone(),
                SegmentChangeTruthEntry {
                    source: SegmentTruthSource::Stored,
                    location: location.clone(),
                    change: change.clone(),
                },
            )
        })
        .collect::<HashMap<_, _>>();

    for segment in staged {
        validate_segment_shape(segment)?;
        if !segment_ids.insert(segment.header.segment_id.clone()) {
            return Err(LixError::unknown(format!(
                "changelog segment '{}' already exists",
                segment.header.segment_id
            )));
        }
        for (commit_id, location, commit) in validated_segment_commit_entries(segment)? {
            if commits
                .insert(
                    commit_id.to_string(),
                    SegmentCommitTruthEntry {
                        source: SegmentTruthSource::Staged,
                        location,
                        commit: commit.clone(),
                    },
                )
                .is_some()
            {
                return Err(LixError::unknown(format!(
                    "changelog commit '{commit_id}' already exists in another segment"
                )));
            }
        }
        for (change_id, location, change) in validated_segment_change_entries(segment)? {
            if changes
                .insert(
                    change_id.to_string(),
                    SegmentChangeTruthEntry {
                        source: SegmentTruthSource::Staged,
                        location,
                        change: change.clone(),
                    },
                )
                .is_some()
            {
                return Err(LixError::unknown(format!(
                    "changelog change '{change_id}' already exists in another segment"
                )));
            }
        }
    }

    Ok(SegmentTruthOverlay {
        segment_ids,
        commits,
        changes,
    })
}

async fn scan_segment_truth<S>(
    store: &mut S,
    mut visit: impl FnMut(&str, &Segment) -> Result<(), LixError>,
) -> Result<(), LixError>
where
    S: ChangelogStorageRead + ?Sized,
{
    let mut after = None;
    loop {
        let page = scan_segment_truth_page(store, after).await?;
        for index in 0..page.len() {
            let (_, segment) =
                decode_validated_scanned_segment(page.key(index), page.value(index))?;
            visit(&segment.header.segment_id, &segment)?;
        }
        let Some(next_after) = page.resume_after else {
            break;
        };
        after = Some(next_after);
    }
    Ok(())
}

async fn scan_segment_truth_page<S>(
    store: &mut S,
    after: Option<Vec<u8>>,
) -> Result<ChangelogScanPage, LixError>
where
    S: ChangelogStorageRead + ?Sized,
{
    store
        .changelog_scan(
            SEGMENT_SPACE,
            Vec::new(),
            after,
            64,
            StorageCoreProjection::FullValue,
        )
        .await
}

fn decode_validated_scanned_segment(
    key: Option<&[u8]>,
    value: Option<&[u8]>,
) -> Result<(String, Segment), LixError> {
    let Some(key) = key else {
        return Err(LixError::unknown(
            "changelog segment scan returned row without key",
        ));
    };
    let segment_id = std::str::from_utf8(key)
        .map_err(|error| {
            LixError::unknown(format!(
                "changelog found invalid UTF-8 segment key: {error}"
            ))
        })?
        .to_string();
    let Some(bytes) = value else {
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
    Ok((segment_id, segment))
}

fn validated_segment_commit_entries(
    segment: &Segment,
) -> Result<Vec<(&str, SegmentObjectLocation, &SegmentCommit)>, LixError> {
    segment
        .commits
        .iter()
        .map(|commit| {
            let commit_id = commit.header.id.as_str();
            let location = directory_commit_location(segment, commit_id)?;
            validate_commit_checksum(&location.checksum, commit_id, commit)?;
            Ok((commit_id, location, commit))
        })
        .collect()
}

fn validated_segment_change_entries(
    segment: &Segment,
) -> Result<Vec<(&str, SegmentObjectLocation, &SegmentChange)>, LixError> {
    segment
        .changes
        .iter()
        .map(|change| {
            let change_id = change.id.as_str();
            let location = directory_change_location(segment, change_id)?;
            validate_change_checksum(&location.checksum, change_id, change)?;
            Ok((change_id, location, change))
        })
        .collect()
}
