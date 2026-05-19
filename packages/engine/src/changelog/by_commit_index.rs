//! Rebuildable by_commit index behavior.

use std::collections::{HashMap, HashSet};

use super::segment::directory_commit_location;
use super::types::{ByCommitEntry, Segment};
use crate::LixError;

pub(super) fn by_commit_entries_for_segment(
    segment: &Segment,
    external_generations: &HashMap<String, u64>,
) -> Result<Vec<ByCommitEntry>, LixError> {
    let generations = segment_commit_generations(segment, external_generations)?;
    segment
        .commits
        .iter()
        .map(|commit| {
            Ok(ByCommitEntry {
                commit_id: commit.header.id.clone(),
                location: directory_commit_location(segment, &commit.header.id)?,
                parent_commit_ids: commit.header.parent_commit_ids.clone(),
                generation: *generations.get(&commit.header.id).ok_or_else(|| {
                    LixError::unknown(format!(
                        "changelog segment '{}' did not compute generation for commit '{}'",
                        segment.header.segment_id, commit.header.id
                    ))
                })?,
            })
        })
        .collect()
}

pub(super) fn by_commit_entries_for_segments(
    segments: &[Segment],
) -> Result<Vec<ByCommitEntry>, LixError> {
    let generations = rebuilt_commit_generations(segments)?;
    let mut entries = Vec::new();
    let mut seen = HashSet::new();
    for segment in segments {
        for commit in &segment.commits {
            if !seen.insert(commit.header.id.as_str()) {
                return Err(LixError::unknown(format!(
                    "changelog index rebuild found duplicate commit '{}'",
                    commit.header.id
                )));
            }
            entries.push(ByCommitEntry {
                commit_id: commit.header.id.clone(),
                location: directory_commit_location(segment, &commit.header.id)?,
                parent_commit_ids: commit.header.parent_commit_ids.clone(),
                generation: *generations.get(&commit.header.id).ok_or_else(|| {
                    LixError::unknown(format!(
                        "changelog index rebuild did not compute generation for commit '{}'",
                        commit.header.id
                    ))
                })?,
            });
        }
    }
    Ok(entries)
}

fn segment_commit_generations(
    segment: &Segment,
    external_generations: &HashMap<String, u64>,
) -> Result<HashMap<String, u64>, LixError> {
    let mut generations = HashMap::new();
    let parents_by_commit = segment
        .commits
        .iter()
        .map(|commit| {
            (
                commit.header.id.clone(),
                commit.header.parent_commit_ids.clone(),
            )
        })
        .collect::<HashMap<_, _>>();
    let mut visiting = HashSet::new();
    for commit_id in parents_by_commit.keys() {
        let generation = segment_commit_generation(
            commit_id,
            &parents_by_commit,
            external_generations,
            &mut generations,
            &mut visiting,
        )?;
        generations.insert(commit_id.clone(), generation);
    }
    Ok(generations)
}

fn segment_commit_generation(
    commit_id: &str,
    parents_by_commit: &HashMap<String, Vec<String>>,
    external_generations: &HashMap<String, u64>,
    generations: &mut HashMap<String, u64>,
    visiting: &mut HashSet<String>,
) -> Result<u64, LixError> {
    if let Some(generation) = generations.get(commit_id) {
        return Ok(*generation);
    }
    if !visiting.insert(commit_id.to_string()) {
        return Err(LixError::unknown(format!(
            "changelog commit graph contains a parent cycle at '{commit_id}'"
        )));
    }

    let Some(parent_ids) = parents_by_commit.get(commit_id) else {
        visiting.remove(commit_id);
        return Err(LixError::unknown(format!(
            "changelog segment generation requested unknown local commit '{commit_id}'"
        )));
    };

    let mut generation = 0;
    for parent_id in parent_ids {
        let parent_generation = if parents_by_commit.contains_key(parent_id) {
            segment_commit_generation(
                parent_id,
                parents_by_commit,
                external_generations,
                generations,
                visiting,
            )?
        } else {
            external_generations.get(parent_id).copied().ok_or_else(|| {
                LixError::unknown(format!(
                    "cannot compute generation for changelog commit '{commit_id}' because parent '{parent_id}' has no segment-derived generation"
                ))
            })?
        };
        generation = generation.max(parent_generation.saturating_add(1));
    }

    visiting.remove(commit_id);
    generations.insert(commit_id.to_string(), generation);
    Ok(generation)
}

fn rebuilt_commit_generations(segments: &[Segment]) -> Result<HashMap<String, u64>, LixError> {
    let mut parents_by_commit = HashMap::new();
    for segment in segments {
        for commit in &segment.commits {
            if parents_by_commit
                .insert(
                    commit.header.id.clone(),
                    commit.header.parent_commit_ids.clone(),
                )
                .is_some()
            {
                return Err(LixError::unknown(format!(
                    "changelog index rebuild found duplicate commit '{}'",
                    commit.header.id
                )));
            }
        }
    }

    let mut generations = HashMap::new();
    let mut visiting = HashSet::new();
    for commit_id in parents_by_commit.keys() {
        let generation = rebuilt_commit_generation(
            commit_id,
            &parents_by_commit,
            &mut generations,
            &mut visiting,
        )?;
        generations.insert(commit_id.clone(), generation);
    }
    Ok(generations)
}

fn rebuilt_commit_generation(
    commit_id: &str,
    parents_by_commit: &HashMap<String, Vec<String>>,
    generations: &mut HashMap<String, u64>,
    visiting: &mut HashSet<String>,
) -> Result<u64, LixError> {
    if let Some(generation) = generations.get(commit_id) {
        return Ok(*generation);
    }
    if !visiting.insert(commit_id.to_string()) {
        return Err(LixError::unknown(format!(
            "changelog commit graph contains a parent cycle at '{commit_id}'"
        )));
    }
    let Some(parent_ids) = parents_by_commit.get(commit_id) else {
        return Err(LixError::unknown(format!(
            "cannot rebuild by_commit generation because parent commit '{commit_id}' is missing from changelog segments"
        )));
    };
    let mut generation = 0;
    for parent_id in parent_ids {
        let parent_generation =
            rebuilt_commit_generation(parent_id, parents_by_commit, generations, visiting)?;
        generation = generation.max(parent_generation.saturating_add(1));
    }
    visiting.remove(commit_id);
    generations.insert(commit_id.to_string(), generation);
    Ok(generation)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::changelog::{
        CommitBody, CommitHeader, SegmentCommit, SegmentCommitDirectory, SegmentDirectory,
        SegmentHeader, SegmentObjectLocation,
    };

    #[test]
    fn segment_entries_compute_generation_values() {
        let segment = segment_with_commits(vec![
            commit("root", vec![]),
            commit("child", vec!["root"]),
            commit("merge", vec!["root", "external-parent"]),
        ]);
        let external = HashMap::from([("external-parent".to_string(), 10)]);

        let entries = by_commit_entries_for_segment(&segment, &external).unwrap();

        assert_eq!(generation(&entries, "root"), 0);
        assert_eq!(generation(&entries, "child"), 1);
        assert_eq!(generation(&entries, "merge"), 11);
    }

    #[test]
    fn segment_entries_error_on_missing_external_parent_generation() {
        let segment = segment_with_commits(vec![commit("child", vec!["missing-parent"])]);

        let error = by_commit_entries_for_segment(&segment, &HashMap::new())
            .expect_err("missing external parent generation must error");

        assert!(
            error
                .message
                .contains("parent 'missing-parent' has no segment-derived generation"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_entries_error_on_local_parent_cycle() {
        let segment = segment_with_commits(vec![
            commit("left", vec!["right"]),
            commit("right", vec!["left"]),
        ]);

        let error = by_commit_entries_for_segment(&segment, &HashMap::new())
            .expect_err("local parent cycle must error");

        assert!(
            error.message.contains("parent cycle"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn rebuild_entries_error_on_missing_parent() {
        let segment = segment_with_commits(vec![commit("child", vec!["missing-parent"])]);

        let error = by_commit_entries_for_segments(&[segment])
            .expect_err("missing rebuild parent must error");

        assert!(
            error.message.contains(
                "cannot rebuild by_commit generation because parent commit 'missing-parent' is missing"
            ),
            "unexpected error: {error}"
        );
    }

    fn generation(entries: &[ByCommitEntry], commit_id: &str) -> u64 {
        entries
            .iter()
            .find(|entry| entry.commit_id == commit_id)
            .unwrap()
            .generation
    }

    fn segment_with_commits(commits: Vec<SegmentCommit>) -> Segment {
        Segment {
            header: SegmentHeader {
                segment_id: "segment-1".to_string(),
                format_version: 1,
                commit_count: commits.len() as u32,
                change_count: 0,
                byte_count: 0,
                payload_count: 0,
                checksum: String::new(),
            },
            directory: SegmentDirectory {
                commits: commits
                    .iter()
                    .enumerate()
                    .map(|(ordinal, commit)| {
                        (
                            commit.header.id.clone(),
                            location("segment-1", ordinal as u64, &commit.checksum),
                        )
                    })
                    .collect(),
                changes: Vec::new(),
            },
            commits,
            changes: Vec::new(),
        }
    }

    fn commit(id: &str, parent_commit_ids: Vec<&str>) -> SegmentCommit {
        SegmentCommit {
            header: CommitHeader {
                id: id.to_string(),
                parent_commit_ids: parent_commit_ids.into_iter().map(str::to_string).collect(),
                derivable_change_id: format!("{id}-derivable"),
                author_account_ids: vec!["account-1".to_string()],
                created_at: "2026-05-12T00:00:00Z".to_string(),
                membership_count: 0,
            },
            body: CommitBody {
                membership: Vec::new(),
            },
            directory: SegmentCommitDirectory::default(),
            checksum: format!("{id}-checksum"),
        }
    }

    fn location(segment_id: &str, offset: u64, checksum: &str) -> SegmentObjectLocation {
        SegmentObjectLocation {
            segment_id: segment_id.to_string(),
            offset,
            len: 0,
            checksum: checksum.to_string(),
        }
    }
}
