//! Segment canonicalization, directory lookup, and physical invariants.

use std::collections::{HashMap, HashSet};

use super::codec::{
    decode_segment, decode_segment_change, decode_segment_commit,
    encode_segment_with_object_locations, view_segment_directory, view_segment_object_ranges,
    view_segment_object_slices,
};
use super::store::segment_value;
use super::types::{
    MembershipRole, Segment, SegmentChange, SegmentCommit, SegmentDirectory, SegmentObjectLocation,
    SegmentPayloadLocation, StateRowIdentity,
};
use crate::common::{CanonicalSchemaKey, EntityId, FileId};
use crate::LixError;

pub(super) fn directory_commit_location(
    segment: &Segment,
    commit_id: &str,
) -> Result<SegmentObjectLocation, LixError> {
    Ok(directory_commit_location_ref(segment, commit_id)?.clone())
}

pub(super) fn directory_commit_location_ref<'a>(
    segment: &'a Segment,
    commit_id: &str,
) -> Result<&'a SegmentObjectLocation, LixError> {
    segment
        .directory
        .commits
        .iter()
        .find_map(|(candidate, location)| {
            if candidate == commit_id {
                Some(location)
            } else {
                None
            }
        })
        .ok_or_else(|| {
            LixError::unknown(format!(
                "changelog segment '{}' is missing directory location for commit '{}'",
                segment.header.segment_id, commit_id
            ))
        })
}

pub(super) fn directory_change_location(
    segment: &Segment,
    change_id: &str,
) -> Result<SegmentObjectLocation, LixError> {
    Ok(directory_change_location_ref(segment, change_id)?.clone())
}

pub(super) fn directory_change_location_ref<'a>(
    segment: &'a Segment,
    change_id: &str,
) -> Result<&'a SegmentObjectLocation, LixError> {
    segment
        .directory
        .changes
        .iter()
        .find_map(|(candidate, location)| {
            if candidate == change_id {
                Some(location)
            } else {
                None
            }
        })
        .ok_or_else(|| {
            LixError::unknown(format!(
                "changelog segment '{}' is missing directory location for change '{}'",
                segment.header.segment_id, change_id
            ))
        })
}

pub(super) struct DecodedSegmentIndex {
    bytes: Vec<u8>,
    segment_id: String,
    commit_ordinals: HashMap<String, usize>,
    change_ordinals: HashMap<String, usize>,
    commit_locations: HashMap<String, SegmentObjectLocation>,
    change_locations: HashMap<String, SegmentObjectLocation>,
    commit_ranges: HashMap<String, SegmentObjectRangeMeta>,
    change_ranges: HashMap<String, SegmentObjectRangeMeta>,
}

struct SegmentObjectRangeMeta {
    offset: u64,
    len: u64,
    encoded_checksum: Option<String>,
}

impl DecodedSegmentIndex {
    pub(super) fn decode(bytes: &[u8]) -> Result<Self, LixError> {
        let view = view_segment_directory(bytes)?;
        let segment_id = view.segment_id.to_string();
        let mut commit_ordinals = HashMap::new();
        let mut commit_locations = HashMap::new();
        for (ordinal, entry) in view.directory_commits.iter().enumerate() {
            commit_ordinals.insert(entry.id.to_string(), ordinal);
            commit_locations.insert(
                entry.id.to_string(),
                SegmentObjectLocation {
                    segment_id: entry.location.segment_id.to_string(),
                    offset: entry.location.offset,
                    len: entry.location.len,
                    checksum: entry.location.checksum.to_string(),
                },
            );
        }
        let mut change_ordinals = HashMap::new();
        let mut change_locations = HashMap::new();
        for (ordinal, entry) in view.directory_changes.iter().enumerate() {
            change_ordinals.insert(entry.id.to_string(), ordinal);
            change_locations.insert(
                entry.id.to_string(),
                SegmentObjectLocation {
                    segment_id: entry.location.segment_id.to_string(),
                    offset: entry.location.offset,
                    len: entry.location.len,
                    checksum: entry.location.checksum.to_string(),
                },
            );
        }
        let (commit_ranges, change_ranges) = view_segment_object_ranges(bytes)?;
        let commit_ranges = commit_ranges
            .into_iter()
            .map(|slice| {
                (
                    slice.id.to_string(),
                    SegmentObjectRangeMeta {
                        offset: slice.offset,
                        len: slice.len,
                        encoded_checksum: slice.encoded_checksum.map(str::to_string),
                    },
                )
            })
            .collect();
        let change_ranges = change_ranges
            .into_iter()
            .map(|slice| {
                (
                    slice.id.to_string(),
                    SegmentObjectRangeMeta {
                        offset: slice.offset,
                        len: slice.len,
                        encoded_checksum: slice.encoded_checksum.map(str::to_string),
                    },
                )
            })
            .collect();
        Ok(Self {
            bytes: bytes.to_vec(),
            segment_id,
            commit_ordinals,
            change_ordinals,
            commit_locations,
            change_locations,
            commit_ranges,
            change_ranges,
        })
    }

    pub(super) fn contains_commit(&self, commit_id: &str) -> bool {
        self.commit_ordinals.contains_key(commit_id)
    }

    pub(super) fn commit_location(&self, commit_id: &str) -> Option<&SegmentObjectLocation> {
        self.commit_locations.get(commit_id)
    }

    pub(super) fn contains_change(&self, change_id: &str) -> bool {
        self.change_ordinals.contains_key(change_id)
    }

    pub(super) fn commit(&self, commit_id: &str) -> Result<Option<SegmentCommit>, LixError> {
        let Some(location) = self.commit_locations.get(commit_id) else {
            return Ok(None);
        };
        let bytes = self.object_bytes(location, "commit", commit_id)?;
        let commit = decode_segment_commit(bytes)?;
        if commit.header.id != commit_id {
            return Err(LixError::unknown(format!(
                "changelog commit locator for '{commit_id}' decoded commit '{}'",
                commit.header.id
            )));
        }
        Ok(Some(commit))
    }

    pub(super) fn change(&self, change_id: &str) -> Result<Option<SegmentChange>, LixError> {
        let Some(location) = self.change_locations.get(change_id) else {
            return Ok(None);
        };
        let bytes = self.object_bytes(location, "change", change_id)?;
        let change = decode_segment_change(bytes)?;
        if change.id != change_id {
            return Err(LixError::unknown(format!(
                "changelog change locator for '{change_id}' decoded change '{}'",
                change.id
            )));
        }
        Ok(Some(change))
    }

    pub(super) fn validate_commit_location(
        &self,
        location: &SegmentObjectLocation,
        commit_id: &str,
    ) -> Result<(), LixError> {
        let Some(expected) = self.commit_locations.get(commit_id) else {
            return Err(LixError::unknown(format!(
                "changelog segment '{}' is missing directory location for commit '{}'",
                self.segment_id, commit_id
            )));
        };
        if location != expected {
            return Err(LixError::unknown(format!(
                "changelog commit '{commit_id}' locator does not match segment directory"
            )));
        }
        self.validate_commit_range(location, commit_id)?;
        Ok(())
    }

    pub(super) fn validate_change_location(
        &self,
        location: &SegmentObjectLocation,
        change_id: &str,
    ) -> Result<(), LixError> {
        let Some(expected) = self.change_locations.get(change_id) else {
            return Err(LixError::unknown(format!(
                "changelog segment '{}' is missing directory location for change '{}'",
                self.segment_id, change_id
            )));
        };
        if location != expected {
            return Err(LixError::unknown(format!(
                "changelog change '{change_id}' locator does not match segment directory"
            )));
        }
        self.validate_change_range(location, change_id)?;
        let Some(change) = self.change(change_id)? else {
            return Err(LixError::unknown(format!(
                "changelog segment '{}' is missing change '{}'",
                self.segment_id, change_id
            )));
        };
        validate_change_checksum(&location.checksum, change_id, &change)?;
        Ok(())
    }

    fn validate_commit_range(
        &self,
        location: &SegmentObjectLocation,
        commit_id: &str,
    ) -> Result<(), LixError> {
        let Some(range) = self.commit_ranges.get(commit_id) else {
            return Err(LixError::unknown(format!(
                "changelog segment '{}' is missing encoded commit object '{}'",
                self.segment_id, commit_id
            )));
        };
        if location.offset != range.offset || location.len != range.len {
            return Err(LixError::unknown(format!(
                "changelog commit '{commit_id}' locator does not match encoded object slice"
            )));
        }
        if range.encoded_checksum.as_deref() != Some(location.checksum.as_str()) {
            return Err(LixError::unknown(format!(
                "changelog commit '{commit_id}' locator checksum does not match encoded object checksum"
            )));
        }
        Ok(())
    }

    fn validate_change_range(
        &self,
        location: &SegmentObjectLocation,
        change_id: &str,
    ) -> Result<(), LixError> {
        let Some(range) = self.change_ranges.get(change_id) else {
            return Err(LixError::unknown(format!(
                "changelog segment '{}' is missing encoded change object '{}'",
                self.segment_id, change_id
            )));
        };
        if location.offset != range.offset || location.len != range.len {
            return Err(LixError::unknown(format!(
                "changelog change '{change_id}' locator does not match encoded object slice"
            )));
        }
        Ok(())
    }

    fn object_bytes(
        &self,
        location: &SegmentObjectLocation,
        kind: &str,
        id: &str,
    ) -> Result<&[u8], LixError> {
        if location.segment_id != self.segment_id {
            return Err(LixError::unknown(format!(
                "changelog {kind} '{id}' locator points to segment '{}' but loaded '{}'",
                location.segment_id, self.segment_id
            )));
        }
        let start = usize::try_from(location.offset).map_err(|_| {
            LixError::unknown(format!(
                "changelog {kind} '{id}' locator offset does not fit usize"
            ))
        })?;
        let len = usize::try_from(location.len).map_err(|_| {
            LixError::unknown(format!(
                "changelog {kind} '{id}' locator len does not fit usize"
            ))
        })?;
        let end = start.checked_add(len).ok_or_else(|| {
            LixError::unknown(format!("changelog {kind} '{id}' locator range overflows"))
        })?;
        self.bytes.get(start..end).ok_or_else(|| {
            LixError::unknown(format!(
                "changelog {kind} '{id}' locator range is outside segment '{}'",
                self.segment_id
            ))
        })
    }
}

pub(super) fn canonicalize_segment(mut segment: Segment) -> Result<Segment, LixError> {
    let segment_id = segment.header.segment_id.clone();
    segment.header.format_version = 1;
    segment.header.commit_count = u32::try_from(segment.commits.len()).map_err(|_| {
        LixError::unknown(format!(
            "changelog segment '{segment_id}' has too many commits"
        ))
    })?;
    segment.header.change_count = u32::try_from(segment.changes.len()).map_err(|_| {
        LixError::unknown(format!(
            "changelog segment '{segment_id}' has too many changes"
        ))
    })?;
    segment.header.payload_count = segment
        .changes
        .iter()
        .try_fold(0_u32, |count, change| {
            let payload_count = u32::try_from(change.inline_payloads.len()).map_err(|_| ())?;
            count.checked_add(payload_count).ok_or(())
        })
        .map_err(|_| {
            LixError::unknown(format!(
                "changelog segment '{segment_id}' inline payload count exceeds u32"
            ))
        })?;

    for commit in &mut segment.commits {
        commit.header.membership_count =
            u32::try_from(commit.body.membership.len()).map_err(|_| {
                LixError::unknown(format!(
                    "changelog commit '{}' has too many membership records",
                    commit.header.id
                ))
            })?;
        commit.checksum = checksum_commit(commit)?;
    }

    let mut commit_locations = Vec::with_capacity(segment.commits.len());
    for commit in &segment.commits {
        commit_locations.push((
            commit.header.id.clone(),
            SegmentObjectLocation {
                segment_id: segment_id.clone(),
                offset: 0,
                len: 0,
                checksum: commit.checksum.clone(),
            },
        ));
    }

    let mut change_locations = Vec::with_capacity(segment.changes.len());
    let mut change_checksums = Vec::with_capacity(segment.changes.len());
    for change in &mut segment.changes {
        change.directory.payloads = change
            .inline_payloads
            .iter()
            .enumerate()
            .map(|(payload_ordinal, payload)| SegmentPayloadLocation {
                json_ref: payload.json_ref.clone(),
                offset: payload_ordinal as u64,
                len: payload.bytes.len() as u64,
            })
            .collect();
        let change_checksum = checksum_change(change)?;
        change_checksums.push((change.id.clone(), change_checksum.clone()));
        change_locations.push((
            change.id.clone(),
            SegmentObjectLocation {
                segment_id: segment_id.clone(),
                offset: 0,
                len: 0,
                checksum: change_checksum,
            },
        ));
    }

    segment.directory = SegmentDirectory {
        commits: commit_locations,
        changes: change_locations,
    };
    segment.header.byte_count = 0;
    segment.header.checksum = empty_checksum();
    let encoded = encode_segment_with_object_locations(&segment)?;
    segment.header.byte_count = encoded.bytes.len() as u64;
    segment.header.checksum = checksum_segment_with_change_checksums(&segment, &change_checksums)?;
    apply_encoded_object_locations_from_encoded(&mut segment, &encoded)?;
    Ok(segment)
}

fn empty_checksum() -> String {
    "0".repeat(64)
}

fn apply_encoded_object_locations_from_encoded(
    segment: &mut Segment,
    encoded: &super::codec::EncodedSegment,
) -> Result<(), LixError> {
    let encoded_commits = encoded
        .commits
        .iter()
        .map(|object| (object.id.as_str(), object))
        .collect::<HashMap<_, _>>();
    for (commit_id, location) in &mut segment.directory.commits {
        let Some(object) = encoded_commits.get(commit_id.as_str()) else {
            return Err(LixError::unknown(format!(
                "changelog segment '{}' could not locate encoded commit '{}'",
                segment.header.segment_id, commit_id
            )));
        };
        location.offset = object.offset;
        location.len = object.len;
    }

    let encoded_changes = encoded
        .changes
        .iter()
        .map(|object| (object.id.as_str(), object))
        .collect::<HashMap<_, _>>();
    for (change_id, location) in &mut segment.directory.changes {
        let Some(object) = encoded_changes.get(change_id.as_str()) else {
            return Err(LixError::unknown(format!(
                "changelog segment '{}' could not locate encoded change '{}'",
                segment.header.segment_id, change_id
            )));
        };
        location.offset = object.offset;
        location.len = object.len;
    }

    Ok(())
}

pub(super) fn validate_segment_shape(segment: &Segment) -> Result<(), LixError> {
    validate_stage_segment_shape(segment)?;

    let encoded = segment_value(segment)?;
    let (encoded_commits, encoded_changes) = view_segment_object_slices(&encoded)?;
    let commits_by_id = segment
        .commits
        .iter()
        .map(|commit| (commit.header.id.as_str(), commit))
        .collect::<HashMap<_, _>>();
    let changes_by_id = segment
        .changes
        .iter()
        .map(|change| (change.id.as_str(), change))
        .collect::<HashMap<_, _>>();
    let encoded_commits_by_id = encoded_commits
        .iter()
        .map(|slice| (slice.id, slice))
        .collect::<HashMap<_, _>>();
    let encoded_changes_by_id = encoded_changes
        .iter()
        .map(|slice| (slice.id, slice))
        .collect::<HashMap<_, _>>();
    let mut change_checksums = Vec::with_capacity(segment.changes.len());
    for change in &segment.changes {
        change_checksums.push((change.id.clone(), checksum_change(change)?));
    }
    let change_checksums_by_id = change_checksums
        .iter()
        .map(|(change_id, checksum)| (change_id.as_str(), checksum.as_str()))
        .collect::<HashMap<_, _>>();

    for (commit_id, location) in &segment.directory.commits {
        let commit = commits_by_id.get(commit_id.as_str()).ok_or_else(|| {
            LixError::unknown(format!(
                "changelog segment '{}' directory points to missing commit '{}'",
                segment.header.segment_id, commit_id
            ))
        })?;
        let Some(slice) = encoded_commits_by_id.get(commit_id.as_str()) else {
            return Err(LixError::unknown(format!(
                "changelog segment '{}' is missing encoded commit '{}'",
                segment.header.segment_id, commit_id
            )));
        };
        validate_encoded_object_location_from_parts(
            &segment.header.segment_id,
            "commit",
            commit_id,
            location,
            slice.offset,
            slice.len,
            slice.encoded_checksum.unwrap_or_default(),
        )?;
        validate_commit_checksum(&location.checksum, commit_id, commit)?;
    }

    for (change_id, location) in &segment.directory.changes {
        let _change = changes_by_id.get(change_id.as_str()).ok_or_else(|| {
            LixError::unknown(format!(
                "changelog segment '{}' directory points to missing change '{}'",
                segment.header.segment_id, change_id
            ))
        })?;
        let Some(slice) = encoded_changes_by_id.get(change_id.as_str()) else {
            return Err(LixError::unknown(format!(
                "changelog segment '{}' is missing encoded change '{}'",
                segment.header.segment_id, change_id
            )));
        };
        validate_encoded_object_location_from_parts(
            &segment.header.segment_id,
            "change",
            change_id,
            location,
            slice.offset,
            slice.len,
            "",
        )?;
        let canonical = change_checksums_by_id
            .get(change_id.as_str())
            .expect("validated change must have canonical checksum");
        if location.checksum != *canonical {
            return Err(LixError::unknown(format!(
                "changelog change '{change_id}' checksum '{}' does not match canonical checksum '{}'",
                location.checksum, canonical
            )));
        }
    }

    let encoded_len = encoded.len() as u64;
    if segment.header.byte_count != encoded_len {
        return Err(LixError::unknown(format!(
            "changelog segment '{}' byte_count {} does not match encoded length {}",
            segment.header.segment_id, segment.header.byte_count, encoded_len
        )));
    }

    let checksum = checksum_segment_with_change_checksums(segment, &change_checksums)?;
    if segment.header.checksum != checksum {
        return Err(LixError::unknown(format!(
            "changelog segment '{}' checksum '{}' does not match canonical checksum '{}'",
            segment.header.segment_id, segment.header.checksum, checksum
        )));
    }

    Ok(())
}

pub(super) fn validate_stage_segment_shape(segment: &Segment) -> Result<(), LixError> {
    if segment.header.format_version != 1 {
        return Err(LixError::unknown(format!(
            "changelog segment '{}' format_version {} is not supported",
            segment.header.segment_id, segment.header.format_version
        )));
    }
    let commit_count = u32::try_from(segment.commits.len()).map_err(|_| {
        LixError::unknown(format!(
            "changelog segment '{}' has too many commits",
            segment.header.segment_id
        ))
    })?;
    if segment.header.commit_count != commit_count {
        return Err(LixError::unknown(format!(
            "changelog segment '{}' commit_count {} does not match {} commits",
            segment.header.segment_id,
            segment.header.commit_count,
            segment.commits.len()
        )));
    }
    let change_count = u32::try_from(segment.changes.len()).map_err(|_| {
        LixError::unknown(format!(
            "changelog segment '{}' has too many changes",
            segment.header.segment_id
        ))
    })?;
    if segment.header.change_count != change_count {
        return Err(LixError::unknown(format!(
            "changelog segment '{}' change_count {} does not match {} changes",
            segment.header.segment_id,
            segment.header.change_count,
            segment.changes.len()
        )));
    }
    let payload_count = segment
        .changes
        .iter()
        .try_fold(0_u32, |count, change| {
            let payload_count = u32::try_from(change.inline_payloads.len()).map_err(|_| ())?;
            count.checked_add(payload_count).ok_or(())
        })
        .map_err(|_| {
            LixError::unknown(format!(
                "changelog segment '{}' inline payload count exceeds u32",
                segment.header.segment_id
            ))
        })?;
    if segment.header.payload_count != payload_count {
        return Err(LixError::unknown(format!(
            "changelog segment '{}' payload_count {} does not match {} inline payloads",
            segment.header.segment_id, segment.header.payload_count, payload_count
        )));
    }
    let mut commit_ids = HashSet::new();
    let directory_commit_ids = segment
        .directory
        .commits
        .iter()
        .map(|(id, _)| id.as_str())
        .collect::<HashSet<_>>();
    for commit in &segment.commits {
        if !commit_ids.insert(commit.header.id.as_str()) {
            return Err(LixError::unknown(format!(
                "changelog segment '{}' contains duplicate commit '{}'",
                segment.header.segment_id, commit.header.id
            )));
        }
        validate_commit_shape(segment, commit, &directory_commit_ids)?;
    }

    let mut change_ids = HashSet::new();
    for change in &segment.changes {
        if !change_ids.insert(change.id.as_str()) {
            return Err(LixError::unknown(format!(
                "changelog segment '{}' contains duplicate change '{}'",
                segment.header.segment_id, change.id
            )));
        }
        validate_change_shape(change)?;
    }

    validate_directory_exact_cover(
        &segment.header.segment_id,
        "commit",
        segment
            .commits
            .iter()
            .map(|commit| commit.header.id.as_str()),
        segment
            .directory
            .commits
            .iter()
            .map(|(id, location)| (id.as_str(), location)),
    )?;
    validate_directory_exact_cover(
        &segment.header.segment_id,
        "change",
        segment.changes.iter().map(|change| change.id.as_str()),
        segment
            .directory
            .changes
            .iter()
            .map(|(id, location)| (id.as_str(), location)),
    )?;
    validate_segment_cross_object_semantics(segment)?;

    Ok(())
}

fn validate_encoded_object_location<'a>(
    segment_id: &str,
    kind: &str,
    object_id: &str,
    location: &SegmentObjectLocation,
    encoded_locations: impl Iterator<Item = (&'a str, u64, u64, &'a str)>,
) -> Result<(), LixError> {
    let Some((_, offset, len, encoded_checksum)) = encoded_locations
        .into_iter()
        .find(|(id, _, _, _)| id == &object_id)
    else {
        return Err(LixError::unknown(format!(
            "changelog segment '{segment_id}' is missing encoded {kind} '{object_id}'"
        )));
    };
    validate_encoded_object_location_from_parts(
        segment_id,
        kind,
        object_id,
        location,
        offset,
        len,
        encoded_checksum,
    )
}

fn validate_encoded_object_location_from_parts(
    _segment_id: &str,
    kind: &str,
    object_id: &str,
    location: &SegmentObjectLocation,
    offset: u64,
    len: u64,
    encoded_checksum: &str,
) -> Result<(), LixError> {
    if location.offset != offset || location.len != len {
        return Err(LixError::unknown(format!(
            "changelog {kind} '{object_id}' locator offset/len does not match encoded byte range"
        )));
    }
    if !encoded_checksum.is_empty() && location.checksum != encoded_checksum {
        return Err(LixError::unknown(format!(
            "changelog {kind} '{object_id}' locator checksum does not match encoded object checksum"
        )));
    }
    Ok(())
}

fn checksum_segment(segment: &Segment) -> Result<String, LixError> {
    let change_checksums = segment
        .changes
        .iter()
        .map(|change| Ok((change.id.clone(), checksum_change(change)?)))
        .collect::<Result<Vec<_>, LixError>>()?;
    checksum_segment_with_change_checksums(segment, &change_checksums)
}

fn checksum_segment_with_change_checksums(
    segment: &Segment,
    change_checksums: &[(String, String)],
) -> Result<String, LixError> {
    let change_checksums = change_checksums
        .iter()
        .map(|(change_id, checksum)| (change_id.as_str(), checksum.as_str()))
        .collect::<HashMap<_, _>>();
    let mut hasher = blake3::Hasher::new();
    hash_part(&mut hasher, "segment");
    hash_field(&mut hasher, "segment_id", &segment.header.segment_id);
    hash_field(
        &mut hasher,
        "format_version",
        &segment.header.format_version.to_string(),
    );
    hash_field(
        &mut hasher,
        "commit_count",
        &segment.header.commit_count.to_string(),
    );
    hash_field(
        &mut hasher,
        "change_count",
        &segment.header.change_count.to_string(),
    );
    hash_field(
        &mut hasher,
        "byte_count",
        &segment.header.byte_count.to_string(),
    );
    hash_field(
        &mut hasher,
        "payload_count",
        &segment.header.payload_count.to_string(),
    );
    hash_list_len(&mut hasher, "commits", segment.commits.len());
    for commit in &segment.commits {
        hash_part(&mut hasher, "commit");
        hash_field(&mut hasher, "id", &commit.header.id);
        hash_field(&mut hasher, "checksum", &commit.checksum);
    }
    hash_list_len(&mut hasher, "changes", segment.changes.len());
    for change in &segment.changes {
        hash_part(&mut hasher, "change");
        hash_field(&mut hasher, "id", &change.id);
        let Some(checksum) = change_checksums.get(change.id.as_str()) else {
            return Err(LixError::unknown(format!(
                "changelog segment '{}' is missing canonical checksum for change '{}'",
                segment.header.segment_id, change.id
            )));
        };
        hash_field(&mut hasher, "checksum", checksum);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

fn checksum_commit(commit: &SegmentCommit) -> Result<String, LixError> {
    let mut hasher = blake3::Hasher::new();
    hash_part(&mut hasher, "commit");
    hash_field(&mut hasher, "id", &commit.header.id);
    hash_list_len(
        &mut hasher,
        "parent_commit_ids",
        commit.header.parent_commit_ids.len(),
    );
    for parent in &commit.header.parent_commit_ids {
        hash_field(&mut hasher, "parent_commit_id", parent);
    }
    hash_field(
        &mut hasher,
        "derivable_change_id",
        &commit.header.derivable_change_id,
    );
    hash_list_len(
        &mut hasher,
        "author_account_ids",
        commit.header.author_account_ids.len(),
    );
    for account_id in &commit.header.author_account_ids {
        hash_field(&mut hasher, "author_account_id", account_id);
    }
    hash_field(&mut hasher, "created_at", &commit.header.created_at);
    hash_field(
        &mut hasher,
        "membership_count",
        &commit.header.membership_count.to_string(),
    );
    hash_list_len(&mut hasher, "memberships", commit.body.membership.len());
    for membership in &commit.body.membership {
        hash_part(&mut hasher, "membership");
        hash_field(
            &mut hasher,
            "member_change_id",
            &membership.member_change_id,
        );
        hash_field(
            &mut hasher,
            "role",
            match membership.role {
                MembershipRole::Authored => "authored",
                MembershipRole::Adopted => "adopted",
            },
        );
        hash_field(
            &mut hasher,
            "source_parent_ordinal",
            &membership
                .source_parent_ordinal
                .map(|ordinal| ordinal.to_string())
                .unwrap_or_default(),
        );
    }
    hash_list_len(
        &mut hasher,
        "state_row_identities",
        commit.directory.state_row_identities.len(),
    );
    for (identity, change_id) in &commit.directory.state_row_identities {
        hash_part(&mut hasher, "state_row_identity");
        hash_field(&mut hasher, "schema_key", identity.schema_key.as_str());
        hash_field(&mut hasher, "file_id", identity.file_id.as_str());
        hash_field(&mut hasher, "entity_id", identity.entity_id.as_str());
        hash_field(&mut hasher, "change_id", change_id);
    }
    hash_list_len(
        &mut hasher,
        "membership_ordinals",
        commit.directory.membership_ordinals.len(),
    );
    for (change_id, ordinal) in &commit.directory.membership_ordinals {
        hash_part(&mut hasher, "membership_ordinal");
        hash_field(&mut hasher, "change_id", change_id);
        hash_field(&mut hasher, "ordinal", &ordinal.to_string());
    }
    Ok(hasher.finalize().to_hex().to_string())
}

fn checksum_change(change: &SegmentChange) -> Result<String, LixError> {
    let mut hasher = blake3::Hasher::new();
    hash_part(&mut hasher, "change");
    hash_field(&mut hasher, "id", &change.id);
    hash_optional_str(
        &mut hasher,
        "authored_commit_id",
        change.authored_commit_id.as_deref(),
    );
    hash_list_len(&mut hasher, "entity_id", change.entity_id.parts.len());
    for part in &change.entity_id.parts {
        hash_field(&mut hasher, "entity_id_part", part);
    }
    hash_field(&mut hasher, "schema_key", &change.schema_key);
    hash_optional_str(&mut hasher, "file_id", change.file_id.as_deref());
    hash_optional_json_ref(&mut hasher, "snapshot_ref", change.snapshot_ref.as_ref());
    hash_optional_json_ref(&mut hasher, "metadata_ref", change.metadata_ref.as_ref());
    hash_field(&mut hasher, "created_at", &change.created_at);
    hash_list_len(&mut hasher, "inline_payloads", change.inline_payloads.len());
    for payload in &change.inline_payloads {
        hash_part(&mut hasher, "inline_payload");
        hash_json_ref(&mut hasher, "json_ref", &payload.json_ref);
        hash_bytes_field(&mut hasher, "bytes", &payload.bytes);
    }
    hash_list_len(
        &mut hasher,
        "directory_payloads",
        change.directory.payloads.len(),
    );
    for payload_location in &change.directory.payloads {
        hash_part(&mut hasher, "payload_location");
        hash_json_ref(&mut hasher, "json_ref", &payload_location.json_ref);
        hash_field(&mut hasher, "offset", &payload_location.offset.to_string());
        hash_field(&mut hasher, "len", &payload_location.len.to_string());
    }
    Ok(hasher.finalize().to_hex().to_string())
}

fn hash_part(hasher: &mut blake3::Hasher, value: &str) {
    hasher.update(&(value.len() as u64).to_le_bytes());
    hasher.update(value.as_bytes());
}

fn hash_field(hasher: &mut blake3::Hasher, field: &str, value: &str) {
    hash_part(hasher, field);
    hash_part(hasher, value);
}

fn hash_list_len(hasher: &mut blake3::Hasher, field: &str, len: usize) {
    hash_part(hasher, field);
    hasher.update(&(len as u64).to_le_bytes());
}

fn hash_bytes_part(hasher: &mut blake3::Hasher, value: &[u8]) {
    hasher.update(&(value.len() as u64).to_le_bytes());
    hasher.update(value);
}

fn hash_bytes_field(hasher: &mut blake3::Hasher, field: &str, value: &[u8]) {
    hash_part(hasher, field);
    hash_bytes_part(hasher, value);
}

fn hash_optional_str(hasher: &mut blake3::Hasher, field: &str, value: Option<&str>) {
    hash_part(hasher, field);
    match value {
        Some(value) => {
            hash_part(hasher, "some");
            hash_part(hasher, value);
        }
        None => hash_part(hasher, "none"),
    }
}

fn hash_optional_json_ref(
    hasher: &mut blake3::Hasher,
    field: &str,
    value: Option<&crate::json_store::JsonRef>,
) {
    hash_part(hasher, field);
    match value {
        Some(value) => {
            hash_part(hasher, "some");
            hash_json_ref(hasher, "json_ref", value);
        }
        None => hash_part(hasher, "none"),
    }
}

fn hash_json_ref(hasher: &mut blake3::Hasher, field: &str, value: &crate::json_store::JsonRef) {
    hash_bytes_field(hasher, field, value.as_hash_bytes());
}

fn validate_commit_shape(
    segment: &Segment,
    commit: &SegmentCommit,
    directory_commit_ids: &HashSet<&str>,
) -> Result<(), LixError> {
    let membership_count = u32::try_from(commit.body.membership.len()).map_err(|_| {
        LixError::unknown(format!(
            "changelog commit '{}' has too many membership records",
            commit.header.id
        ))
    })?;
    if commit.header.membership_count != membership_count {
        return Err(LixError::unknown(format!(
            "changelog commit '{}' membership_count {} does not match {} membership records",
            commit.header.id,
            commit.header.membership_count,
            commit.body.membership.len()
        )));
    }
    let membership_ordinals_by_id = commit
        .directory
        .membership_ordinals
        .iter()
        .map(|(change_id, ordinal)| (change_id.as_str(), *ordinal))
        .collect::<HashMap<_, _>>();

    let mut member_ids = HashSet::new();
    for (ordinal, membership) in commit.body.membership.iter().enumerate() {
        match membership.role {
            MembershipRole::Authored => {
                if membership.source_parent_ordinal.is_some() {
                    return Err(LixError::unknown(format!(
                        "changelog commit '{}' authored membership change '{}' must not record a source_parent_ordinal",
                        commit.header.id, membership.member_change_id
                    )));
                }
            }
            MembershipRole::Adopted => {
                let Some(source_parent_ordinal) = membership.source_parent_ordinal else {
                    return Err(LixError::unknown(format!(
                        "changelog commit '{}' adopted membership change '{}' is missing source_parent_ordinal",
                        commit.header.id, membership.member_change_id
                    )));
                };
                if source_parent_ordinal as usize >= commit.header.parent_commit_ids.len() {
                    return Err(LixError::unknown(format!(
                        "changelog commit '{}' adopted membership change '{}' source_parent_ordinal {} is out of bounds for {} parents",
                        commit.header.id,
                        membership.member_change_id,
                        source_parent_ordinal,
                        commit.header.parent_commit_ids.len()
                    )));
                }
            }
        }
        if !member_ids.insert(membership.member_change_id.as_str()) {
            return Err(LixError::unknown(format!(
                "changelog commit '{}' contains duplicate membership change '{}'",
                commit.header.id, membership.member_change_id
            )));
        }
        let Some(directory_ordinal) =
            membership_ordinals_by_id.get(membership.member_change_id.as_str())
        else {
            return Err(LixError::unknown(format!(
                "changelog commit '{}' is missing membership ordinal for change '{}'",
                commit.header.id, membership.member_change_id
            )));
        };
        if *directory_ordinal as usize != ordinal {
            return Err(LixError::unknown(format!(
                "changelog commit '{}' membership ordinal for change '{}' is {}, expected {}",
                commit.header.id, membership.member_change_id, directory_ordinal, ordinal
            )));
        }
    }

    let mut ordinal_ids = HashSet::new();
    for (change_id, ordinal) in &commit.directory.membership_ordinals {
        if !ordinal_ids.insert(change_id.as_str()) {
            return Err(LixError::unknown(format!(
                "changelog commit '{}' contains duplicate membership ordinal for change '{}'",
                commit.header.id, change_id
            )));
        }
        if *ordinal as usize >= commit.body.membership.len() {
            return Err(LixError::unknown(format!(
                "changelog commit '{}' membership ordinal {} for change '{}' is out of bounds",
                commit.header.id, ordinal, change_id
            )));
        }
    }

    let mut identities = HashSet::new();
    let mut directory_change_ids = HashSet::new();
    for (identity, change_id) in &commit.directory.state_row_identities {
        if !identities.insert(identity) {
            return Err(LixError::unknown(format!(
                "changelog commit '{}' contains duplicate StateRowIdentity winner",
                commit.header.id
            )));
        }
        if !directory_change_ids.insert(change_id.as_str()) {
            return Err(LixError::unknown(format!(
                "changelog commit '{}' contains duplicate StateRowIdentity winner for change '{}'",
                commit.header.id, change_id
            )));
        }
        if !member_ids.contains(change_id.as_str()) {
            return Err(LixError::unknown(format!(
                "changelog commit '{}' StateRowIdentity winner references non-member change '{}'",
                commit.header.id, change_id
            )));
        }
    }
    for membership in &commit.body.membership {
        if !directory_change_ids.contains(membership.member_change_id.as_str()) {
            return Err(LixError::unknown(format!(
                "changelog commit '{}' membership change '{}' is missing from SegmentCommitDirectory",
                commit.header.id, membership.member_change_id
            )));
        }
    }

    if commit.checksum.is_empty() {
        return Err(LixError::unknown(format!(
            "changelog commit '{}' has empty checksum",
            commit.header.id
        )));
    }

    if !directory_commit_ids.contains(commit.header.id.as_str()) {
        return Err(LixError::unknown(format!(
            "changelog segment '{}' is missing directory location for commit '{}'",
            segment.header.segment_id, commit.header.id
        )));
    }

    Ok(())
}

fn validate_change_shape(change: &SegmentChange) -> Result<(), LixError> {
    let directory_payloads_by_ref = change
        .directory
        .payloads
        .iter()
        .map(|location| (location.json_ref.as_hash_bytes(), location))
        .collect::<HashMap<_, _>>();
    let mut inline_payload_refs = HashSet::with_capacity(change.inline_payloads.len());
    for (ordinal, payload) in change.inline_payloads.iter().enumerate() {
        if !inline_payload_refs.insert(payload.json_ref.as_hash_bytes()) {
            return Err(LixError::unknown(format!(
                "changelog change '{}' contains duplicate inline payload ref",
                change.id
            )));
        }
        let actual = crate::json_store::JsonRef::for_content(&payload.bytes);
        if payload.json_ref != actual {
            return Err(LixError::unknown(format!(
                "changelog change '{}' inline payload ref '{}' does not match payload bytes '{}'",
                change.id,
                payload.json_ref.to_hex(),
                actual.to_hex()
            )));
        }
        let Some(location) = directory_payloads_by_ref.get(&payload.json_ref.as_hash_bytes())
        else {
            return Err(LixError::unknown(format!(
                "changelog change '{}' is missing payload directory entry",
                change.id
            )));
        };
        if location.offset != ordinal as u64 || location.len != payload.bytes.len() as u64 {
            return Err(LixError::unknown(format!(
                "changelog change '{}' payload directory entry does not match inline payload",
                change.id
            )));
        }
    }

    let mut directory_payload_refs = HashSet::with_capacity(change.directory.payloads.len());
    for location in &change.directory.payloads {
        if !directory_payload_refs.insert(location.json_ref.as_hash_bytes()) {
            return Err(LixError::unknown(format!(
                "changelog change '{}' contains duplicate payload directory entry",
                change.id
            )));
        }
        if !inline_payload_refs.contains(&location.json_ref.as_hash_bytes()) {
            return Err(LixError::unknown(format!(
                "changelog change '{}' payload directory references unknown inline payload",
                change.id
            )));
        }
    }

    Ok(())
}

fn validate_directory_exact_cover<'a>(
    segment_id: &str,
    kind: &str,
    object_ids: impl Iterator<Item = &'a str>,
    directory_ids: impl Iterator<Item = (&'a str, &'a SegmentObjectLocation)>,
) -> Result<(), LixError> {
    let objects: HashSet<&str> = object_ids.collect();
    let mut directory = HashSet::new();
    for (id, location) in directory_ids {
        if location.segment_id != segment_id {
            return Err(LixError::unknown(format!(
                "changelog segment '{segment_id}' {kind} directory location for '{id}' points to segment '{}'",
                location.segment_id
            )));
        }
        if !directory.insert(id) {
            return Err(LixError::unknown(format!(
                "changelog segment '{segment_id}' contains duplicate {kind} directory entry '{id}'"
            )));
        }
    }
    for id in objects.difference(&directory) {
        return Err(LixError::unknown(format!(
            "changelog segment '{segment_id}' is missing {kind} directory entry '{id}'"
        )));
    }
    for id in directory.difference(&objects) {
        return Err(LixError::unknown(format!(
            "changelog segment '{segment_id}' {kind} directory references unknown object '{id}'"
        )));
    }
    Ok(())
}

fn validate_segment_cross_object_semantics(segment: &Segment) -> Result<(), LixError> {
    let mut changes_by_id = HashMap::with_capacity(segment.changes.len());
    for change in &segment.changes {
        changes_by_id.insert(change.id.as_str(), change);
    }

    for commit in &segment.commits {
        let memberships_by_id = commit
            .body
            .membership
            .iter()
            .map(|membership| (membership.member_change_id.as_str(), membership.role))
            .collect::<HashMap<_, _>>();

        for membership in &commit.body.membership {
            match membership.role {
                MembershipRole::Authored => {
                    let Some(change) = changes_by_id.get(membership.member_change_id.as_str())
                    else {
                        return Err(LixError::unknown(format!(
                            "changelog commit '{}' authored membership references missing change '{}'",
                            commit.header.id, membership.member_change_id
                        )));
                    };
                    if change.authored_commit_id.as_deref() != Some(commit.header.id.as_str()) {
                        return Err(LixError::unknown(format!(
                            "changelog commit '{}' authored membership change '{}' has mismatched authored_commit_id",
                            commit.header.id, membership.member_change_id
                        )));
                    }
                }
                MembershipRole::Adopted => {
                    if let Some(change) = changes_by_id.get(membership.member_change_id.as_str()) {
                        if change.authored_commit_id.as_deref() == Some(commit.header.id.as_str()) {
                            return Err(LixError::unknown(format!(
                                "changelog commit '{}' adopted membership change '{}' must not be authored by the same commit",
                                commit.header.id, membership.member_change_id
                            )));
                        }
                    }
                }
            }
        }

        for (identity, change_id) in &commit.directory.state_row_identities {
            let Some(change) = changes_by_id.get(change_id.as_str()) else {
                if memberships_by_id.get(change_id.as_str()) == Some(&MembershipRole::Adopted) {
                    continue;
                }
                return Err(LixError::unknown(format!(
                    "changelog commit '{}' StateRowIdentity winner references missing authored change '{}'",
                    commit.header.id, change_id
                )));
            };
            let actual = state_row_identity_for_change(change)?;
            if &actual != identity {
                return Err(LixError::unknown(format!(
                    "changelog commit '{}' StateRowIdentity winner for change '{}' does not match changelog.change",
                    commit.header.id, change_id
                )));
            }
        }
    }

    Ok(())
}

fn state_row_identity_for_change(change: &SegmentChange) -> Result<StateRowIdentity, LixError> {
    Ok(StateRowIdentity {
        schema_key: CanonicalSchemaKey::new(change.schema_key.clone())?,
        file_id: FileId::new(
            change
                .file_id
                .clone()
                .unwrap_or_else(|| "__global__".to_string()),
        )?,
        entity_id: EntityId::new(change.entity_id.as_json_array_text()?)?,
    })
}

pub(super) fn validate_commit_location(
    location: &SegmentObjectLocation,
    segment: &Segment,
    commit_id: &str,
) -> Result<(), LixError> {
    let expected = directory_commit_location_ref(segment, commit_id)?;
    if location != expected {
        return Err(LixError::unknown(format!(
            "changelog commit '{commit_id}' locator does not match segment directory"
        )));
    }
    if !segment
        .commits
        .iter()
        .any(|commit| commit.header.id == commit_id)
    {
        return Err(LixError::unknown(format!(
            "changelog segment '{}' is missing commit '{}'",
            segment.header.segment_id, commit_id
        )));
    }
    Ok(())
}

pub(super) fn validate_commit_checksum(
    checksum: &str,
    commit_id: &str,
    commit: &SegmentCommit,
) -> Result<(), LixError> {
    let canonical = checksum_commit(commit)?;
    if commit.checksum != canonical {
        return Err(LixError::unknown(format!(
            "changelog commit '{commit_id}' checksum '{}' does not match canonical checksum '{}'",
            commit.checksum, canonical
        )));
    }
    if checksum != canonical {
        return Err(LixError::unknown(format!(
            "changelog commit '{commit_id}' checksum '{checksum}' does not match canonical checksum '{canonical}'"
        )));
    }
    Ok(())
}

pub(super) fn validate_change_location(
    location: &SegmentObjectLocation,
    segment: &Segment,
    change_id: &str,
) -> Result<(), LixError> {
    let expected = directory_change_location_ref(segment, change_id)?;
    if location != expected {
        return Err(LixError::unknown(format!(
            "changelog change '{change_id}' locator does not match segment directory"
        )));
    }
    if !segment.changes.iter().any(|change| change.id == change_id) {
        return Err(LixError::unknown(format!(
            "changelog segment '{}' is missing change '{}'",
            segment.header.segment_id, change_id
        )));
    }
    Ok(())
}

pub(super) fn validate_change_checksum(
    checksum: &str,
    change_id: &str,
    change: &SegmentChange,
) -> Result<(), LixError> {
    let canonical = checksum_change(change)?;
    if checksum != canonical {
        return Err(LixError::unknown(format!(
            "changelog change '{change_id}' checksum '{checksum}' does not match canonical checksum '{canonical}'"
        )));
    }
    Ok(())
}

pub(super) fn segment_commit<'a>(
    segment: &'a Segment,
    commit_id: &str,
) -> Option<&'a SegmentCommit> {
    segment
        .commits
        .iter()
        .find(|commit| commit.header.id == commit_id)
}

pub(super) fn segment_change<'a>(
    segment: &'a Segment,
    change_id: &str,
) -> Option<&'a SegmentChange> {
    segment.changes.iter().find(|change| change.id == change_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changelog::{
        CommitBody, CommitHeader, MembershipRecord, SegmentChangeDirectory, SegmentCommitDirectory,
        SegmentHeader, SegmentInlinePayload,
    };
    use crate::common::{CanonicalSchemaKey, EntityId, FileId};
    use crate::entity_identity::EntityIdentity;
    use crate::json_store::JsonRef;

    #[test]
    fn validation_rejects_duplicate_commit_ids() {
        let mut segment = test_segment();
        segment.commits.push(segment.commits[0].clone());
        segment.header.commit_count = segment.commits.len() as u32;

        let error = validate_segment_shape(&segment).expect_err("duplicate commit id must fail");

        assert!(
            error
                .message
                .contains("contains duplicate commit 'commit-1'"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn validation_rejects_duplicate_change_ids() {
        let mut segment = test_segment();
        segment.changes.push(segment.changes[0].clone());
        segment.header.change_count = segment.changes.len() as u32;

        let error = validate_segment_shape(&segment).expect_err("duplicate change id must fail");

        assert!(
            error
                .message
                .contains("contains duplicate change 'change-1'"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn validation_rejects_membership_count_drift() {
        let mut segment = test_segment();
        segment.commits[0].header.membership_count = 0;

        let error = validate_segment_shape(&segment).expect_err("membership_count drift must fail");

        assert!(
            error
                .message
                .contains("membership_count 0 does not match 1"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn validation_rejects_membership_directory_drift() {
        let mut segment = test_segment();
        segment.commits[0].directory.membership_ordinals.clear();

        let error =
            validate_segment_shape(&segment).expect_err("membership directory drift must fail");

        assert!(
            error
                .message
                .contains("is missing membership ordinal for change 'change-1'"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn validation_rejects_authored_membership_with_source_parent_ordinal() {
        let mut segment = test_segment();
        segment.commits[0].body.membership[0].source_parent_ordinal = Some(0);

        let error = validate_segment_shape(&segment)
            .expect_err("authored membership must not carry source parent provenance");

        assert!(
            error
                .message
                .contains("authored membership change 'change-1' must not record"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn validation_rejects_adopted_membership_without_source_parent_ordinal() {
        let mut segment = test_segment();
        segment.commits[0].body.membership[0].role = MembershipRole::Adopted;

        let error = validate_segment_shape(&segment)
            .expect_err("adopted membership must carry source parent provenance");

        assert!(
            error
                .message
                .contains("adopted membership change 'change-1' is missing source_parent_ordinal"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn validation_rejects_adopted_membership_source_parent_ordinal_out_of_bounds() {
        let mut segment = test_segment();
        segment.commits[0].body.membership[0].role = MembershipRole::Adopted;
        segment.commits[0].body.membership[0].source_parent_ordinal = Some(1);
        segment.commits[0]
            .header
            .parent_commit_ids
            .push("parent-1".to_string());

        let error = validate_segment_shape(&segment)
            .expect_err("adopted membership source parent ordinal must point at a parent");

        assert!(
            error
                .message
                .contains("source_parent_ordinal 1 is out of bounds for 1 parents"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn validation_rejects_payload_directory_drift() {
        let payload_ref = JsonRef::for_content(b"payload");
        let mut segment = test_segment_with_inline_payload(payload_ref);
        segment.changes[0].directory.payloads[0].len = 999;

        let error =
            validate_segment_shape(&segment).expect_err("payload directory drift must fail");

        assert!(
            error
                .message
                .contains("payload directory entry does not match inline payload"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn validation_rejects_inline_payload_ref_mismatch() {
        let payload_ref = JsonRef::for_content(b"payload");
        let mut segment = test_segment_with_inline_payload(payload_ref);
        let bogus_ref = JsonRef::from_hash_bytes([9; 32]);
        segment.changes[0].inline_payloads[0].json_ref = bogus_ref;
        segment.changes[0].directory.payloads[0].json_ref = bogus_ref;

        let error =
            validate_segment_shape(&segment).expect_err("inline payload hash drift must fail");

        assert!(
            error.message.contains("does not match payload bytes"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn validation_rejects_authored_membership_commit_mismatch() {
        let mut segment = test_segment();
        segment.changes[0].authored_commit_id = Some("other-commit".to_string());

        let error = validate_segment_shape(&segment)
            .expect_err("authored membership ownership drift must fail");

        assert!(
            error.message.contains("mismatched authored_commit_id"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn validation_rejects_adopted_membership_authored_by_same_commit() {
        let mut segment = test_segment();
        segment.commits[0]
            .header
            .parent_commit_ids
            .push("parent-1".to_string());
        segment.commits[0].body.membership[0].role = MembershipRole::Adopted;
        segment.commits[0].body.membership[0].source_parent_ordinal = Some(0);

        let error = validate_segment_shape(&segment)
            .expect_err("self-authored adopted membership must fail");

        assert!(
            error
                .message
                .contains("must not be authored by the same commit"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn validation_rejects_state_row_identity_change_mismatch() {
        let mut segment = test_segment();
        segment.commits[0].directory.state_row_identities[0]
            .0
            .schema_key = CanonicalSchemaKey::new("other").unwrap();

        let error =
            validate_segment_shape(&segment).expect_err("state-row identity drift must fail");

        assert!(
            error
                .message
                .contains("StateRowIdentity winner for change 'change-1' does not match"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn canonicalization_writes_encoded_byte_ranges() {
        let segment = test_segment();
        let commit_location = directory_commit_location(&segment, "commit-1").unwrap();
        let change_location = directory_change_location(&segment, "change-1").unwrap();

        assert!(
            commit_location.offset > 0 && commit_location.len > 0,
            "commit location should be a real encoded byte range"
        );
        assert!(
            change_location.offset > commit_location.offset && change_location.len > 0,
            "change location should be a real encoded byte range after commits"
        );

        let encoded = segment_value(&segment).unwrap();
        let (commit_slices, change_slices) = view_segment_object_slices(&encoded).unwrap();
        assert_eq!(commit_location.offset, commit_slices[0].offset);
        assert_eq!(commit_location.len, commit_slices[0].len);
        assert_eq!(change_location.offset, change_slices[0].offset);
        assert_eq!(change_location.len, change_slices[0].len);
    }

    fn test_segment() -> Segment {
        canonicalize_segment(Segment {
            header: SegmentHeader {
                segment_id: "segment-1".to_string(),
                format_version: 1,
                commit_count: 1,
                change_count: 1,
                byte_count: 0,
                payload_count: 0,
                checksum: String::new(),
            },
            directory: SegmentDirectory::default(),
            commits: vec![SegmentCommit {
                header: CommitHeader {
                    id: "commit-1".to_string(),
                    parent_commit_ids: Vec::new(),
                    derivable_change_id: "commit-1-derivable".to_string(),
                    author_account_ids: vec!["account-1".to_string()],
                    created_at: "2026-05-12T00:00:00Z".to_string(),
                    membership_count: 1,
                },
                body: CommitBody {
                    membership: vec![MembershipRecord {
                        member_change_id: "change-1".to_string(),
                        role: MembershipRole::Authored,
                        source_parent_ordinal: None,
                    }],
                },
                directory: SegmentCommitDirectory {
                    state_row_identities: vec![(state_row_identity(), "change-1".to_string())],
                    membership_ordinals: vec![("change-1".to_string(), 0)],
                },
                checksum: String::new(),
            }],
            changes: vec![change("change-1", Vec::new())],
        })
        .unwrap()
    }

    fn test_segment_with_inline_payload(payload_ref: JsonRef) -> Segment {
        canonicalize_segment(Segment {
            changes: vec![change(
                "change-1",
                vec![SegmentInlinePayload {
                    json_ref: payload_ref,
                    bytes: b"payload".to_vec(),
                }],
            )],
            ..test_segment()
        })
        .unwrap()
    }

    fn change(id: &str, inline_payloads: Vec<SegmentInlinePayload>) -> SegmentChange {
        SegmentChange {
            id: id.to_string(),
            authored_commit_id: Some("commit-1".to_string()),
            entity_id: EntityIdentity::single("entity-1"),
            schema_key: "message".to_string(),
            file_id: Some("file-1".to_string()),
            snapshot_ref: None,
            metadata_ref: None,
            created_at: "2026-05-12T00:00:00Z".to_string(),
            inline_payloads,
            directory: SegmentChangeDirectory::default(),
        }
    }

    fn state_row_identity() -> super::super::types::StateRowIdentity {
        super::super::types::StateRowIdentity {
            schema_key: CanonicalSchemaKey::new("message").unwrap(),
            file_id: FileId::new("file-1").unwrap(),
            entity_id: EntityId::new(
                EntityIdentity::single("entity-1")
                    .as_json_array_text()
                    .unwrap(),
            )
            .unwrap(),
        }
    }
}
