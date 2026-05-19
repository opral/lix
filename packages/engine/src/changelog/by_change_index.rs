//! Rebuildable by_change index behavior.

use super::segment::directory_change_location;
use super::types::{ByChangeEntry, Segment};
use crate::LixError;
use std::collections::HashSet;

pub(super) fn by_change_entries_for_segments(
    segments: &[Segment],
) -> Result<Vec<ByChangeEntry>, LixError> {
    let mut entries = Vec::new();
    let mut seen = HashSet::new();
    for segment in segments {
        for change in &segment.changes {
            if !seen.insert(change.id.as_str()) {
                return Err(LixError::unknown(format!(
                    "changelog index rebuild found duplicate change '{}'",
                    change.id
                )));
            }
            entries.push(ByChangeEntry {
                change_id: change.id.clone(),
                location: directory_change_location(segment, &change.id)?,
            });
        }
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changelog::{
        SegmentChange, SegmentChangeDirectory, SegmentDirectory, SegmentHeader,
        SegmentObjectLocation,
    };
    use crate::entity_identity::EntityIdentity;

    #[test]
    fn entries_match_segment_change_directories() {
        let segment = Segment {
            header: SegmentHeader {
                segment_id: "segment-1".to_string(),
                format_version: 1,
                commit_count: 0,
                change_count: 2,
                byte_count: 0,
                payload_count: 0,
                checksum: String::new(),
            },
            directory: SegmentDirectory {
                commits: Vec::new(),
                changes: vec![
                    ("change-1".to_string(), location("segment-1", 0, "c1")),
                    ("change-2".to_string(), location("segment-1", 1, "c2")),
                ],
            },
            commits: Vec::new(),
            changes: vec![change("change-1"), change("change-2")],
        };

        let entries = by_change_entries_for_segments(&[segment]).unwrap();

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].change_id, "change-1");
        assert_eq!(entries[0].location, location("segment-1", 0, "c1"));
        assert_eq!(entries[1].change_id, "change-2");
        assert_eq!(entries[1].location, location("segment-1", 1, "c2"));
    }

    fn change(id: &str) -> SegmentChange {
        SegmentChange {
            id: id.to_string(),
            authored_commit_id: None,
            entity_id: EntityIdentity::single(id),
            schema_key: "message".to_string(),
            file_id: Some("file-1".to_string()),
            snapshot_ref: None,
            metadata_ref: None,
            created_at: "2026-05-12T00:00:00Z".to_string(),
            inline_payloads: Vec::new(),
            directory: SegmentChangeDirectory::default(),
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
