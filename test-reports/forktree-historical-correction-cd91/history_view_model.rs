//! Dependency-free model for the direct historical-correction successor.
//!
//! This file is intentionally not registered with Cargo. A future owner may
//! compile it as a standalone test after the production successor is
//! compile-green; this package does not run it.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ViewId(pub u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HistoryQuerySource {
    pub retained_view: ViewId,
    pub forktree_reader: ViewId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Surface {
    SqlCheckpoint,
    CheckpointCreation,
    FilesystemCheckpoint,
    WorkingDiff,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Failure {
    ViewMismatch,
    MalformedRow,
    MissingRow,
    WrongKind,
    DuplicateBlobRef,
    BlobIdentityMismatch,
    MissingPayload,
    PayloadDigestMismatch,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BlobKind {
    Canonical,
    Wrong,
    Malformed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Cell<'a> {
    Value(&'a str),
    Null,
    Tombstone,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlobRef<'a> {
    pub file_id: &'a str,
    pub blob_id: &'a str,
    pub kind: BlobKind,
    pub payload: Option<&'a [u8]>,
    pub digest: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileRow<'a> {
    pub file_id: &'a str,
    pub cell: Cell<'a>,
    pub malformed: bool,
    pub refs: &'a [BlobRef<'a>],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResultRow {
    Value,
    Deletion,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Counters {
    pub retained_reads: u8,
    pub plans: u8,
    pub prepared_writes: u8,
    pub commits: u8,
    pub selector_rotations: u8,
    pub legacy_fallbacks: u8,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadResult {
    pub surface: Surface,
    pub row: Option<ResultRow>,
    pub counters: Counters,
}

impl HistoryQuerySource {
    pub const fn new(view: ViewId) -> Self {
        Self {
            retained_view: view,
            forktree_reader: view,
        }
    }
}

pub fn read_surface(
    source: HistoryQuerySource,
    caller_view: ViewId,
    surface: Surface,
    row: FileRow<'_>,
) -> Result<ReadResult, Failure> {
    if source.retained_view != caller_view || source.forktree_reader != caller_view {
        return Err(Failure::ViewMismatch);
    }
    let row = authenticate_file_row(row)?;
    Ok(ReadResult {
        surface,
        row,
        counters: Counters {
            retained_reads: 1,
            ..Counters::default()
        },
    })
}

fn authenticate_file_row(row: FileRow<'_>) -> Result<Option<ResultRow>, Failure> {
    if row.malformed {
        return Err(Failure::MalformedRow);
    }
    match row.cell {
        Cell::Tombstone => return Ok(Some(ResultRow::Deletion)),
        Cell::Null => return Err(Failure::MissingRow),
        Cell::Value(_) => {}
    }
    let [blob] = row.refs else {
        return Err(if row.refs.is_empty() {
            Failure::MissingRow
        } else {
            Failure::DuplicateBlobRef
        });
    };
    if blob.file_id != row.file_id {
        return Err(Failure::BlobIdentityMismatch);
    }
    match blob.kind {
        BlobKind::Canonical => {}
        BlobKind::Wrong => return Err(Failure::WrongKind),
        BlobKind::Malformed => return Err(Failure::MalformedRow),
    }
    if blob.blob_id.is_empty() {
        return Err(Failure::WrongKind);
    }
    let Some(payload) = blob.payload else {
        return Err(Failure::MissingPayload);
    };
    if checksum(payload) != blob.digest {
        return Err(Failure::PayloadDigestMismatch);
    }
    Ok(Some(ResultRow::Value))
}

pub const fn checksum(bytes: &[u8]) -> u64 {
    let mut value = 0xcbf2_9ce4_8422_2325_u64;
    let mut index = 0;
    while index < bytes.len() {
        value = value
            .wrapping_mul(0x1000_0000_01b3)
            .wrapping_add(bytes[index] as u64);
        index += 1;
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    const VIEW: ViewId = ViewId(0x735f_7669_6577_5f31);
    const BYTES: &[u8] = b"authenticated payload";

    fn valid_row() -> FileRow<'static> {
        static REFS: [BlobRef<'static>; 1] = [BlobRef {
            file_id: "file-a",
            blob_id: "blob-a",
            kind: BlobKind::Canonical,
            payload: Some(BYTES),
            digest: checksum(BYTES),
        }];
        FileRow {
            file_id: "file-a",
            cell: Cell::Value("descriptor"),
            malformed: false,
            refs: &REFS,
        }
    }

    #[test]
    fn all_four_surfaces_use_one_caller_view_and_do_not_publish() {
        for surface in [
            Surface::SqlCheckpoint,
            Surface::CheckpointCreation,
            Surface::FilesystemCheckpoint,
            Surface::WorkingDiff,
        ] {
            let result = read_surface(HistoryQuerySource::new(VIEW), VIEW, surface, valid_row())
                .expect("valid row");
            assert_eq!(result.row, Some(ResultRow::Value));
            assert_eq!(result.counters.retained_reads, 1);
            assert_eq!(result.counters.plans, 0);
            assert_eq!(result.counters.prepared_writes, 0);
            assert_eq!(result.counters.commits, 0);
            assert_eq!(result.counters.selector_rotations, 0);
            assert_eq!(result.counters.legacy_fallbacks, 0);
        }
    }

    #[test]
    fn tombstone_is_a_deletion_event_without_blob_materialization() {
        let row = FileRow {
            file_id: "file-a",
            cell: Cell::Tombstone,
            malformed: false,
            refs: &[],
        };
        let result = read_surface(
            HistoryQuerySource::new(VIEW),
            VIEW,
            Surface::FilesystemCheckpoint,
            row,
        )
        .expect("tombstone");
        assert_eq!(result.row, Some(ResultRow::Deletion));
    }

    #[test]
    fn missing_duplicate_substituted_and_unavailable_blobs_fail_closed() {
        let valid = valid_row();
        assert_eq!(
            read_surface(
                HistoryQuerySource::new(VIEW),
                VIEW,
                Surface::WorkingDiff,
                FileRow { refs: &[], ..valid },
            ),
            Err(Failure::MissingRow)
        );
        let duplicate = [valid.refs[0], valid.refs[0]];
        assert_eq!(
            read_surface(
                HistoryQuerySource::new(VIEW),
                VIEW,
                Surface::WorkingDiff,
                FileRow {
                    refs: &duplicate,
                    ..valid
                },
            ),
            Err(Failure::DuplicateBlobRef)
        );
        let bad_identity = [BlobRef {
            file_id: "other-file",
            ..valid.refs[0]
        }];
        assert_eq!(
            read_surface(
                HistoryQuerySource::new(VIEW),
                VIEW,
                Surface::WorkingDiff,
                FileRow {
                    refs: &bad_identity,
                    ..valid
                },
            ),
            Err(Failure::BlobIdentityMismatch)
        );
        let unavailable = [BlobRef {
            payload: None,
            ..valid.refs[0]
        }];
        assert_eq!(
            read_surface(
                HistoryQuerySource::new(VIEW),
                VIEW,
                Surface::WorkingDiff,
                FileRow {
                    refs: &unavailable,
                    ..valid
                },
            ),
            Err(Failure::MissingPayload)
        );
        assert_eq!(
            read_surface(
                HistoryQuerySource::new(VIEW),
                VIEW,
                Surface::WorkingDiff,
                FileRow {
                    malformed: true,
                    ..valid
                },
            ),
            Err(Failure::MalformedRow)
        );
        let wrong_kind = [BlobRef {
            kind: BlobKind::Wrong,
            ..valid.refs[0]
        }];
        assert_eq!(
            read_surface(
                HistoryQuerySource::new(VIEW),
                VIEW,
                Surface::WorkingDiff,
                FileRow {
                    refs: &wrong_kind,
                    ..valid
                },
            ),
            Err(Failure::WrongKind)
        );
        let malformed_blob = [BlobRef {
            kind: BlobKind::Malformed,
            ..valid.refs[0]
        }];
        assert_eq!(
            read_surface(
                HistoryQuerySource::new(VIEW),
                VIEW,
                Surface::WorkingDiff,
                FileRow {
                    refs: &malformed_blob,
                    ..valid
                },
            ),
            Err(Failure::MalformedRow)
        );
    }

    #[test]
    fn a_view_mismatch_cannot_refresh_or_fallback() {
        assert_eq!(
            read_surface(
                HistoryQuerySource::new(VIEW),
                ViewId(VIEW.0 + 1),
                Surface::SqlCheckpoint,
                valid_row(),
            ),
            Err(Failure::ViewMismatch)
        );
    }
}
