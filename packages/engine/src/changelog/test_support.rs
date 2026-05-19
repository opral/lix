use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::context::ChangelogStorageRead;
use super::segment::{canonicalize_segment, directory_change_location, directory_commit_location};
use super::store::{
    BY_CHANGE_INDEX_SPACE, BY_CHANGE_MEMBERSHIP_INDEX_SPACE, BY_COMMIT_INDEX_SPACE, SEGMENT_SPACE,
    by_change_index_value, by_change_key, by_change_membership_index_value,
    by_change_membership_key, by_commit_index_value, by_commit_key, segment_key, segment_value,
};
use super::{
    ByChangeEntry, ByCommitEntry, CommitBody, CommitHeader, CommitVisibility, MembershipRecord,
    MembershipRole, Segment, SegmentChange, SegmentChangeDirectory, SegmentCommit,
    SegmentCommitDirectory, SegmentDirectory, SegmentHeader, SegmentObjectLocation,
    StateRowIdentity, decode_by_change_entry, decode_by_commit_entry,
};
use crate::LixError;
use crate::changelog::ChangelogContext;
use crate::common::{CanonicalSchemaKey, EntityId, FileId};
use crate::entity_identity::EntityIdentity;
use crate::storage::{
    InMemoryStorageBackend, PointReadPlan, StorageContext, StorageGetOptions, StorageKey,
    StorageProjectedValue, StorageReadOptions, StorageSpace, StorageWriteSet,
};

pub(crate) fn changelog_test_context() -> (ChangelogContext, StorageContext) {
    (
        ChangelogContext::new(),
        StorageContext::new(InMemoryStorageBackend::new()),
    )
}

pub(crate) fn test_segment() -> Segment {
    let identity = state_row_identity("message", "file-1", "entity-1");
    canonicalize_segment(Segment {
        header: SegmentHeader {
            segment_id: "segment-1".to_string(),
            format_version: 1,
            commit_count: 1,
            change_count: 1,
            byte_count: 123,
            payload_count: 0,
            checksum: "segment-checksum".to_string(),
        },
        directory: SegmentDirectory {
            commits: vec![(
                "commit-1".to_string(),
                location("segment-1", 10, 20, "commit-checksum"),
            )],
            changes: vec![("change-1".to_string(), location("segment-1", 30, 40, "ch"))],
        },
        commits: vec![SegmentCommit {
            header: CommitHeader {
                id: "commit-1".to_string(),
                parent_commit_ids: Vec::new(),
                derivable_change_id: "derived-change-1".to_string(),
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
                state_row_identities: vec![(identity, "change-1".to_string())],
                membership_ordinals: vec![("change-1".to_string(), 0)],
            },
            checksum: "commit-checksum".to_string(),
        }],
        changes: vec![SegmentChange {
            id: "change-1".to_string(),
            authored_commit_id: Some("commit-1".to_string()),
            entity_id: EntityIdentity::single("entity-1"),
            schema_key: "message".to_string(),
            file_id: Some("file-1".to_string()),
            snapshot_ref: None,
            metadata_ref: None,
            created_at: "2026-05-12T00:00:00Z".to_string(),
            inline_payloads: Vec::new(),
            directory: SegmentChangeDirectory::default(),
        }],
    })
    .unwrap()
}

pub(crate) fn two_commit_segment() -> Segment {
    let mut segment = test_segment();
    segment.commits.push(SegmentCommit {
        header: CommitHeader {
            id: "commit-2".to_string(),
            parent_commit_ids: vec!["commit-1".to_string()],
            derivable_change_id: "derived-change-2".to_string(),
            author_account_ids: vec!["account-2".to_string()],
            created_at: "2026-05-12T00:01:00Z".to_string(),
            membership_count: 1,
        },
        body: CommitBody {
            membership: vec![MembershipRecord {
                member_change_id: "change-1".to_string(),
                role: MembershipRole::Adopted,
                source_parent_ordinal: Some(0),
            }],
        },
        directory: SegmentCommitDirectory {
            state_row_identities: vec![(
                state_row_identity("message", "file-1", "entity-1"),
                "change-1".to_string(),
            )],
            membership_ordinals: vec![("change-1".to_string(), 0)],
        },
        checksum: String::new(),
    });
    canonicalize_segment(segment).unwrap()
}

pub(crate) fn two_change_segment() -> Segment {
    let mut segment = test_segment();
    let identity = state_row_identity("message", "file-1", "entity-2");
    segment.commits[0].body.membership.push(MembershipRecord {
        member_change_id: "change-2".to_string(),
        role: MembershipRole::Authored,
        source_parent_ordinal: None,
    });
    segment.commits[0]
        .directory
        .state_row_identities
        .push((identity, "change-2".to_string()));
    segment.commits[0]
        .directory
        .membership_ordinals
        .push(("change-2".to_string(), 1));
    segment.changes.push(SegmentChange {
        id: "change-2".to_string(),
        authored_commit_id: Some("commit-1".to_string()),
        entity_id: EntityIdentity::single("entity-2"),
        schema_key: "message".to_string(),
        file_id: Some("file-1".to_string()),
        snapshot_ref: None,
        metadata_ref: None,
        created_at: "2026-05-12T00:01:00Z".to_string(),
        inline_payloads: Vec::new(),
        directory: SegmentChangeDirectory::default(),
    });
    canonicalize_segment(segment).unwrap()
}

pub(crate) fn commit_visibility_from_segment(
    segment: &Segment,
    commit_id: &str,
) -> CommitVisibility {
    let location = directory_commit_location(segment, commit_id).unwrap();
    CommitVisibility {
        commit_id: commit_id.to_string(),
        checksum: location.checksum.clone(),
        location,
    }
}

pub(crate) async fn assert_mandatory_index_rows_match_segment(
    storage: &StorageContext,
    segment: &Segment,
) {
    let result = read_test_value_groups(
        storage,
        vec![
            (BY_COMMIT_INDEX_SPACE, vec![by_commit_key("commit-1")]),
            (BY_CHANGE_INDEX_SPACE, vec![by_change_key("change-1")]),
            (
                BY_CHANGE_MEMBERSHIP_INDEX_SPACE,
                vec![by_change_membership_key("change-1", "commit-1")],
            ),
        ],
    );

    let by_commit = decode_by_commit_entry(result[0][0].as_deref().unwrap()).unwrap();
    assert_eq!(by_commit.commit_id, "commit-1");
    assert_eq!(
        by_commit.location,
        directory_commit_location(segment, "commit-1").unwrap()
    );
    assert_eq!(by_commit.parent_commit_ids, Vec::<String>::new());
    assert_eq!(by_commit.generation, 0);

    let by_change = decode_by_change_entry(result[1][0].as_deref().unwrap()).unwrap();
    assert_eq!(by_change.change_id, "change-1");
    assert_eq!(
        by_change.location,
        directory_change_location(segment, "change-1").unwrap()
    );

    assert_eq!(result[2][0].as_deref(), Some([].as_slice()));
}

pub(crate) fn stage_stale_mandatory_index_rows(writes: &mut StorageWriteSet) {
    writes.put(
        BY_COMMIT_INDEX_SPACE,
        by_commit_key("stale-commit"),
        by_commit_index_value(&ByCommitEntry {
            commit_id: "stale-commit".to_string(),
            location: location("missing-segment", 0, 0, "stale-commit-checksum"),
            parent_commit_ids: Vec::new(),
            generation: 0,
        })
        .unwrap(),
    );
    writes.put(
        BY_CHANGE_INDEX_SPACE,
        by_change_key("stale-change"),
        by_change_index_value(&ByChangeEntry {
            change_id: "stale-change".to_string(),
            location: location("missing-segment", 0, 0, "stale-change-checksum"),
        })
        .unwrap(),
    );
    writes.put(
        BY_CHANGE_MEMBERSHIP_INDEX_SPACE,
        by_change_membership_key("stale-change", "stale-commit"),
        by_change_membership_index_value(),
    );
}

pub(crate) async fn assert_stale_mandatory_index_rows_deleted(storage: &StorageContext) {
    let result = read_test_value_groups(
        storage,
        vec![
            (
                BY_COMMIT_INDEX_SPACE,
                vec![by_commit_key("stale-commit"), by_commit_key("commit-1")],
            ),
            (
                BY_CHANGE_INDEX_SPACE,
                vec![by_change_key("stale-change"), by_change_key("change-1")],
            ),
            (
                BY_CHANGE_MEMBERSHIP_INDEX_SPACE,
                vec![
                    by_change_membership_key("stale-change", "stale-commit"),
                    by_change_membership_key("change-1", "commit-1"),
                ],
            ),
        ],
    );

    assert_eq!(result[0][0], None);
    assert!(result[0][1].is_some());
    assert_eq!(result[1][0], None);
    assert!(result[1][1].is_some());
    assert_eq!(result[2][0], None);
    assert_eq!(result[2][1].as_deref(), Some([].as_slice()));
}

pub(crate) async fn write_raw_segment(storage: &StorageContext, segment: &Segment) {
    let mut transaction = storage.begin_write_transaction().await.unwrap();
    let mut writes = StorageWriteSet::new();
    writes.put(
        SEGMENT_SPACE,
        segment_key(&segment.header.segment_id),
        segment_value(segment).unwrap(),
    );
    writes.apply(&mut *transaction).await.unwrap();
    transaction.commit().await.unwrap();
}

pub(crate) fn location(
    segment_id: &str,
    offset: u64,
    len: u64,
    checksum: &str,
) -> SegmentObjectLocation {
    SegmentObjectLocation {
        segment_id: segment_id.to_string(),
        offset,
        len,
        checksum: checksum.to_string(),
    }
}

pub(crate) fn state_row_identity(
    schema_key: &str,
    file_id: &str,
    entity_id: &str,
) -> StateRowIdentity {
    let entity_id = EntityIdentity::single(entity_id)
        .as_json_array_text()
        .unwrap();
    StateRowIdentity {
        schema_key: CanonicalSchemaKey::new(schema_key).unwrap(),
        file_id: FileId::new(file_id).unwrap(),
        entity_id: EntityId::new(entity_id).unwrap(),
    }
}

pub(crate) fn counting_reader(inner: StorageContext) -> (CountingReader, Arc<AtomicUsize>) {
    let segment_gets = Arc::new(AtomicUsize::new(0));
    (
        CountingReader {
            inner,
            segment_gets: segment_gets.clone(),
        },
        segment_gets,
    )
}

pub(crate) struct CountingReader {
    inner: StorageContext,
    segment_gets: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl ChangelogStorageRead for CountingReader {
    async fn changelog_get_many(
        &mut self,
        space: crate::storage::StorageSpace,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, LixError> {
        if space == SEGMENT_SPACE {
            self.segment_gets.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.changelog_get_many(space, keys).await
    }

    async fn changelog_scan(
        &mut self,
        space: crate::storage::StorageSpace,
        prefix: Vec<u8>,
        after: Option<Vec<u8>>,
        limit: usize,
        projection: crate::storage::StorageCoreProjection,
    ) -> Result<super::context::ChangelogScanPage, LixError> {
        self.inner
            .changelog_scan(space, prefix, after, limit, projection)
            .await
    }
}

pub(crate) fn read_test_value_groups(
    storage: &StorageContext,
    groups: Vec<(StorageSpace, Vec<Vec<u8>>)>,
) -> Vec<Vec<Option<Vec<u8>>>> {
    let mut read = storage.begin_read(StorageReadOptions::default()).unwrap();
    groups
        .into_iter()
        .map(|(space, keys)| {
            let keys = keys
                .into_iter()
                .map(|key| StorageKey(bytes::Bytes::from(key)))
                .collect::<Vec<_>>();
            PointReadPlan::new(space, &keys)
                .materialize(&mut read, StorageGetOptions::default())
                .unwrap()
                .value
                .into_iter()
                .map(|value| match value {
                    Some(StorageProjectedValue::FullValue(bytes)) => Some(bytes.to_vec()),
                    Some(StorageProjectedValue::KeyOnly) => Some(Vec::new()),
                    None => None,
                })
                .collect()
        })
        .collect()
}
