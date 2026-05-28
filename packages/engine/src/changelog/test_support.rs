use crate::changelog::{
    ChangeRecord, ChangelogAppend, CommitChangeRef, CommitChangeRefSet, CommitRecord,
};
use crate::entity_pk::EntityPk;
use crate::storage::{InMemoryStorageBackend, StorageContext};

use super::ChangelogContext;

pub(crate) fn changelog_test_context() -> (ChangelogContext, StorageContext) {
    (
        ChangelogContext::new(),
        StorageContext::new(InMemoryStorageBackend::new()),
    )
}

pub(crate) fn test_append() -> ChangelogAppend {
    ChangelogAppend {
        commits: vec![test_commit_record()],
        changes: vec![test_change_record()],
        commit_change_refs: vec![CommitChangeRefSet {
            commit_id: "commit-1".to_string(),
            entries: vec![CommitChangeRef {
                schema_key: "message".to_string(),
                file_id: Some("file-1".to_string()),
                entity_pk: EntityPk::single("entity-1"),
                change_id: "change-1".to_string(),
            }],
        }],
    }
}

pub(crate) fn test_commit_record() -> CommitRecord {
    CommitRecord {
        format_version: 1,
        commit_id: "commit-1".to_string(),
        parent_commit_ids: Vec::new(),
        change_id: "commit-row-change-1".to_string(),
        author_account_ids: vec!["account-1".to_string()],
        created_at: crate::common::LixTimestamp::expect_parse("created_at", "2026-05-12T00:00:00Z"),
    }
}

pub(crate) fn test_change_record() -> ChangeRecord {
    ChangeRecord {
        format_version: 1,
        change_id: "change-1".to_string(),
        schema_key: "message".to_string(),
        entity_pk: EntityPk::single("entity-1"),
        file_id: Some("file-1".to_string()),
        snapshot_ref: None,
        metadata_ref: None,
        created_at: crate::common::LixTimestamp::expect_parse("created_at", "2026-05-12T00:00:00Z"),
    }
}
