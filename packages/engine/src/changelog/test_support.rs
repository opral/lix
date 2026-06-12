use crate::changelog::{
    ChangeId, ChangeRecord, ChangelogAppend, CommitChangeRefSet, CommitId, CommitRecord,
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
            commit_id: CommitId::for_test_label("commit-1"),
            entries: vec![ChangeId::for_test_label("change-1")],
        }],
    }
}

pub(crate) fn test_commit_record() -> CommitRecord {
    CommitRecord {
        format_version: 1,
        commit_id: CommitId::for_test_label("commit-1"),
        parent_commit_ids: Vec::new(),
        change_id: ChangeId::for_test_label("commit-row-change-1"),
        author_account_ids: vec!["account-1".to_string()],
        created_at: crate::common::LixTimestamp::expect_parse("created_at", "2026-05-12T00:00:00Z"),
    }
}

pub(crate) fn test_change_record() -> ChangeRecord {
    ChangeRecord {
        format_version: 1,
        change_id: ChangeId::for_test_label("change-1"),
        schema_key: "message".to_string(),
        entity_pk: EntityPk::single("entity-1"),
        file_id: Some("file-1".to_string()),
        snapshot: crate::json_store::JsonSlot::None,
        metadata: crate::json_store::JsonSlot::None,
        created_at: crate::common::LixTimestamp::expect_parse("created_at", "2026-05-12T00:00:00Z"),
    }
}
