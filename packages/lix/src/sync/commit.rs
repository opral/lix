//! Exact, storage-independent representation of one immutable Lix commit.
//!
//! The wire shape carries the logical commit authority rather than Lix's
//! physical packed-delta layout. JSON sidecars are materialized so a commit is
//! self-contained, while binary file content remains referenced by the
//! `lix_binary_blob_ref` row and travels through the binary CAS protocol.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::changelog::{
    ChangeRecordProjection, ChangelogContext, ChangelogReader, CommitId, CommitLoadRequest,
    materialize_known_change_payloads_in_order,
};
use crate::common::LixTimestamp;
use crate::row_pk::RowPk;
use crate::storage_adapter::{Storage, StorageAdapterRead, StorageReadOptions};
use crate::tracked_state::load_commit_delta_members_with_payloads;
use crate::{Lix, LixError};

/// A complete immutable commit, independent of the local storage layout.
///
/// Generation and first-parent jump pointers are intentionally omitted: they
/// are derived indexes validated from `parent_commit_ids` when the commit is
/// imported. The remaining header fields are semantic commit authority.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncCommit {
    pub commit_id: String,
    pub parent_commit_ids: Vec<String>,
    pub account_id: String,
    pub created_at: String,
    pub selected_source_commit_id: Option<String>,
    pub members: Vec<SyncCommitMember>,
}

/// One identity-ordered member of a commit delta.
///
/// Row lifecycle timestamps are distinct from the authored change timestamp:
/// selected merge members retain their source change while acquiring state
/// coordinates in the selecting commit.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncCommitMember {
    pub change_id: String,
    pub authored: bool,
    pub schema_key: String,
    pub file_id: Option<String>,
    pub row_pk: serde_json::Value,
    pub deleted: bool,
    pub snapshot: Option<serde_json::Value>,
    pub metadata: Option<serde_json::Value>,
    pub row_created_at: String,
    pub row_updated_at: String,
    pub change_account_id: String,
    pub change_created_at: String,
    pub origin_key: Option<String>,
}

pub(crate) struct SyncCommitMemberRef<'a> {
    pub(crate) change_id: crate::changelog::ChangeId,
    pub(crate) authored: bool,
    pub(crate) schema_key: &'a str,
    pub(crate) file_id: Option<&'a str>,
    pub(crate) row_pk: &'a RowPk,
    pub(crate) deleted: bool,
    pub(crate) snapshot_json: Option<&'a str>,
    pub(crate) metadata_json: Option<&'a str>,
    pub(crate) row_created_at: LixTimestamp,
    pub(crate) row_updated_at: LixTimestamp,
    pub(crate) change_account_id: &'a str,
    pub(crate) change_created_at: LixTimestamp,
    pub(crate) origin_key: Option<&'a str>,
}

/// One canonical member encoder shared by staged authority preflight and
/// post-commit storage export. Keeping JSON parsing and typed primary-key
/// projection here makes byte-size admission test the exact public wire shape.
pub(crate) fn encode_sync_commit_member(
    member: SyncCommitMemberRef<'_>,
) -> Result<SyncCommitMember, LixError> {
    Ok(SyncCommitMember {
        change_id: member.change_id.to_string(),
        authored: member.authored,
        schema_key: member.schema_key.to_owned(),
        file_id: member.file_id.map(str::to_owned),
        row_pk: member.row_pk.as_typed_json_array_value()?,
        deleted: member.deleted,
        snapshot: parse_materialized_json(member.snapshot_json, member.change_id, "snapshot")?,
        metadata: parse_materialized_json(member.metadata_json, member.change_id, "metadata")?,
        row_created_at: member.row_created_at.to_string(),
        row_updated_at: member.row_updated_at.to_string(),
        change_account_id: member.change_account_id.to_owned(),
        change_created_at: member.change_created_at.to_string(),
        origin_key: member.origin_key.map(str::to_owned),
    })
}

impl SyncCommit {
    /// Validates all untrusted identities before storage or graph code sees
    /// them. Member order is part of the canonical wire representation.
    pub(crate) fn validate(&self) -> Result<(), LixError> {
        let commit_id = CommitId::parse_lix(&self.commit_id, "sync commit id")?;
        parse_timestamp("sync commit createdAt", &self.created_at)?;
        if self.account_id.is_empty() {
            return invalid("sync commit accountId must not be empty");
        }

        let mut parents = BTreeSet::new();
        for parent in &self.parent_commit_ids {
            let parent = CommitId::parse_lix(parent, "sync parent commit id")?;
            if parent == commit_id {
                return invalid("sync commit cannot be its own parent");
            }
            if !parents.insert(parent) {
                return invalid("sync commit parent ids must be unique");
            }
        }
        let selected_source_commit_id = self
            .selected_source_commit_id
            .as_deref()
            .map(|source| CommitId::parse_lix(source, "sync selected source commit id"))
            .transpose()?;
        if selected_source_commit_id == Some(commit_id) {
            return invalid("sync commit cannot select itself as its source");
        }

        let mut previous: Option<(String, Option<String>, RowPk)> = None;
        let mut authored_change_ids = BTreeSet::new();
        for member in &self.members {
            if member.schema_key.is_empty() {
                return invalid("sync commit member schemaKey must not be empty");
            }
            if member.change_account_id.is_empty() {
                return invalid("sync commit member changeAccountId must not be empty");
            }
            let change_id = crate::changelog::ChangeId::parse_lix(
                &member.change_id,
                "sync commit member change id",
            )?;
            if member.authored && member.change_account_id != self.account_id {
                return invalid("authored sync member account must match its commit account");
            }
            if member.authored && !authored_change_ids.insert(change_id) {
                return invalid("authored sync member change ids must be unique");
            }
            let row_created_at =
                parse_timestamp("sync member rowCreatedAt", &member.row_created_at)?;
            let row_updated_at =
                parse_timestamp("sync member rowUpdatedAt", &member.row_updated_at)?;
            if row_created_at > row_updated_at {
                return invalid("sync member rowCreatedAt must not follow rowUpdatedAt");
            }
            parse_timestamp("sync member changeCreatedAt", &member.change_created_at)?;
            if member.deleted == member.snapshot.is_some() {
                return invalid(
                    "sync commit member must have a snapshot exactly when it is not deleted",
                );
            }
            let row_pk = RowPk::from_typed_json_array_value(&member.row_pk).map_err(|error| {
                LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!("sync commit member rowPk is invalid: {error}"),
                )
            })?;
            let identity = (member.schema_key.clone(), member.file_id.clone(), row_pk);
            if previous
                .as_ref()
                .is_some_and(|previous| previous >= &identity)
            {
                return invalid("sync commit members must be strictly identity ordered");
            }
            previous = Some(identity);
        }
        let has_selected_members = self.members.iter().any(|member| !member.authored);
        let is_merge = self.parent_commit_ids.len() > 1;
        if is_merge && has_selected_members {
            if self.selected_source_commit_id.as_deref()
                != self.parent_commit_ids.get(1).map(String::as_str)
            {
                return invalid("merge selectedSourceCommitId must equal its second parent");
            }
        } else if selected_source_commit_id.is_some() {
            return invalid(
                "selectedSourceCommitId is allowed only for a merge with selected members",
            );
        }
        Ok(())
    }
}

fn parse_timestamp(context: &str, value: &str) -> Result<LixTimestamp, LixError> {
    LixTimestamp::parse(value).map_err(|error| {
        LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("{context} is invalid: {error}"),
        )
    })
}

fn invalid<T>(message: impl Into<String>) -> Result<T, LixError> {
    Err(LixError::new(LixError::CODE_INVALID_PARAM, message.into()))
}

/// Exports one complete logical commit from a repository.
///
/// Missing ids are ordinary history results, so they remain `None` instead of
/// being collapsed into a transport error.
pub(crate) async fn export_sync_commit<StorageImpl>(
    lix: &Lix<StorageImpl>,
    commit_id: &str,
) -> Result<Option<SyncCommit>, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let commit_id = CommitId::parse_lix(commit_id, "sync commit id")?;
    let adapter = lix.storage_adapter();
    let read = adapter.begin_read(StorageReadOptions::default()).await?;
    load_sync_commit(&read, commit_id).await
}

/// Exports one complete logical commit from a pinned repository snapshot.
async fn export_sync_commit_from_store<S>(
    store: &S,
    commit_id: CommitId,
) -> Result<SyncCommit, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    load_sync_commit(store, commit_id).await?.ok_or_else(|| {
        LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("sync commit '{commit_id}' does not exist"),
        )
    })
}

pub(crate) async fn load_sync_commit<S>(
    store: &S,
    commit_id: CommitId,
) -> Result<Option<SyncCommit>, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    let requested = [commit_id];
    let record = ChangelogContext::new()
        .reader(store)
        .load_commits(CommitLoadRequest {
            commit_ids: &requested,
        })
        .await?
        .into_iter()
        .next()
        .and_then(|(_, record)| record);
    let Some(record) = record else {
        return Ok(None);
    };

    let delta = load_commit_delta_members_with_payloads(store, commit_id).await?;
    let payloads = materialize_known_change_payloads_in_order(
        store,
        delta.iter().map(|member| member.change.clone()),
        ChangeRecordProjection::full(),
    )
    .await?;
    if payloads.len() != delta.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "sync commit payload count does not match its delta membership",
        ));
    }

    let members = delta
        .into_iter()
        .zip(payloads)
        .map(|(member, (change_id, payload))| {
            if change_id != member.value.change_id || change_id != member.change.change_id {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "sync commit '{commit_id}' member payload identity does not match its delta"
                    ),
                ));
            }
            encode_sync_commit_member(SyncCommitMemberRef {
                change_id,
                authored: member.authored,
                schema_key: &member.key.schema_key,
                file_id: member.key.file_id.as_deref(),
                row_pk: &member.key.row_pk,
                deleted: member.value.deleted,
                snapshot_json: payload.snapshot_content.as_deref(),
                metadata_json: payload.metadata.as_deref(),
                row_created_at: member.value.created_at,
                row_updated_at: member.value.updated_at,
                change_account_id: &member.change.account_id,
                change_created_at: member.change.created_at,
                origin_key: member.change.origin_key.as_deref(),
            })
        })
        .collect::<Result<Vec<_>, LixError>>()?;

    // Merge provenance is already canonical in the commit graph: parent zero
    // is the target and parent one is the selected source. Non-merge
    // checkpoints carry complete selected members and need no source pointer.
    let has_selected_members = members.iter().any(|member| !member.authored);
    let selected_source_commit_id = (has_selected_members && record.parent_commit_ids.len() > 1)
        .then(|| record.parent_commit_ids[1]);

    let exported = SyncCommit {
        commit_id: record.commit_id.to_string(),
        parent_commit_ids: record
            .parent_commit_ids
            .into_iter()
            .map(|parent| parent.to_string())
            .collect(),
        account_id: record.account_id,
        created_at: record.created_at.to_string(),
        selected_source_commit_id: selected_source_commit_id.map(|source| source.to_string()),
        members,
    };
    exported.validate()?;
    Ok(Some(exported))
}

fn parse_materialized_json(
    json: Option<&str>,
    change_id: crate::changelog::ChangeId,
    field: &str,
) -> Result<Option<serde_json::Value>, LixError> {
    json.map(|json| {
        serde_json::from_str(json).map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("sync commit change '{change_id}' has invalid {field} JSON: {error}"),
            )
        })
    })
    .transpose()
}

/// Returns whether an incoming commit is already stored byte-for-byte at the
/// logical protocol level. A reused commit id with different content fails
/// closed instead of being mistaken for an idempotent retry.
pub(crate) async fn sync_commit_already_present<S>(
    store: &S,
    incoming: &SyncCommit,
) -> Result<bool, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    incoming.validate()?;
    let commit_id = CommitId::parse_lix(&incoming.commit_id, "sync commit id")?;
    let Some(existing) = load_sync_commit(store, commit_id).await? else {
        return Ok(false);
    };
    if existing == *incoming {
        return Ok(true);
    }
    invalid(format!(
        "sync commit id '{}' already exists with different content",
        incoming.commit_id
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn exported_key_value_commit() -> (Lix, SyncCommit) {
        let lix = crate::open_lix().await.expect("open lix");
        lix.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ($1, CAST($2 AS JSONB))",
            &[
                crate::Value::Text("sync-commit-codec".to_owned()),
                crate::Value::Text("{\"answer\":42}".to_owned()),
            ],
        )
        .with_origin_key("sync-commit-codec-origin")
        .await
        .expect("write commit fixture");
        let commit_id = lix
            .execute("SELECT lix_active_branch_commit_id() AS id", &[])
            .await
            .expect("load fixture head")
            .rows()[0]
            .get::<String>("id")
            .expect("fixture head id");
        let adapter = lix.storage_adapter();
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open export snapshot");
        let exported = export_sync_commit_from_store(
            &read,
            CommitId::parse_lix(&commit_id, "test commit").expect("parse fixture head"),
        )
        .await
        .expect("export fixture commit");
        (lix, exported)
    }

    #[tokio::test]
    async fn export_is_complete_deterministic_and_idempotent() {
        let (lix, exported) = exported_key_value_commit().await;
        assert!(!exported.members.is_empty());
        assert!(exported.members.iter().any(|member| {
            member.schema_key == "lix_key_value"
                && member.origin_key.as_deref() == Some("sync-commit-codec-origin")
                && member.snapshot.as_ref().is_some_and(|snapshot| {
                    snapshot["key"] == "sync-commit-codec" && snapshot["value"]["answer"] == 42
                })
        }));
        let adapter = lix.storage_adapter();
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open comparison snapshot");
        assert!(
            sync_commit_already_present(&read, &exported)
                .await
                .expect("compare existing commit")
        );
        let again = export_sync_commit_from_store(
            &read,
            CommitId::parse_lix(&exported.commit_id, "test commit").expect("parse fixture commit"),
        )
        .await
        .expect("re-export fixture commit");
        assert_eq!(exported, again);
        assert_eq!(
            export_sync_commit(&lix, &exported.commit_id)
                .await
                .expect("repository export"),
            Some(exported)
        );
        lix.close().await.expect("close fixture");
    }

    #[tokio::test]
    async fn same_id_with_different_content_is_not_an_idempotent_retry() {
        let (lix, mut incoming) = exported_key_value_commit().await;
        incoming.members[0].snapshot = Some(serde_json::json!({"different": true}));
        let adapter = lix.storage_adapter();
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open comparison snapshot");
        let error = sync_commit_already_present(&read, &incoming)
            .await
            .expect_err("different content must fail");
        assert!(error.message.contains("different content"));
        lix.close().await.expect("close fixture");
    }

    #[tokio::test]
    async fn ordinary_merge_exports_graph_derived_selection_provenance() {
        let lix = crate::open_lix().await.expect("open merge fixture");
        let source_branch = lix
            .create_branch(crate::CreateBranchOptions {
                id: None,
                name: "sync codec selected source".to_owned(),
                from_commit_id: None,
            })
            .await
            .expect("create source branch");
        let source = lix
            .open_another_session()
            .await
            .expect("open source session");
        source
            .switch_branch(crate::SwitchBranchOptions {
                branch_id: source_branch.id.clone(),
            })
            .await
            .expect("switch source branch");
        source
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('codec-source', 'selected')",
                &[],
            )
            .await
            .expect("write source branch");
        lix.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('codec-target', 'authored')",
            &[],
        )
        .await
        .expect("write target branch");
        let merge = lix
            .merge_branch(crate::MergeBranchOptions {
                source_branch_id: source_branch.id,
            })
            .await
            .expect("merge disjoint branches");
        let merge_commit_id = merge
            .created_merge_commit_id
            .expect("divergence should create a merge commit");
        let exported = export_sync_commit(&lix, &merge_commit_id)
            .await
            .expect("export merge commit")
            .expect("merge commit exists");
        assert_eq!(
            exported.selected_source_commit_id,
            Some(merge.source_head_before_commit_id)
        );
        assert!(exported.members.iter().any(|member| !member.authored));
        source.close().await.expect("close source session");
        lix.close().await.expect("close merge fixture");
    }

    #[tokio::test]
    async fn public_nested_merge_exports_a_valid_complete_sync_commit() {
        let lix = crate::open_lix().await.expect("open nested merge fixture");
        let main_branch_id = lix.active_branch_id().await.expect("load main branch");
        let source_branch = lix
            .create_branch(crate::CreateBranchOptions {
                id: None,
                name: "nested merge source".to_owned(),
                from_commit_id: None,
            })
            .await
            .expect("create first source branch");
        let second_target = lix
            .create_branch(crate::CreateBranchOptions {
                id: None,
                name: "nested merge target".to_owned(),
                from_commit_id: None,
            })
            .await
            .expect("create second target branch");

        let source = lix
            .open_another_session()
            .await
            .expect("open source session");
        source
            .switch_branch(crate::SwitchBranchOptions {
                branch_id: source_branch.id.clone(),
            })
            .await
            .expect("switch first source branch");
        source
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('nested-source', 'selected')",
                &[],
            )
            .await
            .expect("write first source branch");
        lix.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('nested-main', 'authored')",
            &[],
        )
        .await
        .expect("write main branch");
        let first = lix
            .merge_branch(crate::MergeBranchOptions {
                source_branch_id: source_branch.id,
            })
            .await
            .expect("create first merge");
        let first_id = first
            .created_merge_commit_id
            .expect("first merge should create a commit");
        let first_export = export_sync_commit(&lix, &first_id)
            .await
            .expect("export first merge")
            .expect("first merge exists");
        first_export
            .validate()
            .expect("first merge must be a valid sync commit");
        assert!(first_export.selected_source_commit_id.is_some());

        let target = lix
            .open_another_session()
            .await
            .expect("open second target");
        target
            .switch_branch(crate::SwitchBranchOptions {
                branch_id: second_target.id,
            })
            .await
            .expect("switch second target branch");
        target
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('nested-target', 'authored')",
                &[],
            )
            .await
            .expect("write second target branch");
        let nested = target
            .merge_branch(crate::MergeBranchOptions {
                source_branch_id: main_branch_id,
            })
            .await
            .expect("public merge should accept a source head that is itself a merge");
        let nested_id = nested
            .created_merge_commit_id
            .expect("nested divergence should create a merge commit");
        let nested_export = export_sync_commit(&lix, &nested_id)
            .await
            .expect("export nested merge")
            .expect("nested merge exists");
        nested_export
            .validate()
            .expect("every public merge must export as a valid complete sync commit");

        target.close().await.expect("close second target");
        source.close().await.expect("close source session");
        lix.close().await.expect("close nested merge fixture");
    }

    #[tokio::test]
    async fn checkpoint_exports_complete_selected_members_without_a_source_pointer() {
        let lix = crate::open_lix().await.expect("open checkpoint fixture");
        lix.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('checkpoint-source', 'selected')",
            &[],
        )
        .await
        .expect("write checkpoint interval");
        lix.create_checkpoint().await.expect("create checkpoint");
        let checkpoint_id = lix
            .execute("SELECT lix_active_branch_commit_id() AS id", &[])
            .await
            .expect("load checkpoint head")
            .rows()[0]
            .get::<String>("id")
            .expect("checkpoint id");
        let exported = export_sync_commit(&lix, &checkpoint_id)
            .await
            .expect("export checkpoint")
            .expect("checkpoint exists");
        exported
            .validate()
            .expect("checkpoint alias must remain a valid sync commit");
        assert!(exported.members.iter().any(|member| !member.authored));
        assert_eq!(exported.selected_source_commit_id, None);
        lix.close().await.expect("close checkpoint fixture");
    }

    #[test]
    fn validation_rejects_noncanonical_member_order_and_authorship() {
        let commit_id = CommitId::for_test_label("sync-validation");
        let member = |label: &str, ordinal: u32| {
            let mut change_bytes = *commit_id.as_uuid().as_bytes();
            change_bytes[12..].copy_from_slice(&ordinal.to_be_bytes());
            SyncCommitMember {
                change_id: crate::changelog::ChangeId::new(uuid::Uuid::from_bytes(change_bytes))
                    .to_string(),
                authored: true,
                schema_key: "schema".to_owned(),
                file_id: None,
                row_pk: serde_json::json!([{ "type": "string", "value": label }]),
                deleted: false,
                snapshot: Some(serde_json::json!({"id": label})),
                metadata: None,
                row_created_at: "2026-08-19T00:00:00Z".to_owned(),
                row_updated_at: "2026-08-19T00:00:00Z".to_owned(),
                change_account_id: crate::ANONYMOUS_ACCOUNT_ID.to_owned(),
                change_created_at: "2026-08-19T00:00:00Z".to_owned(),
                origin_key: None,
            }
        };
        let mut commit = SyncCommit {
            commit_id: commit_id.to_string(),
            parent_commit_ids: Vec::new(),
            account_id: crate::ANONYMOUS_ACCOUNT_ID.to_owned(),
            created_at: "2026-08-19T00:00:00Z".to_owned(),
            selected_source_commit_id: None,
            members: vec![member("b", 1), member("a", 2)],
        };
        assert!(
            commit
                .validate()
                .expect_err("descending identities must fail")
                .message
                .contains("strictly identity ordered")
        );
        commit.members = vec![member("a", 1)];
        commit.members[0].change_account_id = crate::SYSTEM_ACCOUNT_ID.to_owned();
        assert!(
            commit
                .validate()
                .expect_err("foreign authored account must fail")
                .message
                .contains("account")
        );

        commit.members[0].authored = false;
        commit.members[0].change_account_id = crate::ANONYMOUS_ACCOUNT_ID.to_owned();
        commit
            .validate()
            .expect("non-merge checkpoint members are self-contained");
        commit.selected_source_commit_id = Some(CommitId::for_test_label("source").to_string());
        assert!(
            commit
                .validate()
                .expect_err("a non-merge source pointer must fail")
                .message
                .contains("allowed only")
        );

        commit.parent_commit_ids = vec![
            CommitId::for_test_label("target").to_string(),
            CommitId::for_test_label("source").to_string(),
        ];
        commit
            .validate()
            .expect("merge selected source equals the second parent");
        commit.selected_source_commit_id = None;
        assert!(
            commit
                .validate()
                .expect_err("merge selected members require the second parent source")
                .message
                .contains("second parent")
        );
    }
}
