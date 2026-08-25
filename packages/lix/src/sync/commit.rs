//! Exact, storage-independent representation of one immutable Lix commit.
//!
//! The wire shape carries the logical commit authority rather than Lix's
//! physical packed-delta layout. JSON sidecars are materialized so a commit is
//! self-contained, while binary file content remains referenced by the
//! `lix_binary_blob_ref` row and travels through the binary CAS protocol.

use std::collections::BTreeSet;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::changelog::{
    ChangeRecordProjection, ChangelogContext, ChangelogReader, CommitId, CommitLoadRequest,
    materialize_known_change_payloads_in_order,
};
use crate::common::LixTimestamp;
use crate::row_pk::RowPk;
use crate::storage_adapter::{
    Storage, StorageAdapterRead, StorageGetManyRequest, StorageGetOptions, StorageKey,
    StorageProjectedValue, StorageReadOptions, StorageSpace, StorageSpaceId, StorageValue,
    StorageWriteSet, ValueSemantics, exact_get_many,
};
use crate::tracked_state::{
    load_commit_delta_members_with_payloads, load_commit_state_manifest,
    load_local_commit_delta_members_with_payloads,
};
use crate::{Lix, LixError};

pub(crate) const SYNC_MATERIALIZED_STATE_ALIAS_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0007_0015),
    "sync.materialized_state_alias.v1",
    ValueSemantics::Immutable,
);

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
    #[serde(default)]
    pub global_scope: bool,
    pub selected_source_commit_id: Option<String>,
    /// Authenticated O(1) representation of a complete-state checkpoint.
    /// The source is a dependency and `state_root_id` binds the alias to the
    /// exact persistent tree the authority captured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_alias: Option<SyncCommitStateAlias>,
    pub members: Vec<SyncCommitMember>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncCommitStateAlias {
    pub source_commit_id: String,
    pub state_root_id: String,
}

fn materialized_state_alias_key(commit_id: CommitId) -> StorageKey {
    StorageKey(Bytes::copy_from_slice(commit_id.as_uuid().as_bytes()))
}

pub(crate) fn stage_materialized_sync_state_alias(
    writes: &mut StorageWriteSet,
    commit_id: CommitId,
    alias: &SyncCommitStateAlias,
) -> Result<(), LixError> {
    let bytes = serde_json::to_vec(alias).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("encode materialized sync state alias: {error}"),
        )
    })?;
    writes.put(
        SYNC_MATERIALIZED_STATE_ALIAS_SPACE,
        materialized_state_alias_key(commit_id),
        StorageValue {
            bytes: Bytes::from(bytes),
        },
    );
    Ok(())
}

pub(crate) fn stage_delete_materialized_sync_state_alias(
    writes: &mut StorageWriteSet,
    commit_id: CommitId,
) {
    writes.delete(
        SYNC_MATERIALIZED_STATE_ALIAS_SPACE,
        materialized_state_alias_key(commit_id),
    );
}

async fn load_materialized_sync_state_alias(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
) -> Result<Option<SyncCommitStateAlias>, LixError> {
    let key = materialized_state_alias_key(commit_id);
    let values = exact_get_many(
        store,
        &[StorageGetManyRequest {
            space: SYNC_MATERIALIZED_STATE_ALIAS_SPACE,
            keys: std::slice::from_ref(&key),
            opts: StorageGetOptions::default(),
        }],
    )
    .await?;
    let Some(value) = values.values.into_iter().next().flatten() else {
        return Ok(None);
    };
    let StorageProjectedValue::FullValue(value) = value else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "materialized sync state alias read omitted its value",
        ));
    };
    serde_json::from_slice(&value).map(Some).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("decode materialized sync state alias: {error}"),
        )
    })
}

pub(crate) async fn load_sync_commit_state_alias(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
) -> Result<Option<SyncCommitStateAlias>, LixError> {
    if let Some(alias) = load_materialized_sync_state_alias(store, commit_id).await? {
        return Ok(Some(alias));
    }
    load_manifest_sync_state_alias(store, commit_id).await
}

async fn load_manifest_sync_state_alias(
    store: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
) -> Result<Option<SyncCommitStateAlias>, LixError> {
    Ok(load_commit_state_manifest(store, commit_id)
        .await?
        .and_then(|manifest| manifest.snapshot_root)
        .filter(|root| root.complete_state_fence)
        .and_then(|root| {
            let source = root.parent_roots.into_iter().next()?;
            Some(SyncCommitStateAlias {
                source_commit_id: source.commit_id.to_string(),
                state_root_id: blake3::Hash::from_bytes(*root.root_id.as_bytes())
                    .to_hex()
                    .to_string(),
            })
        }))
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
        if let Some(alias) = &self.state_alias {
            let source = CommitId::parse_lix(
                &alias.source_commit_id,
                "sync complete-state source commit id",
            )?;
            if source == commit_id {
                return invalid("sync commit cannot alias its own state");
            }
            super::validate_blake3_id(
                &alias.state_root_id,
                "sync complete-state alias stateRootId",
            )?;
            if self.selected_source_commit_id.is_some() {
                return invalid(
                    "sync commit cannot carry both selected and complete-state sources",
                );
            }
            if self.parent_commit_ids.len() != 1 {
                return invalid("sync complete-state alias must have exactly one semantic parent");
            }
            if !self.members.is_empty() {
                return invalid("sync complete-state alias must not carry commit members");
            }
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

    let materialized_state_alias = load_materialized_sync_state_alias(store, commit_id).await?;
    let state_alias = match materialized_state_alias.clone() {
        Some(alias) => Some(alias),
        None => load_manifest_sync_state_alias(store, commit_id).await?,
    };
    let delta = if materialized_state_alias.is_some() {
        Vec::new()
    } else if state_alias.is_some() {
        load_local_commit_delta_members_with_payloads(store, commit_id).await?
    } else {
        load_commit_delta_members_with_payloads(store, commit_id).await?
    };
    let payloads = materialize_known_change_payloads_in_order(
        delta.iter().map(|member| member.change.clone()),
        ChangeRecordProjection::full(),
    )?;
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
        global_scope: crate::tracked_state::load_published_commit_state_topology(store, commit_id)
            .await?
            .ok_or_else(|| LixError::unknown(format!("sync commit '{commit_id}' has no tracked-state authority")))?
            .global_scope(),
        selected_source_commit_id: selected_source_commit_id.map(|source| source.to_string()),
        state_alias,
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
        let exported = load_sync_commit(
            &read,
            CommitId::parse_lix(&commit_id, "test commit").expect("parse fixture head"),
        )
        .await
        .expect("export fixture commit")
        .expect("fixture commit exists");
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
        let again = load_sync_commit(
            &read,
            CommitId::parse_lix(&exported.commit_id, "test commit").expect("parse fixture commit"),
        )
        .await
        .expect("re-export fixture commit")
        .expect("fixture commit still exists");
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
    async fn checkpoint_exports_bounded_authenticated_state_alias() {
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
        assert!(exported.members.iter().all(|member| member.authored));
        assert!(
            exported.members.is_empty(),
            "the branch checkpoint boundary has no local row mutations"
        );
        assert_eq!(exported.selected_source_commit_id, None);
        let alias = exported
            .state_alias
            .as_ref()
            .expect("checkpoint export must carry its physical state source");
        super::super::validate_blake3_id(&alias.state_root_id, "checkpoint state root")
            .expect("checkpoint root id must be canonical");
        lix.close().await.expect("close checkpoint fixture");
    }

    #[tokio::test]
    async fn materialized_state_alias_sidecar_retires_by_commit_identity() {
        let lix = crate::open_lix().await.expect("open sidecar fixture");
        let commit_id = CommitId::for_test_label("materialized-alias-owner");
        let alias = SyncCommitStateAlias {
            source_commit_id: CommitId::for_test_label("materialized-alias-source").to_string(),
            state_root_id: blake3::hash(b"materialized-alias-root")
                .to_hex()
                .to_string(),
        };
        let adapter = lix.storage_adapter();
        let mut writes = adapter.new_write_set();
        stage_materialized_sync_state_alias(&mut writes, commit_id, &alias)
            .expect("stage sidecar");
        adapter
            .commit_write_set(writes, crate::storage_adapter::StorageWriteOptions::default())
            .await
            .expect("publish sidecar");
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open sidecar read");
        assert_eq!(
            load_materialized_sync_state_alias(&read, commit_id)
                .await
                .expect("load sidecar"),
            Some(alias),
        );
        drop(read);

        let mut writes = adapter.new_write_set();
        stage_delete_materialized_sync_state_alias(&mut writes, commit_id);
        adapter
            .commit_write_set(writes, crate::storage_adapter::StorageWriteOptions::default())
            .await
            .expect("retire sidecar");
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open retired sidecar read");
        assert_eq!(
            load_materialized_sync_state_alias(&read, commit_id)
                .await
                .expect("load retired sidecar"),
            None,
        );
        drop(read);
        lix.close().await.expect("close sidecar fixture");
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
            global_scope: false,
            selected_source_commit_id: None,
            state_alias: None,
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

    #[test]
    fn validation_rejects_non_metadata_only_state_aliases() {
        let commit_id = CommitId::for_test_label("alias-validation");
        let parent = CommitId::for_test_label("alias-parent");
        let source = CommitId::for_test_label("alias-source");
        let mut commit = SyncCommit {
            commit_id: commit_id.to_string(),
            parent_commit_ids: vec![parent.to_string()],
            account_id: crate::ANONYMOUS_ACCOUNT_ID.to_owned(),
            created_at: "2026-08-19T00:00:00Z".to_owned(),
            global_scope: false,
            selected_source_commit_id: None,
            state_alias: Some(SyncCommitStateAlias {
                source_commit_id: source.to_string(),
                state_root_id: blake3::hash(b"alias-root").to_hex().to_string(),
            }),
            members: Vec::new(),
        };
        commit.validate().expect("canonical state alias validates");

        commit.parent_commit_ids.clear();
        assert!(
            commit
                .validate()
                .expect_err("alias without one semantic parent must fail")
                .message
                .contains("exactly one")
        );
        commit.parent_commit_ids = vec![parent.to_string()];
        commit.members.push(SyncCommitMember {
            change_id: crate::changelog::ChangeId::for_test_label("alias-member").to_string(),
            authored: true,
            schema_key: "schema".to_owned(),
            file_id: None,
            row_pk: serde_json::json!([{ "type": "string", "value": "row" }]),
            deleted: false,
            snapshot: Some(serde_json::json!({ "id": "row" })),
            metadata: None,
            row_created_at: "2026-08-19T00:00:00Z".to_owned(),
            row_updated_at: "2026-08-19T00:00:00Z".to_owned(),
            change_account_id: crate::ANONYMOUS_ACCOUNT_ID.to_owned(),
            change_created_at: "2026-08-19T00:00:00Z".to_owned(),
            origin_key: None,
        });
        assert!(
            commit
                .validate()
                .expect_err("alias with members must fail")
                .message
                .contains("must not carry")
        );
    }
}
