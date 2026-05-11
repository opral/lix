use crate::commit_store::{
    Change, ChangeIndexEntry, ChangeLocator, ChangeRef, Commit, CommitDraftRef,
    StagedCommitStoreCommit, StoredCommitRef,
};
use crate::storage::{
    KvGetGroup, KvGetRequest, KvScanRange, KvScanRequest, StorageReader, StorageWriteSet,
};
use crate::LixError;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) const COMMIT_NAMESPACE: &str = "commit_store.commit";
pub(crate) const CHANGE_PACK_NAMESPACE: &str = "commit_store.change_pack";
pub(crate) const MEMBERSHIP_PACK_NAMESPACE: &str = "commit_store.membership_pack";

const SINGLE_PACK_ID: u32 = 0;

pub(crate) fn stage_commit(
    writes: &mut StorageWriteSet,
    commit: CommitDraftRef<'_>,
    authored_changes: Vec<ChangeRef<'_>>,
    adopted_changes: Vec<ChangeLocator>,
) -> Result<StagedCommitStoreCommit, LixError> {
    let stored_commit = StoredCommitRef {
        id: commit.id,
        change_id: commit.change_id,
        parent_ids: commit.parent_ids,
        author_account_ids: commit.author_account_ids,
        created_at: commit.created_at,
        change_pack_count: if authored_changes.is_empty() { 0 } else { 1 },
        membership_pack_count: if adopted_changes.is_empty() { 0 } else { 1 },
    };

    writes.put(
        COMMIT_NAMESPACE,
        commit_key(commit.id),
        crate::commit_store::codec::encode_commit_ref(stored_commit)?,
    );

    let mut authored_locators = Vec::with_capacity(authored_changes.len());
    if !authored_changes.is_empty() {
        writes.put(
            CHANGE_PACK_NAMESPACE,
            pack_key(commit.id, SINGLE_PACK_ID)?,
            crate::commit_store::codec::encode_change_pack(
                commit.id,
                SINGLE_PACK_ID,
                &authored_changes,
            )?,
        );
        for (source_ordinal, change) in authored_changes.iter().enumerate() {
            authored_locators.push(ChangeLocator {
                source_commit_id: commit.id.to_string(),
                source_pack_id: SINGLE_PACK_ID,
                source_ordinal: u32::try_from(source_ordinal).map_err(|_| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "commit-store change pack ordinal exceeds u32",
                    )
                })?,
                change_id: change.id.to_string(),
            });
        }
    }

    if !adopted_changes.is_empty() {
        writes.put(
            MEMBERSHIP_PACK_NAMESPACE,
            pack_key(commit.id, SINGLE_PACK_ID)?,
            crate::commit_store::codec::encode_membership_pack(
                commit.id,
                SINGLE_PACK_ID,
                adopted_changes.iter().map(ChangeLocator::as_ref),
            )?,
        );
    }

    Ok(StagedCommitStoreCommit {
        authored_locators,
        adopted_locators: adopted_changes,
    })
}

pub(crate) async fn load_commit(
    store: &mut (impl StorageReader + ?Sized),
    commit_id: &str,
) -> Result<Option<Commit>, LixError> {
    let Some(bytes) = get_one(store, COMMIT_NAMESPACE, commit_key(commit_id)).await? else {
        return Ok(None);
    };
    crate::commit_store::codec::decode_commit(&bytes).map(Some)
}

pub(crate) async fn scan_commits(
    store: &mut (impl StorageReader + ?Sized),
) -> Result<Vec<Commit>, LixError> {
    let page = store
        .scan_values(KvScanRequest {
            namespace: COMMIT_NAMESPACE.to_string(),
            range: KvScanRange::prefix(Vec::new()),
            after: None,
            limit: usize::MAX,
        })
        .await?;
    page.values
        .iter()
        .map(|bytes| crate::commit_store::codec::decode_commit(bytes))
        .collect()
}

pub(crate) async fn load_change_pack(
    store: &mut (impl StorageReader + ?Sized),
    commit_id: &str,
    pack_id: u32,
) -> Result<Option<Vec<Change>>, LixError> {
    let Some(bytes) = get_one(store, CHANGE_PACK_NAMESPACE, pack_key(commit_id, pack_id)?).await?
    else {
        return Ok(None);
    };
    let (stored_commit_id, stored_pack_id, changes) =
        crate::commit_store::codec::decode_change_pack(&bytes)?;
    ensure_pack_identity(
        "change pack",
        commit_id,
        pack_id,
        &stored_commit_id,
        stored_pack_id,
    )?;
    Ok(Some(changes))
}

pub(crate) async fn load_membership_pack(
    store: &mut (impl StorageReader + ?Sized),
    commit_id: &str,
    pack_id: u32,
) -> Result<Option<Vec<ChangeLocator>>, LixError> {
    let Some(bytes) = get_one(
        store,
        MEMBERSHIP_PACK_NAMESPACE,
        pack_key(commit_id, pack_id)?,
    )
    .await?
    else {
        return Ok(None);
    };
    let (stored_commit_id, stored_pack_id, members) =
        crate::commit_store::codec::decode_membership_pack(&bytes)?;
    ensure_pack_identity(
        "membership pack",
        commit_id,
        pack_id,
        &stored_commit_id,
        stored_pack_id,
    )?;
    Ok(Some(members))
}

pub(crate) async fn load_change_index_entries(
    store: &mut (impl StorageReader + ?Sized),
    change_ids: &[String],
) -> Result<Vec<Option<ChangeIndexEntry>>, LixError> {
    if change_ids.is_empty() {
        return Ok(Vec::new());
    }

    let mut unresolved = change_ids.iter().cloned().collect::<BTreeSet<_>>();
    let mut entries_by_change_id = BTreeMap::new();
    let commits = scan_commits(store).await?;
    for commit in commits {
        if unresolved.remove(&commit.change_id) {
            entries_by_change_id.insert(
                commit.change_id.clone(),
                ChangeIndexEntry::CommitHeader {
                    commit_id: commit.id.clone(),
                    change_id: commit.change_id.clone(),
                },
            );
        }
        if unresolved.is_empty() {
            break;
        }

        for pack_id in 0..commit.change_pack_count {
            let Some(changes) = load_change_pack(store, &commit.id, pack_id).await? else {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "commit-store missing change pack ({}, {pack_id})",
                        commit.id
                    ),
                ));
            };
            for (source_ordinal, change) in changes.iter().enumerate() {
                if !unresolved.remove(&change.id) {
                    continue;
                }
                entries_by_change_id.insert(
                    change.id.clone(),
                    ChangeIndexEntry::PackedChange {
                        locator: ChangeLocator {
                            source_commit_id: commit.id.clone(),
                            source_pack_id: pack_id,
                            source_ordinal: u32::try_from(source_ordinal).map_err(|_| {
                                LixError::new(
                                    LixError::CODE_INTERNAL_ERROR,
                                    "commit-store change pack ordinal exceeds u32",
                                )
                            })?,
                            change_id: change.id.clone(),
                        },
                    },
                );
                if unresolved.is_empty() {
                    break;
                }
            }
            if unresolved.is_empty() {
                break;
            }
        }
        if unresolved.is_empty() {
            break;
        }
    }

    Ok(change_ids
        .iter()
        .map(|change_id| entries_by_change_id.get(change_id).cloned())
        .collect())
}

async fn get_one(
    store: &mut (impl StorageReader + ?Sized),
    namespace: &str,
    key: Vec<u8>,
) -> Result<Option<Vec<u8>>, LixError> {
    Ok(store
        .get_values(KvGetRequest {
            groups: vec![KvGetGroup {
                namespace: namespace.to_string(),
                keys: vec![key],
            }],
        })
        .await?
        .groups
        .into_iter()
        .next()
        .and_then(|group| group.single_value_owned()))
}

fn ensure_pack_identity(
    label: &str,
    expected_commit_id: &str,
    expected_pack_id: u32,
    actual_commit_id: &str,
    actual_pack_id: u32,
) -> Result<(), LixError> {
    if actual_commit_id != expected_commit_id || actual_pack_id != expected_pack_id {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "commit-store {label} identity mismatch: expected ({expected_commit_id}, {expected_pack_id}), got ({actual_commit_id}, {actual_pack_id})"
            ),
        ));
    }
    Ok(())
}

fn commit_key(commit_id: &str) -> Vec<u8> {
    commit_id.as_bytes().to_vec()
}

fn pack_key(commit_id: &str, pack_id: u32) -> Result<Vec<u8>, LixError> {
    let commit_id_len = u32::try_from(commit_id.len()).map_err(|_| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "commit-store pack key commit id exceeds u32 length",
        )
    })?;
    let mut key = Vec::with_capacity(8 + commit_id.len());
    key.extend_from_slice(&commit_id_len.to_be_bytes());
    key.extend_from_slice(commit_id.as_bytes());
    key.extend_from_slice(&pack_id.to_be_bytes());
    Ok(key)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::backend::testing::UnitTestBackend;
    use crate::commit_store::CommitDraftRef;
    use crate::entity_identity::EntityIdentity;
    use crate::json_store::JsonRef;
    use crate::storage::{StorageContext, StorageWriteTransaction};

    use super::*;

    #[tokio::test]
    async fn stage_commit_writes_all_commit_store_namespaces() {
        let storage = StorageContext::new(Arc::new(UnitTestBackend::new()));
        let mut tx = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        let mut writes = StorageWriteSet::new();
        let commit = test_commit();
        let change = test_change("change-1");
        let adopted = ChangeLocator {
            source_commit_id: "source-commit".to_string(),
            source_pack_id: 3,
            source_ordinal: 7,
            change_id: "adopted-change".to_string(),
        };

        let staged = stage_commit(
            &mut writes,
            CommitDraftRef {
                id: &commit.id,
                change_id: &commit.change_id,
                parent_ids: &commit.parent_ids,
                author_account_ids: &commit.author_account_ids,
                created_at: &commit.created_at,
            },
            vec![change.as_ref()],
            vec![adopted.clone()],
        )
        .expect("commit should stage");
        writes
            .apply(&mut tx.as_mut())
            .await
            .expect("writes should apply");
        tx.commit().await.expect("commit should succeed");

        assert_eq!(
            staged.authored_locators,
            vec![ChangeLocator {
                source_commit_id: "commit-1".to_string(),
                source_pack_id: 0,
                source_ordinal: 0,
                change_id: "change-1".to_string(),
            }]
        );
        assert_eq!(staged.adopted_locators, vec![adopted.clone()]);

        let mut reader = storage.clone();
        assert_eq!(
            load_commit(&mut reader, "commit-1")
                .await
                .expect("commit should load"),
            Some(commit)
        );
        assert_eq!(
            load_change_pack(&mut reader, "commit-1", 0)
                .await
                .expect("change pack should load"),
            Some(vec![change])
        );
        assert_eq!(
            load_membership_pack(&mut reader, "commit-1", 0)
                .await
                .expect("membership pack should load"),
            Some(vec![adopted])
        );

        let index_entries = load_change_index_entries(
            &mut reader,
            &["commit-change-1".to_string(), "change-1".to_string()],
        )
        .await
        .expect("index entries should load");
        assert_eq!(
            index_entries,
            vec![
                Some(ChangeIndexEntry::CommitHeader {
                    commit_id: "commit-1".to_string(),
                    change_id: "commit-change-1".to_string(),
                }),
                Some(ChangeIndexEntry::PackedChange {
                    locator: ChangeLocator {
                        source_commit_id: "commit-1".to_string(),
                        source_pack_id: 0,
                        source_ordinal: 0,
                        change_id: "change-1".to_string(),
                    },
                }),
            ]
        );
    }

    fn test_commit() -> Commit {
        Commit {
            id: "commit-1".to_string(),
            change_id: "commit-change-1".to_string(),
            parent_ids: vec!["parent-1".to_string()],
            author_account_ids: Vec::new(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            change_pack_count: 1,
            membership_pack_count: 1,
        }
    }

    fn test_change(id: &str) -> Change {
        Change {
            id: id.to_string(),
            entity_id: EntityIdentity::single("entity-1"),
            schema_key: "test_schema".to_string(),
            file_id: None,
            snapshot_ref: Some(JsonRef::from_hash_bytes([1; 32])),
            metadata_ref: None,
            created_at: "2026-01-01T00:00:00Z".to_string(),
        }
    }
}
