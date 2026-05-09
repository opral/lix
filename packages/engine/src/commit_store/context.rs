use crate::commit_store::{
    Change, ChangeBorrowed, ChangeIndexEntry, ChangeLocator, ChangeScanRequest, Commit,
    CommitDraftBorrowed, StagedCommitStoreCommit,
};
use crate::storage::{StorageReader, StorageWriteSet};
use crate::LixError;
use std::collections::{BTreeMap, BTreeSet};
use tokio::sync::Mutex;

/// Canonical physical storage boundary for commits and their changes.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct CommitStoreContext;

impl CommitStoreContext {
    pub(crate) fn new() -> Self {
        Self
    }

    /// Creates a commit-store writer over read visibility and a pending write set.
    pub(crate) fn writer<'a, S>(
        &self,
        store: &'a mut S,
        writes: &'a mut StorageWriteSet,
    ) -> CommitStoreWriter<'a, S>
    where
        S: StorageReader + ?Sized,
    {
        CommitStoreWriter { store, writes }
    }

    /// Creates a commit-store reader over a storage snapshot or transaction.
    pub(crate) fn reader<S>(&self, store: S) -> CommitStoreReader<S>
    where
        S: StorageReader,
    {
        CommitStoreReader {
            store: Mutex::new(store),
        }
    }
}

/// Commit-store reader over a storage snapshot or transaction.
pub(crate) struct CommitStoreReader<S> {
    store: Mutex<S>,
}

impl<S> CommitStoreReader<S>
where
    S: StorageReader,
{
    pub(crate) async fn load_change_index_entries(
        &self,
        change_ids: &[String],
    ) -> Result<Vec<Option<crate::commit_store::ChangeIndexEntry>>, LixError> {
        crate::commit_store::storage::load_change_index_entries(
            &mut *self.store.lock().await,
            change_ids,
        )
        .await
    }

    pub(crate) async fn load_commit(
        &self,
        commit_id: &str,
    ) -> Result<Option<crate::commit_store::Commit>, LixError> {
        crate::commit_store::storage::load_commit(&mut *self.store.lock().await, commit_id).await
    }

    pub(crate) async fn scan_commits(&self) -> Result<Vec<crate::commit_store::Commit>, LixError> {
        crate::commit_store::storage::scan_commits(&mut *self.store.lock().await).await
    }

    pub(crate) async fn load_change_pack(
        &self,
        commit_id: &str,
        pack_id: u32,
    ) -> Result<Option<Vec<crate::commit_store::Change>>, LixError> {
        crate::commit_store::storage::load_change_pack(
            &mut *self.store.lock().await,
            commit_id,
            pack_id,
        )
        .await
    }

    pub(crate) async fn load_membership_pack(
        &self,
        commit_id: &str,
        pack_id: u32,
    ) -> Result<Option<Vec<crate::commit_store::ChangeLocator>>, LixError> {
        crate::commit_store::storage::load_membership_pack(
            &mut *self.store.lock().await,
            commit_id,
            pack_id,
        )
        .await
    }

    pub(crate) async fn load_changes(
        &self,
        change_ids: &[String],
    ) -> Result<Vec<Option<crate::commit_store::Change>>, LixError> {
        if change_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut store = self.store.lock().await;
        let entries =
            crate::commit_store::storage::load_change_index_entries(&mut *store, change_ids)
                .await?;
        let mut changes = Vec::with_capacity(entries.len());
        let mut commits_by_id = BTreeMap::new();
        let mut packs_by_locator = BTreeMap::new();
        for (change_id, entry) in change_ids.iter().zip(entries) {
            changes.push(match entry {
                Some(ChangeIndexEntry::CommitHeader { commit_id, .. }) => {
                    if !commits_by_id.contains_key(&commit_id) {
                        let commit =
                            crate::commit_store::storage::load_commit(&mut *store, &commit_id)
                                .await?;
                        commits_by_id.insert(commit_id.clone(), commit);
                    }
                    commits_by_id
                        .get(&commit_id)
                        .cloned()
                        .flatten()
                        .map(commit_header_change)
                }
                Some(ChangeIndexEntry::PackedChange { locator }) => {
                    Some(
                        load_change_by_locator_cached(
                            &mut *store,
                            &mut packs_by_locator,
                            &locator,
                            change_id,
                        )
                        .await?,
                    )
                }
                None => None,
            });
        }
        Ok(changes)
    }

    pub(crate) async fn load_commit_changes(
        &self,
        commit_id: &str,
    ) -> Result<Vec<crate::commit_store::Change>, LixError> {
        let mut store = self.store.lock().await;
        let Some(commit) =
            crate::commit_store::storage::load_commit(&mut *store, commit_id).await?
        else {
            return Ok(Vec::new());
        };

        let mut changes = Vec::new();
        for pack_id in 0..commit.change_pack_count {
            let Some(mut pack_changes) =
                crate::commit_store::storage::load_change_pack(&mut *store, commit_id, pack_id)
                    .await?
            else {
                return Err(missing_pack_error("change", commit_id, pack_id));
            };
            changes.append(&mut pack_changes);
        }

        for pack_id in 0..commit.membership_pack_count {
            let Some(locators) =
                crate::commit_store::storage::load_membership_pack(&mut *store, commit_id, pack_id)
                    .await?
            else {
                return Err(missing_pack_error("membership", commit_id, pack_id));
            };
            for locator in locators {
                let change =
                    load_change_by_locator(&mut *store, &locator, &locator.change_id).await?;
                changes.push(change);
            }
        }

        Ok(changes)
    }

    pub(crate) async fn scan_changes(
        &self,
        request: &ChangeScanRequest,
    ) -> Result<Vec<crate::commit_store::Change>, LixError> {
        scan_changes_from_commit_store(&mut *self.store.lock().await, request).await
    }
}

/// Commit-store writer over read visibility and a transaction-local write set.
pub(crate) struct CommitStoreWriter<'a, S: ?Sized> {
    store: &'a mut S,
    writes: &'a mut StorageWriteSet,
}

struct PendingCommitDraft<'a> {
    commit: CommitDraftBorrowed<'a>,
    authored_changes: Vec<ChangeBorrowed<'a>>,
    adopted_changes: Vec<ChangeBorrowed<'a>>,
}

impl<S> CommitStoreWriter<'_, S>
where
    S: StorageReader + ?Sized,
{
    /// Validates and stages canonical commit-store writes for complete commits.
    ///
    /// Callers provide logical commit facts and borrowed change facts. The
    /// commit store owns change-id uniqueness, adoption resolution, pack
    /// locators, and physical namespace writes.
    pub(crate) async fn stage_commit_draft<'a>(
        &mut self,
        commit: CommitDraftBorrowed<'a>,
        authored_changes: Vec<ChangeBorrowed<'a>>,
        adopted_changes: Vec<ChangeBorrowed<'a>>,
    ) -> Result<StagedCommitStoreCommit, LixError> {
        let mut staged = self
            .stage_commit_drafts([(commit, authored_changes, adopted_changes)])
            .await?;
        staged.pop().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "commit-store staged no result for one commit draft",
            )
        })
    }

    /// Validates and stages multiple commit drafts as one commit-store batch.
    pub(crate) async fn stage_commit_drafts<'a>(
        &mut self,
        commits: impl IntoIterator<
            Item = (
                CommitDraftBorrowed<'a>,
                Vec<ChangeBorrowed<'a>>,
                Vec<ChangeBorrowed<'a>>,
            ),
        >,
    ) -> Result<Vec<StagedCommitStoreCommit>, LixError> {
        let commits = commits
            .into_iter()
            .map(
                |(commit, authored_changes, adopted_changes)| PendingCommitDraft {
                    commit,
                    authored_changes,
                    adopted_changes,
                },
            )
            .collect::<Vec<_>>();
        let adopted_locators = validate_stage_commits(self.store, &commits).await?;
        let mut staged = Vec::with_capacity(commits.len());
        for commit in commits {
            let mut adopted_changes = Vec::with_capacity(commit.adopted_changes.len());
            for change in &commit.adopted_changes {
                let Some(locator) = adopted_locators.get(change.id) else {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!(
                            "validated adopted commit-store change id '{}' has no locator",
                            change.id
                        ),
                    ));
                };
                adopted_changes.push(locator.clone());
            }
            staged.push(crate::commit_store::storage::stage_commit(
                self.writes,
                commit.commit,
                commit.authored_changes,
                adopted_changes,
            )?);
        }
        Ok(staged)
    }
}

async fn validate_stage_commits<'a>(
    store: &mut (impl StorageReader + ?Sized),
    commits: &[PendingCommitDraft<'a>],
) -> Result<BTreeMap<&'a str, ChangeLocator>, LixError> {
    validate_new_changes_absent(store, commits).await?;
    validate_adopted_changes_present(store, commits).await
}

async fn scan_changes_from_commit_store(
    store: &mut (impl StorageReader + ?Sized),
    request: &ChangeScanRequest,
) -> Result<Vec<Change>, LixError> {
    let limit = request.limit.unwrap_or(usize::MAX);
    let commits = crate::commit_store::storage::scan_commits(store).await?;
    let mut changes = Vec::new();
    for commit in commits {
        if changes.len() >= limit {
            break;
        }
        for pack_id in 0..commit.change_pack_count {
            if changes.len() >= limit {
                break;
            }
            let Some(mut pack_changes) =
                crate::commit_store::storage::load_change_pack(store, &commit.id, pack_id).await?
            else {
                return Err(missing_pack_error("change", &commit.id, pack_id));
            };
            let remaining = limit - changes.len();
            if pack_changes.len() > remaining {
                pack_changes.truncate(remaining);
            }
            changes.extend(pack_changes);
        }
        if changes.len() < limit {
            changes.push(commit_header_change(commit));
        }
    }
    Ok(changes)
}

async fn load_change_by_locator(
    store: &mut (impl StorageReader + ?Sized),
    locator: &ChangeLocator,
    expected_change_id: &str,
) -> Result<Change, LixError> {
    let Some(changes) = crate::commit_store::storage::load_change_pack(
        store,
        &locator.source_commit_id,
        locator.source_pack_id,
    )
    .await?
    else {
        return Err(missing_pack_error(
            "change",
            &locator.source_commit_id,
            locator.source_pack_id,
        ));
    };
    let change = changes
        .get(usize::try_from(locator.source_ordinal).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "commit-store change locator ordinal does not fit usize",
            )
        })?)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "commit-store change locator for '{}' points past pack '{}' in commit '{}'",
                    expected_change_id, locator.source_pack_id, locator.source_commit_id
                ),
            )
        })?;
    if change.id != expected_change_id || change.id != locator.change_id {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "commit-store change locator expected '{}' but found '{}'",
                expected_change_id, change.id
            ),
        ));
    }
    Ok(change.clone())
}

async fn load_change_by_locator_cached(
    store: &mut (impl StorageReader + ?Sized),
    packs_by_locator: &mut BTreeMap<(String, u32), Vec<Change>>,
    locator: &ChangeLocator,
    expected_change_id: &str,
) -> Result<Change, LixError> {
    let key = (locator.source_commit_id.clone(), locator.source_pack_id);
    if !packs_by_locator.contains_key(&key) {
        let Some(changes) = crate::commit_store::storage::load_change_pack(
            store,
            &locator.source_commit_id,
            locator.source_pack_id,
        )
        .await?
        else {
            return Err(missing_pack_error(
                "change",
                &locator.source_commit_id,
                locator.source_pack_id,
            ));
        };
        packs_by_locator.insert(key.clone(), changes);
    }
    let changes = packs_by_locator.get(&key).ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "commit-store change pack cache lost a loaded pack",
        )
    })?;
    let change = changes
        .get(usize::try_from(locator.source_ordinal).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "commit-store change locator ordinal does not fit usize",
            )
        })?)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "commit-store change locator for '{}' points past pack '{}' in commit '{}'",
                    expected_change_id, locator.source_pack_id, locator.source_commit_id
                ),
            )
        })?;
    if change.id != expected_change_id || change.id != locator.change_id {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "commit-store change locator expected '{}' but found '{}'",
                expected_change_id, change.id
            ),
        ));
    }
    Ok(change.clone())
}

fn commit_header_change(commit: Commit) -> Change {
    Change {
        id: commit.change_id,
        entity_id: crate::entity_identity::EntityIdentity::single(commit.id),
        schema_key: "lix_commit".to_string(),
        file_id: None,
        snapshot_ref: None,
        metadata_ref: None,
        created_at: commit.created_at,
    }
}

fn missing_pack_error(label: &str, commit_id: &str, pack_id: u32) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("commit-store missing {label} pack ({commit_id}, {pack_id})"),
    )
}

async fn validate_new_changes_absent<'a>(
    store: &mut (impl StorageReader + ?Sized),
    commits: &[PendingCommitDraft<'a>],
) -> Result<(), LixError> {
    let mut change_ids = Vec::new();
    let mut seen_change_ids = BTreeSet::new();
    for commit in commits {
        if !seen_change_ids.insert(commit.commit.change_id) {
            return Err(duplicate_change_id_error(commit.commit.change_id));
        }
        change_ids.push(commit.commit.change_id.to_string());
        for change in &commit.authored_changes {
            if !seen_change_ids.insert(change.id) {
                return Err(duplicate_change_id_error(change.id));
            }
            change_ids.push(change.id.to_string());
        }
    }

    let reader = CommitStoreContext::new().reader(&mut *store);
    let existing_changes = reader.load_change_index_entries(&change_ids).await?;
    for (change_id, existing) in change_ids.iter().zip(existing_changes) {
        if existing.is_some() {
            return Err(LixError::new(
                LixError::CODE_UNIQUE,
                format!("commit-store change id '{}' already exists", change_id),
            ));
        }
    }
    Ok(())
}

async fn validate_adopted_changes_present<'a>(
    store: &mut (impl StorageReader + ?Sized),
    commits: &[PendingCommitDraft<'a>],
) -> Result<BTreeMap<&'a str, ChangeLocator>, LixError> {
    let mut expected_changes = Vec::new();
    let mut seen_change_ids = BTreeSet::new();
    for commit in commits {
        for change in &commit.adopted_changes {
            if !seen_change_ids.insert(change.id) {
                return Err(LixError::new(
                    LixError::CODE_UNIQUE,
                    format!(
                        "adopted commit-store change id '{}' appears more than once in the same transaction",
                        change.id
                    ),
                ));
            }
            expected_changes.push(*change);
        }
    }
    if expected_changes.is_empty() {
        return Ok(BTreeMap::new());
    }

    let change_ids = expected_changes
        .iter()
        .map(|change| change.id.to_string())
        .collect::<Vec<_>>();
    let reader = CommitStoreContext::new().reader(&mut *store);
    let existing_entries = reader.load_change_index_entries(&change_ids).await?;
    let mut locators_by_change_id = BTreeMap::new();
    for (expected, existing) in expected_changes.into_iter().zip(existing_entries) {
        match existing {
            Some(ChangeIndexEntry::PackedChange { locator }) => {
                let existing_change = load_packed_change(&reader, &locator, expected.id).await?;
                if !change_matches_borrowed(&existing_change, expected) {
                    let entity_id = existing_change
                        .entity_id
                        .as_json_array_text()
                        .unwrap_or_else(|_| "<invalid entity_id>".to_string());
                    return Err(LixError::new(
                        LixError::CODE_UNIQUE,
                        format!(
                            "adopted commit-store change id '{}' exists with different content for schema '{}' entity '{}'",
                            expected.id, existing_change.schema_key, entity_id
                        ),
                    ));
                }
                locators_by_change_id.insert(expected.id, locator);
            }
            Some(ChangeIndexEntry::CommitHeader { .. }) => {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "adopted commit-store change id '{}' resolves to a commit header, not a packed state change",
                        expected.id
                    ),
                ));
            }
            None => {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "adopted commit-store change id '{}' does not exist",
                        expected.id
                    ),
                ));
            }
        }
    }
    Ok(locators_by_change_id)
}

async fn load_packed_change<S>(
    reader: &CommitStoreReader<S>,
    locator: &ChangeLocator,
    expected_change_id: &str,
) -> Result<Change, LixError>
where
    S: StorageReader,
{
    let pack = reader
        .load_change_pack(&locator.source_commit_id, locator.source_pack_id)
        .await?
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "commit-store change pack '{}:{}' for change '{}' is missing",
                    locator.source_commit_id, locator.source_pack_id, expected_change_id
                ),
            )
        })?;
    let change = pack
        .get(usize::try_from(locator.source_ordinal).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "commit-store change locator ordinal exceeds usize",
            )
        })?)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "commit-store change locator '{}' points past pack length",
                    expected_change_id
                ),
            )
        })?
        .clone();
    if change.id != expected_change_id {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "commit-store change locator expected '{}' but loaded '{}'",
                expected_change_id, change.id
            ),
        ));
    }
    Ok(change)
}

fn change_matches_borrowed(change: &Change, expected: ChangeBorrowed<'_>) -> bool {
    change.id == expected.id
        && &change.entity_id == expected.entity_id
        && change.schema_key == expected.schema_key
        && change.file_id.as_deref() == expected.file_id
        && change.snapshot_ref.as_ref() == expected.snapshot_ref
        && change.metadata_ref.as_ref() == expected.metadata_ref
        && change.created_at == expected.created_at
}

fn duplicate_change_id_error(change_id: &str) -> LixError {
    LixError::new(
        LixError::CODE_UNIQUE,
        format!(
            "commit-store change id '{}' appears more than once in the same transaction",
            change_id
        ),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::backend::testing::UnitTestBackend;
    use crate::commit_store::{
        ChangeIndexEntry, ChangeLocator, CommitDraftBorrowed, CommitStoreContext,
    };
    use crate::entity_identity::EntityIdentity;
    use crate::json_store::JsonRef;
    use crate::storage::{StorageContext, StorageWriteSet, StorageWriteTransaction};

    use super::*;

    #[tokio::test]
    async fn load_changes_materializes_commit_header_and_packed_change() {
        let storage = StorageContext::new(Arc::new(UnitTestBackend::new()));
        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        let mut writes = StorageWriteSet::new();
        let parent_ids = vec!["parent-1".to_string()];
        let author_account_ids = vec!["author-1".to_string()];
        let commit_id = "commit-1".to_string();
        let commit_change_id = "commit-change-1".to_string();
        let change_set_id = "change-set-1".to_string();
        let authored_change = test_change("change-1");

        CommitStoreContext::new()
            .writer(transaction.as_mut(), &mut writes)
            .stage_commit_draft(
                CommitDraftBorrowed {
                    id: &commit_id,
                    change_id: &commit_change_id,
                    change_set_id: &change_set_id,
                    parent_ids: &parent_ids,
                    author_account_ids: &author_account_ids,
                    created_at: "2026-01-01T00:00:00Z",
                },
                vec![authored_change.as_borrowed()],
                Vec::new(),
            )
            .await
            .expect("commit should stage");
        writes
            .apply(&mut transaction.as_mut())
            .await
            .expect("writes should apply");
        transaction.commit().await.expect("commit should persist");

        let reader = CommitStoreContext::new().reader(storage.clone());
        let index_entries = reader
            .load_change_index_entries(&[
                commit_change_id.clone(),
                authored_change.id.clone(),
                "missing-change".to_string(),
            ])
            .await
            .expect("index entries should load");
        assert_eq!(
            index_entries,
            vec![
                Some(ChangeIndexEntry::CommitHeader {
                    commit_id: commit_id.clone(),
                    change_id: commit_change_id.clone(),
                }),
                Some(ChangeIndexEntry::PackedChange {
                    locator: ChangeLocator {
                        source_commit_id: commit_id.clone(),
                        source_pack_id: 0,
                        source_ordinal: 0,
                        change_id: authored_change.id.clone(),
                    },
                }),
                None,
            ]
        );

        let changes = reader
            .load_changes(&[
                commit_change_id.clone(),
                authored_change.id.clone(),
                "missing-change".to_string(),
            ])
            .await
            .expect("changes should load");
        assert_eq!(changes.len(), 3);

        let header_change = changes[0]
            .as_ref()
            .expect("commit-header change should materialize");
        assert_eq!(header_change.id, commit_change_id);
        assert_eq!(header_change.entity_id, EntityIdentity::single(&commit_id));
        assert_eq!(header_change.schema_key, "lix_commit");
        assert_eq!(header_change.file_id, None);
        assert_eq!(header_change.snapshot_ref, None);
        assert_eq!(header_change.metadata_ref, None);
        assert_eq!(header_change.created_at, "2026-01-01T00:00:00Z");

        assert_eq!(
            changes[1]
                .as_ref()
                .expect("packed change should decode from change pack"),
            &authored_change
        );
        assert_eq!(changes[2], None);
    }

    #[tokio::test]
    async fn load_commit_changes_returns_equivalent_authored_and_adopted_changes() {
        let storage = StorageContext::new(Arc::new(UnitTestBackend::new()));
        let authored_change = test_change("shared-change-1");

        stage_test_commit(
            storage.clone(),
            "source-commit",
            "source-commit-change",
            "source-change-set",
            vec![authored_change.as_borrowed()],
            Vec::new(),
        )
        .await;
        stage_test_commit(
            storage.clone(),
            "adopting-commit",
            "adopting-commit-change",
            "adopting-change-set",
            Vec::new(),
            vec![authored_change.as_borrowed()],
        )
        .await;

        let reader = CommitStoreContext::new().reader(storage.clone());
        let source_changes = reader
            .load_commit_changes("source-commit")
            .await
            .expect("source commit changes should load");
        let adopting_changes = reader
            .load_commit_changes("adopting-commit")
            .await
            .expect("adopting commit changes should load");

        assert_eq!(source_changes, vec![authored_change.clone()]);
        assert_eq!(adopting_changes, source_changes);
        assert_eq!(
            reader
                .load_membership_pack("adopting-commit", 0)
                .await
                .expect("membership pack should load"),
            Some(vec![ChangeLocator {
                source_commit_id: "source-commit".to_string(),
                source_pack_id: 0,
                source_ordinal: 0,
                change_id: authored_change.id.clone(),
            }])
        );
    }

    async fn stage_test_commit(
        storage: StorageContext,
        commit_id: &str,
        commit_change_id: &str,
        change_set_id: &str,
        authored_changes: Vec<ChangeBorrowed<'_>>,
        adopted_changes: Vec<ChangeBorrowed<'_>>,
    ) {
        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        let mut writes = StorageWriteSet::new();
        let parent_ids = Vec::new();
        let author_account_ids = Vec::new();

        CommitStoreContext::new()
            .writer(transaction.as_mut(), &mut writes)
            .stage_commit_draft(
                CommitDraftBorrowed {
                    id: commit_id,
                    change_id: commit_change_id,
                    change_set_id,
                    parent_ids: &parent_ids,
                    author_account_ids: &author_account_ids,
                    created_at: "2026-01-01T00:00:00Z",
                },
                authored_changes,
                adopted_changes,
            )
            .await
            .expect("commit should stage");
        writes
            .apply(&mut transaction.as_mut())
            .await
            .expect("writes should apply");
        transaction.commit().await.expect("commit should persist");
    }

    fn test_change(id: &str) -> Change {
        Change {
            id: id.to_string(),
            entity_id: EntityIdentity::single("entity-1"),
            schema_key: "test_schema".to_string(),
            file_id: Some("file-1".to_string()),
            snapshot_ref: Some(JsonRef::from_hash_bytes([1; 32])),
            metadata_ref: Some(JsonRef::from_hash_bytes([2; 32])),
            created_at: "2026-01-02T00:00:00Z".to_string(),
        }
    }
}
