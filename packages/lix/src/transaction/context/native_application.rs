//! Filesystem ownership changes that accompany native incoming row application.
use super::*;
use crate::filesystem::{FileDeleteInput, plan_file_delete};

impl<S: Storage + Clone + Send + Sync + 'static> Transaction<S> {
    /// Native file lifecycle is resolved before ordinary row reconciliation.
    /// A later file edit revives its complete captured source incarnation;
    /// a later delete uses the current target's ordinary deletion closure.
    pub(crate) async fn prepare_incoming_file_lifecycle(
        &mut self,
        analysis: &crate::session::MergeAnalysis,
    ) -> Result<Option<crate::session::MergeAnalysis>, LixError> {
        const MAX_ROWS: usize = 65_536;
        let affected = analysis
            .source_diff
            .entries
            .iter()
            .filter_map(|entry| entry.identity.file_id().map(str::to_owned))
            .collect::<BTreeSet<_>>();
        if affected.is_empty() {
            return Ok(None);
        }
        let keys = affected
            .iter()
            .map(|id| {
                Ok(TrackedStateKey {
                    schema_key: "lix_file_descriptor".into(),
                    file_id: Some(id.clone()),
                    row_pk: native_file_uuid_key(id)?,
                })
            })
            .collect::<Result<Vec<_>, LixError>>()?;
        let source_id = analysis.commits.source_commit_id.to_string();
        let target_id = analysis.commits.target_commit_id.to_string();
        // The immutable native tree orders schema before file identity. Use
        // the already validated transaction catalog to issue finite
        // (schema,file) prefixes, never an all-schema file-only tree walk.
        let mut source_schemas = self
            .sql_schema_snapshot
            .plans()
            .map(|plan| plan.key.schema_key.to_string())
            .collect::<Vec<_>>();
        // SQL catalog plans need not expose canonical storage-only schemas.
        // These exact prefixes carry the file descriptor, bytes, and plugin
        // incarnation/reservation metadata required by a restored file.
        source_schemas.extend([
            "lix_file_descriptor".to_owned(),
            "lix_binary_blob_ref".to_owned(),
            "lix_key_value".to_owned(),
        ]);
        source_schemas.sort();
        source_schemas.dedup();
        let mut reader = self.tracked_state_reader().await?;
        let source = reader
            .load_projected_batch_at_commit(&source_id, &keys, &ChangeRecordProjection::full())
            .await?;
        let target = reader
            .load_projected_batch_at_commit(&target_id, &keys, &ChangeRecordProjection::full())
            .await?;
        let mut deletes = BTreeSet::new();
        let mut restores = BTreeSet::new();
        for (slot, id) in affected.iter().enumerate() {
            let source_row = source.row(slot);
            let target_row = target.row(slot);
            let incoming_delete = analysis.source_diff.entries.iter().any(|entry| {
                entry.identity.schema_key() == "lix_file_descriptor"
                    && entry.identity.file_id() == Some(id.as_str())
                    && entry.after.as_ref().is_some_and(|row| row.deleted)
            });
            if incoming_delete {
                deletes.insert(id.clone());
            } else if source_row.is_some_and(|row| !row.deleted())
                && target_row.is_none_or(|row| row.deleted())
            {
                restores.insert(id.clone());
            }
        }
        if deletes.is_empty() && restores.is_empty() {
            return Ok(None);
        }
        let handled = deletes.union(&restores).cloned().collect::<BTreeSet<_>>();
        let plan = analysis
            .merge_plan()
            .expect("file lifecycle requires native merge plan");
        let incoming_directory_keys = plan
            .picks
            .iter()
            .map(|pick| &pick.identity)
            .chain(plan.conflicts.iter().map(|conflict| &conflict.identity))
            .filter(|identity| {
                identity.schema_key() == "lix_directory_descriptor" && identity.file_id().is_none()
            })
            .map(|identity| identity.row_pk().clone())
            .collect::<BTreeSet<_>>();
        let mut restored = BTreeMap::new();
        for id in &restores {
            let rows = reader
                .scan_batch_at_commit_page(
                    &source_id,
                    &crate::tracked_state::TrackedStateScanRequest {
                        filter: TrackedStateFilter {
                            schema_keys: source_schemas.clone(),
                            file_ids: vec![NullableKeyFilter::Value(id.clone())],
                            include_tombstones: true,
                            ..Default::default()
                        },
                        read_columns: crate::tracked_state::TrackedStateReadColumns {
                            columns: Vec::new(),
                        },
                        limit: Some(MAX_ROWS + 1),
                    },
                    None,
                )
                .await?;
            if rows.len() > MAX_ROWS || restored.len().saturating_add(rows.len()) > MAX_ROWS {
                return Err(LixError::new(
                    "LIX_PARTIAL_MERGE_PREPARATION_LIMIT",
                    "captured file resurrection exceeds the bounded row closure",
                ));
            }
            for slot in 0..rows.len() {
                let row = rows.row(slot);
                let pick = native_file_lifecycle_pick(row)?;
                restored.insert(
                    TrackedStateKey {
                        schema_key: row.schema_key().into(),
                        file_id: row.file_id().map(str::to_owned),
                        row_pk: row.row_pk().clone(),
                    },
                    pick,
                );
            }
            let descriptor = source
                .row(
                    affected
                        .iter()
                        .position(|candidate| candidate == id)
                        .expect("restored file is affected"),
                )
                .expect("restore requires source descriptor");
            let payload = native_file_descriptor_json(descriptor)?;
            let mut ancestor = payload
                .get("directory_id")
                .and_then(JsonValue::as_str)
                .map(str::to_owned);
            let mut visited = BTreeSet::new();
            while let Some(directory) = ancestor {
                if visited.len() >= 256 || !visited.insert(directory.clone()) {
                    return Err(LixError::new(
                        LixError::CODE_CONSTRAINT_VIOLATION,
                        "resurrected file has an invalid ancestor path",
                    ));
                }
                let key = TrackedStateKey {
                    schema_key: "lix_directory_descriptor".into(),
                    file_id: None,
                    row_pk: native_file_uuid_key(&directory)?,
                };
                let source_directory = reader
                    .load_projected_batch_at_commit(
                        &source_id,
                        std::slice::from_ref(&key),
                        &ChangeRecordProjection::full(),
                    )
                    .await?;
                let row = source_directory
                    .row(0)
                    .filter(|row| !row.deleted())
                    .ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_CONSTRAINT_VIOLATION,
                            "resurrected file source ancestor is absent",
                        )
                    })?;
                let payload = native_file_descriptor_json(row)?;
                let target_directory = reader
                    .load_projected_batch_at_commit(
                        &target_id,
                        std::slice::from_ref(&key),
                        &ChangeRecordProjection::identity_only(),
                    )
                    .await?;
                let incoming_directory_change = incoming_directory_keys.contains(&key.row_pk);
                if target_directory.row(0).is_some_and(|row| !row.deleted())
                    && !incoming_directory_change
                {
                    // This unchanged effective ancestor is already validated
                    // in the target root. Its current parent may differ from
                    // the source's old parent; do not revive that obsolete
                    // source chain. Incoming directory selections still need
                    // their own captured source dependency closure.
                    break;
                }
                if target_directory.row(0).is_none_or(|row| row.deleted()) {
                    if restored.len() >= MAX_ROWS && !restored.contains_key(&key) {
                        return Err(LixError::new(
                            "LIX_PARTIAL_MERGE_PREPARATION_LIMIT",
                            "file resurrection structural closure exceeds row budget",
                        ));
                    }
                    restored.insert(key, native_file_lifecycle_pick(row)?);
                }
                ancestor = payload
                    .get("parent_id")
                    .and_then(JsonValue::as_str)
                    .map(str::to_owned);
            }
        }
        drop(reader);
        let mut next = analysis.clone();
        let plan = next
            .merge_plan
            .as_mut()
            .expect("native file lifecycle requires merge plan");
        let keep = |identity: &crate::tracked_state::TrackedStateDiffIdentity| {
            identity.file_id().is_none_or(|id| !handled.contains(id))
        };
        let mut picks = plan
            .picks
            .iter()
            .filter(|pick| keep(&pick.identity))
            .cloned()
            .map(|pick| {
                (
                    TrackedStateKey {
                        schema_key: pick.identity.schema_key().into(),
                        file_id: pick.identity.file_id().map(str::to_owned),
                        row_pk: pick.identity.row_pk().clone(),
                    },
                    pick,
                )
            })
            .collect::<BTreeMap<_, _>>();
        // Incoming explicit directory changes remain authoritative over an
        // ancestor restored only as a structural dependency.
        for (key, pick) in restored {
            picks.entry(key).or_insert(pick);
        }
        plan.picks = picks.into_values().collect();
        plan.conflicts = plan
            .conflicts
            .iter()
            .filter(|conflict| keep(&conflict.identity))
            .cloned()
            .collect();
        next.source_diff
            .entries
            .retain(|entry| keep(&entry.identity));
        next.target_diff
            .entries
            .retain(|entry| keep(&entry.identity));
        if !deletes.is_empty() {
            let branch = self.active_branch_id().to_owned();
            let index = self
                .filesystem_path_index(&FilesystemPathIndexRequest::new(vec![branch.clone()]))
                .await?;
            let mut rows = RawWriteBatch::new();
            for id in deletes {
                if index
                    .exact_file_id_entries(&id)
                    .iter()
                    .any(|entry| is_plugin_storage_path(&entry.path))
                {
                    return Err(LixError::new(
                        LixError::CODE_CONSTRAINT_VIOLATION,
                        "native file deletion cannot implicitly uninstall a plugin archive",
                    ));
                }
                let blob_key = TrackedStateKey {
                    schema_key: "lix_binary_blob_ref".into(),
                    file_id: Some(id.clone()),
                    row_pk: native_file_uuid_key(&id)?,
                };
                let mut reader = self.tracked_state_reader().await?;
                let blob = reader
                    .load_projected_batch_at_commit(
                        &target_id,
                        &[blob_key],
                        &ChangeRecordProjection::identity_only(),
                    )
                    .await?;
                let has_blob_ref = blob.row(0).is_some_and(|row| !row.deleted());
                drop(reader);
                rows.append(
                    plan_file_delete(FileDeleteInput {
                        file_id: id.clone(),
                        has_blob_ref,
                        context: FilesystemRowContext {
                            branch_id: branch.clone(),
                            global: false,
                            untracked: false,
                            file_id: Some(id),
                            metadata: None,
                        },
                    })
                    .rows,
                );
            }
            self.stage_write(TransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows,
            })
            .await?;
        }
        Ok(Some(next))
    }

    /// Retire an existing file when an accepted incoming descriptor claims its
    /// name. This uses the ordinary deletion owner so plugin rows and blobs are
    /// retired together. Directory occupants and plugin archives retain their
    /// existing validation rules; neither is implicitly recursively deleted.
    pub(crate) async fn retire_incoming_file_path_occupants(
        &mut self,
        analysis: &crate::session::MergeAnalysis,
        semantic_rows: &RawWriteBatch,
    ) -> Result<(), LixError> {
        let plan = analysis
            .merge_plan()
            .expect("native application has a plan");
        let mut changes = BTreeMap::<String, Option<(Option<String>, String)>>::new();
        let mut keys = Vec::new();
        for pick in &plan.picks {
            if pick.identity.schema_key() == "lix_file_descriptor" {
                keys.push(TrackedStateKey {
                    schema_key: pick.identity.schema_key().into(),
                    file_id: pick.identity.file_id().map(str::to_owned),
                    row_pk: pick.identity.row_pk().clone(),
                });
            }
        }
        if !keys.is_empty() {
            let mut reader = self.tracked_state_reader().await?;
            let rows = reader
                .load_projected_batch_at_commit(
                    &analysis.commits.source_commit_id.to_string(),
                    &keys,
                    &ChangeRecordProjection::full(),
                )
                .await?;
            for (index, key) in keys.iter().enumerate() {
                let row = rows.row(index).ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "incoming descriptor selection is absent",
                    )
                })?;
                let value = if row.deleted() {
                    None
                } else {
                    let json = if let Some(typed) = row.decoded_snapshot() {
                        typed.to_json_shared()?
                    } else {
                        row.snapshot_content().cloned().ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                "incoming descriptor has no payload",
                            )
                        })?
                    };
                    Some(
                        serde_json::from_str::<JsonValue>(&json)
                            .map_err(|e| LixError::unknown(e.to_string()))?,
                    )
                };
                changes.insert(
                    key.row_pk.as_single_string_owned()?,
                    descriptor_name(value.as_ref())?,
                );
            }
        }
        for row in semantic_rows
            .iter()
            .filter(|row| row.schema_key.as_str() == "lix_file_descriptor")
        {
            let id = row
                .row_pk
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "resolved descriptor has no identity",
                    )
                })?
                .as_single_string_owned()?;
            let value = row.snapshot_json();
            changes.insert(id, descriptor_name(value.map(|value| &**value))?);
        }
        if !changes.values().any(Option::is_some) {
            return Ok(());
        }
        // This is the existing namespace metadata dependency, shared with
        // descriptor write validation. It does not load file contents. A cold
        // namespace index currently materializes branch descriptor metadata.
        let branch = self.active_branch_id().to_owned();
        let index = self
            .filesystem_path_index(&FilesystemPathIndexRequest::new(vec![branch.clone()]))
            .await?;
        let incoming_names = changes.values().flatten().collect::<BTreeSet<_>>();
        let mut deletes = RawWriteBatch::new();
        for entry in index.entries() {
            if entry.kind != FilesystemPathKind::File
                || entry.key.global()
                || entry.key.is_untracked()
                || entry.key.branch_id() != branch
                || changes.contains_key(entry.id())
                || !incoming_names.contains(&(entry.parent_id.clone(), entry.name.clone()))
            {
                continue;
            }
            if is_plugin_storage_path(&entry.path) {
                return Err(LixError::new(
                    LixError::CODE_CONSTRAINT_VIOLATION,
                    "incoming file path cannot implicitly uninstall a plugin archive",
                ));
            }
            let key = TrackedStateKey {
                schema_key: "lix_binary_blob_ref".into(),
                file_id: Some(entry.id().into()),
                row_pk: native_file_uuid_key(entry.id())?,
            };
            let has_blob_ref = {
                let mut reader = self.tracked_state_reader().await?;
                let rows = reader
                    .load_projected_batch_at_commit(
                        &analysis.commits.target_commit_id.to_string(),
                        &[key],
                        &ChangeRecordProjection::full(),
                    )
                    .await?;
                rows.row(0).is_some_and(|row| !row.deleted())
            };
            deletes.append(
                plan_file_delete(FileDeleteInput {
                    file_id: entry.id().into(),
                    has_blob_ref,
                    context: FilesystemRowContext {
                        branch_id: branch.clone(),
                        global: false,
                        untracked: false,
                        file_id: Some(entry.id().into()),
                        metadata: None,
                    },
                })
                .rows,
            );
        }
        if !deletes.is_empty() {
            self.stage_write(TransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: deletes,
            })
            .await?;
        }
        Ok(())
    }
}

fn descriptor_name(
    value: Option<&JsonValue>,
) -> Result<Option<(Option<String>, String)>, LixError> {
    value
        .map(|value| {
            let name = value
                .get("name")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_SCHEMA_VALIDATION,
                        "file descriptor has no name",
                    )
                })?;
            let directory = match value.get("directory_id") {
                None | Some(JsonValue::Null) => None,
                Some(JsonValue::String(id)) => Some(id.clone()),
                _ => {
                    return Err(LixError::new(
                        LixError::CODE_SCHEMA_VALIDATION,
                        "invalid directory identity",
                    ));
                }
            };
            Ok((directory, name.into()))
        })
        .transpose()
}

fn native_file_descriptor_json(
    row: crate::tracked_state::MaterializedTrackedStateRowRef<'_>,
) -> Result<JsonValue, LixError> {
    let json = if let Some(typed) = row.decoded_snapshot() {
        typed.to_json_shared()?
    } else {
        row.snapshot_content().cloned().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "native file descriptor payload is absent",
            )
        })?
    };
    serde_json::from_str(&json).map_err(|error| LixError::unknown(error.to_string()))
}
fn native_file_lifecycle_pick(
    row: crate::tracked_state::MaterializedTrackedStateRowRef<'_>,
) -> Result<crate::tracked_state::TrackedStateMergePick, LixError> {
    let identity =
        crate::tracked_state::TrackedStateDiffIdentity::from_key_batch(vec![TrackedStateKey {
            schema_key: row.schema_key().into(),
            file_id: row.file_id().map(str::to_owned),
            row_pk: row.row_pk().clone(),
        }])?
        .pop()
        .expect("one native identity");
    Ok(crate::tracked_state::TrackedStateMergePick {
        identity: identity.clone(),
        change_id: row.change_id(),
        selected_row: crate::tracked_state::TrackedStateDiffRow {
            identity,
            change_id: row.change_id(),
            commit_id: row.commit_id(),
            deleted: row.deleted(),
            created_at: row.created_at(),
            updated_at: row.updated_at(),
        },
    })
}

// Native built-in structural primary keys are typed UUIDs, not string keys.
fn native_file_uuid_key(id: &str) -> Result<RowPk, LixError> {
    RowPk::uuid_from_canonical(id).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("native file lifecycle contains a noncanonical structural ID: {error}"),
        )
    })
}

#[cfg(test)]
mod ancestor_restore_tests {
    use crate::{CreateBranchOptions, MergeBranchOptions, open_lix};

    #[tokio::test]
    async fn file_restore_stops_at_unchanged_effective_target_directory() {
        let target = open_lix().await.unwrap();
        target.execute("INSERT INTO lix_file(path,content) VALUES('/old/child/note.txt',CAST('base' AS BYTEA))", &[]).await.unwrap();
        let branch = target
            .create_branch(CreateBranchOptions {
                id: None,
                name: "incoming".into(),
                from_commit_id: None,
            })
            .await
            .unwrap();
        let source = target
            .open_another_session()
            .with_branch(branch.id.clone())
            .await
            .unwrap();
        source.execute("UPDATE lix_file SET content=CAST('restored' AS BYTEA) WHERE path='/old/child/note.txt'", &[]).await.unwrap();
        for sql in [
            "DELETE FROM lix_file WHERE path='/old/child/note.txt'",
            "UPDATE lix_directory SET path='/moved' WHERE path='/old/child'",
            "DELETE FROM lix_directory WHERE path='/old'",
        ] {
            target.execute(sql, &[]).await.unwrap();
        }
        target
            .merge_branch(MergeBranchOptions {
                source_branch_id: branch.id,
            })
            .await
            .unwrap();
        let restored = target.execute("SELECT id FROM lix_file WHERE path='/moved/note.txt' AND content=CAST('restored' AS BYTEA)", &[]).await.unwrap();
        assert_eq!(restored.rows().len(), 1);
        let obsolete = target
            .execute("SELECT id FROM lix_directory WHERE path='/old'", &[])
            .await
            .unwrap();
        assert!(
            obsolete.rows().is_empty(),
            "restoration must not revive the moved directory's obsolete parent"
        );
    }

    #[tokio::test]
    async fn incoming_directory_pick_retains_its_required_source_parent() {
        let target = open_lix().await.unwrap();
        target.execute("INSERT INTO lix_file(path,content) VALUES('/old/child/note.txt',CAST('base' AS BYTEA))", &[]).await.unwrap();
        target
            .execute("INSERT INTO lix_directory(path) VALUES('/new')", &[])
            .await
            .unwrap();
        let branch = target
            .create_branch(CreateBranchOptions {
                id: None,
                name: "incoming".into(),
                from_commit_id: None,
            })
            .await
            .unwrap();
        let source = target
            .open_another_session()
            .with_branch(branch.id.clone())
            .await
            .unwrap();
        source
            .execute(
                "UPDATE lix_directory SET path='/new/child' WHERE path='/old/child'",
                &[],
            )
            .await
            .unwrap();
        source.execute("UPDATE lix_file SET content=CAST('restored' AS BYTEA) WHERE path='/new/child/note.txt'", &[]).await.unwrap();
        target
            .execute("DELETE FROM lix_file WHERE path='/old/child/note.txt'", &[])
            .await
            .unwrap();
        target
            .execute("DELETE FROM lix_directory WHERE path='/new'", &[])
            .await
            .unwrap();
        target
            .merge_branch(MergeBranchOptions {
                source_branch_id: branch.id,
            })
            .await
            .unwrap();
        let restored = target.execute("SELECT id FROM lix_file WHERE path='/new/child/note.txt' AND content=CAST('restored' AS BYTEA)", &[]).await.unwrap();
        assert_eq!(restored.rows().len(), 1);
    }
}
