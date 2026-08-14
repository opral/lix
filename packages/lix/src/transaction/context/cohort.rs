use std::collections::{BTreeMap, BTreeSet};

use super::*;

struct PreparedCohortMember<StorageImpl>
where
    StorageImpl: Storage + 'static,
{
    transaction: Transaction<StorageImpl>,
    runtime_functions: FunctionContext,
    prepared_writes: PreparedWriteSet,
}

pub(super) async fn commit_transaction_cohort<StorageImpl>(
    cohort: Vec<(Transaction<StorageImpl>, FunctionContext)>,
) -> Vec<Result<TransactionCommitOutcome, LixError>>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    if cohort.len() <= 1 {
        return commit_individually(cohort).await;
    }
    if !cohort_keys_match(&cohort) {
        return commit_individually(cohort).await;
    }

    // Hold every session lifecycle open for the whole shared persistence
    // boundary. If one member is already invalid, preserve per-transaction
    // results by taking the ordinary serialized path.
    let commit_guards = cohort
        .iter()
        .map(|(transaction, _)| begin_commit_boundary(transaction.commit_boundary.as_ref()))
        .collect::<Vec<_>>();
    if cohort.iter().any(|(transaction, _)| {
        check_commit_boundary(transaction.commit_boundary.as_ref()).is_err()
    }) {
        drop(commit_guards);
        return commit_individually(cohort).await;
    }

    let member_count = cohort.len();
    let mut prepared: Vec<PreparedCohortMember<StorageImpl>> = Vec::with_capacity(member_count);
    let mut cohort = cohort.into_iter();
    while let Some((mut transaction, runtime_functions)) = cohort.next() {
        let prepared_writes = match transaction.staged_writes.drain() {
            Ok(writes) => writes,
            Err(error) => {
                transaction
                    .discard_pending_plugin_actor_publications()
                    .await;
                for (mut remaining, _) in cohort {
                    remaining.discard_pending_plugin_actor_publications().await;
                }
                for mut earlier in prepared {
                    earlier
                        .transaction
                        .discard_pending_plugin_actor_publications()
                        .await;
                }
                return vec![Err(error); member_count];
            }
        };
        prepared.push(PreparedCohortMember {
            transaction,
            runtime_functions,
            prepared_writes,
        });
    }

    if !can_merge_cohort(&prepared) {
        return commit_prepared_individually(prepared).await;
    }
    let outcomes = Box::pin(commit_merged_cohort(prepared)).await;
    drop(commit_guards);
    outcomes
}

fn cohort_keys_match<StorageImpl>(cohort: &[(Transaction<StorageImpl>, FunctionContext)]) -> bool
where
    StorageImpl: Storage + 'static,
{
    let Some((leader, _)) = cohort.first() else {
        return true;
    };
    cohort.iter().all(|(transaction, _)| {
        transaction.active_branch_id == leader.active_branch_id
            && transaction.opening_active_branch_head == leader.opening_active_branch_head
            && transaction.opening_global_branch_head == leader.opening_global_branch_head
            && transaction.opening_selector_fence == leader.opening_selector_fence
            && transaction.idempotency_receipt.is_none()
            && transaction.pending_forktree_publication.is_none()
            && !transaction.await_durable_commit
    })
}

fn can_merge_cohort<StorageImpl>(members: &[PreparedCohortMember<StorageImpl>]) -> bool
where
    StorageImpl: Storage + 'static,
{
    let Some(leader) = members.first() else {
        return false;
    };
    let branch_id = leader.transaction.active_branch_id.as_str();
    let mut unfiled_identitys = BTreeSet::new();
    for member in members {
        let writes = &member.prepared_writes;
        if writes.commit_change_refs_by_branch.len() != 1
            || !writes.commit_change_refs_by_branch.contains_key(branch_id)
            || !writes.first_commit_parent_override_by_branch.is_empty()
            || !writes.checkpoint_publications.is_empty()
            || !writes.extra_commit_parents_by_branch.is_empty()
            || !writes.intermediate_commits.is_empty()
        {
            return false;
        }
        for row in &writes.state_rows {
            if row.untracked || row.global || row.branch_id.as_str() != branch_id {
                return false;
            }
            if row.file_id.is_none()
                && !unfiled_identitys.insert((
                    row.branch_id.to_string(),
                    row.schema_key.to_string(),
                    row.row_pk.clone(),
                ))
            {
                return false;
            }
        }
    }
    true
}

async fn commit_merged_cohort<StorageImpl>(
    mut members: Vec<PreparedCohortMember<StorageImpl>>,
) -> Vec<Result<TransactionCommitOutcome, LixError>>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let member_count = members.len();
    let mut leader = members.remove(0);
    let branch_id = leader.transaction.active_branch_id.clone();
    let cohort_commit_id =
        leader.prepared_writes.commit_change_refs_by_branch[&branch_id].commit_id;
    let affected_file_ids =
        affected_cohort_file_ids(std::iter::once(&leader).chain(members.iter()));
    let cohort_file_ids = all_cohort_file_ids(std::iter::once(&leader).chain(members.iter()));

    let replacement = if affected_file_ids.is_empty() {
        None
    } else {
        match Box::pin(reconcile_cohort_files(
            &mut leader.transaction,
            std::iter::once(&leader.prepared_writes)
                .chain(members.iter().map(|member| &member.prepared_writes)),
            &affected_file_ids,
            cohort_commit_id,
        ))
        .await
        {
            Ok(replacement) => Some(replacement),
            Err(_) => {
                // A cohort optimization must never widen one plugin failure
                // into a failure for unrelated transactions. Discard any
                // partial replay staging and retry through the established
                // per-transaction commit semantics.
                let _ = leader.transaction.staged_writes.drain();
                let mut individual = Vec::with_capacity(member_count);
                individual.push(leader);
                individual.extend(members);
                return commit_prepared_individually(individual).await;
            }
        }
    };
    let session_views = std::iter::once(&leader)
        .chain(members.iter())
        .map(|member| member.transaction.session_file_views.clone())
        .collect::<Vec<_>>();
    for views in &session_views {
        views.apply_mutations(cohort_file_ids.iter().map(|file_id| {
            SessionFileViewMutation::Remove {
                key: SessionFileViewKey::new(&branch_id, file_id),
            }
        }));
    }
    leader
        .transaction
        .pending_file_view_mutations
        .retain(|key, _| !cohort_file_ids.contains(&key.file_id));
    let mut merged_writes = leader.prepared_writes.clone();
    for member in &members {
        if let Err(error) = merged_writes.append_cohort_member(
            member.prepared_writes.clone(),
            &branch_id,
            cohort_commit_id,
        ) {
            leader
                .transaction
                .discard_pending_plugin_actor_publications()
                .await;
            return vec![Err(error); member_count];
        }
    }
    if let Some(replacement) = replacement {
        merged_writes.replace_reconciled_file_writes(replacement, &affected_file_ids);
    }

    for member in &mut members {
        member
            .transaction
            .discard_pending_plugin_actor_publications()
            .await;
    }
    let result = leader
        .transaction
        .commit_prepared(&leader.runtime_functions, merged_writes)
        .instrument(tracing::debug_span!(
            target: "lix_transaction",
            "lix.transaction.commit_cohort.execute",
            cohort_size = member_count,
        ))
        .await;
    match result {
        Ok(outcome) => {
            let mut outcomes = vec![Ok(TransactionCommitOutcome::default()); member_count];
            outcomes[0] = Ok(outcome);
            outcomes
        }
        Err(error) => vec![Err(error); member_count],
    }
}

fn all_cohort_file_ids<'a, StorageImpl>(
    members: impl Iterator<Item = &'a PreparedCohortMember<StorageImpl>>,
) -> BTreeSet<String>
where
    StorageImpl: Storage + 'static,
{
    let mut file_ids = BTreeSet::new();
    for member in members {
        file_ids.extend(
            member
                .prepared_writes
                .state_rows
                .iter()
                .filter_map(|row| row.file_id.map(ToString::to_string)),
        );
        file_ids.extend(
            member
                .prepared_writes
                .file_content_writes
                .iter()
                .map(|write| write.file_id.to_string()),
        );
    }
    file_ids
}

fn affected_cohort_file_ids<'a, StorageImpl>(
    members: impl Iterator<Item = &'a PreparedCohortMember<StorageImpl>>,
) -> BTreeSet<String>
where
    StorageImpl: Storage + 'static,
{
    let mut member_count_by_file = BTreeMap::<String, usize>::new();
    for member in members {
        let mut files = member
            .prepared_writes
            .state_rows
            .iter()
            .filter_map(|row| row.file_id.map(ToString::to_string))
            .collect::<BTreeSet<_>>();
        files.extend(
            member
                .prepared_writes
                .file_content_writes
                .iter()
                .map(|write| write.file_id.to_string()),
        );
        for file_id in files {
            *member_count_by_file.entry(file_id).or_default() += 1;
        }
    }
    member_count_by_file
        .into_iter()
        .filter_map(|(file_id, count)| (count > 1).then_some(file_id))
        .collect()
}

#[derive(Clone)]
struct CohortSemanticCandidate {
    payload: Option<StaleConflictPayload>,
    rank: ConflictRank,
}

struct CohortPluginGroup {
    plugin: PluginRegistryEntry,
    descriptor: WasmFileDescriptor,
    candidates: BTreeMap<StateKey, Vec<CohortSemanticCandidate>>,
}

async fn reconcile_cohort_files<'a, StorageImpl>(
    transaction: &mut Transaction<StorageImpl>,
    prepared: impl Iterator<Item = &'a PreparedWriteSet>,
    file_ids: &BTreeSet<String>,
    cohort_commit_id: CommitId,
) -> Result<PreparedWriteSet, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let opening_head = transaction.opening_active_branch_head.ok_or_else(|| {
        LixError::new(
            LixError::CODE_TRANSACTION_CONFLICT,
            "rootless branch transactions cannot join a semantic commit cohort",
        )
    })?;
    let facade = transaction.forktree_read_facade();
    let mut groups =
        load_cohort_plugin_groups(transaction, &facade, opening_head, file_ids).await?;
    for writes in prepared {
        for row in &writes.state_rows {
            let Some(file_id) = row.file_id.map(SharedStr::as_str) else {
                continue;
            };
            let Some(group) = groups.get_mut(file_id) else {
                continue;
            };
            if !group
                .plugin
                .schema_keys()
                .iter()
                .any(|schema_key| schema_key == row.schema_key.as_str())
            {
                continue;
            }
            let change_id = row.change_id.ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "cohort semantic row is missing change_id",
                )
            })?;
            let key = StateKey {
                schema_key: row.schema_key.to_string(),
                file_id: Some(file_id.to_string()),
                row_pk: row.row_pk.clone(),
            };
            group
                .candidates
                .entry(key)
                .or_default()
                .push(CohortSemanticCandidate {
                    payload: row.snapshot.map(|snapshot| StaleConflictPayload {
                        snapshot: snapshot.materialize_shared(),
                        metadata: row.metadata.map(|metadata| metadata.materialize_shared()),
                    }),
                    rank: ConflictRank::new(row.updated_at, change_id),
                });
        }
    }

    let mut replay_batches = BTreeMap::<String, RawWriteBatch>::new();
    for (file_id, group) in &mut groups {
        let keys = group.candidates.keys().cloned().collect::<Vec<_>>();
        let base_rows = load_historical_rows_at_commit(&facade, opening_head, &keys).await?;
        let mut frontiers = BTreeMap::<StateKey, Vec<Option<StaleConflictPayload>>>::new();
        let mut bases = BTreeMap::<StateKey, Option<StaleConflictPayload>>::new();
        let rows = replay_batches
            .entry(file_id.clone())
            .or_insert_with(|| RawWriteBatch::with_capacity(keys.len()));
        for (slot, key) in keys.iter().enumerate() {
            let candidates = group
                .candidates
                .get_mut(key)
                .expect("candidate key originates from group");
            candidates.sort_by_key(|candidate| candidate.rank);
            let base = stale_payload_from_historical(base_rows[slot].as_ref())?;
            candidates.retain(|candidate| candidate.payload != base);
            let mut seen = BTreeSet::new();
            candidates.retain(|candidate| seen.insert(candidate.payload.clone()));
            if candidates.is_empty() {
                continue;
            }
            bases.insert(key.clone(), base);
            frontiers.insert(
                key.clone(),
                std::mem::take(candidates)
                    .into_iter()
                    .map(|candidate| candidate.payload)
                    .collect(),
            );
        }

        while frontiers.values().any(|frontier| frontier.len() > 1) {
            let mut conflicts = Vec::new();
            let mut semantic_conflicts = Vec::new();
            let mut next_frontiers = BTreeMap::<StateKey, Vec<Option<StaleConflictPayload>>>::new();
            for key in &keys {
                let Some(frontier) = frontiers.get(key) else {
                    continue;
                };
                let next = next_frontiers.entry(key.clone()).or_default();
                for pair in frontier.chunks(2) {
                    if pair.len() == 1 {
                        next.push(pair[0].clone());
                        continue;
                    }
                    let conflict = StaleSemanticConflict {
                        key: key.clone(),
                        base: bases.get(key).cloned().flatten(),
                        a: pair[0].clone(),
                        b: pair[1].clone(),
                    };
                    let ordinal = u32::try_from(conflicts.len()).map_err(|_| {
                        LixError::new(
                            LixError::CODE_INVALID_PLUGIN,
                            "cohort conflict batch exceeds the u32 ordinal limit",
                        )
                    })?;
                    conflicts.push(WasmRowConflict {
                        ordinal,
                        key: WasmRowKey::from_owned_parts(
                            key.schema_key.clone(),
                            key.row_pk.clone().into_parts(),
                        ),
                        base: stale_conflict_bytes(conflict.base.as_ref()),
                        a: stale_conflict_bytes(conflict.a.as_ref()),
                        b: stale_conflict_bytes(conflict.b.as_ref()),
                    });
                    semantic_conflicts.push(conflict);
                }
            }
            let resolutions = transaction
                .resolve_plugin_conflicts(&group.plugin, group.descriptor.clone(), conflicts)
                .instrument(tracing::debug_span!(
                    target: "lix_transaction",
                    "lix.transaction.cohort.resolve_plugin",
                    plugin_key = group.plugin.key(),
                    conflict_rows = semantic_conflicts.len(),
                ))
                .await?;
            for (conflict, resolution) in
                semantic_conflicts.into_iter().zip(resolutions.resolutions)
            {
                let key = conflict.key.clone();
                let payload = stale_conflict_resolution_payload(&conflict, resolution)?;
                next_frontiers.entry(key).or_default().push(payload);
            }
            frontiers = next_frontiers;
        }
        for (key, frontier) in frontiers {
            let payload = frontier.into_iter().next().expect("non-empty frontier");
            push_cohort_payload(rows, &key, payload.as_ref(), &transaction.active_branch_id);
        }
    }

    // The replay is a new consolidated semantic transition. Original private
    // actor publications describe per-member byte transitions and cannot be
    // chained into it.
    transaction
        .discard_pending_plugin_actor_publications()
        .await;
    for rows in replay_batches.into_values() {
        transaction
            .stage_write(TransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows,
            })
            .await?;
    }
    // A cohort transition can apply several independently-produced semantic
    // deltas at once. Persist its checkpoint, but force the next transaction
    // to hydrate from the committed graph instead of treating the cohort's
    // private renderer document as an ordinary single-writer successor.
    transaction.release_pending_plugin_actor_leases().await;
    let mut replacement = transaction.staged_writes.drain()?;
    let mut latest_file_content = BTreeMap::new();
    for write in replacement.file_content_writes.drain(..) {
        latest_file_content.insert((write.branch_id.clone(), write.file_id.clone()), write);
    }
    replacement
        .file_content_writes
        .extend(latest_file_content.into_values());
    replacement.state_rows.set_commit_id_all(cohort_commit_id);
    Ok(replacement)
}

pub(super) fn push_cohort_payload(
    rows: &mut RawWriteBatch,
    key: &StateKey,
    payload: Option<&StaleConflictPayload>,
    branch_id: &str,
) {
    rows.push_parts(
        Some(key.row_pk.clone()),
        SharedStr::from(key.schema_key.as_str()),
        key.file_id.as_deref().map(SharedStr::from),
        payload.map(|payload| {
            TransactionJson::from_unvalidated_shared_normalized_content(payload.snapshot.clone())
        }),
        payload.and_then(|payload| {
            payload
                .metadata
                .clone()
                .map(TransactionJson::from_unvalidated_shared_normalized_content)
        }),
        None,
        None,
        None,
        false,
        None,
        None,
        false,
        SharedStr::from(branch_id),
    );
}

async fn load_cohort_plugin_groups<StorageImpl, R>(
    transaction: &mut Transaction<StorageImpl>,
    facade: &ForkTreeReadFacade<R>,
    opening_head: CommitId,
    file_ids: &BTreeSet<String>,
) -> Result<BTreeMap<String, CohortPluginGroup>, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
    R: StorageAdapterRead,
{
    let owner_keys = file_ids
        .iter()
        .map(|file_id| StateKey {
            schema_key: KEY_VALUE_SCHEMA_KEY.to_owned(),
            file_id: Some(file_id.clone()),
            row_pk: RowPk::single(PLUGIN_OWNER_KEY),
        })
        .collect::<Vec<_>>();
    let registry_key = StateKey {
        schema_key: KEY_VALUE_SCHEMA_KEY.to_owned(),
        file_id: None,
        row_pk: RowPk::single(PLUGIN_REGISTRY_KEY),
    };
    let owners = load_historical_rows_at_commit(facade, opening_head, &owner_keys).await?;
    let registry_rows =
        load_historical_rows_at_commit(facade, opening_head, std::slice::from_ref(&registry_key))
            .await?;
    let registry_row = registry_rows
        .first()
        .and_then(Option::as_ref)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_TRANSACTION_CONFLICT,
                "cohort plugin registry row is missing",
            )
        })?;
    let registry_content = registry_row.seed_snapshot_content()?;
    if registry_row.deleted || registry_content.is_none() {
        return Err(LixError::new(
            LixError::CODE_TRANSACTION_CONFLICT,
            "cohort plugin registry row is not an authenticated value",
        ));
    }
    let registry_snapshot: serde_json::Value = serde_json::from_str(
        registry_content
            .as_ref()
            .expect("checked above")
            .as_str(),
    )
    .map_err(|error| {
        LixError::new(
            LixError::CODE_INVALID_PLUGIN,
            format!("plugin registry snapshot is invalid JSON: {error}"),
        )
    })?;
    let registry = PluginRegistry::from_optional_snapshot(Some(&registry_snapshot))?;
    let path_index = transaction
        .filesystem_path_index(&FilesystemPathIndexRequest::new(vec![
            transaction.active_branch_id.clone(),
        ]))
        .await?;
    let mut groups = BTreeMap::new();
    for (owner_index, file_id) in file_ids.iter().enumerate() {
        let owner_row = owners[owner_index].as_ref().ok_or_else(|| {
            LixError::new(
                LixError::CODE_TRANSACTION_CONFLICT,
                "cohort plugin owner row is missing",
            )
        })?;
        let owner = PluginFileOwner::from_historical_state_row(owner_row)?.ok_or_else(|| {
            LixError::new(
                LixError::CODE_TRANSACTION_CONFLICT,
                "cohort file is not owned by a stable plugin generation",
            )
        })?;
        let plugin = registry
            .plugin(owner.plugin_key())
            .filter(|plugin| plugin.schema_keys() == owner.schema_keys())
            .cloned()
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_TRANSACTION_CONFLICT,
                    "cohort file plugin ownership does not match the registry",
                )
            })?;
        let path = path_index
            .exact_file_id_entries(file_id)
            .iter()
            .find(|entry| entry.key.branch_id() == transaction.active_branch_id)
            .map(|entry| entry.path.clone())
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_TRANSACTION_CONFLICT,
                    "cohort file has no stable active-branch path",
                )
            })?;
        groups.insert(
            file_id.clone(),
            CohortPluginGroup {
                descriptor: WasmFileDescriptor {
                    path: Some(path.clone()),
                    plugin: WasmPluginSelection {
                        plugin_key: plugin.key().to_owned(),
                        generation: plugin.archive_blob_hash().to_owned(),
                    },
                },
                plugin,
                candidates: BTreeMap::new(),
            },
        );
    }
    Ok(groups)
}

async fn commit_prepared_individually<StorageImpl>(
    members: Vec<PreparedCohortMember<StorageImpl>>,
) -> Vec<Result<TransactionCommitOutcome, LixError>>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let mut outcomes = Vec::with_capacity(members.len());
    for member in members {
        outcomes.push(
            member
                .transaction
                .commit_prepared(&member.runtime_functions, member.prepared_writes)
                .await,
        );
    }
    outcomes
}

async fn commit_individually<StorageImpl>(
    cohort: Vec<(Transaction<StorageImpl>, FunctionContext)>,
) -> Vec<Result<TransactionCommitOutcome, LixError>>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let mut outcomes = Vec::with_capacity(cohort.len());
    for (transaction, runtime_functions) in cohort {
        outcomes.push(transaction.commit(&runtime_functions).await);
    }
    outcomes
}
