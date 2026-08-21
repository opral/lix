use std::collections::{BTreeMap, BTreeSet, VecDeque};

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
    let commit_cohort_id = crate::telemetry::current_commit_cohort_id()
        .unwrap_or_else(crate::telemetry::next_commit_cohort_id);
    let materialize_span = start_materialize_span(member_count, &commit_cohort_id);
    let mut prepared: Vec<PreparedCohortMember<StorageImpl>> = Vec::with_capacity(member_count);
    let mut cohort = cohort.into_iter();
    while let Some((mut transaction, runtime_functions)) = cohort.next() {
        let prepared_writes = match materialize_span.as_ref() {
            Some(span) => {
                let _entered = span.enter();
                transaction.staged_writes.drain()
            }
            None => transaction.staged_writes.drain(),
        };
        let prepared_writes = match prepared_writes {
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
                if let Some(span) = materialize_span {
                    span.finish(
                        Status::error(error.code.clone()),
                        vec![TelemetryAttribute::string("error.type", error.code.clone())],
                    );
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
        if let Some(span) = materialize_span {
            span.finish(Status::Unset, Vec::new());
        }
        return commit_prepared_individually(prepared).await;
    }
    let outcomes = Box::pin(commit_merged_cohort(
        prepared,
        commit_cohort_id,
        materialize_span,
    ))
    .await;
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
            && transaction.opening_tracked_mutation_revision
                == leader.opening_tracked_mutation_revision
            && transaction.idempotency_receipt.is_none()
            && transaction.atomic_metadata_writes.is_none()
            && transaction.atomic_metadata_preconditions.is_empty()
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
        }
    }
    true
}

async fn commit_merged_cohort<StorageImpl>(
    members: Vec<PreparedCohortMember<StorageImpl>>,
    commit_cohort_id: String,
    materialize_span: Option<ActiveTelemetrySpan>,
) -> Vec<Result<TransactionCommitOutcome, LixError>>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let member_count = members.len();
    let preparation = prepare_merged_cohort(members);
    let preparation = match materialize_span.as_ref() {
        Some(span) => span.instrument(preparation).await,
        None => preparation.await,
    };
    let (leader, merged_writes) = match preparation {
        MergedCohortPreparation::Ready {
            leader,
            merged_writes,
        } => (leader, merged_writes),
        MergedCohortPreparation::Fallback(individual) => {
            if let Some(span) = materialize_span {
                span.finish(Status::Unset, Vec::new());
            }
            return commit_prepared_individually(individual).await;
        }
        MergedCohortPreparation::Error(error) => {
            if let Some(span) = materialize_span {
                span.finish(
                    Status::error(error.code.clone()),
                    vec![TelemetryAttribute::string("error.type", error.code.clone())],
                );
            }
            return vec![Err(error); member_count];
        }
    };
    let result = leader
        .transaction
        .commit_prepared(
            &leader.runtime_functions,
            merged_writes,
            member_count,
            commit_cohort_id,
            materialize_span,
        )
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

enum MergedCohortPreparation<StorageImpl>
where
    StorageImpl: Storage + 'static,
{
    Ready {
        leader: PreparedCohortMember<StorageImpl>,
        merged_writes: PreparedWriteSet,
    },
    Fallback(Vec<PreparedCohortMember<StorageImpl>>),
    Error(LixError),
}

async fn prepare_merged_cohort<StorageImpl>(
    mut members: Vec<PreparedCohortMember<StorageImpl>>,
) -> MergedCohortPreparation<StorageImpl>
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

    let has_overlapping_unfiled_rows =
        cohort_has_overlapping_unfiled_rows(std::iter::once(&leader).chain(members.iter()));
    let replacement = if affected_file_ids.is_empty() && !has_overlapping_unfiled_rows {
        None
    } else {
        match Box::pin(reconcile_cohort_rows(
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
                leader
                    .transaction
                    .discard_pending_plugin_actor_publications()
                    .await;
                let mut individual = Vec::with_capacity(member_count);
                individual.push(leader);
                individual.extend(members);
                return MergedCohortPreparation::Fallback(individual);
            }
        }
    };
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
            return MergedCohortPreparation::Error(error);
        }
    }
    if let Some(replacement) = replacement {
        merged_writes.replace_reconciled_writes(replacement, &affected_file_ids);
    }

    let validation_storage = leader.transaction.storage.clone();
    let validation_read = match validation_storage
        .begin_read(StorageReadOptions::default())
        .await
    {
        Ok(read) => read,
        Err(_) => {
            let _ = leader.transaction.staged_writes.drain();
            leader
                .transaction
                .discard_pending_plugin_actor_publications()
                .await;
            let mut individual = vec![leader];
            individual.extend(members);
            return MergedCohortPreparation::Fallback(individual);
        }
    };
    let validation_read = SharedStorageAdapterRead::new(validation_read);
    if leader
        .transaction
        .validate_prepared_writes_by_branch(&validation_read, &mut merged_writes)
        .await
        .is_err()
    {
        let _ = leader.transaction.staged_writes.drain();
        leader
            .transaction
            .discard_pending_plugin_actor_publications()
            .await;
        let mut individual = vec![leader];
        individual.extend(members);
        return MergedCohortPreparation::Fallback(individual);
    }
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
    for member in &mut members {
        member
            .transaction
            .discard_pending_plugin_actor_publications()
            .await;
    }
    MergedCohortPreparation::Ready {
        leader,
        merged_writes,
    }
}

fn cohort_has_overlapping_unfiled_rows<'a, StorageImpl>(
    members: impl Iterator<Item = &'a PreparedCohortMember<StorageImpl>>,
) -> bool
where
    StorageImpl: Storage + 'static,
{
    let mut seen = BTreeSet::new();
    for member in members {
        for row in &member.prepared_writes.state_rows {
            if row.file_id.is_none()
                && !seen.insert((row.schema_key.to_string(), row.row_pk.clone()))
            {
                return true;
            }
        }
    }
    false
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

fn reconcile_native_frontier(
    base: Option<&StaleConflictPayload>,
    candidates: &[CohortSemanticCandidate],
    primary_key_columns: &BTreeSet<String>,
) -> Result<Option<StaleConflictPayload>, LixError> {
    if let [candidate] = candidates {
        return Ok(candidate.payload.clone());
    }
    // Host-native column LWW is associative for successors ranked against one
    // common base. Fold the complete frontier while its rows are decoded so a
    // large same-base cohort does not repeatedly cross the async batch helper
    // and decode/encode the evolving row once per writer.
    let base = decode_stale_payload(base)?;
    let mut current = decode_stale_payload(
        candidates
            .first()
            .and_then(|candidate| candidate.payload.as_ref()),
    )?;
    for candidate in candidates.iter().skip(1) {
        let next = decode_stale_payload(candidate.payload.as_ref())?;
        current = reconcile_row(
            row_version_ref(base.as_ref()),
            row_version_ref(current.as_ref()),
            row_version_ref(next.as_ref()),
            primary_key_columns,
            |_| Ok(None),
        )?
        .map(|row| DecodedStalePayload {
            snapshot: row.snapshot,
            metadata: row.metadata,
        });
    }
    current
        .map(|row| {
            encoded_stale_payload(crate::plugin::runtime::ReconciledRow {
                snapshot: row.snapshot,
                metadata: row.metadata,
            })
        })
        .transpose()
}

async fn reconcile_cohort_rows<'a, StorageImpl>(
    transaction: &mut Transaction<StorageImpl>,
    prepared: impl Iterator<Item = &'a PreparedWriteSet>,
    affected_file_ids: &BTreeSet<String>,
    cohort_commit_id: CommitId,
) -> Result<PreparedWriteSet, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let opening_head = transaction.opening_active_branch_head.ok_or_else(|| {
        LixError::new(
            LixError::CODE_TRANSACTION_CONFLICT,
            "rootless branch transactions cannot join a row reconciliation cohort",
        )
    })?;
    let mut candidates = BTreeMap::<TrackedStateKey, Vec<CohortSemanticCandidate>>::new();
    let mut primary_keys_by_key = BTreeMap::<TrackedStateKey, BTreeSet<String>>::new();
    for writes in prepared {
        for row in &writes.state_rows {
            let include = row
                .file_id
                .map_or(true, |file_id| affected_file_ids.contains(file_id.as_str()));
            if !include {
                continue;
            }
            let change_id = row.change_id.ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "cohort row is missing change_id",
                )
            })?;
            let key = TrackedStateKey {
                schema_key: row.schema_key.to_string(),
                file_id: row.file_id.map(ToString::to_string),
                row_pk: row.row_pk.clone(),
            };
            let primary_keys = transaction
                .sql_schema_snapshot
                .plan(row.schema_plan_id)
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "cohort row reconciliation lost its schema plan",
                    )
                })
                .and_then(crate::plugin::runtime::primary_key_columns)?;
            if let Some(existing) = primary_keys_by_key.insert(key.clone(), primary_keys.clone())
                && existing != primary_keys
            {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "one cohort row identity resolved to inconsistent schema plans",
                ));
            }
            candidates
                .entry(key)
                .or_default()
                .push(CohortSemanticCandidate {
                    payload: row
                        .snapshot
                        .map(|snapshot| {
                            Ok::<_, LixError>(StaleConflictPayload {
                                snapshot: None,
                                decoded_snapshot: Some(Arc::new(
                                    WasmTypedRow::decode_durable_payload(
                                        Arc::from(snapshot),
                                        row.schema_key.as_str(),
                                        row.row_pk,
                                    )?,
                                )),
                                metadata: row
                                    .metadata
                                    .map(|metadata| metadata.to_json_string().map(SharedStr::from))
                                    .transpose()
                                    .map_err(|error| LixError::unknown(error.to_string()))?,
                            })
                        })
                        .transpose()?,
                    rank: ConflictRank::new(row.updated_at, change_id),
                });
        }
    }
    // Ordinary rows need replay only when at least two cohort members wrote
    // the same identity. File-backed candidates are narrowed to plugin-owned
    // semantic rows below because their projection must observe the complete
    // combined semantic delta for the file.
    candidates.retain(|key, values| key.file_id.is_some() || values.len() > 1);
    let read = transaction.opening_read();
    let mut tracked = transaction.tracked_state.reader(&read);
    let opening_registry =
        load_plugin_registry_at_commit(&mut tracked, &opening_head.to_string()).await?;
    // File-internal rows such as `lix_binary_blob_ref` are outputs of the
    // consolidated projection below, not semantic inputs to it. Replaying
    // them here would stage the old blob reference and then generate its
    // successor a second time, forcing the cohort onto the serialized
    // fallback through a duplicate-primary-key error.
    candidates
        .retain(|key, _| key.file_id.is_none() || opening_registry.owns_schema(&key.schema_key));
    let keys = candidates.keys().cloned().collect::<Vec<_>>();
    let base_rows = tracked
        .load_projected_batch_at_commit(
            &opening_head.to_string(),
            &keys,
            &ChangeRecordProjection::full(),
        )
        .await?;
    drop(tracked);
    struct CohortFrontier {
        base: Option<StaleConflictPayload>,
        current: Option<StaleConflictPayload>,
        remaining: VecDeque<Option<StaleConflictPayload>>,
        primary_key_columns: BTreeSet<String>,
        typed: bool,
        plugin: Option<PluginRegistryEntry>,
    }
    let mut frontiers = BTreeMap::<TrackedStateKey, CohortFrontier>::new();
    for (slot, key) in keys.iter().enumerate() {
        let base = stale_payload_from_tracked(base_rows.row(slot));
        let versions = candidates
            .get_mut(key)
            .expect("candidate key originates from map");
        versions.sort_by_key(|candidate| candidate.rank);
        versions.retain(|candidate| candidate.payload != base);
        let Some(first) = versions.first() else {
            continue;
        };
        let plugin = opening_registry.plugins().iter().find(|plugin| {
            plugin.has_column_merger()
                && plugin
                    .schema_keys()
                    .binary_search_by(|schema| schema.as_str().cmp(key.schema_key.as_str()))
                    .is_ok()
        });
        let typed = opening_registry.owns_schema(&key.schema_key);
        let primary_key_columns = primary_keys_by_key
            .get(key)
            .cloned()
            .expect("candidate row has primary-key metadata");
        let (current, remaining) = if !typed {
            (
                reconcile_native_frontier(base.as_ref(), versions, &primary_key_columns)?,
                VecDeque::new(),
            )
        } else {
            (
                first.payload.clone(),
                versions
                    .iter()
                    .skip(1)
                    .map(|candidate| candidate.payload.clone())
                    .collect(),
            )
        };
        frontiers.insert(
            key.clone(),
            CohortFrontier {
                base,
                current,
                remaining,
                primary_key_columns,
                typed,
                plugin: plugin.cloned(),
            },
        );
    }
    // Per-member file actors describe transitions now superseded by the
    // consolidated cohort and may otherwise consume the Store slots needed
    // by a stateless column merger.
    transaction
        .discard_pending_plugin_actor_publications()
        .await;
    while frontiers
        .values()
        .any(|frontier| !frontier.remaining.is_empty())
    {
        let mut merge_keys = Vec::new();
        let mut inputs = Vec::new();
        for (key, frontier) in &mut frontiers {
            if frontier.remaining.is_empty() {
                continue;
            }
            let next = frontier
                .remaining
                .pop_front()
                .expect("non-empty frontier has a next version");
            merge_keys.push(key.clone());
            inputs.push(StaleColumnMergeInput {
                key: key.clone(),
                base: frontier.base.clone(),
                a: frontier.current.clone(),
                b: next,
                primary_key_columns: frontier.primary_key_columns.clone(),
                typed: frontier.typed,
                plugin: frontier.plugin.clone(),
            });
        }
        let merged = transaction.merge_stale_column_inputs(&inputs).await?;
        for (key, payload) in merge_keys.into_iter().zip(merged) {
            frontiers
                .get_mut(&key)
                .expect("frontier key originates from map")
                .current = payload;
        }
    }

    let mut rows = RawWriteBatch::with_capacity(keys.len());
    for (key, frontier) in frontiers {
        push_cohort_payload(
            &mut rows,
            &key,
            frontier.current.as_ref(),
            frontier
                .current
                .as_ref()
                .and_then(|payload| payload.decoded_snapshot.clone()),
            &transaction.active_branch_id,
        );
    }

    transaction
        .stage_write(TransactionWrite::Rows {
            mode: TransactionWriteMode::Replace,
            rows,
        })
        .await?;
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
    key: &TrackedStateKey,
    payload: Option<&StaleConflictPayload>,
    decoded_snapshot: Option<Arc<WasmTypedRow>>,
    branch_id: &str,
) {
    let common = (
        Some(key.row_pk.clone()),
        SharedStr::from(key.schema_key.as_str()),
        key.file_id.as_deref().map(SharedStr::from),
        payload.and_then(|payload| {
            payload
                .metadata
                .clone()
                .map(TransactionJson::from_unvalidated_shared_normalized_content)
        }),
        SharedStr::from(branch_id),
    );
    if let Some(decoded_snapshot) = decoded_snapshot {
        rows.push_typed_parts(
            common.0,
            common.1,
            common.2,
            Some(decoded_snapshot),
            common.3,
            None,
            None,
            None,
            false,
            None,
            None,
            false,
            common.4,
        );
    } else {
        rows.push_parts(
            common.0,
            common.1,
            common.2,
            payload
                .and_then(|payload| payload.snapshot.clone())
                .map(|snapshot| {
                    TransactionJson::from_unvalidated_shared_normalized_content(snapshot)
                }),
            common.3,
            None,
            None,
            None,
            false,
            None,
            None,
            false,
            common.4,
        );
    }
}

async fn commit_prepared_individually<StorageImpl>(
    members: Vec<PreparedCohortMember<StorageImpl>>,
) -> Vec<Result<TransactionCommitOutcome, LixError>>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let mut outcomes = Vec::with_capacity(members.len());
    for member in members {
        let commit_cohort_id = crate::telemetry::current_commit_cohort_id()
            .unwrap_or_else(crate::telemetry::next_commit_cohort_id);
        let materialize_span = start_materialize_span(1, &commit_cohort_id);
        outcomes.push(
            member
                .transaction
                .commit_prepared(
                    &member.runtime_functions,
                    member.prepared_writes,
                    1,
                    commit_cohort_id,
                    materialize_span,
                )
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
