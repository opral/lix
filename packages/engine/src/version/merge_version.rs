use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::canonical::append::{
    append_tracked, CreateCommitArgs, CreateCommitExpectedHead, CreateCommitIdempotencyKey,
    CreateCommitPreconditions, CreateCommitWriteLane,
};
use crate::canonical::readers::{
    load_canonical_change_row_by_id, load_commit_lineage_entry_by_id,
    load_exact_committed_state_row_from_commit_with_executor, ExactCommittedStateRow,
    ExactCommittedStateRowRequest,
};
use crate::canonical::ProposedDomainChange;
use crate::engine::TransactionBackendAdapter;
use crate::functions::LixFunctionProvider;
use crate::live_state::{
    apply_live_state_scope_in_transaction, live_state_rebuild_plan_with_executor,
    LiveStateRebuildDebugMode, LiveStateRebuildRequest, LiveStateRebuildScope,
};
use crate::state::stream::{StateCommitStreamChange, StateCommitStreamOperation};
use crate::{ExecuteOptions, LixError, Session, SessionTransaction, Value};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MergeVersionOptions {
    pub source_version_id: String,
    pub target_version_id: String,
    pub expected_heads: Option<ExpectedVersionHeads>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ExpectedVersionHeads {
    pub source_head_commit_id: Option<String>,
    pub target_head_commit_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MergeVersionResult {
    pub outcome: MergeOutcome,
    pub source_version_id: String,
    pub target_version_id: String,
    pub merge_base_commit_id: Option<String>,
    pub source_head_before_commit_id: String,
    pub target_head_before_commit_id: String,
    pub target_head_after_commit_id: String,
    pub created_merge_commit_id: Option<String>,
    pub applied_change_count: usize,
    pub created_tombstone_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum MergeOutcome {
    AlreadyUpToDate,
    FastForwarded,
    MergeCommitted,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct EntityKey {
    entity_id: String,
    schema_key: String,
    file_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct VisibleEntityState {
    change_id: String,
    schema_version: String,
    plugin_key: String,
    snapshot_content: String,
    metadata: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MergeDecision {
    KeepTarget,
    ApplySource,
    Delete { previous: VisibleEntityState },
    Conflict,
}

pub(crate) async fn merge_version_in_session(
    session: &Session,
    options: MergeVersionOptions,
) -> Result<MergeVersionResult, LixError> {
    session
        .transaction(ExecuteOptions::default(), move |tx| {
            let options = options.clone();
            Box::pin(async move { merge_version_in_transaction(tx, options).await })
        })
        .await
}

async fn merge_version_in_transaction(
    tx: &mut SessionTransaction<'_>,
    options: MergeVersionOptions,
) -> Result<MergeVersionResult, LixError> {
    let source_version_id =
        normalize_required_text(options.source_version_id, "source_version_id")?;
    let target_version_id =
        normalize_required_text(options.target_version_id, "target_version_id")?;
    if source_version_id == crate::version::GLOBAL_VERSION_ID
        || target_version_id == crate::version::GLOBAL_VERSION_ID
    {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "merge_version does not support the reserved 'global' version",
        ));
    }

    let source_head = load_version_head_commit_id(tx, &source_version_id).await?;
    let target_head = load_version_head_commit_id(tx, &target_version_id).await?;
    validate_expected_heads(
        options.expected_heads.as_ref(),
        &source_head,
        &target_head,
        &source_version_id,
        &target_version_id,
    )?;

    let mut executor = TransactionBackendAdapter::new(tx.backend_transaction_mut()?);

    let source_depths = load_commit_depths(&mut executor, &source_head).await?;
    let target_depths = load_commit_depths(&mut executor, &target_head).await?;
    let merge_base_commit_id = resolve_merge_base(&source_depths, &target_depths);

    if source_version_id == target_version_id || target_depths.contains_key(&source_head) {
        return Ok(MergeVersionResult {
            outcome: MergeOutcome::AlreadyUpToDate,
            source_version_id,
            target_version_id,
            merge_base_commit_id,
            source_head_before_commit_id: source_head.clone(),
            target_head_before_commit_id: target_head.clone(),
            target_head_after_commit_id: target_head,
            created_merge_commit_id: None,
            applied_change_count: 0,
            created_tombstone_count: 0,
        });
    }

    if source_depths.contains_key(&target_head) {
        tx.execute(
            "UPDATE lix_version SET commit_id = $1 WHERE id = $2",
            &[
                Value::Text(source_head.clone()),
                Value::Text(target_version_id.clone()),
            ],
        )
        .await?;

        {
            let engine = tx.engine;
            let write_transaction = tx
                .write_transaction
                .as_mut()
                .ok_or_else(|| LixError::unknown("transaction is no longer active"))?;
            let context = &mut tx.context;
            write_transaction
                .prepare_buffered_write_commit(engine, context)
                .await?;
        }
        let transaction = tx.backend_transaction_mut()?;
        let mut versions = BTreeSet::new();
        versions.insert(target_version_id.clone());
        let plan = {
            let mut executor = &mut *transaction;
            live_state_rebuild_plan_with_executor(
                &mut executor,
                &LiveStateRebuildRequest {
                    scope: LiveStateRebuildScope::Versions(versions),
                    debug: LiveStateRebuildDebugMode::Off,
                    debug_row_limit: 0,
                },
            )
            .await?
        };
        let _ = apply_live_state_scope_in_transaction(transaction, &plan).await?;

        return Ok(MergeVersionResult {
            outcome: MergeOutcome::FastForwarded,
            source_version_id,
            target_version_id,
            merge_base_commit_id,
            source_head_before_commit_id: source_head.clone(),
            target_head_before_commit_id: target_head,
            target_head_after_commit_id: source_head,
            created_merge_commit_id: None,
            applied_change_count: 0,
            created_tombstone_count: 0,
        });
    }

    let candidate_entities =
        collect_candidate_entities(&mut executor, &source_head, merge_base_commit_id.as_deref())
            .await?
            .into_iter()
            .chain(
                collect_candidate_entities(
                    &mut executor,
                    &target_head,
                    merge_base_commit_id.as_deref(),
                )
                .await?,
            )
            .collect::<BTreeSet<_>>();

    let mut conflicts = Vec::new();
    let mut proposed_changes = Vec::new();
    let mut stream_changes = Vec::new();
    let mut applied_change_count = 0usize;
    let mut created_tombstone_count = 0usize;

    for entity in candidate_entities {
        let base_state = load_visible_entity_state_at_commit(
            &mut executor,
            merge_base_commit_id.as_deref(),
            &source_version_id,
            &entity,
        )
        .await?;
        let source_state = load_visible_entity_state_at_commit(
            &mut executor,
            Some(&source_head),
            &source_version_id,
            &entity,
        )
        .await?;
        let target_state = load_visible_entity_state_at_commit(
            &mut executor,
            Some(&target_head),
            &target_version_id,
            &entity,
        )
        .await?;

        match classify_merge_decision(
            base_state.as_ref().map(|(state, _)| state),
            source_state.as_ref().map(|(state, _)| state),
            target_state.as_ref().map(|(state, _)| state),
        ) {
            MergeDecision::KeepTarget => {}
            MergeDecision::Conflict => conflicts.push(entity),
            MergeDecision::ApplySource => {
                let (_source_state, source_row) = source_state
                    .as_ref()
                    .ok_or_else(|| LixError::unknown("merge decision expected source state"))?;
                proposed_changes.push(proposed_change_from_exact_row(
                    &target_version_id,
                    source_row,
                )?);
                stream_changes.push(stream_change_from_row(
                    if target_state.is_some() {
                        StateCommitStreamOperation::Update
                    } else {
                        StateCommitStreamOperation::Insert
                    },
                    &target_version_id,
                    source_row,
                )?);
                applied_change_count += 1;
            }
            MergeDecision::Delete { previous } => {
                proposed_changes.push(tombstone_change_from_state(
                    &target_version_id,
                    &entity,
                    &previous,
                )?);
                stream_changes.push(StateCommitStreamChange {
                    operation: StateCommitStreamOperation::Delete,
                    entity_id: entity.entity_id.clone(),
                    schema_key: entity.schema_key.clone(),
                    schema_version: previous.schema_version.clone(),
                    file_id: entity.file_id.clone(),
                    version_id: target_version_id.clone(),
                    plugin_key: previous.plugin_key.clone(),
                    snapshot_content: None,
                    untracked: false,
                    writer_key: None,
                });
                created_tombstone_count += 1;
            }
        }
    }

    if !conflicts.is_empty() {
        return Err(merge_conflict_error(
            &source_version_id,
            &target_version_id,
            &conflicts,
        ));
    }

    let engine = tx.engine;
    let active_account_ids = tx.context.active_account_ids.clone();
    let transaction = tx.backend_transaction_mut()?;
    let backend = TransactionBackendAdapter::new(transaction);
    let (_settings, _sequence_start, functions) = engine
        .prepare_runtime_functions_with_backend(&backend, true)
        .await?;
    engine
        .ensure_runtime_sequence_initialized_in_transaction(transaction, &functions)
        .await?;
    let mut functions = functions;
    let merge_result = append_tracked(
        transaction,
        CreateCommitArgs {
            timestamp: Some(functions.timestamp()),
            changes: proposed_changes,
            filesystem_state: Default::default(),
            preconditions: CreateCommitPreconditions {
                write_lane: CreateCommitWriteLane::Version(target_version_id.clone()),
                expected_head: CreateCommitExpectedHead::CommitId(target_head.clone()),
                idempotency_key: CreateCommitIdempotencyKey::Exact(format!(
                    "merge:{}:{}:{}:{}",
                    source_version_id, target_version_id, source_head, target_head
                )),
            },
            active_account_ids,
            lane_parent_commit_ids_override: Some(vec![target_head.clone(), source_head.clone()]),
            allow_empty_commit: true,
            should_emit_observe_tick: false,
            observe_tick_writer_key: None,
            writer_key: None,
        },
        &mut functions,
        None,
    )
    .await?;

    tx.record_state_commit_stream_changes(stream_changes)?;

    Ok(MergeVersionResult {
        outcome: MergeOutcome::MergeCommitted,
        source_version_id,
        target_version_id,
        merge_base_commit_id,
        source_head_before_commit_id: source_head,
        target_head_before_commit_id: target_head,
        target_head_after_commit_id: merge_result.committed_head.clone(),
        created_merge_commit_id: Some(merge_result.committed_head),
        applied_change_count,
        created_tombstone_count,
    })
}

async fn load_version_head_commit_id(
    tx: &mut SessionTransaction<'_>,
    version_id: &str,
) -> Result<String, LixError> {
    let result = tx
        .execute(
            "SELECT commit_id FROM lix_version WHERE id = $1 LIMIT 1",
            &[Value::Text(version_id.to_string())],
        )
        .await?;
    let [statement] = result.statements.as_slice() else {
        return Err(LixError::unknown(
            "expected one statement for version head lookup",
        ));
    };
    let Some(row) = statement.rows.first() else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("version '{}' does not exist", version_id),
        ));
    };
    match row.first() {
        Some(Value::Text(commit_id)) if !commit_id.is_empty() => Ok(commit_id.clone()),
        Some(other) => Err(LixError::unknown(format!(
            "expected text commit_id for version '{}', got {other:?}",
            version_id
        ))),
        None => Err(LixError::unknown(format!(
            "version '{}' is missing commit_id",
            version_id
        ))),
    }
}

fn validate_expected_heads(
    expected: Option<&ExpectedVersionHeads>,
    source_head: &str,
    target_head: &str,
    source_version_id: &str,
    target_version_id: &str,
) -> Result<(), LixError> {
    let Some(expected) = expected else {
        return Ok(());
    };
    if let Some(expected_source_head) = expected.source_head_commit_id.as_deref() {
        if expected_source_head != source_head {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "merge_version expected source version '{}' head '{}' but found '{}'",
                    source_version_id, expected_source_head, source_head
                ),
            ));
        }
    }
    if let Some(expected_target_head) = expected.target_head_commit_id.as_deref() {
        if expected_target_head != target_head {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "merge_version expected target version '{}' head '{}' but found '{}'",
                    target_version_id, expected_target_head, target_head
                ),
            ));
        }
    }
    Ok(())
}

async fn load_commit_depths(
    executor: &mut TransactionBackendAdapter<'_>,
    head_commit_id: &str,
) -> Result<BTreeMap<String, usize>, LixError> {
    let mut depths = BTreeMap::new();
    let mut queue = VecDeque::from([(head_commit_id.to_string(), 0usize)]);
    while let Some((commit_id, depth)) = queue.pop_front() {
        if depths.contains_key(&commit_id) {
            continue;
        }
        depths.insert(commit_id.clone(), depth);
        let entry = load_commit_lineage_entry_by_id(executor, &commit_id)
            .await?
            .ok_or_else(|| {
                LixError::unknown(format!("missing commit lineage entry for '{}'", commit_id))
            })?;
        let mut parents = entry.parent_commit_ids;
        parents.sort();
        for parent_id in parents {
            queue.push_back((parent_id, depth + 1));
        }
    }
    Ok(depths)
}

fn resolve_merge_base(
    source_depths: &BTreeMap<String, usize>,
    target_depths: &BTreeMap<String, usize>,
) -> Option<String> {
    source_depths
        .iter()
        .filter_map(|(commit_id, source_depth)| {
            target_depths.get(commit_id).map(|target_depth| {
                (
                    source_depth + target_depth,
                    std::cmp::max(*source_depth, *target_depth),
                    commit_id.clone(),
                )
            })
        })
        .min()
        .map(|(_, _, commit_id)| commit_id)
}

async fn collect_candidate_entities(
    executor: &mut TransactionBackendAdapter<'_>,
    head_commit_id: &str,
    stop_at_commit_id: Option<&str>,
) -> Result<BTreeSet<EntityKey>, LixError> {
    let mut entities = BTreeSet::new();
    let mut visited = BTreeSet::new();
    let mut queue = VecDeque::from([head_commit_id.to_string()]);
    while let Some(commit_id) = queue.pop_front() {
        if !visited.insert(commit_id.clone()) {
            continue;
        }
        if stop_at_commit_id == Some(commit_id.as_str()) {
            continue;
        }
        let entry = load_commit_lineage_entry_by_id(executor, &commit_id)
            .await?
            .ok_or_else(|| {
                LixError::unknown(format!("missing commit lineage entry for '{}'", commit_id))
            })?;
        for change_id in entry.change_ids {
            let Some(change) = load_canonical_change_row_by_id(executor, &change_id).await? else {
                return Err(LixError::unknown(format!(
                    "missing canonical change row '{}'",
                    change_id
                )));
            };
            entities.insert(EntityKey {
                entity_id: change.entity_id,
                schema_key: change.schema_key,
                file_id: change.file_id,
            });
        }
        let mut parents = entry.parent_commit_ids;
        parents.sort();
        for parent_id in parents {
            queue.push_back(parent_id);
        }
    }
    Ok(entities)
}

async fn load_visible_entity_state_at_commit(
    executor: &mut TransactionBackendAdapter<'_>,
    head_commit_id: Option<&str>,
    version_id: &str,
    entity: &EntityKey,
) -> Result<Option<(VisibleEntityState, ExactCommittedStateRow)>, LixError> {
    let Some(head_commit_id) = head_commit_id else {
        return Ok(None);
    };
    let Some(row) = load_exact_committed_state_row_from_commit_with_executor(
        executor,
        head_commit_id,
        &ExactCommittedStateRowRequest {
            entity_id: entity.entity_id.clone(),
            schema_key: entity.schema_key.clone(),
            version_id: version_id.to_string(),
            exact_filters: BTreeMap::from([(
                "file_id".to_string(),
                Value::Text(entity.file_id.clone()),
            )]),
        },
    )
    .await?
    else {
        return Ok(None);
    };
    let state = visible_state_from_exact_row(&row)?;
    Ok(Some((state, row)))
}

fn visible_state_from_exact_row(
    row: &ExactCommittedStateRow,
) -> Result<VisibleEntityState, LixError> {
    let schema_version = required_text(row.values.get("schema_version"), "schema_version")?;
    let plugin_key = required_text(row.values.get("plugin_key"), "plugin_key")?;
    let snapshot_content = required_text(row.values.get("snapshot_content"), "snapshot_content")?;
    let change_id = row.source_change_id.clone().ok_or_else(|| {
        LixError::unknown("exact committed state row is missing source_change_id")
    })?;
    Ok(VisibleEntityState {
        change_id,
        schema_version,
        plugin_key,
        snapshot_content,
        metadata: row.values.get("metadata").and_then(value_ref_as_text),
    })
}

fn classify_merge_decision(
    base: Option<&VisibleEntityState>,
    source: Option<&VisibleEntityState>,
    target: Option<&VisibleEntityState>,
) -> MergeDecision {
    let source_changed = base != source;
    let target_changed = base != target;
    match (source_changed, target_changed) {
        (false, false) => MergeDecision::KeepTarget,
        (true, false) => match source {
            Some(_) => MergeDecision::ApplySource,
            None => target
                .cloned()
                .map(|previous| MergeDecision::Delete { previous })
                .unwrap_or(MergeDecision::KeepTarget),
        },
        (false, true) => MergeDecision::KeepTarget,
        (true, true) => {
            if source == target {
                MergeDecision::KeepTarget
            } else {
                MergeDecision::Conflict
            }
        }
    }
}

fn proposed_change_from_exact_row(
    version_id: &str,
    row: &ExactCommittedStateRow,
) -> Result<ProposedDomainChange, LixError> {
    Ok(ProposedDomainChange {
        entity_id: parse_identity(row.entity_id.clone(), "merge entity_id")?,
        schema_key: parse_identity(row.schema_key.clone(), "merge schema_key")?,
        schema_version: Some(parse_identity(
            required_text(row.values.get("schema_version"), "schema_version")?,
            "merge schema_version",
        )?),
        file_id: Some(parse_identity(row.file_id.clone(), "merge file_id")?),
        plugin_key: Some(parse_identity(
            required_text(row.values.get("plugin_key"), "plugin_key")?,
            "merge plugin_key",
        )?),
        snapshot_content: Some(required_text(
            row.values.get("snapshot_content"),
            "snapshot_content",
        )?),
        metadata: row.values.get("metadata").and_then(value_ref_as_text),
        version_id: parse_identity(version_id.to_string(), "merge version_id")?,
        writer_key: None,
    })
}

fn tombstone_change_from_state(
    version_id: &str,
    entity: &EntityKey,
    previous: &VisibleEntityState,
) -> Result<ProposedDomainChange, LixError> {
    Ok(ProposedDomainChange {
        entity_id: parse_identity(entity.entity_id.clone(), "merge tombstone entity_id")?,
        schema_key: parse_identity(entity.schema_key.clone(), "merge tombstone schema_key")?,
        schema_version: Some(parse_identity(
            previous.schema_version.clone(),
            "merge tombstone schema_version",
        )?),
        file_id: Some(parse_identity(
            entity.file_id.clone(),
            "merge tombstone file_id",
        )?),
        plugin_key: Some(parse_identity(
            previous.plugin_key.clone(),
            "merge tombstone plugin_key",
        )?),
        snapshot_content: None,
        metadata: None,
        version_id: parse_identity(version_id.to_string(), "merge tombstone version_id")?,
        writer_key: None,
    })
}

fn stream_change_from_row(
    operation: StateCommitStreamOperation,
    version_id: &str,
    row: &ExactCommittedStateRow,
) -> Result<StateCommitStreamChange, LixError> {
    Ok(StateCommitStreamChange {
        operation,
        entity_id: row.entity_id.clone(),
        schema_key: row.schema_key.clone(),
        schema_version: required_text(row.values.get("schema_version"), "schema_version")?,
        file_id: row.file_id.clone(),
        version_id: version_id.to_string(),
        plugin_key: required_text(row.values.get("plugin_key"), "plugin_key")?,
        snapshot_content: Some(
            serde_json::from_str(&required_text(
                row.values.get("snapshot_content"),
                "snapshot_content",
            )?)
            .map_err(|error| {
                LixError::unknown(format!(
                    "merge_version expected JSON snapshot_content text: {error}"
                ))
            })?,
        ),
        untracked: false,
        writer_key: None,
    })
}

fn merge_conflict_error(
    source_version_id: &str,
    target_version_id: &str,
    conflicts: &[EntityKey],
) -> LixError {
    let details = conflicts
        .iter()
        .take(5)
        .map(|conflict| {
            format!(
                "{}:{}:{}",
                conflict.schema_key, conflict.file_id, conflict.entity_id
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let suffix = if conflicts.len() > 5 {
        format!(", and {} more", conflicts.len() - 5)
    } else {
        String::new()
    };
    LixError::new(
        "LIX_ERROR_MERGE_CONFLICT",
        format!(
            "merge_version found {} conflicting Entities while merging '{}' into '{}': {}{}",
            conflicts.len(),
            source_version_id,
            target_version_id,
            details,
            suffix
        ),
    )
}

fn normalize_required_text(value: String, field: &str) -> Result<String, LixError> {
    if value.trim().is_empty() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("{field} must be a non-empty string"),
        ));
    }
    Ok(value)
}

fn required_text(value: Option<&Value>, field: &str) -> Result<String, LixError> {
    value_as_text(value).ok_or_else(|| LixError::unknown(format!("missing {field}")))
}

fn value_as_text(value: Option<&Value>) -> Option<String> {
    match value {
        Some(Value::Text(text)) => Some(text.clone()),
        Some(Value::Integer(value)) => Some(value.to_string()),
        Some(Value::Boolean(value)) => Some(value.to_string()),
        Some(Value::Real(value)) => Some(value.to_string()),
        _ => None,
    }
}

fn value_ref_as_text(value: &Value) -> Option<String> {
    value_as_text(Some(value))
}

fn parse_identity<T>(value: String, context: &str) -> Result<T, LixError>
where
    T: TryFrom<String, Error = LixError>,
{
    T::try_from(value)
        .map_err(|error| LixError::unknown(format!("{context} is invalid: {}", error.description)))
}
