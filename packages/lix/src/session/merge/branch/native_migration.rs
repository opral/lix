//! Explicit migration stages native selections and plugin resolutions.
//! Transaction admission supplies the exact base and validates all selected
//! rows before atomically publishing the native outcome receipt.
use super::*;
pub(crate) async fn stage_merge_native_heads<S: Storage + Clone + Send + Sync + 'static>(
    transaction: &mut crate::transaction::Transaction<S>,
    source_branch_id: String,
    expected_base: crate::changelog::CommitId,
    target_head: crate::changelog::CommitId,
    source_head: crate::changelog::CommitId,
) -> Result<MergeBranchReceipt, LixError> {
    let merge_base = async {
        let mut reader = transaction.commit_graph_reader().await?;
        reader.merge_base(&target_head, &source_head).await
    }
    .instrument(tracing::debug_span!(
        target: "lix_perf",
        "lix.perf.merge_base"
    ))
    .await?;
    if merge_base != expected_base || merge_base == source_head || target_head == source_head {
        return Err(LixError::new(
            "LIX_MIGRATION_MERGE_SCOPE_UNSUPPORTED",
            "native migration requires the captured exact merge base and distinct heads",
        ));
    }
    let base_commit_id = merge_base;
    let analysis = async {
        let mut reader = transaction.tracked_state_reader().await?;
        super::super::analysis::analyze_native_migration(
            &mut reader,
            MergeCommits {
                base_commit_id,
                target_commit_id: target_head,
                source_commit_id: source_head,
            },
        )
        .await
    }
    .instrument(tracing::debug_span!(
        target: "lix_perf",
        "lix.perf.merge_analysis"
    ))
    .await?;
    stage_native_change_application(transaction, source_branch_id, &analysis).await
}

/// Apply an already bounded, authenticated native change plan through the same
/// row/plugin reconciliation and file materialization owner as branch merges.
pub(crate) async fn stage_native_change_application<S: Storage + Clone + Send + Sync + 'static>(
    transaction: &mut crate::transaction::Transaction<S>,
    source_branch_id: String,
    analysis: &super::super::analysis::MergeAnalysis,
) -> Result<MergeBranchReceipt, LixError> {
    let adjusted = transaction
        .prepare_incoming_file_lifecycle(analysis)
        .await?;
    let analysis = adjusted.as_ref().unwrap_or(analysis);
    let active_branch_id = transaction.active_branch_id().to_owned();
    let derived_blob_files = async {
        let mut reader = transaction.tracked_state_reader().await?;
        derived_plugin_blob_conflicts(&mut reader, &analysis).await
    }
    .instrument(tracing::debug_span!(
        target: "lix_perf",
        "lix.perf.merge_derived_blob_detection"
    ))
    .await?;

    if analysis.outcome != MergeOutcome::MergeCommitted {
        return Err(LixError::new(
            "LIX_MIGRATION_MERGE_SCOPE_UNSUPPORTED",
            "migration analysis omitted a validated native publication plan",
        ));
    }

    let merge_plan = analysis
        .merge_plan()
        .expect("merge analysis should include a plan for mergeCommitted");

    let semantic_branch_id = SharedStr::from(active_branch_id.as_str());
    let resolved_plugin_rows = resolve_row_merge_conflicts(
        transaction,
        &analysis,
        &derived_blob_files,
        &semantic_branch_id,
    )
    .instrument(tracing::debug_span!(
        target: "lix_perf",
        "lix.perf.merge_plugin_conflict_resolve"
    ))
    .await?;
    let plugin_resolution_stats = async {
        let mut reader = transaction.tracked_state_reader().await?;
        plugin_resolution_change_stats(&mut reader, &analysis, &resolved_plugin_rows).await
    }
    .instrument(tracing::debug_span!(
        target: "lix_perf",
        "lix.perf.merge_plugin_resolution_stats"
    ))
    .await?;

    let mut semantic_rows = async {
        let mut reader = transaction.tracked_state_reader().await?;
        materialized_plugin_merge_rows(
            &mut reader,
            &analysis,
            &derived_blob_files,
            &semantic_branch_id,
            resolved_plugin_rows,
        )
        .await
    }
    .instrument(tracing::debug_span!(
        target: "lix_perf",
        "lix.perf.merge_materialized_rows"
    ))
    .await?;
    transaction
        .retire_incoming_file_path_occupants(analysis, &semantic_rows)
        .await?;
    if derived_blob_files.materialize_filesystem {
        let indices = semantic_rows
            .iter()
            .enumerate()
            .filter(|(_, row)| is_filesystem_descriptor(row.schema_key.as_str()))
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        let remaining = (0..semantic_rows.len())
            .filter(|index| indices.binary_search(index).is_err())
            .collect::<Vec<_>>();
        let descriptors = semantic_rows.take_rows(&indices);
        semantic_rows = semantic_rows.take_rows(&remaining);
        if !descriptors.is_empty() {
            transaction
                .stage_write(TransactionWrite::Rows {
                    mode: TransactionWriteMode::Replace,
                    rows: descriptors,
                })
                .await?;
        }
    }
    let (semantic_rows, source_equal_picks) = {
        let mut reader = transaction.tracked_state_reader().await?;
        retain_exact_source_conflict_rows(&mut reader, analysis, &derived_blob_files, semantic_rows)
            .await?
    };
    if !semantic_rows.is_empty() {
        transaction
            .stage_write(TransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: semantic_rows,
            })
            .instrument(tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.merge_stage_semantic_rows"
            ))
            .await?;
    }
    let created_merge_commit_id = tracing::debug_span!(
        target: "lix_perf",
        "lix.perf.merge_stage_commit"
    )
    .in_scope(|| {
        let mut selected_changes =
            StagedCommitChangeBatchBuilder::with_capacity(merge_plan.picks.len());
        for pick in merge_plan
            .picks
            .iter()
            .filter(|pick| !pick_is_derived_plugin_state(pick, &derived_blob_files))
        {
            selected_changes.push(
                pick.identity.clone(),
                pick.selected_row.commit_id,
                pick.change_id,
                pick.selected_row.deleted,
                pick.selected_row.created_at,
                pick.selected_row.updated_at,
            );
        }
        for pick in source_equal_picks {
            selected_changes.push(
                pick.identity,
                pick.selected_row.commit_id,
                pick.change_id,
                pick.selected_row.deleted,
                pick.selected_row.created_at,
                pick.selected_row.updated_at,
            );
        }
        transaction.stage_merge_commit(
            active_branch_id.clone(),
            analysis.commits.source_commit_id,
            selected_changes.finish(),
        )
    })?;
    Ok(MergeBranchReceipt {
        outcome: MergeBranchOutcome::MergeCommitted,
        target_branch_id: active_branch_id,
        source_branch_id,
        base_commit_id: analysis.commits.base_commit_id.to_string(),
        target_head_after_commit_id: created_merge_commit_id.clone(),
        target_head_before_commit_id: analysis.commits.target_commit_id.to_string(),
        source_head_before_commit_id: analysis.commits.source_commit_id.to_string(),
        created_merge_commit_id: Some(created_merge_commit_id),
        change_stats: merge_change_stats_with_plugin_resolutions(
            &analysis.stats,
            &plugin_resolution_stats,
        ),
    })
}

/// Reconciliation has already run, including every applicable component hook.
/// An unchanged incoming result can retain its authenticated original identity.
/// Derived file rows still pass through ordinary staging so serialization sees
/// the complete combined semantic state. Descriptors retain their existing
/// staging/namespace-validation ordering as well.
async fn retain_exact_source_conflict_rows<S>(
    reader: &mut TrackedStateStoreReader<S>,
    analysis: &super::super::analysis::MergeAnalysis,
    derived: &DerivedPluginConflictIndex,
    mut rows: RawWriteBatch,
) -> Result<(RawWriteBatch, Vec<TrackedStateMergePick>), LixError>
where
    S: crate::storage_adapter::StorageAdapterRead,
{
    let plan = analysis
        .merge_plan()
        .expect("resolved conflicts require a native plan");
    let conflicts = plan
        .conflicts
        .iter()
        .map(|conflict| {
            (
                (
                    conflict.identity.schema_key(),
                    conflict.identity.file_id(),
                    conflict.identity.row_pk(),
                ),
                conflict,
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut candidates = Vec::new();
    let mut keys = Vec::new();
    for (index, row) in rows.iter().enumerate() {
        if is_filesystem_descriptor(row.schema_key.as_str())
            || row
                .file_id
                .is_some_and(|id| derived.owner(id.as_str()).is_some())
        {
            continue;
        }
        let Some(pk) = row.row_pk else {
            continue;
        };
        let Some(conflict) = conflicts.get(&(
            row.schema_key.as_str(),
            row.file_id.map(|id| id.as_str()),
            pk,
        )) else {
            continue;
        };
        candidates.push((index, *conflict));
        keys.push(TrackedStateKey {
            schema_key: row.schema_key.to_string(),
            file_id: row.file_id.map(ToString::to_string),
            row_pk: pk.clone(),
        });
    }
    if candidates.is_empty() {
        return Ok((rows, Vec::new()));
    }
    let source = reader
        .load_projected_batch_at_commit(
            &analysis.commits.source_commit_id.to_string(),
            &keys,
            &ChangeRecordProjection::full(),
        )
        .await?;
    let mut reused = BTreeSet::new();
    let mut picks = Vec::new();
    for (slot, (index, conflict)) in candidates.into_iter().enumerate() {
        let source_row = source.row(slot);
        verify_historical_conflict_row_ref(source_row, conflict.source.after.as_ref(), "source")?;
        let Some(source_row) = source_row else {
            continue;
        };
        let expected = conflict.source.after.as_ref().expect("verified source row");
        if source_row.deleted() != expected.deleted
            || source_row.schema_key() != conflict.identity.schema_key()
            || source_row.file_id() != conflict.identity.file_id()
            || source_row.row_pk() != conflict.identity.row_pk()
        {
            return Err(LixError::unknown(
                "source reuse identity differs from merge analysis",
            ));
        }
        let row = rows.row(index);
        if !resolved_row_equals_native_source(row, source_row)? {
            continue;
        }
        let selected_row = conflict
            .source
            .after
            .as_ref()
            .expect("verified source row")
            .clone();
        picks.push(TrackedStateMergePick {
            identity: conflict.identity.clone(),
            change_id: selected_row.change_id,
            selected_row,
        });
        reused.insert(index);
    }
    if picks.is_empty() {
        return Ok((rows, picks));
    }
    let remaining = (0..rows.len())
        .filter(|index| !reused.contains(index))
        .collect::<Vec<_>>();
    Ok((rows.take_rows(&remaining), picks))
}

fn resolved_row_equals_native_source(
    row: crate::transaction_types::RawWriteRowRef<'_>,
    source: MaterializedTrackedStateRowRef<'_>,
) -> Result<bool, LixError> {
    if source.deleted() {
        // Reuse only an exact canonical tombstone, not an absent key.
        return Ok(row.snapshot.is_none() && row.metadata.is_none() && source.metadata().is_none());
    }
    let snapshot_equal = match (row.decoded_snapshot(), row.snapshot_json()) {
        (Some(resolved), _) => source
            .decoded_snapshot()
            .is_some_and(|original| original == resolved),
        (_, Some(resolved)) => {
            let original = historical_live_payload_ref(Some(source), false)?
                .expect("live canonical source has a payload");
            let MergeConflictSnapshot::Json(original) = original.snapshot else {
                unreachable!("requested canonical JSON view");
            };
            let original = serde_json::from_str::<JsonValue>(&original).map_err(|error| {
                LixError::unknown(format!("invalid canonical source payload: {error}"))
            })?;
            **resolved == original
        }
        _ => false,
    };
    if !snapshot_equal {
        return Ok(false);
    }
    let source_metadata = source
        .metadata()
        .map(|metadata| {
            serde_json::from_str::<JsonValue>(metadata).map_err(|error| {
                LixError::unknown(format!("invalid canonical source metadata: {error}"))
            })
        })
        .transpose()?;
    Ok(row.metadata.map(|metadata| &**metadata) == source_metadata.as_ref())
}

#[cfg(test)]
mod source_reuse_tests {
    use super::*;

    #[test]
    fn source_reuse_requires_matching_payload_and_metadata() {
        let native = crate::tracked_state::MaterializedTrackedStateBatch::from_rows(vec![
            MaterializedTrackedStateRow {
                row_pk: RowPk::single("key"),
                schema_key: "lix_key_value".into(),
                file_id: None,
                snapshot_content: Some(r#"{"key":"key","value":"incoming"}"#.into()),
                decoded_snapshot: None,
                metadata: Some(r#"{"tag":"incoming"}"#.into()),
                deleted: false,
                created_at: "2026-01-01T00:00:00Z".into(),
                updated_at: "2026-01-01T00:00:00Z".into(),
                change_id: crate::changelog::ChangeId::for_test_label("source-reuse-row"),
                commit_id: crate::changelog::CommitId::for_test_label("source-reuse-commit"),
            },
        ])
        .unwrap();
        for (value, metadata, expected) in [
            ("incoming", Some("incoming"), true),
            ("combined", Some("incoming"), false),
            ("incoming", Some("target"), false),
            ("incoming", None, false),
        ] {
            let mut rows = RawWriteBatch::new();
            rows.push_parts(
                Some(RowPk::single("key")),
                SharedStr::from("lix_key_value"),
                None,
                Some(
                    TransactionJson::from_value(json!({"key":"key","value":value}), "test")
                        .unwrap(),
                ),
                metadata
                    .map(|tag| TransactionJson::from_value(json!({"tag":tag}), "test").unwrap()),
                None,
                None,
                None,
                false,
                None,
                None,
                false,
                SharedStr::from("branch"),
            );
            assert_eq!(
                resolved_row_equals_native_source(rows.row(0), native.row(0)).unwrap(),
                expected
            );
        }
    }
}
