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
    let active_branch_id = transaction.active_branch_id().to_owned();
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

    let semantic_rows = async {
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
