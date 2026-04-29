use std::sync::Arc;

use crate::engine2::commit_graph::CommitGraphContext;
use crate::engine2::functions::FunctionContext;
use crate::engine2::tracked_state::{
    TrackedStateDiffRequest, TrackedStateMergePlan, TrackedStateRow,
};
use crate::engine2::transaction::types::StageRow;
use crate::engine2::transaction::Transaction;
use crate::version::GLOBAL_VERSION_ID;
use crate::LixError;

use super::context::SessionContext;

/// Options for merging another version into this session's active version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeVersionOptions {
    /// Version whose changes should be merged into the active session version.
    pub source_version_id: String,
}

/// Receipt returned after merging a version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MergeVersionReceipt {
    /// Number of source-side changes merged into the target version.
    pub merged_changes: usize,
}

impl SessionContext {
    /// Merges `source_version_id` into this session's active version.
    ///
    /// The merge is materialized as a normal tracked write in the target
    /// version. The generated target commit keeps the previous target head as
    /// its first parent and records the source head as an additional parent,
    /// so the commit graph preserves branch ancestry while tracked-state
    /// storage can still build the new root by applying source patches onto
    /// the target root.
    pub async fn merge_version(
        &self,
        options: MergeVersionOptions,
    ) -> Result<MergeVersionReceipt, LixError> {
        let live_state: Arc<dyn crate::engine2::live_state::LiveStateReader> =
            Arc::new(self.live_state.reader(Arc::clone(&self.backend)));
        let runtime_functions = FunctionContext::prepare(live_state.as_ref()).await?;
        let functions = runtime_functions.provider();
        let active_version_id = self.active_version_id().await?;

        let mut transaction = Transaction::open(
            active_version_id.clone(),
            &self.backend,
            Arc::clone(&self.live_state),
            Arc::clone(&self.binary_cas),
            Arc::clone(&self.changelog),
            Arc::clone(&self.version_ref),
            Arc::clone(&self.schema_registry),
            functions,
        )
        .await?;

        let (target_head, source_head) = {
            let reader = self.version_ref.reader(transaction.kv_store());
            let target_head = reader
                .load_head_commit_id(&active_version_id)
                .await?
                .ok_or_else(|| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "cannot merge into missing active version ref '{}'",
                            active_version_id
                        ),
                    )
                })?;
            let source_head = reader
                .load_head_commit_id(&options.source_version_id)
                .await?
                .ok_or_else(|| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "cannot merge from missing source version ref '{}'",
                            options.source_version_id
                        ),
                    )
                })?;
            (target_head, source_head)
        };

        let merge_base = {
            let commit_graph = CommitGraphContext::new(self.changelog.as_ref().clone());
            let mut reader = commit_graph.reader(transaction.kv_store());
            reader.merge_base(&target_head, &source_head).await?
        };

        let merge_plan = {
            let mut reader = self.tracked_state.reader(transaction.kv_store());
            reader
                .plan_merge(
                    &merge_base.commit_id,
                    &target_head,
                    &source_head,
                    &TrackedStateDiffRequest::default(),
                )
                .await?
        };

        if !merge_plan.conflicts.is_empty() {
            let conflict_count = merge_plan.conflicts.len();
            transaction.rollback().await?;
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("engine2 merge_version found {conflict_count} tracked-state conflict(s)"),
            ));
        }

        let rows = stage_rows_from_merge_plan(&merge_plan, &active_version_id);
        if rows.is_empty() {
            transaction.rollback().await?;
            return Ok(MergeVersionReceipt { merged_changes: 0 });
        }

        let merged_changes = rows.len();
        transaction.stage_rows(rows)?;
        transaction.add_commit_parent(active_version_id, source_head)?;
        transaction.commit(&runtime_functions).await?;
        Ok(MergeVersionReceipt { merged_changes })
    }
}

fn stage_rows_from_merge_plan(
    plan: &TrackedStateMergePlan,
    target_version_id: &str,
) -> Vec<StageRow> {
    plan.patches
        .iter()
        .map(|patch| stage_row_from_tracked_row(&patch.source_row, target_version_id))
        .collect()
}

fn stage_row_from_tracked_row(row: &TrackedStateRow, target_version_id: &str) -> StageRow {
    StageRow {
        entity_id: row.entity_id.clone(),
        schema_key: row.schema_key.clone(),
        file_id: row.file_id.clone(),
        plugin_key: row.plugin_key.clone(),
        snapshot_content: row.snapshot_content.clone(),
        metadata: row.metadata.clone(),
        schema_version: row.schema_version.clone(),
        created_at: None,
        updated_at: None,
        global: target_version_id == GLOBAL_VERSION_ID,
        change_id: None,
        commit_id: None,
        untracked: false,
        version_id: target_version_id.to_string(),
    }
}
