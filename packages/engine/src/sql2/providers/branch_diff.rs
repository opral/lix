use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::TableType;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use tokio::sync::Mutex;

use crate::LixError;
use crate::branch::BranchRefReader;
use crate::commit_graph::CommitGraphReader;
use crate::session::{
    BranchDiff, BranchDiffChangeKind, MergeBranchOutcome, MergeConflict, MergeConflictChangeKind,
    MergeConflictKind, branch_diff_from_readers,
};
use crate::sql2::error::lix_error_to_datafusion_error;
use crate::sql2::result_metadata::json_field;
use crate::sql2::{SqlChangelogQuerySource, WriteAccess};
use crate::storage_adapter::StorageAdapterRead;

use super::checkpoint::filter_conjuncts;
use super::columns::{Col, ColumnTable, ColumnTableError};
use super::file::{FileIdConstraint, exact_string_column_constraint_from_filters};
use super::spec::{PlannedScan, TableSpec, projected_schema, register_spec_table, row_source};

/// Registers the review surface that returns the authored source changes from
/// the pair's merge base, plus per-row merge outcome metadata.
pub(super) async fn register_branch_diff_provider<S>(
    session: &datafusion::prelude::SessionContext,
    surface_name: &str,
    branch_ref: Arc<dyn BranchRefReader>,
    commit_graph: Box<dyn CommitGraphReader>,
    query_source: SqlChangelogQuerySource<S>,
) -> Result<(), LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    register_branch_review_provider(
        session,
        surface_name,
        BranchReviewSurface::Diff,
        branch_ref,
        commit_graph,
        query_source,
    )
    .await
}

/// Registers the review surface that returns merge conflicts for the exact
/// same source/target pair accepted by [`register_branch_diff_provider`].
pub(super) async fn register_branch_merge_conflict_provider<S>(
    session: &datafusion::prelude::SessionContext,
    surface_name: &str,
    branch_ref: Arc<dyn BranchRefReader>,
    commit_graph: Box<dyn CommitGraphReader>,
    query_source: SqlChangelogQuerySource<S>,
) -> Result<(), LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    register_branch_review_provider(
        session,
        surface_name,
        BranchReviewSurface::Conflict,
        branch_ref,
        commit_graph,
        query_source,
    )
    .await
}

async fn register_branch_review_provider<S>(
    session: &datafusion::prelude::SessionContext,
    surface_name: &str,
    surface: BranchReviewSurface,
    branch_ref: Arc<dyn BranchRefReader>,
    commit_graph: Box<dyn CommitGraphReader>,
    query_source: SqlChangelogQuerySource<S>,
) -> Result<(), LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    register_spec_table(
        session,
        surface_name,
        Arc::new(BranchReviewSpec {
            surface_name: surface_name.to_string(),
            surface,
            branch_ref,
            commit_graph: Arc::new(Mutex::new(commit_graph)),
            store: query_source.store,
        }),
        WriteAccess::read_only(),
    )
}

#[derive(Clone, Copy)]
enum BranchReviewSurface {
    Diff,
    Conflict,
}

struct BranchReviewSpec<S> {
    surface_name: String,
    surface: BranchReviewSurface,
    branch_ref: Arc<dyn BranchRefReader>,
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    store: S,
}

#[async_trait]
impl<S> TableSpec for BranchReviewSpec<S>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    fn table_name(&self) -> &str {
        &self.surface_name
    }

    fn schema(&self) -> SchemaRef {
        match self.surface {
            BranchReviewSurface::Diff => branch_diff_schema(),
            BranchReviewSurface::Conflict => branch_merge_conflict_schema(),
        }
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn filter_pushdown(&self, filter: &Expr) -> TableProviderFilterPushDown {
        if filter.column_refs().iter().any(|column| {
            matches!(
                column.name.as_str(),
                "source_branch_id" | "target_branch_id"
            )
        }) {
            // Pair predicates route this virtual table but remain residual so
            // the SQL engine enforces its normal expression semantics too.
            TableProviderFilterPushDown::Inexact
        } else {
            TableProviderFilterPushDown::Unsupported
        }
    }

    async fn plan_scan(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
        _props: &ExecutionProps,
    ) -> Result<PlannedScan> {
        let schema = projected_schema(&self.schema(), projection);
        let pair = BranchPairRoute::from_filters(filters)?;
        Ok(PlannedScan {
            schema: Arc::clone(&schema),
            ordering: None,
            load: row_source(
                (
                    self.surface,
                    Arc::clone(&self.branch_ref),
                    Arc::clone(&self.commit_graph),
                    self.store.clone(),
                    schema,
                    pair,
                ),
                move |(surface, branch_ref, commit_graph, store, schema, pair)| async move {
                    let review = {
                        let mut graph = commit_graph.lock().await;
                        branch_diff_from_readers(
                            branch_ref.as_ref(),
                            graph.as_mut(),
                            store,
                            &pair.source_branch_id,
                            &pair.target_branch_id,
                        )
                        .await
                        .map_err(lix_error_to_datafusion_error)?
                    };
                    match surface {
                        BranchReviewSurface::Diff => {
                            let rows = review
                                .changes
                                .iter()
                                .map(|change| BranchDiffSqlRow::from_review(&review, change))
                                .collect::<Vec<_>>();
                            BRANCH_DIFF_COLS
                                .build(schema, &rows)
                                .map_err(branch_review_batch_error)
                        }
                        BranchReviewSurface::Conflict => {
                            let rows = review
                                .conflicts
                                .iter()
                                .map(|conflict| {
                                    BranchMergeConflictSqlRow::from_review(&review, conflict)
                                })
                                .collect::<Vec<_>>();
                            BRANCH_MERGE_CONFLICT_COLS
                                .build(schema, &rows)
                                .map_err(branch_review_batch_error)
                        }
                    }
                },
            ),
        })
    }
}

/// A branch-pair virtual relation has no useful all-pairs meaning. Requiring
/// one exact source and target bounds the work to one merge-base calculation
/// and avoids a branch-count-squared scan by accident.
#[derive(Clone, Debug)]
struct BranchPairRoute {
    source_branch_id: String,
    target_branch_id: String,
}

impl BranchPairRoute {
    fn from_filters(filters: &[Expr]) -> Result<Self> {
        let conjuncts = filter_conjuncts(filters);
        let source = exact_string_column_constraint_from_filters(&conjuncts, "source_branch_id")?;
        let target = exact_string_column_constraint_from_filters(&conjuncts, "target_branch_id")?;
        Ok(Self {
            source_branch_id: require_exact_branch_pair_member("source_branch_id", source)?,
            target_branch_id: require_exact_branch_pair_member("target_branch_id", target)?,
        })
    }
}

fn require_exact_branch_pair_member(
    column_name: &str,
    constraint: FileIdConstraint,
) -> Result<String> {
    match constraint {
        FileIdConstraint::Ids(values) if values.len() == 1 => Ok(values
            .into_iter()
            .next()
            .expect("single branch-pair constraint should contain one value")),
        FileIdConstraint::All => Err(branch_pair_filter_error(format!(
            "lix branch review surfaces require an exact '{column_name}' predicate"
        ))),
        FileIdConstraint::None => Err(branch_pair_filter_error(format!(
            "lix branch review surface predicates for '{column_name}' are contradictory"
        ))),
        FileIdConstraint::Ids(_) => Err(branch_pair_filter_error(format!(
            "lix branch review surfaces require exactly one '{column_name}' value"
        ))),
    }
}

fn branch_pair_filter_error(message: String) -> DataFusionError {
    lix_error_to_datafusion_error(
        LixError::new(LixError::CODE_INVALID_PARAM, message).with_hint(
            "Filter by one source_branch_id and one target_branch_id, for example: WHERE source_branch_id = ? AND target_branch_id = ?.",
        ),
    )
}

pub(super) fn branch_diff_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("source_branch_id", DataType::Utf8, false),
        Field::new("target_branch_id", DataType::Utf8, false),
        Field::new("base_commit_id", DataType::Utf8, false),
        Field::new("source_head_commit_id", DataType::Utf8, false),
        Field::new("target_head_commit_id", DataType::Utf8, false),
        Field::new("merge_outcome", DataType::Utf8, false),
        json_field("entity_pk", false),
        Field::new("schema_key", DataType::Utf8, false),
        Field::new("file_id", DataType::Utf8, true),
        Field::new("change_kind", DataType::Utf8, false),
        Field::new("before_change_id", DataType::Utf8, true),
        Field::new("after_change_id", DataType::Utf8, true),
    ]))
}

pub(super) fn branch_merge_conflict_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("source_branch_id", DataType::Utf8, false),
        Field::new("target_branch_id", DataType::Utf8, false),
        Field::new("base_commit_id", DataType::Utf8, false),
        Field::new("source_head_commit_id", DataType::Utf8, false),
        Field::new("target_head_commit_id", DataType::Utf8, false),
        Field::new("merge_outcome", DataType::Utf8, false),
        Field::new("conflict_kind", DataType::Utf8, false),
        json_field("entity_pk", false),
        Field::new("schema_key", DataType::Utf8, false),
        Field::new("file_id", DataType::Utf8, true),
        Field::new("target_change_kind", DataType::Utf8, false),
        Field::new("target_before_change_id", DataType::Utf8, true),
        Field::new("target_after_change_id", DataType::Utf8, true),
        Field::new("source_change_kind", DataType::Utf8, false),
        Field::new("source_before_change_id", DataType::Utf8, true),
        Field::new("source_after_change_id", DataType::Utf8, true),
    ]))
}

struct BranchDiffSqlRow {
    source_branch_id: String,
    target_branch_id: String,
    base_commit_id: String,
    source_head_commit_id: String,
    target_head_commit_id: String,
    merge_outcome: &'static str,
    entity_pk: String,
    schema_key: String,
    file_id: Option<String>,
    change_kind: &'static str,
    before_change_id: Option<String>,
    after_change_id: Option<String>,
}

impl BranchDiffSqlRow {
    fn from_review(review: &BranchDiff, change: &crate::session::BranchDiffEntry) -> Self {
        Self {
            source_branch_id: review.source_branch_id.clone(),
            target_branch_id: review.target_branch_id.clone(),
            base_commit_id: review.base_commit_id.clone(),
            source_head_commit_id: review.source_head_commit_id.clone(),
            target_head_commit_id: review.target_head_commit_id.clone(),
            merge_outcome: merge_outcome_label(review.outcome),
            entity_pk: change.entity_pk.to_string(),
            schema_key: change.schema_key.clone(),
            file_id: change.file_id.clone(),
            change_kind: branch_diff_change_kind_label(change.kind),
            before_change_id: change.before_change_id.clone(),
            after_change_id: change.after_change_id.clone(),
        }
    }
}

struct BranchMergeConflictSqlRow {
    source_branch_id: String,
    target_branch_id: String,
    base_commit_id: String,
    source_head_commit_id: String,
    target_head_commit_id: String,
    merge_outcome: &'static str,
    conflict_kind: &'static str,
    entity_pk: String,
    schema_key: String,
    file_id: Option<String>,
    target_change_kind: &'static str,
    target_before_change_id: Option<String>,
    target_after_change_id: Option<String>,
    source_change_kind: &'static str,
    source_before_change_id: Option<String>,
    source_after_change_id: Option<String>,
}

impl BranchMergeConflictSqlRow {
    fn from_review(review: &BranchDiff, conflict: &MergeConflict) -> Self {
        Self {
            source_branch_id: review.source_branch_id.clone(),
            target_branch_id: review.target_branch_id.clone(),
            base_commit_id: review.base_commit_id.clone(),
            source_head_commit_id: review.source_head_commit_id.clone(),
            target_head_commit_id: review.target_head_commit_id.clone(),
            merge_outcome: merge_outcome_label(review.outcome),
            conflict_kind: match conflict.kind {
                MergeConflictKind::SameEntityChanged => "same_entity_changed",
            },
            entity_pk: conflict.entity_pk.to_string(),
            schema_key: conflict.schema_key.clone(),
            file_id: conflict.file_id.clone(),
            target_change_kind: merge_conflict_change_kind_label(conflict.target.kind),
            target_before_change_id: conflict.target.before_change_id.clone(),
            target_after_change_id: conflict.target.after_change_id.clone(),
            source_change_kind: merge_conflict_change_kind_label(conflict.source.kind),
            source_before_change_id: conflict.source.before_change_id.clone(),
            source_after_change_id: conflict.source.after_change_id.clone(),
        }
    }
}

fn merge_outcome_label(outcome: MergeBranchOutcome) -> &'static str {
    match outcome {
        MergeBranchOutcome::AlreadyUpToDate => "already_up_to_date",
        MergeBranchOutcome::FastForward => "fast_forward",
        MergeBranchOutcome::MergeCommitted => "merge_committed",
    }
}

fn branch_diff_change_kind_label(kind: BranchDiffChangeKind) -> &'static str {
    match kind {
        BranchDiffChangeKind::Added => "added",
        BranchDiffChangeKind::Modified => "modified",
        BranchDiffChangeKind::Removed => "removed",
    }
}

fn merge_conflict_change_kind_label(kind: MergeConflictChangeKind) -> &'static str {
    match kind {
        MergeConflictChangeKind::Added => "added",
        MergeConflictChangeKind::Modified => "modified",
        MergeConflictChangeKind::Removed => "removed",
    }
}

static BRANCH_DIFF_COLS: ColumnTable<BranchDiffSqlRow> = ColumnTable {
    columns: &[
        (
            "source_branch_id",
            Col::Utf8(|row| Some(&row.source_branch_id)),
        ),
        (
            "target_branch_id",
            Col::Utf8(|row| Some(&row.target_branch_id)),
        ),
        ("base_commit_id", Col::Utf8(|row| Some(&row.base_commit_id))),
        (
            "source_head_commit_id",
            Col::Utf8(|row| Some(&row.source_head_commit_id)),
        ),
        (
            "target_head_commit_id",
            Col::Utf8(|row| Some(&row.target_head_commit_id)),
        ),
        ("merge_outcome", Col::Utf8(|row| Some(row.merge_outcome))),
        ("entity_pk", Col::Utf8(|row| Some(&row.entity_pk))),
        ("schema_key", Col::Utf8(|row| Some(&row.schema_key))),
        ("file_id", Col::Utf8(|row| row.file_id.as_deref())),
        ("change_kind", Col::Utf8(|row| Some(row.change_kind))),
        (
            "before_change_id",
            Col::Utf8(|row| row.before_change_id.as_deref()),
        ),
        (
            "after_change_id",
            Col::Utf8(|row| row.after_change_id.as_deref()),
        ),
    ],
};

static BRANCH_MERGE_CONFLICT_COLS: ColumnTable<BranchMergeConflictSqlRow> = ColumnTable {
    columns: &[
        (
            "source_branch_id",
            Col::Utf8(|row| Some(&row.source_branch_id)),
        ),
        (
            "target_branch_id",
            Col::Utf8(|row| Some(&row.target_branch_id)),
        ),
        ("base_commit_id", Col::Utf8(|row| Some(&row.base_commit_id))),
        (
            "source_head_commit_id",
            Col::Utf8(|row| Some(&row.source_head_commit_id)),
        ),
        (
            "target_head_commit_id",
            Col::Utf8(|row| Some(&row.target_head_commit_id)),
        ),
        ("merge_outcome", Col::Utf8(|row| Some(row.merge_outcome))),
        ("conflict_kind", Col::Utf8(|row| Some(row.conflict_kind))),
        ("entity_pk", Col::Utf8(|row| Some(&row.entity_pk))),
        ("schema_key", Col::Utf8(|row| Some(&row.schema_key))),
        ("file_id", Col::Utf8(|row| row.file_id.as_deref())),
        (
            "target_change_kind",
            Col::Utf8(|row| Some(row.target_change_kind)),
        ),
        (
            "target_before_change_id",
            Col::Utf8(|row| row.target_before_change_id.as_deref()),
        ),
        (
            "target_after_change_id",
            Col::Utf8(|row| row.target_after_change_id.as_deref()),
        ),
        (
            "source_change_kind",
            Col::Utf8(|row| Some(row.source_change_kind)),
        ),
        (
            "source_before_change_id",
            Col::Utf8(|row| row.source_before_change_id.as_deref()),
        ),
        (
            "source_after_change_id",
            Col::Utf8(|row| row.source_after_change_id.as_deref()),
        ),
    ],
};

fn branch_review_batch_error(error: ColumnTableError) -> DataFusionError {
    match error {
        ColumnTableError::UnsupportedColumn(column) => {
            DataFusionError::Execution(format!("unsupported branch-review column '{column}'"))
        }
        ColumnTableError::Arrow(error) | ColumnTableError::ArrowZeroColumn(error) => {
            DataFusionError::from(error)
        }
        ColumnTableError::Row(error) => lix_error_to_datafusion_error(error),
    }
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::{col, lit};

    use super::BranchPairRoute;

    #[test]
    fn requires_one_exact_source_and_target_branch() {
        let route = BranchPairRoute::from_filters(&[
            col("source_branch_id").eq(lit("source")),
            col("target_branch_id").eq(lit("target")),
        ])
        .expect("exact source/target predicates should route");

        assert_eq!(route.source_branch_id, "source");
        assert_eq!(route.target_branch_id, "target");
    }

    #[test]
    fn rejects_unbounded_branch_pair() {
        let error = BranchPairRoute::from_filters(&[col("source_branch_id").eq(lit("source"))])
            .expect_err("target predicate should be required");

        assert!(error.to_string().contains("target_branch_id"));
    }
}
