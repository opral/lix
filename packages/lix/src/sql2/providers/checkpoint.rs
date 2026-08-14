use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::TableType;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};

use crate::LixError;
use crate::branch::{BranchHead, BranchRefReader};
use crate::sql2::error::lix_error_to_datafusion_error;
use crate::sql2::history_route::{HistoryRoute, parse_history_filter};
use crate::sql2::{SqlChangelogQuerySource, WriteAccess};
use crate::storage_adapter::StorageAdapterRead;

use super::columns::{Col, ColumnTable, ColumnTableError};
use super::file::{FileIdConstraint, exact_string_column_constraint_from_filters};
use super::spec::{PlannedScan, TableSpec, projected_schema, register_spec_table, scan_row_source};

pub(super) async fn register_checkpoint_provider<S>(
    session: &datafusion::prelude::SessionContext,
    surface_name: &str,
    active_branch_id: Option<String>,
    branch_ref: Arc<dyn BranchRefReader>,
    query_source: SqlChangelogQuerySource<S>,
) -> Result<(), LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    register_spec_table(
        session,
        surface_name,
        Arc::new(CheckpointSpec {
            by_branch: active_branch_id.is_none(),
            active_branch_id,
            branch_ref,
            forktree_reader: query_source.forktree_reader,
        }),
        WriteAccess::read_only(),
    )
}

struct CheckpointSpec<S> {
    by_branch: bool,
    active_branch_id: Option<String>,
    branch_ref: Arc<dyn BranchRefReader>,
    forktree_reader: crate::forktree::ForkTreeReadFacade<S>,
}

#[async_trait]
impl<S> TableSpec<S> for CheckpointSpec<S>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    fn table_name(&self) -> &str {
        if self.by_branch {
            "lix_checkpoint_by_branch"
        } else {
            "lix_checkpoint"
        }
    }

    fn schema(&self) -> SchemaRef {
        checkpoint_schema(self.by_branch)
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn filter_pushdown(&self, filter: &Expr) -> TableProviderFilterPushDown {
        if filter
            .column_refs()
            .iter()
            .any(|column| matches!(column.name.as_str(), "lixcol_branch_id" | "lixcol_depth"))
            || parse_history_filter(filter).is_some()
        {
            TableProviderFilterPushDown::Inexact
        } else {
            TableProviderFilterPushDown::Unsupported
        }
    }

    async fn plan_scan(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
        _props: &ExecutionProps,
    ) -> Result<PlannedScan> {
        let schema = projected_schema(&self.schema(), projection);
        let conjuncts = filter_conjuncts(filters);
        let branch_ids =
            exact_string_column_constraint_from_filters(&conjuncts, "lixcol_branch_id")?;
        let depth_route = HistoryRoute::from_filters(filters);
        Ok(PlannedScan {
            schema: Arc::clone(&schema),
            ordering: None,
            source: scan_row_source(
                Arc::clone(&schema),
                (
                    self.active_branch_id.clone(),
                    Arc::clone(&self.branch_ref),
                    self.forktree_reader.clone(),
                    schema,
                    branch_ids,
                    depth_route,
                    limit,
                ),
                move |(
                    _active_branch_id,
                    _branch_ref,
                    historical,
                    schema,
                    branch_ids,
                    depth_route,
                    limit,
                )| async move {
                    if depth_route.is_contradictory()
                        || matches!(branch_ids, FileIdConstraint::None)
                    {
                        return CHECKPOINT_COLS
                            .build(schema, &[])
                            .map_err(checkpoint_batch_error);
                    }
                    let heads = selected_heads(
                        _branch_ref.as_ref(),
                        _active_branch_id.as_deref(),
                        &branch_ids,
                    )
                    .await
                    .map_err(lix_error_to_datafusion_error)?;
                    let mut rows = Vec::new();
                    for head in heads {
                        let remaining = limit.map(|limit| limit.saturating_sub(rows.len()));
                        let min_depth = depth_route
                            .min_depth
                            .map(|depth| u32::try_from(depth).expect("negative depth rejected"));
                        let max_depth = depth_route
                            .max_depth
                            .map(|depth| u32::try_from(depth).expect("negative depth rejected"));
                        let history = historical
                            .checkpoint_history_for_branch(
                                &head.branch_id,
                                min_depth,
                                max_depth,
                                remaining,
                            )
                            .await
                            .map_err(lix_error_to_datafusion_error)?;
                        for entry in history.entries {
                            rows.push(CheckpointSqlRow {
                                commit_id: entry.commit_id.to_string(),
                                created_at: entry.created_at,
                                branch_id: head.branch_id.clone(),
                                depth: i64::from(entry.depth),
                            });
                            if limit.is_some_and(|limit| rows.len() >= limit) {
                                break;
                            }
                        }
                        if limit.is_some_and(|limit| rows.len() >= limit) {
                            break;
                        }
                    }
                    CHECKPOINT_COLS
                        .build(schema, &rows)
                        .map_err(checkpoint_batch_error)
                },
            ),
        })
    }
}

pub(super) async fn selected_heads(
    branch_ref: &dyn BranchRefReader,
    active_branch_id: Option<&str>,
    branch_ids: &FileIdConstraint,
) -> Result<Vec<BranchHead>, LixError> {
    if let Some(branch_id) = active_branch_id {
        if !string_constraint_allows(branch_ids, branch_id) {
            return Ok(Vec::new());
        }
        return Ok(branch_ref.load_head(branch_id).await?.into_iter().collect());
    }
    if let FileIdConstraint::Ids(branch_ids) = branch_ids {
        let mut heads = Vec::with_capacity(branch_ids.len());
        for branch_id in branch_ids {
            if branch_id == crate::GLOBAL_BRANCH_ID {
                continue;
            }
            if let Some(head) = branch_ref.load_head(branch_id).await? {
                heads.push(head);
            }
        }
        return Ok(heads);
    }
    if matches!(branch_ids, FileIdConstraint::None) {
        return Ok(Vec::new());
    }
    let mut heads = branch_ref.scan_heads().await?;
    heads.retain(|head| head.branch_id != crate::GLOBAL_BRANCH_ID);
    Ok(heads)
}

fn string_constraint_allows(constraint: &FileIdConstraint, value: &str) -> bool {
    match constraint {
        FileIdConstraint::All => true,
        FileIdConstraint::None => false,
        FileIdConstraint::Ids(values) => values.contains(value),
    }
}

#[cfg(test)]
fn checkpoint_depth_scan_limit(route: &HistoryRoute) -> Option<usize> {
    route
        .max_depth
        .and_then(|depth| usize::try_from(depth).ok())
        .and_then(|depth| depth.checked_add(1))
}

#[cfg(test)]
fn checkpoint_depth_matches(route: &HistoryRoute, depth: u32) -> bool {
    let depth = i64::from(depth);
    route.min_depth.is_none_or(|minimum| depth >= minimum)
        && route.max_depth.is_none_or(|maximum| depth <= maximum)
}

#[cfg(test)]
fn min_optional(left: Option<usize>, right: Option<usize>) -> Option<usize> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

pub(super) fn filter_conjuncts(filters: &[Expr]) -> Vec<Expr> {
    fn append(expr: &Expr, conjuncts: &mut Vec<Expr>) {
        match expr {
            Expr::BinaryExpr(binary) if binary.op == datafusion::logical_expr::Operator::And => {
                append(&binary.left, conjuncts);
                append(&binary.right, conjuncts);
            }
            _ => conjuncts.push(expr.clone()),
        }
    }

    let mut conjuncts = Vec::new();
    for filter in filters {
        append(filter, &mut conjuncts);
    }
    conjuncts
}

pub(super) fn checkpoint_schema(by_branch: bool) -> SchemaRef {
    let mut fields = vec![
        Field::new("commit_id", DataType::Utf8, false),
        Field::new("created_at", DataType::Utf8, false),
    ];
    if by_branch {
        fields.push(Field::new("lixcol_branch_id", DataType::Utf8, false));
    }
    fields.push(Field::new("lixcol_depth", DataType::Int64, false));
    Arc::new(Schema::new(fields))
}

struct CheckpointSqlRow {
    commit_id: String,
    created_at: String,
    branch_id: String,
    depth: i64,
}

static CHECKPOINT_COLS: ColumnTable<CheckpointSqlRow> = ColumnTable {
    columns: &[
        ("commit_id", Col::Utf8(|row| Some(&row.commit_id))),
        ("created_at", Col::Utf8(|row| Some(&row.created_at))),
        ("lixcol_branch_id", Col::Utf8(|row| Some(&row.branch_id))),
        ("lixcol_depth", Col::I64(|row| Some(row.depth))),
    ],
};

fn checkpoint_batch_error(error: ColumnTableError) -> DataFusionError {
    match error {
        ColumnTableError::UnsupportedColumn(column) => {
            DataFusionError::Execution(format!("unsupported checkpoint column '{column}'"))
        }
        ColumnTableError::Arrow(error) | ColumnTableError::ArrowZeroColumn(error) => {
            DataFusionError::from(error)
        }
        ColumnTableError::Row(error) => lix_error_to_datafusion_error(error),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use datafusion::prelude::{col, lit};

    use crate::LixError;
    use crate::branch::{BranchHead, BranchRefReader};
    use crate::changelog::CommitId;
    use crate::sql2::history_route::HistoryRoute;

    use super::{
        FileIdConstraint, checkpoint_depth_matches, checkpoint_depth_scan_limit, filter_conjuncts,
        min_optional, selected_heads,
    };

    struct CountingBranchRefReader {
        load_calls: AtomicUsize,
        scan_calls: AtomicUsize,
    }

    #[async_trait]
    impl BranchRefReader for CountingBranchRefReader {
        async fn load_head(&self, branch_id: &str) -> Result<Option<BranchHead>, LixError> {
            self.load_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Some(BranchHead {
                branch_id: branch_id.to_string(),
                commit_id: CommitId::for_test_label(branch_id),
            }))
        }

        async fn scan_heads(&self) -> Result<Vec<BranchHead>, LixError> {
            self.scan_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Vec::new())
        }
    }

    #[test]
    fn routes_checkpoint_depth_as_commit_distance() {
        let filters = [col("lixcol_depth")
            .gt_eq(lit(3_i64))
            .and(col("lixcol_depth").lt_eq(lit(7_i64)))];
        let route = HistoryRoute::from_filters(&filters);

        assert_eq!(checkpoint_depth_scan_limit(&route), Some(8));
        assert!(!checkpoint_depth_matches(&route, 2));
        assert!(checkpoint_depth_matches(&route, 3));
        assert!(checkpoint_depth_matches(&route, 7));
        assert!(!checkpoint_depth_matches(&route, 8));
    }

    #[test]
    fn splits_conjunctions_for_exact_branch_routing() {
        let filters = [col("lixcol_branch_id")
            .eq(lit("01920000-0000-7000-8000-0000000000a1"))
            .and(col("commit_id").eq(lit("commit-a")))];

        assert_eq!(filter_conjuncts(&filters).len(), 2);
        assert_eq!(min_optional(Some(5), Some(8)), Some(5));
    }

    #[tokio::test]
    async fn exact_branch_selection_uses_point_reads() {
        let reader = CountingBranchRefReader {
            load_calls: AtomicUsize::new(0),
            scan_calls: AtomicUsize::new(0),
        };
        let branch_ids = FileIdConstraint::Ids(
            [
                "01920000-0000-7000-8000-0000000000a1".to_string(),
                "01920000-0000-7000-8000-0000000000b1".to_string(),
            ]
            .into(),
        );

        let heads = selected_heads(&reader, None, &branch_ids)
            .await
            .expect("exact branch selection should succeed");

        assert_eq!(heads.len(), 2);
        assert_eq!(reader.load_calls.load(Ordering::SeqCst), 2);
        assert_eq!(reader.scan_calls.load(Ordering::SeqCst), 0);
    }
}
