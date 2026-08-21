use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{TableFunctionImpl, TableProvider};
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::TableType;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use tokio::sync::Mutex;

use crate::changelog::CommitId;
use crate::commit_graph::CommitGraphReader;
use crate::sql2::error::lix_error_to_datafusion_error;

use super::columns::{Col, ColumnTable, ColumnTableError};
use super::spec::{PlannedScan, SpecTableProvider, TableSpec, projected_schema, scan_row_source};

pub(super) fn register_commit_ancestry_function(
    session: &datafusion::prelude::SessionContext,
    surface_name: &str,
    active_branch_commit_id: String,
    commit_graph: Box<dyn CommitGraphReader>,
) {
    session.register_udtf(
        surface_name,
        Arc::new(CommitAncestryFunction {
            name: surface_name.to_string(),
            active_branch_commit_id,
            commit_graph: Arc::new(Mutex::new(commit_graph)),
        }),
    );
}

struct CommitAncestryFunction {
    name: String,
    active_branch_commit_id: String,
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
}

impl fmt::Debug for CommitAncestryFunction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CommitAncestryFunction")
            .field("name", &self.name)
            .field("active_branch_commit_id", &self.active_branch_commit_id)
            .finish_non_exhaustive()
    }
}

impl TableFunctionImpl for CommitAncestryFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let anchor_commit_id = match args {
            [] => self.active_branch_commit_id.clone(),
            [anchor] => commit_id_argument(anchor)?,
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "{} expects zero arguments or one commit ID argument",
                    self.name
                )));
            }
        };
        Ok(Arc::new(SpecTableProvider::new(Arc::new(
            CommitAncestrySpec {
                anchor_commit_id,
                commit_graph: Arc::clone(&self.commit_graph),
            },
        ))))
    }
}

fn commit_id_argument(argument: &Expr) -> Result<String> {
    let Expr::Literal(value, _) = argument else {
        return Err(DataFusionError::Plan(
            "lix_commit_ancestry argument must be a commit ID literal or parameter".to_string(),
        ));
    };
    value
        .try_as_str()
        .flatten()
        .map(ToString::to_string)
        .ok_or_else(|| {
            DataFusionError::Plan(
                "lix_commit_ancestry argument must be a non-null text commit ID".to_string(),
            )
        })
}

struct CommitAncestrySpec {
    anchor_commit_id: String,
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
}

#[async_trait]
impl TableSpec for CommitAncestrySpec {
    fn table_name(&self) -> &str {
        "lix_commit_ancestry"
    }

    fn schema(&self) -> SchemaRef {
        commit_ancestry_schema()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn filter_pushdown(&self, _filter: &Expr) -> TableProviderFilterPushDown {
        TableProviderFilterPushDown::Unsupported
    }

    async fn plan_scan(
        &self,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
        _props: &ExecutionProps,
    ) -> Result<PlannedScan> {
        let schema = projected_schema(&commit_ancestry_schema(), projection);
        Ok(PlannedScan {
            schema: Arc::clone(&schema),
            ordering: None,
            source: scan_row_source(
                Arc::clone(&schema),
                (
                    self.anchor_commit_id.clone(),
                    Arc::clone(&self.commit_graph),
                    schema,
                ),
                move |(anchor_commit_id, commit_graph, schema)| async move {
                    if limit == Some(0) {
                        return ANCESTRY_COLS
                            .build(schema, &[])
                            .map_err(ancestry_batch_error);
                    }
                    let anchor_commit_id = CommitId::parse(&anchor_commit_id).map_err(|error| {
                        lix_error_to_datafusion_error(crate::LixError::new(
                            crate::LixError::CODE_INVALID_PARAM,
                            format!("lix_commit_ancestry anchor must be a UUID commit id: {error}"),
                        ))
                    })?;
                    let reachable = match limit {
                        Some(limit) => {
                            commit_graph
                                .lock()
                                .await
                                .reachable_nodes_limited(&anchor_commit_id, limit)
                                .await
                        }
                        None => {
                            commit_graph
                                .lock()
                                .await
                                .reachable_nodes(&anchor_commit_id)
                                .await
                        }
                    }
                    .map_err(lix_error_to_datafusion_error)?;
                    let rows = reachable
                        .iter()
                        .take(limit.unwrap_or(usize::MAX))
                        .map(|node| CommitAncestryRow {
                            commit_id: node.commit.commit_id.to_string(),
                            depth: i64::from(node.depth),
                        })
                        .collect::<Vec<_>>();
                    ANCESTRY_COLS
                        .build(schema, &rows)
                        .map_err(ancestry_batch_error)
                },
            ),
        })
    }
}

pub(crate) fn commit_ancestry_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("commit_id", DataType::Utf8, false),
        Field::new("depth", DataType::Int64, false),
    ]))
}

struct CommitAncestryRow {
    commit_id: String,
    depth: i64,
}

static ANCESTRY_COLS: ColumnTable<CommitAncestryRow> = ColumnTable {
    columns: &[
        ("commit_id", Col::Utf8(|row| Some(&row.commit_id))),
        ("depth", Col::I64(|row| Some(row.depth))),
    ],
};

fn ancestry_batch_error(error: ColumnTableError) -> DataFusionError {
    match error {
        ColumnTableError::UnsupportedColumn(column) => {
            DataFusionError::Execution(format!("unsupported lix_commit_ancestry column '{column}'"))
        }
        ColumnTableError::Arrow(error) | ColumnTableError::ArrowZeroColumn(error) => {
            DataFusionError::from(error)
        }
        ColumnTableError::Row(error) => lix_error_to_datafusion_error(error),
    }
}
