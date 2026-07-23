use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::TableType;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::Expr;
use tokio::sync::Mutex;

use crate::LixError;
use crate::branch::{BranchHead, BranchRefReader};
use crate::checkpoint::checkpoint_history_from_head;
use crate::commit_graph::CommitGraphReader;
use crate::sql2::WriteAccess;
use crate::sql2::error::lix_error_to_datafusion_error;

use super::columns::{Col, ColumnTable, ColumnTableError};
use super::spec::{PlannedScan, TableSpec, projected_schema, register_spec_table, row_source};

pub(super) async fn register_checkpoint_provider(
    session: &datafusion::prelude::SessionContext,
    surface_name: &str,
    active_branch_id: Option<String>,
    branch_ref: Arc<dyn BranchRefReader>,
    commit_graph: Box<dyn CommitGraphReader>,
) -> Result<(), LixError> {
    register_spec_table(
        session,
        surface_name,
        Arc::new(CheckpointSpec {
            by_branch: active_branch_id.is_none(),
            active_branch_id,
            branch_ref,
            commit_graph: Arc::new(Mutex::new(commit_graph)),
        }),
        WriteAccess::read_only(),
    )
}

struct CheckpointSpec {
    by_branch: bool,
    active_branch_id: Option<String>,
    branch_ref: Arc<dyn BranchRefReader>,
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
}

#[async_trait]
impl TableSpec for CheckpointSpec {
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

    async fn plan_scan(
        &self,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
        _props: &ExecutionProps,
    ) -> Result<PlannedScan> {
        let schema = projected_schema(&self.schema(), projection);
        Ok(PlannedScan {
            schema: Arc::clone(&schema),
            ordering: None,
            load: row_source(
                (
                    self.active_branch_id.clone(),
                    Arc::clone(&self.branch_ref),
                    Arc::clone(&self.commit_graph),
                    schema,
                ),
                move |(active_branch_id, branch_ref, commit_graph, schema)| async move {
                    let heads = selected_heads(branch_ref.as_ref(), active_branch_id.as_deref())
                        .await
                        .map_err(lix_error_to_datafusion_error)?;
                    let mut reader = commit_graph.lock().await;
                    let mut rows = Vec::new();
                    for head in heads {
                        for checkpoint in
                            checkpoint_history_from_head(reader.as_mut(), &head.commit_id)
                                .await
                                .map_err(lix_error_to_datafusion_error)?
                        {
                            rows.push(CheckpointSqlRow {
                                commit_id: checkpoint.commit_id.to_string(),
                                created_at: checkpoint.created_at,
                                branch_id: head.branch_id.clone(),
                                depth: i64::from(checkpoint.depth),
                            });
                        }
                    }
                    if let Some(limit) = limit {
                        rows.truncate(limit);
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
) -> Result<Vec<BranchHead>, LixError> {
    if let Some(branch_id) = active_branch_id {
        return Ok(branch_ref.load_head(branch_id).await?.into_iter().collect());
    }
    let mut heads = branch_ref.scan_heads().await?;
    heads.retain(|head| head.branch_id != crate::GLOBAL_BRANCH_ID);
    Ok(heads)
}

fn checkpoint_schema(by_branch: bool) -> SchemaRef {
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
