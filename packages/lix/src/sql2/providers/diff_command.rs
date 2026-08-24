use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::TableType;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;

use crate::LixError;
use crate::sql2::error::lix_error_to_datafusion_error;
use crate::sql2::{DiffCommand, SqlWriteContext, WriteAccess};

use super::spec::{InsertApply, PlannedScan, TableSpec, register_spec_table, scan_row_source};

pub(super) async fn register_diff_command_provider(
    session: &datafusion::prelude::SessionContext,
    surface_name: &str,
    command: DiffCommand,
    write_ctx: SqlWriteContext,
) -> Result<(), LixError> {
    register_spec_table(
        session,
        surface_name,
        Arc::new(DiffCommandSpec {
            table_name: surface_name.to_string(),
            command,
        }),
        WriteAccess::write(write_ctx),
    )
}

struct DiffCommandSpec {
    table_name: String,
    command: DiffCommand,
}

#[async_trait]
impl TableSpec for DiffCommandSpec {
    fn table_name(&self) -> &str {
        &self.table_name
    }

    fn schema(&self) -> SchemaRef {
        command_schema()
    }

    fn table_type(&self) -> TableType {
        // A command sink is closest to an insertable view in the standard
        // information schema. Lix's exact classification is exposed through
        // information_schema.lix_surfaces.
        TableType::View
    }

    async fn plan_scan(
        &self,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
        _props: &ExecutionProps,
    ) -> Result<PlannedScan> {
        let table = self.table_name.clone();
        Ok(PlannedScan {
            schema: command_schema(),
            ordering: None,
            source: scan_row_source(command_schema(), table, |table| async move {
                Err(DataFusionError::Execution(format!(
                    "{table} is a command sink and cannot be read"
                )))
            }),
        })
    }

    async fn plan_insert(
        &self,
        write_ctx: SqlWriteContext,
        _input: &Arc<dyn ExecutionPlan>,
    ) -> Result<Option<InsertApply>> {
        let command = self.command;
        Ok(Some(Arc::new(move |batches| {
            let write_ctx = write_ctx.clone();
            Box::pin(async move {
                let diff_ids = diff_ids_from_batches(&batches)?;
                if diff_ids.is_empty() {
                    return Ok(0);
                }
                write_ctx
                    .execute_diff_command(command, diff_ids)
                    .await
                    .map(|outcome| outcome.rows_affected)
                    .map_err(lix_error_to_datafusion_error)
            })
        })))
    }
}

fn command_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("diff_id", DataType::Utf8, false),
        Field::new("commit_id", DataType::Utf8, false),
    ]))
}

fn diff_ids_from_batches(batches: &[RecordBatch]) -> Result<Vec<String>> {
    let mut ids = Vec::new();
    for batch in batches {
        let values = batch
            .column_by_name("diff_id")
            .ok_or_else(|| DataFusionError::Execution("diff_id column is required".to_string()))?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Execution("diff_id must be text".to_string()))?;
        for index in 0..values.len() {
            if values.is_null(index) {
                return Err(DataFusionError::Execution(
                    "diff_id cannot be NULL".to_string(),
                ));
            }
            ids.push(values.value(index).to_string());
        }
    }
    ids.sort();
    if ids.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(DataFusionError::Execution(
            "diff command selection contains duplicate diff_id rows".to_string(),
        ));
    }
    Ok(ids)
}
