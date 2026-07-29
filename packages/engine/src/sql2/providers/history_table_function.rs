use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::datasource::TableType;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, col, lit};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;

use crate::LixError;

/// Registers a history relation as a table-valued function.
///
/// The wrapped provider retains its internal anchor column for commit-graph
/// routing, while callers only see the row-shaped history columns.
pub(super) fn register_history_table_function(
    session: &SessionContext,
    name: &str,
    provider: Arc<dyn TableProvider>,
    anchor_column: &'static str,
) -> Result<(), LixError> {
    let anchor_index = provider.schema().index_of(anchor_column).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("history provider is missing its anchor column: {error}"),
        )
    })?;
    session.register_udtf(
        name,
        Arc::new(HistoryTableFunction {
            name: name.to_string(),
            provider,
            anchor_column,
            anchor_index,
        }),
    );
    Ok(())
}

struct HistoryTableFunction {
    name: String,
    provider: Arc<dyn TableProvider>,
    anchor_column: &'static str,
    anchor_index: usize,
}

impl std::fmt::Debug for HistoryTableFunction {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HistoryTableFunction")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl datafusion::catalog::TableFunctionImpl for HistoryTableFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let anchor = match args {
            [] => None,
            [Expr::Literal(ScalarValue::Utf8(Some(value)), _)]
            | [Expr::Literal(ScalarValue::LargeUtf8(Some(value)), _)]
            | [Expr::Literal(ScalarValue::Utf8View(Some(value)), _)] => Some(value.clone()),
            [_] => {
                return Err(DataFusionError::Plan(format!(
                    "{}(as_of) requires a non-null text commit ID",
                    self.name
                )));
            }
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "{} expects zero or one argument",
                    self.name
                )));
            }
        };
        Ok(Arc::new(HistoryFunctionProvider {
            inner: Arc::clone(&self.provider),
            schema: public_history_schema(&self.provider.schema(), self.anchor_index),
            anchor,
            anchor_column: self.anchor_column,
            anchor_index: self.anchor_index,
        }))
    }
}

struct HistoryFunctionProvider {
    inner: Arc<dyn TableProvider>,
    schema: SchemaRef,
    anchor: Option<String>,
    anchor_column: &'static str,
    anchor_index: usize,
}

impl std::fmt::Debug for HistoryFunctionProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HistoryFunctionProvider")
            .field("anchor", &self.anchor)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TableProvider for HistoryFunctionProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projection = projection
            .cloned()
            .unwrap_or_else(|| (0..self.schema.fields().len()).collect());
        let inner_projection = projection
            .into_iter()
            .map(|index| {
                if index < self.anchor_index {
                    index
                } else {
                    index + 1
                }
            })
            .collect::<Vec<_>>();
        let mut inner_filters = filters.to_vec();
        if let Some(anchor) = &self.anchor {
            inner_filters.push(col(self.anchor_column).eq(lit(anchor.clone())));
        }
        self.inner
            .scan(state, Some(&inner_projection), &inner_filters, limit)
            .await
    }
}

fn public_history_schema(schema: &SchemaRef, anchor_index: usize) -> SchemaRef {
    Arc::new(Schema::new(
        schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != anchor_index)
            .map(|(_, field)| field.as_ref().clone())
            .collect::<Vec<_>>(),
    ))
}
