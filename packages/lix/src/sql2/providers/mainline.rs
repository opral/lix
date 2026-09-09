//! First-parent commit log and endpoint history. Metadata predicates select
//! commits before opening a diff; row predicates never redefine an endpoint.
use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, BooleanArray, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::catalog::{TableFunctionImpl, TableProvider};
use datafusion::common::{DFSchema, DataFusionError, Result};
use datafusion::datasource::TableType;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_expr::{PhysicalExpr, create_physical_expr};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures_util::TryStreamExt;

use crate::changelog::CommitId;
use crate::commit_graph::{CommitGraphContext, CommitGraphNode};
use crate::sql2::SqlChangelogQuerySource;
use crate::sql2::catalog::PublicCatalog;
use crate::sql2::error::lix_error_to_datafusion_error;
use crate::sql2::udfs::{ExecutionSlots, execution_slots};
use crate::storage_adapter::StorageAdapterRead;

use super::diff::{DiffMode, DiffRelation, DiffSpec};
use super::spec::{
    PlannedScan, SpecTableProvider, TableSpec, batch_stream_source, projected_schema,
};

pub(super) fn register_functions<S>(
    session: &datafusion::prelude::SessionContext,
    source: SqlChangelogQuerySource<S>,
    catalog: Arc<PublicCatalog>,
) where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    for history in [false, true] {
        session.register_udtf(
            if history { "lix_history" } else { "lix_log" },
            Arc::new(MainlineFunction {
                store: source.store.clone(),
                catalog: catalog.clone(),
                slots: execution_slots(session),
                history,
            }),
        );
    }
}

struct MainlineFunction<S> {
    store: S,
    catalog: Arc<PublicCatalog>,
    slots: Arc<ExecutionSlots>,
    history: bool,
}
impl<S> std::fmt::Debug for MainlineFunction<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MainlineFunction")
    }
}
fn argument(expr: &Expr) -> Result<String> {
    if let Expr::Literal(value, _) = expr {
        if let Some(value) = value.try_as_str().flatten() {
            return Ok(value.to_owned());
        }
    }
    Err(DataFusionError::Plan(
        "mainline function arguments must be non-null text literals or parameters".into(),
    ))
}
impl<S: StorageAdapterRead + Clone + Send + Sync + 'static> TableFunctionImpl
    for MainlineFunction<S>
{
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let (relation, anchor) = if self.history {
            match args {
                [relation] => (
                    Some(DiffRelation::from_catalog(
                        &self.catalog,
                        &argument(relation)?,
                    )?),
                    None,
                ),
                [relation, anchor] => (
                    Some(DiffRelation::from_catalog(
                        &self.catalog,
                        &argument(relation)?,
                    )?),
                    Some(argument(anchor)?),
                ),
                _ => {
                    return Err(DataFusionError::Plan(
                        "lix_history expects a relation and optional anchor commit ID".into(),
                    ));
                }
            }
        } else {
            match args {
                [] => (None, None),
                [anchor] => (None, Some(argument(anchor)?)),
                _ => {
                    return Err(DataFusionError::Plan(
                        "lix_log expects an optional anchor commit ID".into(),
                    ));
                }
            }
        };
        let anchor = anchor
            .or_else(|| self.slots.active_branch_commit_id())
            .ok_or_else(|| {
                DataFusionError::Plan("mainline requires an active branch head".into())
            })?;
        let anchor = CommitId::parse_lix(&anchor, "mainline anchor")
            .map_err(lix_error_to_datafusion_error)?;
        Ok(Arc::new(SpecTableProvider::new(Arc::new(MainlineSpec {
            store: self.store.clone(),
            relation,
            anchor,
            active_branch_id: self.slots.active_branch_id(),
        }))))
    }
}

pub(super) fn metadata_schema(history: bool) -> SchemaRef {
    let fields = if history {
        vec![
            Field::new("lixcol_from_commit_id", DataType::Utf8, true),
            Field::new("lixcol_to_commit_id", DataType::Utf8, false),
            Field::new("lixcol_commit_created_at", DataType::Utf8, false),
            Field::new("lixcol_commit_is_checkpoint", DataType::Boolean, false),
            Field::new("lixcol_position", DataType::Int64, false),
        ]
    } else {
        vec![
            Field::new("parent_commit_id", DataType::Utf8, true),
            Field::new("commit_id", DataType::Utf8, false),
            Field::new("created_at", DataType::Utf8, false),
            Field::new("is_checkpoint", DataType::Boolean, false),
            Field::new("position", DataType::Int64, false),
        ]
    };
    Arc::new(Schema::new(fields))
}
fn metadata_batch(
    node: &CommitGraphNode,
    position: i64,
    history: bool,
    count: usize,
) -> Result<RecordBatch> {
    let parent = node.parent_commit_ids.first().map(ToString::to_string);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![parent.as_deref(); count])),
        Arc::new(StringArray::from(vec![node.commit_id.to_string(); count])),
        Arc::new(StringArray::from(vec![node.created_at.to_string(); count])),
        Arc::new(BooleanArray::from(vec![node.is_checkpoint; count])),
        Arc::new(Int64Array::from(vec![position; count])),
    ];
    Ok(RecordBatch::try_new(metadata_schema(history), columns)?)
}
fn conjuncts(filters: &[Expr]) -> Vec<Expr> {
    fn append(e: &Expr, out: &mut Vec<Expr>) {
        if let Expr::BinaryExpr(b) = e {
            if b.op == Operator::And {
                append(&b.left, out);
                append(&b.right, out);
                return;
            }
        }
        out.push(e.clone());
    }
    let mut out = Vec::new();
    for e in filters {
        append(e, &mut out);
    }
    out
}
fn metadata_only(expr: &Expr, schema: &SchemaRef) -> bool {
    expr.column_refs()
        .iter()
        .all(|c| schema.index_of(&c.name).is_ok())
}
/// A conjunction's finite position ceiling lets a latest-event query stop
/// without consulting unrelated older history. OR predicates remain residual.
fn position_upper_bound(filters: &[Expr], column: &str) -> Option<i64> {
    use datafusion::common::ScalarValue;
    filters
        .iter()
        .filter_map(|filter| {
            let Expr::BinaryExpr(binary) = filter else {
                return None;
            };
            let (value, operator) = match (binary.left.as_ref(), binary.right.as_ref()) {
                (Expr::Column(c), Expr::Literal(value, _)) if c.name == column => {
                    (value, binary.op)
                }
                (Expr::Literal(value, _), Expr::Column(c)) if c.name == column => (
                    value,
                    match binary.op {
                        Operator::Gt => Operator::Lt,
                        Operator::GtEq => Operator::LtEq,
                        Operator::Eq => Operator::Eq,
                        _ => return None,
                    },
                ),
                _ => return None,
            };
            let value = match value {
                ScalarValue::Int64(Some(v)) => *v,
                ScalarValue::Int32(Some(v)) => i64::from(*v),
                ScalarValue::UInt64(Some(v)) => i64::try_from(*v).ok()?,
                _ => return None,
            };
            match operator {
                Operator::Eq | Operator::LtEq => Some(value),
                Operator::Lt => Some(value.saturating_sub(1)),
                _ => None,
            }
        })
        .min()
}

fn matches_metadata(batch: &RecordBatch, filters: &[Arc<dyn PhysicalExpr>]) -> Result<bool> {
    for expr in filters {
        let value = expr.evaluate(batch)?.into_array(batch.num_rows())?;
        let value = value
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| DataFusionError::Internal("metadata filter must be boolean".into()))?;
        if value.iter().next().flatten() != Some(true) {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
thread_local! {
    static MAINLINE_WORK: std::cell::Cell<(usize, usize)> = const { std::cell::Cell::new((0, 0)) };
}
#[cfg(test)]
pub(crate) fn take_mainline_work() -> (usize, usize) {
    MAINLINE_WORK.with(|work| work.replace((0, 0)))
}
#[inline]
fn record_work(_diff: bool) {
    #[cfg(test)]
    MAINLINE_WORK.with(|work| {
        let (nodes, diffs) = work.get();
        work.set(if _diff {
            (nodes, diffs + 1)
        } else {
            (nodes + 1, diffs)
        });
    });
}

struct MainlineSpec<S> {
    store: S,
    relation: Option<DiffRelation>,
    anchor: CommitId,
    active_branch_id: Option<String>,
}
#[async_trait]
impl<S: StorageAdapterRead + Clone + Send + Sync + 'static> TableSpec for MainlineSpec<S> {
    fn table_name(&self) -> &str {
        if self.relation.is_some() {
            "lix_history"
        } else {
            "lix_log"
        }
    }
    fn table_type(&self) -> TableType {
        TableType::View
    }
    fn schema(&self) -> SchemaRef {
        match &self.relation {
            None => metadata_schema(false),
            Some(relation) => {
                let mut fields = relation
                    .schema
                    .fields()
                    .iter()
                    .map(|f| f.as_ref().clone())
                    .collect::<Vec<_>>();
                fields.extend(
                    metadata_schema(true)
                        .fields()
                        .iter()
                        .skip(2)
                        .map(|f| f.as_ref().clone()),
                );
                Arc::new(Schema::new(fields))
            }
        }
    }
    fn filter_pushdown(&self, filter: &Expr) -> TableProviderFilterPushDown {
        if metadata_only(filter, &metadata_schema(self.relation.is_some())) {
            TableProviderFilterPushDown::Exact
        } else {
            TableProviderFilterPushDown::Inexact
        }
    }
    fn probe_key_columns(&self, _: &[Expr]) -> Vec<String> {
        vec![
            if self.relation.is_some() {
                "lixcol_to_commit_id"
            } else {
                "commit_id"
            }
            .into(),
        ]
    }
    async fn plan_scan(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
        _: &ExecutionProps,
    ) -> Result<PlannedScan> {
        let schema = projected_schema(&self.schema(), projection);
        let meta_schema = metadata_schema(self.relation.is_some());
        let (metadata_filters, row_filters): (Vec<_>, Vec<_>) = conjuncts(filters)
            .into_iter()
            .partition(|f| metadata_only(f, &meta_schema));
        let max_position = position_upper_bound(
            &metadata_filters,
            if self.relation.is_some() {
                "lixcol_position"
            } else {
                "position"
            },
        );
        let id_column = if self.relation.is_some() {
            "lixcol_to_commit_id"
        } else {
            "commit_id"
        };
        let selected_ids = match super::file::exact_string_column_constraint_from_filters(
            &metadata_filters,
            id_column,
        ) {
            Ok(super::file::FileIdConstraint::Ids(ids)) => Some(ids),
            Ok(super::file::FileIdConstraint::None) => Some(BTreeSet::new()),
            _ => None,
        };
        let df_meta_schema = DFSchema::try_from(meta_schema.as_ref().clone())?;
        let metadata_filters = metadata_filters
            .iter()
            .map(|f| create_physical_expr(f, &df_meta_schema, &ExecutionProps::new()))
            .collect::<Result<Vec<_>>>()?;
        let relation = self.relation.clone();
        let store = self.store.clone();
        let anchor = self.anchor;
        let active_branch_id = self.active_branch_id.clone();
        let output_schema = schema.clone();
        let ordering = Some(
            if relation.is_some() {
                "lixcol_position"
            } else {
                "position"
            }
            .into(),
        );
        let source = batch_stream_source(schema.clone(), 1, move |_, context| {
            let (store, relation, schema, metadata_filters, row_filters, active_branch_id) = (
                store.clone(),
                relation.clone(),
                output_schema.clone(),
                metadata_filters.clone(),
                row_filters.clone(),
                active_branch_id.clone(),
            );
            let mut remaining_ids = selected_ids.clone();
            let stream_schema = schema.clone();
            let stream = async_stream::try_stream! {
                let mut graph = CommitGraphContext::new().reader(store.clone());
                let mut next = Some(anchor);
                let mut position = 0i64;
                let mut emitted = 0usize;
                while let Some(id) = next {
                    if max_position.is_some_and(|ceiling| position > ceiling) { break; }
                    if limit.is_some_and(|n| emitted >= n) || remaining_ids.as_ref().is_some_and(|ids| ids.is_empty()) { break; }
                    record_work(false);
                    let node = graph.load_node(&id).await.map_err(lix_error_to_datafusion_error)?
                        .ok_or_else(|| lix_error_to_datafusion_error(crate::LixError::commit_not_found(id.to_string(), "walk_commit_graph", "graph_node")))?;
                    next = node.parent_commit_ids.first().copied();
                    if let Some(ids) = remaining_ids.as_mut() { ids.remove(&id.to_string()); }
                    let current_position = position;
                    position += 1;
                    if !matches_metadata(&metadata_batch(&node, current_position, relation.is_some(), 1)?, &metadata_filters)? { continue; }
                    if let Some(relation) = &relation {
                        let Some(parent) = next else { continue; }; // root is a baseline, not a synthetic change
                        let diff_projection = schema.fields().iter().filter_map(|field| relation.schema.index_of(field.name()).ok()).collect::<Vec<_>>();
                        record_work(true);
                        let diff = DiffSpec { store: store.clone(), relation: relation.clone(), from_commit_id: parent.to_string(),
                            to_commit_id: id.to_string(), active_branch_id: active_branch_id.clone(), mode: DiffMode::General };
                        let plan = diff.plan_scan(Some(&diff_projection), &row_filters, None, &ExecutionProps::new()).await?;
                        let mut batches = plan.source.open(0, context.clone())?;
                        while let Some(batch) = batches.try_next().await? {
                            let meta = metadata_batch(&node, current_position, true, batch.num_rows())?;
                            let columns = schema.fields().iter().map(|field| {
                                batch.column_by_name(field.name()).or_else(|| meta.column_by_name(field.name())).cloned()
                                    .ok_or_else(|| DataFusionError::Internal(format!("missing history column {}", field.name())))
                            }).collect::<Result<Vec<_>>>()?;
                            let mut output = RecordBatch::try_new_with_options(schema.clone(), columns, &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())))?;
                            if let Some(n) = limit { output = output.slice(0, output.num_rows().min(n - emitted)); }
                            emitted += output.num_rows();
                            yield output;
                            if limit.is_some_and(|n| emitted >= n) { break; }
                        }
                    } else {
                        let batch = metadata_batch(&node, current_position, false, 1)?;
                        let indices = schema.fields().iter().map(|f| batch.schema().index_of(f.name())).collect::<std::result::Result<Vec<_>, _>>()?;
                        emitted += 1;
                        yield batch.project(&indices)?;
                    }
                }
            };
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                stream_schema,
                stream,
            )))
        });
        Ok(PlannedScan {
            schema,
            source,
            ordering,
        })
    }
}

pub(crate) fn relation_history_schema(
    catalog: &PublicCatalog,
    relation: &str,
) -> Result<SchemaRef> {
    let relation = DiffRelation::from_catalog(catalog, relation)?;
    let mut fields = relation
        .schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect::<Vec<_>>();
    fields.extend(
        metadata_schema(true)
            .fields()
            .iter()
            .skip(2)
            .map(|f| f.as_ref().clone()),
    );
    Ok(Arc::new(Schema::new(fields)))
}
