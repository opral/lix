//! First-parent commit log and endpoint history. Metadata predicates select
//! commits before opening a diff; row predicates never redefine an endpoint.
use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, BooleanArray, Int64Array, StringArray};
use datafusion::arrow::compute::filter_record_batch;
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
use crate::sql2::error::{datafusion_error_to_lix_error, lix_error_to_datafusion_error};
use crate::sql2::udfs::{ExecutionSlots, execution_slots};
use crate::storage_adapter::StorageAdapterRead;

mod frontier;

use super::diff::{DiffMode, DiffRelation, DiffSpec};
use super::spec::{
    PlannedScan, ScanSource, SpecTableProvider, TableSpec, batch_stream_source, projected_schema,
};

pub(super) fn register_functions<S>(
    session: &datafusion::prelude::SessionContext,
    source: SqlChangelogQuerySource<S>,
    catalog: Arc<PublicCatalog>,
    blob_reader: Arc<dyn crate::binary_cas::BlobDataReader>,
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
                blob_reader: Arc::clone(&blob_reader),
            }),
        );
    }
}

struct MainlineFunction<S> {
    blob_reader: Arc<dyn crate::binary_cas::BlobDataReader>,
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
            blob_reader: Arc::clone(&self.blob_reader),
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
    checkpoint_active: bool,
) -> Result<RecordBatch> {
    let parent = node.parent_commit_ids.first().map(ToString::to_string);
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![parent.as_deref(); count])),
        Arc::new(StringArray::from(vec![node.commit_id.to_string(); count])),
        Arc::new(StringArray::from(vec![node.created_at.to_string(); count])),
        Arc::new(Int64Array::from(vec![position; count])),
    ];
    if !history {
        columns.insert(
            3,
            Arc::new(BooleanArray::from(vec![checkpoint_active; count])),
        );
    }
    Ok(RecordBatch::try_new(metadata_schema(history), columns)?)
}

fn log_metadata_window_batch(rows: &[(&CommitGraphNode, i64, bool)]) -> Result<RecordBatch> {
    let parent_ids = rows
        .iter()
        .map(|(node, _, _)| node.parent_commit_ids.first().map(ToString::to_string))
        .collect::<Vec<_>>();
    let commit_ids = rows
        .iter()
        .map(|(node, _, _)| Some(node.commit_id.to_string()))
        .collect::<Vec<_>>();
    let created_at = rows
        .iter()
        .map(|(node, _, _)| Some(node.created_at.to_string()))
        .collect::<Vec<_>>();
    let checkpoint_active = rows
        .iter()
        .map(|(_, _, checkpoint_active)| *checkpoint_active)
        .collect::<Vec<_>>();
    let positions = rows
        .iter()
        .map(|(_, position, _)| *position)
        .collect::<Vec<_>>();
    record_log_metadata_batch(rows.len());
    Ok(RecordBatch::try_new(
        metadata_schema(false),
        vec![
            Arc::new(StringArray::from(parent_ids)),
            Arc::new(StringArray::from(commit_ids)),
            Arc::new(StringArray::from(created_at)),
            Arc::new(BooleanArray::from(checkpoint_active)),
            Arc::new(Int64Array::from(positions)),
        ],
    )?)
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

fn filter_metadata_batch(
    mut batch: RecordBatch,
    filters: &[Arc<dyn PhysicalExpr>],
) -> Result<RecordBatch> {
    for expr in filters {
        let value = expr.evaluate(&batch)?.into_array(batch.num_rows())?;
        let value = value
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| DataFusionError::Internal("metadata filter must be boolean".into()))?;
        let keep = BooleanArray::from(
            value
                .iter()
                .map(|value| value == Some(true))
                .collect::<Vec<_>>(),
        );
        batch = filter_record_batch(&batch, &keep)?;
        if batch.num_rows() == 0 {
            break;
        }
    }
    Ok(batch)
}

#[cfg(test)]
thread_local! {
    static MAINLINE_WORK: std::cell::Cell<(usize, usize)> = const { std::cell::Cell::new((0, 0)) };
    static CHECKPOINT_RETIREMENT_WORK: std::cell::Cell<(usize, usize)> = const { std::cell::Cell::new((0, 0)) };
    static MAINLINE_METADATA_WORK: std::cell::Cell<(usize, usize)> = const { std::cell::Cell::new((0, 0)) };
}
#[cfg(test)]
pub(crate) fn take_mainline_work() -> (usize, usize) {
    MAINLINE_WORK.with(|work| work.replace((0, 0)))
}
#[cfg(test)]
pub(crate) fn take_checkpoint_retirement_work() -> (usize, usize) {
    CHECKPOINT_RETIREMENT_WORK.with(|work| work.replace((0, 0)))
}
#[cfg(test)]
pub(crate) fn take_mainline_metadata_work() -> (usize, usize) {
    MAINLINE_METADATA_WORK.with(|work| work.replace((0, 0)))
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
    blob_reader: Arc<dyn crate::binary_cas::BlobDataReader>,
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
        let needs_checkpoint_active = self.relation.is_none()
            && (schema.index_of("is_checkpoint").is_ok()
                || metadata_filters.iter().any(|filter| {
                    filter
                        .column_refs()
                        .iter()
                        .any(|column| column.name == "is_checkpoint")
                }));
        let df_meta_schema = DFSchema::try_from(meta_schema.as_ref().clone())?;
        let metadata_filters = metadata_filters
            .iter()
            .map(|f| create_physical_expr(f, &df_meta_schema, &ExecutionProps::new(), &datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext::default()))
            .collect::<Result<Vec<_>>>()?;
        let relation = self.relation.clone();
        let store = self.store.clone();
        let anchor = self.anchor;
        let active_branch_id = self.active_branch_id.clone();
        let output_schema = schema.clone();
        let blob_reader = Arc::clone(&self.blob_reader);
        let ordering = Some(
            if relation.is_some() {
                "lixcol_position"
            } else {
                "position"
            }
            .into(),
        );
        let can_rebind_fetch = relation.is_none() && row_filters.is_empty();
        let build_source: Arc<dyn Fn(Option<usize>) -> ScanSource + Send + Sync> = {
            let source_schema = schema.clone();
            let source_store = store.clone();
            let source_relation = relation.clone();
            let source_output_schema = output_schema.clone();
            let source_metadata_filters = metadata_filters.clone();
            let source_row_filters = row_filters.clone();
            let source_selected_ids = selected_ids.clone();
            let source_active_branch_id = active_branch_id.clone();
            let source_blob_reader = Arc::clone(&blob_reader);
            let planned_limit = limit;
            Arc::new(move |fetch| {
                let limit = match (planned_limit, fetch) {
                    (Some(planned), Some(fetch)) => Some(planned.min(fetch)),
                    (Some(planned), None) => Some(planned),
                    (None, fetch) => fetch,
                };
                let window_size = if limit.is_some() || source_relation.is_some() {
                    1
                } else {
                    64
                };
                let (
                    store,
                    relation,
                    output_schema,
                    metadata_filters,
                    row_filters,
                    active_branch_id,
                ) = (
                    source_store.clone(),
                    source_relation.clone(),
                    source_output_schema.clone(),
                    source_metadata_filters.clone(),
                    source_row_filters.clone(),
                    source_active_branch_id.clone(),
                );
                let selected_ids = source_selected_ids.clone();
                let blob_reader = Arc::clone(&source_blob_reader);
                let max_position = max_position;
                let needs_checkpoint_active = needs_checkpoint_active;
                let anchor = anchor;
                let scan_schema = source_schema.clone();
                batch_stream_source(scan_schema, 1, move |_, context| {
                    let (store, relation, schema, metadata_filters, row_filters, active_branch_id) = (
                        store.clone(),
                        relation.clone(),
                        output_schema.clone(),
                        metadata_filters.clone(),
                        row_filters.clone(),
                        active_branch_id.clone(),
                    );
                    let mut remaining_ids = selected_ids.clone();
                    let blob_reader = Arc::clone(&blob_reader);
                    let stream_schema = schema.clone();
                    let include_state_headers = relation.is_some();
                    let stream = async_stream::try_stream! {
                        let path_cache = Arc::new(crate::filesystem::HistoricalPathIndexCache::default());
                        let mut graph = CommitGraphContext::new().reader(store.clone());
                        let mut next = Some(anchor);
                        let mut position = 0i64;
                        let mut emitted = 0usize;
                        let mut completed_history_checkpoints = 0usize;
                        loop {
                            // Collect a bounded, ordered graph window before resolving the
                            // checkpoint status keys at this query's pinned anchor.
                            // Pushed limits and history diffs use a single-node window.
                            // Ordered pages stop promptly, and each history diff keeps
                            // its original checkpoint and selected-ID frontier.
                            let mut window = Vec::with_capacity(window_size);
                            while window.len() < window_size {
                                if max_position.is_some_and(|ceiling| position > ceiling) { break; }
                                if limit.is_some_and(|n| emitted >= n) || remaining_ids.as_ref().is_some_and(|ids| ids.is_empty()) { break; }
                                let Some(id) = next else { break; };
                                record_work(false);
                                let node = graph.load_node(&id).await.map_err(lix_error_to_datafusion_error)?
                                    .ok_or_else(|| lix_error_to_datafusion_error(crate::commit_graph::missing_commit_graph_error(&id)))?;
                                next = node.parent_commit_ids.first().copied();
                                let current_position = position;
                                position += 1;
                                let selected = remaining_ids
                                    .as_mut()
                                    .map_or(true, |ids| ids.remove(&id.to_string()));
                                let parent = node.parent_commit_ids.first().copied();
                                window.push((node, current_position, selected, parent));
                                if remaining_ids.as_ref().is_some_and(|ids| ids.is_empty()) { break; }
                            }
                            if window.is_empty() { break; }

                            let mut checkpoint_active = window.iter()
                                .map(|(node, _, _, _)| node.is_checkpoint)
                                .collect::<Vec<_>>();
                            if needs_checkpoint_active {
                                let mut key_indices = Vec::new();
                                let mut keys = Vec::new();
                                for (index, (node, _, selected, _)) in window.iter().enumerate() {
                                    if !selected || !node.is_checkpoint { continue; }
                                    let key = crate::tracked_state::TrackedStateKey {
                                        schema_key: crate::undo_redo::UNDO_STATE_SCHEMA_KEY.into(),
                                        file_id: None,
                                        row_pk: crate::row_pk::RowPk::uuid_from_canonical(&node.commit_id.to_string())
                                            .map_err(|error| lix_error_to_datafusion_error(crate::LixError::unknown(error.to_string())))?,
                                    };
                                    key_indices.push(index);
                                    keys.push(key);
                                }
                                if !keys.is_empty() {
                                    record_checkpoint_retirement_work(keys.len());
                                    let mut tracked_state = crate::tracked_state::TrackedStateContext::new().reader(store.clone());
                                    let retirement_rows = tracked_state.load_projected_batch_at_commit(
                                        &anchor.to_string(),
                                        &keys,
                                        &crate::changelog::ChangeRecordProjection::from_columns(&["snapshot_content".into()]),
                                    ).await.map_err(lix_error_to_datafusion_error)?;
                                    for (slot, index) in key_indices.into_iter().enumerate() {
                                        checkpoint_active[index] = !checkpoint_retired_from_row(retirement_rows.row(slot))
                                            .map_err(lix_error_to_datafusion_error)?;
                                    }
                                }
                            }

                            if relation.is_none() {
                                let selected_rows = window
                                    .iter()
                                    .zip(&checkpoint_active)
                                    .filter_map(
                                        |((node, current_position, selected, _), active)| {
                                            (*selected).then_some((node, *current_position, *active))
                                        },
                                    )
                                    .collect::<Vec<_>>();
                                if !selected_rows.is_empty() {
                                    let metadata = log_metadata_window_batch(&selected_rows)?;
                                    let filtered = filter_metadata_batch(metadata, &metadata_filters)?;
                                    if filtered.num_rows() > 0 {
                                        let indices = schema.fields().iter().map(|field| filtered.schema().index_of(field.name())).collect::<std::result::Result<Vec<_>, _>>()?;
                                        let output = filtered.project(&indices)?;
                                        emitted += output.num_rows();
                                        yield output;
                                        if limit.is_some_and(|n| emitted >= n) { break; }
                                    }
                                }
                                continue;
                            }

                            for ((node, current_position, selected, parent), checkpoint_active) in
                                window.into_iter().zip(checkpoint_active)
                            {
                                if !selected { continue; }
                                if !matches_metadata(&metadata_batch(&node, current_position, relation.is_some(), 1, checkpoint_active)?, &metadata_filters)? { continue; }
                                if let Some(relation) = &relation {
                                    let Some(parent) = parent else { continue; }; // root is a baseline, not a synthetic change
                                    let diff_projection = schema.fields().iter().filter_map(|field| relation.schema.index_of(field.name()).ok()).collect::<Vec<_>>();
                                    record_work(true);
                                    let diff = DiffSpec { path_cache: Some(path_cache.clone()), blob_reader: Arc::clone(&blob_reader), store: store.clone(), read_interests: None, interest_endpoints: None, relation: relation.clone(), from_commit_id: parent.to_string(),
                                        to_commit_id: node.commit_id.to_string(), active_branch_id: active_branch_id.clone(), mode: DiffMode::General };
                                    let plan = diff.plan_scan(Some(&diff_projection), &row_filters, None, &ExecutionProps::new()).await?;
                                    let mut batches = plan.source.open(0, context.clone())?;
                                    while let Some(batch) = match batches.try_next().await {
                                        Ok(batch) => batch,
                                        Err(error) => {
                                            let error = frontier::discover(
                                                &diff, parent, &diff_projection, &row_filters,
                                                completed_history_checkpoints, remaining_ids.as_ref(), datafusion_error_to_lix_error(error),
                                            ).await;
                                            Err(lix_error_to_datafusion_error(error))?
                                        }
                                    } {
                                        let meta = metadata_batch(&node, current_position, true, batch.num_rows(), checkpoint_active)?;
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
                                    // Reaching the next selected checkpoint means the consumer still
                                    // needs history, even when the completed diff produced no rows.
                                    completed_history_checkpoints = completed_history_checkpoints.saturating_add(1);
                                } else {
                                    let batch = metadata_batch(&node, current_position, false, 1, checkpoint_active)?;
                                    let indices = schema.fields().iter().map(|f| batch.schema().index_of(f.name())).collect::<std::result::Result<Vec<_>, _>>()?;
                                    emitted += 1;
                                    yield batch.project(&indices)?;
                                }
                            }
                        }
                    };
                    let stream = stream.map_err(move |error| {
                        lix_error_to_datafusion_error(
                            crate::tracked_state::NativeMetadataRef::annotate_history_demand(
                                datafusion_error_to_lix_error(error),
                                include_state_headers,
                            ),
                        )
                    });
                    Ok(Box::pin(RecordBatchStreamAdapter::new(
                        stream_schema,
                        stream,
                    )))
                })
            })
        };
        let source = build_source(None);
        let source = if can_rebind_fetch {
            let rebind = Arc::clone(&build_source);
            source.with_fetch_rebind(move |fetch| rebind(fetch))
        } else {
            source
        };
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

fn checkpoint_retired_from_row(
    row: Option<crate::tracked_state::MaterializedTrackedStateRowRef<'_>>,
) -> std::result::Result<bool, crate::LixError> {
    let Some(snapshot) = row
        .filter(|row| !row.deleted())
        .and_then(|row| row.snapshot_content())
    else {
        return Ok(false);
    };
    #[derive(serde::Deserialize)]
    struct RetirementSnapshot {
        state: RetirementState,
    }
    #[derive(serde::Deserialize)]
    struct RetirementState {
        retired: bool,
    }
    let value: RetirementSnapshot = serde_json::from_str(snapshot.as_str()).map_err(|error| {
        crate::LixError::unknown(format!("invalid checkpoint retirement state: {error}"))
    })?;
    Ok(value.state.retired)
}

#[inline]
fn record_checkpoint_retirement_work(keys: usize) {
    #[cfg(test)]
    CHECKPOINT_RETIREMENT_WORK.with(|work| {
        let (batches, total_keys) = work.get();
        work.set((batches + 1, total_keys + keys));
    });
    #[cfg(not(test))]
    let _ = keys;
}

#[inline]
fn record_log_metadata_batch(rows: usize) {
    #[cfg(test)]
    MAINLINE_METADATA_WORK.with(|work| {
        let (batches, total_rows) = work.get();
        work.set((batches + 1, total_rows + rows));
    });
    #[cfg(not(test))]
    let _ = rows;
}
