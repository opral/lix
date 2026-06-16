//! Generic `INSERT ... ON CONFLICT` (upsert) driver shared by the writable
//! table specs.
//!
//! The algorithm is table-agnostic: build the proposed insert rows, scan the
//! existing rows that share their conflict identity, and per proposed row
//! either keep the insert (no conflict), drop it (`DO NOTHING`), or apply the
//! `DO UPDATE` assignments to the existing row (with `excluded.*` resolving to
//! the proposed values). Everything is staged as `Replace`, which inserts when
//! absent and replaces when present.
//!
//! Each spec contributes only the small pieces that genuinely vary — its
//! identity columns, its insert/candidate-scan/assignment-apply builders —
//! via [`UpsertSupport`]. The loop, identity matching, and the `excluded`
//! batch augmentation live here once.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, UInt64Array};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::physical_expr::PhysicalExpr;

use crate::sql2::SqlWriteContext;
use crate::sql2::error::lix_error_to_datafusion_error;
use crate::transaction::types::{
    TransactionFileData, TransactionWrite, TransactionWriteMode, TransactionWriteRow,
};

/// Which `ON CONFLICT` action to take on a conflicting row.
pub(crate) enum UpsertAction {
    /// `DO UPDATE SET ...` — assignments compiled over `[table cols, excluded.*]`.
    DoUpdate {
        assignments: Vec<(String, Arc<dyn PhysicalExpr>)>,
    },
    /// `DO NOTHING` — keep the existing row.
    DoNothing,
}

/// The staged writes a spec produces for a slice of upsert rows: the state
/// rows plus any file-data blobs (only `lix_file` populates the latter).
#[derive(Default)]
pub(super) struct StagedUpsert {
    pub(super) rows: Vec<TransactionWriteRow>,
    pub(super) file_data: Vec<TransactionFileData>,
}

impl StagedUpsert {
    /// Plain state rows (the common case for every table except `lix_file`).
    pub(super) fn rows(rows: Vec<TransactionWriteRow>) -> Self {
        Self {
            rows,
            file_data: Vec::new(),
        }
    }

    pub(super) fn with_file_data(
        rows: Vec<TransactionWriteRow>,
        file_data: Vec<TransactionFileData>,
    ) -> Self {
        Self { rows, file_data }
    }

    fn extend(&mut self, other: Self) {
        self.rows.extend(other.rows);
        self.file_data.extend(other.file_data);
    }

    fn is_empty(&self) -> bool {
        self.rows.is_empty() && self.file_data.is_empty()
    }
}

/// The per-table capabilities the generic upsert driver composes. Every method
/// reuses logic the spec already has for plain INSERT/UPDATE.
#[async_trait]
pub(super) trait UpsertSupport: Send + Sync {
    /// The columns forming the unique identity; the conflicting-row lookup
    /// matches on these, and the `ON CONFLICT` target must be a subset.
    fn conflict_identity_columns(&self) -> &[&'static str];

    /// Build staged INSERT rows for a proposed batch (the same builder
    /// `stage_insert` uses).
    async fn insert_staged_rows(
        &self,
        write_ctx: &SqlWriteContext,
        batch: &RecordBatch,
    ) -> Result<StagedUpsert>;

    /// Scan existing rows whose identity matches a proposed row, returned as a
    /// batch in this table's column schema.
    async fn scan_conflict_candidates(
        &self,
        write_ctx: &SqlWriteContext,
        proposed: &RecordBatch,
    ) -> Result<RecordBatch>;

    /// Apply the `DO UPDATE` assignments to an augmented batch — this table's
    /// columns (carrying the existing row) plus `excluded.*` columns (carrying
    /// the proposed row) — producing the staged replacement rows.
    async fn apply_conflict_update(
        &self,
        write_ctx: &SqlWriteContext,
        augmented: &RecordBatch,
        assignments: &[(String, Arc<dyn PhysicalExpr>)],
    ) -> Result<StagedUpsert>;
}

/// Run an upsert over the collected proposed input batches and return the
/// affected-row count (the number of logical rows inserted or updated).
pub(super) async fn execute_upsert<S: UpsertSupport + ?Sized>(
    spec: &S,
    write_ctx: &SqlWriteContext,
    proposed_batches: Vec<RecordBatch>,
    action: &UpsertAction,
) -> Result<u64> {
    let identity_columns = spec.conflict_identity_columns();
    let mut staged = StagedUpsert::default();
    let mut affected: u64 = 0;

    for batch in &proposed_batches {
        let existing = spec.scan_conflict_candidates(write_ctx, batch).await?;
        let existing_by_identity = index_by_identity(&existing, identity_columns)?;

        let mut matched_proposed = Vec::new();
        let mut matched_existing = Vec::new();
        let mut unmatched_proposed = Vec::new();
        for row in 0..batch.num_rows() {
            let key = identity_key(batch, row, identity_columns)?;
            if let Some(&existing_row) = existing_by_identity.get(&key) {
                matched_proposed.push(row as u64);
                matched_existing.push(existing_row as u64);
            } else {
                unmatched_proposed.push(row as u64);
            }
        }

        // Non-conflicting rows are always plain inserts.
        if !unmatched_proposed.is_empty() {
            let insert_batch = take_rows(batch, &unmatched_proposed)?;
            staged.extend(spec.insert_staged_rows(write_ctx, &insert_batch).await?);
            affected += unmatched_proposed.len() as u64;
        }

        // Conflicting rows: DO NOTHING leaves them; DO UPDATE applies assignments.
        if !matched_proposed.is_empty() {
            if let UpsertAction::DoUpdate { assignments } = action {
                let existing_matched = take_rows(&existing, &matched_existing)?;
                let proposed_matched = take_rows(batch, &matched_proposed)?;
                let augmented = augment_with_excluded(&existing_matched, &proposed_matched)?;
                staged.extend(
                    spec.apply_conflict_update(write_ctx, &augmented, assignments)
                        .await?,
                );
                affected += matched_proposed.len() as u64;
            }
        }
    }

    if !staged.is_empty() {
        let write = if staged.file_data.is_empty() {
            TransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: staged.rows,
            }
        } else {
            TransactionWrite::RowsWithFileData {
                mode: TransactionWriteMode::Replace,
                rows: staged.rows,
                file_data: staged.file_data,
                count: affected,
            }
        };
        write_ctx
            .stage_write(write)
            .await
            .map_err(lix_error_to_datafusion_error)?;
    }
    Ok(affected)
}

/// Validate the stated `ON CONFLICT (...)` target: it must be non-empty and
/// every named column must be part of the table's conflict identity (the
/// driver always matches on the full identity).
pub(super) fn validate_conflict_target<S: UpsertSupport + ?Sized>(
    spec: &S,
    table_name: &str,
    target_columns: &[String],
) -> Result<()> {
    let identity = spec.conflict_identity_columns();
    if target_columns.is_empty() {
        return Err(DataFusionError::Execution(format!(
            "INSERT ON CONFLICT on {table_name} requires a conflict target"
        )));
    }
    for column in target_columns {
        if !identity.contains(&column.as_str()) {
            return Err(DataFusionError::Execution(format!(
                "INSERT ON CONFLICT on {table_name} does not support the conflict target column '{column}'; expected a subset of ({})",
                identity.join(", ")
            )));
        }
    }
    Ok(())
}

/// Build the `excluded.<col>` field name for the augmented schema; this is the
/// name the conflict assignments compile their `excluded.*` references to.
pub(crate) fn excluded_field_name(column: &str) -> String {
    format!("excluded.{column}")
}

/// Map each existing row's identity tuple to its row index.
fn index_by_identity(
    batch: &RecordBatch,
    identity_columns: &[&str],
) -> Result<HashMap<Vec<ScalarValue>, usize>> {
    let mut index = HashMap::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        index.insert(identity_key(batch, row, identity_columns)?, row);
    }
    Ok(index)
}

/// The identity tuple of a row, as scalar values of its identity columns.
fn identity_key(
    batch: &RecordBatch,
    row: usize,
    identity_columns: &[&str],
) -> Result<Vec<ScalarValue>> {
    identity_columns
        .iter()
        .map(|column| {
            let index = batch.schema().index_of(column)?;
            ScalarValue::try_from_array(batch.column(index).as_ref(), row)
        })
        .collect()
}

/// Select `indices` rows from `batch` into a new batch.
fn take_rows(batch: &RecordBatch, indices: &[u64]) -> Result<RecordBatch> {
    let index_array = UInt64Array::from(indices.to_vec());
    let columns = batch
        .columns()
        .iter()
        .map(|column| take(column.as_ref(), &index_array, None))
        .collect::<std::result::Result<Vec<ArrayRef>, _>>()?;
    let options = RecordBatchOptions::new().with_row_count(Some(indices.len()));
    RecordBatch::try_new_with_options(batch.schema(), columns, &options)
        .map_err(DataFusionError::from)
}

/// Concatenate the existing-row columns with the proposed-row columns renamed
/// `excluded.<col>`, row-aligned. Both batches must have the same row count.
fn augment_with_excluded(existing: &RecordBatch, proposed: &RecordBatch) -> Result<RecordBatch> {
    let mut fields: Vec<Field> = existing
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    let mut columns: Vec<ArrayRef> = existing.columns().to_vec();

    for (field, column) in proposed
        .schema()
        .fields()
        .iter()
        .zip(proposed.columns().iter())
    {
        fields.push(Field::new(
            excluded_field_name(field.name()),
            field.data_type().clone(),
            field.is_nullable(),
        ));
        columns.push(Arc::clone(column));
    }

    let schema: SchemaRef = Arc::new(Schema::new(fields));
    let options = RecordBatchOptions::new().with_row_count(Some(existing.num_rows()));
    RecordBatch::try_new_with_options(schema, columns, &options).map_err(DataFusionError::from)
}
