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
//! conflict-target resolution, its insert/candidate-scan/assignment-apply
//! builders — via [`UpsertSupport`]. The loop, matching, and the `excluded`
//! batch augmentation live here once.

use std::collections::{BTreeSet, HashMap};
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

/// The semantic identity the table resolved the SQL conflict target to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum UpsertConflictKind {
    Id,
    Path,
}

/// A provider-resolved `ON CONFLICT (...)` target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct UpsertConflictTarget {
    kind: UpsertConflictKind,
    columns: Vec<&'static str>,
}

impl UpsertConflictTarget {
    pub(super) fn id(columns: &[&'static str]) -> Self {
        Self {
            kind: UpsertConflictKind::Id,
            columns: columns.to_vec(),
        }
    }

    pub(super) fn path(columns: &[&'static str]) -> Self {
        Self {
            kind: UpsertConflictKind::Path,
            columns: columns.to_vec(),
        }
    }

    pub(super) fn kind(&self) -> UpsertConflictKind {
        self.kind
    }

    pub(super) fn columns(&self) -> &[&'static str] {
        &self.columns
    }
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
    /// The columns forming the default physical identity.
    fn conflict_identity_columns(&self) -> &[&'static str];

    /// Resolve and validate the SQL `ON CONFLICT (...)` target for this table.
    fn resolve_conflict_target(
        &self,
        table_name: &str,
        target_columns: &[String],
    ) -> Result<UpsertConflictTarget> {
        let identity = self.conflict_identity_columns();
        validate_target_columns(
            table_name,
            target_columns,
            identity,
            "conflict identity columns",
        )?;
        Ok(UpsertConflictTarget::id(identity))
    }

    /// Build staged INSERT rows for a proposed batch (the same builder
    /// `stage_insert` uses).
    async fn insert_staged_rows(
        &self,
        write_ctx: &SqlWriteContext,
        batch: &RecordBatch,
    ) -> Result<StagedUpsert>;

    /// Validate all proposed rows before scanning existing conflict candidates.
    fn validate_proposed_batches(
        &self,
        _batches: &[RecordBatch],
        _target: &UpsertConflictTarget,
    ) -> Result<()> {
        Ok(())
    }

    /// Scan existing rows whose identity matches a proposed row, returned as a
    /// batch in this table's column schema.
    async fn scan_conflict_candidates(
        &self,
        write_ctx: &SqlWriteContext,
        proposed: &RecordBatch,
        target: &UpsertConflictTarget,
    ) -> Result<RecordBatch>;

    /// Validate a matched existing/proposed pair before applying the conflict
    /// action. Most tables need no extra check; filesystem path targets use it
    /// to reject tracked/untracked namespace collisions.
    fn validate_conflict_pair(
        &self,
        _existing: &RecordBatch,
        _existing_row: usize,
        _proposed: &RecordBatch,
        _proposed_row: usize,
        _target: &UpsertConflictTarget,
    ) -> Result<()> {
        Ok(())
    }

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
    target: &UpsertConflictTarget,
    action: &UpsertAction,
) -> Result<u64> {
    let conflict_columns = target.columns();
    let mut staged = StagedUpsert::default();
    let mut affected: u64 = 0;

    spec.validate_proposed_batches(&proposed_batches, target)?;

    for batch in &proposed_batches {
        let existing = spec
            .scan_conflict_candidates(write_ctx, batch, target)
            .await?;
        let existing_by_identity = index_by_identity(&existing, conflict_columns)?;

        let mut matched_proposed = Vec::new();
        let mut matched_existing = Vec::new();
        let mut unmatched_proposed = Vec::new();
        for row in 0..batch.num_rows() {
            let key = identity_key(batch, row, conflict_columns)?;
            if let Some(existing_rows) = existing_by_identity.get(&key) {
                for &existing_row in existing_rows {
                    spec.validate_conflict_pair(&existing, existing_row, batch, row, target)?;
                }
                let existing_row = existing_rows[0];
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

pub(super) fn validate_target_columns(
    table_name: &str,
    target_columns: &[String],
    expected_columns: &[&'static str],
    expected_label: &str,
) -> Result<()> {
    if target_columns.is_empty() {
        return Err(DataFusionError::Execution(format!(
            "INSERT ON CONFLICT on {table_name} requires a conflict target"
        )));
    }
    if target_columns.len() != expected_columns.len() {
        return Err(DataFusionError::Execution(format!(
            "INSERT ON CONFLICT on {table_name} target must match {expected_label} ({})",
            expected_columns.join(", ")
        )));
    }
    let actual = target_columns
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let expected = expected_columns.iter().copied().collect::<BTreeSet<_>>();
    if actual != expected {
        return Err(DataFusionError::Execution(format!(
            "INSERT ON CONFLICT on {table_name} target must match {expected_label} ({})",
            expected_columns.join(", ")
        )));
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
) -> Result<HashMap<Vec<ScalarValue>, Vec<usize>>> {
    let mut index = HashMap::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        index
            .entry(identity_key(batch, row, identity_columns)?)
            .or_insert_with(Vec::new)
            .push(row);
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
