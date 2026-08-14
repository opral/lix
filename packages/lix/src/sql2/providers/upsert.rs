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
use datafusion::arrow::array::{Array, ArrayRef, UInt64Array};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::physical_expr::PhysicalExpr;

use crate::changelog::CommitId;
use crate::sql2::SqlWriteContext;
use crate::sql2::error::lix_error_to_datafusion_error;
use crate::sql2::exec::datafusion::LIX_INSERT_COLUMN_OMITTED_METADATA_KEY;
use crate::sql2::write_normalization::{insert_column_is_omitted, mark_omitted_insert_columns};
use crate::storage_adapter::StorageAdapterRead;
use crate::transaction::types::{
    RawWriteBatch, TransactionFileContent, TransactionWrite, TransactionWriteMode,
};

use super::spec::DmlReturning;

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
/// rows plus any file-content blobs (only `lix_file` populates the latter).
#[derive(Default)]
pub(super) struct StagedUpsert {
    pub(super) rows: RawWriteBatch,
    pub(super) file_content: Vec<TransactionFileContent>,
    pub(super) branch_ref_intents: Vec<(String, Option<CommitId>, bool)>,
}

impl StagedUpsert {
    /// Plain state rows (the common case for every table except `lix_file`).
    pub(super) fn rows(rows: RawWriteBatch) -> Self {
        Self {
            rows,
            file_content: Vec::new(),
            branch_ref_intents: Vec::new(),
        }
    }

    pub(super) fn rows_with_branch_ref_intents(
        rows: RawWriteBatch,
        branch_ref_intents: Vec<(String, Option<CommitId>, bool)>,
    ) -> Self {
        Self {
            rows,
            file_content: Vec::new(),
            branch_ref_intents,
        }
    }

    pub(super) fn with_file_content(
        rows: RawWriteBatch,
        file_content: Vec<TransactionFileContent>,
    ) -> Self {
        Self {
            rows,
            file_content,
            branch_ref_intents: Vec::new(),
        }
    }

    fn extend(&mut self, other: Self) {
        self.rows.append(other.rows);
        self.file_content.extend(other.file_content);
        self.branch_ref_intents.extend(other.branch_ref_intents);
    }

    fn is_empty(&self) -> bool {
        self.rows.len() == 0 && self.file_content.is_empty()
    }
}

/// One logical row affected by an upsert, retained solely to let a provider
/// recover its exact post-image after the shared driver has staged all writes.
/// The row comes from the proposed input for an insert and from the existing
/// batch for a conflict update; either way it retains the stable row identity.
#[derive(Clone)]
pub(super) struct UpsertReturningRow {
    batch: RecordBatch,
    row_index: usize,
}

impl UpsertReturningRow {
    fn proposed(batch: &RecordBatch, row_index: usize) -> Self {
        Self {
            batch: batch.clone(),
            row_index,
        }
    }

    fn existing(batch: &RecordBatch, row_index: usize) -> Self {
        Self {
            batch: batch.clone(),
            row_index,
        }
    }

    pub(super) fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    pub(super) fn row_index(&self) -> usize {
        self.row_index
    }
}

/// The per-table capabilities the generic upsert driver composes. Every method
/// reuses logic the spec already has for plain INSERT/UPDATE.
#[async_trait]
pub(super) trait UpsertSupport<R>: Send + Sync
where
    R: StorageAdapterRead + Clone + Send + Sync + 'static,
{
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
        write_ctx: &SqlWriteContext<R>,
        batch: &RecordBatch,
    ) -> Result<StagedUpsert>;

    /// Validate every proposed row before conflict routing. INSERT-only
    /// constraints must still run when the conflict action bypasses the
    /// plain-insert builder.
    fn validate_proposed_batch(&self, _batch: &RecordBatch) -> Result<()> {
        Ok(())
    }

    /// Materialize provider defaults that a `DO UPDATE` expression can read
    /// through `excluded.*`.
    ///
    /// DataFusion represents omitted INSERT columns as typed NULLs plus intent
    /// metadata at the provider boundary. That is sufficient for plain INSERT
    /// staging, but `excluded.*` denotes the proposed row after defaults have
    /// been applied. Providers with defaults that can be referenced by a
    /// conflict assignment replace those placeholders here.
    async fn materialize_excluded_defaults(
        &self,
        _write_ctx: &SqlWriteContext<R>,
        proposed: &RecordBatch,
    ) -> Result<RecordBatch> {
        Ok(proposed.clone())
    }

    /// Materialize any generated insert identity needed to reload an upsert's
    /// post-image. Implementations must make that value explicit to their
    /// insert staging path. The default deliberately rejects so `RETURNING`
    /// can never silently fall back to a count-only upsert.
    async fn materialize_returning_insert_defaults(
        &self,
        _write_ctx: &SqlWriteContext<R>,
        _proposed: &RecordBatch,
    ) -> Result<RecordBatch> {
        Err(DataFusionError::Execution(
            "INSERT ON CONFLICT RETURNING is not supported on this table".to_string(),
        ))
    }

    /// Capture an upsert's post-image after shared staging has succeeded.
    /// Filesystem providers reload derived and audit values from the overlay.
    async fn capture_upsert_returning(
        &self,
        _write_ctx: &SqlWriteContext<R>,
        _affected_rows: Vec<UpsertReturningRow>,
        _returning: DmlReturning,
    ) -> Result<()> {
        Err(DataFusionError::Execution(
            "INSERT ON CONFLICT RETURNING is not supported on this table".to_string(),
        ))
    }

    /// Scan existing rows whose identity matches a proposed row, returned as a
    /// batch in this table's column schema.
    async fn scan_conflict_candidates(
        &self,
        write_ctx: &SqlWriteContext<R>,
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
        write_ctx: &SqlWriteContext<R>,
        augmented: &RecordBatch,
        assignments: &[(String, Arc<dyn PhysicalExpr>)],
    ) -> Result<StagedUpsert>;
}

/// Run an upsert over the collected proposed input batches and return the
/// affected-row count (the number of logical rows inserted or updated).
pub(super) async fn execute_upsert<R, S: UpsertSupport<R> + ?Sized>(
    spec: &S,
    write_ctx: &SqlWriteContext<R>,
    proposed_batches: Vec<RecordBatch>,
    target: &UpsertConflictTarget,
    action: &UpsertAction,
) -> Result<u64>
where
    R: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    let conflict_columns = target.columns();
    let mut staged = StagedUpsert::default();
    let mut affected: u64 = 0;
    let proposed_batches = proposed_batches
        .into_iter()
        .map(|batch| {
            let Some(explicit_columns) = write_ctx.explicit_insert_columns() else {
                return Ok(batch);
            };
            let omitted_columns = batch
                .schema()
                .fields()
                .iter()
                .filter(|field| !explicit_columns.contains(field.name().as_str()))
                .map(|field| field.name().clone())
                .collect::<BTreeSet<_>>();
            mark_omitted_insert_columns(batch, &omitted_columns)
        })
        .collect::<Result<Vec<_>>>()?;

    for batch in &proposed_batches {
        spec.validate_proposed_batch(batch)?;
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
                let proposed_matched = spec
                    .materialize_excluded_defaults(write_ctx, &proposed_matched)
                    .await?;
                let augmented = augment_with_excluded(&existing_matched, &proposed_matched)?;
                staged.extend(
                    spec.apply_conflict_update(write_ctx, &augmented, assignments)
                        .await?,
                );
                affected += matched_proposed.len() as u64;
            }
        }
    }

    stage_upsert(write_ctx, staged, affected).await?;
    Ok(affected)
}

/// Run an upsert that must produce an exact post-image. The shared conflict
/// algorithm retains the stable identity for every logical insert/update in
/// input-row order, then asks the provider to reload that post-image from the
/// transaction overlay once all staged writes are visible.
pub(super) async fn execute_upsert_with_returning<R, S: UpsertSupport<R> + ?Sized>(
    spec: &S,
    write_ctx: &SqlWriteContext<R>,
    proposed_batches: Vec<RecordBatch>,
    target: &UpsertConflictTarget,
    action: &UpsertAction,
    returning: DmlReturning,
) -> Result<u64>
where
    R: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    let conflict_columns = target.columns();
    let mut staged = StagedUpsert::default();
    let mut affected = 0_u64;
    let mut returning_rows = Vec::new();
    let mut normalized_batches = Vec::with_capacity(proposed_batches.len());

    for batch in proposed_batches {
        let batch = if let Some(explicit_columns) = write_ctx.explicit_insert_columns() {
            let omitted_columns = batch
                .schema()
                .fields()
                .iter()
                .filter(|field| !explicit_columns.contains(field.name().as_str()))
                .map(|field| field.name().clone())
                .collect::<BTreeSet<_>>();
            mark_omitted_insert_columns(batch, &omitted_columns)?
        } else {
            batch
        };
        normalized_batches.push(
            spec.materialize_returning_insert_defaults(write_ctx, &batch)
                .await?,
        );
    }

    for batch in &normalized_batches {
        spec.validate_proposed_batch(batch)?;
        let existing = spec
            .scan_conflict_candidates(write_ctx, batch, target)
            .await?;
        let existing_by_identity = index_by_identity(&existing, conflict_columns)?;

        let mut matched_proposed = Vec::new();
        let mut matched_existing = Vec::new();
        let mut unmatched_proposed = Vec::new();
        let mut existing_for_proposed = vec![None; batch.num_rows()];
        for row in 0..batch.num_rows() {
            let key = identity_key(batch, row, conflict_columns)?;
            if let Some(existing_rows) = existing_by_identity.get(&key) {
                for &existing_row in existing_rows {
                    spec.validate_conflict_pair(&existing, existing_row, batch, row, target)?;
                }
                let existing_row = existing_rows[0];
                existing_for_proposed[row] = Some(existing_row);
                matched_proposed.push(row as u64);
                matched_existing.push(existing_row as u64);
            } else {
                unmatched_proposed.push(row as u64);
            }
        }

        if !unmatched_proposed.is_empty() {
            let insert_batch = take_rows(batch, &unmatched_proposed)?;
            staged.extend(spec.insert_staged_rows(write_ctx, &insert_batch).await?);
            affected = affected
                .checked_add(u64::try_from(unmatched_proposed.len()).map_err(|_| {
                    DataFusionError::Execution("UPSERT row count overflow".to_string())
                })?)
                .ok_or_else(|| {
                    DataFusionError::Execution("UPSERT row count overflow".to_string())
                })?;
        }

        if !matched_proposed.is_empty() {
            if let UpsertAction::DoUpdate { assignments } = action {
                let existing_matched = take_rows(&existing, &matched_existing)?;
                let proposed_matched = take_rows(batch, &matched_proposed)?;
                let proposed_matched = spec
                    .materialize_excluded_defaults(write_ctx, &proposed_matched)
                    .await?;
                let augmented = augment_with_excluded(&existing_matched, &proposed_matched)?;
                staged.extend(
                    spec.apply_conflict_update(write_ctx, &augmented, assignments)
                        .await?,
                );
                affected = affected
                    .checked_add(u64::try_from(matched_proposed.len()).map_err(|_| {
                        DataFusionError::Execution("UPSERT row count overflow".to_string())
                    })?)
                    .ok_or_else(|| {
                        DataFusionError::Execution("UPSERT row count overflow".to_string())
                    })?;
            }
        }

        // A conflict `DO NOTHING` emits no RETURNING row. All other rows are
        // kept in SQL input order; the existing row owns an update identity.
        for (row, existing_row) in existing_for_proposed.into_iter().enumerate() {
            match (existing_row, action) {
                (None, _) => returning_rows.push(UpsertReturningRow::proposed(batch, row)),
                (Some(existing_row), UpsertAction::DoUpdate { .. }) => {
                    returning_rows.push(UpsertReturningRow::existing(&existing, existing_row));
                }
                (Some(_), UpsertAction::DoNothing) => {}
            }
        }
    }

    stage_upsert(write_ctx, staged, affected).await?;
    spec.capture_upsert_returning(write_ctx, returning_rows, returning)
        .await?;
    Ok(affected)
}

async fn stage_upsert<R>(
    write_ctx: &SqlWriteContext<R>,
    staged: StagedUpsert,
    affected: u64,
) -> Result<()>
where
    R: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    if staged.is_empty() {
        return Ok(());
    }
    let StagedUpsert {
        rows,
        file_content,
        branch_ref_intents,
    } = staged;
    let write = if file_content.is_empty() {
        TransactionWrite::Rows {
            mode: TransactionWriteMode::Replace,
            rows,
        }
    } else {
        TransactionWrite::RowsWithFileContent {
            mode: TransactionWriteMode::Replace,
            rows,
            file_content,
            count: affected,
        }
    };
    write_ctx
        .stage_write(write)
        .await
        .map_err(lix_error_to_datafusion_error)?;
    for (branch_id, commit_id, create) in branch_ref_intents {
        write_ctx
            .stage_branch_ref_intent(&branch_id, commit_id, create)
            .await
            .map_err(lix_error_to_datafusion_error)?;
    }
    Ok(())
}

/// Replace one omitted INSERT placeholder with its provider-evaluated default.
///
/// Keep the omission metadata on the field: it records how the SQL row was
/// authored, while the replacement array is the semantic value visible
/// through `excluded.*`.
pub(super) fn materialize_omitted_column<A>(
    proposed: &RecordBatch,
    column_name: &str,
    values: Arc<A>,
) -> Result<RecordBatch>
where
    A: Array + 'static,
{
    if !insert_column_is_omitted(proposed, column_name) {
        return Ok(proposed.clone());
    }
    let column_index = proposed.schema().index_of(column_name)?;
    let mut columns = proposed.columns().to_vec();
    columns[column_index] = values;
    RecordBatch::try_new(proposed.schema(), columns).map_err(DataFusionError::from)
}

/// Materialize a provider default for a plain INSERT and make the column
/// visible as an explicit value to the staging path. Unlike
/// [`materialize_omitted_column`], this removes the omission marker: plain
/// INSERT builders intentionally treat that marker as "generate later", but
/// a `RETURNING` write needs one stable generated value it can both stage and
/// read back by identity.
pub(super) fn materialize_omitted_insert_default<A>(
    proposed: &RecordBatch,
    column_name: &str,
    values: Arc<A>,
) -> Result<RecordBatch>
where
    A: Array + 'static,
{
    if !insert_column_is_omitted(proposed, column_name) {
        return Ok(proposed.clone());
    }
    let materialized = materialize_omitted_column(proposed, column_name, values)?;
    let fields = materialized
        .schema()
        .fields()
        .iter()
        .map(|field| {
            if field.name() != column_name {
                return field.as_ref().clone();
            }
            let mut metadata = field.metadata().clone();
            metadata.remove(LIX_INSERT_COLUMN_OMITTED_METADATA_KEY);
            field.as_ref().clone().with_metadata(metadata)
        })
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        materialized.schema().metadata().clone(),
    ));
    RecordBatch::try_new(schema, materialized.columns().to_vec()).map_err(DataFusionError::from)
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
