#![allow(
    clippy::match_same_arms,
    clippy::needless_borrow,
    clippy::needless_continue,
    clippy::redundant_closure_for_method_calls,
    clippy::ref_option,
    clippy::unnecessary_wraps
)]

use std::collections::{BTreeMap, BTreeSet, HashSet};

use serde_json::Value as JsonValue;
use tracing::Instrument as _;

use crate::LixError;
use crate::branch::{BRANCH_DESCRIPTOR_SCHEMA_KEY, BRANCH_REF_SCHEMA_KEY};
use crate::catalog::{CatalogSnapshot, ForeignKeyPlan, SchemaCatalogKey, SchemaPlan};
#[cfg(test)]
use crate::changelog::ChangeId;
use crate::changelog::CommitId;
use crate::common::NullableKeyFilter;
use crate::common::format_json_pointer;
#[cfg(test)]
use crate::common::parse_json_pointer;
use crate::common::{json_pointer_get, validate_row_metadata};
#[cfg(test)]
use crate::domain::DomainFileScope;
use crate::domain::{Domain, DomainRowIdentity, committed_row_ref_is_exact_branch_scoped};
use crate::entity_pk::{EntityPk, EntityPkError, canonical_json_text};
use crate::hot_state::{
    HotStateExactBatchRequest, HotStateExactRowRequest, HotStateFilter, HotStateProjection,
    HotStateReadDomain, HotStateReader, HotStateScanRequest, MaterializedHotStateBatch,
    MaterializedHotStateRowRef,
};
use crate::plugin::PLUGIN_OWNER_KEY;
#[cfg(test)]
use crate::schema::{SchemaKey, validate_lix_schema, validate_lix_schema_definition};
use crate::schema::{
    format_lix_schema_validation_errors, schema_from_registered_snapshot, validate_schema_amendment,
};
use crate::transaction::normalization::reject_reserved_schema_namespace;
use crate::transaction::staging::duplicate_insert_identity_message;
use crate::transaction::staging::{
    PreparedInsertRef, PreparedValidationRow, PreparedWriteSet, PreparedWriteValidationSet,
};
#[cfg(test)]
use crate::transaction_types::TransactionWriteOrigin;
use crate::transaction_types::{
    PreparedStateBatch, PreparedStateRowRef, StagedIndexRow, StagedIndexValues,
    TransactionWriteOperation,
};
const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";
const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";
const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";
const COMMIT_SCHEMA_KEY: &str = "lix_commit";
const MAX_DIRECTORY_PARENT_DEPTH: usize = 1024;

/// Immutable view of the final transaction write set before persistence.
///
/// Validation intentionally runs after staging has coalesced overwrites and
/// hydrated generated fields, but before changelog, tracked-state, untracked
/// state, or binary CAS writes are flushed.
pub(crate) struct TransactionValidationInput<'a> {
    staged_writes: &'a PreparedWriteValidationSet<'a>,
    schema_catalog: &'a CatalogSnapshot,
    hot_state: &'a dyn HotStateReader,
    staged_commit_ids: BTreeSet<CommitId>,
    trust_filesystem_planner: bool,
}

impl<'a> TransactionValidationInput<'a> {
    pub(crate) fn new(
        staged_writes: &'a PreparedWriteValidationSet<'a>,
        schema_catalog: &'a CatalogSnapshot,
        hot_state: &'a dyn HotStateReader,
    ) -> Self {
        Self {
            staged_writes,
            schema_catalog,
            hot_state,
            staged_commit_ids: BTreeSet::new(),
            trust_filesystem_planner: false,
        }
    }

    pub(crate) fn with_staged_commit_ids(mut self, staged_commit_ids: BTreeSet<CommitId>) -> Self {
        self.staged_commit_ids = staged_commit_ids;
        self
    }

    /// Trust namespace checks performed while a serialized, bounded write
    /// statement was planned. Explicit transactions retain commit-time
    /// validation because their planner snapshot can become stale.
    pub(crate) fn with_trusted_filesystem_planner(mut self) -> Self {
        self.trust_filesystem_planner = true;
        self
    }

    #[cfg(test)]
    fn from_visible_schemas_for_tests(
        staged_writes: &'a PreparedWriteSet,
        visible_schemas: &'a [JsonValue],
        hot_state: &'a dyn HotStateReader,
    ) -> Self {
        let catalog = Box::leak(Box::new(
            CatalogSnapshot::from_visible_schemas(visible_schemas)
                .expect("test schema catalog should build"),
        ));
        let validation_set = Box::leak(Box::new(staged_writes.validation_set_for_tests()));
        Self::new(validation_set, catalog, hot_state)
    }
}

/// One committed live-state batch plus an optional stable selection.
///
/// The ordinary case remains dense and therefore retains only the batch
/// owner. A defensive post-scan filter allocates one compact ordinal column
/// after the first rejected row; it never rebuilds rows or their payloads.
struct CommittedHotStateRows {
    batch: MaterializedHotStateBatch,
    selected: Option<Vec<u32>>,
}

impl CommittedHotStateRows {
    fn select(
        batch: MaterializedHotStateBatch,
        mut keep: impl FnMut(MaterializedHotStateRowRef<'_>) -> bool,
    ) -> Result<Self, LixError> {
        u32::try_from(batch.len()).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "constraint live-state batch exceeds u32 rows",
            )
        })?;
        let mut selected: Option<Vec<u32>> = None;
        for index in 0..batch.len() {
            let keep = keep(batch.row(index));
            if let Some(ordinals) = selected.as_mut() {
                if keep {
                    ordinals.push(u32::try_from(index).map_err(|_| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "constraint live-state selection exceeds u32 rows",
                        )
                    })?);
                }
            } else if !keep {
                let mut ordinals = Vec::with_capacity(batch.len().saturating_sub(1));
                for ordinal in 0..index {
                    ordinals.push(u32::try_from(ordinal).map_err(|_| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "constraint live-state selection exceeds u32 rows",
                        )
                    })?);
                }
                selected = Some(ordinals);
            }
        }
        Ok(Self { batch, selected })
    }

    fn len(&self) -> usize {
        self.selected
            .as_ref()
            .map_or_else(|| self.batch.len(), Vec::len)
    }

    fn iter(&self) -> CommittedHotStateRowsIter<'_> {
        CommittedHotStateRowsIter {
            rows: self,
            next: 0,
        }
    }

    fn row(&self, index: usize) -> MaterializedHotStateRowRef<'_> {
        let batch_index = self
            .selected
            .as_ref()
            .map_or(index, |selected| selected[index] as usize);
        self.batch.row(batch_index)
    }

    fn first(&self) -> Option<MaterializedHotStateRowRef<'_>> {
        self.iter().next()
    }

    #[cfg(test)]
    fn is_dense(&self) -> bool {
        self.selected.is_none()
    }
}

struct CommittedHotStateRowsIter<'a> {
    rows: &'a CommittedHotStateRows,
    next: usize,
}

impl<'a> Iterator for CommittedHotStateRowsIter<'a> {
    type Item = MaterializedHotStateRowRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next == self.rows.len() {
            return None;
        }
        let selected = self
            .rows
            .selected
            .as_ref()
            .map_or(self.next, |ordinals| ordinals[self.next] as usize);
        self.next += 1;
        Some(self.rows.batch.row(selected))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.rows.len() - self.next;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for CommittedHotStateRowsIter<'_> {}

async fn scan_committed_constraint_rows(
    hot_state: &dyn HotStateReader,
    domain: &Domain,
    schema_keys: Vec<String>,
    entity_pks: Vec<EntityPk>,
    include_tombstones: bool,
) -> Result<CommittedHotStateRows, LixError> {
    let request = HotStateScanRequest {
        filter: HotStateFilter {
            schema_keys: schema_keys.clone(),
            entity_pks: entity_pks.clone(),
            branch_ids: vec![domain.branch_id().to_string()],
            file_ids: domain.file_filters(),
            untracked: Some(domain.untracked()),
            include_tombstones,
            ..Default::default()
        },
        ..Default::default()
    };
    let batch = hot_state
        .scan_domain_batch(
            &request,
            if domain.untracked() {
                HotStateReadDomain::Untracked
            } else {
                HotStateReadDomain::Tracked
            },
        )
        .await?;
    CommittedHotStateRows::select(batch, |row| {
        domain.contains_ref(row)
            && (schema_keys.is_empty() || schema_keys.iter().any(|key| key == row.schema_key()))
            && (entity_pks.is_empty() || entity_pks.contains(row.entity_pk()))
    })
}

/// The tracked committed rows of one collection whose declared column equals a
/// value, resolved through the hot index plane when it can serve the column.
///
/// The index resolves to candidate identities inside the live-state reader; if
/// the collection has no completeness witness the reader silently keeps its
/// ordinary scan, so this is an access-path choice and never a semantic one.
/// The caller re-checks every returned row's actual value.
async fn scan_committed_constraint_rows_by_declared_column(
    hot_state: &dyn HotStateReader,
    domain: &Domain,
    schema_key: &str,
    declared_column_eq: crate::hot_state::DeclaredColumnEq,
) -> Result<CommittedHotStateRows, LixError> {
    let request = HotStateScanRequest {
        filter: HotStateFilter {
            schema_keys: vec![schema_key.to_string()],
            branch_ids: vec![domain.branch_id().to_string()],
            file_ids: domain.file_filters(),
            untracked: Some(domain.untracked()),
            declared_column_eq: Some(declared_column_eq),
            ..Default::default()
        },
        ..Default::default()
    };
    let batch = hot_state
        .scan_domain_batch(
            &request,
            if domain.untracked() {
                HotStateReadDomain::Untracked
            } else {
                HotStateReadDomain::Tracked
            },
        )
        .await?;
    CommittedHotStateRows::select(batch, |row| {
        domain.contains_ref(row) && row.schema_key() == schema_key
    })
}

async fn scan_committed_canonical_rows(
    hot_state: &dyn HotStateReader,
    domain: &Domain,
    schema_key: &str,
    entity_pks: Vec<EntityPk>,
) -> Result<CommittedHotStateRows, LixError> {
    let file_id = match domain.file_filters().as_slice() {
        [] => None,
        [NullableKeyFilter::Null] => None,
        [NullableKeyFilter::Value(file_id)] => Some(file_id.clone()),
        _ => {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "canonical identity validation requires one exact file scope",
            ));
        }
    };
    let requested_entity_pks = entity_pks.clone();
    let rows = entity_pks
        .into_iter()
        .map(|entity_pk| HotStateExactRowRequest {
            schema_key: schema_key.to_string(),
            branch_id: domain.branch_id().to_string(),
            entity_pk,
            file_id: file_id.clone(),
        })
        .collect::<Vec<_>>();
    let projection = HotStateProjection {
        columns: vec![
            "schema_key".to_string(),
            "entity_pk".to_string(),
            "file_id".to_string(),
            "deleted".to_string(),
            "untracked".to_string(),
        ],
    };
    // One plane means one row per identity, so a single retention-agnostic
    // probe already returns whichever member owns the identity. The request is
    // exactly K identities and therefore remains bounded by the directory
    // point-read path; no schema or `All` expansion is permitted here.
    let batch = hot_state
        .load_exact_batch(&HotStateExactBatchRequest {
            rows,
            projection,
            untracked: None,
            include_tombstones: false,
        })
        .await?
        .into_present_batch();
    CommittedHotStateRows::select(batch, |row| {
        domain.contains_canonical_ref(row)
            && row.schema_key() == schema_key
            && requested_entity_pks.contains(row.entity_pk())
    })
}

async fn load_committed_constraint_rows(
    hot_state: &dyn HotStateReader,
    domain: &Domain,
    schema_key: &str,
    entity_pk: EntityPk,
    include_tombstones: bool,
) -> Result<CommittedHotStateRows, LixError> {
    scan_committed_constraint_rows(
        hot_state,
        domain,
        vec![schema_key.to_string()],
        vec![entity_pk],
        include_tombstones,
    )
    .await
}

/// Validates the final transaction write set before durable persistence.
///
/// The validator owns semantic write correctness for every engine write
/// frontend. It builds one transaction-visible schema catalog, validates pending
/// schema registrations, checks exact schema existence, and validates each
/// non-tombstone snapshot against the compiled JSON Schema for its
/// `schema_key`.
///
/// Cross-row constraints such as `x-lix-unique` and foreign keys should also
/// live here so they can share transaction-local indexes and see the final
/// coalesced staged write set.
pub(crate) async fn validate_prepared_writes(
    input: TransactionValidationInput<'_>,
) -> Result<StagedIndexValues, LixError> {
    validate_foreign_key_definitions(input.schema_catalog)?;
    let staged_rows = input.staged_writes.rows().collect::<Vec<_>>();
    let constraint_rows = input.staged_writes.constraint_rows().collect::<Vec<_>>();
    let pending_file_descriptors = PendingFileDescriptorIndex::from_rows(&constraint_rows);
    let pending_schema_domains = PendingSchemaDomains::from_staged_rows(&staged_rows)?;
    validate_registered_schema_identity_is_canonical(&input, &staged_rows)
        .instrument(tracing::debug_span!(
            target: "lix_perf",
            "lix.perf.validation.registered_schema_identity"
        ))
        .await?;
    if row_local_certificates_cover_validation(&staged_rows) {
        validate_committed_insert_identities(&input, None)
            .instrument(tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.validation.insert_identities"
            ))
            .await?;
        // Extraction is skipped here, and that is sound rather than lucky.
        // Every row on this path carries `!requires_transaction_validation`,
        // which `normalization.rs` grants only when the schema declares no
        // uniques and no foreign keys — precisely the schemas whose
        // `indexed_columns` is empty — or when `constraints_unchanged` proves
        // an UPDATE assigned none of the primary key, uniques, or foreign-key
        // local properties, a strict superset of the indexed columns. See
        // `declared_column_rows_never_bypass_extraction`.
        return Ok(StagedIndexValues::default());
    }
    let mut pending_constraints = PendingConstraintIndexes::default();
    let mut validated_constraint_rows =
        BTreeMap::<DomainRowIdentity, ValidatedRowContent<'_>>::new();
    let mut file_owner_validator = FileOwnerReferenceValidator::default();
    let mut staged_snapshots = Vec::new();
    let mut index_extractor = StagedIndexExtractor::new(input.schema_catalog);
    for row in &constraint_rows {
        let row = *row;
        let Some(snapshot) = row.snapshot_json() else {
            pending_constraints.remember_tombstone(row);
            continue;
        };
        let validated = validate_row_content(input.schema_catalog, &pending_schema_domains, row)?;
        pending_constraints.remember_row(row, validated.schema_plan, snapshot)?;
        validated_constraint_rows.insert(row.domain_row_identity(), validated);
    }
    for row in &staged_rows {
        let row = *row;
        if !row.row_content_validated() {
            validate_staged_row_shape(row)?;
            validate_staged_row_metadata(row)?;
        }
        let validated = validated_constraint_rows
            .get(&row.domain_row_identity())
            .copied()
            .map(Ok)
            .unwrap_or_else(|| {
                validate_row_content(input.schema_catalog, &pending_schema_domains, row)
            })?;
        let schema_plan = validated.schema_plan;
        let snapshot = validated.snapshot;
        if let Some(snapshot) = snapshot {
            file_owner_validator
                .validate(&input, &pending_file_descriptors, row)
                .instrument(tracing::debug_span!(
                    target: "lix_perf",
                    "lix.perf.validation.file_owner"
                ))
                .await?;
            if !row.row_content_validated() {
                validate_primary_key_identity(row, schema_plan, snapshot)?;
            }
            pending_constraints.remember_foreign_key_references(row, schema_plan, snapshot)?;
            // The hot index plane's values are lifted out here, where the
            // snapshot is already a parsed `JsonValue` that validation owns.
            // Commit therefore receives them pre-extracted and never decodes a
            // snapshot again — `StageJson::value()` panics after this point on
            // purpose, and routing around it with a second
            // `serde_json::from_str` was a whole extra parse of every row.
            index_extractor.observe(row, snapshot);
            staged_snapshots.push((row, schema_plan, snapshot));
        } else {
            pending_constraints.remember_tombstone(row);
        }
    }
    let unresolved_foreign_keys =
        validate_pending_foreign_keys(&input, &pending_constraints, &staged_snapshots)?;
    validate_pending_delete_restrictions(input.schema_catalog, &pending_constraints)?;
    let unresolved_foreign_keys =
        validate_committed_foreign_keys(&input, &pending_constraints, &unresolved_foreign_keys)
            .instrument(tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.validation.committed_foreign_keys"
            ))
            .await?;
    reject_unresolved_foreign_keys(&unresolved_foreign_keys)?;
    validate_committed_delete_restrictions(&input, input.schema_catalog, &pending_constraints)
        .instrument(tracing::debug_span!(
            target: "lix_perf",
            "lix.perf.validation.delete_restrictions"
        ))
        .await?;
    validate_branch_ref_delete_restrictions(&input, &pending_constraints)
        .instrument(tracing::debug_span!(
            target: "lix_perf",
            "lix.perf.validation.branch_ref_delete_restrictions"
        ))
        .await?;
    validate_committed_insert_identities(&input, Some(&pending_constraints))
        .instrument(tracing::debug_span!(
            target: "lix_perf",
            "lix.perf.validation.insert_identities"
        ))
        .await?;
    validate_committed_unique_constraints(&input, &pending_constraints)
        .instrument(tracing::debug_span!(
            target: "lix_perf",
            "lix.perf.validation.unique_constraints"
        ))
        .await?;
    validate_directory_descriptor_parent_graph(&input, &staged_rows, &constraint_rows)
        .instrument(tracing::debug_span!(
            target: "lix_perf",
            "lix.perf.validation.directory_parent_graph"
        ))
        .await?;
    validate_filesystem_namespace(&input, &staged_rows)
        .instrument(tracing::debug_span!(
            target: "lix_perf",
            "lix.perf.validation.filesystem_namespace"
        ))
        .await?;
    Ok(index_extractor.finish())
}

/// Lifts declared-column values out of snapshots validation has already
/// parsed, so the commit-time hot index hook needs no JSON decode of its own.
///
/// Entity surface specs are derived once per schema key and memoized, because
/// a bulk insert visits one schema thousands of times.
struct StagedIndexExtractor<'a> {
    schema_catalog: &'a CatalogSnapshot,
    specs: BTreeMap<String, Option<std::sync::Arc<crate::sql2::EntitySurfaceSpec>>>,
    values: StagedIndexValues,
}

impl<'a> StagedIndexExtractor<'a> {
    fn new(schema_catalog: &'a CatalogSnapshot) -> Self {
        Self {
            schema_catalog,
            specs: BTreeMap::new(),
            values: StagedIndexValues::default(),
        }
    }

    fn observe(&mut self, row: PreparedValidationRow<'_>, snapshot: &JsonValue) {
        if row.schema_key() == REGISTERED_SCHEMA_KEY {
            // A schema registration is the moment a collection's index is
            // complete for free: no row can exist for a schema that is not
            // registered yet, so an empty index is a correct index and the
            // witness costs one key per declared column. The registration
            // row's `value` column carries the schema document itself.
            let registered = snapshot.get("value").unwrap_or(snapshot);
            if let Ok(spec) = crate::sql2::derive_entity_surface_spec_from_schema(registered) {
                for column in &spec.indexed_columns {
                    self.values
                        .registered_collections
                        .insert((spec.schema_key.clone(), column.ordinal));
                }
            }
            return;
        }
        let schema_catalog = self.schema_catalog;
        let spec = self
            .specs
            .entry(row.schema_key().to_owned())
            .or_insert_with(|| {
                schema_catalog
                    .schema(row.schema_key())
                    .and_then(|schema| {
                        crate::sql2::derive_entity_surface_spec_from_schema(schema).ok()
                    })
                    .map(std::sync::Arc::new)
            })
            .clone();
        let Some(spec) = spec else {
            return;
        };
        if spec.indexed_columns.is_empty() {
            return;
        }
        let PreparedValidationRow::State(state_row) = row;
        self.values.rows.push(StagedIndexRow {
            branch_id: state_row.branch_id.clone(),
            schema_key: state_row.schema_key.clone(),
            entity_pk: state_row.entity_pk.clone(),
            columns: spec
                .indexed_columns
                .iter()
                .map(|column| (column.ordinal, hot_index_value(snapshot, column)))
                .collect(),
        });
    }

    fn finish(self) -> StagedIndexValues {
        self.values
    }
}

/// The two JSON scalar shapes the index plane has an order-preserving key
/// encoding for. Everything else stays on the collection scan.
fn hot_index_value(
    snapshot: &JsonValue,
    column: &crate::sql2::EntityIndexedColumn,
) -> Option<crate::hot_state::HotIndexValue> {
    match snapshot.get(&column.name)? {
        JsonValue::String(value) => Some(crate::hot_state::HotIndexValue::String(value.clone())),
        JsonValue::Number(value) => value.as_i64().map(crate::hot_state::HotIndexValue::Integer),
        _ => None,
    }
}

fn row_local_certificates_cover_validation(staged_rows: &[PreparedValidationRow<'_>]) -> bool {
    !staged_rows.is_empty()
        && staged_rows.iter().all(|row| {
            row.row_content_validated()
                && !row.requires_transaction_validation()
                && row.file_id().is_none()
                && !matches!(
                    row.schema_key(),
                    REGISTERED_SCHEMA_KEY
                        | DIRECTORY_DESCRIPTOR_SCHEMA_KEY
                        | FILE_DESCRIPTOR_SCHEMA_KEY
                        | BRANCH_REF_SCHEMA_KEY
                )
        })
}

/// Returns whether every prepared row carries the same tracked, row-local
/// validation certificate that the per-schema validation path recognizes.
///
/// Normal tracked entity writes reach commit with this certificate. Checking
/// it before constructing the validation index avoids allocating one wrapper
/// and BTreeMap entry per row only to discover every schema scope can skip
/// validation independently.
pub(crate) fn prepared_tracked_rows_have_row_local_certificates(rows: &PreparedStateBatch) -> bool {
    if let Some((facts, schema_key, _branch_id)) = rows.dense_certified_parameter_summary() {
        return !rows.is_empty()
            && facts.row_content_validated
            && !facts.requires_transaction_validation
            && !matches!(
                schema_key,
                REGISTERED_SCHEMA_KEY
                    | DIRECTORY_DESCRIPTOR_SCHEMA_KEY
                    | FILE_DESCRIPTOR_SCHEMA_KEY
                    | BRANCH_REF_SCHEMA_KEY
            );
    }
    !rows.is_empty()
        && rows.iter().all(|row| {
            row.facts.row_content_validated
                && !row.facts.requires_transaction_validation
                && !row.untracked
                && row.file_id.is_none()
                && !matches!(
                    row.schema_key.as_str(),
                    REGISTERED_SCHEMA_KEY
                        | DIRECTORY_DESCRIPTOR_SCHEMA_KEY
                        | FILE_DESCRIPTOR_SCHEMA_KEY
                        | BRANCH_REF_SCHEMA_KEY
                )
        })
}

/// A narrow post-drain proof for the common first import of a plugin-owned
/// file.
///
/// Semantic rows have already passed schema, metadata, and primary-key
/// normalization. The proof adds the only relationship that normally keeps
/// file-scoped rows on the expensive transaction validator: one pending,
/// planner-owned file descriptor and its engine-created blob materialization
/// for their exact file incarnation. It rejects every transaction-wide schema
/// constraint and every lifecycle shape that needs the ordinary validator.
///
/// This is intentionally derived from the immutable drained write set rather
/// than stored during staging. Later writes and overlay coalescing therefore
/// cannot leave a stale certificate behind.
pub(crate) struct FreshPluginFileImportCertificate<'a> {
    state_rows: &'a PreparedStateBatch,
    insert_selection: &'a crate::transaction::staging::PreparedInsertSelection,
}

pub(crate) fn fresh_plugin_file_import_certificate(
    prepared_writes: &PreparedWriteSet,
) -> Option<FreshPluginFileImportCertificate<'_>> {
    let [file_content] = prepared_writes.file_content_writes.as_slice() else {
        return None;
    };
    if file_content.global || file_content.untracked || file_content.had_blob_ref {
        return None;
    }
    if prepared_writes.commit_change_refs_by_branch.len() != 1
        || !prepared_writes
            .commit_change_refs_by_branch
            .contains_key(&file_content.branch_id)
        || !prepared_writes
            .first_commit_parent_override_by_branch
            .is_empty()
        || !prepared_writes.extra_commit_parents_by_branch.is_empty()
        || !prepared_writes.checkpoint_publications.is_empty()
        || !(1..=2).contains(&prepared_writes.insert_selection.len())
    {
        return None;
    }

    let mut descriptor = None;
    let mut blob_ref = None;
    let mut plugin_owner_count = 0_usize;
    for (row_index, row) in prepared_writes.state_rows.iter().enumerate() {
        if row.global
            || row.untracked
            || row.branch_id.as_str() != file_content.branch_id
            || row.snapshot.is_none()
            || !row.facts.row_content_validated
            || row.change_id.is_none()
            || row.commit_id.is_none()
        {
            return None;
        }

        match row.schema_key.as_str() {
            FILE_DESCRIPTOR_SCHEMA_KEY => {
                if row.file_id.map(crate::common::SharedStr::as_str)
                    != Some(file_content.file_id.as_str())
                    || row.entity_pk.as_single_string_owned().ok().as_deref()
                        != Some(file_content.file_id.as_str())
                    || !filesystem_planner_validated_insert(&PreparedValidationRow::State(row))
                    || descriptor.replace((row_index, row)).is_some()
                {
                    return None;
                }
            }
            BLOB_REF_SCHEMA_KEY => {
                // Reconciliation replaces the planner's provisional blob ref
                // with the exact post-plugin materialization. That internal
                // replacement deliberately has no public SQL origin, but it
                // remains a public INSERT under the outer file INSERT mode.
                if row.file_id.map(crate::common::SharedStr::as_str)
                    != Some(file_content.file_id.as_str())
                    || row.entity_pk.as_single_string_owned().ok().as_deref()
                        != Some(file_content.file_id.as_str())
                    || !(row.origin.is_none() || plugin_reconciliation_update(row))
                    || row.facts.requires_transaction_validation
                    || blob_ref.replace((row_index, row)).is_some()
                {
                    return None;
                }
            }
            REGISTERED_SCHEMA_KEY | DIRECTORY_DESCRIPTOR_SCHEMA_KEY | BRANCH_REF_SCHEMA_KEY => {
                return None;
            }
            _ => {
                if row.file_id.map(crate::common::SharedStr::as_str)
                    != Some(file_content.file_id.as_str())
                    || row.facts.requires_transaction_validation
                    || !plugin_reconciliation_update(row)
                {
                    return None;
                }
                if row.schema_key == "lix_key_value"
                    && row
                        .entity_pk
                        .as_single_string()
                        .is_ok_and(|key| key == PLUGIN_OWNER_KEY)
                {
                    plugin_owner_count += 1;
                }
            }
        }
    }

    let (Some((descriptor_index, descriptor)), Some((blob_ref_index, blob_ref))) =
        (descriptor, blob_ref)
    else {
        return None;
    };
    let descriptor_selected = prepared_insert_selection_matches_row(
        &prepared_writes.insert_selection,
        descriptor_index,
        descriptor,
    );
    let blob_ref_selected = prepared_insert_selection_matches_row(
        &prepared_writes.insert_selection,
        blob_ref_index,
        blob_ref,
    );
    let blob_ref_is_internal_update = plugin_reconciliation_update(blob_ref);
    if plugin_owner_count != 1
        || !descriptor_selected
        || prepared_writes.insert_selection.len() != 1 + usize::from(blob_ref_selected)
        || blob_ref_selected == blob_ref_is_internal_update
    {
        return None;
    }

    Some(FreshPluginFileImportCertificate {
        state_rows: &prepared_writes.state_rows,
        insert_selection: &prepared_writes.insert_selection,
    })
}

fn plugin_reconciliation_update(row: PreparedStateRowRef<'_>) -> bool {
    row.origin.is_some_and(|origin| {
        origin.surface == "plugin_reconciliation"
            && origin.operation == TransactionWriteOperation::Update
            && origin.primary_key.is_none()
    })
}

fn prepared_insert_selection_matches_row(
    insert_selection: &crate::transaction::staging::PreparedInsertSelection,
    row_index: usize,
    row: PreparedStateRowRef<'_>,
) -> bool {
    insert_selection.contains(row_index)
        && !row.untracked
        && insert_selection.origin(row_index) == row.origin
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct DirectoryDescriptorScope {
    domain: Domain,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct DirectoryDescriptorSnapshot {
    id: String,
    parent_id: Option<String>,
    name: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct FileDescriptorSnapshot {
    directory_id: Option<String>,
    name: String,
}

async fn validate_directory_descriptor_parent_graph(
    input: &TransactionValidationInput<'_>,
    staged_rows: &[PreparedValidationRow<'_>],
    constraint_rows: &[PreparedValidationRow<'_>],
) -> Result<(), LixError> {
    let scopes = staged_directory_descriptor_scopes(staged_rows);
    for scope in scopes {
        let mut parents = committed_directory_parent_map(input.hot_state, &scope).await?;
        apply_staged_directory_parent_rows(constraint_rows, &scope, &mut parents)?;
        validate_directory_parent_map(&scope, &parents)?;
    }
    Ok(())
}

async fn validate_registered_schema_identity_is_canonical(
    input: &TransactionValidationInput<'_>,
    staged_rows: &[PreparedValidationRow<'_>],
) -> Result<(), LixError> {
    let pending_schema_rows = staged_rows
        .iter()
        .filter(|row| row.schema_key() == REGISTERED_SCHEMA_KEY && row.snapshot_json().is_some())
        .collect::<Vec<_>>();
    if pending_schema_rows.is_empty() {
        return Ok(());
    }

    for pending_row in pending_schema_rows {
        let pending_snapshot = pending_row
            .snapshot_json()
            .expect("pending registered schema row has snapshot_content");
        let (key, _) = schema_from_registered_snapshot(pending_snapshot)?;
        reject_reserved_schema_namespace(&key)?;

        let committed_rows = load_committed_constraint_rows(
            input.hot_state,
            &pending_row.domain().with_exact_file_scope(None),
            REGISTERED_SCHEMA_KEY,
            pending_row.entity_pk().clone(),
            false,
        )
        .await?;
        let Some(row) = committed_rows.first() else {
            continue;
        };
        let Some(snapshot_content) = row.snapshot_content().map(|snapshot| snapshot.as_str())
        else {
            continue;
        };
        let snapshot = parse_registered_schema_snapshot(snapshot_content)?;
        if &snapshot != pending_snapshot {
            let (key, pending_schema) = schema_from_registered_snapshot(pending_snapshot)?;
            let (_, committed_schema) = schema_from_registered_snapshot(&snapshot)?;
            validate_schema_amendment(&committed_schema, &pending_schema).map_err(|_| {
                LixError::new(
                    LixError::CODE_SCHEMA_DEFINITION,
                    format!(
                        "schema '{}' is already registered with a different definition; schema identity must be canonical",
                        key.schema_key
                    ),
                )
            })?;
            continue;
        }
    }

    Ok(())
}

fn parse_registered_schema_snapshot(snapshot_content: &str) -> Result<JsonValue, LixError> {
    serde_json::from_str::<JsonValue>(snapshot_content).map_err(|error| {
        LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            format!("registered schema snapshot_content is invalid JSON: {error}"),
        )
    })
}

fn staged_directory_descriptor_scopes(
    staged_rows: &[PreparedValidationRow<'_>],
) -> BTreeSet<DirectoryDescriptorScope> {
    staged_rows
        .iter()
        .filter(|row| row.schema_key() == DIRECTORY_DESCRIPTOR_SCHEMA_KEY)
        .map(|row| DirectoryDescriptorScope {
            domain: row.domain(),
        })
        .collect()
}

async fn committed_directory_parent_map(
    hot_state: &dyn HotStateReader,
    scope: &DirectoryDescriptorScope,
) -> Result<BTreeMap<String, Option<String>>, LixError> {
    let mut parents = BTreeMap::new();
    for domain in scope.domain.directory_parent_domains() {
        let rows = scan_committed_constraint_rows(
            hot_state,
            &domain,
            vec![DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string()],
            Vec::new(),
            false,
        )
        .await?;
        for row in rows.iter() {
            if !committed_directory_row_is_in_domain(row, scope, &domain) {
                continue;
            }
            let Some(snapshot_content) = row.snapshot_content().map(|snapshot| snapshot.as_str())
            else {
                continue;
            };
            let snapshot = parse_directory_descriptor_snapshot(snapshot_content)?;
            parents.insert(snapshot.id, snapshot.parent_id);
        }
    }
    Ok(parents)
}

fn committed_directory_row_is_in_domain(
    row: MaterializedHotStateRowRef<'_>,
    _scope: &DirectoryDescriptorScope,
    domain: &Domain,
) -> bool {
    row.schema_key() == DIRECTORY_DESCRIPTOR_SCHEMA_KEY && domain.contains_ref(row)
}

fn apply_staged_directory_parent_rows(
    staged_rows: &[PreparedValidationRow<'_>],
    scope: &DirectoryDescriptorScope,
    parents: &mut BTreeMap<String, Option<String>>,
) -> Result<(), LixError> {
    let reachable_domains = scope.domain.directory_parent_domains();
    for row in staged_rows {
        if row.schema_key() != DIRECTORY_DESCRIPTOR_SCHEMA_KEY
            || !reachable_domains.contains(&row.domain())
        {
            continue;
        }
        let id = row.entity_pk().as_single_string_owned()?;
        let Some(snapshot) = row.snapshot_json() else {
            parents.remove(&id);
            continue;
        };
        let snapshot = directory_descriptor_snapshot_from_value(snapshot)?;
        parents.insert(snapshot.id, snapshot.parent_id);
    }
    Ok(())
}

fn parse_directory_descriptor_snapshot(
    snapshot_content: &str,
) -> Result<DirectoryDescriptorSnapshot, LixError> {
    serde_json::from_str::<DirectoryDescriptorSnapshot>(snapshot_content).map_err(|error| {
        LixError::new(
            LixError::CODE_SCHEMA_VALIDATION,
            format!("lix_directory_descriptor snapshot_content is invalid JSON: {error}"),
        )
    })
}

fn directory_descriptor_snapshot_from_value(
    snapshot: &JsonValue,
) -> Result<DirectoryDescriptorSnapshot, LixError> {
    Ok(DirectoryDescriptorSnapshot {
        id: required_snapshot_string(snapshot, "lix_directory_descriptor", "id")?,
        parent_id: optional_snapshot_string(snapshot, "lix_directory_descriptor", "parent_id")?,
        name: required_snapshot_string(snapshot, "lix_directory_descriptor", "name")?,
    })
}

fn file_descriptor_snapshot_from_value(
    snapshot: &JsonValue,
) -> Result<FileDescriptorSnapshot, LixError> {
    Ok(FileDescriptorSnapshot {
        directory_id: optional_snapshot_string(snapshot, "lix_file_descriptor", "directory_id")?,
        name: required_snapshot_string(snapshot, "lix_file_descriptor", "name")?,
    })
}

fn required_snapshot_string(
    snapshot: &JsonValue,
    schema_key: &str,
    field: &str,
) -> Result<String, LixError> {
    let Some(value) = snapshot.get(field) else {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_VALIDATION,
            format!("{schema_key} snapshot_content is missing field '{field}'"),
        ));
    };
    value.as_str().map(str::to_string).ok_or_else(|| {
        LixError::new(
            LixError::CODE_SCHEMA_VALIDATION,
            format!("{schema_key} snapshot_content field '{field}' must be a string"),
        )
    })
}

fn optional_snapshot_string(
    snapshot: &JsonValue,
    schema_key: &str,
    field: &str,
) -> Result<Option<String>, LixError> {
    let Some(value) = snapshot.get(field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    value
        .as_str()
        .map(|value| Some(value.to_string()))
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!("{schema_key} snapshot_content field '{field}' must be a string or null"),
            )
        })
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct FilesystemNamespaceIdentity {
    schema_key: String,
    entity_pk: EntityPk,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum FilesystemNamespaceOccupant {
    Directory {
        entity_pk: EntityPk,
        parent_id: Option<String>,
        name: String,
    },
    File {
        entity_pk: EntityPk,
        directory_id: Option<String>,
        entry_name: String,
    },
}

impl FilesystemNamespaceOccupant {
    fn entity_pk(&self) -> &EntityPk {
        match self {
            Self::Directory { entity_pk, .. } | Self::File { entity_pk, .. } => entity_pk,
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            Self::Directory { .. } => "directory",
            Self::File { .. } => "file",
        }
    }

    fn parent_id(&self) -> &Option<String> {
        match self {
            Self::Directory { parent_id, .. } => parent_id,
            Self::File { directory_id, .. } => directory_id,
        }
    }

    fn entry_name(&self) -> &str {
        match self {
            Self::Directory { name, .. } => name,
            Self::File { entry_name, .. } => entry_name,
        }
    }
}

async fn validate_filesystem_namespace(
    input: &TransactionValidationInput<'_>,
    staged_rows: &[PreparedValidationRow<'_>],
) -> Result<(), LixError> {
    // Filesystem namespace constraints are storage-scope local. Global rows are
    // validated in the global scope and may be projected into branch reads, but
    // projected globals do not participate in branch-local constraint checks.
    let domains = staged_filesystem_namespace_domains(staged_rows);
    for domain in domains {
        if !filesystem_namespace_domain_changed(input, staged_rows, &domain).await? {
            continue;
        }
        let mut occupants =
            committed_filesystem_namespace_occupants(input.hot_state, &domain).await?;
        apply_staged_filesystem_namespace_rows(staged_rows, &domain, &mut occupants)?;
        validate_filesystem_namespace_occupants(&domain, occupants)?;
    }
    Ok(())
}

async fn filesystem_namespace_domain_changed(
    input: &TransactionValidationInput<'_>,
    staged_rows: &[PreparedValidationRow<'_>],
    domain: &Domain,
) -> Result<bool, LixError> {
    // Existing occupants that keep the same kind, identity, parent, and name
    // cannot introduce a namespace collision. Every uncertain case falls back
    // to validating the complete domain below.
    let descriptor_rows = staged_rows
        .iter()
        .copied()
        .filter(|row| {
            prepared_filesystem_row_is_in_domain(*row, domain)
                && (row.schema_key() == DIRECTORY_DESCRIPTOR_SCHEMA_KEY
                    || row.schema_key() == FILE_DESCRIPTOR_SCHEMA_KEY)
        })
        .collect::<Vec<_>>();
    let Some(row) = descriptor_rows.first().copied() else {
        return Ok(false);
    };
    // Removing namespace occupants cannot introduce a collision. Keep mixed
    // delete+insert/rename batches on the complete-domain validation below.
    if descriptor_rows
        .iter()
        .all(|row| row.snapshot_json().is_none())
    {
        return Ok(false);
    }
    // Bounded filesystem writes are serialized from planning through commit.
    // Their transaction-visible namespace resolver already rejects duplicate
    // paths and cross-kind collisions. TransactionWriteOrigin is crate-private,
    // so an exact origin/row match can identify those planner-owned inserts.
    if input.trust_filesystem_planner
        && descriptor_rows
            .iter()
            .all(|row| filesystem_planner_validated_insert(row))
    {
        return Ok(false);
    }
    if descriptor_rows.len() > 1 {
        return Ok(true);
    }
    if row
        .origin()
        .is_some_and(|origin| origin.operation == TransactionWriteOperation::Insert)
        || input.staged_writes.inserts().any(|insert| {
            insert.row.branch_id == row.branch_id()
                && insert.row.untracked == row.untracked()
                && insert.row.file_id.map(crate::common::SharedStr::as_str) == row.file_id()
                && insert.row.schema_key == row.schema_key()
                && insert.row.entity_pk == row.entity_pk()
        })
    {
        return Ok(true);
    }
    let Some(snapshot) = row.snapshot_json() else {
        return Ok(true);
    };
    let committed_rows = load_committed_constraint_rows(
        input.hot_state,
        domain,
        row.schema_key(),
        row.entity_pk().clone(),
        false,
    )
    .await?;
    let Some(committed) = committed_rows.first() else {
        return Ok(true);
    };
    let Some((_, committed_occupant)) = filesystem_namespace_occupant_from_live_row(committed)?
    else {
        return Ok(true);
    };
    Ok(committed_occupant != filesystem_namespace_occupant_from_staged_row(row, snapshot)?)
}

fn filesystem_planner_validated_insert(row: &PreparedValidationRow<'_>) -> bool {
    if row.snapshot_json().is_none() {
        return false;
    }
    let Some(origin) = row.origin() else {
        return false;
    };
    if origin.operation != TransactionWriteOperation::Insert {
        return false;
    }
    let surface_matches_schema = match row.schema_key() {
        FILE_DESCRIPTOR_SCHEMA_KEY => {
            matches!(origin.surface.as_str(), "lix_file" | "lix_file_by_branch")
        }
        DIRECTORY_DESCRIPTOR_SCHEMA_KEY => {
            matches!(
                origin.surface.as_str(),
                "lix_directory" | "lix_directory_by_branch"
            )
        }
        _ => false,
    };
    if !surface_matches_schema {
        return false;
    }
    let Some(primary_key) = origin.primary_key.as_ref() else {
        return false;
    };
    primary_key.columns.len() == 1
        && primary_key.columns[0] == "id"
        && primary_key.values.len() == 1
        && row
            .entity_pk()
            .as_single_string_owned()
            .is_ok_and(|entity_pk| primary_key.values[0] == entity_pk)
}

fn staged_filesystem_namespace_domains(
    staged_rows: &[PreparedValidationRow<'_>],
) -> BTreeSet<Domain> {
    staged_rows
        .iter()
        .filter(|row| {
            row.schema_key() == DIRECTORY_DESCRIPTOR_SCHEMA_KEY
                || row.schema_key() == FILE_DESCRIPTOR_SCHEMA_KEY
        })
        .map(|row| row.domain())
        .map(|domain| Domain::any_file(domain.branch_id().to_string(), domain.untracked()))
        .collect()
}

async fn committed_filesystem_namespace_occupants(
    hot_state: &dyn HotStateReader,
    domain: &Domain,
) -> Result<BTreeMap<FilesystemNamespaceIdentity, FilesystemNamespaceOccupant>, LixError> {
    let rows = scan_committed_constraint_rows(
        hot_state,
        domain,
        vec![
            DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string(),
            FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
        ],
        Vec::new(),
        false,
    )
    .await?;
    let mut occupants = BTreeMap::new();
    for row in rows.iter() {
        if !committed_filesystem_row_is_in_domain(row, domain) {
            continue;
        }
        if let Some((identity, occupant)) = filesystem_namespace_occupant_from_live_row(row)? {
            occupants.insert(identity, occupant);
        }
    }
    Ok(occupants)
}

fn committed_filesystem_row_is_in_domain(
    row: MaterializedHotStateRowRef<'_>,
    domain: &Domain,
) -> bool {
    (row.schema_key() == DIRECTORY_DESCRIPTOR_SCHEMA_KEY
        || row.schema_key() == FILE_DESCRIPTOR_SCHEMA_KEY)
        && domain.contains_ref(row)
}

fn prepared_filesystem_row_is_in_domain(row: PreparedValidationRow<'_>, domain: &Domain) -> bool {
    row.branch_id() == domain.branch_id() && row.untracked() == domain.untracked()
}

fn apply_staged_filesystem_namespace_rows(
    staged_rows: &[PreparedValidationRow<'_>],
    domain: &Domain,
    occupants: &mut BTreeMap<FilesystemNamespaceIdentity, FilesystemNamespaceOccupant>,
) -> Result<(), LixError> {
    for row in staged_rows {
        if (row.schema_key() != DIRECTORY_DESCRIPTOR_SCHEMA_KEY
            && row.schema_key() != FILE_DESCRIPTOR_SCHEMA_KEY)
            || !prepared_filesystem_row_is_in_domain(*row, domain)
        {
            continue;
        }
        let identity = FilesystemNamespaceIdentity {
            schema_key: row.schema_key().to_string(),
            entity_pk: row.entity_pk().clone(),
        };
        let Some(snapshot) = row.snapshot_json() else {
            occupants.remove(&identity);
            continue;
        };
        occupants.insert(
            identity,
            filesystem_namespace_occupant_from_staged_row(*row, snapshot)?,
        );
    }
    Ok(())
}

fn filesystem_namespace_occupant_from_live_row(
    row: MaterializedHotStateRowRef<'_>,
) -> Result<Option<(FilesystemNamespaceIdentity, FilesystemNamespaceOccupant)>, LixError> {
    let Some(snapshot_content) = row.snapshot_content().map(|snapshot| snapshot.as_str()) else {
        return Ok(None);
    };
    let identity = FilesystemNamespaceIdentity {
        schema_key: row.schema_key().to_owned(),
        entity_pk: row.entity_pk().clone(),
    };
    let occupant = match row.schema_key() {
        DIRECTORY_DESCRIPTOR_SCHEMA_KEY => {
            directory_namespace_occupant(row.entity_pk(), snapshot_content)?
        }
        FILE_DESCRIPTOR_SCHEMA_KEY => file_namespace_occupant(row.entity_pk(), snapshot_content)?,
        _ => return Ok(None),
    };
    Ok(Some((identity, occupant)))
}

fn filesystem_namespace_occupant_from_staged_row(
    row: PreparedValidationRow<'_>,
    snapshot: &JsonValue,
) -> Result<FilesystemNamespaceOccupant, LixError> {
    match row.schema_key() {
        DIRECTORY_DESCRIPTOR_SCHEMA_KEY => {
            directory_namespace_occupant_from_value(row.entity_pk(), snapshot)
        }
        FILE_DESCRIPTOR_SCHEMA_KEY => file_namespace_occupant_from_value(row.entity_pk(), snapshot),
        _ => Err(LixError::new(
            LixError::CODE_SCHEMA_VALIDATION,
            format!(
                "filesystem namespace validation cannot parse schema '{}'",
                row.schema_key()
            ),
        )),
    }
}

fn directory_namespace_occupant(
    entity_pk: &EntityPk,
    snapshot_content: &str,
) -> Result<FilesystemNamespaceOccupant, LixError> {
    let snapshot = parse_directory_descriptor_snapshot(snapshot_content)?;
    Ok(FilesystemNamespaceOccupant::Directory {
        entity_pk: entity_pk.clone(),
        parent_id: snapshot.parent_id,
        name: snapshot.name,
    })
}

fn directory_namespace_occupant_from_value(
    entity_pk: &EntityPk,
    snapshot: &JsonValue,
) -> Result<FilesystemNamespaceOccupant, LixError> {
    let snapshot = directory_descriptor_snapshot_from_value(snapshot)?;
    Ok(FilesystemNamespaceOccupant::Directory {
        entity_pk: entity_pk.clone(),
        parent_id: snapshot.parent_id,
        name: snapshot.name,
    })
}

fn file_namespace_occupant(
    entity_pk: &EntityPk,
    snapshot_content: &str,
) -> Result<FilesystemNamespaceOccupant, LixError> {
    let snapshot =
        serde_json::from_str::<FileDescriptorSnapshot>(snapshot_content).map_err(|error| {
            LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!("lix_file_descriptor snapshot_content is invalid JSON: {error}"),
            )
        })?;
    Ok(FilesystemNamespaceOccupant::File {
        entity_pk: entity_pk.clone(),
        directory_id: snapshot.directory_id,
        entry_name: snapshot.name,
    })
}

fn file_namespace_occupant_from_value(
    entity_pk: &EntityPk,
    snapshot: &JsonValue,
) -> Result<FilesystemNamespaceOccupant, LixError> {
    let snapshot = file_descriptor_snapshot_from_value(snapshot)?;
    Ok(FilesystemNamespaceOccupant::File {
        entity_pk: entity_pk.clone(),
        directory_id: snapshot.directory_id,
        entry_name: snapshot.name,
    })
}

fn validate_filesystem_namespace_occupants(
    domain: &Domain,
    occupants: BTreeMap<FilesystemNamespaceIdentity, FilesystemNamespaceOccupant>,
) -> Result<(), LixError> {
    let mut by_parent_and_name =
        BTreeMap::<(Option<String>, String), FilesystemNamespaceOccupant>::new();
    for occupant in occupants.into_values() {
        let key = (
            occupant.parent_id().clone(),
            occupant.entry_name().to_string(),
        );
        if let Some(existing) = by_parent_and_name.insert(key.clone(), occupant.clone()) {
            if existing != occupant {
                return Err(filesystem_namespace_conflict_error(
                    domain, &key.0, &key.1, &existing, &occupant,
                ));
            }
        }
    }
    Ok(())
}

fn filesystem_namespace_conflict_error(
    domain: &Domain,
    parent_id: &Option<String>,
    entry_name: &str,
    existing: &FilesystemNamespaceOccupant,
    conflicting: &FilesystemNamespaceOccupant,
) -> LixError {
    let parent = parent_id.as_deref().unwrap_or("<root>");
    let existing_id = existing
        .entity_pk()
        .as_single_string_owned()
        .unwrap_or_else(|_| "<non-string-entity-pk>".to_string());
    let conflicting_id = conflicting
        .entity_pk()
        .as_single_string_owned()
        .unwrap_or_else(|_| "<non-string-entity-pk>".to_string());
    LixError::new(
        LixError::CODE_UNIQUE,
        format!(
            "filesystem namespace conflict in branch '{}' for parent {parent:?} entry {entry_name:?}: {} '{}' conflicts with {} '{}'",
            domain.branch_id(),
            existing.kind(),
            existing_id,
            conflicting.kind(),
            conflicting_id
        ),
    )
}

fn validate_directory_parent_map(
    scope: &DirectoryDescriptorScope,
    parents: &BTreeMap<String, Option<String>>,
) -> Result<(), LixError> {
    for directory_id in parents.keys() {
        validate_directory_parent_chain(scope, parents, directory_id)?;
    }
    Ok(())
}

fn validate_directory_parent_chain(
    scope: &DirectoryDescriptorScope,
    parents: &BTreeMap<String, Option<String>>,
    start_id: &str,
) -> Result<(), LixError> {
    let mut current_id = start_id;
    let mut seen = BTreeSet::<String>::new();
    for depth in 0..=MAX_DIRECTORY_PARENT_DEPTH {
        if !seen.insert(current_id.to_string()) {
            return Err(directory_parent_cycle_error(scope, start_id, current_id));
        }
        let Some(parent_id) = parents.get(current_id) else {
            return Err(directory_parent_missing_error(scope, start_id, current_id));
        };
        let Some(parent_id) = parent_id.as_deref() else {
            return Ok(());
        };
        current_id = parent_id;
        if depth == MAX_DIRECTORY_PARENT_DEPTH {
            return Err(directory_parent_depth_error(scope, start_id));
        }
    }
    Err(directory_parent_depth_error(scope, start_id))
}

fn directory_parent_cycle_error(
    scope: &DirectoryDescriptorScope,
    start_id: &str,
    repeated_id: &str,
) -> LixError {
    LixError::new(
        LixError::CODE_CONSTRAINT_VIOLATION,
        format!(
            "lix_directory_descriptor parent_id cycle in branch '{}': directory '{}' reaches ancestor '{}' twice",
            scope.domain.branch_id(), start_id, repeated_id
        ),
    )
    .with_hint("Set parent_id to null or to an existing directory outside the directory's descendants.")
}

fn directory_parent_missing_error(
    scope: &DirectoryDescriptorScope,
    start_id: &str,
    missing_id: &str,
) -> LixError {
    LixError::new(
        LixError::CODE_FOREIGN_KEY,
        format!(
            "lix_directory_descriptor parent_id chain in branch '{}' for directory '{}' references missing directory '{}'",
            scope.domain.branch_id(),
            start_id,
            missing_id
        ),
    )
}

fn directory_parent_depth_error(scope: &DirectoryDescriptorScope, start_id: &str) -> LixError {
    LixError::new(
        LixError::CODE_CONSTRAINT_VIOLATION,
        format!(
            "lix_directory_descriptor parent_id chain in branch '{}' for directory '{}' exceeds maximum depth {}",
            scope.domain.branch_id(),
            start_id,
            MAX_DIRECTORY_PARENT_DEPTH
        ),
    )
}

async fn validate_committed_insert_identities(
    input: &TransactionValidationInput<'_>,
    pending_constraints: Option<&PendingConstraintIndexes>,
) -> Result<(), LixError> {
    validate_committed_insert_identity_entries(
        input.hot_state,
        input.staged_writes.inserts().filter(|insert| {
            // `PreparedWriteValidationSet::inserts` and `constraint_rows`
            // contain the same exact schema scope. Building pending constraints
            // records one identity target for every non-tombstone constraint
            // row, so probing that owned identity vector for every insert is
            // equivalent to this column check but turns a large batch into
            // O(n²) entity-primary-key comparisons.
            pending_constraints.is_none() || insert.row.snapshot.is_some()
        }),
        pending_constraints,
    )
    .await
}

/// Retains public INSERT absence semantics for the tracked row-local
/// certificate fast lane without building the per-schema validation index.
///
/// Row-local certificates cover row shape and schema validation only. INSERT
/// identity absence is a committed-state property and must still be checked
/// against the coherent transaction snapshot.
pub(crate) async fn validate_certified_tracked_insert_identities(
    hot_state: &dyn HotStateReader,
    prepared_writes: &PreparedWriteSet,
) -> Result<(), LixError> {
    if let Some((_facts, schema_key, branch_id)) = prepared_writes
        .state_rows
        .dense_certified_parameter_summary()
        && prepared_writes
            .insert_selection
            .is_complete_ordinal_selection(prepared_writes.state_rows.len())
    {
        let scope = crate::collection_generation::CollectionScopeRef {
            schema_key,
            file_id: None,
        };
        if hot_state
            .collection_generation(branch_id, scope)
            .await?
            .is_some_and(|generation| generation.live_count == 0)
        {
            return Ok(());
        }
    }
    let mut inserts = prepared_writes
        .insert_selection
        .iter(&prepared_writes.state_rows);
    if let Some(first) = inserts.next()
        && inserts.all(|insert| {
            insert.row.branch_id == first.row.branch_id
                && insert.row.schema_key == first.row.schema_key
                && insert.row.file_id == first.row.file_id
        })
    {
        let scope = crate::collection_generation::CollectionScopeRef {
            schema_key: first.row.schema_key,
            file_id: first.row.file_id.map(crate::common::SharedStr::as_str),
        };
        if hot_state
            .collection_generation(first.row.branch_id, scope)
            .await?
            .is_some_and(|generation| generation.live_count == 0)
        {
            // This proof comes from the exact coherent read used to resolve
            // commit parents and branch-control preconditions. A concurrent
            // insert before the read makes the count nonzero; one after the
            // read invalidates the publication CAS.
            return Ok(());
        }
    }
    validate_committed_insert_identity_entries(
        hot_state,
        prepared_writes
            .insert_selection
            .iter(&prepared_writes.state_rows),
        None,
    )
    .await
}

/// Retains public INSERT absence semantics for a structural fresh-file import
/// without rebuilding its full transaction validation index.
pub(crate) async fn validate_certified_fresh_plugin_file_import(
    hot_state: &dyn HotStateReader,
    certificate: FreshPluginFileImportCertificate<'_>,
) -> Result<(), LixError> {
    validate_committed_insert_identity_entries(
        hot_state,
        certificate.insert_selection.iter(certificate.state_rows),
        None,
    )
    .await
}

async fn validate_committed_insert_identity_entries<'a, I>(
    hot_state: &dyn HotStateReader,
    entries: I,
    pending_constraints: Option<&PendingConstraintIndexes>,
) -> Result<(), LixError>
where
    I: IntoIterator<Item = PreparedInsertRef<'a>>,
{
    let mut checks = entries.into_iter().collect::<Vec<_>>();
    checks.sort_unstable_by(|left, right| {
        insert_scope_key(*left)
            .cmp(&insert_scope_key(*right))
            .then_with(|| left.row.entity_pk.cmp(right.row.entity_pk))
            .then_with(|| left.row_index.cmp(&right.row_index))
    });

    let mut group_start = 0usize;
    while group_start < checks.len() {
        let first = checks[group_start];
        let mut group_end = group_start + 1;
        while group_end < checks.len()
            && insert_scope_key(checks[group_end]) == insert_scope_key(first)
        {
            group_end += 1;
        }
        let group = &checks[group_start..group_end];
        let domain = Domain::exact_file(
            first.row.branch_id.to_string(),
            first.row.untracked,
            first.row.file_id.map(ToString::to_string),
        );
        let entity_pks = group
            .iter()
            .map(|insert| insert.row.entity_pk.clone())
            .collect::<Vec<_>>();
        let committed_rows =
            scan_committed_canonical_rows(hot_state, &domain, first.row.schema_key, entity_pks)
                .await?;
        let mut committed_ordinals = committed_rows
            .iter()
            .enumerate()
            .filter_map(|(ordinal, row)| {
                (!row.deleted()
                    && !pending_constraints.is_some_and(|pending| pending.tombstones_identity(row)))
                .then_some(ordinal)
            })
            .collect::<Vec<_>>();
        committed_ordinals.sort_unstable_by(|&left, &right| {
            committed_rows
                .row(left)
                .entity_pk()
                .cmp(committed_rows.row(right).entity_pk())
        });

        let mut committed_cursor = 0usize;
        for insert in group {
            while committed_cursor < committed_ordinals.len()
                && committed_rows
                    .row(committed_ordinals[committed_cursor])
                    .entity_pk()
                    < insert.row.entity_pk
            {
                committed_cursor += 1;
            }
            let Some(&committed_ordinal) = committed_ordinals.get(committed_cursor) else {
                continue;
            };
            let committed_row = committed_rows.row(committed_ordinal);
            if committed_row.entity_pk() != insert.row.entity_pk {
                continue;
            }
            if committed_row.untracked() != domain.untracked() {
                let requested = if domain.untracked() {
                    "untracked"
                } else {
                    "tracked"
                };
                let existing = if committed_row.untracked() {
                    "untracked"
                } else {
                    "tracked"
                };
                return Err(with_insert_statement_index(
                    LixError::new(
                        LixError::CODE_UNIQUE,
                        format!(
                            "cannot insert {requested} row for schema '{}' entity_pk {:?}: a canonical {existing} row already exists; delete it first",
                            insert.row.schema_key, insert.row.entity_pk,
                        ),
                    ),
                    insert.statement_index,
                ));
            }
            return Err(with_insert_statement_index(
                LixError::new(
                    LixError::CODE_UNIQUE,
                    duplicate_insert_identity_message(
                        insert.row.schema_key,
                        insert.row.entity_pk,
                        None,
                        insert.origin,
                    ),
                ),
                insert.statement_index,
            ));
        }
        group_start = group_end;
    }
    Ok(())
}

fn with_insert_statement_index(mut error: LixError, statement_index: Option<usize>) -> LixError {
    let Some(statement_index) = statement_index else {
        return error;
    };
    let mut details = match error.details.take() {
        Some(serde_json::Value::Object(details)) => details,
        Some(details) => {
            let mut wrapped = serde_json::Map::new();
            wrapped.insert("cause".to_string(), details);
            wrapped
        }
        None => serde_json::Map::new(),
    };
    details.insert(
        "statementIndex".to_string(),
        serde_json::Value::from(statement_index),
    );
    error.details = Some(serde_json::Value::Object(details));
    error
}

fn insert_scope_key<'a>(
    insert: PreparedInsertRef<'a>,
) -> (&'a str, bool, Option<&'a str>, &'a str) {
    (
        insert.row.branch_id,
        insert.row.untracked,
        insert.row.file_id.map(crate::common::SharedStr::as_str),
        insert.row.schema_key,
    )
}

async fn validate_branch_ref_delete_restrictions(
    input: &TransactionValidationInput<'_>,
    pending_constraints: &PendingConstraintIndexes,
) -> Result<(), LixError> {
    for tombstone in &pending_constraints.tombstones {
        if tombstone.identity.schema_key() != BRANCH_REF_SCHEMA_KEY {
            continue;
        }

        for source_domain in tombstone
            .identity
            .domain()
            .branch_descriptor_domains_for_ref_delete()
        {
            let descriptor_identity = DomainRowIdentity::in_domain(
                source_domain,
                BRANCH_DESCRIPTOR_SCHEMA_KEY,
                tombstone.identity.entity_pk_owned(),
            );
            if pending_constraints.tombstones_target_identity(&descriptor_identity) {
                continue;
            }
            if pending_constraints.has_identity_target(&descriptor_identity) {
                return Err(branch_ref_delete_restriction_error(
                    &tombstone.identity,
                    &descriptor_identity,
                )?);
            }

            let descriptor_rows = load_committed_constraint_rows(
                input.hot_state,
                descriptor_identity.domain(),
                descriptor_identity.schema_key(),
                descriptor_identity.entity_pk_owned(),
                false,
            )
            .await?;
            let Some(descriptor_row) = descriptor_rows.first() else {
                continue;
            };
            if descriptor_row.snapshot_content().is_some()
                && !pending_constraints.tombstones_identity(descriptor_row)
            {
                return Err(branch_ref_delete_restriction_error(
                    &tombstone.identity,
                    &descriptor_identity,
                )?);
            }
        }
    }
    Ok(())
}

fn branch_ref_delete_restriction_error(
    ref_identity: &DomainRowIdentity,
    descriptor_identity: &DomainRowIdentity,
) -> Result<LixError, LixError> {
    Ok(LixError::new(
        LixError::CODE_FOREIGN_KEY,
        format!(
            "cannot delete '{}' row '{}' in branch '{}' because matching '{}' row '{}' would remain without a branch ref",
            ref_identity.schema_key(),
            ref_identity.entity_pk().as_single_string_owned()?,
            ref_identity.domain().branch_id(),
            descriptor_identity.schema_key(),
            descriptor_identity.entity_pk().as_single_string_owned()?,
        ),
    ))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingFileDescriptorState {
    Present,
    Tombstone,
}

#[derive(Debug, Clone, Default)]
struct PendingFileDescriptorIndex {
    by_identity: BTreeMap<DomainRowIdentity, PendingFileDescriptorState>,
}

impl PendingFileDescriptorIndex {
    fn from_rows(staged_rows: &[PreparedValidationRow<'_>]) -> Self {
        let mut index = Self::default();
        for row in staged_rows {
            if row.schema_key() != FILE_DESCRIPTOR_SCHEMA_KEY {
                continue;
            }
            if row.entity_pk().as_single_string_owned().is_ok() {
                let state = if (*row).snapshot_json().is_some() {
                    PendingFileDescriptorState::Present
                } else {
                    PendingFileDescriptorState::Tombstone
                };
                index.by_identity.insert(row.domain_row_identity(), state);
            }
        }
        index
    }

    fn state_in_domain(
        &self,
        domain: &Domain,
        file_id: &str,
    ) -> Option<PendingFileDescriptorState> {
        let entity_pk = EntityPk::uuid_from_canonical(file_id).ok()?;
        self.by_identity
            .get(&DomainRowIdentity::in_domain(
                domain.with_exact_file_scope(Some(file_id.to_string())),
                FILE_DESCRIPTOR_SCHEMA_KEY,
                entity_pk,
            ))
            .copied()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct FileOwnerDescriptorKey {
    domain: Domain,
    file_id: String,
}

#[derive(Default)]
struct FileOwnerReferenceValidator {
    committed_descriptor_exists: BTreeMap<FileOwnerDescriptorKey, bool>,
}

impl FileOwnerReferenceValidator {
    async fn validate(
        &mut self,
        input: &TransactionValidationInput<'_>,
        pending_file_descriptors: &PendingFileDescriptorIndex,
        row: PreparedValidationRow<'_>,
    ) -> Result<(), LixError> {
        let Some(file_id) = row.file_id() else {
            return Ok(());
        };

        let row_domain = row.domain();
        let target_domains = row_domain
            .with_untracked(row.untracked())
            .file_owner_domains();

        for domain in &target_domains {
            if pending_file_descriptors.state_in_domain(domain, file_id)
                == Some(PendingFileDescriptorState::Present)
            {
                return Ok(());
            }
        }

        for domain in &target_domains {
            if pending_file_descriptors.state_in_domain(domain, file_id)
                == Some(PendingFileDescriptorState::Tombstone)
            {
                continue;
            }
            if self
                .committed_file_descriptor_exists_in_domain(input.hot_state, domain, file_id)
                .await?
            {
                return Ok(());
            }
        }

        // The file exists, just in the other lane. Saying "missing file_id" for
        // a file that `SELECT * FROM lix_file` plainly returns sends people
        // looking for the wrong bug, so name the actual mismatch and both
        // lanes. This covers each direction: an untracked row pointing at a
        // tracked file, and a tracked row pointing at an untracked file.
        let other_lane = row_domain.with_untracked(!row.untracked());
        let exists_in_other_lane = match pending_file_descriptors
            .state_in_domain(&other_lane, file_id)
        {
            Some(PendingFileDescriptorState::Present) => true,
            Some(PendingFileDescriptorState::Tombstone) => false,
            None => {
                self.committed_file_descriptor_exists_in_domain(
                    input.hot_state,
                    &other_lane,
                    file_id,
                )
                .await?
            }
        };
        if exists_in_other_lane {
            return Err(lane_mismatched_file_owner_error(row, file_id)?);
        }

        Err(missing_file_owner_reference_error(row, file_id)?)
    }

    async fn committed_file_descriptor_exists_in_domain(
        &mut self,
        hot_state: &dyn HotStateReader,
        domain: &Domain,
        file_id: &str,
    ) -> Result<bool, LixError> {
        let descriptor_domain = domain.with_exact_file_scope(Some(file_id.to_string()));
        let key = FileOwnerDescriptorKey {
            domain: descriptor_domain.clone(),
            file_id: file_id.to_string(),
        };
        if let Some(exists) = self.committed_descriptor_exists.get(&key) {
            return Ok(*exists);
        }
        let exists =
            committed_file_descriptor_exists_in_domain(hot_state, &descriptor_domain, file_id)
                .await?;
        self.committed_descriptor_exists.insert(key, exists);
        Ok(exists)
    }
}

async fn committed_file_descriptor_exists_in_domain(
    hot_state: &dyn HotStateReader,
    descriptor_domain: &Domain,
    file_id: &str,
) -> Result<bool, LixError> {
    let Ok(entity_pk) = EntityPk::uuid_from_canonical(file_id) else {
        return Ok(false);
    };
    let rows = load_committed_constraint_rows(
        hot_state,
        descriptor_domain,
        FILE_DESCRIPTOR_SCHEMA_KEY,
        entity_pk.clone(),
        false,
    )
    .await?;
    let Some(row) = rows.first() else {
        return Ok(false);
    };
    Ok(row.snapshot_content().is_some()
        && row.schema_key() == FILE_DESCRIPTOR_SCHEMA_KEY
        && row.entity_pk() == &entity_pk
        && row.file_id() == Some(file_id))
}

fn lane_mismatched_file_owner_error(
    row: PreparedValidationRow<'_>,
    file_id: &str,
) -> Result<LixError, LixError> {
    let lane = |untracked: bool| if untracked { "untracked" } else { "tracked" };
    let row_lane = lane(row.untracked());
    let file_lane = lane(!row.untracked());
    Ok(LixError::new(
        LixError::CODE_CONSTRAINT_VIOLATION,
        format!(
            "file ownership validation failed for schema '{}': {row_lane} entity '{}' references file_id '{}', which exists but is {file_lane}, on branch '{}'",
            row.schema_key(),
            row.entity_pk().as_json_array_text()?,
            file_id,
            row.branch_id()
        ),
    )
    .with_hint(
        "A row and the file that owns it must share one lane. Write the row as \
         lixcol_untracked = <the file's own value>, or move the file to the row's lane.",
    ))
}

fn missing_file_owner_reference_error(
    row: PreparedValidationRow<'_>,
    file_id: &str,
) -> Result<LixError, LixError> {
    Ok(LixError::new(
        LixError::CODE_FILE_NOT_FOUND,
            format!(
                "file ownership validation failed for schema '{}': entity '{}' references missing file_id '{}' in effective file scope for branch '{}'",
                row.schema_key(),
                row.entity_pk().as_json_array_text()?,
                file_id,
                row.branch_id()
            ),
    )
    .with_hint("Insert a row into lix_file with this id first, or use null for a global entity."))
}

fn validate_staged_row_shape(row: PreparedValidationRow<'_>) -> Result<(), LixError> {
    if row.schema_key().is_empty() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "engine transaction validation requires non-empty schema_key",
        ));
    }
    if row.schema_key() == REGISTERED_SCHEMA_KEY && row.file_id().is_some() {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            "lix_registered_schema rows must not be scoped to a file",
        )
        .with_hint("Schema definitions are scoped by branch and durability only; write them with null file_id."));
    }
    Ok(())
}

fn validate_staged_row_metadata(row: PreparedValidationRow<'_>) -> Result<(), LixError> {
    let Some(metadata) = row.metadata_json() else {
        return Ok(());
    };
    validate_row_metadata(
        metadata,
        format!("metadata for schema '{}'", row.schema_key()),
    )?;
    Ok(())
}

#[derive(Default)]
struct PendingSchemaDomains {
    domains_by_key: BTreeMap<SchemaCatalogKey, BTreeSet<Domain>>,
}

impl PendingSchemaDomains {
    fn from_staged_rows(staged_rows: &[PreparedValidationRow<'_>]) -> Result<Self, LixError> {
        let mut domains_by_key = BTreeMap::<SchemaCatalogKey, BTreeSet<Domain>>::new();
        for row in staged_rows {
            if row.schema_key() != REGISTERED_SCHEMA_KEY {
                continue;
            }
            let Some(snapshot) = row.snapshot_json() else {
                continue;
            };
            let (key, _) = schema_from_registered_snapshot(snapshot)?;
            domains_by_key
                .entry(SchemaCatalogKey::from_schema_key(key))
                .or_default()
                .insert(row.domain().schema_catalog_domain());
        }
        Ok(Self { domains_by_key })
    }

    fn validate_row_schema_domain(&self, row: PreparedValidationRow<'_>) -> Result<(), LixError> {
        let key = SchemaCatalogKey {
            schema_key: row.schema_key().to_string(),
        };
        let Some(domains) = self.domains_by_key.get(&key) else {
            return Ok(());
        };
        let visible_schema_domains = row.domain().schema_catalog_domains();
        if domains
            .iter()
            .any(|domain| visible_schema_domains.contains(domain))
        {
            return Ok(());
        }
        Err(LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            format!(
                "schema '{}' is pending in another validation domain",
                row.schema_key()
            ),
        ))
    }
}

#[derive(Clone, Copy)]
struct ValidatedRowContent<'a> {
    schema_plan: &'a SchemaPlan,
    snapshot: Option<&'a JsonValue>,
}

fn validate_row_content<'a>(
    schema_catalog: &'a CatalogSnapshot,
    pending_schema_domains: &PendingSchemaDomains,
    row: PreparedValidationRow<'a>,
) -> Result<ValidatedRowContent<'a>, LixError> {
    let schema_plan = schema_plan_for_row(schema_catalog, pending_schema_domains, row)?;
    validate_schema_matches_row(row, schema_plan)?;
    let snapshot = if row.row_content_validated() {
        row.snapshot_json()
    } else {
        validate_snapshot_content(row, schema_plan)?
    };
    Ok(ValidatedRowContent {
        schema_plan,
        snapshot,
    })
}

fn schema_plan_for_row<'a>(
    schema_catalog: &'a CatalogSnapshot,
    pending_schema_domains: &PendingSchemaDomains,
    row: PreparedValidationRow<'_>,
) -> Result<&'a SchemaPlan, LixError> {
    pending_schema_domains.validate_row_schema_domain(row)?;
    if let Some(plan) = schema_catalog.plan(row.schema_plan_id()) {
        if plan.key.schema_key == row.schema_key() {
            return Ok(plan);
        }
    }
    #[cfg(test)]
    if let Some((_, plan)) = schema_catalog.plan_for_key(row.schema_key()) {
        return Ok(plan);
    }
    Err(LixError::new(
        LixError::CODE_SCHEMA_DEFINITION,
        format!(
            "schema plan for schema '{}' is not visible to this transaction",
            row.schema_key()
        ),
    ))
}

fn validate_schema_matches_row(
    row: PreparedValidationRow<'_>,
    schema_plan: &SchemaPlan,
) -> Result<(), LixError> {
    if schema_plan.key.schema_key != row.schema_key() {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            format!(
                "schema plan mismatch: row targets schema '{}' but plan is schema '{}'",
                row.schema_key(),
                schema_plan.key.schema_key,
            ),
        ));
    }
    Ok(())
}

fn validate_snapshot_content<'a>(
    row: PreparedValidationRow<'a>,
    schema_plan: &SchemaPlan,
) -> Result<Option<&'a JsonValue>, LixError> {
    let Some(snapshot) = row.snapshot_json() else {
        return Ok(None);
    };
    if let Err(errors) = schema_plan.compiled_schema.validate(snapshot) {
        let details = format_lix_schema_validation_errors(errors);
        return Err(LixError::new(
            LixError::CODE_SCHEMA_VALIDATION,
            format!(
                "snapshot_content validation failed for schema '{}': {details}",
                row.schema_key()
            ),
        ));
    }
    Ok(Some(snapshot))
}

fn validate_primary_key_identity(
    row: PreparedValidationRow<'_>,
    schema_plan: &SchemaPlan,
    snapshot: &JsonValue,
) -> Result<(), LixError> {
    let Some(primary_key_paths) = schema_plan.primary_key.as_ref() else {
        return Ok(());
    };
    let component_types = schema_plan
        .primary_key_component_types
        .as_deref()
        .expect("primary-key paths and component types are compiled together");
    let derived = EntityPk::from_primary_key_plan(snapshot, primary_key_paths, component_types)
        .map_err(|error| primary_key_identity_error(row, primary_key_paths, error))?;
    if row.entity_pk() != &derived {
        return Err(LixError::new(
            LixError::CODE_UNIQUE,
            format!(
                "primary-key constraint violation on schema '{}': entity_pk '{}' does not match derived primary key '{}'",
                row.schema_key(),
                row.entity_pk().as_json_array_text()?,
                derived.as_json_array_text()?
            ),
        ));
    }
    Ok(())
}

#[derive(Default)]
struct PendingConstraintIndexes {
    unique_values: BTreeMap<PendingUniqueKey, EntityPk>,
    identity_targets: HashSet<DomainRowIdentity>,
    fk_targets: BTreeMap<PendingForeignKeyTargetKey, Vec<PendingForeignKeyTarget>>,
    fk_references: BTreeMap<PendingForeignKeyReferenceTarget, Vec<PendingForeignKeyReference>>,
    tombstones: Vec<PendingTombstone>,
    tombstone_identities: HashSet<DomainRowIdentity>,
}

impl PendingConstraintIndexes {
    fn remember_tombstone(&mut self, row: PreparedValidationRow<'_>) {
        let identity = row.domain_row_identity();
        self.tombstone_identities.insert(identity.clone());
        self.tombstones.push(PendingTombstone { identity });
    }

    fn remember_row(
        &mut self,
        row: PreparedValidationRow<'_>,
        schema_plan: &SchemaPlan,
        snapshot: &JsonValue,
    ) -> Result<(), LixError> {
        self.remember_identity_target(row);
        self.remember_primary_key_target(row, schema_plan, snapshot);
        self.remember_unique_targets(row, schema_plan, snapshot)?;
        Ok(())
    }

    fn remember_identity_target(&mut self, row: PreparedValidationRow<'_>) {
        self.identity_targets.insert(row.domain_row_identity());
    }

    fn remember_primary_key_target(
        &mut self,
        row: PreparedValidationRow<'_>,
        schema_plan: &SchemaPlan,
        snapshot: &JsonValue,
    ) {
        if let Some(primary_key_paths) = schema_plan.primary_key.as_ref() {
            self.remember_fk_target(row, primary_key_paths, snapshot);
        }
    }

    fn remember_unique_targets(
        &mut self,
        row: PreparedValidationRow<'_>,
        schema_plan: &SchemaPlan,
        snapshot: &JsonValue,
    ) -> Result<(), LixError> {
        for unique_paths in &schema_plan.uniques {
            let Some(value) = UniqueConstraintValue::from_snapshot(snapshot, unique_paths) else {
                continue;
            };
            self.remember_fk_target(row, unique_paths, snapshot);
            let key = PendingUniqueKey {
                schema_key: row.schema_key().to_string(),
                domain: row.domain(),
                pointer_group: unique_paths.clone(),
                value,
            };
            if let Some(existing_entity_pk) = self
                .unique_values
                .insert(key.clone(), row.entity_pk().clone())
            {
                if existing_entity_pk != *row.entity_pk() {
                    return Err(LixError::new(
                        LixError::CODE_UNIQUE,
                        format!(
                            "unique constraint violation on {}.{} for value {}: rows '{}' and '{}' conflict",
                            row.schema_key(),
                            format_pointer_group(&key.pointer_group),
                            key.value.display(),
                            existing_entity_pk.as_json_array_text()?,
                            row.entity_pk().as_json_array_text()?
                        ),
                    ));
                }
            }
        }
        Ok(())
    }

    fn remember_fk_target(
        &mut self,
        row: PreparedValidationRow<'_>,
        pointer_group: &[Vec<String>],
        snapshot: &JsonValue,
    ) {
        let Some(value) = UniqueConstraintValue::from_snapshot(snapshot, pointer_group) else {
            return;
        };
        self.fk_targets
            .entry(PendingForeignKeyTargetKey {
                schema_key: row.schema_key().to_string(),
                domain: row.domain(),
                pointer_group: pointer_group.to_vec(),
                value,
            })
            .or_default()
            .push(PendingForeignKeyTarget {
                entity_pk: row.entity_pk().clone(),
            });
    }

    fn remember_foreign_key_references(
        &mut self,
        row: PreparedValidationRow<'_>,
        schema_plan: &SchemaPlan,
        snapshot: &JsonValue,
    ) -> Result<(), LixError> {
        for foreign_key in &schema_plan.foreign_keys {
            let Some(local_value) = UniqueConstraintValue::from_snapshot_non_null(
                snapshot,
                &foreign_key.local_properties,
            ) else {
                continue;
            };
            let target = PendingForeignKeyReferenceTarget::Key(PendingForeignKeyTargetKey {
                schema_key: foreign_key.referenced_schema.schema_key.clone(),
                domain: foreign_key_target_domain(row, foreign_key),
                pointer_group: foreign_key.referenced_properties.clone(),
                value: local_value,
            });
            self.fk_references
                .entry(target)
                .or_default()
                .push(PendingForeignKeyReference {
                    identity: row.domain_row_identity(),
                });
        }

        Ok(())
    }

    fn tombstones_identity(&self, row: MaterializedHotStateRowRef<'_>) -> bool {
        !self.tombstone_identities.is_empty()
            && committed_row_ref_is_exact_branch_scoped(row, row.branch_id())
            && self
                .tombstone_identities
                .contains(&DomainRowIdentity::in_domain(
                    Domain::for_live_row_ref(row),
                    row.schema_key().to_string(),
                    row.entity_pk().clone(),
                ))
    }

    fn replaces_committed_identity(&self, row: MaterializedHotStateRowRef<'_>) -> bool {
        self.identity_targets
            .contains(&DomainRowIdentity::in_domain(
                Domain::for_live_row_ref(row),
                row.schema_key().to_string(),
                row.entity_pk().clone(),
            ))
    }

    fn has_identity_target(&self, identity: &DomainRowIdentity) -> bool {
        self.identity_targets.contains(identity)
    }

    fn tombstones_target_identity(&self, identity: &DomainRowIdentity) -> bool {
        self.tombstone_identities.contains(identity)
    }

    fn has_fk_target_key(&self, key: &PendingForeignKeyTargetKey) -> bool {
        self.fk_targets
            .get(key)
            .is_some_and(|targets| !targets.is_empty())
    }

    fn has_reachable_fk_target_key(&self, key: &PendingForeignKeyTargetKey) -> bool {
        key.domain.fk_target_domains().iter().any(|domain| {
            self.has_fk_target_key(&PendingForeignKeyTargetKey {
                domain: domain.clone(),
                ..key.clone()
            })
        })
    }

    fn active_references_to(
        &self,
        target: &PendingForeignKeyReferenceTarget,
    ) -> Vec<&PendingForeignKeyReference> {
        self.fk_references
            .get(target)
            .into_iter()
            .flat_map(|references| references.iter())
            .filter(|reference| !self.tombstones_target_identity(&reference.identity))
            .collect()
    }

    fn active_references_to_any(
        &self,
        targets: &[PendingForeignKeyReferenceTarget],
    ) -> Vec<&PendingForeignKeyReference> {
        let mut references = Vec::new();
        for target in targets {
            references.extend(self.active_references_to(target));
        }
        references
    }

    #[cfg(test)]
    fn has_fk_reference_to_key(
        &self,
        schema_key: &str,
        branch_id: &str,
        file_id: Option<&str>,
        pointer_group: &[&str],
        value: UniqueConstraintValue,
    ) -> Result<bool, LixError> {
        let pointer_group = pointer_group
            .iter()
            .map(|pointer| parse_json_pointer(pointer))
            .collect::<Result<Vec<_>, _>>()?;
        let key = PendingForeignKeyReferenceTarget::Key(PendingForeignKeyTargetKey {
            schema_key: schema_key.to_string(),
            domain: Domain::exact_file(branch_id.to_string(), false, file_id.map(str::to_string)),
            pointer_group,
            value,
        });
        Ok(self.fk_references.contains_key(&key))
    }

    #[cfg(test)]
    fn has_fk_target(
        &self,
        schema_key: &str,
        branch_id: &str,
        file_id: Option<&str>,
        pointer_group: &[&str],
        value: UniqueConstraintValue,
    ) -> Result<bool, LixError> {
        let pointer_group = pointer_group
            .iter()
            .map(|pointer| parse_json_pointer(pointer))
            .collect::<Result<Vec<_>, _>>()?;
        let key = PendingForeignKeyTargetKey {
            schema_key: schema_key.to_string(),
            domain: Domain::exact_file(branch_id.to_string(), false, file_id.map(str::to_string)),
            pointer_group,
            value,
        };
        Ok(self.fk_targets.contains_key(&key))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingTombstone {
    identity: DomainRowIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingForeignKeyTarget {
    entity_pk: EntityPk,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingForeignKeyReference {
    identity: DomainRowIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PendingUniqueKey {
    schema_key: String,
    domain: Domain,
    pointer_group: Vec<Vec<String>>,
    value: UniqueConstraintValue,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PendingUniqueConstraintScope {
    schema_key: String,
    domain: Domain,
    pointer_group: Vec<Vec<String>>,
}

impl From<&PendingUniqueKey> for PendingUniqueConstraintScope {
    fn from(key: &PendingUniqueKey) -> Self {
        Self {
            schema_key: key.schema_key.clone(),
            domain: key.domain.clone(),
            pointer_group: key.pointer_group.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PendingForeignKeyTargetKey {
    schema_key: String,
    domain: Domain,
    pointer_group: Vec<Vec<String>>,
    value: UniqueConstraintValue,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum PendingForeignKeyReferenceTarget {
    Key(PendingForeignKeyTargetKey),
}

fn validate_pending_delete_restrictions(
    schema_catalog: &CatalogSnapshot,
    pending_constraints: &PendingConstraintIndexes,
) -> Result<(), LixError> {
    if pending_constraints.fk_references.is_empty() {
        return Ok(());
    }

    for tombstone in &pending_constraints.tombstones {
        let Some((_, schema_plan)) = schema_catalog.plan_for_key(tombstone.identity.schema_key())
        else {
            continue;
        };
        if let Some(primary_key_paths) = schema_plan.primary_key.as_ref() {
            let targets = tombstone
                .identity
                .domain()
                .fk_source_domains_for_target()
                .into_iter()
                .map(|domain| {
                    PendingForeignKeyReferenceTarget::Key(PendingForeignKeyTargetKey {
                        schema_key: tombstone.identity.schema_key_owned(),
                        domain,
                        pointer_group: primary_key_paths.clone(),
                        value: UniqueConstraintValue::from_entity_pk(
                            tombstone.identity.entity_pk(),
                        ),
                    })
                })
                .collect::<Vec<_>>();
            reject_pending_delete_references(
                &tombstone.identity,
                &targets,
                pending_constraints.active_references_to_any(&targets),
            )?;
        }
    }
    Ok(())
}

fn reject_pending_delete_references(
    deleted_identity: &DomainRowIdentity,
    targets: &[PendingForeignKeyReferenceTarget],
    references: Vec<&PendingForeignKeyReference>,
) -> Result<(), LixError> {
    let Some(reference) = references.first() else {
        return Ok(());
    };
    let target = targets
        .first()
        .expect("delete restriction callers provide at least one target");
    Err(LixError::new(
        LixError::CODE_FOREIGN_KEY,
        format!(
            "cannot delete '{}' row '{}' in branch '{}' because pending row '{}' references it{}",
            deleted_identity.schema_key(),
            deleted_identity.entity_pk().as_json_array_text()?,
            deleted_identity.domain().branch_id(),
            reference.identity.entity_pk().as_json_array_text()?,
            pending_foreign_key_reference_target_description(target)?
        ),
    ))
}

fn pending_foreign_key_reference_target_description(
    target: &PendingForeignKeyReferenceTarget,
) -> Result<String, LixError> {
    match target {
        PendingForeignKeyReferenceTarget::Key(target) => Ok(format!(
            " through '{}.{}' value {}",
            target.schema_key,
            format_pointer_group(&target.pointer_group),
            target.value.display()
        )),
    }
}

async fn validate_committed_delete_restrictions(
    input: &TransactionValidationInput<'_>,
    schema_catalog: &CatalogSnapshot,
    pending_constraints: &PendingConstraintIndexes,
) -> Result<(), LixError> {
    let mut normal_batches = BTreeMap::<
        NormalDeleteRestrictionBatchKey,
        BTreeMap<UniqueConstraintValue, Vec<DomainRowIdentity>>,
    >::new();
    for tombstone in &pending_constraints.tombstones {
        let delete_plan = schema_catalog.delete_plan_for_key(tombstone.identity.schema_key());
        if !delete_plan.has_committed_checks() {
            continue;
        }
        for reference in delete_plan.foreign_key_references {
            let Some(deleted_value) = committed_deleted_row_value(
                input.hot_state,
                tombstone,
                &reference.foreign_key.referenced_properties,
            )
            .await?
            else {
                continue;
            };
            for source_domain in delete_restriction_source_domains(
                &tombstone.identity,
                reference.source_key.schema_key.as_str(),
            ) {
                normal_batches
                    .entry(NormalDeleteRestrictionBatchKey {
                        source_key: reference.source_key.clone(),
                        source_domain,
                        local_properties: reference.foreign_key.local_properties.clone(),
                    })
                    .or_default()
                    .entry(deleted_value.clone())
                    .or_default()
                    .push(tombstone.identity.clone());
            }
        }
    }
    validate_committed_normal_delete_restriction_batches(
        input.hot_state,
        pending_constraints,
        normal_batches,
    )
    .await?;
    Ok(())
}

/// Delete-side mirror of [`foreign_key_target_domain`].
///
/// The insert side rewrites the *target* domain of a
/// `lix_file_descriptor -> lix_directory_descriptor` reference to
/// `Exact(None)`, because a file-descriptor row carries `file_id = own id`
/// while the directory row it points at carries `file_id = NULL`. The delete
/// side has to invert that rewrite: given the deleted directory, which file
/// scopes can hold a file descriptor that still references it?
///
/// Because `canonicalize_descriptor_file_id` forces
/// `lix_file_descriptor.file_id` to equal the row's own entity id, the answer
/// is "every file scope, and at most one row per scope". `DomainFileScope::Any`
/// is therefore *exact* for this one pair rather than a widening: no two
/// file-descriptor rows can share a file scope, so nothing can be conflated
/// across scopes.
///
/// This stays a targeted rewrite instead of widening
/// `fk_source_domains_for_target` for every schema. For an ordinary
/// file-scoped schema the same referenced key may legitimately exist in two
/// different files, and a source row in file G is satisfied by the target in
/// file G; scanning every file scope would let the copy in G block a delete of
/// the copy in F and turn a legal delete into a foreign-key error. See
/// `widening_delete_source_domain_to_any_file_falsely_rejects`.
fn delete_restriction_source_domains(
    deleted_identity: &DomainRowIdentity,
    source_schema_key: &str,
) -> Vec<Domain> {
    let domain = deleted_identity.domain();
    if deleted_identity.schema_key() == DIRECTORY_DESCRIPTOR_SCHEMA_KEY
        && source_schema_key == FILE_DESCRIPTOR_SCHEMA_KEY
    {
        return Domain::any_file(domain.branch_id().to_string(), domain.untracked())
            .fk_source_domains_for_target();
    }
    domain.fk_source_domains_for_target()
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct NormalDeleteRestrictionBatchKey {
    source_key: SchemaCatalogKey,
    source_domain: Domain,
    local_properties: Vec<Vec<String>>,
}

async fn validate_committed_normal_delete_restriction_batches(
    hot_state: &dyn HotStateReader,
    pending_constraints: &PendingConstraintIndexes,
    batches: BTreeMap<
        NormalDeleteRestrictionBatchKey,
        BTreeMap<UniqueConstraintValue, Vec<DomainRowIdentity>>,
    >,
) -> Result<(), LixError> {
    for (batch, tombstones_by_value) in batches {
        let rows = scan_committed_constraint_rows(
            hot_state,
            &batch.source_domain,
            vec![batch.source_key.schema_key.clone()],
            Vec::new(),
            false,
        )
        .await?;

        for row in rows.iter() {
            if pending_constraints.tombstones_identity(row)
                || pending_constraints.replaces_committed_identity(row)
            {
                continue;
            }
            let Some(snapshot_content) = row.snapshot_content().map(|snapshot| snapshot.as_str())
            else {
                continue;
            };
            let snapshot = parse_committed_snapshot(row, snapshot_content)?;
            let Some(value) =
                UniqueConstraintValue::from_snapshot_non_null(&snapshot, &batch.local_properties)
            else {
                continue;
            };
            let Some(tombstone) = tombstones_by_value
                .get(&value)
                .and_then(|tombstones| tombstones.first())
            else {
                continue;
            };
            return Err(committed_delete_restriction_error(
                tombstone,
                row,
                &batch.local_properties,
            )?);
        }
    }
    Ok(())
}

async fn committed_deleted_row_value(
    hot_state: &dyn HotStateReader,
    tombstone: &PendingTombstone,
    referenced_properties: &[Vec<String>],
) -> Result<Option<UniqueConstraintValue>, LixError> {
    let rows = load_committed_constraint_rows(
        hot_state,
        tombstone.identity.domain(),
        tombstone.identity.schema_key(),
        tombstone.identity.entity_pk_owned(),
        true,
    )
    .await?;
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    let Some(snapshot_content) = row.snapshot_content().map(|snapshot| snapshot.as_str()) else {
        return Ok(None);
    };
    let snapshot = parse_committed_snapshot(row, snapshot_content)?;
    Ok(UniqueConstraintValue::from_snapshot(
        &snapshot,
        referenced_properties,
    ))
}

fn committed_delete_restriction_error(
    deleted_identity: &DomainRowIdentity,
    referencing_row: MaterializedHotStateRowRef<'_>,
    local_properties: &[Vec<String>],
) -> Result<LixError, LixError> {
    Ok(LixError::new(
        LixError::CODE_FOREIGN_KEY,
        format!(
            "cannot delete '{}' row '{}' in branch '{}' because committed row '{}' references it through {}",
            deleted_identity.schema_key(),
            deleted_identity.entity_pk().as_json_array_text()?,
            deleted_identity.domain().branch_id(),
            referencing_row.entity_pk().as_json_array_text()?,
            format_pointer_group(local_properties)
        ),
    ))
}

fn parse_committed_snapshot(
    row: MaterializedHotStateRowRef<'_>,
    snapshot_content: &str,
) -> Result<JsonValue, LixError> {
    serde_json::from_str::<JsonValue>(snapshot_content).map_err(|error| {
        LixError::new(
            LixError::CODE_SCHEMA_VALIDATION,
            format!(
                "committed snapshot_content for schema '{}' is invalid JSON: {error}",
                row.schema_key()
            ),
        )
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct UnresolvedForeignKeyCheck {
    source_identity: DomainRowIdentity,
    source_schema_key: String,
    source_pointer_group: Vec<Vec<String>>,
    target: PendingForeignKeyTargetKey,
}

fn validate_pending_foreign_keys(
    input: &TransactionValidationInput<'_>,
    pending_constraints: &PendingConstraintIndexes,
    staged_snapshots: &[(PreparedValidationRow<'_>, &SchemaPlan, &JsonValue)],
) -> Result<Vec<UnresolvedForeignKeyCheck>, LixError> {
    let mut unresolved = Vec::new();
    for (row, schema_plan, snapshot) in staged_snapshots {
        for foreign_key in &schema_plan.foreign_keys {
            let Some(local_value) = UniqueConstraintValue::from_snapshot_non_null(
                snapshot,
                &foreign_key.local_properties,
            ) else {
                continue;
            };
            if staged_commit_foreign_key_is_satisfied(input, *row, foreign_key, &local_value)? {
                continue;
            }
            if let Some(check) = validate_pending_normal_foreign_key(
                *row,
                foreign_key,
                local_value,
                pending_constraints,
            )? {
                unresolved.push(check);
            }
        }
    }
    Ok(unresolved)
}

fn staged_commit_foreign_key_is_satisfied(
    input: &TransactionValidationInput<'_>,
    row: PreparedValidationRow<'_>,
    foreign_key: &ForeignKeyPlan,
    local_value: &UniqueConstraintValue,
) -> Result<bool, LixError> {
    if row.schema_key() != crate::checkpoint::CHECKPOINT_SCHEMA_KEY
        || foreign_key.local_properties.as_slice() != [vec!["commit_id".to_string()]]
        || foreign_key.referenced_schema.schema_key != COMMIT_SCHEMA_KEY
        || foreign_key.referenced_properties.as_slice() != [vec!["id".to_string()]]
    {
        return Ok(false);
    }
    let [encoded_commit_id] = local_value.0.as_slice() else {
        return Ok(false);
    };
    let JsonValue::String(commit_id) =
        serde_json::from_str(encoded_commit_id).map_err(|error| {
            LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!("checkpoint commit_id is not a JSON string: {error}"),
            )
        })?
    else {
        return Ok(false);
    };
    let commit_id = CommitId::parse_lix(&commit_id, "checkpoint commit_id")?;
    Ok(input.staged_commit_ids.contains(&commit_id))
}

fn validate_pending_normal_foreign_key(
    row: PreparedValidationRow<'_>,
    foreign_key: &ForeignKeyPlan,
    local_value: UniqueConstraintValue,
    pending_constraints: &PendingConstraintIndexes,
) -> Result<Option<UnresolvedForeignKeyCheck>, LixError> {
    let key = PendingForeignKeyTargetKey {
        schema_key: foreign_key.referenced_schema.schema_key.clone(),
        domain: foreign_key_target_domain(row, foreign_key),
        pointer_group: foreign_key.referenced_properties.clone(),
        value: local_value,
    };
    if pending_constraints.has_reachable_fk_target_key(&key) {
        return Ok(None);
    }
    Ok(Some(UnresolvedForeignKeyCheck {
        source_identity: row.domain_row_identity(),
        source_schema_key: row.schema_key().to_string(),
        source_pointer_group: foreign_key.local_properties.clone(),
        target: key,
    }))
}

fn foreign_key_target_domain(
    row: PreparedValidationRow<'_>,
    foreign_key: &ForeignKeyPlan,
) -> Domain {
    if row.schema_key() == FILE_DESCRIPTOR_SCHEMA_KEY
        && foreign_key.referenced_schema.schema_key == DIRECTORY_DESCRIPTOR_SCHEMA_KEY
    {
        row.domain().with_exact_file_scope(None)
    } else {
        row.domain()
    }
}

async fn validate_committed_foreign_keys(
    input: &TransactionValidationInput<'_>,
    pending_constraints: &PendingConstraintIndexes,
    unresolved_checks: &[UnresolvedForeignKeyCheck],
) -> Result<Vec<UnresolvedForeignKeyCheck>, LixError> {
    let mut still_unresolved = Vec::new();
    for check in unresolved_checks {
        let resolved = committed_normal_foreign_key_target_exists(
            input.hot_state,
            input.schema_catalog,
            pending_constraints,
            &check.target,
        )
        .await?;
        if !resolved {
            still_unresolved.push(check.clone());
        }
    }
    Ok(still_unresolved)
}

fn reject_unresolved_foreign_keys(
    unresolved_checks: &[UnresolvedForeignKeyCheck],
) -> Result<(), LixError> {
    let Some(check) = unresolved_checks.first() else {
        return Ok(());
    };
    Err(LixError::new(
        LixError::CODE_FOREIGN_KEY,
        format!(
            "foreign key on schema '{}' row '{}' via {} has no matching target in branch '{}'{}",
            check.source_schema_key,
            check.source_identity.entity_pk().as_json_array_text()?,
            format_pointer_group(&check.source_pointer_group),
            check.source_identity.domain().branch_id(),
            unresolved_foreign_key_target_description(&check.target)?
        ),
    ))
}

fn unresolved_foreign_key_target_description(
    target: &PendingForeignKeyTargetKey,
) -> Result<String, LixError> {
    Ok(format!(
        " for target '{}.{}' value {}",
        target.schema_key,
        format_pointer_group(&target.pointer_group),
        target.value.display()
    ))
}

async fn committed_normal_foreign_key_target_exists(
    hot_state: &dyn HotStateReader,
    schema_catalog: &CatalogSnapshot,
    pending_constraints: &PendingConstraintIndexes,
    target: &PendingForeignKeyTargetKey,
) -> Result<bool, LixError> {
    let entity_pks: Vec<EntityPk> = primary_key_entity_pk_for_target(schema_catalog, target)
        .into_iter()
        .collect();
    for domain in target.domain.fk_target_domains() {
        let rows = scan_committed_constraint_rows(
            hot_state,
            &domain,
            vec![target.schema_key.clone()],
            entity_pks.clone(),
            false,
        )
        .await?;

        for row in rows.iter() {
            if pending_constraints.tombstones_identity(row) {
                continue;
            }
            if row.schema_key() != target.schema_key {
                continue;
            }
            let Some(snapshot_content) = row.snapshot_content().map(|snapshot| snapshot.as_str())
            else {
                continue;
            };
            let snapshot =
                serde_json::from_str::<JsonValue>(snapshot_content).map_err(|error| {
                    LixError::new(
                        LixError::CODE_SCHEMA_VALIDATION,
                        format!(
                            "committed snapshot_content for schema '{}' is invalid JSON: {error}",
                            row.schema_key()
                        ),
                    )
                })?;
            if UniqueConstraintValue::from_snapshot(&snapshot, &target.pointer_group).as_ref()
                == Some(&target.value)
            {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn primary_key_entity_pk_for_target(
    schema_catalog: &CatalogSnapshot,
    target: &PendingForeignKeyTargetKey,
) -> Option<EntityPk> {
    let (_, target_plan) = schema_catalog.plan_for_key(&target.schema_key)?;
    if target_plan.primary_key.as_ref()? != &target.pointer_group {
        return None;
    }
    let values = target
        .value
        .0
        .iter()
        .map(|value| serde_json::from_str::<JsonValue>(value).ok())
        .collect::<Option<Vec<_>>>()?;
    EntityPk::from_json_values(&values, target_plan.primary_key_component_types.as_deref()?).ok()
}

async fn validate_committed_unique_constraints(
    input: &TransactionValidationInput<'_>,
    pending_constraints: &PendingConstraintIndexes,
) -> Result<(), LixError> {
    let mut pending_by_scope = BTreeMap::<
        PendingUniqueConstraintScope,
        BTreeMap<UniqueConstraintValue, Vec<&EntityPk>>,
    >::new();
    let can_skip_unchanged = pending_constraints.unique_values.len() == 1;
    for (key, pending_entity_pk) in &pending_constraints.unique_values {
        // The fixed descriptor schemas only declare parent/name uniqueness.
        // Filesystem namespace validation below is strictly stronger: it
        // rejects same-kind duplicates and file/directory cross-kind clashes.
        // Avoid scanning the descriptor tree once here and again there.
        if filesystem_namespace_owns_unique_constraint(key) {
            continue;
        }
        let is_insert =
            can_skip_unchanged && pending_unique_owner_is_insert(input, key, pending_entity_pk);
        if can_skip_unchanged
            && !is_insert
            && committed_unique_value_is_unchanged(input.hot_state, key, pending_entity_pk).await?
        {
            continue;
        }
        pending_by_scope
            .entry(PendingUniqueConstraintScope::from(key))
            .or_default()
            .entry(key.value.clone())
            .or_default()
            .push(pending_entity_pk);
    }

    for (scope, pending_values) in pending_by_scope {
        // A scope with exactly one pending value becomes a point probe instead
        // of a whole-collection scan with a parse per row. Everything else —
        // composite groups, columns the plane cannot key, and scopes holding
        // several pending values — keeps the single scan it always had.
        match declared_column_probe(input.schema_catalog, &scope, &pending_values) {
            Some(probe) => {
                let committed_rows = scan_committed_constraint_rows_by_declared_column(
                    input.hot_state,
                    &scope.domain,
                    &scope.schema_key,
                    probe,
                )
                .await?;
                reject_committed_unique_conflicts(
                    &committed_rows,
                    &scope,
                    &pending_values,
                    pending_constraints,
                )?;
            }
            None => {
                let committed_rows = scan_committed_constraint_rows(
                    input.hot_state,
                    &scope.domain,
                    vec![scope.schema_key.clone()],
                    Vec::new(),
                    false,
                )
                .await?;
                reject_committed_unique_conflicts(
                    &committed_rows,
                    &scope,
                    &pending_values,
                    pending_constraints,
                )?;
            }
        }
    }
    Ok(())
}

/// The one `declared_column_eq` predicate that can replace this scope's scan,
/// or `None` when the scope must keep scanning.
///
/// **Deliberately one value only.** Probing per distinct value would turn one
/// scan-per-scope into N probes, and a collection with no completeness witness
/// silently falls back to its ordinary scan — so each of those N probes would
/// become a *full scan*. That is exactly the population this change must not
/// hurt: repositories whose collections predate the index plane. Capping at
/// one value makes the probe route weakly better than the scan route in every
/// case, which is what lets it be chosen here without first asking whether the
/// index can serve. Lifting the cap needs that question answered first.
///
/// Declines unless the scope is a single-column group the schema's
/// `indexed_columns` cover **and** the value round-trips exactly through the
/// index's key encoding. A probe built from a value that does not round-trip
/// could miss a committed conflict — a unique violation silently accepted, not
/// a slow query — so an inexact value goes back to the scan.
fn declared_column_probe(
    schema_catalog: &CatalogSnapshot,
    scope: &PendingUniqueConstraintScope,
    pending_values: &BTreeMap<UniqueConstraintValue, Vec<&EntityPk>>,
) -> Option<crate::hot_state::DeclaredColumnEq> {
    let [pointer] = scope.pointer_group.as_slice() else {
        return None;
    };
    let [property] = pointer.as_slice() else {
        return None;
    };
    let [value] = pending_values.keys().collect::<Vec<_>>()[..] else {
        return None;
    };
    let schema = schema_catalog.schema(&scope.schema_key)?;
    let spec = crate::sql2::derive_entity_surface_spec_from_schema(schema).ok()?;
    let ordinal = spec
        .indexed_columns
        .iter()
        .find(|column| column.name == *property)?
        .ordinal;
    Some(crate::hot_state::DeclaredColumnEq {
        schema_key: scope.schema_key.clone(),
        ordinal,
        values: vec![value.exact_hot_index_value()?],
    })
}

/// The committed-row half of the unique check, shared by the probe and scan
/// routes so both reject exactly the same conflicts.
fn reject_committed_unique_conflicts(
    committed_rows: &CommittedHotStateRows,
    scope: &PendingUniqueConstraintScope,
    pending_values: &BTreeMap<UniqueConstraintValue, Vec<&EntityPk>>,
    pending_constraints: &PendingConstraintIndexes,
) -> Result<(), LixError> {
    for committed_row in committed_rows.iter() {
        if !committed_row_is_in_exact_unique_scope(committed_row, scope) {
            continue;
        }
        if pending_constraints.tombstones_identity(committed_row) {
            continue;
        }
        let Some(snapshot_content) = committed_row
            .snapshot_content()
            .map(|snapshot| snapshot.as_str())
        else {
            continue;
        };
        let snapshot = serde_json::from_str::<JsonValue>(snapshot_content).map_err(|error| {
            LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!(
                    "committed snapshot_content for schema '{}' is invalid JSON: {error}",
                    committed_row.schema_key()
                ),
            )
        })?;
        let Some(committed_value) =
            UniqueConstraintValue::from_snapshot(&snapshot, &scope.pointer_group)
        else {
            continue;
        };
        // Index entries are candidates, never answers: a probe can return a
        // row whose value has since changed. This lookup is what rejects it.
        let Some(pending_entity_pks) = pending_values.get(&committed_value) else {
            continue;
        };
        for pending_entity_pk in pending_entity_pks {
            if committed_row.entity_pk() == *pending_entity_pk {
                continue;
            }
            return Err(LixError::new(
                LixError::CODE_UNIQUE,
                format!(
                    "unique constraint violation on {}.{} for value {}: committed row '{}' conflicts with staged row '{}'",
                    scope.schema_key,
                    format_pointer_group(&scope.pointer_group),
                    committed_value.display(),
                    committed_row.entity_pk().as_json_array_text()?,
                    pending_entity_pk.as_json_array_text()?
                ),
            ));
        }
    }
    Ok(())
}

fn filesystem_namespace_owns_unique_constraint(key: &PendingUniqueKey) -> bool {
    matches!(
        key.schema_key.as_str(),
        DIRECTORY_DESCRIPTOR_SCHEMA_KEY | FILE_DESCRIPTOR_SCHEMA_KEY
    )
}

fn pending_unique_owner_is_insert(
    input: &TransactionValidationInput<'_>,
    key: &PendingUniqueKey,
    entity_pk: &EntityPk,
) -> bool {
    // Conflict-aware inserts use Replace mode, but retain their logical Insert
    // origin when the row is new. Both forms must keep the original single scan.
    input.staged_writes.inserts().any(|insert| {
        insert.row.schema_key.as_str() == key.schema_key.as_str()
            && insert.row.entity_pk == entity_pk
            && Domain::exact_file(
                insert.row.branch_id.to_string(),
                insert.row.untracked,
                insert.row.file_id.map(ToString::to_string),
            ) == key.domain
    }) || input.staged_writes.rows().any(|row| {
        row.schema_key() == key.schema_key
            && row.entity_pk() == entity_pk
            && row.domain() == key.domain
            && row
                .origin()
                .is_some_and(|origin| origin.operation == TransactionWriteOperation::Insert)
    })
}

async fn committed_unique_value_is_unchanged(
    hot_state: &dyn HotStateReader,
    key: &PendingUniqueKey,
    entity_pk: &EntityPk,
) -> Result<bool, LixError> {
    let committed_rows = load_committed_constraint_rows(
        hot_state,
        &key.domain,
        &key.schema_key,
        entity_pk.clone(),
        false,
    )
    .await?;
    let Some(committed) = committed_rows.first() else {
        return Ok(false);
    };
    let Some(snapshot_content) = committed
        .snapshot_content()
        .map(|snapshot| snapshot.as_str())
    else {
        return Ok(false);
    };
    let snapshot = parse_committed_snapshot(committed, snapshot_content)?;
    Ok(
        UniqueConstraintValue::from_snapshot(&snapshot, &key.pointer_group).as_ref()
            == Some(&key.value),
    )
}

fn committed_row_is_in_exact_unique_scope(
    row: MaterializedHotStateRowRef<'_>,
    scope: &PendingUniqueConstraintScope,
) -> bool {
    // HotStateReader may return serving projections such as global rows
    // projected into a requested branch. Constraint validation is root-local:
    // only rows authored in the exact branch participate.
    scope.domain.contains_ref(row) && row.schema_key() == scope.schema_key
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct UniqueConstraintValue(Vec<String>);

impl UniqueConstraintValue {
    #[cfg(test)]
    fn string_values<const N: usize>(values: [&str; N]) -> Self {
        Self(
            values
                .into_iter()
                .map(|value| format!("{value:?}"))
                .collect(),
        )
    }

    fn from_entity_pk(identity: &EntityPk) -> Self {
        Self(
            identity
                .components
                .iter()
                .map(|component| format!("{:?}", component.external_json()))
                .collect(),
        )
    }

    fn from_snapshot(snapshot: &JsonValue, pointers: &[Vec<String>]) -> Option<Self> {
        let mut values = Vec::with_capacity(pointers.len());
        for pointer in pointers {
            let value = json_pointer_get(snapshot, pointer)?;
            values.push(stable_unique_value(value));
        }
        Some(Self(values))
    }

    fn from_snapshot_non_null(snapshot: &JsonValue, pointers: &[Vec<String>]) -> Option<Self> {
        let mut values = Vec::with_capacity(pointers.len());
        for pointer in pointers {
            let value = json_pointer_get(snapshot, pointer)?;
            if value.is_null() {
                return None;
            }
            values.push(stable_unique_value(value));
        }
        Some(Self(values))
    }

    /// The hot index plane's encoding of a single-column value, but only when
    /// the recovered value re-encodes to exactly this stable form.
    ///
    /// `stable_unique_value` is lossy for anything the index cannot key
    /// anyway, and an approximate probe would be a missed conflict rather
    /// than a slow one. The round-trip check makes "recoverable" provable
    /// instead of assumed.
    fn exact_hot_index_value(&self) -> Option<crate::hot_state::HotIndexValue> {
        let [encoded] = self.0.as_slice() else {
            return None;
        };
        if let Ok(text) = serde_json::from_str::<String>(encoded)
            && stable_unique_value(&JsonValue::String(text.clone())) == *encoded
        {
            return Some(crate::hot_state::HotIndexValue::String(text));
        }
        let number = encoded.parse::<i64>().ok()?;
        (stable_unique_value(&JsonValue::Number(number.into())) == *encoded)
            .then_some(crate::hot_state::HotIndexValue::Integer(number))
    }

    fn display(&self) -> String {
        if let [value] = self.0.as_slice() {
            return value.clone();
        }
        format!("({})", self.0.join(", "))
    }
}

fn stable_unique_value(value: &JsonValue) -> String {
    match value {
        JsonValue::String(value) => format!("{value:?}"),
        JsonValue::Number(value) => value.to_string(),
        JsonValue::Bool(value) => value.to_string(),
        JsonValue::Null => "null".to_string(),
        JsonValue::Array(_) | JsonValue::Object(_) => {
            canonical_json_text(value).unwrap_or_else(|_| value.to_string())
        }
    }
}

fn format_pointer_group(group: &[Vec<String>]) -> String {
    let pointers = group
        .iter()
        .map(|pointer| format_json_pointer(pointer))
        .collect::<Vec<_>>();
    if let [pointer] = pointers.as_slice() {
        pointer.clone()
    } else {
        format!("({})", pointers.join(", "))
    }
}

fn primary_key_identity_error(
    row: PreparedValidationRow<'_>,
    primary_key_paths: &[Vec<String>],
    error: EntityPkError,
) -> LixError {
    let reason = match error {
        EntityPkError::EmptyPrimaryKey => "empty x-lix-primary-key".to_string(),
        EntityPkError::EmptyPrimaryKeyPath { index } => {
            format!("empty x-lix-primary-key pointer at index {index}")
        }
        EntityPkError::MissingPrimaryKeyValue { index } => {
            let pointer = format_json_pointer(&primary_key_paths[index]);
            format!("missing value at primary-key pointer '{pointer}'")
        }
        EntityPkError::UnsupportedPrimaryKeyValue { index } => {
            let pointer = format_json_pointer(&primary_key_paths[index]);
            format!("unsupported value at primary-key pointer '{pointer}'")
        }
        EntityPkError::InvalidPrimaryKeyValue { index, expected } => {
            let pointer = format_json_pointer(&primary_key_paths[index]);
            format!("value at primary-key pointer '{pointer}' must be a valid {expected}")
        }
        EntityPkError::InvalidEncodedEntityPk => "invalid encoded entity primary key".to_string(),
    };
    LixError::new(
        LixError::CODE_UNIQUE,
        format!(
            "primary-key constraint violation on schema '{}': {reason}",
            row.schema_key()
        ),
    )
}

fn validate_foreign_key_definition(
    catalog: &CatalogSnapshot,
    source_key: &SchemaCatalogKey,
    source_schema: &JsonValue,
    foreign_key: &ForeignKeyPlan,
) -> Result<(), LixError> {
    for pointer in &foreign_key.local_properties {
        validate_schema_field_pointer(source_schema, pointer).map_err(|detail| {
            LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                format!(
                    "foreign key on schema '{}' references missing local property '{}': {detail}",
                    source_key.schema_key,
                    format_json_pointer(pointer)
                ),
            )
        })?;
    }

    let target_plan = catalog
        .plan(foreign_key.referenced_plan_id)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                format!(
                    "foreign key on schema '{}' references missing bound schema plan '{}'",
                    source_key.schema_key, foreign_key.referenced_schema.schema_key,
                ),
            )
        })?;
    let target_schema = target_plan.schema.as_ref();
    if target_plan.key != foreign_key.referenced_schema {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            format!(
                "foreign key on schema '{}' is bound to schema '{}' but declares schema '{}'",
                source_key.schema_key,
                target_plan.key.schema_key,
                foreign_key.referenced_schema.schema_key,
            ),
        ));
    }

    for pointer in &foreign_key.referenced_properties {
        validate_schema_field_pointer(target_schema, pointer).map_err(|detail| {
            LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                format!(
                    "foreign key on schema '{}' references missing target property '{}.{}': {detail}",
                    source_key.schema_key,
                    foreign_key.referenced_schema.schema_key,
                    format_json_pointer(pointer)
                ),
            )
        })?;
    }

    if !referenced_properties_are_keyed(target_plan, &foreign_key.referenced_properties) {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            format!(
                "foreign key on schema '{}' references '{}.{}', but referenced properties must match the target primary key or a unique constraint",
                source_key.schema_key,
                foreign_key.referenced_schema.schema_key,
                format_pointer_group(&foreign_key.referenced_properties)
            ),
        ));
    }

    Ok(())
}

fn validate_schema_field_pointer(schema: &JsonValue, pointer: &[String]) -> Result<(), String> {
    if pointer.is_empty() {
        return Err("empty pointer does not name a field".to_string());
    }
    let mut current = schema;
    for segment in pointer {
        let properties = current
            .get("properties")
            .and_then(JsonValue::as_object)
            .ok_or_else(|| format!("schema segment before '{segment}' has no object properties"))?;
        current = properties
            .get(segment)
            .ok_or_else(|| format!("property '{segment}' does not exist"))?;
    }
    Ok(())
}

fn referenced_properties_are_keyed(
    target_plan: &SchemaPlan,
    referenced_properties: &[Vec<String>],
) -> bool {
    if let Some(primary_key) = target_plan.primary_key.as_ref() {
        if primary_key == referenced_properties {
            return true;
        }
    }
    target_plan
        .uniques
        .iter()
        .any(|unique_group| unique_group == referenced_properties)
}

fn validate_foreign_key_definitions(catalog: &CatalogSnapshot) -> Result<(), LixError> {
    for plan in catalog.plans() {
        for foreign_key in &plan.foreign_keys {
            validate_foreign_key_definition(catalog, &plan.key, plan.schema.as_ref(), foreign_key)?;
        }
    }
    Ok(())
}

#[cfg(test)]
fn validate_pending_registered_schema(
    row: PreparedValidationRow<'_>,
    registered_schema_definition: &JsonValue,
) -> Result<(SchemaKey, JsonValue), LixError> {
    let snapshot_content = row.snapshot_content().ok_or_else(|| {
        LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            "registered schema write requires snapshot_content",
        )
    })?;
    let snapshot = serde_json::from_str::<JsonValue>(snapshot_content).map_err(|error| {
        LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            format!("pending registered schema snapshot_content is invalid JSON: {error}"),
        )
    })?;
    if !snapshot.get("value").is_some_and(JsonValue::is_object) {
        validate_lix_schema(registered_schema_definition, &snapshot)?;
    }
    // A registered-schema row stores the schema definition under `value`.
    // Validate both layers: the outer row must match the builtin
    // `lix_registered_schema` schema, and the inner definition must be a valid
    // Lix schema before it can extend the transaction-visible catalog.
    let (key, schema) = schema_from_registered_snapshot(&snapshot)?;
    reject_reserved_schema_namespace(&key)?;
    validate_lix_schema_definition(&schema)?;
    validate_lix_schema(registered_schema_definition, &snapshot)?;
    Ok((key, schema))
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use async_trait::async_trait;
    use serde_json::json;

    use super::*;
    use crate::common::SharedStr;
    use crate::hot_state::{
        HotStateScanRequest, MaterializedHotStateBatchBuilder, MaterializedHotStateRow,
    };
    use crate::schema::{schema_key_from_definition, seed_schema_definition};
    use crate::transaction_types::{
        LogicalPrimaryKey, StageJson, TestPreparedStateRow, TransactionJson, shared_origin_surface,
    };

    macro_rules! prepared_rows {
        ($($row:expr),* $(,)?) => {
            PreparedStateBatch::from_test_rows(vec![$($row),*])
        };
    }

    struct EmptyHotStateReader;

    struct BatchOnlyConstraintHotStateReader {
        rows: Mutex<Option<MaterializedHotStateBatch>>,
    }

    #[async_trait]
    impl HotStateReader for BatchOnlyConstraintHotStateReader {
        async fn scan_constraint_batch(
            &self,
            _request: &HotStateScanRequest,
            _tracked_only: bool,
        ) -> Result<MaterializedHotStateBatch, LixError> {
            Ok(self
                .rows
                .lock()
                .expect("constraint batch lock should not be poisoned")
                .take()
                .expect("constraint batch should be consumed once"))
        }

        async fn scan_batch(
            &self,
            _request: &HotStateScanRequest,
        ) -> Result<MaterializedHotStateBatch, LixError> {
            panic!("constraint validation must not project the shared batch into owned rows")
        }

        async fn load_exact_batch(
            &self,
            request: &HotStateExactBatchRequest,
        ) -> Result<crate::hot_state::MaterializedHotStateExactBatch, LixError> {
            crate::hot_state::MaterializedHotStateExactBatch::new(
                MaterializedHotStateBatch::default(),
                vec![None; request.rows.len()],
            )
        }
    }

    fn ts(value: &str) -> crate::common::LixTimestamp {
        crate::common::LixTimestamp::expect_parse("timestamp", value)
    }

    #[tokio::test]
    async fn ten_thousand_constraint_rows_retain_one_dense_batch_and_shared_payload() {
        const ROW_COUNT: usize = 10_000;
        let branch_id = "01920000-0000-7000-8000-0000000000a1";
        let entity_pk = EntityPk::single("shared-entity");
        let snapshot = SharedStr::from(r#"{"value":"shared"}"#);
        let metadata = SharedStr::from(r#"{"source":"constraint-batch"}"#);
        let timestamp = ts("2026-01-01T00:00:00Z");
        let mut builder = MaterializedHotStateBatchBuilder::with_capacity(ROW_COUNT);
        for _ in 0..ROW_COUNT {
            builder.push_materialized_ref(
                &entity_pk,
                "constraint_schema",
                None,
                Some(snapshot.clone()),
                Some(metadata.clone()),
                false,
                timestamp,
                timestamp,
                false,
                None,
                None,
                false,
                branch_id,
            );
        }
        let batch = builder.finish();
        let original_entity_column = batch.entity_column_ptr();
        let reader = BatchOnlyConstraintHotStateReader {
            rows: Mutex::new(Some(batch)),
        };

        let rows = scan_committed_constraint_rows(
            &reader,
            &Domain::any_file(branch_id, false),
            vec!["constraint_schema".to_string()],
            Vec::new(),
            false,
        )
        .await
        .expect("constraint batch scan should succeed");

        assert_eq!(rows.len(), ROW_COUNT);
        assert!(
            rows.is_dense(),
            "a conforming constraint scan must retain the owner without an ordinal allocation"
        );
        assert_eq!(rows.batch.entity_column_ptr(), original_entity_column);
        assert_eq!(rows.batch.dictionary_entry_count(), 2);
        let first = rows
            .first()
            .and_then(MaterializedHotStateRowRef::snapshot_content)
            .expect("first row should retain its snapshot");
        let last = rows
            .batch
            .row(ROW_COUNT - 1)
            .snapshot_content()
            .expect("last row should retain its snapshot");
        assert!(first.shares_buffer_with(last));
    }

    #[test]
    fn defensive_constraint_selection_keeps_original_batch_order() {
        let timestamp = ts("2026-01-01T00:00:00Z");
        let mut builder = MaterializedHotStateBatchBuilder::with_capacity(4);
        for (entity_id, schema_key) in [
            ("first", "selected"),
            ("rejected", "other"),
            ("second", "selected"),
            ("third", "selected"),
        ] {
            builder.push_materialized_ref(
                &EntityPk::single(entity_id),
                schema_key,
                None,
                None,
                None,
                false,
                timestamp,
                timestamp,
                false,
                None,
                None,
                false,
                "branch",
            );
        }
        let batch = builder.finish();
        let original_entity_column = batch.entity_column_ptr();
        let rows = CommittedHotStateRows::select(batch, |row| row.schema_key() == "selected")
            .expect("four rows fit the selection ordinal column");

        assert!(!rows.is_dense());
        assert_eq!(rows.batch.entity_column_ptr(), original_entity_column);
        assert_eq!(
            rows.iter()
                .map(|row| {
                    row.entity_pk()
                        .as_single_string_owned()
                        .expect("test key is one string")
                })
                .collect::<Vec<_>>(),
            vec![
                "first".to_string(),
                "second".to_string(),
                "third".to_string(),
            ]
        );
    }

    fn test_stage_json(value: &str) -> StageJson {
        let parsed = test_json_text(value).expect("test staged JSON should parse");
        crate::transaction_types::stage_json_from_value(
            TransactionJson::from_value_for_test(parsed),
            "test staged JSON",
        )
        .expect("test staged JSON should prepare")
    }

    fn test_json_text(value: &str) -> Result<serde_json::Value, LixError> {
        serde_json::from_str::<serde_json::Value>(value).map_err(|error| {
            LixError::new(
                LixError::CODE_UNKNOWN,
                format!("test staged JSON is invalid JSON: {error}"),
            )
        })
    }

    fn test_plan_from_schema(schema: JsonValue) -> &'static SchemaPlan {
        let key = schema_key_from_definition(&schema).expect("test schema should have key");
        let visible_schemas = match key.schema_key.as_str() {
            "fk_child_schema" => vec![fk_parent_schema(), schema],
            FILE_DESCRIPTOR_SCHEMA_KEY => vec![directory_descriptor_schema(), schema],
            DIRECTORY_DESCRIPTOR_SCHEMA_KEY => vec![schema],
            _ => vec![schema],
        };
        let catalog = Box::leak(Box::new(
            CatalogSnapshot::from_visible_schemas(&visible_schemas)
                .expect("test schema plan catalog should build"),
        ));
        catalog
            .plan_for_key(&key.schema_key)
            .expect("test schema key should resolve")
            .1
    }

    #[async_trait]
    impl HotStateReader for EmptyHotStateReader {
        async fn load_exact_batch(
            &self,
            request: &HotStateExactBatchRequest,
        ) -> Result<crate::hot_state::MaterializedHotStateExactBatch, LixError> {
            crate::hot_state::load_exact_batch_via_scan_for_test(self, request).await
        }

        async fn scan_batch(
            &self,
            request: &HotStateScanRequest,
        ) -> Result<MaterializedHotStateBatch, LixError> {
            Ok(test_file_descriptor_rows()
                .into_iter()
                .filter(|row| hot_state_row_matches_scan(row, request))
                .collect::<Vec<_>>()
                .into())
        }
    }

    fn validation_input<'a>(
        staged_writes: &'a PreparedWriteSet,
        visible_schemas: &'a [JsonValue],
    ) -> TransactionValidationInput<'a> {
        let catalog = Box::leak(Box::new(
            catalog_from_transaction_parts_unchecked(staged_writes, visible_schemas)
                .expect("test schema catalog should build"),
        ));
        let validation_set = Box::leak(Box::new(staged_writes.validation_set_for_tests()));
        TransactionValidationInput::new(validation_set, catalog, &EmptyHotStateReader)
    }

    async fn filesystem_namespace_domain_changed_for_test(
        staged_rows: Vec<TestPreparedStateRow>,
        committed_rows: Vec<MaterializedHotStateRow>,
    ) -> Result<bool, LixError> {
        let staged_writes = PreparedWriteSet {
            state_rows: PreparedStateBatch::from_test_rows(staged_rows),
            ..empty_staged_write_set()
        };
        let validation_set = staged_writes.validation_set_for_tests();
        let staged_rows = validation_set.rows().collect::<Vec<_>>();
        let domain = staged_rows
            .first()
            .expect("namespace test requires a staged descriptor")
            .domain();
        let catalog = CatalogSnapshot::from_visible_schemas(&[
            file_descriptor_schema(),
            directory_descriptor_schema(),
        ])?;
        let hot_state = StrictStaticHotStateReader {
            rows: committed_rows,
        };
        let input = TransactionValidationInput::new(&validation_set, &catalog, &hot_state);
        filesystem_namespace_domain_changed(&input, &staged_rows, &domain).await
    }

    async fn assert_filesystem_namespace_domain_changed(
        staged_rows: Vec<TestPreparedStateRow>,
        committed_rows: Vec<MaterializedHotStateRow>,
        expected: bool,
        scenario: &str,
    ) {
        let actual = filesystem_namespace_domain_changed_for_test(staged_rows, committed_rows)
            .await
            .unwrap_or_else(|error| panic!("{scenario}: {error}"));
        assert_eq!(actual, expected, "{scenario}");
    }

    async fn filesystem_namespace_validation_scan_count_for_test(
        staged_writes: PreparedWriteSet,
        trust_filesystem_planner: bool,
    ) -> Result<usize, LixError> {
        let validation_set = staged_writes.validation_set_for_tests();
        let staged_rows = validation_set.rows().collect::<Vec<_>>();
        let catalog = CatalogSnapshot::from_visible_schemas(&[
            file_descriptor_schema(),
            directory_descriptor_schema(),
        ])?;
        let hot_state = CountingStaticHotStateReader {
            rows: Vec::new(),
            scan_count: AtomicUsize::new(0),
        };
        let mut input = TransactionValidationInput::new(&validation_set, &catalog, &hot_state);
        if trust_filesystem_planner {
            input = input.with_trusted_filesystem_planner();
        }
        validate_filesystem_namespace(&input, &staged_rows).await?;
        Ok(hot_state.scan_count.load(Ordering::Relaxed))
    }

    fn filesystem_insert_origin(surface: &str, entity_id: &str) -> TransactionWriteOrigin {
        TransactionWriteOrigin {
            surface: shared_origin_surface(surface),
            operation: TransactionWriteOperation::Insert,
            primary_key: Some(Arc::new(LogicalPrimaryKey::single_id(entity_id))),
        }
    }

    fn catalog_from_transaction_input<'a>(
        input: &'a TransactionValidationInput<'a>,
    ) -> Result<&'a CatalogSnapshot, LixError> {
        validate_foreign_key_definitions(input.schema_catalog)?;
        Ok(input.schema_catalog)
    }

    fn catalog_from_transaction_parts(
        staged_writes: &PreparedWriteSet,
        visible_schemas: &[JsonValue],
    ) -> Result<CatalogSnapshot, LixError> {
        let catalog = catalog_from_transaction_parts_unchecked(staged_writes, visible_schemas)?;
        let mut pending_keys = BTreeMap::<SchemaCatalogKey, EntityPk>::new();
        for row in staged_writes
            .validation_rows()
            .filter(|row| row.schema_key() == REGISTERED_SCHEMA_KEY)
        {
            let snapshot_content = row.snapshot_content().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_SCHEMA_DEFINITION,
                    "registered schema write requires snapshot_content",
                )
            })?;
            let snapshot =
                serde_json::from_str::<JsonValue>(snapshot_content).map_err(|error| {
                    LixError::new(
                        LixError::CODE_SCHEMA_DEFINITION,
                        format!(
                            "pending registered schema snapshot_content is invalid JSON: {error}"
                        ),
                    )
                })?;
            let (key, _) = schema_from_registered_snapshot(&snapshot)?;
            let catalog_key = SchemaCatalogKey::from_schema_key(key);
            if let Some(existing_entity_pk) =
                pending_keys.insert(catalog_key.clone(), row.entity_pk().clone())
            {
                return Err(LixError::new(
                    LixError::CODE_SCHEMA_DEFINITION,
                    format!(
                        "duplicate pending registered schema '{}' in transaction: rows '{}' and '{}'",
                        catalog_key.schema_key,
                        existing_entity_pk.as_json_array_text()?,
                        row.entity_pk().as_json_array_text()?
                    ),
                ));
            }
        }
        validate_foreign_key_definitions(&catalog)?;
        Ok(catalog)
    }

    fn catalog_from_transaction_parts_unchecked(
        staged_writes: &PreparedWriteSet,
        visible_schemas: &[JsonValue],
    ) -> Result<CatalogSnapshot, LixError> {
        let mut catalog = CatalogSnapshot::from_visible_schemas(visible_schemas)?;
        for row in staged_writes
            .validation_rows()
            .filter(|row| row.schema_key() == REGISTERED_SCHEMA_KEY)
        {
            let registered_schema_definition = catalog
                .schema(REGISTERED_SCHEMA_KEY)
                .cloned()
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_SCHEMA_DEFINITION,
                        "lix_registered_schema schema is not visible to this transaction",
                    )
                })?;
            let (key, schema) =
                validate_pending_registered_schema(row, &registered_schema_definition)?;
            catalog.insert_schema_for_domain(row.domain(), key, schema)?;
        }
        Ok(catalog)
    }

    struct StaticHotStateReader {
        rows: Vec<MaterializedHotStateRow>,
    }

    #[async_trait]
    impl HotStateReader for StaticHotStateReader {
        async fn load_exact_batch(
            &self,
            request: &HotStateExactBatchRequest,
        ) -> Result<crate::hot_state::MaterializedHotStateExactBatch, LixError> {
            crate::hot_state::load_exact_batch_via_scan_for_test(self, request).await
        }

        async fn scan_batch(
            &self,
            request: &HotStateScanRequest,
        ) -> Result<MaterializedHotStateBatch, LixError> {
            Ok(self
                .rows
                .iter()
                .cloned()
                .chain(test_file_descriptor_rows())
                .filter(|row| hot_state_row_matches_scan(row, request))
                .collect::<Vec<_>>()
                .into())
        }
    }

    struct OverlayingStaticHotStateReader {
        rows: Vec<MaterializedHotStateRow>,
    }

    #[async_trait]
    impl HotStateReader for OverlayingStaticHotStateReader {
        async fn load_exact_batch(
            &self,
            request: &HotStateExactBatchRequest,
        ) -> Result<crate::hot_state::MaterializedHotStateExactBatch, LixError> {
            crate::hot_state::load_exact_batch_via_scan_for_test(self, request).await
        }

        async fn scan_batch(
            &self,
            request: &HotStateScanRequest,
        ) -> Result<MaterializedHotStateBatch, LixError> {
            let rows = self
                .rows
                .iter()
                .cloned()
                .chain(test_file_descriptor_rows())
                .filter(|row| hot_state_row_matches_scan(row, request))
                .collect::<Vec<_>>();
            if request.filter.untracked.is_some() {
                return Ok(rows.into());
            }
            let tracked_rows = rows
                .iter()
                .filter(|row| !row.untracked)
                .cloned()
                .collect::<Vec<_>>();
            let untracked_rows = rows
                .into_iter()
                .filter(|row| row.untracked)
                .collect::<Vec<_>>();
            Ok(overlay_untracked_rows_for_test(tracked_rows, untracked_rows).into())
        }
    }

    fn overlay_untracked_rows_for_test(
        tracked_rows: Vec<MaterializedHotStateRow>,
        untracked_rows: Vec<MaterializedHotStateRow>,
    ) -> Vec<MaterializedHotStateRow> {
        let mut rows_by_identity = BTreeMap::new();
        for row in tracked_rows {
            rows_by_identity.insert(DomainRowIdentity::from_live_row(&row), row);
        }
        for row in untracked_rows {
            rows_by_identity.insert(DomainRowIdentity::from_live_row(&row), row);
        }
        rows_by_identity.into_values().collect()
    }

    struct StrictEmptyHotStateReader;

    #[async_trait]
    impl HotStateReader for StrictEmptyHotStateReader {
        async fn load_exact_batch(
            &self,
            request: &HotStateExactBatchRequest,
        ) -> Result<crate::hot_state::MaterializedHotStateExactBatch, LixError> {
            crate::hot_state::load_exact_batch_via_scan_for_test(self, request).await
        }

        async fn scan_batch(
            &self,
            _request: &HotStateScanRequest,
        ) -> Result<MaterializedHotStateBatch, LixError> {
            Ok(Vec::new().into())
        }
    }

    struct StrictStaticHotStateReader {
        rows: Vec<MaterializedHotStateRow>,
    }

    #[async_trait]
    impl HotStateReader for StrictStaticHotStateReader {
        async fn load_exact_batch(
            &self,
            request: &HotStateExactBatchRequest,
        ) -> Result<crate::hot_state::MaterializedHotStateExactBatch, LixError> {
            crate::hot_state::load_exact_batch_via_scan_for_test(self, request).await
        }

        async fn scan_batch(
            &self,
            request: &HotStateScanRequest,
        ) -> Result<MaterializedHotStateBatch, LixError> {
            Ok(self
                .rows
                .iter()
                .filter(|row| hot_state_row_matches_scan(row, request))
                .cloned()
                .collect::<Vec<_>>()
                .into())
        }
    }

    struct CountingStaticHotStateReader {
        rows: Vec<MaterializedHotStateRow>,
        scan_count: AtomicUsize,
    }

    #[async_trait]
    impl HotStateReader for CountingStaticHotStateReader {
        async fn load_exact_batch(
            &self,
            request: &HotStateExactBatchRequest,
        ) -> Result<crate::hot_state::MaterializedHotStateExactBatch, LixError> {
            crate::hot_state::load_exact_batch_via_scan_for_test(self, request).await
        }

        async fn scan_batch(
            &self,
            request: &HotStateScanRequest,
        ) -> Result<MaterializedHotStateBatch, LixError> {
            self.scan_count.fetch_add(1, Ordering::Relaxed);
            Ok(self
                .rows
                .iter()
                .cloned()
                .chain(test_file_descriptor_rows())
                .filter(|row| hot_state_row_matches_scan(row, request))
                .collect::<Vec<_>>()
                .into())
        }
    }

    #[test]
    fn schema_catalog_indexes_visible_schemas_by_key_and_branch() {
        let visible_schemas = vec![json!({
            "x-lix-key": "visible_schema",
            "type": "object",
        })];
        let staged_writes = empty_staged_write_set();
        let input = validation_input(&staged_writes, &visible_schemas);

        let catalog = catalog_from_transaction_input(&input).expect("schema catalog should build");

        assert_eq!(catalog.len(), 1);
        assert!(catalog.contains("visible_schema"));
    }

    #[test]
    fn schema_catalog_includes_pending_registered_schema_rows() {
        let visible_schemas = vec![
            registered_schema(),
            json!({
                "x-lix-key": "visible_schema",
                "type": "object",
            }),
        ];
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![pending_registered_schema_row("pending_schema")],
            ..empty_staged_write_set()
        };
        let input = validation_input(&staged_writes, &visible_schemas);

        let catalog = catalog_from_transaction_input(&input).expect("schema catalog should build");

        assert_eq!(catalog.len(), 3);
        assert!(catalog.contains("visible_schema"));
        assert!(catalog.contains("pending_schema"));
    }

    #[tokio::test]
    async fn filesystem_namespace_change_detection_skips_unchanged_exact_occupants() {
        let mut tracked = staged_file_descriptor_row(
            "01920000-0000-7000-8000-0000000000a2",
            "01920000-0000-7000-8000-0000000000a1",
        );
        tracked.metadata = Some(test_stage_json(r#"{"revision":2}"#));
        let mut untracked = staged_file_descriptor_row(
            "01920000-0000-7000-8000-000000000182",
            "01920000-0000-7000-8000-0000000000a1",
        );
        mark_prepared_row_untracked(&mut untracked);
        let mut committed_untracked = committed_file_descriptor_row(
            "01920000-0000-7000-8000-000000000182",
            "01920000-0000-7000-8000-0000000000a1",
        );
        mark_live_row_untracked(&mut committed_untracked);
        let directory = directory_descriptor_row(
            "01920000-0000-7000-8000-0000000000a3",
            None,
            "dir",
            "01920000-0000-7000-8000-0000000000a1",
        );

        let cases = vec![
            (
                vec![tracked],
                vec![committed_file_descriptor_row(
                    "01920000-0000-7000-8000-0000000000a2",
                    "01920000-0000-7000-8000-0000000000a1",
                )],
                "tracked descriptor",
            ),
            (
                vec![untracked],
                vec![committed_untracked],
                "untracked descriptor",
            ),
            (
                vec![staged_file_descriptor_row(
                    "01920000-0000-7000-8000-000000000192",
                    crate::GLOBAL_BRANCH_ID,
                )],
                vec![committed_file_descriptor_row(
                    "01920000-0000-7000-8000-000000000192",
                    crate::GLOBAL_BRANCH_ID,
                )],
                "global descriptor",
            ),
            (
                vec![directory.clone()],
                vec![MaterializedHotStateRow::from(directory)],
                "directory descriptor",
            ),
        ];
        for (staged, committed, scenario) in cases {
            assert_filesystem_namespace_domain_changed(staged, committed, false, scenario).await;
        }
    }

    #[tokio::test]
    async fn filesystem_namespace_change_detection_falls_back_for_namespace_mutations() {
        let committed_file = committed_file_descriptor_row(
            "01920000-0000-7000-8000-0000000000a2",
            "01920000-0000-7000-8000-0000000000a1",
        );

        let mut renamed = staged_file_descriptor_row(
            "01920000-0000-7000-8000-0000000000a2",
            "01920000-0000-7000-8000-0000000000a1",
        );
        renamed.snapshot = Some(test_stage_json(
            r#"{"id":"01920000-0000-7000-8000-0000000000a2","directory_id":null,"name":"renamed"}"#,
        ));
        let mut moved = staged_file_descriptor_row(
            "01920000-0000-7000-8000-0000000000a2",
            "01920000-0000-7000-8000-0000000000a1",
        );
        moved.snapshot = Some(test_stage_json(
            r#"{"id":"01920000-0000-7000-8000-0000000000a2","directory_id":"01920000-0000-7000-8000-0000000000a3","name":"01920000-0000-7000-8000-0000000000a2"}"#,
        ));
        let mut untracked = staged_file_descriptor_row(
            "01920000-0000-7000-8000-0000000000a2",
            "01920000-0000-7000-8000-0000000000a1",
        );
        mark_prepared_row_untracked(&mut untracked);
        let mut renamed_directory = directory_descriptor_row(
            "01920000-0000-7000-8000-0000000000a3",
            None,
            "before",
            "01920000-0000-7000-8000-0000000000a1",
        );
        let committed_directory = MaterializedHotStateRow::from(renamed_directory.clone());
        renamed_directory.snapshot = Some(test_stage_json(
            r#"{"id":"01920000-0000-7000-8000-0000000000a3","parent_id":null,"name":"after"}"#,
        ));

        let cases = vec![
            (vec![renamed], vec![committed_file.clone()], "rename"),
            (vec![moved], vec![committed_file.clone()], "move"),
            (
                vec![
                    staged_file_descriptor_row(
                        "01920000-0000-7000-8000-0000000000a2",
                        "01920000-0000-7000-8000-0000000000a1",
                    ),
                    staged_file_descriptor_row(
                        "01920000-0000-7000-8000-000000000172",
                        "01920000-0000-7000-8000-0000000000a1",
                    ),
                ],
                vec![committed_file.clone()],
                "mixed existing/new descriptors",
            ),
            (
                vec![
                    staged_file_descriptor_row(
                        "01920000-0000-7000-8000-0000000000a2",
                        "01920000-0000-7000-8000-0000000000a1",
                    ),
                    staged_file_descriptor_row(
                        "01920000-0000-7000-8000-0000000000b2",
                        "01920000-0000-7000-8000-0000000000a1",
                    ),
                ],
                vec![
                    committed_file.clone(),
                    committed_file_descriptor_row(
                        "01920000-0000-7000-8000-0000000000b2",
                        "01920000-0000-7000-8000-0000000000a1",
                    ),
                ],
                "multiple unchanged descriptors",
            ),
            (vec![untracked], vec![committed_file], "durability mismatch"),
            (
                vec![renamed_directory],
                vec![committed_directory],
                "directory rename",
            ),
        ];
        for (staged, committed, scenario) in cases {
            assert_filesystem_namespace_domain_changed(staged, committed, true, scenario).await;
        }
    }

    #[tokio::test]
    async fn filesystem_namespace_tombstone_only_batches_skip_full_scan() {
        let mut deleted = staged_file_descriptor_row(
            "01920000-0000-7000-8000-0000000000a2",
            "01920000-0000-7000-8000-0000000000a1",
        );
        deleted.snapshot = None;
        let delete_write = PreparedWriteSet {
            state_rows: prepared_rows![deleted.clone()],
            ..empty_staged_write_set()
        };
        assert_eq!(
            filesystem_namespace_validation_scan_count_for_test(delete_write, false)
                .await
                .expect("descriptor deletion should validate"),
            0,
            "removing an occupant cannot create a namespace collision"
        );

        let mixed_write = PreparedWriteSet {
            state_rows: prepared_rows![
                deleted,
                staged_file_descriptor_row(
                    "01920000-0000-7000-8000-000000000172",
                    "01920000-0000-7000-8000-0000000000a1",
                ),
            ],
            ..empty_staged_write_set()
        };
        assert_eq!(
            filesystem_namespace_validation_scan_count_for_test(mixed_write, false)
                .await
                .expect("mixed namespace mutation should validate"),
            1,
            "a delete mixed with an insertion must still validate the complete namespace"
        );
    }

    #[tokio::test]
    async fn filesystem_planner_inserts_skip_redundant_namespace_scan() {
        for (surface, schema_key, entity_id) in [
            (
                "lix_file",
                FILE_DESCRIPTOR_SCHEMA_KEY,
                "01920000-0000-7000-8000-000000000202",
            ),
            (
                "lix_file_by_branch",
                FILE_DESCRIPTOR_SCHEMA_KEY,
                "01920000-0000-7000-8000-000000000212",
            ),
            (
                "lix_directory",
                DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
                "01920000-0000-7000-8000-000000000203",
            ),
            (
                "lix_directory_by_branch",
                DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
                "01920000-0000-7000-8000-000000000213",
            ),
        ] {
            let mut logical_insert = if schema_key == FILE_DESCRIPTOR_SCHEMA_KEY {
                staged_file_descriptor_row(entity_id, "01920000-0000-7000-8000-0000000000a1")
            } else {
                directory_descriptor_row(
                    entity_id,
                    None,
                    surface,
                    "01920000-0000-7000-8000-0000000000a1",
                )
            };
            logical_insert.origin = Some(filesystem_insert_origin(surface, entity_id));
            let untrusted_transaction_write = PreparedWriteSet {
                state_rows: prepared_rows![logical_insert.clone()],
                ..empty_staged_write_set()
            };
            assert_eq!(
                filesystem_namespace_validation_scan_count_for_test(
                    untrusted_transaction_write,
                    false,
                )
                .await
                .expect("explicit transaction insert should succeed"),
                1,
                "{surface} must be revalidated when planning was not serialized"
            );
            let logical_write = PreparedWriteSet {
                state_rows: prepared_rows![logical_insert],
                ..empty_staged_write_set()
            };
            assert_eq!(
                filesystem_namespace_validation_scan_count_for_test(logical_write, true)
                    .await
                    .expect("planner-validated insert should succeed"),
                0,
                "{surface} already validated the transaction-visible namespace"
            );
        }
    }

    #[tokio::test]
    async fn filesystem_planner_certificate_must_match_final_descriptor() {
        let mut wrong_surface = staged_file_descriptor_row(
            "01920000-0000-7000-8000-000000000222",
            "01920000-0000-7000-8000-0000000000a1",
        );
        wrong_surface.origin = Some(filesystem_insert_origin(
            "lix_directory",
            "01920000-0000-7000-8000-000000000222",
        ));
        let mut missing_key = staged_file_descriptor_row(
            "01920000-0000-7000-8000-000000000232",
            "01920000-0000-7000-8000-0000000000a1",
        );
        missing_key.origin = Some(TransactionWriteOrigin {
            surface: "lix_file".into(),
            operation: TransactionWriteOperation::Insert,
            primary_key: None,
        });
        let mut wrong_key = staged_file_descriptor_row(
            "01920000-0000-7000-8000-000000000242",
            "01920000-0000-7000-8000-0000000000a1",
        );
        wrong_key.origin = Some(filesystem_insert_origin(
            "lix_file",
            "01920000-0000-7000-8000-000000000252",
        ));
        let mut update = staged_file_descriptor_row(
            "01920000-0000-7000-8000-000000000262",
            "01920000-0000-7000-8000-0000000000a1",
        );
        update.origin = Some(TransactionWriteOrigin {
            operation: TransactionWriteOperation::Update,
            ..filesystem_insert_origin("lix_file", "01920000-0000-7000-8000-000000000262")
        });
        let mut near_match = staged_file_descriptor_row(
            "01920000-0000-7000-8000-000000000272",
            "01920000-0000-7000-8000-0000000000a1",
        );
        near_match.origin = Some(filesystem_insert_origin(
            "lix_file_internal",
            "01920000-0000-7000-8000-000000000272",
        ));

        for (row, scenario) in [
            (wrong_surface, "wrong schema/surface pair"),
            (missing_key, "missing logical primary key"),
            (wrong_key, "logical primary key mismatch"),
            (update, "non-insert operation"),
            (near_match, "near-match surface"),
        ] {
            let staged_writes = PreparedWriteSet {
                state_rows: prepared_rows![row],
                ..empty_staged_write_set()
            };
            assert!(
                filesystem_namespace_validation_scan_count_for_test(staged_writes, true)
                    .await
                    .unwrap_or_else(|error| panic!("{scenario}: {error}"))
                    >= 1,
                "{scenario} must retain commit-time namespace validation"
            );
        }
    }

    #[tokio::test]
    async fn direct_and_mixed_descriptor_inserts_keep_namespace_scan() {
        let explicit_insert = staged_file_descriptor_row(
            "01920000-0000-7000-8000-000000000282",
            "01920000-0000-7000-8000-0000000000a1",
        );
        let mut explicit_write = PreparedWriteSet {
            state_rows: prepared_rows![explicit_insert.clone()],
            ..empty_staged_write_set()
        };
        explicit_write.remember_insert_identity_for_tests(&explicit_insert);
        assert_eq!(
            filesystem_namespace_validation_scan_count_for_test(explicit_write, true)
                .await
                .expect("explicit insert validation should succeed"),
            1,
            "direct descriptor inserts have no trusted planner certificate"
        );

        let mut logical_insert = staged_file_descriptor_row(
            "01920000-0000-7000-8000-000000000292",
            "01920000-0000-7000-8000-0000000000a1",
        );
        logical_insert.origin = Some(filesystem_insert_origin(
            "lix_file",
            "01920000-0000-7000-8000-000000000292",
        ));
        let mixed_write = PreparedWriteSet {
            state_rows: prepared_rows![logical_insert, explicit_insert],
            ..empty_staged_write_set()
        };
        assert_eq!(
            filesystem_namespace_validation_scan_count_for_test(mixed_write, true)
                .await
                .expect("mixed insert validation should succeed"),
            1,
            "one untrusted descriptor keeps validation for the whole domain"
        );
    }

    #[tokio::test]
    async fn descriptor_committed_unique_scan_defers_to_namespace_validation() {
        let mut inserted = staged_file_descriptor_row(
            "01920000-0000-7000-8000-000000000172",
            "01920000-0000-7000-8000-0000000000a1",
        );
        inserted.origin = Some(TransactionWriteOrigin {
            surface: "lix_file".into(),
            operation: TransactionWriteOperation::Insert,
            primary_key: None,
        });
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![inserted],
            ..empty_staged_write_set()
        };
        let validation_set = staged_writes.validation_set_for_tests();
        let row = validation_set
            .rows()
            .next()
            .expect("descriptor fixture should contain one validation row");
        let snapshot = row
            .snapshot_json()
            .expect("inserted descriptor should have a snapshot");
        let mut pending_constraints = PendingConstraintIndexes::default();
        pending_constraints
            .remember_row(
                row,
                test_plan_from_schema(file_descriptor_schema()),
                snapshot,
            )
            .expect("descriptor constraints should index");
        let catalog = CatalogSnapshot::from_visible_schemas(&[
            file_descriptor_schema(),
            directory_descriptor_schema(),
        ])
        .expect("descriptor catalog should compile");
        let hot_state = CountingStaticHotStateReader {
            rows: vec![committed_file_descriptor_row(
                "01920000-0000-7000-8000-0000000000a2",
                "01920000-0000-7000-8000-0000000000a1",
            )],
            scan_count: AtomicUsize::new(0),
        };
        let input = TransactionValidationInput::new(&validation_set, &catalog, &hot_state);

        validate_committed_unique_constraints(&input, &pending_constraints)
            .await
            .expect("namespace validation owns descriptor parent/name uniqueness");

        assert_eq!(
            hot_state.scan_count.load(Ordering::Relaxed),
            0,
            "descriptor unique validation must not rescan committed descriptors"
        );
    }

    #[tokio::test]
    async fn descriptor_namespace_still_rejects_same_kind_and_cross_kind_conflicts() {
        let mut inserted = staged_file_descriptor_row(
            "01920000-0000-7000-8000-000000000172",
            "01920000-0000-7000-8000-0000000000a1",
        );
        inserted.snapshot = Some(test_stage_json(
            r#"{"id":"01920000-0000-7000-8000-000000000172","directory_id":null,"name":"occupied"}"#,
        ));
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![inserted],
            ..empty_staged_write_set()
        };
        let visible_schemas = vec![file_descriptor_schema(), directory_descriptor_schema()];
        let mut committed_file = committed_file_descriptor_row(
            "01920000-0000-7000-8000-000000000162",
            "01920000-0000-7000-8000-0000000000a1",
        );
        committed_file.snapshot_content =
            Some(r#"{"id":"01920000-0000-7000-8000-000000000162","directory_id":null,"name":"occupied"}"#.into());
        let committed_directory = MaterializedHotStateRow::from(directory_descriptor_row(
            "01920000-0000-7000-8000-000000000163",
            None,
            "occupied",
            "01920000-0000-7000-8000-0000000000a1",
        ));

        for (committed, scenario) in [
            (committed_file, "file/file"),
            (committed_directory, "file/directory"),
        ] {
            let hot_state = StrictStaticHotStateReader {
                rows: vec![committed],
            };
            let error = validate_prepared_writes(
                TransactionValidationInput::from_visible_schemas_for_tests(
                    &staged_writes,
                    &visible_schemas,
                    &hot_state,
                ),
            )
            .await
            .unwrap_err();
            assert_eq!(error.code, LixError::CODE_UNIQUE, "{scenario}");
            assert!(
                error.message.contains("filesystem namespace conflict"),
                "{scenario}: {error:?}"
            );
        }
    }

    #[test]
    fn schema_catalog_rejects_pending_schema_duplicate_of_visible_identity() {
        let visible_schemas = vec![
            registered_schema(),
            json!({
                "x-lix-key": "same_schema",
                "type": "object",
                "properties": {
                    "old": { "type": "string" }
                }
            }),
        ];
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![pending_registered_schema_row("same_schema")],
            ..empty_staged_write_set()
        };

        let error = catalog_from_transaction_parts_unchecked(&staged_writes, &visible_schemas)
            .expect_err("pending schema must not override a visible domain fact");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(error.message.contains("more than one schema domain"));
    }

    #[test]
    fn pending_registered_schema_requires_snapshot_content() {
        let mut row = pending_registered_schema_row("missing_snapshot");
        row.snapshot = None;

        let error = validate_pending_registered_schema(
            PreparedValidationRow::State(row.borrowed()),
            &registered_schema(),
        )
        .expect_err("registered schema writes require snapshot_content");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
    }

    #[test]
    fn pending_registered_schema_rejects_invalid_snapshot_json() {
        let error =
            test_json_text("{not-json").expect_err("invalid JSON should fail before validation");

        assert_eq!(error.code, LixError::CODE_UNKNOWN);
    }

    #[test]
    fn pending_registered_schema_uses_builtin_schema_for_outer_value_shape() {
        let mut row = pending_registered_schema_row("missing_value");
        row.snapshot = Some(test_stage_json(&json!({}).to_string()));

        let error = validate_pending_registered_schema(
            PreparedValidationRow::State(row.borrowed()),
            &registered_schema(),
        )
        .expect_err("builtin lix_registered_schema validation should fail");

        assert_eq!(error.code, LixError::CODE_SCHEMA_VALIDATION);
    }

    #[test]
    fn pending_registered_schema_rejects_malformed_nested_lix_schema_definition() {
        let mut row = pending_registered_schema_row("bad_schema_definition");
        row.snapshot = Some(test_stage_json(
            &json!({
                "value": {
                    "x-lix-key": "bad_schema_definition",
                    "x-lix-primary-key": ["id"],
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" }
                    },
                    "required": ["id"],
                    "additionalProperties": false,
                }
            })
            .to_string(),
        ));

        let error = validate_pending_registered_schema(
            PreparedValidationRow::State(row.borrowed()),
            &registered_schema(),
        )
        .expect_err("nested Lix schema definition should be rejected");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
    }

    #[test]
    fn schema_catalog_rejects_duplicate_pending_registered_schema_identity() {
        let mut duplicate = pending_registered_schema_row("duplicate_schema");
        duplicate.entity_pk = registered_schema_entity_pk("duplicate_schema_duplicate");
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![
                pending_registered_schema_row("duplicate_schema"),
                duplicate
            ],
            ..empty_staged_write_set()
        };
        let visible_schemas = vec![registered_schema()];

        let error = catalog_from_transaction_parts(&staged_writes, &visible_schemas)
            .expect_err("duplicate pending schema keys should fail");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
    }

    #[test]
    fn schema_catalog_allows_pending_foreign_key_to_pending_schema() {
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![
                pending_registered_schema_from_definition(fk_parent_schema()),
                pending_registered_schema_from_definition(fk_child_schema()),
            ],
            ..empty_staged_write_set()
        };
        let visible_schemas = vec![registered_schema()];
        let input = validation_input(&staged_writes, &visible_schemas);

        let catalog = catalog_from_transaction_input(&input)
            .expect("pending parent schema should satisfy pending child foreign key");

        assert!(catalog.contains("fk_parent_schema"));
        assert!(catalog.contains("fk_child_schema"));
    }

    #[test]
    fn schema_catalog_rejects_foreign_key_missing_target_schema() {
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![
                pending_registered_schema_from_definition(fk_child_schema())
            ],
            ..empty_staged_write_set()
        };
        let visible_schemas = vec![registered_schema()];

        let error = catalog_from_transaction_parts(&staged_writes, &visible_schemas)
            .expect_err("missing referenced schema should fail");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
    }

    #[test]
    fn schema_catalog_rejects_foreign_key_missing_local_field() {
        let mut child = fk_child_schema();
        child["x-lix-foreign-keys"][0]["properties"] = json!(["/missing_parent_id"]);
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![
                pending_registered_schema_from_definition(fk_parent_schema()),
                pending_registered_schema_from_definition(child),
            ],
            ..empty_staged_write_set()
        };
        let visible_schemas = vec![registered_schema()];

        let error = catalog_from_transaction_parts(&staged_writes, &visible_schemas)
            .expect_err("missing local FK field should fail");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
    }

    #[test]
    fn schema_catalog_rejects_foreign_key_missing_referenced_field() {
        let mut child = fk_child_schema();
        child["x-lix-foreign-keys"][0]["references"]["properties"] = json!(["/missing_id"]);
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![
                pending_registered_schema_from_definition(fk_parent_schema()),
                pending_registered_schema_from_definition(child),
            ],
            ..empty_staged_write_set()
        };
        let visible_schemas = vec![registered_schema()];

        let error = catalog_from_transaction_parts(&staged_writes, &visible_schemas)
            .expect_err("missing referenced FK field should fail");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
    }

    #[test]
    fn schema_catalog_rejects_foreign_key_to_non_unique_target_field() {
        let mut parent = fk_parent_schema();
        parent["properties"]["name"] = json!({ "type": "string" });
        let mut child = fk_child_schema();
        child["x-lix-foreign-keys"][0]["references"]["properties"] = json!(["/name"]);
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![
                pending_registered_schema_from_definition(parent),
                pending_registered_schema_from_definition(child),
            ],
            ..empty_staged_write_set()
        };
        let visible_schemas = vec![registered_schema()];

        let error = catalog_from_transaction_parts(&staged_writes, &visible_schemas)
            .expect_err("FK target must be primary-key or unique");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
    }

    #[tokio::test]
    async fn validation_rejects_unknown_schema_key() {
        let visible_schemas = vec![key_value_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![staged_row("unknown_schema", Some(json!({}).to_string()))],
            ..empty_staged_write_set()
        };

        let error = validate_prepared_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect_err("unknown schema_key should fail");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
    }

    #[tokio::test]
    async fn validation_checks_schema_existence_for_tombstones() {
        let visible_schemas = vec![key_value_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![staged_row("unknown_schema", None)],
            ..empty_staged_write_set()
        };

        let error = validate_prepared_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect_err("tombstone with unknown schema should fail");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
    }

    #[tokio::test]
    async fn validation_allows_pending_registered_schema_to_validate_later_rows() {
        let visible_schemas = vec![key_value_schema(), registered_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![
                pending_registered_schema_row("pending_schema"),
                staged_row(
                    "pending_schema",
                    Some(json!({ "id": "entity-1" }).to_string()),
                ),
            ],
            ..empty_staged_write_set()
        };

        validate_prepared_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect("pending registered schema should be visible to later staged rows");
    }

    /// A commit that both registers a schema and writes rows of it must
    /// publish the collection's witness **and** those rows' entries.
    ///
    /// This is the one failure the hot index plane cannot have. The
    /// registration alone earns the collection a completeness witness, so if
    /// the same commit's rows are missing from the index the read path trusts
    /// a complete-looking index that holds nothing and returns no rows —
    /// silently, for every reader, until the next generation.
    ///
    /// The commit-time hook that this rework replaced resolved schemas through
    /// the catalog snapshot taken when the transaction opened, which by
    /// construction cannot contain a registration the transaction is still
    /// staging. Extraction now runs here instead, against the same
    /// transaction-visible catalog that
    /// `validation_allows_pending_registered_schema_to_validate_later_rows`
    /// pins — so the fix is structural, not a special case.
    ///
    /// (The SQL surface cannot currently reach this shape: an entity table
    /// registered inside an explicit transaction is not bindable until that
    /// transaction commits. The prepared-write path can, which is why the
    /// invariant is pinned at this level rather than through SQL.)
    #[tokio::test]
    async fn a_schema_registered_in_this_commit_indexes_its_own_rows() {
        let visible_schemas = vec![registered_schema()];
        let global_unique_row = |entity_pk: &str, slug: &str| {
            let mut row = staged_row(
                "unique_schema",
                Some(
                    json!({ "id": entity_pk, "slug": slug, "title": "title" })
                        .to_string(),
                ),
            );
            row.entity_pk = EntityPk::single(entity_pk);
            row
        };
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![
                pending_registered_schema_from_definition(unique_schema()),
                global_unique_row("entity-1", "slug-1"),
                global_unique_row("entity-2", "slug-2"),
            ],
            ..empty_staged_write_set()
        };

        let extracted = validate_prepared_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect("registration plus rows of that schema should validate");

        assert_eq!(
            extracted
                .registered_collections
                .iter()
                .cloned()
                .collect::<Vec<_>>(),
            vec![("unique_schema".to_string(), 0_u16)],
            "the registration must witness its one declared column"
        );
        let indexed = extracted
            .rows
            .iter()
            .flat_map(|row| {
                row.columns.iter().map(move |(ordinal, value)| {
                    (row.schema_key.as_str().to_owned(), *ordinal, value.clone())
                })
            })
            .collect::<Vec<_>>();
        assert_eq!(
            indexed,
            vec![
                (
                    "unique_schema".to_string(),
                    0,
                    Some(crate::hot_state::HotIndexValue::String("slug-1".into()))
                ),
                (
                    "unique_schema".to_string(),
                    0,
                    Some(crate::hot_state::HotIndexValue::String("slug-2".into()))
                ),
            ],
            "every row of the freshly registered schema must be extracted, or the \
             witness above publishes an index that is missing them"
        );
    }

    /// Every declared ordinal is carried, `None` included.
    ///
    /// Commit earns a witness per `(schema, ordinal)` from the row's presence,
    /// not from a value being found, so a row that omits an indexable value
    /// must still report that ordinal. Dropping it would leave the column
    /// unwitnessed and silently un-indexable for the whole collection.
    #[tokio::test]
    async fn extraction_reports_declared_ordinals_without_a_value() {
        let schema = json!({
            "x-lix-key": "sparse_index_schema",
            "x-lix-primary-key": ["/id"],
            "x-lix-unique": [["/slug"], ["/rank"]],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "slug": { "type": ["string", "null"] },
                "rank": { "type": ["integer", "null"] }
            },
            "required": ["id"],
            "additionalProperties": false
        });
        let mut row = staged_row(
            "sparse_index_schema",
            Some(json!({ "id": "entity-1", "slug": null, "rank": 7 }).to_string()),
        );
        row.entity_pk = EntityPk::single("entity-1");
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![
                pending_registered_schema_from_definition(schema),
                row,
            ],
            ..empty_staged_write_set()
        };

        let extracted =
            validate_prepared_writes(validation_input(&staged_writes, &vec![registered_schema()]))
                .await
                .expect("sparse indexed row should validate");

        let [row] = extracted.rows.as_slice() else {
            panic!("expected exactly one extracted row, got {:?}", extracted.rows);
        };
        assert_eq!(
            row.columns,
            vec![
                (0_u16, Some(crate::hot_state::HotIndexValue::Integer(7))),
                (1_u16, None),
            ],
            "a null indexed value must still report its ordinal"
        );
    }

    #[test]
    fn pending_schema_domain_covers_file_scoped_rows_in_the_same_catalog() {
        let mut pending_schema = pending_registered_schema_row("pending_file_schema");
        pending_schema.global = false;
        pending_schema.branch_id = "01920000-0000-7000-8000-0000000000a1".into();
        let pending_rows = [PreparedValidationRow::State(pending_schema.borrowed())];
        let domains = PendingSchemaDomains::from_staged_rows(&pending_rows)
            .expect("pending schema domains should build");

        let mut file_row = staged_row(
            "pending_file_schema",
            Some(json!({ "id": "entity-1" }).to_string()),
        );
        file_row.global = false;
        file_row.branch_id = "01920000-0000-7000-8000-0000000000a1".into();
        file_row.file_id = Some("01920000-0000-7000-8000-0000000000a2".into());

        domains
            .validate_row_schema_domain(PreparedValidationRow::State(file_row.borrowed()))
            .expect("schema catalog scope should cover every file scope in its branch");
    }

    #[tokio::test]
    async fn validation_rejects_tracked_row_using_pending_untracked_schema_definition() {
        let visible_schemas = vec![registered_schema()];
        let mut untracked_schema = pending_registered_schema_row("untracked_only_schema");
        mark_prepared_row_untracked(&mut untracked_schema);
        let mut tracked_row = staged_row(
            "untracked_only_schema",
            Some(json!({ "id": "row-1" }).to_string()),
        );
        tracked_row.entity_pk = EntityPk::single("row-1");
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![untracked_schema, tracked_row],
            ..empty_staged_write_set()
        };

        let error = validate_prepared_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect_err("tracked rows must not validate against untracked schema definitions");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
    }

    #[tokio::test]
    async fn validation_validates_snapshot_content_against_schema() {
        let visible_schemas = vec![key_value_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![staged_row(
                "lix_key_value",
                Some(json!({ "key": "k" }).to_string()),
            )],
            ..empty_staged_write_set()
        };

        let error = validate_prepared_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect_err("missing required snapshot field should fail");

        assert_eq!(error.code, LixError::CODE_SCHEMA_VALIDATION);
    }

    #[tokio::test]
    async fn validation_rejects_invalid_snapshot_json() {
        let error = test_json_text("{not-json")
            .expect_err("invalid snapshot JSON should fail before validation");

        assert_eq!(error.code, LixError::CODE_UNKNOWN);
    }

    #[tokio::test]
    async fn validation_skips_snapshot_validation_for_tombstones() {
        let visible_schemas = vec![key_value_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![staged_row("lix_key_value", None)],
            ..empty_staged_write_set()
        };

        validate_prepared_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect("tombstone should only require schema existence");
    }

    #[tokio::test]
    async fn validation_rejects_missing_file_owner_reference() {
        let visible_schemas = vec![unique_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![unique_row("post-1", "hello-world", "first")],
            ..empty_staged_write_set()
        };

        let error =
            validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &StrictEmptyHotStateReader,
            ))
            .await
            .expect_err("non-null file_id should require a file descriptor");

        assert_eq!(error.code, LixError::CODE_FILE_NOT_FOUND);
    }

    #[tokio::test]
    async fn validation_allows_pending_file_owner_reference() {
        let visible_schemas = vec![
            unique_schema(),
            file_descriptor_schema(),
            directory_descriptor_schema(),
        ];
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![
                staged_file_descriptor_row(
                    "01920000-0000-7000-8000-0000000000a2",
                    "01920000-0000-7000-8000-0000000000a1",
                ),
                unique_row("post-1", "hello-world", "first"),
            ],
            ..empty_staged_write_set()
        };

        validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
            &staged_writes,
            &visible_schemas,
            &StrictEmptyHotStateReader,
        ))
        .await
        .expect("same-transaction file descriptor should satisfy file ownership");
    }

    #[tokio::test]
    async fn validation_rejects_tracked_file_owner_reference_pending_only_as_untracked() {
        let visible_schemas = vec![
            unique_schema(),
            file_descriptor_schema(),
            directory_descriptor_schema(),
        ];
        let mut untracked_file_descriptor = staged_file_descriptor_row(
            "01920000-0000-7000-8000-0000000000a2",
            "01920000-0000-7000-8000-0000000000a1",
        );
        mark_prepared_row_untracked(&mut untracked_file_descriptor);
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![
                untracked_file_descriptor,
                unique_row("post-1", "hello-world", "first"),
            ],
            ..empty_staged_write_set()
        };

        let error =
            validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &StrictEmptyHotStateReader,
            ))
            .await
            .expect_err("tracked file owner must not resolve through pending untracked descriptor");

        // Same lane mismatch as the committed case: the file is being created
        // in this very transaction, just in the other lane.
        assert_eq!(error.code, LixError::CODE_CONSTRAINT_VIOLATION);
        assert!(
            error.message.contains("which exists but is untracked"),
            "the error must name the file's actual lane, got: {}",
            error.message
        );
    }

    /// Pending-descriptor sibling of
    /// `validation_rejects_untracked_file_owner_reference_committed_as_tracked`.
    #[tokio::test]
    async fn validation_rejects_untracked_file_owner_reference_pending_as_tracked() {
        let visible_schemas = vec![
            unique_schema(),
            file_descriptor_schema(),
            directory_descriptor_schema(),
        ];
        let mut untracked_row = unique_row("post-1", "hello-world", "first");
        mark_prepared_row_untracked(&mut untracked_row);
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![
                staged_file_descriptor_row(
                    "01920000-0000-7000-8000-0000000000a2",
                    "01920000-0000-7000-8000-0000000000a1",
                ),
                untracked_row,
            ],
            ..empty_staged_write_set()
        };

        let error =
            validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &StrictEmptyHotStateReader,
            ))
            .await
            .expect_err("an untracked row must not be owned by a pending tracked file");

        assert_eq!(error.code, LixError::CODE_CONSTRAINT_VIOLATION);
        assert!(
            error.message.contains("which exists but is tracked"),
            "the error must name the file's actual lane, got: {}",
            error.message
        );
    }

    #[tokio::test]
    async fn validation_rejects_file_owner_reference_when_descriptor_tombstoned_in_transaction() {
        let visible_schemas = vec![
            unique_schema(),
            file_descriptor_schema(),
            directory_descriptor_schema(),
        ];
        let mut file_descriptor_delete = staged_file_descriptor_row(
            "01920000-0000-7000-8000-0000000000a2",
            "01920000-0000-7000-8000-0000000000a1",
        );
        file_descriptor_delete.snapshot = None;
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![
                file_descriptor_delete,
                unique_row("post-1", "hello-world", "first"),
            ],
            ..empty_staged_write_set()
        };

        let error = validate_prepared_writes(
            TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &EmptyHotStateReader,
            ),
        )
        .await
        .expect_err("same-transaction file descriptor tombstone must hide committed descriptor");

        assert_eq!(error.code, LixError::CODE_FILE_NOT_FOUND);
    }

    #[tokio::test]
    async fn validation_allows_committed_file_owner_reference() {
        let visible_schemas = vec![unique_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![unique_row("post-1", "hello-world", "first")],
            ..empty_staged_write_set()
        };
        let hot_state = StaticHotStateReader {
            rows: vec![committed_file_descriptor_row(
                "01920000-0000-7000-8000-0000000000a2",
                "01920000-0000-7000-8000-0000000000a1",
            )],
        };

        validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
            &staged_writes,
            &visible_schemas,
            &hot_state,
        ))
        .await
        .expect("committed file descriptor should satisfy file ownership");
    }

    #[tokio::test]
    async fn validation_caches_committed_file_owner_reference_by_domain() {
        let visible_schemas = vec![unique_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![
                unique_row("post-1", "hello-world", "first"),
                unique_row("post-2", "second-slug", "second"),
            ],
            ..empty_staged_write_set()
        };
        let hot_state = CountingStaticHotStateReader {
            rows: Vec::new(),
            scan_count: AtomicUsize::new(0),
        };

        validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
            &staged_writes,
            &visible_schemas,
            &hot_state,
        ))
        .await
        .expect("shared committed file descriptor should satisfy file ownership");

        assert_eq!(hot_state.scan_count.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn validation_rejects_tracked_file_owner_reference_committed_only_as_untracked() {
        let visible_schemas = vec![unique_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![unique_row("post-1", "hello-world", "first")],
            ..empty_staged_write_set()
        };
        let mut untracked_file_descriptor = committed_file_descriptor_row(
            "01920000-0000-7000-8000-0000000000a2",
            "01920000-0000-7000-8000-0000000000a1",
        );
        mark_live_row_untracked(&mut untracked_file_descriptor);
        let hot_state = StrictStaticHotStateReader {
            rows: vec![untracked_file_descriptor],
        };

        let error = validate_prepared_writes(
            TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &hot_state,
            ),
        )
        .await
        .expect_err("tracked file owner must not resolve through committed untracked descriptor");

        // The file exists, in the other lane, so this is a lane mismatch rather
        // than a missing file. It used to report CODE_FILE_NOT_FOUND for a file
        // `SELECT * FROM lix_file` plainly returns.
        assert_eq!(error.code, LixError::CODE_CONSTRAINT_VIOLATION);
        assert!(
            error.message.contains("which exists but is untracked"),
            "the error must name the file's actual lane, got: {}",
            error.message
        );
    }

    /// The inverse of the behaviour PR D removes.
    ///
    /// This test previously asserted that an untracked row *may* be owned by a
    /// tracked file — the cross-lane pairing that let a tracked file deletion
    /// silently cascade untracked rows away. Its inversion is the evidence that
    /// the enforcement seam is live.
    #[tokio::test]
    async fn validation_rejects_untracked_file_owner_reference_committed_as_tracked() {
        let visible_schemas = vec![unique_schema()];
        let mut untracked_row = unique_row("post-1", "hello-world", "first");
        mark_prepared_row_untracked(&mut untracked_row);
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![untracked_row],
            ..empty_staged_write_set()
        };
        let hot_state = StrictStaticHotStateReader {
            rows: vec![committed_file_descriptor_row(
                "01920000-0000-7000-8000-0000000000a2",
                "01920000-0000-7000-8000-0000000000a1",
            )],
        };

        let error = validate_prepared_writes(
            TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &hot_state,
            ),
        )
        .await
        .expect_err("an untracked row must not be owned by a tracked file");

        assert_eq!(error.code, LixError::CODE_CONSTRAINT_VIOLATION);
        assert!(
            error.message.contains("which exists but is tracked"),
            "the error must name the file's actual lane, got: {}",
            error.message
        );
    }

    #[tokio::test]
    async fn validation_allows_tracked_file_owner_reference_committed_behind_untracked_overlay() {
        let visible_schemas = vec![unique_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![unique_row("post-1", "hello-world", "first")],
            ..empty_staged_write_set()
        };
        let tracked_file_descriptor = committed_file_descriptor_row(
            "01920000-0000-7000-8000-0000000000a2",
            "01920000-0000-7000-8000-0000000000a1",
        );
        let mut untracked_tombstone = committed_file_descriptor_row(
            "01920000-0000-7000-8000-0000000000a2",
            "01920000-0000-7000-8000-0000000000a1",
        );
        untracked_tombstone.snapshot_content = None;
        mark_live_row_untracked(&mut untracked_tombstone);
        let hot_state = OverlayingStaticHotStateReader {
            rows: vec![tracked_file_descriptor, untracked_tombstone],
        };

        validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
            &staged_writes,
            &visible_schemas,
            &hot_state,
        ))
        .await
        .expect("tracked file owner should resolve against tracked descriptor behind overlay");
    }

    #[tokio::test]
    async fn validation_allows_file_delete_cascade_over_committed_tracked_rows() {
        let visible_schemas = vec![
            unique_schema(),
            file_descriptor_schema(),
            directory_descriptor_schema(),
        ];
        let mut file_descriptor_delete = staged_file_descriptor_row(
            "01920000-0000-7000-8000-0000000000a2",
            "01920000-0000-7000-8000-0000000000a1",
        );
        file_descriptor_delete.snapshot = None;
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![file_descriptor_delete],
            ..empty_staged_write_set()
        };
        let hot_state = StaticHotStateReader {
            rows: vec![committed_unique_row("post-1", "hello-world", "first")],
        };

        validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
            &staged_writes,
            &visible_schemas,
            &hot_state,
        ))
        .await
        .expect("file descriptor deletion cascades committed file-owned rows");
    }

    #[tokio::test]
    async fn validation_allows_file_delete_cascade_over_committed_untracked_rows() {
        let visible_schemas = vec![
            unique_schema(),
            file_descriptor_schema(),
            directory_descriptor_schema(),
        ];
        let mut file_descriptor_delete = staged_file_descriptor_row(
            "01920000-0000-7000-8000-0000000000a2",
            "01920000-0000-7000-8000-0000000000a1",
        );
        file_descriptor_delete.snapshot = None;
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![file_descriptor_delete],
            ..empty_staged_write_set()
        };
        let mut untracked_row =
            MaterializedHotStateRow::from(unique_row("post-1", "hello-world", "first"));
        mark_live_row_untracked(&mut untracked_row);
        let hot_state = StrictStaticHotStateReader {
            rows: vec![
                committed_file_descriptor_row(
                    "01920000-0000-7000-8000-0000000000a2",
                    "01920000-0000-7000-8000-0000000000a1",
                ),
                untracked_row,
            ],
        };

        validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
            &staged_writes,
            &visible_schemas,
            &hot_state,
        ))
        .await
        .expect("file descriptor deletion cascades untracked file-owned rows");
    }

    #[tokio::test]
    async fn validation_allows_untracked_directory_parent_to_tracked_directory() {
        let visible_schemas = vec![directory_descriptor_schema()];
        let tracked_parent = directory_descriptor_row(
            "01920000-0000-7000-8000-000000000173",
            None,
            "parent",
            "01920000-0000-7000-8000-0000000000a1",
        );
        let mut untracked_child = directory_descriptor_row(
            "01920000-0000-7000-8000-000000000183",
            Some("01920000-0000-7000-8000-000000000173"),
            "child",
            "01920000-0000-7000-8000-0000000000a1",
        );
        mark_prepared_row_untracked(&mut untracked_child);
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![tracked_parent, untracked_child],
            ..empty_staged_write_set()
        };

        validate_prepared_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect("untracked directory parent_id should resolve through tracked directory");
    }

    #[tokio::test]
    async fn validation_rejects_file_owner_reference_that_exists_only_in_global() {
        let visible_schemas = vec![unique_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![unique_row("post-1", "hello-world", "first")],
            ..empty_staged_write_set()
        };
        let hot_state = StrictStaticHotStateReader {
            rows: vec![committed_file_descriptor_row(
                "01920000-0000-7000-8000-0000000000a2",
                crate::GLOBAL_BRANCH_ID,
            )],
        };

        let error =
            validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &hot_state,
            ))
            .await
            .expect_err("global file descriptor should not satisfy a branch-local row");

        assert_eq!(error.code, LixError::CODE_FILE_NOT_FOUND);
    }

    #[tokio::test]
    async fn validation_rejects_primary_key_duplicate_with_different_identity() {
        let visible_schemas = vec![unique_schema()];
        let mut conflicting = unique_row("post-1", "hello-world", "first");
        conflicting.entity_pk = EntityPk::single("post-2");
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![unique_row("post-1", "hello-world", "first"), conflicting],
            ..empty_staged_write_set()
        };

        let error = validate_prepared_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect_err("same primary key under different identity should fail");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
    }

    #[tokio::test]
    async fn validation_rejects_pending_unique_value_duplicate() {
        let visible_schemas = vec![unique_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![
                unique_row("post-1", "hello-world", "first"),
                unique_row("post-2", "hello-world", "second"),
            ],
            ..empty_staged_write_set()
        };

        let error = validate_prepared_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect_err("duplicate pending unique value should fail");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
    }

    #[tokio::test]
    async fn validation_rejects_pending_unique_duplicate_with_null_component() {
        let visible_schemas = vec![nullable_unique_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![
                nullable_unique_row("row-1", None, "root-name"),
                nullable_unique_row("row-2", None, "root-name"),
            ],
            ..empty_staged_write_set()
        };

        let error = validate_prepared_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect_err("duplicate nullable unique value should fail");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
    }

    #[tokio::test]
    async fn validation_rejects_pending_unique_same_value_in_same_branch() {
        let visible_schemas = vec![unique_schema()];
        let mut duplicate = unique_row("post-2", "hello-world", "second");
        duplicate.branch_id = "01920000-0000-7000-8000-0000000000a1".into();
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![unique_row("post-1", "hello-world", "first"), duplicate],
            ..empty_staged_write_set()
        };

        let error = validate_prepared_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect_err("same unique value in the same branch should fail");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
    }

    #[tokio::test]
    async fn validation_allows_pending_unique_same_value_in_different_branches() {
        let visible_schemas = vec![unique_schema()];
        let mut branch_b = unique_row("post-2", "hello-world", "second");
        branch_b.branch_id = "01920000-0000-7000-8000-0000000000b1".into();
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![unique_row("post-1", "hello-world", "first"), branch_b],
            ..empty_staged_write_set()
        };

        validate_prepared_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect("unique values should be scoped to the exact branch_id");
    }

    #[tokio::test]
    async fn validation_allows_pending_unique_overwrite_of_same_identity() {
        let visible_schemas = vec![unique_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![
                unique_row("post-1", "hello-world", "first"),
                unique_row("post-1", "hello-world", "updated"),
            ],
            ..empty_staged_write_set()
        };

        validate_prepared_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect("same identity should be treated as replacement, not duplicate");
    }

    #[tokio::test]
    async fn validation_skips_pending_unique_indexes_for_tombstones() {
        let visible_schemas = vec![unique_schema()];
        let mut tombstone = unique_row("post-1", "hello-world", "deleted");
        tombstone.snapshot = None;
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![tombstone, unique_row("post-2", "hello-world", "second")],
            ..empty_staged_write_set()
        };

        validate_prepared_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect("tombstones should not claim pending unique values");
    }

    #[tokio::test]
    async fn validation_scopes_pending_unique_values_by_file_and_branch() {
        let visible_schemas = vec![unique_schema()];
        let mut different_file = unique_row("post-2", "hello-world", "second");
        different_file.file_id = Some("01920000-0000-7000-8000-0000000000b2".into());
        let mut different_branch = unique_row("post-3", "hello-world", "third");
        different_branch.branch_id = "01920000-0000-7000-8000-0000000000b1".into();
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![
                unique_row("post-1", "hello-world", "first"),
                different_file,
                different_branch,
            ],
            ..empty_staged_write_set()
        };

        validate_prepared_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect("unique values are scoped by file and branch");
    }

    #[tokio::test]
    async fn validation_rejects_committed_visible_unique_value_duplicate() {
        let visible_schemas = vec![unique_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![unique_row("post-2", "hello-world", "second")],
            ..empty_staged_write_set()
        };
        let hot_state = StaticHotStateReader {
            rows: vec![committed_unique_row("post-1", "hello-world", "first")],
        };

        let error =
            validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &hot_state,
            ))
            .await
            .expect_err("committed visible unique value should conflict");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
    }

    #[tokio::test]
    async fn validation_rejects_committed_tracked_unique_duplicate_behind_untracked_overlay() {
        let visible_schemas = vec![unique_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![unique_row("post-2", "hello-world", "second")],
            ..empty_staged_write_set()
        };
        let tracked_duplicate = committed_unique_row("post-1", "hello-world", "first");
        let mut untracked_overlay = committed_unique_row("post-1", "draft-slug", "draft");
        mark_live_row_untracked(&mut untracked_overlay);
        let hot_state = OverlayingStaticHotStateReader {
            rows: vec![tracked_duplicate, untracked_overlay],
        };

        let error =
            validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &hot_state,
            ))
            .await
            .expect_err("tracked unique duplicate must be detected behind untracked overlay");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
    }

    #[tokio::test]
    async fn validation_rejects_committed_unique_duplicate_when_untracked_tombstone_shadows_owner()
    {
        let visible_schemas = vec![unique_schema()];
        let mut untracked_tombstone = unique_row("post-1", "ignored", "deleted");
        untracked_tombstone.snapshot = None;
        mark_prepared_row_untracked(&mut untracked_tombstone);
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![
                untracked_tombstone,
                unique_row("post-2", "hello-world", "second"),
            ],
            ..empty_staged_write_set()
        };
        let hot_state = StaticHotStateReader {
            rows: vec![committed_unique_row("post-1", "hello-world", "first")],
        };

        let error =
            validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &hot_state,
            ))
            .await
            .expect_err("untracked tombstone must not hide tracked unique owner");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
    }

    #[tokio::test]
    async fn validation_rejects_committed_unique_duplicate_with_null_component() {
        let visible_schemas = vec![nullable_unique_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![nullable_unique_row("row-2", None, "root-name")],
            ..empty_staged_write_set()
        };
        let hot_state = StaticHotStateReader {
            rows: vec![committed_nullable_unique_row("row-1", None, "root-name")],
        };

        let error =
            validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &hot_state,
            ))
            .await
            .expect_err("committed duplicate nullable unique value should conflict");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
    }

    #[tokio::test]
    async fn validation_rejects_committed_unique_same_value_in_same_branch() {
        let visible_schemas = vec![unique_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![unique_row("post-2", "hello-world", "second")],
            ..empty_staged_write_set()
        };
        let hot_state = StaticHotStateReader {
            rows: vec![committed_unique_row("post-1", "hello-world", "first")],
        };

        let error =
            validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &hot_state,
            ))
            .await
            .expect_err("same unique value in the same branch should conflict");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
    }

    #[tokio::test]
    async fn validation_allows_committed_unique_same_value_in_different_branches() {
        let visible_schemas = vec![unique_schema()];
        let mut branch_b = unique_row("post-2", "hello-world", "second");
        branch_b.branch_id = "01920000-0000-7000-8000-0000000000b1".into();
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![branch_b],
            ..empty_staged_write_set()
        };
        let hot_state = StaticHotStateReader {
            rows: vec![committed_unique_row("post-1", "hello-world", "first")],
        };

        validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
            &staged_writes,
            &visible_schemas,
            &hot_state,
        ))
        .await
        .expect("committed unique values should be scoped to the exact branch_id");
    }

    #[tokio::test]
    async fn validation_ignores_projected_hot_state_rows_for_unique_constraints() {
        let visible_schemas = vec![unique_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![unique_row("post-2", "hello-world", "second")],
            ..empty_staged_write_set()
        };
        let mut projected_overlay_row = committed_unique_row("post-1", "hello-world", "first");
        projected_overlay_row.branch_id = "01920000-0000-7000-8000-0000000000a1".into();
        projected_overlay_row.global = true;
        let hot_state = StaticHotStateReader {
            rows: vec![projected_overlay_row],
        };

        validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
            &staged_writes,
            &visible_schemas,
            &hot_state,
        ))
        .await
        .expect("validation should ignore live-state overlay projections");
    }

    #[tokio::test]
    async fn validation_allows_committed_visible_unique_update_of_same_identity() {
        let visible_schemas = vec![unique_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![unique_row("post-1", "hello-world", "updated")],
            ..empty_staged_write_set()
        };
        let hot_state = StaticHotStateReader {
            rows: vec![committed_unique_row("post-1", "hello-world", "first")],
        };

        validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
            &staged_writes,
            &visible_schemas,
            &hot_state,
        ))
        .await
        .expect("same identity should update committed unique owner");
    }

    #[tokio::test]
    async fn validation_rejects_unique_update_to_another_committed_value() {
        let visible_schemas = vec![unique_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![unique_row("post-1", "second-slug", "updated")],
            ..empty_staged_write_set()
        };
        let hot_state = StaticHotStateReader {
            rows: vec![
                committed_unique_row("post-1", "hello-world", "first"),
                committed_unique_row("post-2", "second-slug", "second"),
            ],
        };

        let error =
            validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &hot_state,
            ))
            .await
            .expect_err("changing to another committed unique value should conflict");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
    }

    #[tokio::test]
    async fn validation_insert_origin_keeps_single_committed_unique_scan() {
        let visible_schemas = vec![unique_schema()];
        let mut inserted = unique_row("post-new", "new-slug", "new");
        inserted.file_id = None;
        // Models INSERT ... ON CONFLICT: Replace mode has no insert identity,
        // while the newly inserted physical row retains its logical origin.
        inserted.origin = Some(TransactionWriteOrigin {
            surface: "lix_file".into(),
            operation: TransactionWriteOperation::Insert,
            primary_key: None,
        });
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![inserted],
            ..empty_staged_write_set()
        };
        let mut committed = committed_unique_row("post-1", "hello-world", "first");
        committed.file_id = None;
        let hot_state = CountingStaticHotStateReader {
            rows: vec![committed],
            scan_count: AtomicUsize::new(0),
        };

        validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
            &staged_writes,
            &visible_schemas,
            &hot_state,
        ))
        .await
        .expect("insert with a distinct unique value should succeed");

        assert_eq!(hot_state.scan_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn validation_batches_committed_unique_scans_by_constraint_group() {
        let visible_schemas = vec![unique_schema()];
        let mut staged_one = unique_row("post-3", "new-slug-3", "third");
        staged_one.file_id = None;
        let mut staged_two = unique_row("post-4", "new-slug-4", "fourth");
        staged_two.file_id = None;
        let mut committed_one = committed_unique_row("post-1", "hello-world", "first");
        committed_one.file_id = None;
        let mut committed_two = committed_unique_row("post-2", "second-slug", "second");
        committed_two.file_id = None;
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![staged_one, staged_two],
            ..empty_staged_write_set()
        };
        let hot_state = CountingStaticHotStateReader {
            rows: vec![committed_one, committed_two],
            scan_count: AtomicUsize::new(0),
        };

        validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
            &staged_writes,
            &visible_schemas,
            &hot_state,
        ))
        .await
        .expect("distinct pending unique values should not conflict");

        assert_eq!(hot_state.scan_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn validation_ignores_committed_unique_owner_tombstoned_by_transaction() {
        let visible_schemas = vec![unique_schema()];
        let mut tombstone = unique_row("post-1", "hello-world", "deleted");
        tombstone.snapshot = None;
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![tombstone, unique_row("post-2", "hello-world", "second")],
            ..empty_staged_write_set()
        };
        let hot_state = StaticHotStateReader {
            rows: vec![committed_unique_row("post-1", "hello-world", "first")],
        };

        validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
            &staged_writes,
            &visible_schemas,
            &hot_state,
        ))
        .await
        .expect("tombstoned committed owner should not conflict");
    }

    #[tokio::test]
    async fn validation_allows_committed_unique_same_value_in_different_file_or_branch() {
        let visible_schemas = vec![unique_schema()];
        let mut different_file = unique_row("post-2", "hello-world", "second");
        different_file.file_id = Some("01920000-0000-7000-8000-0000000000b2".into());
        let mut different_branch = unique_row("post-3", "hello-world", "third");
        different_branch.branch_id = "01920000-0000-7000-8000-0000000000b1".into();
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![different_file, different_branch],
            ..empty_staged_write_set()
        };
        let hot_state = StaticHotStateReader {
            rows: vec![committed_unique_row("post-1", "hello-world", "first")],
        };

        validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
            &staged_writes,
            &visible_schemas,
            &hot_state,
        ))
        .await
        .expect("committed uniqueness is scoped by file and branch");
    }

    #[tokio::test]
    async fn validation_rejects_foreign_key_target_missing_in_same_branch() {
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![fk_child_row(
                "child-1",
                "parent-1",
                "01920000-0000-7000-8000-0000000000a1",
            )],
            ..empty_staged_write_set()
        };

        let error = validate_prepared_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect_err("foreign key must resolve in the same branch");

        assert_eq!(error.code, LixError::CODE_FOREIGN_KEY);
    }

    #[tokio::test]
    async fn validation_allows_foreign_key_target_in_same_branch() {
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![
                fk_parent_row("parent-1", "01920000-0000-7000-8000-0000000000a1"),
                fk_child_row(
                    "child-1",
                    "parent-1",
                    "01920000-0000-7000-8000-0000000000a1",
                ),
            ],
            ..empty_staged_write_set()
        };

        validate_prepared_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect("foreign key should resolve against pending rows in the same branch");
    }

    #[tokio::test]
    async fn validation_rejects_tracked_foreign_key_target_pending_only_as_untracked() {
        let visible_schemas = vec![
            fk_parent_schema(),
            fk_child_schema(),
            file_descriptor_schema(),
            directory_descriptor_schema(),
        ];
        let mut untracked_parent =
            fk_parent_row("parent-1", "01920000-0000-7000-8000-0000000000a1");
        mark_prepared_row_untracked(&mut untracked_parent);
        let mut untracked_file_descriptor = staged_file_descriptor_row(
            "01920000-0000-7000-8000-0000000000a2",
            "01920000-0000-7000-8000-0000000000a1",
        );
        mark_prepared_row_untracked(&mut untracked_file_descriptor);
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![
                untracked_file_descriptor,
                untracked_parent,
                fk_child_row(
                    "child-1",
                    "parent-1",
                    "01920000-0000-7000-8000-0000000000a1",
                ),
            ],
            ..empty_staged_write_set()
        };

        let error = validate_prepared_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect_err("tracked FK must not resolve through a pending untracked target");

        assert_eq!(error.code, LixError::CODE_FOREIGN_KEY);
    }

    #[tokio::test]
    async fn validation_allows_untracked_foreign_key_target_pending_as_tracked() {
        let visible_schemas = vec![
            fk_parent_schema(),
            fk_child_schema(),
            file_descriptor_schema(),
            directory_descriptor_schema(),
        ];
        let tracked_file_descriptor = staged_file_descriptor_row(
            "01920000-0000-7000-8000-0000000000a2",
            "01920000-0000-7000-8000-0000000000a1",
        );
        let tracked_parent = fk_parent_row("parent-1", "01920000-0000-7000-8000-0000000000a1");
        let mut untracked_file_descriptor = staged_file_descriptor_row(
            "01920000-0000-7000-8000-0000000000a2",
            "01920000-0000-7000-8000-0000000000a1",
        );
        mark_prepared_row_untracked(&mut untracked_file_descriptor);
        let mut untracked_child = fk_child_row(
            "child-1",
            "parent-1",
            "01920000-0000-7000-8000-0000000000a1",
        );
        mark_prepared_row_untracked(&mut untracked_child);
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![
                tracked_file_descriptor,
                tracked_parent,
                untracked_file_descriptor,
                untracked_child,
            ],
            ..empty_staged_write_set()
        };

        validate_prepared_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect("untracked FK should be allowed to reference a pending tracked target");
    }

    #[tokio::test]
    async fn validation_rejects_foreign_key_target_that_exists_only_in_different_branch() {
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![
                fk_parent_row("parent-1", "01920000-0000-7000-8000-0000000000b1"),
                fk_child_row(
                    "child-1",
                    "parent-1",
                    "01920000-0000-7000-8000-0000000000a1",
                ),
            ],
            ..empty_staged_write_set()
        };

        let error = validate_prepared_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect_err("foreign key target in another branch should not satisfy this branch");

        assert_eq!(error.code, LixError::CODE_FOREIGN_KEY);
    }

    #[tokio::test]
    async fn validation_primary_key_fk_point_lookup_ignores_unrelated_rows() {
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![fk_child_row(
                "child-1",
                "parent-1",
                "01920000-0000-7000-8000-0000000000a1",
            )],
            ..empty_staged_write_set()
        };
        let hot_state = StaticHotStateReader {
            rows: vec![
                {
                    let mut unrelated = MaterializedHotStateRow::from(fk_parent_row(
                        "unrelated",
                        "01920000-0000-7000-8000-0000000000a1",
                    ));
                    unrelated.snapshot_content = Some("{invalid".into());
                    unrelated
                },
                MaterializedHotStateRow::from(fk_parent_row(
                    "parent-1",
                    "01920000-0000-7000-8000-0000000000a1",
                )),
            ],
        };

        validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
            &staged_writes,
            &visible_schemas,
            &hot_state,
        ))
        .await
        .expect("foreign key should resolve against committed rows in the same branch");
    }

    #[tokio::test]
    async fn validation_rejects_tracked_foreign_key_target_committed_only_as_untracked() {
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![fk_child_row(
                "child-1",
                "parent-1",
                "01920000-0000-7000-8000-0000000000a1",
            )],
            ..empty_staged_write_set()
        };
        let mut untracked_parent = MaterializedHotStateRow::from(fk_parent_row(
            "parent-1",
            "01920000-0000-7000-8000-0000000000a1",
        ));
        mark_live_row_untracked(&mut untracked_parent);
        let hot_state = StaticHotStateReader {
            rows: vec![untracked_parent],
        };

        let error =
            validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &hot_state,
            ))
            .await
            .expect_err("tracked FK must not resolve through a committed untracked target");

        assert_eq!(error.code, LixError::CODE_FOREIGN_KEY);
    }

    #[tokio::test]
    async fn validation_allows_untracked_foreign_key_target_committed_as_tracked() {
        let visible_schemas = vec![
            fk_parent_schema(),
            fk_child_schema(),
            file_descriptor_schema(),
            directory_descriptor_schema(),
        ];
        let mut untracked_file_descriptor = staged_file_descriptor_row(
            "01920000-0000-7000-8000-0000000000a2",
            "01920000-0000-7000-8000-0000000000a1",
        );
        mark_prepared_row_untracked(&mut untracked_file_descriptor);
        let mut untracked_child = fk_child_row(
            "child-1",
            "parent-1",
            "01920000-0000-7000-8000-0000000000a1",
        );
        mark_prepared_row_untracked(&mut untracked_child);
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![untracked_file_descriptor, untracked_child],
            ..empty_staged_write_set()
        };
        let hot_state = StaticHotStateReader {
            rows: vec![
                committed_file_descriptor_row(
                    "01920000-0000-7000-8000-0000000000a2",
                    "01920000-0000-7000-8000-0000000000a1",
                ),
                MaterializedHotStateRow::from(fk_parent_row(
                    "parent-1",
                    "01920000-0000-7000-8000-0000000000a1",
                )),
            ],
        };

        validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
            &staged_writes,
            &visible_schemas,
            &hot_state,
        ))
        .await
        .expect("untracked FK should be allowed to reference a committed tracked target");
    }

    #[tokio::test]
    async fn validation_allows_tracked_foreign_key_target_committed_behind_untracked_overlay() {
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![fk_child_row(
                "child-1",
                "parent-1",
                "01920000-0000-7000-8000-0000000000a1",
            )],
            ..empty_staged_write_set()
        };
        let tracked_parent = MaterializedHotStateRow::from(fk_parent_row(
            "parent-1",
            "01920000-0000-7000-8000-0000000000a1",
        ));
        let mut untracked_overlay = MaterializedHotStateRow::from(fk_parent_row(
            "parent-1",
            "01920000-0000-7000-8000-0000000000a1",
        ));
        mark_live_row_untracked(&mut untracked_overlay);
        let hot_state = OverlayingStaticHotStateReader {
            rows: vec![tracked_parent, untracked_overlay],
        };

        validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
            &staged_writes,
            &visible_schemas,
            &hot_state,
        ))
        .await
        .expect(
            "tracked FK should resolve against tracked storage target behind untracked overlay",
        );
    }

    #[tokio::test]
    async fn validation_rejects_deleting_tracked_fk_target_referenced_behind_untracked_overlay() {
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let mut parent_delete = fk_parent_row("parent-1", "01920000-0000-7000-8000-0000000000a1");
        parent_delete.snapshot = None;
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![parent_delete],
            ..empty_staged_write_set()
        };
        let tracked_parent = MaterializedHotStateRow::from(fk_parent_row(
            "parent-1",
            "01920000-0000-7000-8000-0000000000a1",
        ));
        let tracked_child = MaterializedHotStateRow::from(fk_child_row(
            "child-1",
            "parent-1",
            "01920000-0000-7000-8000-0000000000a1",
        ));
        let mut untracked_child_overlay = MaterializedHotStateRow::from(fk_child_row(
            "child-1",
            "other-parent",
            "01920000-0000-7000-8000-0000000000a1",
        ));
        mark_live_row_untracked(&mut untracked_child_overlay);
        let hot_state = OverlayingStaticHotStateReader {
            rows: vec![tracked_parent, tracked_child, untracked_child_overlay],
        };

        let error =
            validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &hot_state,
            ))
            .await
            .expect_err("tracked referencing row behind overlay must block target delete");

        assert_eq!(error.code, LixError::CODE_FOREIGN_KEY);
    }

    #[tokio::test]
    async fn validation_rejects_deleting_tracked_fk_target_referenced_by_committed_untracked_row() {
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let mut parent_delete = fk_parent_row("parent-1", "01920000-0000-7000-8000-0000000000a1");
        parent_delete.snapshot = None;
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![parent_delete],
            ..empty_staged_write_set()
        };
        let tracked_parent = MaterializedHotStateRow::from(fk_parent_row(
            "parent-1",
            "01920000-0000-7000-8000-0000000000a1",
        ));
        let mut untracked_child = MaterializedHotStateRow::from(fk_child_row(
            "child-1",
            "parent-1",
            "01920000-0000-7000-8000-0000000000a1",
        ));
        mark_live_row_untracked(&mut untracked_child);
        let hot_state = StaticHotStateReader {
            rows: vec![tracked_parent, untracked_child],
        };

        let error =
            validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &hot_state,
            ))
            .await
            .expect_err("tracked target delete must be blocked by committed untracked references");

        assert_eq!(error.code, LixError::CODE_FOREIGN_KEY);
    }

    #[tokio::test]
    async fn validation_rejects_foreign_key_target_committed_only_in_different_branch() {
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![fk_child_row(
                "child-1",
                "parent-1",
                "01920000-0000-7000-8000-0000000000a1",
            )],
            ..empty_staged_write_set()
        };
        let hot_state = StaticHotStateReader {
            rows: vec![MaterializedHotStateRow::from(fk_parent_row(
                "parent-1",
                "01920000-0000-7000-8000-0000000000b1",
            ))],
        };

        let error =
            validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &hot_state,
            ))
            .await
            .expect_err(
                "foreign key target in another committed branch should not satisfy this branch",
            );

        assert_eq!(error.code, LixError::CODE_FOREIGN_KEY);
    }

    #[tokio::test]
    async fn validation_rejects_foreign_key_target_tombstoned_by_transaction() {
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let mut parent_delete = fk_parent_row("parent-1", "01920000-0000-7000-8000-0000000000a1");
        parent_delete.snapshot = None;
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![
                parent_delete,
                fk_child_row(
                    "child-1",
                    "parent-1",
                    "01920000-0000-7000-8000-0000000000a1",
                ),
            ],
            ..empty_staged_write_set()
        };
        let hot_state = StaticHotStateReader {
            rows: vec![MaterializedHotStateRow::from(fk_parent_row(
                "parent-1",
                "01920000-0000-7000-8000-0000000000a1",
            ))],
        };

        let error =
            validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &hot_state,
            ))
            .await
            .expect_err("same-transaction tombstone should hide the committed FK target");

        assert_eq!(error.code, LixError::CODE_FOREIGN_KEY);
    }

    #[tokio::test]
    async fn validation_allows_tracked_fk_target_when_untracked_tombstone_shadows_same_identity() {
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let mut untracked_parent_delete =
            fk_parent_row("parent-1", "01920000-0000-7000-8000-0000000000a1");
        untracked_parent_delete.snapshot = None;
        mark_prepared_row_untracked(&mut untracked_parent_delete);
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![
                untracked_parent_delete,
                fk_child_row(
                    "child-1",
                    "parent-1",
                    "01920000-0000-7000-8000-0000000000a1",
                ),
            ],
            ..empty_staged_write_set()
        };
        let hot_state = StaticHotStateReader {
            rows: vec![MaterializedHotStateRow::from(fk_parent_row(
                "parent-1",
                "01920000-0000-7000-8000-0000000000a1",
            ))],
        };

        validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
            &staged_writes,
            &visible_schemas,
            &hot_state,
        ))
        .await
        .expect("untracked tombstone must not hide tracked FK target");
    }

    #[tokio::test]
    async fn validation_rejects_pending_reference_to_deleted_identity() {
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let mut parent_delete = fk_parent_row("parent-1", "01920000-0000-7000-8000-0000000000a1");
        parent_delete.snapshot = None;
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![
                parent_delete,
                fk_child_row(
                    "child-1",
                    "parent-1",
                    "01920000-0000-7000-8000-0000000000a1",
                ),
            ],
            ..empty_staged_write_set()
        };
        let hot_state = StaticHotStateReader {
            rows: vec![MaterializedHotStateRow::from(fk_parent_row(
                "parent-1",
                "01920000-0000-7000-8000-0000000000a1",
            ))],
        };

        let error =
            validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &hot_state,
            ))
            .await
            .expect_err("pending child reference should block parent delete");

        assert_eq!(error.code, LixError::CODE_FOREIGN_KEY);
    }

    #[tokio::test]
    async fn validation_allows_delete_with_pending_reference_in_different_branch() {
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let mut parent_delete = fk_parent_row("parent-1", "01920000-0000-7000-8000-0000000000a1");
        parent_delete.snapshot = None;
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![
                parent_delete,
                fk_parent_row("parent-1", "01920000-0000-7000-8000-0000000000b1"),
                fk_child_row(
                    "child-1",
                    "parent-1",
                    "01920000-0000-7000-8000-0000000000b1",
                ),
            ],
            ..empty_staged_write_set()
        };

        validate_prepared_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect("pending references in another branch should not block this delete");
    }

    #[tokio::test]
    async fn validation_rejects_delete_when_same_branch_reference_exists() {
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let mut parent_delete = fk_parent_row("parent-1", "01920000-0000-7000-8000-0000000000a1");
        parent_delete.snapshot = None;
        let hot_state = StaticHotStateReader {
            rows: vec![
                MaterializedHotStateRow::from(fk_parent_row(
                    "parent-1",
                    "01920000-0000-7000-8000-0000000000a1",
                )),
                MaterializedHotStateRow::from(fk_child_row(
                    "child-1",
                    "parent-1",
                    "01920000-0000-7000-8000-0000000000a1",
                )),
            ],
        };
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![parent_delete],
            ..empty_staged_write_set()
        };

        let error =
            validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &hot_state,
            ))
            .await
            .expect_err("delete should be restricted by same-branch references");

        assert_eq!(error.code, LixError::CODE_FOREIGN_KEY);
    }

    #[tokio::test]
    async fn validation_batches_committed_delete_reference_scans_by_constraint_scope() {
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let branch_id = "01920000-0000-7000-8000-0000000000a1";
        let mut parent_one_delete = fk_parent_row("parent-1", branch_id);
        parent_one_delete.snapshot = None;
        let mut parent_two_delete = fk_parent_row("parent-2", branch_id);
        parent_two_delete.snapshot = None;
        let hot_state = CountingStaticHotStateReader {
            rows: vec![
                MaterializedHotStateRow::from(fk_parent_row("parent-1", branch_id)),
                MaterializedHotStateRow::from(fk_parent_row("parent-2", branch_id)),
                MaterializedHotStateRow::from(fk_child_row("child-1", "parent-2", branch_id)),
            ],
            scan_count: AtomicUsize::new(0),
        };
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![parent_one_delete, parent_two_delete],
            ..empty_staged_write_set()
        };
        let validation_set = staged_writes.validation_set_for_tests();
        let catalog = CatalogSnapshot::from_visible_schemas(&visible_schemas)
            .expect("foreign-key schemas should compile");
        let mut pending_constraints = PendingConstraintIndexes::default();
        for row in validation_set.rows() {
            pending_constraints.remember_tombstone(row);
        }
        let input = TransactionValidationInput::new(&validation_set, &catalog, &hot_state);

        let error = validate_committed_delete_restrictions(&input, &catalog, &pending_constraints)
            .await
            .expect_err("either deleted target must still reject a committed reference");

        assert_eq!(error.code, LixError::CODE_FOREIGN_KEY);
        assert_eq!(
            hot_state.scan_count.load(Ordering::Relaxed),
            3,
            "two target point loads should share the committed source-schema scan"
        );
    }

    fn file_descriptor_row_in_directory(
        file_id: &str,
        directory_id: Option<&str>,
        name: &str,
        branch_id: &str,
    ) -> TestPreparedStateRow {
        let mut row = staged_row(
            FILE_DESCRIPTOR_SCHEMA_KEY,
            Some(
                json!({
                    "id": file_id,
                    "directory_id": directory_id,
                    "name": name,
                })
                .to_string(),
            ),
        );
        row.entity_pk =
            EntityPk::uuid_from_canonical(file_id).expect("fixture file ID should be a UUID");
        row.file_id = Some(file_id.into());
        row.branch_id = branch_id.into();
        row.global = branch_id == crate::GLOBAL_BRANCH_ID;
        row
    }

    const FK_SCOPE_BRANCH_ID: &str = "01920000-0000-7000-8000-0000000000a1";
    const FK_SCOPE_DIRECTORY_ID: &str = "01920000-0000-7000-8000-0000000000d1";
    const FK_SCOPE_CHILD_DIRECTORY_ID: &str = "01920000-0000-7000-8000-0000000000d2";
    const FK_SCOPE_FILE_ID: &str = "01920000-0000-7000-8000-0000000000f1";

    async fn committed_delete_restriction_outcome(
        staged_rows: Vec<TestPreparedStateRow>,
        committed_rows: Vec<MaterializedHotStateRow>,
    ) -> Result<(), LixError> {
        let staged_writes = PreparedWriteSet {
            state_rows: PreparedStateBatch::from_test_rows(staged_rows),
            ..empty_staged_write_set()
        };
        let validation_set = staged_writes.validation_set_for_tests();
        let catalog = CatalogSnapshot::from_visible_schemas(&[
            file_descriptor_schema(),
            directory_descriptor_schema(),
        ])
        .expect("descriptor schemas should compile");
        let hot_state = StrictStaticHotStateReader {
            rows: committed_rows,
        };
        let mut pending_constraints = PendingConstraintIndexes::default();
        for row in validation_set.rows() {
            if row.snapshot_json().is_none() {
                pending_constraints.remember_tombstone(row);
            }
        }
        let input = TransactionValidationInput::new(&validation_set, &catalog, &hot_state);
        validate_committed_delete_restrictions(&input, &catalog, &pending_constraints).await
    }

    async fn directory_delete_with_committed_file_child_outcome(
        untracked: bool,
    ) -> Result<(), LixError> {
        let mut directory_delete =
            directory_descriptor_row(FK_SCOPE_DIRECTORY_ID, None, "docs", FK_SCOPE_BRANCH_ID);
        directory_delete.snapshot = None;
        let mut committed_directory =
            directory_descriptor_row(FK_SCOPE_DIRECTORY_ID, None, "docs", FK_SCOPE_BRANCH_ID);
        let mut committed_file = file_descriptor_row_in_directory(
            FK_SCOPE_FILE_ID,
            Some(FK_SCOPE_DIRECTORY_ID),
            "readme.md",
            FK_SCOPE_BRANCH_ID,
        );
        if untracked {
            mark_prepared_row_untracked(&mut directory_delete);
            mark_prepared_row_untracked(&mut committed_directory);
            mark_prepared_row_untracked(&mut committed_file);
        }
        committed_delete_restriction_outcome(
            vec![directory_delete],
            vec![
                MaterializedHotStateRow::from(committed_directory),
                MaterializedHotStateRow::from(committed_file),
            ],
        )
        .await
    }

    /// Control: the same declared restriction on a same-file-scope pair does
    /// fire, so the batch machinery itself is alive.
    #[tokio::test]
    async fn committed_delete_restriction_rejects_child_directory_of_deleted_directory() {
        let mut directory_delete =
            directory_descriptor_row(FK_SCOPE_DIRECTORY_ID, None, "docs", FK_SCOPE_BRANCH_ID);
        directory_delete.snapshot = None;
        let committed_directory =
            directory_descriptor_row(FK_SCOPE_DIRECTORY_ID, None, "docs", FK_SCOPE_BRANCH_ID);
        let committed_child = directory_descriptor_row(
            FK_SCOPE_CHILD_DIRECTORY_ID,
            Some(FK_SCOPE_DIRECTORY_ID),
            "guides",
            FK_SCOPE_BRANCH_ID,
        );

        let error = committed_delete_restriction_outcome(
            vec![directory_delete],
            vec![
                MaterializedHotStateRow::from(committed_directory),
                MaterializedHotStateRow::from(committed_child),
            ],
        )
        .await
        .expect_err("committed child directory must block its parent's delete");

        assert_eq!(error.code, LixError::CODE_FOREIGN_KEY);
    }

    /// A file descriptor row lives in file scope `Exact(Some(own id))` while
    /// the directory it references lives in `Exact(None)`. Before
    /// `delete_restriction_source_domains`, the committed delete side scanned
    /// only `file_id IS NULL` and this declared foreign key could never fire.
    #[tokio::test]
    async fn committed_delete_restriction_rejects_tracked_file_child_of_deleted_directory() {
        let error = directory_delete_with_committed_file_child_outcome(false)
            .await
            .expect_err("tracked lane: committed file child must block the directory delete");

        assert_eq!(error.code, LixError::CODE_FOREIGN_KEY);
    }

    #[tokio::test]
    async fn committed_delete_restriction_rejects_untracked_file_child_of_deleted_directory() {
        let error = directory_delete_with_committed_file_child_outcome(true)
            .await
            .expect_err("untracked lane: committed file child must block the directory delete");

        assert_eq!(error.code, LixError::CODE_FOREIGN_KEY);
    }

    /// The mirror must not reach across branches, and must not fire for a
    /// file descriptor that sits in a different directory.
    #[tokio::test]
    async fn committed_delete_restriction_allows_directory_delete_without_file_children() {
        let other_directory = "01920000-0000-7000-8000-0000000000d8";
        let mut directory_delete =
            directory_descriptor_row(FK_SCOPE_DIRECTORY_ID, None, "docs", FK_SCOPE_BRANCH_ID);
        directory_delete.snapshot = None;

        let outcome = committed_delete_restriction_outcome(
            vec![directory_delete],
            vec![
                MaterializedHotStateRow::from(directory_descriptor_row(
                    FK_SCOPE_DIRECTORY_ID,
                    None,
                    "docs",
                    FK_SCOPE_BRANCH_ID,
                )),
                // same branch, different parent directory
                MaterializedHotStateRow::from(file_descriptor_row_in_directory(
                    FK_SCOPE_FILE_ID,
                    Some(other_directory),
                    "readme.md",
                    FK_SCOPE_BRANCH_ID,
                )),
                // same directory id, different branch
                MaterializedHotStateRow::from(file_descriptor_row_in_directory(
                    "01920000-0000-7000-8000-0000000000f2",
                    Some(FK_SCOPE_DIRECTORY_ID),
                    "readme.md",
                    "01920000-0000-7000-8000-0000000000b1",
                )),
            ],
        )
        .await;

        assert!(
            outcome.is_ok(),
            "unrelated file descriptors must not block the delete, got {outcome:?}"
        );
    }

    /// A file descriptor staged as a tombstone in the same transaction is not
    /// a live reference, so deleting its directory alongside it is legal --
    /// this is the shape the recursive directory-delete planner produces.
    #[tokio::test]
    async fn committed_delete_restriction_allows_directory_delete_with_staged_file_tombstone() {
        let mut directory_delete =
            directory_descriptor_row(FK_SCOPE_DIRECTORY_ID, None, "docs", FK_SCOPE_BRANCH_ID);
        directory_delete.snapshot = None;
        let mut file_delete = file_descriptor_row_in_directory(
            FK_SCOPE_FILE_ID,
            Some(FK_SCOPE_DIRECTORY_ID),
            "readme.md",
            FK_SCOPE_BRANCH_ID,
        );
        file_delete.snapshot = None;

        let outcome = committed_delete_restriction_outcome(
            vec![directory_delete, file_delete],
            vec![
                MaterializedHotStateRow::from(directory_descriptor_row(
                    FK_SCOPE_DIRECTORY_ID,
                    None,
                    "docs",
                    FK_SCOPE_BRANCH_ID,
                )),
                MaterializedHotStateRow::from(file_descriptor_row_in_directory(
                    FK_SCOPE_FILE_ID,
                    Some(FK_SCOPE_DIRECTORY_ID),
                    "readme.md",
                    FK_SCOPE_BRANCH_ID,
                )),
            ],
        )
        .await;

        assert!(
            outcome.is_ok(),
            "a cascading recursive delete must stay legal, got {outcome:?}"
        );
    }

    fn fk_row_in_file(mut row: TestPreparedStateRow, file_id: &str) -> MaterializedHotStateRow {
        row.file_id = Some(file_id.into());
        MaterializedHotStateRow::from(row)
    }

    /// Simulates the "just widen the delete-side source domain" fix by running
    /// the committed batch with an `Any` file scope, against a corpus where the
    /// same target primary key legitimately exists in two file scopes.
    async fn widened_source_domain_probe(source_domain: Domain) -> Result<(), LixError> {
        let branch_id = "01920000-0000-7000-8000-0000000000a1";
        let file_f = "01920000-0000-7000-8000-0000000000f0";
        let file_g = "01920000-0000-7000-8000-0000000000f9";

        let hot_state = StrictStaticHotStateReader {
            rows: vec![
                fk_row_in_file(fk_parent_row("parent-1", branch_id), file_f),
                fk_row_in_file(fk_parent_row("parent-1", branch_id), file_g),
                fk_row_in_file(fk_child_row("child-1", "parent-1", branch_id), file_g),
            ],
        };
        let catalog =
            CatalogSnapshot::from_visible_schemas(&[fk_parent_schema(), fk_child_schema()])
                .expect("fk schemas should compile");
        let reference = catalog
            .delete_plan_for_key("fk_parent_schema")
            .foreign_key_references
            .first()
            .expect("fk_parent_schema is referenced by fk_child_schema")
            .clone();

        // Delete `parent-1` in file F only. `child-1` lives in file G and is
        // satisfied by the `parent-1` row that remains in file G.
        let deleted_identity = DomainRowIdentity::in_domain(
            Domain::exact_file(branch_id.to_string(), false, Some(file_f.to_string())),
            "fk_parent_schema",
            EntityPk::single("parent-1"),
        );
        let batches = BTreeMap::from([(
            NormalDeleteRestrictionBatchKey {
                source_key: reference.source_key.clone(),
                source_domain,
                local_properties: reference.foreign_key.local_properties.clone(),
            },
            BTreeMap::from([(
                UniqueConstraintValue::from_snapshot(
                    &json!({ "id": "parent-1" }),
                    &reference.foreign_key.referenced_properties,
                )
                .expect("referenced value should encode"),
                vec![deleted_identity],
            )]),
        )]);

        validate_committed_normal_delete_restriction_batches(
            &hot_state,
            &PendingConstraintIndexes::default(),
            batches,
        )
        .await
    }

    #[tokio::test]
    async fn widening_delete_source_domain_to_any_file_falsely_rejects() {
        let branch_id = "01920000-0000-7000-8000-0000000000a1";
        let file_f = "01920000-0000-7000-8000-0000000000f0";

        let narrow = widened_source_domain_probe(Domain::exact_file(
            branch_id.to_string(),
            false,
            Some(file_f.to_string()),
        ))
        .await;
        assert!(
            narrow.is_ok(),
            "today's exact file scope correctly allows this delete, got {narrow:?}"
        );

        let widened =
            widened_source_domain_probe(Domain::any_file(branch_id.to_string(), false)).await;
        let error = widened.expect_err(
            "Domain::any_file conflates same-valued keys across file scopes and must reject",
        );
        assert_eq!(error.code, LixError::CODE_FOREIGN_KEY);
    }

    fn pending_delete_restriction_probe(
        visible_schemas: &[JsonValue],
        staged_rows: Vec<TestPreparedStateRow>,
    ) -> Result<(), LixError> {
        let staged_writes = PreparedWriteSet {
            state_rows: PreparedStateBatch::from_test_rows(staged_rows),
            ..empty_staged_write_set()
        };
        let validation_set = staged_writes.validation_set_for_tests();
        let catalog = CatalogSnapshot::from_visible_schemas(visible_schemas)
            .expect("probe schemas should compile");
        let mut pending_constraints = PendingConstraintIndexes::default();
        for row in validation_set.rows() {
            match row.snapshot_json() {
                Some(snapshot) => {
                    let (_, schema_plan) = catalog
                        .plan_for_key(row.schema_key())
                        .expect("probe schema plan should resolve");
                    pending_constraints
                        .remember_foreign_key_references(row, schema_plan, snapshot)
                        .expect("probe foreign-key references should index");
                }
                None => pending_constraints.remember_tombstone(row),
            }
        }
        validate_pending_delete_restrictions(&catalog, &pending_constraints)
    }

    /// `UniqueConstraintValue::from_entity_pk` renders a component with
    /// `format!("{:?}", JsonValue)` while `from_snapshot*` renders the inner
    /// value. The two encodings can never compare equal for a string or UUID
    /// primary key, and `validate_pending_delete_restrictions` is the only
    /// caller of `from_entity_pk`.
    #[test]
    fn pending_delete_restriction_value_encodings_do_not_agree() {
        let entity_pk = EntityPk::single("parent-1");
        let snapshot = json!({ "id": "parent-1" });
        let pointer_group = vec![vec!["id".to_string()]];

        let from_identity = UniqueConstraintValue::from_entity_pk(&entity_pk);
        let from_snapshot = UniqueConstraintValue::from_snapshot(&snapshot, &pointer_group)
            .expect("snapshot value should encode");

        assert_eq!(from_identity.0, vec!["String(\"parent-1\")".to_string()]);
        assert_eq!(from_snapshot.0, vec!["\"parent-1\"".to_string()]);
        assert_ne!(from_identity, from_snapshot);
    }

    /// Consequence of the encoding mismatch: the pending lane never fires,
    /// for a plain user schema pair with no file-scope subtlety at all.
    #[test]
    fn pending_delete_restriction_scope_probe_generic_schema_pair() {
        let branch_id = "01920000-0000-7000-8000-0000000000a1";
        let mut parent_delete = fk_parent_row("parent-1", branch_id);
        parent_delete.snapshot = None;

        let outcome = pending_delete_restriction_probe(
            &[fk_parent_schema(), fk_child_schema()],
            vec![
                parent_delete,
                fk_child_row("child-1", "parent-1", branch_id),
            ],
        );

        assert!(
            outcome.is_ok(),
            "generic pair: expected the pending restriction to be unreachable, got {outcome:?}"
        );
    }

    #[test]
    fn pending_delete_restriction_scope_probe_staged_file_child() {
        let mut directory_delete =
            directory_descriptor_row(FK_SCOPE_DIRECTORY_ID, None, "docs", FK_SCOPE_BRANCH_ID);
        directory_delete.snapshot = None;
        let staged_file = file_descriptor_row_in_directory(
            FK_SCOPE_FILE_ID,
            Some(FK_SCOPE_DIRECTORY_ID),
            "readme.md",
            FK_SCOPE_BRANCH_ID,
        );

        let outcome = pending_delete_restriction_probe(
            &[file_descriptor_schema(), directory_descriptor_schema()],
            vec![directory_delete, staged_file],
        );

        assert!(
            outcome.is_ok(),
            "descriptor pair: expected the pending restriction to be unreachable, got {outcome:?}"
        );
    }

    #[tokio::test]
    async fn validation_allows_delete_when_only_different_branch_reference_exists() {
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let mut parent_delete = fk_parent_row("parent-1", "01920000-0000-7000-8000-0000000000a1");
        parent_delete.snapshot = None;
        let hot_state = StaticHotStateReader {
            rows: vec![
                MaterializedHotStateRow::from(fk_parent_row(
                    "parent-1",
                    "01920000-0000-7000-8000-0000000000a1",
                )),
                MaterializedHotStateRow::from(fk_child_row(
                    "child-1",
                    "parent-1",
                    "01920000-0000-7000-8000-0000000000b1",
                )),
            ],
        };
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![parent_delete],
            ..empty_staged_write_set()
        };

        validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
            &staged_writes,
            &visible_schemas,
            &hot_state,
        ))
        .await
        .expect("references in another branch should not restrict this branch");
    }

    #[tokio::test]
    async fn validation_allows_delete_when_committed_reference_is_also_deleted() {
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let mut parent_delete = fk_parent_row("parent-1", "01920000-0000-7000-8000-0000000000a1");
        parent_delete.snapshot = None;
        let mut child_delete = fk_child_row(
            "child-1",
            "parent-1",
            "01920000-0000-7000-8000-0000000000a1",
        );
        child_delete.snapshot = None;
        let hot_state = StaticHotStateReader {
            rows: vec![
                MaterializedHotStateRow::from(fk_parent_row(
                    "parent-1",
                    "01920000-0000-7000-8000-0000000000a1",
                )),
                MaterializedHotStateRow::from(fk_child_row(
                    "child-1",
                    "parent-1",
                    "01920000-0000-7000-8000-0000000000a1",
                )),
            ],
        };
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![parent_delete, child_delete],
            ..empty_staged_write_set()
        };

        validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
            &staged_writes,
            &visible_schemas,
            &hot_state,
        ))
        .await
        .expect("committed references deleted in the same transaction should not restrict delete");
    }

    #[tokio::test]
    async fn validation_allows_delete_when_committed_reference_is_replaced() {
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let branch_id = "01920000-0000-7000-8000-0000000000a1";
        let mut parent_delete = fk_parent_row("parent-1", branch_id);
        parent_delete.snapshot = None;
        let child_update = fk_child_row("child-1", "parent-2", branch_id);
        let hot_state = StaticHotStateReader {
            rows: vec![
                MaterializedHotStateRow::from(fk_parent_row("parent-1", branch_id)),
                MaterializedHotStateRow::from(fk_parent_row("parent-2", branch_id)),
                MaterializedHotStateRow::from(fk_child_row("child-1", "parent-1", branch_id)),
            ],
        };
        let staged_writes = PreparedWriteSet {
            state_rows: prepared_rows![parent_delete, child_update],
            ..empty_staged_write_set()
        };

        validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
            &staged_writes,
            &visible_schemas,
            &hot_state,
        ))
        .await
        .expect("a replacement row's final foreign keys should supersede its committed references");
    }

    #[test]
    fn schema_catalog_plans_include_compiled_schema() {
        let visible_schemas = vec![key_value_schema()];
        let staged_writes = empty_staged_write_set();
        let input = validation_input(&staged_writes, &visible_schemas);
        let catalog = catalog_from_transaction_input(&input).expect("schema catalog should build");
        let plan = catalog
            .plan_for_key("lix_key_value")
            .expect("lix_key_value plan should exist");

        assert!(
            plan.1
                .compiled_schema
                .validate(&json!({ "key": "k", "value": "v" }))
                .is_ok()
        );
    }

    #[test]
    fn pending_indexes_record_primary_key_fk_targets_by_exact_scope() {
        let mut indexes = PendingConstraintIndexes::default();
        let row = fk_parent_row("parent-1", "01920000-0000-7000-8000-0000000000a1");
        let snapshot = serde_json::from_str::<JsonValue>(
            row.snapshot
                .as_ref()
                .map(|snapshot| snapshot.normalized())
                .expect("fixture should have snapshot"),
        )
        .expect("fixture JSON should parse");

        indexes
            .remember_row(
                PreparedValidationRow::State(row.borrowed()),
                test_plan_from_schema(fk_parent_schema()),
                &snapshot,
            )
            .expect("parent row should index");

        assert!(
            indexes
                .has_fk_target(
                    "fk_parent_schema",
                    "01920000-0000-7000-8000-0000000000a1",
                    Some("01920000-0000-7000-8000-0000000000a2"),
                    &["/id"],
                    UniqueConstraintValue::string_values(["parent-1"]),
                )
                .expect("lookup should build")
        );
        assert!(
            !indexes
                .has_fk_target(
                    "fk_parent_schema",
                    "01920000-0000-7000-8000-0000000000b1",
                    Some("01920000-0000-7000-8000-0000000000a2"),
                    &["/id"],
                    UniqueConstraintValue::string_values(["parent-1"]),
                )
                .expect("lookup should build")
        );
    }

    #[test]
    fn pending_indexes_record_unique_fk_targets_by_exact_scope() {
        let mut indexes = PendingConstraintIndexes::default();
        let row = unique_row("post-1", "hello-world", "first");
        let snapshot = serde_json::from_str::<JsonValue>(
            row.snapshot
                .as_ref()
                .map(|snapshot| snapshot.normalized())
                .expect("fixture should have snapshot"),
        )
        .expect("fixture JSON should parse");

        indexes
            .remember_row(
                PreparedValidationRow::State(row.borrowed()),
                test_plan_from_schema(unique_schema()),
                &snapshot,
            )
            .expect("unique row should index");

        assert!(
            indexes
                .has_fk_target(
                    "unique_schema",
                    "01920000-0000-7000-8000-0000000000a1",
                    Some("01920000-0000-7000-8000-0000000000a2"),
                    &["/slug"],
                    UniqueConstraintValue::string_values(["hello-world"]),
                )
                .expect("lookup should build")
        );
    }

    #[test]
    fn pending_indexes_record_normal_fk_references_by_exact_scope() {
        let mut indexes = PendingConstraintIndexes::default();
        let row = fk_child_row(
            "child-1",
            "parent-1",
            "01920000-0000-7000-8000-0000000000a1",
        );
        let snapshot = serde_json::from_str::<JsonValue>(
            row.snapshot
                .as_ref()
                .map(|snapshot| snapshot.normalized())
                .expect("fixture should have snapshot"),
        )
        .expect("fixture JSON should parse");
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let staged_writes = empty_staged_write_set();
        let input = validation_input(&staged_writes, &visible_schemas);
        let _catalog = catalog_from_transaction_input(&input).expect("catalog should build");

        indexes
            .remember_foreign_key_references(
                PreparedValidationRow::State(row.borrowed()),
                test_plan_from_schema(fk_child_schema()),
                &snapshot,
            )
            .expect("child row should index FK reference");

        assert!(
            indexes
                .has_fk_reference_to_key(
                    "fk_parent_schema",
                    "01920000-0000-7000-8000-0000000000a1",
                    Some("01920000-0000-7000-8000-0000000000a2"),
                    &["/id"],
                    UniqueConstraintValue::string_values(["parent-1"]),
                )
                .expect("lookup should build")
        );
        assert!(
            !indexes
                .has_fk_reference_to_key(
                    "fk_parent_schema",
                    "01920000-0000-7000-8000-0000000000b1",
                    Some("01920000-0000-7000-8000-0000000000a2"),
                    &["/id"],
                    UniqueConstraintValue::string_values(["parent-1"]),
                )
                .expect("lookup should build")
        );
    }

    #[test]
    fn pending_indexes_match_tombstones_by_exact_committed_identity() {
        let mut indexes = PendingConstraintIndexes::default();
        let branch_id = "01920000-0000-7000-8000-0000000000a1";
        let mut deleted = fk_child_row("child-1", "parent-1", branch_id);
        deleted.snapshot = None;
        indexes.remember_tombstone(PreparedValidationRow::State(deleted.borrowed()));

        let committed =
            MaterializedHotStateRow::from(fk_child_row("child-1", "parent-1", branch_id));
        let mut other_file = committed.clone();
        other_file.file_id = Some("01920000-0000-7000-8000-0000000000b1".into());
        let mut malformed_projection = committed.clone();
        malformed_projection.global = true;
        let batch = MaterializedHotStateBatch::from_rows(vec![
            committed,
            other_file,
            malformed_projection,
        ]);

        assert!(indexes.tombstones_identity(batch.row(0)));
        assert!(!indexes.tombstones_identity(batch.row(1)));
        assert!(!indexes.tombstones_identity(batch.row(2)));
    }

    #[test]
    fn pending_delete_restrictions_ignore_tombstoned_referencing_rows() {
        let mut indexes = PendingConstraintIndexes::default();
        let mut parent_delete = fk_parent_row("parent-1", "01920000-0000-7000-8000-0000000000a1");
        parent_delete.snapshot = None;
        indexes.remember_tombstone(PreparedValidationRow::State(parent_delete.borrowed()));

        let child = fk_child_row(
            "child-1",
            "parent-1",
            "01920000-0000-7000-8000-0000000000a1",
        );
        let child_snapshot = serde_json::from_str::<JsonValue>(
            child
                .snapshot
                .as_ref()
                .map(|snapshot| snapshot.normalized())
                .expect("fixture should have snapshot"),
        )
        .expect("fixture JSON should parse");
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let staged_writes = empty_staged_write_set();
        let input = validation_input(&staged_writes, &visible_schemas);
        let catalog = catalog_from_transaction_input(&input).expect("catalog should build");
        indexes
            .remember_foreign_key_references(
                PreparedValidationRow::State(child.borrowed()),
                test_plan_from_schema(fk_child_schema()),
                &child_snapshot,
            )
            .expect("child row should index FK reference");

        let mut child_delete = fk_child_row(
            "child-1",
            "parent-1",
            "01920000-0000-7000-8000-0000000000a1",
        );
        child_delete.snapshot = None;
        indexes.remember_tombstone(PreparedValidationRow::State(child_delete.borrowed()));

        validate_pending_delete_restrictions(&catalog, &indexes)
            .expect("a row deleted in the same transaction should not block target delete");
    }

    #[test]
    fn pending_fk_validation_collects_unresolved_normal_fk_check() {
        let indexes = PendingConstraintIndexes::default();
        let row = fk_child_row(
            "child-1",
            "parent-1",
            "01920000-0000-7000-8000-0000000000a1",
        );
        let snapshot = serde_json::from_str::<JsonValue>(
            row.snapshot
                .as_ref()
                .map(|snapshot| snapshot.normalized())
                .expect("fixture should have snapshot"),
        )
        .expect("fixture JSON should parse");
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let staged_writes = empty_staged_write_set();
        let input = validation_input(&staged_writes, &visible_schemas);
        let _catalog = catalog_from_transaction_input(&input).expect("catalog should build");

        let unresolved = validate_pending_foreign_keys(
            &input,
            &indexes,
            &[(
                PreparedValidationRow::State(row.borrowed()),
                test_plan_from_schema(fk_child_schema()),
                &snapshot,
            )],
        )
        .expect("FK validation should collect unresolved checks");

        assert_eq!(unresolved.len(), 1);
        assert_eq!(
            unresolved[0].source_identity,
            DomainRowIdentity::exact(
                "01920000-0000-7000-8000-0000000000a1",
                false,
                Some("01920000-0000-7000-8000-0000000000a2".to_string()),
                "fk_child_schema",
                EntityPk::single("child-1"),
            )
        );
        assert_eq!(unresolved[0].source_schema_key, "fk_child_schema");
        assert_eq!(
            unresolved[0].source_pointer_group,
            vec![vec!["parent_id".to_string()]]
        );
        let target = &unresolved[0].target;
        assert_eq!(target.schema_key, "fk_parent_schema");
        assert_eq!(
            target.domain.branch_id(),
            "01920000-0000-7000-8000-0000000000a1"
        );
        assert_eq!(
            target.domain.file_scope(),
            &DomainFileScope::Exact(Some("01920000-0000-7000-8000-0000000000a2".to_string()))
        );
        assert_eq!(target.pointer_group, vec![vec!["id".to_string()]]);
        assert_eq!(
            target.value,
            UniqueConstraintValue::string_values(["parent-1"])
        );
    }

    #[test]
    fn primary_key_fk_targets_have_exact_entity_pk_filters_but_unique_targets_do_not() {
        let catalog = CatalogSnapshot::from_visible_schemas(&[unique_schema()])
            .expect("unique schema catalog should build");
        let mut target = PendingForeignKeyTargetKey {
            schema_key: "unique_schema".to_string(),
            domain: Domain::exact_file(
                "01920000-0000-7000-8000-0000000000a1",
                false,
                Some("01920000-0000-7000-8000-0000000000a2".to_string()),
            ),
            pointer_group: vec![vec!["id".to_string()]],
            value: UniqueConstraintValue::string_values(["entity-1"]),
        };

        assert_eq!(
            primary_key_entity_pk_for_target(&catalog, &target),
            Some(EntityPk::single("entity-1"))
        );

        target.pointer_group = vec![vec!["slug".to_string()]];
        assert_eq!(
            primary_key_entity_pk_for_target(&catalog, &target),
            None,
            "non-primary unique constraints still require value scans"
        );
    }

    #[test]
    fn pending_fk_validation_resolves_normal_fk_against_pending_target() {
        let mut indexes = PendingConstraintIndexes::default();
        let parent = fk_parent_row("parent-1", "01920000-0000-7000-8000-0000000000a1");
        let parent_snapshot = serde_json::from_str::<JsonValue>(
            parent
                .snapshot
                .as_ref()
                .map(|snapshot| snapshot.normalized())
                .expect("fixture should have snapshot"),
        )
        .expect("fixture JSON should parse");
        indexes
            .remember_row(
                PreparedValidationRow::State(parent.borrowed()),
                test_plan_from_schema(fk_parent_schema()),
                &parent_snapshot,
            )
            .expect("parent should index as pending FK target");

        let child = fk_child_row(
            "child-1",
            "parent-1",
            "01920000-0000-7000-8000-0000000000a1",
        );
        let child_snapshot = serde_json::from_str::<JsonValue>(
            child
                .snapshot
                .as_ref()
                .map(|snapshot| snapshot.normalized())
                .expect("fixture should have snapshot"),
        )
        .expect("fixture JSON should parse");
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let staged_writes = empty_staged_write_set();
        let input = validation_input(&staged_writes, &visible_schemas);
        let _catalog = catalog_from_transaction_input(&input).expect("catalog should build");

        let unresolved = validate_pending_foreign_keys(
            &input,
            &indexes,
            &[(
                PreparedValidationRow::State(child.borrowed()),
                test_plan_from_schema(fk_child_schema()),
                &child_snapshot,
            )],
        )
        .expect("FK validation should inspect pending targets");

        assert!(
            unresolved.is_empty(),
            "same-branch pending parent should satisfy the child FK"
        );
    }

    #[test]
    fn pending_fk_validation_keeps_normal_fk_unresolved_across_branches() {
        let mut indexes = PendingConstraintIndexes::default();
        let parent = fk_parent_row("parent-1", "01920000-0000-7000-8000-0000000000b1");
        let parent_snapshot = serde_json::from_str::<JsonValue>(
            parent
                .snapshot
                .as_ref()
                .map(|snapshot| snapshot.normalized())
                .expect("fixture should have snapshot"),
        )
        .expect("fixture JSON should parse");
        indexes
            .remember_row(
                PreparedValidationRow::State(parent.borrowed()),
                test_plan_from_schema(fk_parent_schema()),
                &parent_snapshot,
            )
            .expect("parent should index as pending FK target");

        let child = fk_child_row(
            "child-1",
            "parent-1",
            "01920000-0000-7000-8000-0000000000a1",
        );
        let child_snapshot = serde_json::from_str::<JsonValue>(
            child
                .snapshot
                .as_ref()
                .map(|snapshot| snapshot.normalized())
                .expect("fixture should have snapshot"),
        )
        .expect("fixture JSON should parse");
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let staged_writes = empty_staged_write_set();
        let input = validation_input(&staged_writes, &visible_schemas);
        let _catalog = catalog_from_transaction_input(&input).expect("catalog should build");

        let unresolved = validate_pending_foreign_keys(
            &input,
            &indexes,
            &[(
                PreparedValidationRow::State(child.borrowed()),
                test_plan_from_schema(fk_child_schema()),
                &child_snapshot,
            )],
        )
        .expect("FK validation should inspect pending targets");

        assert_eq!(unresolved.len(), 1);
        let target = &unresolved[0].target;
        assert_eq!(
            target.domain.branch_id(),
            "01920000-0000-7000-8000-0000000000a1",
            "FK checks are exact-branch scoped, not overlay scoped"
        );
    }

    #[tokio::test]
    async fn committed_fk_lookup_resolves_normal_fk_in_exact_scope() {
        let indexes = PendingConstraintIndexes::default();
        let child = fk_child_row(
            "child-1",
            "parent-1",
            "01920000-0000-7000-8000-0000000000a1",
        );
        let child_snapshot = serde_json::from_str::<JsonValue>(
            child
                .snapshot
                .as_ref()
                .map(|snapshot| snapshot.normalized())
                .expect("fixture should have snapshot"),
        )
        .expect("fixture JSON should parse");
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let staged_writes = empty_staged_write_set();
        let input = validation_input(&staged_writes, &visible_schemas);
        let _catalog = catalog_from_transaction_input(&input).expect("catalog should build");
        let unresolved = validate_pending_foreign_keys(
            &input,
            &indexes,
            &[(
                PreparedValidationRow::State(child.borrowed()),
                test_plan_from_schema(fk_child_schema()),
                &child_snapshot,
            )],
        )
        .expect("pending FK validation should collect unresolved check");
        let hot_state = StaticHotStateReader {
            rows: vec![MaterializedHotStateRow::from(fk_parent_row(
                "parent-1",
                "01920000-0000-7000-8000-0000000000a1",
            ))],
        };

        let still_unresolved = validate_committed_foreign_keys(
            &TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &hot_state,
            ),
            &indexes,
            &unresolved,
        )
        .await
        .expect("committed FK lookup should scan live state");

        assert!(
            still_unresolved.is_empty(),
            "same-branch committed parent should satisfy unresolved FK"
        );
    }

    #[tokio::test]
    async fn committed_fk_lookup_keeps_normal_fk_unresolved_across_branches() {
        let indexes = PendingConstraintIndexes::default();
        let child = fk_child_row(
            "child-1",
            "parent-1",
            "01920000-0000-7000-8000-0000000000a1",
        );
        let child_snapshot = serde_json::from_str::<JsonValue>(
            child
                .snapshot
                .as_ref()
                .map(|snapshot| snapshot.normalized())
                .expect("fixture should have snapshot"),
        )
        .expect("fixture JSON should parse");
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let staged_writes = empty_staged_write_set();
        let input = validation_input(&staged_writes, &visible_schemas);
        let _catalog = catalog_from_transaction_input(&input).expect("catalog should build");
        let unresolved = validate_pending_foreign_keys(
            &input,
            &indexes,
            &[(
                PreparedValidationRow::State(child.borrowed()),
                test_plan_from_schema(fk_child_schema()),
                &child_snapshot,
            )],
        )
        .expect("pending FK validation should collect unresolved check");
        let hot_state = StaticHotStateReader {
            rows: vec![MaterializedHotStateRow::from(fk_parent_row(
                "parent-1",
                "01920000-0000-7000-8000-0000000000b1",
            ))],
        };

        let still_unresolved = validate_committed_foreign_keys(
            &TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &hot_state,
            ),
            &indexes,
            &unresolved,
        )
        .await
        .expect("committed FK lookup should scan live state");

        assert_eq!(
            still_unresolved.len(),
            1,
            "committed FK lookup is exact-branch scoped"
        );
    }

    fn empty_staged_write_set() -> PreparedWriteSet {
        PreparedWriteSet {
            state_rows: PreparedStateBatch::new(),
            insert_selection: crate::transaction::staging::PreparedInsertSelection::new(),
            commit_change_refs_by_branch: BTreeMap::new(),
            first_commit_parent_override_by_branch: BTreeMap::new(),
            checkpoint_publications: Vec::new(),
            extra_commit_parents_by_branch: BTreeMap::new(),
            intermediate_commits: Vec::new(),
            file_content_writes: Vec::new(),
        }
    }

    fn hot_state_row_matches_scan(
        row: &MaterializedHotStateRow,
        request: &HotStateScanRequest,
    ) -> bool {
        if request
            .filter
            .untracked
            .is_some_and(|untracked| row.untracked != untracked)
        {
            return false;
        }
        (request.filter.schema_keys.is_empty()
            || request.filter.schema_keys.contains(&row.schema_key))
            && (request.filter.branch_ids.is_empty()
                || request
                    .filter
                    .branch_ids
                    .iter()
                    .any(|branch_id| branch_id == row.branch_id.as_ref()))
            && (request.filter.file_ids.is_empty()
                || request
                    .filter
                    .file_ids
                    .iter()
                    .any(|filter| filter.matches(row.file_id.as_ref())))
    }

    fn test_file_descriptor_rows() -> Vec<MaterializedHotStateRow> {
        vec![
            committed_file_descriptor_row(
                "01920000-0000-7000-8000-0000000000a2",
                "01920000-0000-7000-8000-0000000000a1",
            ),
            committed_file_descriptor_row(
                "01920000-0000-7000-8000-0000000000a2",
                "01920000-0000-7000-8000-0000000000b1",
            ),
            committed_file_descriptor_row(
                "01920000-0000-7000-8000-0000000000b2",
                "01920000-0000-7000-8000-0000000000a1",
            ),
            committed_file_descriptor_row(
                "01920000-0000-7000-8000-0000000000b2",
                "01920000-0000-7000-8000-0000000000b1",
            ),
        ]
    }

    fn pending_registered_schema_row(schema_key: &str) -> TestPreparedStateRow {
        pending_registered_schema_from_definition(json!({
            "x-lix-key": schema_key,
            "type": "object",
            "properties": {
                "id": { "type": "string" }
            },
            "required": ["id"],
            "additionalProperties": false,
        }))
    }

    fn pending_registered_schema_from_definition(schema: JsonValue) -> TestPreparedStateRow {
        let key = schema_key_from_definition(&schema).expect("test schema should have a key");
        TestPreparedStateRow {
            schema_plan_id: crate::catalog::SchemaPlanId::for_test(0),
            facts: crate::transaction_types::PreparedRowFacts::default(),
            entity_pk: registered_schema_entity_pk(&key.schema_key),
            schema_key: REGISTERED_SCHEMA_KEY.into(),
            file_id: None,
            snapshot: Some(test_stage_json(&json!({ "value": schema }).to_string())),
            metadata: None,
            origin: None,
            origin_key: None,
            created_at: ts("2026-04-29T00:00:00.000Z"),
            updated_at: ts("2026-04-29T00:00:00.000Z"),
            global: true,
            change_id: Some(ChangeId::for_test_label("change-registered-schema")),
            commit_id: Some(CommitId::for_test_label("commit-registered-schema")),
            untracked: false,
            branch_id: crate::GLOBAL_BRANCH_ID.into(),
        }
    }

    fn registered_schema_entity_pk(schema_key: &str) -> EntityPk {
        EntityPk::from_primary_key_paths(
            &serde_json::json!({
                "value": {
                    "x-lix-key": schema_key,
                }
            }),
            &[vec!["value".to_string(), "x-lix-key".to_string()]],
        )
        .expect("registered schema identity should derive")
    }

    fn key_value_schema() -> JsonValue {
        seed_schema_definition("lix_key_value")
            .expect("lix_key_value builtin schema should exist")
            .clone()
    }

    fn registered_schema() -> JsonValue {
        seed_schema_definition(REGISTERED_SCHEMA_KEY)
            .expect("lix_registered_schema builtin schema should exist")
            .clone()
    }

    fn file_descriptor_schema() -> JsonValue {
        seed_schema_definition(FILE_DESCRIPTOR_SCHEMA_KEY)
            .expect("lix_file_descriptor builtin schema should exist")
            .clone()
    }

    fn directory_descriptor_schema() -> JsonValue {
        seed_schema_definition(DIRECTORY_DESCRIPTOR_SCHEMA_KEY)
            .expect("lix_directory_descriptor builtin schema should exist")
            .clone()
    }

    fn unique_schema() -> JsonValue {
        json!({
            "x-lix-key": "unique_schema",
            "x-lix-primary-key": ["/id"],
            "x-lix-unique": [["/slug"]],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "slug": { "type": "string" },
                "title": { "type": "string" }
            },
            "required": ["id", "slug", "title"],
            "additionalProperties": false
        })
    }

    fn nullable_unique_schema() -> JsonValue {
        json!({
            "x-lix-key": "nullable_unique_schema",
            "x-lix-primary-key": ["/id"],
            "x-lix-unique": [["/scope", "/name"]],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "scope": { "type": ["string", "null"] },
                "name": { "type": "string" }
            },
            "required": ["id", "scope", "name"],
            "additionalProperties": false
        })
    }

    fn fk_parent_schema() -> JsonValue {
        json!({
            "x-lix-key": "fk_parent_schema",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string" }
            },
            "required": ["id"],
            "additionalProperties": false
        })
    }

    fn fk_child_schema() -> JsonValue {
        json!({
            "x-lix-key": "fk_child_schema",
            "x-lix-primary-key": ["/id"],
            "x-lix-foreign-keys": [{
                "properties": ["/parent_id"],
                "references": {
                    "schemaKey": "fk_parent_schema",
                    "properties": ["/id"]
                }
            }],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "parent_id": { "type": "string" }
            },
            "required": ["id", "parent_id"],
            "additionalProperties": false
        })
    }

    fn unique_row(entity_pk: &str, slug: &str, title: &str) -> TestPreparedStateRow {
        let mut row = staged_row(
            "unique_schema",
            Some(
                json!({
                    "id": entity_pk,
                    "slug": slug,
                    "title": title,
                })
                .to_string(),
            ),
        );
        row.entity_pk = EntityPk::single(entity_pk);
        row.file_id = Some("01920000-0000-7000-8000-0000000000a2".into());
        row.branch_id = "01920000-0000-7000-8000-0000000000a1".into();
        row.global = false;
        row
    }

    fn nullable_unique_row(
        entity_pk: &str,
        scope: Option<&str>,
        name: &str,
    ) -> TestPreparedStateRow {
        let mut row = staged_row(
            "nullable_unique_schema",
            Some(
                json!({
                    "id": entity_pk,
                    "scope": scope,
                    "name": name,
                })
                .to_string(),
            ),
        );
        row.entity_pk = EntityPk::single(entity_pk);
        row.file_id = Some("01920000-0000-7000-8000-0000000000a2".into());
        row.branch_id = "01920000-0000-7000-8000-0000000000a1".into();
        row.global = false;
        row
    }

    fn fk_parent_row(entity_pk: &str, branch_id: &str) -> TestPreparedStateRow {
        let mut row = staged_row(
            "fk_parent_schema",
            Some(json!({ "id": entity_pk }).to_string()),
        );
        row.entity_pk = EntityPk::single(entity_pk);
        row.file_id = Some("01920000-0000-7000-8000-0000000000a2".into());
        row.branch_id = branch_id.into();
        row.global = false;
        row
    }

    fn fk_child_row(entity_pk: &str, parent_id: &str, branch_id: &str) -> TestPreparedStateRow {
        let mut row = staged_row(
            "fk_child_schema",
            Some(json!({ "id": entity_pk, "parent_id": parent_id }).to_string()),
        );
        row.entity_pk = EntityPk::single(entity_pk);
        row.file_id = Some("01920000-0000-7000-8000-0000000000a2".into());
        row.branch_id = branch_id.into();
        row.global = false;
        row
    }

    fn mark_prepared_row_untracked(row: &mut TestPreparedStateRow) {
        row.untracked = true;
        row.change_id = None;
        row.commit_id = None;
    }

    fn mark_live_row_untracked(row: &mut MaterializedHotStateRow) {
        row.untracked = true;
        row.change_id = None;
        row.commit_id = None;
    }

    fn staged_file_descriptor_row(file_id: &str, branch_id: &str) -> TestPreparedStateRow {
        let mut row = staged_row(
            FILE_DESCRIPTOR_SCHEMA_KEY,
            Some(
                json!({
                    "id": file_id,
                    "directory_id": null,
                    "name": file_id,
                })
                .to_string(),
            ),
        );
        row.entity_pk =
            EntityPk::uuid_from_canonical(file_id).expect("fixture file ID should be a UUID");
        row.file_id = Some(file_id.into());
        row.branch_id = branch_id.into();
        row.global = branch_id == crate::GLOBAL_BRANCH_ID;
        row
    }

    fn committed_file_descriptor_row(file_id: &str, branch_id: &str) -> MaterializedHotStateRow {
        MaterializedHotStateRow::from(staged_file_descriptor_row(file_id, branch_id))
    }

    fn directory_descriptor_row(
        directory_id: &str,
        parent_id: Option<&str>,
        name: &str,
        branch_id: &str,
    ) -> TestPreparedStateRow {
        let mut row = staged_row(
            DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
            Some(
                json!({
                    "id": directory_id,
                    "parent_id": parent_id,
                    "name": name,
                })
                .to_string(),
            ),
        );
        row.entity_pk = EntityPk::uuid_from_canonical(directory_id)
            .expect("fixture directory ID should be a UUID");
        row.file_id = None;
        row.branch_id = branch_id.into();
        row.global = branch_id == crate::GLOBAL_BRANCH_ID;
        row
    }

    fn committed_unique_row(entity_pk: &str, slug: &str, title: &str) -> MaterializedHotStateRow {
        let row = unique_row(entity_pk, slug, title);
        MaterializedHotStateRow {
            entity_pk: row.entity_pk,
            schema_key: row.schema_key.into(),
            file_id: row.file_id.map(Into::into),
            snapshot_content: row
                .snapshot
                .as_ref()
                .map(|snapshot| snapshot.materialize_shared()),
            metadata: row
                .metadata
                .as_ref()
                .map(|metadata| metadata.materialize_shared()),
            deleted: row.snapshot.is_none(),
            created_at: row.created_at,
            updated_at: row.updated_at,
            global: row.global,
            change_id: row.change_id,
            commit_id: row.commit_id,
            untracked: row.untracked,
            branch_id: Arc::from(row.branch_id.as_str()),
        }
    }

    fn committed_nullable_unique_row(
        entity_pk: &str,
        scope: Option<&str>,
        name: &str,
    ) -> MaterializedHotStateRow {
        MaterializedHotStateRow::from(nullable_unique_row(entity_pk, scope, name))
    }

    fn staged_row(schema_key: &str, snapshot_content: Option<String>) -> TestPreparedStateRow {
        TestPreparedStateRow {
            schema_plan_id: crate::catalog::SchemaPlanId::for_test(0),
            facts: crate::transaction_types::PreparedRowFacts::default(),
            entity_pk: EntityPk::single("entity-1"),
            schema_key: schema_key.into(),
            file_id: None,
            snapshot: snapshot_content.as_deref().map(test_stage_json),
            metadata: None,
            origin: None,
            origin_key: None,
            created_at: ts("2026-04-29T00:00:00.000Z"),
            updated_at: ts("2026-04-29T00:00:00.000Z"),
            global: true,
            change_id: Some(ChangeId::for_test_label("change-1")),
            commit_id: Some(CommitId::for_test_label("commit-1")),
            untracked: false,
            branch_id: crate::GLOBAL_BRANCH_ID.into(),
        }
    }

    fn plugin_reconciliation_update_origin() -> TransactionWriteOrigin {
        TransactionWriteOrigin {
            surface: "plugin_reconciliation".into(),
            operation: TransactionWriteOperation::Update,
            primary_key: None,
        }
    }

    fn fresh_plugin_file_import_write_set() -> PreparedWriteSet {
        let mut descriptor = staged_file_descriptor_row(
            "01920000-0000-7000-8000-0000000000a2",
            "01920000-0000-7000-8000-0000000000a1",
        );
        descriptor.facts.row_content_validated = true;
        // File descriptors intentionally retain the ordinary FK/namespace
        // fact. The certificate admits this exact trusted-planner shape and
        // keeps its public INSERT absence check below.
        descriptor.facts.requires_transaction_validation = true;
        descriptor.origin = Some(filesystem_insert_origin(
            "lix_file",
            "01920000-0000-7000-8000-0000000000a2",
        ));

        let mut blob_ref = staged_row(
            BLOB_REF_SCHEMA_KEY,
            Some(
                json!({
                    "id": "01920000-0000-7000-8000-0000000000a2",
                    "blob_hash": "a".repeat(64),
                    "size_bytes": 2,
                })
                .to_string(),
            ),
        );
        blob_ref.entity_pk = EntityPk::uuid_from_canonical("01920000-0000-7000-8000-0000000000a2")
            .expect("fixture file ID should be a UUID");
        blob_ref.file_id = Some("01920000-0000-7000-8000-0000000000a2".into());
        blob_ref.branch_id = "01920000-0000-7000-8000-0000000000a1".into();
        blob_ref.global = false;
        blob_ref.facts.row_content_validated = true;
        // Reconciliation replaces the provisional planner blob ref with this
        // exact materialization as an internal update. The public descriptor
        // remains the INSERT identity whose absence proves this is a fresh
        // file incarnation.
        blob_ref.origin = Some(plugin_reconciliation_update_origin());

        let mut owner = staged_row(
            "lix_key_value",
            Some(
                json!({
                    "key": PLUGIN_OWNER_KEY,
                    "value": {
                        "version": 1,
                        "plugin_key": "plugin_json",
                        "schema_keys": ["json_root"],
                    },
                })
                .to_string(),
            ),
        );
        owner.entity_pk = EntityPk::single(PLUGIN_OWNER_KEY);
        owner.file_id = Some("01920000-0000-7000-8000-0000000000a2".into());
        owner.branch_id = "01920000-0000-7000-8000-0000000000a1".into();
        owner.global = false;
        owner.facts.row_content_validated = true;
        owner.origin = Some(plugin_reconciliation_update_origin());

        let mut semantic = staged_row("json_root", Some(json!({ "kind": "object" }).to_string()));
        semantic.entity_pk = EntityPk::single("root");
        semantic.file_id = Some("01920000-0000-7000-8000-0000000000a2".into());
        semantic.branch_id = "01920000-0000-7000-8000-0000000000a1".into();
        semantic.global = false;
        semantic.facts.row_content_validated = true;
        semantic.origin = Some(plugin_reconciliation_update_origin());

        let mut writes = PreparedWriteSet {
            state_rows: prepared_rows![descriptor.clone(), blob_ref.clone(), owner, semantic],
            file_content_writes: vec![crate::transaction_types::TransactionFileContent::new(
                "01920000-0000-7000-8000-0000000000a2".to_string(),
                Some("/a.json".to_string()),
                Some("a.json".to_string()),
                "01920000-0000-7000-8000-0000000000a1".to_string(),
                false,
                false,
                b"{}".to_vec(),
            )],
            ..empty_staged_write_set()
        };
        writes.commit_change_refs_by_branch.insert(
            "01920000-0000-7000-8000-0000000000a1".to_string(),
            crate::transaction::staged_commit_changes::StagedCommitChangeRefs::default(),
        );
        writes.remember_insert_identity_for_tests(&descriptor);
        writes
    }

    #[test]
    fn prepared_tracked_row_certificates_match_the_commit_skip_contract() {
        let mut row = staged_row("normal_schema", Some(r#"{"id":"row"}"#.to_string()));
        row.facts.row_content_validated = true;
        assert!(prepared_tracked_rows_have_row_local_certificates(
            &prepared_rows![row.clone()]
        ));

        let mut requires_cross_row_validation = row.clone();
        requires_cross_row_validation
            .facts
            .requires_transaction_validation = true;
        assert!(!prepared_tracked_rows_have_row_local_certificates(
            &prepared_rows![requires_cross_row_validation]
        ));

        let mut untracked = row.clone();
        untracked.untracked = true;
        assert!(!prepared_tracked_rows_have_row_local_certificates(
            &prepared_rows![untracked]
        ));

        let mut file_scoped = row.clone();
        file_scoped.file_id = Some("01920000-0000-7000-8000-0000000000a2".into());
        assert!(!prepared_tracked_rows_have_row_local_certificates(
            &prepared_rows![file_scoped]
        ));

        let mut reserved = row;
        reserved.schema_key = REGISTERED_SCHEMA_KEY.into();
        assert!(!prepared_tracked_rows_have_row_local_certificates(
            &prepared_rows![reserved]
        ));
    }

    /// A row that carries an indexed column must never reach commit without
    /// passing through the extraction inside `validate_prepared_writes`.
    ///
    /// The hot index plane's one inviolable property is "never a false
    /// negative". A bypassed row that still earns its collection a witness
    /// would publish a complete-looking index that is missing that row, and
    /// every read of it would silently return nothing. Seven certificates can
    /// skip validation; this pins all seven.
    ///
    /// Four live in `bound_public_write.rs` — the insert batch, the two update
    /// batches, and the path-value replacement program — and each declines
    /// outright on `spec.has_inter_row_constraints`. That is sufficient
    /// because a column is only indexable if `x-lix-unique` or
    /// `x-lix-foreign-keys` declared it, which is the same predicate; see
    /// `indexed_columns_imply_inter_row_constraints`.
    ///
    /// The remaining three are asserted here directly: they all require
    /// `!requires_transaction_validation` on every row, which
    /// `normalization.rs` grants to a snapshot row only when its schema plan
    /// declares no uniques and no foreign keys — hence no indexed columns — or
    /// when `constraints_unchanged` proves the UPDATE assigned none of them;
    /// see `every_indexed_column_revokes_the_constraints_unchanged_certificate`.
    #[test]
    fn declared_column_rows_never_bypass_extraction() {
        let mut row = staged_row("indexed_schema", Some(r#"{"id":"row"}"#.to_string()));
        row.facts.row_content_validated = true;
        row.facts.requires_transaction_validation = true;

        // Site 5: `prepared_tracked_rows_have_row_local_certificates`, the
        // early return in `validate_prepared_writes_by_branch`.
        assert!(
            !prepared_tracked_rows_have_row_local_certificates(&prepared_rows![row.clone()]),
            "a row needing transaction validation must not skip the validation index"
        );

        // Site 6: `row_local_certificates_cover_validation`, the early return
        // inside `validate_prepared_writes` itself.
        let borrowed = row.borrowed();
        assert!(
            !row_local_certificates_cover_validation(&[PreparedValidationRow::State(borrowed)]),
            "a row needing transaction validation must not skip per-schema validation"
        );

        // Site 7: `fresh_plugin_file_import_certificate` under
        // `trust_filesystem_planner`. Its plugin-owned rows are admitted only
        // while `requires_transaction_validation` is clear.
        let mut writes = fresh_plugin_file_import_write_set();
        assert!(
            fresh_plugin_file_import_certificate(&writes).is_some(),
            "the unmodified fixture must certify, or this test proves nothing"
        );
        let mut constrained = staged_row("indexed_schema", Some(r#"{"id":"root"}"#.to_string()));
        constrained.entity_pk = EntityPk::single("root");
        constrained.file_id = Some("01920000-0000-7000-8000-0000000000a2".into());
        constrained.branch_id = "01920000-0000-7000-8000-0000000000a1".into();
        constrained.global = false;
        constrained.facts.row_content_validated = true;
        constrained.facts.requires_transaction_validation = true;
        constrained.origin = Some(plugin_reconciliation_update_origin());
        writes.state_rows.push_test_row(constrained);
        assert!(
            fresh_plugin_file_import_certificate(&writes).is_none(),
            "an indexed-schema row inside a plugin import must revoke the certificate"
        );
    }

    #[test]
    fn fresh_plugin_file_import_certificate_matches_the_real_blob_materialization_shape() {
        let writes = fresh_plugin_file_import_write_set();

        assert!(
            fresh_plugin_file_import_certificate(&writes).is_some(),
            "a fresh trusted lix_file INSERT plus v2 blob materialization should certify"
        );
    }

    #[test]
    fn fresh_plugin_file_import_certificate_rejects_missing_descriptor_or_cross_row_schema() {
        let mut missing_descriptor = fresh_plugin_file_import_write_set();
        let retained = missing_descriptor
            .state_rows
            .iter()
            .enumerate()
            .filter_map(|(row_index, row)| {
                (row.schema_key != FILE_DESCRIPTOR_SCHEMA_KEY).then_some(row_index)
            })
            .collect::<Vec<_>>();
        missing_descriptor.state_rows.select_rows(&retained);
        missing_descriptor.insert_selection.select_rows(&retained);
        assert!(fresh_plugin_file_import_certificate(&missing_descriptor).is_none());

        let mut cross_row_schema = fresh_plugin_file_import_write_set();
        let semantic_index = cross_row_schema
            .state_rows
            .iter()
            .enumerate()
            .find(|(_, row)| row.schema_key == "json_root")
            .map(|(index, _)| index)
            .expect("fixture should include a semantic row");
        cross_row_schema
            .state_rows
            .set_requires_transaction_validation(semantic_index, true);
        assert!(fresh_plugin_file_import_certificate(&cross_row_schema).is_none());
    }

    #[tokio::test]
    async fn fresh_plugin_file_import_certificate_retains_public_insert_absence_checks() {
        let writes = fresh_plugin_file_import_write_set();
        let certificate = fresh_plugin_file_import_certificate(&writes)
            .expect("fixture should satisfy the structural certificate");
        validate_certified_fresh_plugin_file_import(&StrictEmptyHotStateReader, certificate)
            .await
            .expect("new descriptor and blob identities should be absent");

        let duplicate_descriptor = writes
            .state_rows
            .iter()
            .find(|row| row.schema_key == FILE_DESCRIPTOR_SCHEMA_KEY)
            .expect("fixture should include descriptor");
        let certificate = fresh_plugin_file_import_certificate(&writes)
            .expect("certificate remains valid before committed lookup");
        let error = validate_certified_fresh_plugin_file_import(
            &StrictStaticHotStateReader {
                rows: vec![MaterializedHotStateRow::from(duplicate_descriptor)],
            },
            certificate,
        )
        .await
        .expect_err("a committed descriptor must still reject the public INSERT");
        assert_eq!(error.code, LixError::CODE_UNIQUE);
    }
}
