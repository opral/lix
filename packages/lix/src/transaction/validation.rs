//! Native transaction validation.
//!
//! This module is deliberately coupled to the concrete transaction state
//! view.  Validation must observe the same staged tracked/untracked overlay
//! that the write will publish; a trait object or a second scan authority
//! would make constraint decisions diverge from the commit plan.

#![allow(clippy::needless_borrow, clippy::unnecessary_wraps)]

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value as JsonValue;

use crate::GLOBAL_BRANCH_ID;
use crate::LixError;
use crate::branch::BRANCH_DESCRIPTOR_SCHEMA_KEY;
use crate::catalog::{CatalogSnapshot, SchemaPlan};
use crate::common::{SharedStr, json_pointer_get, validate_row_metadata};
use crate::domain::Domain;
use crate::entity_pk::{EntityPk, EntityPkComponents};
use crate::forktree::{
    StateCell, StateKey, StateKeyRef, decode_state_key, encode_state_entity_prefix,
    encode_state_key, exclusive_prefix_upper_bound,
};
use crate::state::{StateRow, StateRowSource, TransactionStateView};
use crate::storage_adapter::StorageAdapterRead;
use crate::transaction::staging::{PreparedValidationRow, PreparedWriteValidationSet};

const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";
const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";

/// The complete native input to commit-time validation.  `R` is the concrete
/// retained storage read owned by the transaction; there is intentionally no
/// reader trait, request DTO, or compatibility materialization layer here.
pub(crate) struct TransactionValidationInput<'a, R> {
    pub(crate) staged_writes: PreparedWriteValidationSet<'a>,
    pub(crate) schema_catalog: &'a CatalogSnapshot,
    pub(crate) state_view: &'a TransactionStateView<R>,
    pub(crate) active_branch_id: &'a str,
    pub(crate) trust_filesystem_planner: bool,
}

impl<'a, R> TransactionValidationInput<'a, R> {
    pub(crate) fn new(
        staged_writes: PreparedWriteValidationSet<'a>,
        schema_catalog: &'a CatalogSnapshot,
        state_view: &'a TransactionStateView<R>,
        active_branch_id: &'a str,
    ) -> Self {
        Self {
            staged_writes,
            schema_catalog,
            state_view,
            active_branch_id,
            trust_filesystem_planner: false,
        }
    }

    pub(crate) fn with_trusted_filesystem_planner(mut self) -> Self {
        self.trust_filesystem_planner = true;
        self
    }
}

#[derive(Clone, Debug)]
struct NativeValidationRow {
    key: StateKey,
    branch_id: String,
    global: bool,
    untracked: bool,
    snapshot: Option<SharedStr>,
    metadata: Option<SharedStr>,
    deleted: bool,
}

impl NativeValidationRow {
    fn from_tracked(row: StateRow, branch_id: &str) -> Result<Self, LixError> {
        let key = decode_state_key(&row.key)?;
        let branch_id = if key.schema_key == BRANCH_DESCRIPTOR_SCHEMA_KEY {
            GLOBAL_BRANCH_ID.to_owned()
        } else {
            branch_id.to_owned()
        };
        let deleted = row.value.cell.deleted();
        let snapshot = row.value.cell.seed_logical_text(&key, &branch_id)?;
        Ok(Self {
            key,
            branch_id,
            global: matches!(row.source, StateRowSource::Global),
            untracked: false,
            snapshot,
            metadata: row.value.metadata,
            deleted,
        })
    }

    fn schema_key(&self) -> &str {
        &self.key.schema_key
    }

    fn entity_pk(&self) -> &EntityPk {
        &self.key.entity_pk
    }

    fn file_id(&self) -> Option<&str> {
        self.key.file_id.as_deref()
    }

    fn snapshot_json(&self) -> Result<Option<JsonValue>, LixError> {
        self.snapshot
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .map_err(|error| {
                LixError::new(
                    "LIX_ERROR_INVALID_JSON",
                    format!("native state snapshot is invalid JSON: {error}"),
                )
            })
    }
}

#[derive(Default)]
struct NativeValidationRows {
    rows: Vec<NativeValidationRow>,
}

impl NativeValidationRows {
    fn extend(&mut self, other: Self) {
        self.rows.extend(other.rows);
    }
}

type PropertyGroupKey = Vec<Vec<String>>;
type PropertyValueKey = (String, PropertyGroupKey, Vec<String>);
type InsertIdentityKey = (String, bool, Option<String>, String, EntityPk);
type NamespaceKey = (String, bool, Option<String>, String);

/// One-pass indexes over the authenticated committed rows used by validation.
/// The old implementation re-scanned every committed row for every staged row
/// and every constraint.  These indexes preserve the same row set and filters,
/// but make the committed-side work O(N + K) per validation cohort rather than
/// O(N * K), where N is visible state and K is the changed set.
struct NativeValidationIndex {
    rows: NativeValidationRows,
    property_values: BTreeMap<PropertyValueKey, Vec<usize>>,
    file_owners: BTreeMap<String, Vec<usize>>,
    insert_identities: BTreeMap<InsertIdentityKey, Vec<usize>>,
    namespace_paths: BTreeMap<NamespaceKey, Vec<usize>>,
}

impl NativeValidationIndex {
    fn build(rows: NativeValidationRows, catalog: &CatalogSnapshot) -> Result<Self, LixError> {
        let mut property_groups = BTreeMap::<String, BTreeSet<PropertyGroupKey>>::new();
        for plan in catalog.plans() {
            for unique in &plan.uniques {
                property_groups
                    .entry(plan.key.schema_key.clone())
                    .or_default()
                    .insert(unique.clone());
            }
            for foreign_key in &plan.foreign_keys {
                property_groups
                    .entry(plan.key.schema_key.clone())
                    .or_default()
                    .insert(foreign_key.local_properties.clone());
                property_groups
                    .entry(foreign_key.referenced_schema.schema_key.clone())
                    .or_default()
                    .insert(foreign_key.referenced_properties.clone());
            }
        }

        let mut property_values = BTreeMap::<PropertyValueKey, Vec<usize>>::new();
        let mut file_owners = BTreeMap::<String, Vec<usize>>::new();
        let mut insert_identities = BTreeMap::<InsertIdentityKey, Vec<usize>>::new();
        let mut namespace_paths = BTreeMap::<NamespaceKey, Vec<usize>>::new();
        for (index, row) in rows.rows.iter().enumerate() {
            if row.deleted {
                continue;
            }
            insert_identities
                .entry((
                    row.branch_id.clone(),
                    row.global,
                    row.file_id().map(str::to_owned),
                    row.schema_key().to_owned(),
                    row.entity_pk().clone(),
                ))
                .or_default()
                .push(index);

            if row.schema_key() == FILE_DESCRIPTOR_SCHEMA_KEY {
                if let Some(file_id) = row.file_id() {
                    file_owners
                        .entry(file_id.to_owned())
                        .or_default()
                        .push(index);
                } else if let Ok(file_id) = row.entity_pk().as_single_string() {
                    file_owners
                        .entry(file_id.to_owned())
                        .or_default()
                        .push(index);
                }
            }

            if matches!(
                row.schema_key(),
                DIRECTORY_DESCRIPTOR_SCHEMA_KEY | FILE_DESCRIPTOR_SCHEMA_KEY
            ) {
                if let Ok(Some((parent, name))) = descriptor_namespace_parts(row) {
                    namespace_paths
                        .entry((row.branch_id.clone(), row.untracked, parent, name))
                        .or_default()
                        .push(index);
                }
            }

            let Some(groups) = property_groups.get(row.schema_key()) else {
                continue;
            };
            let Some(snapshot) = row.snapshot_json()? else {
                continue;
            };
            for group in groups {
                if let Some(value) = pointer_group_value(&snapshot, group) {
                    property_values
                        .entry((row.schema_key().to_owned(), group.clone(), value))
                        .or_default()
                        .push(index);
                }
            }
        }

        Ok(Self {
            rows,
            property_values,
            file_owners,
            insert_identities,
            namespace_paths,
        })
    }

    fn row(&self, index: usize) -> &NativeValidationRow {
        &self.rows.rows[index]
    }

    fn property_candidates(
        &self,
        schema_key: &str,
        properties: &PropertyGroupKey,
        value: &[String],
    ) -> &[usize] {
        self.property_values
            .get(&(schema_key.to_owned(), properties.clone(), value.to_vec()))
            .map(Vec::as_slice)
            .unwrap_or_default()
    }

    fn file_owner_candidates(&self, file_id: &str) -> &[usize] {
        self.file_owners
            .get(file_id)
            .map(Vec::as_slice)
            .unwrap_or_default()
    }

    fn insert_identity_candidates(&self, key: &InsertIdentityKey) -> &[usize] {
        self.insert_identities
            .get(key)
            .map(Vec::as_slice)
            .unwrap_or_default()
    }

    fn namespace_candidates(
        &self,
        branch_id: &str,
        untracked: bool,
        parent: Option<&str>,
        name: &str,
    ) -> &[usize] {
        self.namespace_paths
            .get(&(
                branch_id.to_owned(),
                untracked,
                parent.map(str::to_owned),
                name.to_owned(),
            ))
            .map(Vec::as_slice)
            .unwrap_or_default()
    }
}

fn domain_matches(domain: &Domain, row: &NativeValidationRow) -> bool {
    if domain.branch_id() != row.branch_id || domain.untracked() != row.untracked {
        return false;
    }
    match domain.file_filters().as_slice() {
        [] => true,
        [crate::NullableKeyFilter::Null] => row.file_id().is_none(),
        [crate::NullableKeyFilter::Value(file_id)] => row.file_id() == Some(file_id),
        _ => false,
    }
}

async fn visible_rows<R>(
    state_view: &TransactionStateView<R>,
    active_branch_id: &str,
    domain: &Domain,
    schema_key: &str,
) -> Result<NativeValidationRows, LixError>
where
    R: StorageAdapterRead,
{
    let mut output = NativeValidationRows::default();
    let prefix = encode_state_entity_prefix(
        schema_key,
        &EntityPk {
            components: EntityPkComponents::Empty,
        },
    );
    let upper = exclusive_prefix_upper_bound(&prefix);
    // Validation is bounded to each affected schema prefix.  The native
    // state view authenticates ordering, branch/global overlay, tombstones,
    // and the transaction's staged replacement before this filter runs.
    for row in state_view
        .range(Some(&prefix), upper.as_deref(), None, true)
        .await
        .map_err(LixError::from)?
    {
        #[cfg(feature = "storage-benches")]
        crate::storage_bench::record_transaction_validation_row_visited();
        let row = NativeValidationRow::from_tracked(row, active_branch_id)?;
        if domain_matches(domain, &row) {
            output.rows.push(row);
        }
    }
    Ok(output)
}

struct ExactVisibleRequest {
    key: Vec<u8>,
    untracked: bool,
    include_tombstones: bool,
}

fn exact_visible_request(
    domain: &Domain,
    schema_key: &str,
    entity_pk: &EntityPk,
    include_tombstones: bool,
) -> Result<ExactVisibleRequest, LixError> {
    let key = encode_state_key(StateKeyRef {
        schema_key,
        file_id: match domain.file_filters().as_slice() {
            [crate::NullableKeyFilter::Value(file_id)] => Some(file_id.as_str()),
            [crate::NullableKeyFilter::Null] => None,
            [] => None,
            _ => {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "exact validation lookup requires one file-scope selector",
                ));
            }
        },
        entity_pk,
    });
    Ok(ExactVisibleRequest {
        key,
        untracked: false,
        include_tombstones,
    })
}

/// Resolves all exact validation targets through the one transaction-owned
/// retained view. Requests are batched once, while
/// the returned vector remains aligned with the caller's original request
/// order. No committed-only reader or alternate materialization authority is
/// introduced here.
async fn batched_exact_visible_rows<R>(
    state_view: &TransactionStateView<R>,
    active_branch_id: &str,
    requests: Vec<ExactVisibleRequest>,
) -> Result<Vec<Option<NativeValidationRow>>, LixError>
where
    R: StorageAdapterRead,
{
    let tracked_keys = requests
        .iter()
        .filter(|request| !request.untracked)
        .map(|request| request.key.clone())
        .collect::<Vec<_>>();
    let tracked_rows = if tracked_keys.is_empty() {
        Vec::new()
    } else {
        state_view
            .points(&tracked_keys, true)
            .await
            .map_err(LixError::from)?
    };
    let mut tracked_rows = tracked_rows.into_iter();
    let mut output = Vec::with_capacity(requests.len());
    for request in requests {
        let row = tracked_rows
            .next()
            .expect("tracked validation batch slot count matches requests")
            .map(|row| NativeValidationRow::from_tracked(row, active_branch_id))
            .transpose()?;
        output.push(row.filter(|row| request.include_tombstones || !row.deleted));
    }
    debug_assert!(tracked_rows.next().is_none());
    Ok(output)
}

fn prepared_snapshot(row: PreparedValidationRow<'_>) -> Result<Option<JsonValue>, LixError> {
    Ok(row.snapshot_json().cloned())
}

fn prepared_metadata(row: PreparedValidationRow<'_>) -> Result<Option<JsonValue>, LixError> {
    Ok(row.metadata_json().cloned())
}

fn schema_plan<'a>(
    catalog: &'a CatalogSnapshot,
    row: PreparedValidationRow<'_>,
) -> Result<&'a SchemaPlan, LixError> {
    catalog
        .plan_for_key(row.schema_key())
        .map(|(_, plan)| plan)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!(
                    "schema '{}' does not exist in the transaction catalog",
                    row.schema_key()
                ),
            )
        })
}

fn validate_row_content(
    catalog: &CatalogSnapshot,
    row: PreparedValidationRow<'_>,
    snapshot: &JsonValue,
) -> Result<(), LixError> {
    let plan = schema_plan(catalog, row)?;
    if let Err(errors) = plan.compiled_schema.validate(snapshot) {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_VALIDATION,
            format!(
                "snapshot_content validation failed for schema '{}': {}",
                row.schema_key(),
                crate::schema::format_lix_schema_validation_errors(errors)
            ),
        ));
    }
    Ok(())
}

fn validate_primary_key_identity(
    catalog: &CatalogSnapshot,
    row: PreparedValidationRow<'_>,
    snapshot: &JsonValue,
) -> Result<(), LixError> {
    let Some((_, plan)) = catalog.plan_for_key(row.schema_key()) else {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_VALIDATION,
            format!("schema '{}' does not exist", row.schema_key()),
        ));
    };
    let Some(primary_key) = plan.primary_key.as_deref() else {
        return Ok(());
    };
    let actual = primary_key
        .iter()
        .map(|pointer| json_pointer_get(snapshot, pointer).cloned())
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!("schema '{}' primary key is missing", row.schema_key()),
            )
        })?;
    let expected = row.entity_pk().as_json_array_text()?;
    let expected: JsonValue = serde_json::from_str(&expected).map_err(|error| {
        LixError::new(
            LixError::CODE_SCHEMA_VALIDATION,
            format!("prepared entity identity is invalid: {error}"),
        )
    })?;
    if actual != expected.as_array().cloned().unwrap_or_default() {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_VALIDATION,
            format!(
                "snapshot primary key does not match entity identity for schema '{}'",
                row.schema_key()
            ),
        ));
    }
    Ok(())
}

fn stable_unique_value(value: &JsonValue) -> Option<String> {
    if value.is_null() {
        None
    } else {
        crate::entity_pk::canonical_json_text(value).ok()
    }
}

fn pointer_group_value(snapshot: &JsonValue, pointers: &[Vec<String>]) -> Option<Vec<String>> {
    pointers
        .iter()
        .map(|pointer| json_pointer_get(snapshot, pointer).and_then(stable_unique_value))
        .collect()
}

fn constraint_error(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_CONSTRAINT_VIOLATION, message)
}

fn unique_constraint_error(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_UNIQUE, message)
}

fn foreign_key_constraint_error(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_FOREIGN_KEY, message)
}

fn same_identity(row: PreparedValidationRow<'_>, current: &NativeValidationRow) -> bool {
    row.schema_key() == current.schema_key()
        && row.entity_pk() == current.entity_pk()
        && row.file_id() == current.file_id()
        && row.untracked() == current.untracked
        && row.branch_id() == current.branch_id
}

fn same_insert_identity(row: PreparedValidationRow<'_>, current: &NativeValidationRow) -> bool {
    row.schema_key() == current.schema_key()
        && row.entity_pk() == current.entity_pk()
        && row.file_id() == current.file_id()
        && row.branch_id() == current.branch_id
        && prepared_row_is_global(row) == current.global
}

fn prepared_row_domain(row: PreparedValidationRow<'_>) -> Domain {
    Domain::exact_file(
        row.branch_id().to_owned(),
        row.untracked(),
        row.file_id().map(str::to_owned),
    )
}

fn prepared_row_is_global(row: PreparedValidationRow<'_>) -> bool {
    match row {
        PreparedValidationRow::State(row) => row.global,
    }
}

fn same_scope(left: &NativeValidationRow, right: PreparedValidationRow<'_>) -> bool {
    left.branch_id == right.branch_id()
        && left.untracked == right.untracked()
        && (left.global || !prepared_row_is_global(right))
}

fn staged_identity_matches(left: &NativeValidationRow, right: PreparedValidationRow<'_>) -> bool {
    left.schema_key() == right.schema_key()
        && left.entity_pk() == right.entity_pk()
        && left.file_id() == right.file_id()
        && left.untracked == right.untracked()
        && left.branch_id == right.branch_id()
}

fn row_is_tombstone(row: PreparedValidationRow<'_>) -> bool {
    row.is_tombstone()
}

async fn validate_unique_constraints<R>(
    input: &TransactionValidationInput<'_, R>,
    staged_rows: &[PreparedValidationRow<'_>],
    all_rows: &NativeValidationIndex,
) -> Result<(), LixError>
where
    R: StorageAdapterRead,
{
    for row in staged_rows
        .iter()
        .copied()
        .filter(|row| !row_is_tombstone(*row))
    {
        let Some((_, plan)) = input.schema_catalog.plan_for_key(row.schema_key()) else {
            continue;
        };
        let snapshot = prepared_snapshot(row)?.ok_or_else(|| {
            constraint_error(format!(
                "non-tombstone row '{}' has no snapshot",
                row.schema_key()
            ))
        })?;
        for pointers in &plan.uniques {
            let Some(value) = pointer_group_value(&snapshot, pointers) else {
                continue;
            };
            for &other_index in all_rows.property_candidates(row.schema_key(), pointers, &value) {
                let other = all_rows.row(other_index);
                if same_identity(row, other)
                    || other.deleted
                    || other.schema_key() != row.schema_key()
                {
                    continue;
                }
                let Some(other_snapshot) = other.snapshot_json()? else {
                    continue;
                };
                if pointer_group_value(&other_snapshot, pointers).as_ref() == Some(&value) {
                    return Err(unique_constraint_error(format!(
                        "unique constraint violation on schema '{}'",
                        row.schema_key()
                    )));
                }
            }
            for other in staged_rows.iter().copied().filter(|other| {
                !row_is_tombstone(*other)
                    && other.schema_key() == row.schema_key()
                    && !same_identity(
                        *other,
                        &NativeValidationRow {
                            key: StateKey {
                                schema_key: row.schema_key().to_owned(),
                                file_id: row.file_id().map(str::to_owned),
                                entity_pk: row.entity_pk().clone(),
                            },
                            branch_id: row.branch_id().to_owned(),
                            global: false,
                            untracked: row.untracked(),
                            snapshot: None,
                            metadata: None,
                            deleted: false,
                        },
                    )
            }) {
                let Some(other_snapshot) = prepared_snapshot(other)? else {
                    continue;
                };
                if pointer_group_value(&other_snapshot, pointers).as_ref() == Some(&value) {
                    return Err(unique_constraint_error(format!(
                        "unique constraint violation on schema '{}'",
                        row.schema_key()
                    )));
                }
            }
        }
    }
    Ok(())
}

async fn validate_foreign_keys<R>(
    input: &TransactionValidationInput<'_, R>,
    staged_rows: &[PreparedValidationRow<'_>],
    all_rows: &NativeValidationIndex,
) -> Result<(), LixError>
where
    R: StorageAdapterRead,
{
    for row in staged_rows
        .iter()
        .copied()
        .filter(|row| !row_is_tombstone(*row))
    {
        let Some((_, plan)) = input.schema_catalog.plan_for_key(row.schema_key()) else {
            continue;
        };
        let Some(snapshot) = prepared_snapshot(row)? else {
            continue;
        };
        for foreign_key in &plan.foreign_keys {
            let Some(local_value) = pointer_group_value(&snapshot, &foreign_key.local_properties)
            else {
                continue;
            };
            let target_schema = foreign_key.referenced_schema.schema_key.as_str();
            let found = all_rows
                .property_candidates(
                    target_schema,
                    &foreign_key.referenced_properties,
                    &local_value,
                )
                .iter()
                .map(|&target_index| all_rows.row(target_index))
                .any(|target| !target.deleted && target.schema_key() == target_schema)
                || staged_rows.iter().copied().any(|target| {
                    !row_is_tombstone(target)
                        && target.schema_key() == target_schema
                        && prepared_snapshot(target).ok().flatten().and_then(|value| {
                            pointer_group_value(&value, &foreign_key.referenced_properties)
                        }) == Some(local_value.clone())
                });
            if !found {
                return Err(foreign_key_constraint_error(format!(
                    "foreign key on schema '{}' references a missing '{}' row",
                    row.schema_key(),
                    target_schema
                )));
            }
        }
    }
    Ok(())
}

async fn validate_file_ownership<R>(
    input: &TransactionValidationInput<'_, R>,
    staged_rows: &[PreparedValidationRow<'_>],
    all_rows: &NativeValidationIndex,
) -> Result<(), LixError>
where
    R: StorageAdapterRead,
{
    if input.trust_filesystem_planner {
        return Ok(());
    }
    for row in staged_rows
        .iter()
        .copied()
        .filter(|row| !row_is_tombstone(*row))
    {
        let Some(file_id) = row.file_id() else {
            continue;
        };
        let present = all_rows
            .file_owner_candidates(file_id)
            .iter()
            .map(|&candidate_index| all_rows.row(candidate_index))
            .any(|candidate| {
                !candidate.deleted && candidate.schema_key() == FILE_DESCRIPTOR_SCHEMA_KEY
            })
            || staged_rows.iter().copied().any(|candidate| {
                !row_is_tombstone(candidate)
                    && candidate.schema_key() == FILE_DESCRIPTOR_SCHEMA_KEY
                    && ((candidate.file_id().is_none()
                        && candidate.entity_pk().as_single_string().ok() == Some(file_id))
                        || candidate.file_id() == Some(file_id))
            });
        if !present {
            return Err(LixError::new(
                LixError::CODE_FILE_NOT_FOUND,
                format!("row references missing file_id '{file_id}'"),
            ));
        }
    }
    Ok(())
}

fn descriptor_namespace_parts(
    row: &NativeValidationRow,
) -> Result<Option<(Option<String>, String)>, LixError> {
    let Some(snapshot) = row.snapshot_json()? else {
        return Ok(None);
    };
    let parent = if row.schema_key() == DIRECTORY_DESCRIPTOR_SCHEMA_KEY {
        json_pointer_get(&snapshot, &["parent_id".to_string()]).cloned()
    } else if row.schema_key() == FILE_DESCRIPTOR_SCHEMA_KEY {
        json_pointer_get(&snapshot, &["directory_id".to_string()]).cloned()
    } else {
        return Ok(None);
    };
    let name = json_pointer_get(&snapshot, &["name".to_string()])
        .and_then(JsonValue::as_str)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!("{} descriptor is missing string name", row.schema_key()),
            )
        })?;
    let parent = match parent {
        None | Some(JsonValue::Null) => None,
        Some(JsonValue::String(value)) => Some(value),
        Some(_) => {
            return Err(LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!(
                    "{} descriptor has invalid parent identity",
                    row.schema_key()
                ),
            ));
        }
    };
    Ok(Some((parent, name.to_owned())))
}

fn prepared_descriptor_namespace_parts(
    row: PreparedValidationRow<'_>,
) -> Result<Option<(Option<String>, String)>, LixError> {
    let Some(snapshot) = prepared_snapshot(row)? else {
        return Ok(None);
    };
    let Some(snapshot) = snapshot.as_object() else {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_VALIDATION,
            format!("{} descriptor snapshot must be an object", row.schema_key()),
        ));
    };
    let parent = if row.schema_key() == DIRECTORY_DESCRIPTOR_SCHEMA_KEY {
        snapshot.get("parent_id")
    } else if row.schema_key() == FILE_DESCRIPTOR_SCHEMA_KEY {
        snapshot.get("directory_id")
    } else {
        return Ok(None);
    };
    let name = snapshot
        .get("name")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!("{} descriptor is missing string name", row.schema_key()),
            )
        })?;
    let parent = match parent {
        None | Some(JsonValue::Null) => None,
        Some(JsonValue::String(value)) => Some(value.to_owned()),
        Some(_) => {
            return Err(LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!(
                    "{} descriptor has invalid parent identity",
                    row.schema_key()
                ),
            ));
        }
    };
    Ok(Some((parent, name.to_owned())))
}

async fn validate_filesystem_namespace<R>(
    input: &TransactionValidationInput<'_, R>,
    staged_rows: &[PreparedValidationRow<'_>],
    all_rows: &NativeValidationIndex,
) -> Result<(), LixError>
where
    R: StorageAdapterRead,
{
    if input.trust_filesystem_planner {
        return Ok(());
    }
    let descriptors = staged_rows.iter().copied().filter(|row| {
        !row_is_tombstone(*row)
            && matches!(
                row.schema_key(),
                DIRECTORY_DESCRIPTOR_SCHEMA_KEY | FILE_DESCRIPTOR_SCHEMA_KEY
            )
    });
    for row in descriptors {
        let Some((parent, name)) = prepared_descriptor_namespace_parts(row)? else {
            continue;
        };
        let committed_conflict = all_rows
            .namespace_candidates(row.branch_id(), row.untracked(), parent.as_deref(), &name)
            .iter()
            .map(|&candidate_index| all_rows.row(candidate_index))
            .any(|candidate| {
                candidate.schema_key() != row.schema_key()
                    && same_scope(candidate, row)
                    && candidate.entity_pk() != row.entity_pk()
                    && !staged_rows
                        .iter()
                        .copied()
                        .any(|staged| staged_identity_matches(candidate, staged))
            });
        if committed_conflict {
            return Err(constraint_error(format!(
                "filesystem namespace conflict for parent {:?} entry {:?}",
                parent, name
            )));
        }
        let staged_conflict = staged_rows.iter().copied().any(|other| {
            other.schema_key() != row.schema_key()
                && !row_is_tombstone(other)
                && matches!(
                    other.schema_key(),
                    DIRECTORY_DESCRIPTOR_SCHEMA_KEY | FILE_DESCRIPTOR_SCHEMA_KEY
                )
                && other.branch_id() == row.branch_id()
                && other.untracked() == row.untracked()
                && other.entity_pk() != row.entity_pk()
                && prepared_descriptor_namespace_parts(other)
                    .ok()
                    .flatten()
                    .is_some_and(|parts| parts == (parent.clone(), name.clone()))
        });
        if staged_conflict {
            return Err(constraint_error(format!(
                "filesystem namespace conflict for parent {:?} entry {:?}",
                parent, name
            )));
        }
    }
    Ok(())
}

async fn validate_delete_restrictions<R>(
    input: &TransactionValidationInput<'_, R>,
    staged_rows: &[PreparedValidationRow<'_>],
    all_rows: &NativeValidationIndex,
) -> Result<(), LixError>
where
    R: StorageAdapterRead,
{
    let tombstones = staged_rows
        .iter()
        .copied()
        .filter(|row| {
            row_is_tombstone(*row)
                && input
                    .schema_catalog
                    .delete_plan_for_key(row.schema_key())
                    .has_committed_checks()
        })
        .collect::<Vec<_>>();
    let requests = tombstones
        .iter()
        .map(|tombstone| {
            exact_visible_request(
                &prepared_row_domain(*tombstone),
                tombstone.schema_key(),
                tombstone.entity_pk(),
                false,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let targets =
        batched_exact_visible_rows(input.state_view, input.active_branch_id, requests).await?;

    for (tombstone, target) in tombstones.into_iter().zip(targets) {
        let delete_plan = input
            .schema_catalog
            .delete_plan_for_key(tombstone.schema_key());
        let Some(target) = target else {
            continue;
        };
        let Some(target_snapshot) = target.snapshot_json()? else {
            continue;
        };
        for reference in delete_plan.foreign_key_references {
            let Some(deleted_value) = pointer_group_value(
                &target_snapshot,
                &reference.foreign_key.referenced_properties,
            ) else {
                continue;
            };
            let committed_conflict = all_rows
                .property_candidates(
                    reference.source_key.schema_key.as_str(),
                    &reference.foreign_key.local_properties,
                    &deleted_value,
                )
                .iter()
                .map(|&candidate_index| all_rows.row(candidate_index))
                .any(|candidate| {
                    candidate.schema_key() == reference.source_key.schema_key
                        && candidate.untracked == tombstone.untracked()
                        && !staged_rows
                            .iter()
                            .copied()
                            .any(|row| staged_identity_matches(candidate, row))
                });
            let staged_conflict = staged_rows.iter().copied().any(|candidate| {
                !row_is_tombstone(candidate)
                    && candidate.schema_key() == reference.source_key.schema_key
                    && candidate.untracked() == tombstone.untracked()
                    && prepared_snapshot(candidate)
                        .ok()
                        .flatten()
                        .and_then(|snapshot| {
                            pointer_group_value(&snapshot, &reference.foreign_key.local_properties)
                        })
                        == Some(deleted_value.clone())
            });
            if committed_conflict || staged_conflict {
                return Err(LixError::new(
                    LixError::CODE_FOREIGN_KEY,
                    format!(
                        "cannot delete '{}' row because '{}' rows still reference it",
                        tombstone.schema_key(),
                        reference.source_key.schema_key
                    ),
                ));
            }
        }
    }
    Ok(())
}

fn validate_registered_schema_rows(
    _input: &TransactionValidationInput<'_, impl StorageAdapterRead>,
    staged_rows: &[PreparedValidationRow<'_>],
) -> Result<(), LixError> {
    for row in staged_rows
        .iter()
        .copied()
        .filter(|row| row.schema_key() == REGISTERED_SCHEMA_KEY)
    {
        let Some(snapshot) = prepared_snapshot(row)? else {
            continue;
        };
        let (key, _) = crate::schema::schema_from_registered_snapshot(&snapshot)?;
        crate::transaction::normalization::reject_reserved_schema_namespace(&key)?;
    }
    Ok(())
}

fn validate_descriptor_shapes(
    catalog: &CatalogSnapshot,
    staged_rows: &[PreparedValidationRow<'_>],
) -> Result<(), LixError> {
    for row in staged_rows.iter().copied() {
        if row.schema_key().is_empty() {
            return Err(LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                "schema_key must not be empty",
            ));
        }
        if row.schema_key() == REGISTERED_SCHEMA_KEY && row.file_id().is_some() {
            return Err(LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                "registered schema rows cannot have a file scope",
            ));
        }
        if row_is_tombstone(row) {
            continue;
        }
        let Some(snapshot) = prepared_snapshot(row)? else {
            continue;
        };
        validate_row_content(catalog, row, &snapshot)?;
        if let Some(metadata) = prepared_metadata(row)? {
            validate_row_metadata(
                &metadata,
                format!("metadata for schema '{}'", row.schema_key()),
            )?;
        }
        if !row.row_content_validated() {
            validate_primary_key_identity(catalog, row, &snapshot)?;
        }
    }
    Ok(())
}

/// Validate one final coalesced write set through the transaction's native
/// state overlay.  It is called before any ForkTree/storage publication, so a
/// failure cannot leave a partial write or a partially installed validation
/// index.
pub(crate) async fn validate_prepared_writes<R>(
    input: TransactionValidationInput<'_, R>,
) -> Result<(), LixError>
where
    R: StorageAdapterRead,
{
    let staged_rows = input.staged_writes.rows().collect::<Vec<_>>();
    if staged_rows.is_empty() {
        return Ok(());
    }
    validate_descriptor_shapes(input.schema_catalog, &staged_rows)?;
    validate_registered_schema_rows(&input, &staged_rows)?;
    let has_inserts = input.staged_writes.inserts().next().is_some();
    let needs_committed_state = staged_rows.iter().any(|row| {
        let tombstone_needs_committed_state = row_is_tombstone(*row)
            && (row.schema_key() == "lix_account"
                || input
                    .schema_catalog
                    .delete_plan_for_key(row.schema_key())
                    .has_committed_checks());
        row.requires_transaction_validation()
            || row.global()
            || row.untracked()
            || row.file_id().is_some()
            || tombstone_needs_committed_state
            || matches!(
                row.schema_key(),
                FILE_DESCRIPTOR_SCHEMA_KEY | DIRECTORY_DESCRIPTOR_SCHEMA_KEY | "lix_account"
            )
    });
    if !needs_committed_state {
        if has_inserts {
            validate_insert_identities_by_point(&input).await?;
        }
        return Ok(());
    }
    let mut schema_keys = BTreeSet::new();
    for row in &staged_rows {
        schema_keys.insert(row.schema_key().to_owned());
        if let Some((_, plan)) = input.schema_catalog.plan_for_key(row.schema_key()) {
            schema_keys.extend(
                plan.foreign_keys
                    .iter()
                    .map(|foreign_key| foreign_key.referenced_schema.schema_key.clone()),
            );
        }
        if row_is_tombstone(*row) {
            schema_keys.extend(
                input
                    .schema_catalog
                    .delete_plan_for_key(row.schema_key())
                    .foreign_key_references
                    .iter()
                    .map(|reference| reference.source_key.schema_key.clone()),
            );
        }
        if row.file_id().is_some() {
            schema_keys.insert(FILE_DESCRIPTOR_SCHEMA_KEY.to_owned());
        }
        if matches!(
            row.schema_key(),
            DIRECTORY_DESCRIPTOR_SCHEMA_KEY | FILE_DESCRIPTOR_SCHEMA_KEY
        ) {
            schema_keys.insert(DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_owned());
            schema_keys.insert(FILE_DESCRIPTOR_SCHEMA_KEY.to_owned());
        }
    }
    let mut all_rows = NativeValidationRows::default();
    for schema_key in schema_keys {
        all_rows.extend(
            visible_rows(
                input.state_view,
                input.active_branch_id,
                &Domain::any_file(input.active_branch_id, false),
                &schema_key,
            )
            .await?,
        );
    }
    let validation_index = NativeValidationIndex::build(all_rows, input.schema_catalog)?;
    validate_unique_constraints(&input, &staged_rows, &validation_index).await?;
    validate_foreign_keys(&input, &staged_rows, &validation_index).await?;
    validate_file_ownership(&input, &staged_rows, &validation_index).await?;
    validate_filesystem_namespace(&input, &staged_rows, &validation_index).await?;
    for row in staged_rows
        .iter()
        .copied()
        .filter(|row| row.schema_key() == "lix_account")
    {
        let account_id = row.entity_pk().as_single_string_owned()?;
        let disables_builtin = row_is_tombstone(row)
            || prepared_snapshot(row)?
                .as_ref()
                .and_then(|snapshot| snapshot.get("status"))
                .and_then(JsonValue::as_str)
                == Some("disabled");
        if disables_builtin
            && matches!(
                account_id.as_str(),
                crate::SYSTEM_ACCOUNT_ID | crate::ANONYMOUS_ACCOUNT_ID
            )
        {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "built-in accounts must remain active",
            ));
        }
    }
    for tombstone in staged_rows
        .iter()
        .copied()
        .filter(|row| row_is_tombstone(*row) && row.schema_key() == "lix_account")
    {
        let account_id = tombstone.entity_pk().as_single_string_owned()?;
        if input.state_view.has_authored_change(&account_id).await? {
            return Err(LixError::new(
                "LIX_FOREIGN_KEY_VIOLATION",
                "cannot delete 'lix_account' row because 'lix_change' rows still reference it",
            ));
        }
    }
    validate_delete_restrictions(&input, &staged_rows, &validation_index).await?;
    validate_insert_identities(&input, &validation_index).await?;
    Ok(())
}

/// Validate only exact primary-key slots for an unconstrained insert batch.
/// This retains duplicate detection without materializing a schema-wide
/// committed validation index.
async fn validate_insert_identities_by_point<R>(
    input: &TransactionValidationInput<'_, R>,
) -> Result<(), LixError>
where
    R: StorageAdapterRead,
{
    let mut seen = BTreeSet::new();
    let inserts = input.staged_writes.inserts().collect::<Vec<_>>();
    let mut requests = Vec::with_capacity(inserts.len());
    for insert in &inserts {
        let row = insert.row;
        let identity = (
            row.branch_id.to_string(),
            row.global,
            row.file_id.map(ToString::to_string),
            row.schema_key.to_string(),
            row.entity_pk.clone(),
        );
        if !seen.insert(identity) {
            return Err(LixError::new(
                LixError::CODE_UNIQUE,
                format!("duplicate insert identity for schema '{}'", row.schema_key),
            ));
        }
        requests.push(exact_visible_request(
            &prepared_row_domain(PreparedValidationRow::State(insert.row)),
            row.schema_key,
            row.entity_pk,
            true,
        )?);
    }
    let rows =
        batched_exact_visible_rows(input.state_view, input.active_branch_id, requests).await?;
    for (insert, current) in inserts.iter().zip(rows.iter()) {
        let row = insert.row;
        if current.as_ref().is_some_and(|current| {
            !current.deleted
                && same_insert_identity(PreparedValidationRow::State(insert.row), &current)
        }) {
            return Err(unique_constraint_error(
                crate::transaction::duplicate_insert_identity_message(
                    row.schema_key,
                    row.entity_pk,
                    Some(row.branch_id),
                    insert.origin.or(row.origin),
                ),
            ));
        }
    }
    Ok(())
}

async fn validate_insert_identities<R>(
    input: &TransactionValidationInput<'_, R>,
    all_rows: &NativeValidationIndex,
) -> Result<(), LixError>
where
    R: StorageAdapterRead,
{
    let mut seen = BTreeSet::new();
    for insert in input.staged_writes.inserts() {
        let row = insert.row;
        let identity = (
            row.branch_id.to_string(),
            row.global,
            row.file_id.map(ToString::to_string),
            row.schema_key.to_string(),
            row.entity_pk.clone(),
        );
        if !seen.insert(identity) {
            return Err(LixError::new(
                LixError::CODE_UNIQUE,
                format!("duplicate insert identity for schema '{}'", row.schema_key),
            ));
        }
        let key = (
            row.branch_id.to_string(),
            row.global,
            row.file_id.map(ToString::to_string),
            row.schema_key.to_string(),
            row.entity_pk.clone(),
        );
        if all_rows
            .insert_identity_candidates(&key)
            .iter()
            .map(|&current_index| all_rows.row(current_index))
            .any(|current| {
                !current.deleted && same_insert_identity(PreparedValidationRow::State(row), current)
            })
        {
            return Err(unique_constraint_error(
                crate::transaction::duplicate_insert_identity_message(
                    row.schema_key,
                    row.entity_pk,
                    Some(row.branch_id),
                    insert.origin.or(row.origin),
                ),
            ));
        }
    }
    Ok(())
}

/// Validates the prepared write cohorts against the transaction-owned native
/// state view before ForkTree publication.
pub(crate) async fn validate_prepared_writes_by_branch<R>(
    state_view: &TransactionStateView<R>,
    active_branch_id: &str,
    schema_catalog: &CatalogSnapshot,
    prepared_writes: &crate::transaction::staging::PreparedWriteSet,
    trust_filesystem_planner: bool,
) -> Result<(), LixError>
where
    R: StorageAdapterRead,
{
    let index = prepared_writes.validation_index();
    for scope in index.schema_scopes() {
        let staged_writes = index.validation_set_for_schema_scope(scope);
        if staged_writes.rows().next().is_none() {
            continue;
        }
        validate_prepared_writes(TransactionValidationInput {
            staged_writes,
            schema_catalog,
            state_view,
            active_branch_id,
            trust_filesystem_planner,
        })
        .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn native_row(schema_key: &str, snapshot: &str) -> NativeValidationRow {
        NativeValidationRow {
            key: StateKey {
                schema_key: schema_key.to_owned(),
                file_id: None,
                entity_pk: EntityPk::single("id"),
            },
            branch_id: "branch".to_owned(),
            global: false,
            untracked: false,
            snapshot: Some(SharedStr::from(snapshot)),
            metadata: None,
            deleted: false,
        }
    }

    #[test]
    fn unique_pointer_groups_are_canonical_and_null_is_absent() {
        let snapshot = serde_json::json!({"id": "a", "optional": null});
        assert_eq!(
            pointer_group_value(&snapshot, &[vec!["id".to_owned()]]),
            Some(vec!["\"a\"".to_owned()])
        );
        assert_eq!(
            pointer_group_value(&snapshot, &[vec!["optional".to_owned()]]),
            None
        );
    }

    #[test]
    fn filesystem_descriptor_namespace_decodes_directory_parent_and_name() {
        let row = native_row(
            DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
            r#"{"id":"child","parent_id":"parent","name":"entry"}"#,
        );
        assert_eq!(
            descriptor_namespace_parts(&row).expect("valid descriptor"),
            Some((Some("parent".to_owned()), "entry".to_owned()))
        );
    }

    #[test]
    fn filesystem_descriptor_namespace_rejects_non_string_name() {
        let row = native_row(
            FILE_DESCRIPTOR_SCHEMA_KEY,
            r#"{"id":"file","directory_id":null,"name":7}"#,
        );
        let error = descriptor_namespace_parts(&row).expect_err("invalid name must fail");
        assert_eq!(error.code, LixError::CODE_SCHEMA_VALIDATION);
    }

    fn validation_index_candidate_count(row_count: usize) -> usize {
        let schema = serde_json::json!({
            "x-lix-key": "validation_scale",
            "x-lix-primary-key": ["/id"],
            "x-lix-unique": [["/slug"]],
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "slug": {"type": "string"}
            },
            "required": ["id", "slug"],
            "additionalProperties": false
        });
        let catalog =
            CatalogSnapshot::from_visible_schemas(&[schema]).expect("test schema must compile");
        let mut rows = NativeValidationRows::default();
        for index in 0..row_count {
            rows.rows.push(native_row(
                "validation_scale",
                &format!(r#"{{"id":"{index}","slug":"slug-{index}"}}"#),
            ));
        }

        let validation_index =
            NativeValidationIndex::build(rows, &catalog).expect("test rows must index");
        let unique_group = vec![vec!["slug".to_owned()]];
        let candidate_count = validation_index
            .property_candidates(
                "validation_scale",
                &unique_group,
                &["\"slug-2048\"".to_owned()],
            )
            .len();
        assert_eq!(
            validation_index
                .property_candidates(
                    "validation_scale",
                    &unique_group,
                    &["\"missing\"".to_owned()],
                )
                .len(),
            0
        );
        candidate_count
    }

    #[test]
    fn validation_index_4k_candidate_count_stays_bounded_by_changed_values() {
        assert_eq!(validation_index_candidate_count(4_096), 1);
    }

    #[test]
    fn validation_index_50k_candidate_count_stays_bounded_by_changed_values() {
        assert_eq!(validation_index_candidate_count(50_000), 1);
    }
}
