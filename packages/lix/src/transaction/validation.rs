//! Native transaction validation.
//!
//! This module is deliberately coupled to the concrete transaction state
//! view.  Validation must observe the same staged tracked/untracked overlay
//! that the write will publish; a trait object or a second scan authority
//! would make constraint decisions diverge from the commit plan.

#![allow(clippy::needless_borrow, clippy::unnecessary_wraps)]

use std::collections::BTreeSet;

use serde_json::Value as JsonValue;

use crate::LixError;
use crate::catalog::{CatalogSnapshot, SchemaPlan};
use crate::common::{SharedStr, json_pointer_get, validate_row_metadata};
use crate::domain::Domain;
use crate::entity_pk::EntityPk;
use crate::forktree::{StateCell, StateKey, StateKeyRef, decode_state_key, encode_state_key};
use crate::state::{StateRow, StateRowSource, TransactionStateView, UntrackedStateRow};
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
        let (snapshot, deleted) = match row.value.cell {
            StateCell::Value(value) => (Some(value), false),
            StateCell::Null => (Some(SharedStr::from("null")), false),
            StateCell::Tombstone => (None, true),
        };
        Ok(Self {
            key,
            branch_id: branch_id.to_owned(),
            global: matches!(row.source, StateRowSource::Global),
            untracked: false,
            snapshot,
            metadata: row.value.metadata,
            deleted,
        })
    }

    fn from_untracked(row: UntrackedStateRow) -> Self {
        let (snapshot, deleted) = match row.value.cell {
            StateCell::Value(value) => (Some(value), false),
            StateCell::Null => (Some(SharedStr::from("null")), false),
            StateCell::Tombstone => (None, true),
        };
        Self {
            key: row.key,
            branch_id: uuid::Uuid::from_bytes(*row.owner.as_bytes()).to_string(),
            global: false,
            untracked: true,
            snapshot,
            metadata: row.value.metadata,
            deleted,
        }
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
    fn iter(&self) -> impl Iterator<Item = &NativeValidationRow> {
        self.rows.iter()
    }

    fn extend(&mut self, other: Self) {
        self.rows.extend(other.rows);
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
    schema_keys: &[String],
    entity_pks: &[EntityPk],
    include_tombstones: bool,
) -> Result<NativeValidationRows, LixError>
where
    R: StorageAdapterRead,
{
    let mut output = NativeValidationRows::default();
    if domain.untracked() {
        for row in state_view.untracked_branch_range(None, None, None).await? {
            let row = NativeValidationRow::from_untracked(row);
            if domain_matches(domain, &row)
                && (schema_keys.is_empty() || schema_keys.iter().any(|key| key == row.schema_key()))
                && (entity_pks.is_empty() || entity_pks.iter().any(|pk| pk == row.entity_pk()))
                && (include_tombstones || !row.deleted)
            {
                output.rows.push(row);
            }
        }
    } else {
        // This is the explicit full validation range.  The native state view
        // authenticates ordering, branch/global overlay, tombstones, and the
        // transaction's staged replacement before this filter is applied.
        for row in state_view.range(None, None, None, true).await? {
            let row = NativeValidationRow::from_tracked(row, active_branch_id)?;
            if domain_matches(domain, &row)
                && (schema_keys.is_empty() || schema_keys.iter().any(|key| key == row.schema_key()))
                && (entity_pks.is_empty() || entity_pks.iter().any(|pk| pk == row.entity_pk()))
                && (include_tombstones || !row.deleted)
            {
                output.rows.push(row);
            }
        }
    }
    Ok(output)
}

async fn exact_visible_row<R>(
    state_view: &TransactionStateView<R>,
    active_branch_id: &str,
    domain: &Domain,
    schema_key: &str,
    entity_pk: &EntityPk,
    include_tombstones: bool,
) -> Result<Option<NativeValidationRow>, LixError>
where
    R: StorageAdapterRead,
{
    let key = encode_state_key(StateKeyRef {
        schema_key,
        file_id: match domain.file_filters().as_slice() {
            [crate::NullableKeyFilter::Value(file_id)] => Some(file_id.as_str()),
            [crate::NullableKeyFilter::Null] => None,
            [] => None,
            _ => return Ok(None),
        },
        entity_pk,
    });
    if domain.untracked() {
        let row = state_view
            .untracked_points(&[key], true)
            .await?
            .into_iter()
            .next()
            .flatten()
            .map(NativeValidationRow::from_untracked);
        Ok(row.filter(|row| include_tombstones || !row.deleted))
    } else {
        let row = state_view
            .points(&[key], true)
            .await
            .map_err(LixError::from)?
            .into_iter()
            .next()
            .flatten()
            .map(|row| NativeValidationRow::from_tracked(row, active_branch_id))
            .transpose()?;
        Ok(row.filter(|row| include_tombstones || !row.deleted))
    }
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

fn same_identity(row: PreparedValidationRow<'_>, current: &NativeValidationRow) -> bool {
    row.schema_key() == current.schema_key()
        && row.entity_pk() == current.entity_pk()
        && row.file_id() == current.file_id()
        && row.untracked() == current.untracked
        && row.branch_id() == current.branch_id
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
    all_rows: &NativeValidationRows,
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
            for other in all_rows.iter() {
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
                    return Err(constraint_error(format!(
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
                    return Err(constraint_error(format!(
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
    all_rows: &NativeValidationRows,
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
                .iter()
                .filter(|target| !target.deleted && target.schema_key() == target_schema)
                .any(|target| {
                    target.snapshot_json().ok().flatten().and_then(|value| {
                        pointer_group_value(&value, &foreign_key.referenced_properties)
                    }) == Some(local_value.clone())
                })
                || staged_rows.iter().copied().any(|target| {
                    !row_is_tombstone(target)
                        && target.schema_key() == target_schema
                        && prepared_snapshot(target).ok().flatten().and_then(|value| {
                            pointer_group_value(&value, &foreign_key.referenced_properties)
                        }) == Some(local_value.clone())
                });
            if !found {
                return Err(constraint_error(format!(
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
    all_rows: &NativeValidationRows,
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
        let present = all_rows.iter().any(|candidate| {
            !candidate.deleted
                && candidate.schema_key() == FILE_DESCRIPTOR_SCHEMA_KEY
                && candidate.file_id().is_none()
                && candidate.entity_pk().as_single_string().ok() == Some(file_id)
        }) || staged_rows.iter().copied().any(|candidate| {
            !row_is_tombstone(candidate)
                && candidate.schema_key() == FILE_DESCRIPTOR_SCHEMA_KEY
                && candidate.entity_pk().as_single_string().ok() == Some(file_id)
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
    all_rows: &NativeValidationRows,
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
        let committed_conflict = all_rows.iter().any(|candidate| {
            !candidate.deleted
                && candidate.schema_key() != row.schema_key()
                && matches!(
                    candidate.schema_key(),
                    DIRECTORY_DESCRIPTOR_SCHEMA_KEY | FILE_DESCRIPTOR_SCHEMA_KEY
                )
                && same_scope(candidate, row)
                && candidate.entity_pk() != row.entity_pk()
                && !staged_rows
                    .iter()
                    .copied()
                    .any(|staged| staged_identity_matches(candidate, staged))
                && descriptor_namespace_parts(candidate)
                    .ok()
                    .flatten()
                    .is_some_and(|parts| parts == (parent.clone(), name.clone()))
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
    all_rows: &NativeValidationRows,
) -> Result<(), LixError>
where
    R: StorageAdapterRead,
{
    for tombstone in staged_rows
        .iter()
        .copied()
        .filter(|row| row_is_tombstone(*row))
    {
        let delete_plan = input
            .schema_catalog
            .delete_plan_for_key(tombstone.schema_key());
        if !delete_plan.has_committed_checks() {
            continue;
        }
        let target = exact_visible_row(
            input.state_view,
            input.active_branch_id,
            &prepared_row_domain(tombstone),
            tombstone.schema_key(),
            tombstone.entity_pk(),
            false,
        )
        .await?;
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
            let committed_conflict = all_rows.iter().any(|candidate| {
                !candidate.deleted
                    && candidate.schema_key() == reference.source_key.schema_key
                    && candidate.untracked == tombstone.untracked()
                    && !staged_rows
                        .iter()
                        .copied()
                        .any(|row| staged_identity_matches(candidate, row))
                    && candidate
                        .snapshot_json()
                        .ok()
                        .flatten()
                        .and_then(|snapshot| {
                            pointer_group_value(&snapshot, &reference.foreign_key.local_properties)
                        })
                        == Some(deleted_value.clone())
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
    let mut all_rows = visible_rows(
        input.state_view,
        input.active_branch_id,
        &Domain::any_file(input.active_branch_id, false),
        &[],
        &[],
        true,
    )
    .await?;
    all_rows.extend(
        visible_rows(
            input.state_view,
            input.active_branch_id,
            &Domain::any_file(input.active_branch_id, true),
            &[],
            &[],
            true,
        )
        .await?,
    );
    validate_unique_constraints(&input, &staged_rows, &all_rows).await?;
    validate_foreign_keys(&input, &staged_rows, &all_rows).await?;
    validate_file_ownership(&input, &staged_rows, &all_rows).await?;
    validate_filesystem_namespace(&input, &staged_rows, &all_rows).await?;
    validate_delete_restrictions(&input, &staged_rows, &all_rows).await?;
    validate_insert_identities(&input, &staged_rows).await?;
    Ok(())
}

async fn validate_insert_identities<R>(
    input: &TransactionValidationInput<'_, R>,
    staged_rows: &[PreparedValidationRow<'_>],
) -> Result<(), LixError>
where
    R: StorageAdapterRead,
{
    let mut seen = BTreeSet::new();
    for insert in input.staged_writes.inserts() {
        let row = insert.row;
        let identity = (
            row.branch_id.to_string(),
            row.untracked,
            row.file_id.map(ToString::to_string),
            row.schema_key.to_string(),
            row.entity_pk.clone(),
        );
        if !seen.insert(identity) {
            return Err(LixError::new(
                LixError::CODE_CONSTRAINT_VIOLATION,
                format!("duplicate insert identity for schema '{}'", row.schema_key),
            ));
        }
    }
    let _ = staged_rows;
    Ok(())
}

/// Compatibility name retained only as the concrete transaction hook name;
/// all inputs and behavior are native and no legacy reader is involved.
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
