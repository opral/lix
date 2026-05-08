use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value as JsonValue;

use crate::common::format_json_pointer;
#[cfg(test)]
use crate::common::parse_json_pointer;
use crate::common::{json_pointer_get, validate_row_metadata};
use crate::domain::{Domain, DomainFileScope, DomainRowIdentity};
use crate::entity_identity::{canonical_json_text, EntityIdentity, EntityIdentityError};
#[cfg(test)]
use crate::live_state::LiveStateRowIdentity;
use crate::live_state::{
    LiveStateFilter, LiveStateReader, LiveStateScanRequest, MaterializedLiveStateRow,
};
use crate::schema::{
    format_lix_schema_validation_errors, schema_from_registered_snapshot, validate_schema_amendment,
};
#[cfg(test)]
use crate::schema::{
    is_seed_schema_key, validate_lix_schema, validate_lix_schema_definition, SchemaKey,
};
use crate::schema_catalog::{
    ForeignKeyPlan, SchemaCatalog, SchemaCatalogKey, SchemaPlan, StateForeignKeyPlan,
};
use crate::transaction::staging::duplicate_insert_identity_message;
#[cfg(test)]
use crate::transaction::staging::PreparedWriteSet;
use crate::transaction::staging::{
    PreparedStateRowIdentity, PreparedValidationRow, PreparedWriteValidationSet,
};
#[cfg(test)]
use crate::transaction::types::PreparedStateRow;
use crate::version::{VERSION_DESCRIPTOR_SCHEMA_KEY, VERSION_REF_SCHEMA_KEY};
use crate::LixError;

const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";
const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";
const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const STATE_SURFACE_SCHEMA_KEY: &str = "lix_state";
const MAX_DIRECTORY_PARENT_DEPTH: usize = 1024;

/// Immutable view of the final transaction write set before persistence.
///
/// Validation intentionally runs after staging has coalesced overwrites and
/// hydrated generated fields, but before changelog, tracked-state, untracked
/// state, or binary CAS writes are flushed.
pub(crate) struct TransactionValidationInput<'a> {
    staged_writes: &'a PreparedWriteValidationSet<'a>,
    schema_catalog: &'a SchemaCatalog,
    live_state: &'a dyn LiveStateReader,
}

impl<'a> TransactionValidationInput<'a> {
    pub(crate) fn new(
        staged_writes: &'a PreparedWriteValidationSet<'a>,
        schema_catalog: &'a SchemaCatalog,
        live_state: &'a dyn LiveStateReader,
    ) -> Self {
        Self {
            staged_writes,
            schema_catalog,
            live_state,
        }
    }

    #[cfg(test)]
    fn from_visible_schemas_for_tests(
        staged_writes: &'a PreparedWriteSet,
        visible_schemas: &'a [JsonValue],
        live_state: &'a dyn LiveStateReader,
    ) -> Self {
        let catalog = Box::leak(Box::new(
            SchemaCatalog::from_visible_schemas(visible_schemas)
                .expect("test schema catalog should build"),
        ));
        let validation_set = Box::leak(Box::new(staged_writes.validation_set_for_tests()));
        Self::new(validation_set, catalog, live_state)
    }
}

async fn scan_committed_constraint_rows(
    live_state: &dyn LiveStateReader,
    domain: &Domain,
    schema_keys: Vec<String>,
    entity_ids: Vec<EntityIdentity>,
    include_tombstones: bool,
) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
    let rows = live_state
        .scan_rows(&LiveStateScanRequest {
            filter: LiveStateFilter {
                schema_keys: schema_keys.clone(),
                entity_ids: entity_ids.clone(),
                version_ids: vec![domain.version_id().to_string()],
                file_ids: domain.file_filters(),
                untracked: Some(domain.untracked()),
                include_tombstones,
                ..Default::default()
            },
            ..Default::default()
        })
        .await?;
    Ok(rows
        .into_iter()
        .filter(|row| {
            domain.contains(row)
                && (schema_keys.is_empty() || schema_keys.contains(&row.schema_key))
                && (entity_ids.is_empty() || entity_ids.contains(&row.entity_id))
        })
        .collect())
}

async fn load_committed_constraint_row(
    live_state: &dyn LiveStateReader,
    domain: &Domain,
    schema_key: &str,
    entity_id: EntityIdentity,
    include_tombstones: bool,
) -> Result<Option<MaterializedLiveStateRow>, LixError> {
    Ok(scan_committed_constraint_rows(
        live_state,
        domain,
        vec![schema_key.to_string()],
        vec![entity_id],
        include_tombstones,
    )
    .await?
    .into_iter()
    .next())
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
) -> Result<(), LixError> {
    validate_foreign_key_definitions(input.schema_catalog)?;
    let staged_rows = input.staged_writes.rows().collect::<Vec<_>>();
    let constraint_rows = input.staged_writes.constraint_rows().collect::<Vec<_>>();
    let pending_file_descriptors = PendingFileDescriptorIndex::from_rows(&constraint_rows);
    let pending_schema_domains = PendingSchemaDomains::from_staged_rows(&staged_rows)?;
    validate_registered_schema_identity_is_canonical(&input, &staged_rows).await?;
    let mut pending_constraints = PendingConstraintIndexes::default();
    let mut staged_snapshots = Vec::new();
    for row in &constraint_rows {
        let row = *row;
        let Some(snapshot) = row.snapshot_json() else {
            pending_constraints.remember_tombstone(row);
            continue;
        };
        let schema_plan = schema_plan_for_row(input.schema_catalog, &pending_schema_domains, row)?;
        validate_schema_matches_row(row, schema_plan)?;
        validate_snapshot_content(row, schema_plan)?;
        pending_constraints.remember_row(row, schema_plan, snapshot)?;
    }
    for row in &staged_rows {
        let row = *row;
        validate_staged_row_shape(row)?;
        validate_staged_row_metadata(row)?;
        let schema_plan = schema_plan_for_row(input.schema_catalog, &pending_schema_domains, row)?;
        validate_schema_matches_row(row, schema_plan)?;
        let snapshot = validate_snapshot_content(row, schema_plan)?;
        if let Some(snapshot) = snapshot {
            validate_file_owner_reference(&input, &pending_file_descriptors, row).await?;
            validate_primary_key_identity(row, schema_plan, snapshot)?;
            pending_constraints.remember_foreign_key_references(row, schema_plan, snapshot)?;
            staged_snapshots.push((row, schema_plan, snapshot));
        } else {
            pending_constraints.remember_tombstone(row);
        }
    }
    let unresolved_foreign_keys =
        validate_pending_foreign_keys(&pending_constraints, &staged_snapshots)?;
    validate_pending_delete_restrictions(input.schema_catalog, &pending_constraints)?;
    let unresolved_foreign_keys =
        validate_committed_foreign_keys(&input, &pending_constraints, &unresolved_foreign_keys)
            .await?;
    reject_unresolved_foreign_keys(&unresolved_foreign_keys)?;
    validate_committed_delete_restrictions(&input, input.schema_catalog, &pending_constraints)
        .await?;
    validate_file_descriptor_delete_restrictions(&input, &pending_constraints).await?;
    validate_version_ref_delete_restrictions(&input, &pending_constraints).await?;
    validate_committed_insert_identities(&input, &pending_constraints).await?;
    validate_committed_unique_constraints(&input, &pending_constraints).await?;
    validate_directory_descriptor_parent_graph(&input, &staged_rows, &constraint_rows).await?;
    validate_filesystem_namespace(&input, &staged_rows).await?;
    Ok(())
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
        let mut parents = committed_directory_parent_map(input.live_state, &scope).await?;
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
        let Some(row) = load_committed_constraint_row(
            input.live_state,
            &pending_row.domain().with_exact_file_scope(None),
            REGISTERED_SCHEMA_KEY,
            pending_row.entity_id().clone(),
            false,
        )
        .await?
        else {
            continue;
        };
        let Some(snapshot_content) = row.snapshot_content.as_deref() else {
            continue;
        };
        let snapshot = parse_registered_schema_snapshot(snapshot_content)?;
        let pending_snapshot = pending_row
            .snapshot_json()
            .expect("pending registered schema row has snapshot_content");
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
    live_state: &dyn LiveStateReader,
    scope: &DirectoryDescriptorScope,
) -> Result<BTreeMap<String, Option<String>>, LixError> {
    let mut parents = BTreeMap::new();
    for domain in scope.domain.directory_parent_domains() {
        let rows = scan_committed_constraint_rows(
            live_state,
            &domain,
            vec![DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string()],
            Vec::new(),
            false,
        )
        .await?;
        for row in rows {
            if !committed_directory_row_is_in_domain(&row, scope, &domain) {
                continue;
            }
            let Some(snapshot_content) = row.snapshot_content.as_deref() else {
                continue;
            };
            let snapshot = parse_directory_descriptor_snapshot(snapshot_content)?;
            parents.insert(snapshot.id, snapshot.parent_id);
        }
    }
    Ok(parents)
}

fn committed_directory_row_is_in_domain(
    row: &MaterializedLiveStateRow,
    _scope: &DirectoryDescriptorScope,
    domain: &Domain,
) -> bool {
    row.schema_key == DIRECTORY_DESCRIPTOR_SCHEMA_KEY && domain.contains(row)
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
        let id = row.entity_id().as_single_string_owned()?;
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
    entity_id: EntityIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum FilesystemNamespaceOccupant {
    Directory {
        entity_id: EntityIdentity,
        parent_id: Option<String>,
        name: String,
    },
    File {
        entity_id: EntityIdentity,
        directory_id: Option<String>,
        entry_name: String,
    },
}

impl FilesystemNamespaceOccupant {
    fn entity_id(&self) -> &EntityIdentity {
        match self {
            Self::Directory { entity_id, .. } | Self::File { entity_id, .. } => entity_id,
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
    // validated in the global scope and may be projected into version reads, but
    // projected globals do not participate in version-local constraint checks.
    let domains = staged_filesystem_namespace_domains(staged_rows);
    for domain in domains {
        let mut occupants =
            committed_filesystem_namespace_occupants(input.live_state, &domain).await?;
        apply_staged_filesystem_namespace_rows(staged_rows, &domain, &mut occupants)?;
        validate_filesystem_namespace_occupants(&domain, occupants)?;
    }
    Ok(())
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
        .collect()
}

async fn committed_filesystem_namespace_occupants(
    live_state: &dyn LiveStateReader,
    domain: &Domain,
) -> Result<BTreeMap<FilesystemNamespaceIdentity, FilesystemNamespaceOccupant>, LixError> {
    let rows = scan_committed_constraint_rows(
        live_state,
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
    for row in rows {
        if !committed_filesystem_row_is_in_domain(&row, domain) {
            continue;
        }
        if let Some((identity, occupant)) = filesystem_namespace_occupant_from_live_row(&row)? {
            occupants.insert(identity, occupant);
        }
    }
    Ok(occupants)
}

fn committed_filesystem_row_is_in_domain(row: &MaterializedLiveStateRow, domain: &Domain) -> bool {
    (row.schema_key == DIRECTORY_DESCRIPTOR_SCHEMA_KEY
        || row.schema_key == FILE_DESCRIPTOR_SCHEMA_KEY)
        && domain.contains(row)
}

fn apply_staged_filesystem_namespace_rows(
    staged_rows: &[PreparedValidationRow<'_>],
    domain: &Domain,
    occupants: &mut BTreeMap<FilesystemNamespaceIdentity, FilesystemNamespaceOccupant>,
) -> Result<(), LixError> {
    for row in staged_rows {
        if (row.schema_key() != DIRECTORY_DESCRIPTOR_SCHEMA_KEY
            && row.schema_key() != FILE_DESCRIPTOR_SCHEMA_KEY)
            || row.domain() != *domain
        {
            continue;
        }
        let identity = FilesystemNamespaceIdentity {
            schema_key: row.schema_key().to_string(),
            entity_id: row.entity_id().clone(),
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
    row: &MaterializedLiveStateRow,
) -> Result<Option<(FilesystemNamespaceIdentity, FilesystemNamespaceOccupant)>, LixError> {
    let Some(snapshot_content) = row.snapshot_content.as_deref() else {
        return Ok(None);
    };
    let identity = FilesystemNamespaceIdentity {
        schema_key: row.schema_key.clone(),
        entity_id: row.entity_id.clone(),
    };
    let occupant = match row.schema_key.as_str() {
        DIRECTORY_DESCRIPTOR_SCHEMA_KEY => {
            directory_namespace_occupant(&row.entity_id, snapshot_content)?
        }
        FILE_DESCRIPTOR_SCHEMA_KEY => file_namespace_occupant(&row.entity_id, snapshot_content)?,
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
            directory_namespace_occupant_from_value(row.entity_id(), snapshot)
        }
        FILE_DESCRIPTOR_SCHEMA_KEY => file_namespace_occupant_from_value(row.entity_id(), snapshot),
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
    entity_id: &EntityIdentity,
    snapshot_content: &str,
) -> Result<FilesystemNamespaceOccupant, LixError> {
    let snapshot = parse_directory_descriptor_snapshot(snapshot_content)?;
    Ok(FilesystemNamespaceOccupant::Directory {
        entity_id: entity_id.clone(),
        parent_id: snapshot.parent_id,
        name: snapshot.name,
    })
}

fn directory_namespace_occupant_from_value(
    entity_id: &EntityIdentity,
    snapshot: &JsonValue,
) -> Result<FilesystemNamespaceOccupant, LixError> {
    let snapshot = directory_descriptor_snapshot_from_value(snapshot)?;
    Ok(FilesystemNamespaceOccupant::Directory {
        entity_id: entity_id.clone(),
        parent_id: snapshot.parent_id,
        name: snapshot.name,
    })
}

fn file_namespace_occupant(
    entity_id: &EntityIdentity,
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
        entity_id: entity_id.clone(),
        directory_id: snapshot.directory_id,
        entry_name: snapshot.name,
    })
}

fn file_namespace_occupant_from_value(
    entity_id: &EntityIdentity,
    snapshot: &JsonValue,
) -> Result<FilesystemNamespaceOccupant, LixError> {
    let snapshot = file_descriptor_snapshot_from_value(snapshot)?;
    Ok(FilesystemNamespaceOccupant::File {
        entity_id: entity_id.clone(),
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
        .entity_id()
        .as_single_string_owned()
        .unwrap_or_else(|_| "<non-string-entity-id>".to_string());
    let conflicting_id = conflicting
        .entity_id()
        .as_single_string_owned()
        .unwrap_or_else(|_| "<non-string-entity-id>".to_string());
    LixError::new(
        LixError::CODE_UNIQUE,
        format!(
            "filesystem namespace conflict in version '{}' for parent {parent:?} entry {entry_name:?}: {} '{}' conflicts with {} '{}'",
            domain.version_id(),
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
            "lix_directory_descriptor parent_id cycle in version '{}': directory '{}' reaches ancestor '{}' twice",
            scope.domain.version_id(), start_id, repeated_id
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
            "lix_directory_descriptor parent_id chain in version '{}' for directory '{}' references missing directory '{}'",
            scope.domain.version_id(), start_id, missing_id
        ),
    )
}

fn directory_parent_depth_error(scope: &DirectoryDescriptorScope, start_id: &str) -> LixError {
    LixError::new(
        LixError::CODE_CONSTRAINT_VIOLATION,
        format!(
            "lix_directory_descriptor parent_id chain in version '{}' for directory '{}' exceeds maximum depth {}",
            scope.domain.version_id(), start_id, MAX_DIRECTORY_PARENT_DEPTH
        ),
    )
}

async fn validate_committed_insert_identities(
    input: &TransactionValidationInput<'_>,
    pending_constraints: &PendingConstraintIndexes,
) -> Result<(), LixError> {
    for (identity, origin) in input.staged_writes.insert_identities() {
        let Some(pending_identity) =
            pending_constraints.has_identity_target_for_staged_identity(identity)
        else {
            continue;
        };
        let Some(committed_row) = load_committed_constraint_row(
            input.live_state,
            pending_identity.domain(),
            pending_identity.schema_key(),
            pending_identity.entity_id_owned(),
            false,
        )
        .await?
        else {
            continue;
        };
        if committed_row.snapshot_content.is_none()
            || pending_constraints.tombstones_identity(&committed_row)
        {
            continue;
        }
        return Err(LixError::new(
            LixError::CODE_UNIQUE,
            duplicate_insert_identity_message(
                identity.schema_key(),
                identity.entity_id(),
                None,
                origin,
            ),
        ));
    }
    Ok(())
}

async fn validate_version_ref_delete_restrictions(
    input: &TransactionValidationInput<'_>,
    pending_constraints: &PendingConstraintIndexes,
) -> Result<(), LixError> {
    for tombstone in &pending_constraints.tombstones {
        if tombstone.identity.schema_key() != VERSION_REF_SCHEMA_KEY {
            continue;
        }

        for source_domain in tombstone
            .identity
            .domain()
            .version_descriptor_domains_for_ref_delete()
        {
            let descriptor_identity = DomainRowIdentity::in_domain(
                source_domain,
                VERSION_DESCRIPTOR_SCHEMA_KEY,
                tombstone.identity.entity_id_owned(),
            );
            if pending_constraints.tombstones_target_identity(&descriptor_identity) {
                continue;
            }
            if pending_constraints.has_identity_target(&descriptor_identity) {
                return Err(version_ref_delete_restriction_error(
                    &tombstone.identity,
                    &descriptor_identity,
                )?);
            }

            let Some(descriptor_row) = load_committed_constraint_row(
                input.live_state,
                descriptor_identity.domain(),
                descriptor_identity.schema_key(),
                descriptor_identity.entity_id_owned(),
                false,
            )
            .await?
            else {
                continue;
            };
            if descriptor_row.snapshot_content.is_some()
                && !pending_constraints.tombstones_identity(&descriptor_row)
            {
                return Err(version_ref_delete_restriction_error(
                    &tombstone.identity,
                    &descriptor_identity,
                )?);
            }
        }
    }
    Ok(())
}

fn version_ref_delete_restriction_error(
    ref_identity: &DomainRowIdentity,
    descriptor_identity: &DomainRowIdentity,
) -> Result<LixError, LixError> {
    Ok(LixError::new(
        LixError::CODE_FOREIGN_KEY,
        format!(
            "cannot delete '{}' row '{}' in version '{}' because matching '{}' row '{}' would remain without a version ref",
            ref_identity.schema_key(),
            ref_identity.entity_id().as_single_string_owned()?,
            ref_identity.domain().version_id(),
            descriptor_identity.schema_key(),
            descriptor_identity.entity_id().as_single_string_owned()?,
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
            if row.schema_key() != FILE_DESCRIPTOR_SCHEMA_KEY || row.file_id().is_some() {
                continue;
            }
            if row.entity_id().as_single_string_owned().is_ok() {
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
        self.by_identity
            .get(&DomainRowIdentity::in_domain(
                domain.with_exact_file_scope(None),
                FILE_DESCRIPTOR_SCHEMA_KEY,
                EntityIdentity::single(file_id),
            ))
            .copied()
    }
}

async fn validate_file_owner_reference(
    input: &TransactionValidationInput<'_>,
    pending_file_descriptors: &PendingFileDescriptorIndex,
    row: PreparedValidationRow<'_>,
) -> Result<(), LixError> {
    let Some(file_id) = row.file_id().as_deref() else {
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
        if committed_file_descriptor_exists_in_domain(input.live_state, domain, file_id).await? {
            return Ok(());
        }
    }

    Err(missing_file_owner_reference_error(row, file_id)?)
}

async fn committed_file_descriptor_exists_in_domain(
    live_state: &dyn LiveStateReader,
    domain: &Domain,
    file_id: &str,
) -> Result<bool, LixError> {
    let Some(row) = load_committed_constraint_row(
        live_state,
        &domain.with_exact_file_scope(None),
        FILE_DESCRIPTOR_SCHEMA_KEY,
        EntityIdentity::single(file_id),
        false,
    )
    .await?
    else {
        return Ok(false);
    };
    Ok(row.snapshot_content.is_some()
        && row.schema_key == FILE_DESCRIPTOR_SCHEMA_KEY
        && row.entity_id == EntityIdentity::single(file_id)
        && row.file_id.is_none())
}

fn missing_file_owner_reference_error(
    row: PreparedValidationRow<'_>,
    file_id: &str,
) -> Result<LixError, LixError> {
    Ok(LixError::new(
        LixError::CODE_FILE_NOT_FOUND,
            format!(
                "file ownership validation failed for schema '{}': entity '{}' references missing file_id '{}' in effective file scope for version '{}'",
                row.schema_key(),
                row.entity_id().as_json_array_text()?,
                file_id,
                row.version_id()
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
        .with_hint("Schema definitions are scoped by version and durability only; write them with null file_id."));
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
                .insert(row.domain());
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
        let row_domain = row.domain();
        if domains.contains(&row_domain) {
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

fn schema_plan_for_row<'a>(
    schema_catalog: &'a SchemaCatalog,
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
    if let Err(errors) = schema_plan.compiled_schema.validate(&snapshot) {
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
    let derived = EntityIdentity::from_primary_key_paths(snapshot, &primary_key_paths)
        .map_err(|error| primary_key_identity_error(row, &primary_key_paths, error))?;
    if row.entity_id() != &derived {
        return Err(LixError::new(
            LixError::CODE_UNIQUE,
            format!(
                "primary-key constraint violation on schema '{}': entity_id '{}' does not match derived primary key '{}'",
                row.schema_key(),
                row.entity_id().as_json_array_text()?,
                derived.as_json_array_text()?
            ),
        ));
    }
    Ok(())
}

#[derive(Default)]
struct PendingConstraintIndexes {
    unique_values: BTreeMap<PendingUniqueKey, EntityIdentity>,
    identity_targets: Vec<PendingIdentityTarget>,
    fk_targets: BTreeMap<PendingForeignKeyTargetKey, Vec<PendingForeignKeyTarget>>,
    fk_references: BTreeMap<PendingForeignKeyReferenceTarget, Vec<PendingForeignKeyReference>>,
    tombstones: Vec<PendingTombstone>,
}

impl PendingConstraintIndexes {
    fn remember_tombstone(&mut self, row: PreparedValidationRow<'_>) {
        self.tombstones.push(PendingTombstone {
            identity: row.domain_row_identity(),
        });
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
        self.identity_targets.push(PendingIdentityTarget {
            identity: row.domain_row_identity(),
        });
    }

    fn remember_primary_key_target(
        &mut self,
        row: PreparedValidationRow<'_>,
        schema_plan: &SchemaPlan,
        snapshot: &JsonValue,
    ) {
        if let Some(primary_key_paths) = schema_plan.primary_key.as_ref() {
            self.remember_fk_target(row, &primary_key_paths, snapshot);
        }
    }

    fn remember_unique_targets(
        &mut self,
        row: PreparedValidationRow<'_>,
        schema_plan: &SchemaPlan,
        snapshot: &JsonValue,
    ) -> Result<(), LixError> {
        for unique_paths in &schema_plan.uniques {
            let Some(value) = UniqueConstraintValue::from_snapshot(snapshot, &unique_paths) else {
                continue;
            };
            self.remember_fk_target(row, &unique_paths, snapshot);
            let key = PendingUniqueKey {
                schema_key: row.schema_key().to_string(),
                domain: row.domain(),
                pointer_group: unique_paths.clone(),
                value,
            };
            if let Some(existing_entity_id) = self
                .unique_values
                .insert(key.clone(), row.entity_id().clone())
            {
                if existing_entity_id != *row.entity_id() {
                    return Err(LixError::new(
                        LixError::CODE_UNIQUE,
                        format!(
                            "unique constraint violation on {}.{} for value {}: rows '{}' and '{}' conflict",
                            row.schema_key(),
                            format_pointer_group(&key.pointer_group),
                            key.value.display(),
                            existing_entity_id.as_json_array_text()?,
                            row.entity_id().as_json_array_text()?
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
                entity_id: row.entity_id().clone(),
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
                domain: row.domain(),
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

        for foreign_key in &schema_plan.state_foreign_keys {
            let target = PendingForeignKeyReferenceTarget::StateSurfaceIdentity(
                state_surface_target_identity(row.domain(), foreign_key, snapshot)?,
            );
            self.fk_references
                .entry(target)
                .or_default()
                .push(PendingForeignKeyReference {
                    identity: row.domain_row_identity(),
                });
        }
        Ok(())
    }

    fn tombstones_identity(&self, row: &MaterializedLiveStateRow) -> bool {
        let identity = DomainRowIdentity::from_live_row(row);
        self.tombstones
            .iter()
            .any(|tombstone| tombstone.identity == identity)
    }

    fn has_identity_target(&self, identity: &DomainRowIdentity) -> bool {
        self.identity_targets
            .iter()
            .any(|target| target.identity == *identity)
    }

    fn has_reachable_identity_target(&self, identity: &DomainRowIdentity) -> bool {
        identity
            .reachable_target_identities()
            .iter()
            .any(|candidate| self.has_identity_target(candidate))
    }

    fn has_identity_target_for_staged_identity(
        &self,
        identity: &PreparedStateRowIdentity,
    ) -> Option<DomainRowIdentity> {
        let expected_domain = identity.domain();
        self.identity_targets
            .iter()
            .find(|target| {
                target.identity.matches_parts(
                    &expected_domain,
                    identity.schema_key(),
                    identity.entity_id(),
                )
            })
            .map(|target| target.identity.clone())
    }

    fn tombstones_target_identity(&self, identity: &DomainRowIdentity) -> bool {
        self.tombstones
            .iter()
            .any(|tombstone| tombstone.identity == *identity)
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
        version_id: &str,
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
            domain: Domain::exact_file(version_id.to_string(), false, file_id.map(str::to_string)),
            pointer_group,
            value,
        });
        Ok(self.fk_references.contains_key(&key))
    }

    #[cfg(test)]
    fn has_fk_reference_to_identity(&self, identity: DomainRowIdentity) -> bool {
        self.fk_references
            .contains_key(&PendingForeignKeyReferenceTarget::StateSurfaceIdentity(
                identity,
            ))
    }

    #[cfg(test)]
    fn has_fk_target(
        &self,
        schema_key: &str,
        version_id: &str,
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
            domain: Domain::exact_file(version_id.to_string(), false, file_id.map(str::to_string)),
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
struct PendingIdentityTarget {
    identity: DomainRowIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingForeignKeyTarget {
    entity_id: EntityIdentity,
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
struct PendingForeignKeyTargetKey {
    schema_key: String,
    domain: Domain,
    pointer_group: Vec<Vec<String>>,
    value: UniqueConstraintValue,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum PendingForeignKeyReferenceTarget {
    Key(PendingForeignKeyTargetKey),
    StateSurfaceIdentity(DomainRowIdentity),
}

fn validate_pending_delete_restrictions(
    schema_catalog: &SchemaCatalog,
    pending_constraints: &PendingConstraintIndexes,
) -> Result<(), LixError> {
    for tombstone in &pending_constraints.tombstones {
        let identity_targets = tombstone
            .identity
            .source_identities_that_can_reach()
            .into_iter()
            .map(PendingForeignKeyReferenceTarget::StateSurfaceIdentity)
            .collect::<Vec<_>>();
        reject_pending_delete_references(
            &tombstone.identity,
            &identity_targets,
            pending_constraints.active_references_to_any(&identity_targets),
        )?;

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
                        value: UniqueConstraintValue::from_entity_identity(
                            tombstone.identity.entity_id(),
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
            "cannot delete '{}' row '{}' in version '{}' because pending row '{}' references it{}",
            deleted_identity.schema_key(),
            deleted_identity.entity_id().as_json_array_text()?,
            deleted_identity.domain().version_id(),
            reference.identity.entity_id().as_json_array_text()?,
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
        PendingForeignKeyReferenceTarget::StateSurfaceIdentity(target) => Ok(format!(
            " through '{}:{}'",
            target.schema_key(),
            target.entity_id().as_json_array_text()?
        )),
    }
}

async fn validate_committed_delete_restrictions(
    input: &TransactionValidationInput<'_>,
    schema_catalog: &SchemaCatalog,
    pending_constraints: &PendingConstraintIndexes,
) -> Result<(), LixError> {
    for tombstone in &pending_constraints.tombstones {
        for source_plan in schema_catalog.plans() {
            for foreign_key in &source_plan.foreign_keys {
                if foreign_key.referenced_schema.schema_key == tombstone.identity.schema_key() {
                    validate_committed_normal_delete_restriction(
                        input.live_state,
                        pending_constraints,
                        tombstone,
                        &source_plan.key,
                        foreign_key,
                    )
                    .await?;
                }
            }
            for foreign_key in &source_plan.state_foreign_keys {
                validate_committed_state_surface_delete_restriction(
                    input.live_state,
                    pending_constraints,
                    tombstone,
                    &source_plan.key,
                    foreign_key,
                )
                .await?;
            }
        }
    }
    Ok(())
}

async fn validate_file_descriptor_delete_restrictions(
    input: &TransactionValidationInput<'_>,
    pending_constraints: &PendingConstraintIndexes,
) -> Result<(), LixError> {
    for tombstone in &pending_constraints.tombstones {
        if tombstone.identity.schema_key() != FILE_DESCRIPTOR_SCHEMA_KEY {
            continue;
        }
        if !tombstone.identity.domain().is_exact_file(&None) {
            continue;
        }
        let file_id = tombstone.identity.entity_id().as_single_string_owned()?;
        for source_domain in tombstone
            .identity
            .domain()
            .file_scoped_row_domains_for_file_descriptor_delete()
        {
            let rows = scan_committed_constraint_rows(
                input.live_state,
                &source_domain.with_exact_file_scope(Some(file_id.clone())),
                Vec::new(),
                Vec::new(),
                false,
            )
            .await?;

            for row in rows {
                if pending_constraints.tombstones_identity(&row) || row.snapshot_content.is_none() {
                    continue;
                }
                return Err(LixError::new(
                    LixError::CODE_FOREIGN_KEY,
                    format!(
                        "cannot delete file descriptor '{}' in version '{}' because committed row '{}' in schema '{}' is still scoped to that file",
                        file_id,
                        tombstone.identity.domain().version_id(),
                        row.entity_id.as_json_array_text()?,
                        row.schema_key,
                    ),
                ));
            }
        }
    }
    Ok(())
}

async fn validate_committed_normal_delete_restriction(
    live_state: &dyn LiveStateReader,
    pending_constraints: &PendingConstraintIndexes,
    tombstone: &PendingTombstone,
    source_key: &SchemaCatalogKey,
    foreign_key: &ForeignKeyPlan,
) -> Result<(), LixError> {
    let Some(deleted_value) =
        committed_deleted_row_value(live_state, tombstone, &foreign_key.referenced_properties)
            .await?
    else {
        return Ok(());
    };
    for source_domain in tombstone.identity.domain().fk_source_domains_for_target() {
        let rows = scan_committed_constraint_rows(
            live_state,
            &source_domain,
            vec![source_key.schema_key.clone()],
            Vec::new(),
            false,
        )
        .await?;

        for row in rows {
            if pending_constraints.tombstones_identity(&row) {
                continue;
            }
            let Some(snapshot_content) = row.snapshot_content.as_deref() else {
                continue;
            };
            let snapshot = parse_committed_snapshot(&row, snapshot_content)?;
            if UniqueConstraintValue::from_snapshot_non_null(
                &snapshot,
                &foreign_key.local_properties,
            )
            .as_ref()
                == Some(&deleted_value)
            {
                return Err(committed_delete_restriction_error(
                    &tombstone.identity,
                    &row,
                    &foreign_key.local_properties,
                )?);
            }
        }
    }
    Ok(())
}

async fn validate_committed_state_surface_delete_restriction(
    live_state: &dyn LiveStateReader,
    pending_constraints: &PendingConstraintIndexes,
    tombstone: &PendingTombstone,
    source_key: &SchemaCatalogKey,
    foreign_key: &StateForeignKeyPlan,
) -> Result<(), LixError> {
    for source_domain in tombstone.identity.domain().fk_source_domains_for_target() {
        let source_domain = source_domain.with_file_scope(DomainFileScope::Any);
        let rows = scan_committed_constraint_rows(
            live_state,
            &source_domain,
            vec![source_key.schema_key.clone()],
            Vec::new(),
            false,
        )
        .await?;

        for row in rows {
            if pending_constraints.tombstones_identity(&row) {
                continue;
            }
            let Some(snapshot_content) = row.snapshot_content.as_deref() else {
                continue;
            };
            let snapshot = parse_committed_snapshot(&row, snapshot_content)?;
            let target_identity =
                state_surface_target_identity(Domain::for_live_row(&row), foreign_key, &snapshot)?;
            if target_identity
                .reachable_target_identities()
                .contains(&tombstone.identity)
            {
                return Err(committed_delete_restriction_error(
                    &tombstone.identity,
                    &row,
                    &foreign_key.local_properties(),
                )?);
            }
        }
    }
    Ok(())
}

async fn committed_deleted_row_value(
    live_state: &dyn LiveStateReader,
    tombstone: &PendingTombstone,
    referenced_properties: &[Vec<String>],
) -> Result<Option<UniqueConstraintValue>, LixError> {
    let Some(row) = load_committed_constraint_row(
        live_state,
        tombstone.identity.domain(),
        tombstone.identity.schema_key(),
        tombstone.identity.entity_id_owned(),
        true,
    )
    .await?
    else {
        return Ok(None);
    };
    let Some(snapshot_content) = row.snapshot_content.as_deref() else {
        return Ok(None);
    };
    let snapshot = parse_committed_snapshot(&row, snapshot_content)?;
    Ok(UniqueConstraintValue::from_snapshot(
        &snapshot,
        referenced_properties,
    ))
}

fn committed_delete_restriction_error(
    deleted_identity: &DomainRowIdentity,
    referencing_row: &MaterializedLiveStateRow,
    local_properties: &[Vec<String>],
) -> Result<LixError, LixError> {
    Ok(LixError::new(
        LixError::CODE_FOREIGN_KEY,
        format!(
            "cannot delete '{}' row '{}' in version '{}' because committed row '{}' references it through {}",
            deleted_identity.schema_key(),
            deleted_identity.entity_id().as_json_array_text()?,
            deleted_identity.domain().version_id(),
            referencing_row.entity_id.as_json_array_text()?,
            format_pointer_group(local_properties)
        ),
    ))
}

fn parse_committed_snapshot(
    row: &MaterializedLiveStateRow,
    snapshot_content: &str,
) -> Result<JsonValue, LixError> {
    serde_json::from_str::<JsonValue>(snapshot_content).map_err(|error| {
        LixError::new(
            LixError::CODE_SCHEMA_VALIDATION,
            format!(
                "committed snapshot_content for schema '{}' is invalid JSON: {error}",
                row.schema_key
            ),
        )
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct UnresolvedForeignKeyCheck {
    source_identity: DomainRowIdentity,
    source_schema_key: String,
    source_pointer_group: Vec<Vec<String>>,
    target: UnresolvedForeignKeyTarget,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum UnresolvedForeignKeyTarget {
    Key(PendingForeignKeyTargetKey),
    StateSurfaceIdentity(DomainRowIdentity),
}

fn validate_pending_foreign_keys(
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
            if let Some(check) = validate_pending_normal_foreign_key(
                *row,
                foreign_key,
                local_value,
                pending_constraints,
            )? {
                unresolved.push(check);
            }
        }
        for foreign_key in &schema_plan.state_foreign_keys {
            if let Some(check) = validate_pending_state_surface_foreign_key(
                *row,
                foreign_key,
                snapshot,
                pending_constraints,
            )? {
                unresolved.push(check);
            }
        }
    }
    Ok(unresolved)
}

fn validate_pending_normal_foreign_key(
    row: PreparedValidationRow<'_>,
    foreign_key: &ForeignKeyPlan,
    local_value: UniqueConstraintValue,
    pending_constraints: &PendingConstraintIndexes,
) -> Result<Option<UnresolvedForeignKeyCheck>, LixError> {
    let key = PendingForeignKeyTargetKey {
        schema_key: foreign_key.referenced_schema.schema_key.clone(),
        domain: row.domain(),
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
        target: UnresolvedForeignKeyTarget::Key(key),
    }))
}

fn validate_pending_state_surface_foreign_key(
    row: PreparedValidationRow<'_>,
    foreign_key: &StateForeignKeyPlan,
    snapshot: &JsonValue,
    pending_constraints: &PendingConstraintIndexes,
) -> Result<Option<UnresolvedForeignKeyCheck>, LixError> {
    let local_properties = foreign_key.local_properties();
    let target_identity = state_surface_target_identity(row.domain(), foreign_key, snapshot)?;
    if pending_constraints.has_reachable_identity_target(&target_identity) {
        return Ok(None);
    }
    Ok(Some(UnresolvedForeignKeyCheck {
        source_identity: row.domain_row_identity(),
        source_schema_key: row.schema_key().to_string(),
        source_pointer_group: local_properties,
        target: UnresolvedForeignKeyTarget::StateSurfaceIdentity(target_identity),
    }))
}

async fn validate_committed_foreign_keys(
    input: &TransactionValidationInput<'_>,
    pending_constraints: &PendingConstraintIndexes,
    unresolved_checks: &[UnresolvedForeignKeyCheck],
) -> Result<Vec<UnresolvedForeignKeyCheck>, LixError> {
    let mut still_unresolved = Vec::new();
    for check in unresolved_checks {
        let resolved = match &check.target {
            UnresolvedForeignKeyTarget::Key(target) => {
                committed_normal_foreign_key_target_exists(
                    input.live_state,
                    pending_constraints,
                    target,
                )
                .await?
            }
            UnresolvedForeignKeyTarget::StateSurfaceIdentity(target_identity) => {
                committed_state_surface_foreign_key_target_exists(
                    input.live_state,
                    pending_constraints,
                    target_identity,
                )
                .await?
            }
        };
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
            "foreign key on schema '{}' row '{}' via {} has no matching target in version '{}'{}",
            check.source_schema_key,
            check.source_identity.entity_id().as_json_array_text()?,
            format_pointer_group(&check.source_pointer_group),
            check.source_identity.domain().version_id(),
            unresolved_foreign_key_target_description(&check.target)?
        ),
    ))
}

fn unresolved_foreign_key_target_description(
    target: &UnresolvedForeignKeyTarget,
) -> Result<String, LixError> {
    match target {
        UnresolvedForeignKeyTarget::Key(target) => Ok(format!(
            " for target '{}.{}' value {}",
            target.schema_key,
            format_pointer_group(&target.pointer_group),
            target.value.display()
        )),
        UnresolvedForeignKeyTarget::StateSurfaceIdentity(target) => Ok(format!(
            " for target '{}:{}'",
            target.schema_key(),
            target.entity_id().as_json_array_text()?
        )),
    }
}

async fn committed_normal_foreign_key_target_exists(
    live_state: &dyn LiveStateReader,
    pending_constraints: &PendingConstraintIndexes,
    target: &PendingForeignKeyTargetKey,
) -> Result<bool, LixError> {
    for domain in target.domain.fk_target_domains() {
        let rows = scan_committed_constraint_rows(
            live_state,
            &domain,
            vec![target.schema_key.clone()],
            Vec::new(),
            false,
        )
        .await?;

        for row in rows {
            if pending_constraints.tombstones_identity(&row) {
                continue;
            }
            if row.schema_key != target.schema_key {
                continue;
            }
            let Some(snapshot_content) = row.snapshot_content.as_deref() else {
                continue;
            };
            let snapshot =
                serde_json::from_str::<JsonValue>(snapshot_content).map_err(|error| {
                    LixError::new(
                        LixError::CODE_SCHEMA_VALIDATION,
                        format!(
                            "committed snapshot_content for schema '{}' is invalid JSON: {error}",
                            row.schema_key
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

async fn committed_state_surface_foreign_key_target_exists(
    live_state: &dyn LiveStateReader,
    pending_constraints: &PendingConstraintIndexes,
    target_identity: &DomainRowIdentity,
) -> Result<bool, LixError> {
    for candidate in target_identity.reachable_target_identities() {
        let rows = scan_committed_constraint_rows(
            live_state,
            candidate.domain(),
            vec![candidate.schema_key_owned()],
            vec![candidate.entity_id_owned()],
            false,
        )
        .await?;
        for row in rows {
            if pending_constraints.tombstones_identity(&row) {
                continue;
            }
            if candidate.matches_parts(&Domain::for_live_row(&row), &row.schema_key, &row.entity_id)
            {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn state_surface_target_identity(
    source_domain: Domain,
    foreign_key: &StateForeignKeyPlan,
    snapshot: &JsonValue,
) -> Result<DomainRowIdentity, LixError> {
    let entity_id =
        state_surface_local_json_value(snapshot, &foreign_key.entity_id_property, "entity_id")?;
    let schema_key =
        state_surface_local_value(snapshot, &foreign_key.schema_key_property, "schema_key")?;
    let file_id =
        state_surface_nullable_local_value(snapshot, &foreign_key.file_id_property, "file_id")?;
    Ok(DomainRowIdentity::in_domain(
        source_domain.with_exact_file_scope(file_id),
        schema_key,
        EntityIdentity::from_json_array_value(entity_id).map_err(|error| {
            LixError::new(
                LixError::CODE_FOREIGN_KEY,
                format!("state-surface foreign key entity_id is invalid: {error}"),
            )
        })?,
    ))
}

fn state_surface_local_json_value<'a>(
    snapshot: &'a JsonValue,
    local_pointer: &[String],
    state_address_part: &str,
) -> Result<&'a JsonValue, LixError> {
    state_surface_optional_local_json_value(snapshot, local_pointer)?.ok_or_else(|| {
        LixError::new(
            LixError::CODE_FOREIGN_KEY,
            format!(
                "state-surface foreign key {state_address_part} at '{}' is missing",
                format_json_pointer(local_pointer)
            ),
        )
    })
}

fn state_surface_local_value(
    snapshot: &JsonValue,
    local_pointer: &[String],
    state_address_part: &str,
) -> Result<String, LixError> {
    state_surface_nullable_local_value(snapshot, local_pointer, state_address_part)?.ok_or_else(
        || {
            LixError::new(
                LixError::CODE_FOREIGN_KEY,
                format!(
                    "state-surface foreign key {state_address_part} at '{}' is missing",
                    format_json_pointer(local_pointer)
                ),
            )
        },
    )
}

fn state_surface_nullable_local_value(
    snapshot: &JsonValue,
    local_pointer: &[String],
    state_address_part: &str,
) -> Result<Option<String>, LixError> {
    let Some(value) = json_pointer_get(snapshot, local_pointer) else {
        return Err(LixError::new(
            LixError::CODE_FOREIGN_KEY,
            format!(
                "state-surface foreign key {state_address_part} at '{}' is missing",
                format_json_pointer(local_pointer)
            ),
        ));
    };
    if value.is_null() {
        return Ok(None);
    }
    value
        .as_str()
        .map(|value| Some(value.to_string()))
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_FOREIGN_KEY,
                format!(
                    "state-surface foreign key {state_address_part} at '{}' must be a string or null",
                    format_json_pointer(local_pointer)
                ),
            )
        })
}

fn state_surface_optional_local_json_value<'a>(
    snapshot: &'a JsonValue,
    local_pointer: &[String],
) -> Result<Option<&'a JsonValue>, LixError> {
    let Some(value) = json_pointer_get(snapshot, local_pointer) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    Ok(Some(value))
}

async fn validate_committed_unique_constraints(
    input: &TransactionValidationInput<'_>,
    pending_constraints: &PendingConstraintIndexes,
) -> Result<(), LixError> {
    for (key, pending_entity_id) in &pending_constraints.unique_values {
        let committed_rows = scan_committed_constraint_rows(
            input.live_state,
            &key.domain,
            vec![key.schema_key.clone()],
            Vec::new(),
            false,
        )
        .await?;

        for committed_row in committed_rows {
            if !committed_row_is_in_exact_validation_scope(&committed_row, key) {
                continue;
            }
            if committed_row.entity_id == *pending_entity_id {
                continue;
            }
            if pending_constraints.tombstones_identity(&committed_row) {
                continue;
            }
            let Some(snapshot_content) = committed_row.snapshot_content.as_deref() else {
                continue;
            };
            let snapshot =
                serde_json::from_str::<JsonValue>(snapshot_content).map_err(|error| {
                    LixError::new(
                        LixError::CODE_SCHEMA_VALIDATION,
                        format!(
                            "committed snapshot_content for schema '{}' is invalid JSON: {error}",
                            committed_row.schema_key
                        ),
                    )
                })?;
            let Some(committed_value) =
                UniqueConstraintValue::from_snapshot(&snapshot, &key.pointer_group)
            else {
                continue;
            };
            if committed_value == key.value {
                return Err(LixError::new(
                    LixError::CODE_UNIQUE,
                    format!(
                        "unique constraint violation on {}.{} for value {}: committed row '{}' conflicts with staged row '{}'",
                        key.schema_key,
                        format_pointer_group(&key.pointer_group),
                        key.value.display(),
                        committed_row.entity_id.as_json_array_text()?,
                        pending_entity_id.as_json_array_text()?
                    ),
                ));
            }
        }
    }
    Ok(())
}

fn committed_row_is_in_exact_validation_scope(
    row: &MaterializedLiveStateRow,
    key: &PendingUniqueKey,
) -> bool {
    // LiveStateReader may return serving projections such as global rows
    // projected into a requested version. Constraint validation is root-local:
    // only rows authored in the exact version participate.
    key.domain.contains(row) && row.schema_key == key.schema_key
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

    fn from_entity_identity(identity: &EntityIdentity) -> Self {
        Self(
            identity
                .parts
                .iter()
                .map(|part| format!("{part:?}"))
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
    error: EntityIdentityError,
) -> LixError {
    let reason = match error {
        EntityIdentityError::EmptyPrimaryKey => "empty x-lix-primary-key".to_string(),
        EntityIdentityError::EmptyPrimaryKeyPath { index } => {
            format!("empty x-lix-primary-key pointer at index {index}")
        }
        EntityIdentityError::MissingPrimaryKeyValue { index } => {
            let pointer = format_json_pointer(&primary_key_paths[index]);
            format!("missing value at primary-key pointer '{pointer}'")
        }
        EntityIdentityError::UnsupportedPrimaryKeyValue { index } => {
            let pointer = format_json_pointer(&primary_key_paths[index]);
            format!("non-string value at primary-key pointer '{pointer}'")
        }
        EntityIdentityError::InvalidEncodedEntityIdentity => {
            "invalid encoded entity identity".to_string()
        }
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
    catalog: &SchemaCatalog,
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

    if foreign_key.referenced_schema.schema_key == STATE_SURFACE_SCHEMA_KEY {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            format!(
                "foreign key on schema '{}' must not reference schemaKey 'lix_state'; use x-lix-state-foreign-keys with pointers ordered as [entity_id, schema_key, file_id]",
                source_key.schema_key
            ),
        ));
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

fn validate_state_foreign_key_definition(
    source_key: &SchemaCatalogKey,
    source_schema: &JsonValue,
    foreign_key: &StateForeignKeyPlan,
) -> Result<(), LixError> {
    let local_properties = foreign_key.local_properties();
    for pointer in &local_properties {
        validate_schema_field_pointer(source_schema, pointer).map_err(|detail| {
            LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                format!(
                    "state foreign key on schema '{}' references missing local property '{}': {detail}",
                    source_key.schema_key,
                    format_json_pointer(pointer)
                ),
            )
        })?;
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
            .ok_or_else(|| {
                format!(
                    "schema segment before '{}' has no object properties",
                    segment
                )
            })?;
        current = properties
            .get(segment)
            .ok_or_else(|| format!("property '{}' does not exist", segment))?;
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

fn validate_foreign_key_definitions(catalog: &SchemaCatalog) -> Result<(), LixError> {
    for plan in catalog.plans() {
        for foreign_key in &plan.foreign_keys {
            validate_foreign_key_definition(catalog, &plan.key, plan.schema.as_ref(), foreign_key)?;
        }
        for foreign_key in &plan.state_foreign_keys {
            validate_state_foreign_key_definition(&plan.key, plan.schema.as_ref(), foreign_key)?;
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
    reject_seed_schema_registration(&key)?;
    validate_lix_schema_definition(&schema)?;
    validate_lix_schema(registered_schema_definition, &snapshot)?;
    Ok((key, schema))
}

#[cfg(test)]
fn reject_seed_schema_registration(key: &SchemaKey) -> Result<(), LixError> {
    if is_seed_schema_key(&key.schema_key) {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            format!(
                "schema '{}' is a system schema and cannot be registered at runtime",
                key.schema_key
            ),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use serde_json::json;

    use super::*;
    use crate::live_state::{LiveStateRowRequest, LiveStateScanRequest, MaterializedLiveStateRow};
    use crate::schema::{schema_key_from_definition, seed_schema_definition};
    use crate::transaction::types::{StageJson, TransactionJson};

    struct EmptyLiveStateReader;

    fn test_stage_json(value: &str) -> StageJson {
        let mut json_writer = crate::json_store::JsonStoreContext::new().writer();
        let parsed = test_json_text(value).expect("test staged JSON should parse");
        crate::transaction::types::stage_json_from_value(
            &mut json_writer,
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
            SchemaCatalog::from_visible_schemas(&visible_schemas)
                .expect("test schema plan catalog should build"),
        ));
        catalog
            .plan_for_key(&key.schema_key)
            .expect("test schema key should resolve")
            .1
    }

    #[async_trait]
    impl LiveStateReader for EmptyLiveStateReader {
        async fn scan_rows(
            &self,
            request: &LiveStateScanRequest,
        ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
            Ok(test_file_descriptor_rows()
                .into_iter()
                .filter(|row| live_state_row_matches_scan(row, request))
                .collect())
        }

        async fn load_row(
            &self,
            request: &LiveStateRowRequest,
        ) -> Result<Option<MaterializedLiveStateRow>, LixError> {
            Ok(test_file_descriptor_rows()
                .into_iter()
                .find(|row| live_state_row_matches_load(row, request)))
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
        TransactionValidationInput::new(validation_set, catalog, &EmptyLiveStateReader)
    }

    fn catalog_from_transaction_input<'a>(
        input: &'a TransactionValidationInput<'a>,
    ) -> Result<&'a SchemaCatalog, LixError> {
        validate_foreign_key_definitions(input.schema_catalog)?;
        Ok(input.schema_catalog)
    }

    fn catalog_from_transaction_parts(
        staged_writes: &PreparedWriteSet,
        visible_schemas: &[JsonValue],
    ) -> Result<SchemaCatalog, LixError> {
        let catalog = catalog_from_transaction_parts_unchecked(staged_writes, visible_schemas)?;
        let mut pending_keys =
            BTreeMap::<SchemaCatalogKey, crate::entity_identity::EntityIdentity>::new();
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
            if let Some(existing_entity_id) =
                pending_keys.insert(catalog_key.clone(), row.entity_id().clone())
            {
                return Err(LixError::new(
                    LixError::CODE_SCHEMA_DEFINITION,
                    format!(
                        "duplicate pending registered schema '{}' in transaction: rows '{}' and '{}'",
                        catalog_key.schema_key,
                        existing_entity_id.as_json_array_text()?,
                        row.entity_id().as_json_array_text()?
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
    ) -> Result<SchemaCatalog, LixError> {
        let mut catalog = SchemaCatalog::from_visible_schemas(visible_schemas)?;
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

    struct StaticLiveStateReader {
        rows: Vec<MaterializedLiveStateRow>,
    }

    #[async_trait]
    impl LiveStateReader for StaticLiveStateReader {
        async fn scan_rows(
            &self,
            request: &LiveStateScanRequest,
        ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
            Ok(self
                .rows
                .iter()
                .cloned()
                .chain(test_file_descriptor_rows())
                .filter(|row| live_state_row_matches_scan(row, request))
                .collect())
        }

        async fn load_row(
            &self,
            request: &LiveStateRowRequest,
        ) -> Result<Option<MaterializedLiveStateRow>, LixError> {
            Ok(self
                .rows
                .iter()
                .cloned()
                .chain(test_file_descriptor_rows())
                .find(|row| {
                    row.schema_key == request.schema_key
                        && row.version_id == request.version_id
                        && row.entity_id == request.entity_id
                        && request.file_id.matches(row.file_id.as_ref())
                }))
        }
    }

    struct OverlayingStaticLiveStateReader {
        rows: Vec<MaterializedLiveStateRow>,
    }

    #[async_trait]
    impl LiveStateReader for OverlayingStaticLiveStateReader {
        async fn scan_rows(
            &self,
            request: &LiveStateScanRequest,
        ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
            let rows = self
                .rows
                .iter()
                .cloned()
                .chain(test_file_descriptor_rows())
                .filter(|row| live_state_row_matches_scan(row, request))
                .collect::<Vec<_>>();
            if request.filter.untracked.is_some() {
                return Ok(rows);
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
            Ok(overlay_untracked_rows_for_test(
                tracked_rows,
                untracked_rows,
            ))
        }

        async fn load_row(
            &self,
            request: &LiveStateRowRequest,
        ) -> Result<Option<MaterializedLiveStateRow>, LixError> {
            Ok(self
                .scan_rows(&LiveStateScanRequest {
                    filter: LiveStateFilter {
                        schema_keys: vec![request.schema_key.clone()],
                        entity_ids: vec![request.entity_id.clone()],
                        version_ids: vec![request.version_id.clone()],
                        file_ids: vec![request.file_id.clone()],
                        ..Default::default()
                    },
                    ..Default::default()
                })
                .await?
                .into_iter()
                .next())
        }
    }

    fn overlay_untracked_rows_for_test(
        tracked_rows: Vec<MaterializedLiveStateRow>,
        untracked_rows: Vec<MaterializedLiveStateRow>,
    ) -> Vec<MaterializedLiveStateRow> {
        let mut rows_by_identity = BTreeMap::new();
        for row in tracked_rows {
            rows_by_identity.insert(LiveStateRowIdentity::from_row(&row), row);
        }
        for row in untracked_rows {
            rows_by_identity.insert(LiveStateRowIdentity::from_row(&row), row);
        }
        rows_by_identity.into_values().collect()
    }

    struct StrictEmptyLiveStateReader;

    #[async_trait]
    impl LiveStateReader for StrictEmptyLiveStateReader {
        async fn scan_rows(
            &self,
            _request: &LiveStateScanRequest,
        ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
            Ok(Vec::new())
        }

        async fn load_row(
            &self,
            _request: &LiveStateRowRequest,
        ) -> Result<Option<MaterializedLiveStateRow>, LixError> {
            Ok(None)
        }
    }

    struct StrictStaticLiveStateReader {
        rows: Vec<MaterializedLiveStateRow>,
    }

    #[async_trait]
    impl LiveStateReader for StrictStaticLiveStateReader {
        async fn scan_rows(
            &self,
            request: &LiveStateScanRequest,
        ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
            Ok(self
                .rows
                .iter()
                .filter(|row| live_state_row_matches_scan(row, request))
                .cloned()
                .collect())
        }

        async fn load_row(
            &self,
            request: &LiveStateRowRequest,
        ) -> Result<Option<MaterializedLiveStateRow>, LixError> {
            Ok(self
                .rows
                .iter()
                .find(|row| live_state_row_matches_load(row, request))
                .cloned())
        }
    }

    #[test]
    fn schema_catalog_indexes_visible_schemas_by_key_and_version() {
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
            state_rows: vec![pending_registered_schema_row("pending_schema")],
            adopted_rows: Vec::new(),
            ..empty_staged_write_set()
        };
        let input = validation_input(&staged_writes, &visible_schemas);

        let catalog = catalog_from_transaction_input(&input).expect("schema catalog should build");

        assert_eq!(catalog.len(), 3);
        assert!(catalog.contains("visible_schema"));
        assert!(catalog.contains("pending_schema"));
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
            state_rows: vec![pending_registered_schema_row("same_schema")],
            adopted_rows: Vec::new(),
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
            PreparedValidationRow::State(&row),
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
            PreparedValidationRow::State(&row),
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
            PreparedValidationRow::State(&row),
            &registered_schema(),
        )
        .expect_err("nested Lix schema definition should be rejected");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
    }

    #[test]
    fn schema_catalog_rejects_duplicate_pending_registered_schema_identity() {
        let mut duplicate = pending_registered_schema_row("duplicate_schema");
        duplicate.entity_id = registered_schema_entity_id("duplicate_schema_duplicate");
        let staged_writes = PreparedWriteSet {
            state_rows: vec![pending_registered_schema_row("duplicate_schema"), duplicate],
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
            state_rows: vec![
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
            state_rows: vec![pending_registered_schema_from_definition(fk_child_schema())],
            adopted_rows: Vec::new(),
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
            state_rows: vec![
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
            state_rows: vec![
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
            state_rows: vec![
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

    #[test]
    fn schema_catalog_allows_state_surface_foreign_key_target() {
        let staged_writes = PreparedWriteSet {
            state_rows: vec![pending_registered_schema_from_definition(
                state_surface_ref_schema(),
            )],
            ..empty_staged_write_set()
        };
        let visible_schemas = vec![registered_schema()];
        let input = validation_input(&staged_writes, &visible_schemas);

        let catalog = catalog_from_transaction_input(&input)
            .expect("x-lix-state-foreign-keys should validate as a state-surface FK target");

        assert!(catalog.contains("state_surface_ref_schema"));
    }

    #[test]
    fn schema_catalog_rejects_normal_foreign_key_to_lix_state() {
        let mut schema = fk_child_schema();
        schema["x-lix-foreign-keys"][0]["properties"] = json!(["/parent_id"]);
        schema["x-lix-foreign-keys"][0]["references"] = json!({
            "schemaKey": "lix_state",
            "properties": ["/entity_id"]
        });
        let staged_writes = PreparedWriteSet {
            state_rows: vec![pending_registered_schema_from_definition(schema)],
            adopted_rows: Vec::new(),
            ..empty_staged_write_set()
        };
        let visible_schemas = vec![registered_schema()];

        let error = catalog_from_transaction_parts(&staged_writes, &visible_schemas)
            .expect_err("normal FK must not use fake lix_state schema key");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(
            error.message.contains("x-lix-state-foreign-keys"),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn schema_catalog_rejects_state_surface_foreign_key_without_full_address_tuple() {
        let mut schema = state_surface_ref_schema();
        schema["x-lix-state-foreign-keys"][0] = json!(["/target_entity_id"]);
        let staged_writes = PreparedWriteSet {
            state_rows: vec![pending_registered_schema_from_definition(schema)],
            adopted_rows: Vec::new(),
            ..empty_staged_write_set()
        };
        let visible_schemas = vec![registered_schema()];

        let error = catalog_from_transaction_parts_unchecked(&staged_writes, &visible_schemas)
            .expect_err("state FK target must include entity_id, schema_key, and file_id");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(
            error.message.contains("[entity_id, schema_key, file_id]"),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn validation_rejects_unknown_schema_key() {
        let visible_schemas = vec![key_value_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: vec![staged_row("unknown_schema", Some(json!({}).to_string()))],
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
            state_rows: vec![staged_row("unknown_schema", None)],
            adopted_rows: Vec::new(),
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
            state_rows: vec![
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

    #[tokio::test]
    async fn validation_rejects_tracked_row_using_pending_untracked_schema_definition() {
        let visible_schemas = vec![registered_schema()];
        let mut untracked_schema = pending_registered_schema_row("untracked_only_schema");
        mark_prepared_row_untracked(&mut untracked_schema);
        let mut tracked_row = staged_row(
            "untracked_only_schema",
            Some(json!({ "id": "row-1" }).to_string()),
        );
        tracked_row.entity_id = EntityIdentity::single("row-1");
        let staged_writes = PreparedWriteSet {
            state_rows: vec![untracked_schema, tracked_row],
            adopted_rows: Vec::new(),
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
            state_rows: vec![staged_row(
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
            state_rows: vec![staged_row("lix_key_value", None)],
            adopted_rows: Vec::new(),
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
            state_rows: vec![unique_row("post-1", "hello-world", "first")],
            adopted_rows: Vec::new(),
            ..empty_staged_write_set()
        };

        let error =
            validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &StrictEmptyLiveStateReader,
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
            state_rows: vec![
                staged_file_descriptor_row("file-a", "version-a"),
                unique_row("post-1", "hello-world", "first"),
            ],
            ..empty_staged_write_set()
        };

        validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
            &staged_writes,
            &visible_schemas,
            &StrictEmptyLiveStateReader,
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
        let mut untracked_file_descriptor = staged_file_descriptor_row("file-a", "version-a");
        mark_prepared_row_untracked(&mut untracked_file_descriptor);
        let staged_writes = PreparedWriteSet {
            state_rows: vec![
                untracked_file_descriptor,
                unique_row("post-1", "hello-world", "first"),
            ],
            ..empty_staged_write_set()
        };

        let error =
            validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &StrictEmptyLiveStateReader,
            ))
            .await
            .expect_err("tracked file owner must not resolve through pending untracked descriptor");

        assert_eq!(error.code, LixError::CODE_FILE_NOT_FOUND);
    }

    #[tokio::test]
    async fn validation_allows_untracked_file_owner_reference_pending_as_tracked() {
        let visible_schemas = vec![
            unique_schema(),
            file_descriptor_schema(),
            directory_descriptor_schema(),
        ];
        let mut untracked_row = unique_row("post-1", "hello-world", "first");
        mark_prepared_row_untracked(&mut untracked_row);
        let staged_writes = PreparedWriteSet {
            state_rows: vec![
                staged_file_descriptor_row("file-a", "version-a"),
                untracked_row,
            ],
            ..empty_staged_write_set()
        };

        validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
            &staged_writes,
            &visible_schemas,
            &StrictEmptyLiveStateReader,
        ))
        .await
        .expect("untracked file owner should resolve through pending tracked descriptor");
    }

    #[tokio::test]
    async fn validation_rejects_file_owner_reference_when_descriptor_tombstoned_in_transaction() {
        let visible_schemas = vec![
            unique_schema(),
            file_descriptor_schema(),
            directory_descriptor_schema(),
        ];
        let mut file_descriptor_delete = staged_file_descriptor_row("file-a", "version-a");
        file_descriptor_delete.snapshot = None;
        let staged_writes = PreparedWriteSet {
            state_rows: vec![
                file_descriptor_delete,
                unique_row("post-1", "hello-world", "first"),
            ],
            adopted_rows: Vec::new(),
            ..empty_staged_write_set()
        };

        let error = validate_prepared_writes(
            TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &EmptyLiveStateReader,
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
            state_rows: vec![unique_row("post-1", "hello-world", "first")],
            adopted_rows: Vec::new(),
            ..empty_staged_write_set()
        };
        let live_state = StaticLiveStateReader {
            rows: vec![committed_file_descriptor_row("file-a", "version-a")],
        };

        validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
            &staged_writes,
            &visible_schemas,
            &live_state,
        ))
        .await
        .expect("committed file descriptor should satisfy file ownership");
    }

    #[tokio::test]
    async fn validation_rejects_tracked_file_owner_reference_committed_only_as_untracked() {
        let visible_schemas = vec![unique_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: vec![unique_row("post-1", "hello-world", "first")],
            adopted_rows: Vec::new(),
            ..empty_staged_write_set()
        };
        let mut untracked_file_descriptor = committed_file_descriptor_row("file-a", "version-a");
        mark_live_row_untracked(&mut untracked_file_descriptor);
        let live_state = StrictStaticLiveStateReader {
            rows: vec![untracked_file_descriptor],
        };

        let error = validate_prepared_writes(
            TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &live_state,
            ),
        )
        .await
        .expect_err("tracked file owner must not resolve through committed untracked descriptor");

        assert_eq!(error.code, LixError::CODE_FILE_NOT_FOUND);
    }

    #[tokio::test]
    async fn validation_allows_untracked_file_owner_reference_committed_as_tracked() {
        let visible_schemas = vec![unique_schema()];
        let mut untracked_row = unique_row("post-1", "hello-world", "first");
        mark_prepared_row_untracked(&mut untracked_row);
        let staged_writes = PreparedWriteSet {
            state_rows: vec![untracked_row],
            adopted_rows: Vec::new(),
            ..empty_staged_write_set()
        };
        let live_state = StrictStaticLiveStateReader {
            rows: vec![committed_file_descriptor_row("file-a", "version-a")],
        };

        validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
            &staged_writes,
            &visible_schemas,
            &live_state,
        ))
        .await
        .expect("untracked file owner should resolve through committed tracked descriptor");
    }

    #[tokio::test]
    async fn validation_allows_tracked_file_owner_reference_committed_behind_untracked_overlay() {
        let visible_schemas = vec![unique_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: vec![unique_row("post-1", "hello-world", "first")],
            adopted_rows: Vec::new(),
            ..empty_staged_write_set()
        };
        let tracked_file_descriptor = committed_file_descriptor_row("file-a", "version-a");
        let mut untracked_tombstone = committed_file_descriptor_row("file-a", "version-a");
        untracked_tombstone.snapshot_content = None;
        mark_live_row_untracked(&mut untracked_tombstone);
        let live_state = OverlayingStaticLiveStateReader {
            rows: vec![tracked_file_descriptor, untracked_tombstone],
        };

        validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
            &staged_writes,
            &visible_schemas,
            &live_state,
        ))
        .await
        .expect("tracked file owner should resolve against tracked descriptor behind overlay");
    }

    #[tokio::test]
    async fn validation_rejects_deleting_file_descriptor_referenced_by_committed_row() {
        let visible_schemas = vec![
            unique_schema(),
            file_descriptor_schema(),
            directory_descriptor_schema(),
        ];
        let mut file_descriptor_delete = staged_file_descriptor_row("file-a", "version-a");
        file_descriptor_delete.snapshot = None;
        let staged_writes = PreparedWriteSet {
            state_rows: vec![file_descriptor_delete],
            adopted_rows: Vec::new(),
            ..empty_staged_write_set()
        };
        let live_state = StaticLiveStateReader {
            rows: vec![committed_unique_row("post-1", "hello-world", "first")],
        };

        let error =
            validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &live_state,
            ))
            .await
            .expect_err("file descriptor delete must be blocked by committed file-owned rows");

        assert_eq!(error.code, LixError::CODE_FOREIGN_KEY);
    }

    #[tokio::test]
    async fn validation_rejects_deleting_tracked_file_descriptor_referenced_by_committed_untracked_row(
    ) {
        let visible_schemas = vec![
            unique_schema(),
            file_descriptor_schema(),
            directory_descriptor_schema(),
        ];
        let mut file_descriptor_delete = staged_file_descriptor_row("file-a", "version-a");
        file_descriptor_delete.snapshot = None;
        let staged_writes = PreparedWriteSet {
            state_rows: vec![file_descriptor_delete],
            adopted_rows: Vec::new(),
            ..empty_staged_write_set()
        };
        let mut untracked_row =
            MaterializedLiveStateRow::from(unique_row("post-1", "hello-world", "first"));
        mark_live_row_untracked(&mut untracked_row);
        let live_state = StrictStaticLiveStateReader {
            rows: vec![
                committed_file_descriptor_row("file-a", "version-a"),
                untracked_row,
            ],
        };

        let error =
            validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &live_state,
            ))
            .await
            .expect_err("tracked file descriptor delete must be blocked by untracked rows");

        assert_eq!(error.code, LixError::CODE_FOREIGN_KEY);
    }

    #[tokio::test]
    async fn validation_allows_untracked_directory_parent_to_tracked_directory() {
        let visible_schemas = vec![directory_descriptor_schema()];
        let tracked_parent = directory_descriptor_row("dir-parent", None, "parent", "version-a");
        let mut untracked_child =
            directory_descriptor_row("dir-child", Some("dir-parent"), "child", "version-a");
        mark_prepared_row_untracked(&mut untracked_child);
        let staged_writes = PreparedWriteSet {
            state_rows: vec![tracked_parent, untracked_child],
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
            state_rows: vec![unique_row("post-1", "hello-world", "first")],
            adopted_rows: Vec::new(),
            ..empty_staged_write_set()
        };
        let live_state = StrictStaticLiveStateReader {
            rows: vec![committed_file_descriptor_row(
                "file-a",
                crate::GLOBAL_VERSION_ID,
            )],
        };

        let error =
            validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &live_state,
            ))
            .await
            .expect_err("global file descriptor should not satisfy a version-local row");

        assert_eq!(error.code, LixError::CODE_FILE_NOT_FOUND);
    }

    #[tokio::test]
    async fn validation_rejects_primary_key_duplicate_with_different_identity() {
        let visible_schemas = vec![unique_schema()];
        let mut conflicting = unique_row("post-1", "hello-world", "first");
        conflicting.entity_id = crate::entity_identity::EntityIdentity::single("post-2");
        let staged_writes = PreparedWriteSet {
            state_rows: vec![unique_row("post-1", "hello-world", "first"), conflicting],
            adopted_rows: Vec::new(),
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
            state_rows: vec![
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
            state_rows: vec![
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
    async fn validation_rejects_pending_unique_same_value_in_same_version() {
        let visible_schemas = vec![unique_schema()];
        let mut duplicate = unique_row("post-2", "hello-world", "second");
        duplicate.version_id = "version-a".to_string();
        let staged_writes = PreparedWriteSet {
            state_rows: vec![unique_row("post-1", "hello-world", "first"), duplicate],
            adopted_rows: Vec::new(),
            ..empty_staged_write_set()
        };

        let error = validate_prepared_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect_err("same unique value in the same version should fail");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
    }

    #[tokio::test]
    async fn validation_allows_pending_unique_same_value_in_different_versions() {
        let visible_schemas = vec![unique_schema()];
        let mut version_b = unique_row("post-2", "hello-world", "second");
        version_b.version_id = "version-b".to_string();
        let staged_writes = PreparedWriteSet {
            state_rows: vec![unique_row("post-1", "hello-world", "first"), version_b],
            adopted_rows: Vec::new(),
            ..empty_staged_write_set()
        };

        validate_prepared_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect("unique values should be scoped to the exact version_id");
    }

    #[tokio::test]
    async fn validation_allows_pending_unique_overwrite_of_same_identity() {
        let visible_schemas = vec![unique_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: vec![
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
            state_rows: vec![tombstone, unique_row("post-2", "hello-world", "second")],
            adopted_rows: Vec::new(),
            ..empty_staged_write_set()
        };

        validate_prepared_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect("tombstones should not claim pending unique values");
    }

    #[tokio::test]
    async fn validation_scopes_pending_unique_values_by_file_and_version() {
        let visible_schemas = vec![unique_schema()];
        let mut different_file = unique_row("post-2", "hello-world", "second");
        different_file.file_id = Some("file-b".to_string());
        let mut different_version = unique_row("post-3", "hello-world", "third");
        different_version.version_id = "version-b".to_string();
        let staged_writes = PreparedWriteSet {
            state_rows: vec![
                unique_row("post-1", "hello-world", "first"),
                different_file,
                different_version,
            ],
            ..empty_staged_write_set()
        };

        validate_prepared_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect("unique values are scoped by file and version");
    }

    #[tokio::test]
    async fn validation_rejects_committed_visible_unique_value_duplicate() {
        let visible_schemas = vec![unique_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: vec![unique_row("post-2", "hello-world", "second")],
            adopted_rows: Vec::new(),
            ..empty_staged_write_set()
        };
        let live_state = StaticLiveStateReader {
            rows: vec![committed_unique_row("post-1", "hello-world", "first")],
        };

        let error =
            validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &live_state,
            ))
            .await
            .expect_err("committed visible unique value should conflict");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
    }

    #[tokio::test]
    async fn validation_rejects_committed_tracked_unique_duplicate_behind_untracked_overlay() {
        let visible_schemas = vec![unique_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: vec![unique_row("post-2", "hello-world", "second")],
            adopted_rows: Vec::new(),
            ..empty_staged_write_set()
        };
        let tracked_duplicate = committed_unique_row("post-1", "hello-world", "first");
        let mut untracked_overlay = committed_unique_row("post-1", "draft-slug", "draft");
        mark_live_row_untracked(&mut untracked_overlay);
        let live_state = OverlayingStaticLiveStateReader {
            rows: vec![tracked_duplicate, untracked_overlay],
        };

        let error =
            validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &live_state,
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
            state_rows: vec![
                untracked_tombstone,
                unique_row("post-2", "hello-world", "second"),
            ],
            adopted_rows: Vec::new(),
            ..empty_staged_write_set()
        };
        let live_state = StaticLiveStateReader {
            rows: vec![committed_unique_row("post-1", "hello-world", "first")],
        };

        let error =
            validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &live_state,
            ))
            .await
            .expect_err("untracked tombstone must not hide tracked unique owner");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
    }

    #[tokio::test]
    async fn validation_rejects_committed_unique_duplicate_with_null_component() {
        let visible_schemas = vec![nullable_unique_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: vec![nullable_unique_row("row-2", None, "root-name")],
            adopted_rows: Vec::new(),
            ..empty_staged_write_set()
        };
        let live_state = StaticLiveStateReader {
            rows: vec![committed_nullable_unique_row("row-1", None, "root-name")],
        };

        let error =
            validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &live_state,
            ))
            .await
            .expect_err("committed duplicate nullable unique value should conflict");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
    }

    #[tokio::test]
    async fn validation_rejects_committed_unique_same_value_in_same_version() {
        let visible_schemas = vec![unique_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: vec![unique_row("post-2", "hello-world", "second")],
            adopted_rows: Vec::new(),
            ..empty_staged_write_set()
        };
        let live_state = StaticLiveStateReader {
            rows: vec![committed_unique_row("post-1", "hello-world", "first")],
        };

        let error =
            validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &live_state,
            ))
            .await
            .expect_err("same unique value in the same version should conflict");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
    }

    #[tokio::test]
    async fn validation_allows_committed_unique_same_value_in_different_versions() {
        let visible_schemas = vec![unique_schema()];
        let mut version_b = unique_row("post-2", "hello-world", "second");
        version_b.version_id = "version-b".to_string();
        let staged_writes = PreparedWriteSet {
            state_rows: vec![version_b],
            adopted_rows: Vec::new(),
            ..empty_staged_write_set()
        };
        let live_state = StaticLiveStateReader {
            rows: vec![committed_unique_row("post-1", "hello-world", "first")],
        };

        validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
            &staged_writes,
            &visible_schemas,
            &live_state,
        ))
        .await
        .expect("committed unique values should be scoped to the exact version_id");
    }

    #[tokio::test]
    async fn validation_ignores_projected_live_state_rows_for_unique_constraints() {
        let visible_schemas = vec![unique_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: vec![unique_row("post-2", "hello-world", "second")],
            adopted_rows: Vec::new(),
            ..empty_staged_write_set()
        };
        let mut projected_overlay_row = committed_unique_row("post-1", "hello-world", "first");
        projected_overlay_row.version_id = "version-a".to_string();
        projected_overlay_row.global = true;
        let live_state = StaticLiveStateReader {
            rows: vec![projected_overlay_row],
        };

        validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
            &staged_writes,
            &visible_schemas,
            &live_state,
        ))
        .await
        .expect("validation should ignore live-state overlay projections");
    }

    #[tokio::test]
    async fn validation_allows_committed_visible_unique_update_of_same_identity() {
        let visible_schemas = vec![unique_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: vec![unique_row("post-1", "hello-world", "updated")],
            adopted_rows: Vec::new(),
            ..empty_staged_write_set()
        };
        let live_state = StaticLiveStateReader {
            rows: vec![committed_unique_row("post-1", "hello-world", "first")],
        };

        validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
            &staged_writes,
            &visible_schemas,
            &live_state,
        ))
        .await
        .expect("same identity should update committed unique owner");
    }

    #[tokio::test]
    async fn validation_ignores_committed_unique_owner_tombstoned_by_transaction() {
        let visible_schemas = vec![unique_schema()];
        let mut tombstone = unique_row("post-1", "hello-world", "deleted");
        tombstone.snapshot = None;
        let staged_writes = PreparedWriteSet {
            state_rows: vec![tombstone, unique_row("post-2", "hello-world", "second")],
            adopted_rows: Vec::new(),
            ..empty_staged_write_set()
        };
        let live_state = StaticLiveStateReader {
            rows: vec![committed_unique_row("post-1", "hello-world", "first")],
        };

        validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
            &staged_writes,
            &visible_schemas,
            &live_state,
        ))
        .await
        .expect("tombstoned committed owner should not conflict");
    }

    #[tokio::test]
    async fn validation_allows_committed_unique_same_value_in_different_file_or_version() {
        let visible_schemas = vec![unique_schema()];
        let mut different_file = unique_row("post-2", "hello-world", "second");
        different_file.file_id = Some("file-b".to_string());
        let mut different_version = unique_row("post-3", "hello-world", "third");
        different_version.version_id = "version-b".to_string();
        let staged_writes = PreparedWriteSet {
            state_rows: vec![different_file, different_version],
            adopted_rows: Vec::new(),
            ..empty_staged_write_set()
        };
        let live_state = StaticLiveStateReader {
            rows: vec![committed_unique_row("post-1", "hello-world", "first")],
        };

        validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
            &staged_writes,
            &visible_schemas,
            &live_state,
        ))
        .await
        .expect("committed uniqueness is scoped by file and version");
    }

    #[tokio::test]
    async fn validation_rejects_foreign_key_target_missing_in_same_version() {
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: vec![fk_child_row("child-1", "parent-1", "version-a")],
            adopted_rows: Vec::new(),
            ..empty_staged_write_set()
        };

        let error = validate_prepared_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect_err("foreign key must resolve in the same version");

        assert_eq!(error.code, LixError::CODE_FOREIGN_KEY);
    }

    #[tokio::test]
    async fn validation_allows_foreign_key_target_in_same_version() {
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: vec![
                fk_parent_row("parent-1", "version-a"),
                fk_child_row("child-1", "parent-1", "version-a"),
            ],
            ..empty_staged_write_set()
        };

        validate_prepared_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect("foreign key should resolve against pending rows in the same version");
    }

    #[tokio::test]
    async fn validation_rejects_tracked_foreign_key_target_pending_only_as_untracked() {
        let visible_schemas = vec![
            fk_parent_schema(),
            fk_child_schema(),
            file_descriptor_schema(),
            directory_descriptor_schema(),
        ];
        let mut untracked_parent = fk_parent_row("parent-1", "version-a");
        mark_prepared_row_untracked(&mut untracked_parent);
        let mut untracked_file_descriptor = staged_file_descriptor_row("file-a", "version-a");
        mark_prepared_row_untracked(&mut untracked_file_descriptor);
        let staged_writes = PreparedWriteSet {
            state_rows: vec![
                untracked_file_descriptor,
                untracked_parent,
                fk_child_row("child-1", "parent-1", "version-a"),
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
        let tracked_file_descriptor = staged_file_descriptor_row("file-a", "version-a");
        let tracked_parent = fk_parent_row("parent-1", "version-a");
        let mut untracked_file_descriptor = staged_file_descriptor_row("file-a", "version-a");
        mark_prepared_row_untracked(&mut untracked_file_descriptor);
        let mut untracked_child = fk_child_row("child-1", "parent-1", "version-a");
        mark_prepared_row_untracked(&mut untracked_child);
        let staged_writes = PreparedWriteSet {
            state_rows: vec![
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
    async fn validation_rejects_foreign_key_target_that_exists_only_in_different_version() {
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: vec![
                fk_parent_row("parent-1", "version-b"),
                fk_child_row("child-1", "parent-1", "version-a"),
            ],
            ..empty_staged_write_set()
        };

        let error = validate_prepared_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect_err("foreign key target in another version should not satisfy this version");

        assert_eq!(error.code, LixError::CODE_FOREIGN_KEY);
    }

    #[tokio::test]
    async fn validation_allows_foreign_key_target_committed_in_same_version() {
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: vec![fk_child_row("child-1", "parent-1", "version-a")],
            adopted_rows: Vec::new(),
            ..empty_staged_write_set()
        };
        let live_state = StaticLiveStateReader {
            rows: vec![MaterializedLiveStateRow::from(fk_parent_row(
                "parent-1",
                "version-a",
            ))],
        };

        validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
            &staged_writes,
            &visible_schemas,
            &live_state,
        ))
        .await
        .expect("foreign key should resolve against committed rows in the same version");
    }

    #[tokio::test]
    async fn validation_rejects_tracked_foreign_key_target_committed_only_as_untracked() {
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: vec![fk_child_row("child-1", "parent-1", "version-a")],
            adopted_rows: Vec::new(),
            ..empty_staged_write_set()
        };
        let mut untracked_parent =
            MaterializedLiveStateRow::from(fk_parent_row("parent-1", "version-a"));
        mark_live_row_untracked(&mut untracked_parent);
        let live_state = StaticLiveStateReader {
            rows: vec![untracked_parent],
        };

        let error =
            validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &live_state,
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
        let mut untracked_file_descriptor = staged_file_descriptor_row("file-a", "version-a");
        mark_prepared_row_untracked(&mut untracked_file_descriptor);
        let mut untracked_child = fk_child_row("child-1", "parent-1", "version-a");
        mark_prepared_row_untracked(&mut untracked_child);
        let staged_writes = PreparedWriteSet {
            state_rows: vec![untracked_file_descriptor, untracked_child],
            adopted_rows: Vec::new(),
            ..empty_staged_write_set()
        };
        let live_state = StaticLiveStateReader {
            rows: vec![
                committed_file_descriptor_row("file-a", "version-a"),
                MaterializedLiveStateRow::from(fk_parent_row("parent-1", "version-a")),
            ],
        };

        validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
            &staged_writes,
            &visible_schemas,
            &live_state,
        ))
        .await
        .expect("untracked FK should be allowed to reference a committed tracked target");
    }

    #[tokio::test]
    async fn validation_allows_tracked_foreign_key_target_committed_behind_untracked_overlay() {
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: vec![fk_child_row("child-1", "parent-1", "version-a")],
            adopted_rows: Vec::new(),
            ..empty_staged_write_set()
        };
        let tracked_parent = MaterializedLiveStateRow::from(fk_parent_row("parent-1", "version-a"));
        let mut untracked_overlay =
            MaterializedLiveStateRow::from(fk_parent_row("parent-1", "version-a"));
        mark_live_row_untracked(&mut untracked_overlay);
        let live_state = OverlayingStaticLiveStateReader {
            rows: vec![tracked_parent, untracked_overlay],
        };

        validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
            &staged_writes,
            &visible_schemas,
            &live_state,
        ))
        .await
        .expect(
            "tracked FK should resolve against tracked storage target behind untracked overlay",
        );
    }

    #[tokio::test]
    async fn validation_rejects_deleting_tracked_fk_target_referenced_behind_untracked_overlay() {
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let mut parent_delete = fk_parent_row("parent-1", "version-a");
        parent_delete.snapshot = None;
        let staged_writes = PreparedWriteSet {
            state_rows: vec![parent_delete],
            adopted_rows: Vec::new(),
            ..empty_staged_write_set()
        };
        let tracked_parent = MaterializedLiveStateRow::from(fk_parent_row("parent-1", "version-a"));
        let tracked_child =
            MaterializedLiveStateRow::from(fk_child_row("child-1", "parent-1", "version-a"));
        let mut untracked_child_overlay =
            MaterializedLiveStateRow::from(fk_child_row("child-1", "other-parent", "version-a"));
        mark_live_row_untracked(&mut untracked_child_overlay);
        let live_state = OverlayingStaticLiveStateReader {
            rows: vec![tracked_parent, tracked_child, untracked_child_overlay],
        };

        let error =
            validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &live_state,
            ))
            .await
            .expect_err("tracked referencing row behind overlay must block target delete");

        assert_eq!(error.code, LixError::CODE_FOREIGN_KEY);
    }

    #[tokio::test]
    async fn validation_rejects_deleting_tracked_fk_target_referenced_by_committed_untracked_row() {
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let mut parent_delete = fk_parent_row("parent-1", "version-a");
        parent_delete.snapshot = None;
        let staged_writes = PreparedWriteSet {
            state_rows: vec![parent_delete],
            adopted_rows: Vec::new(),
            ..empty_staged_write_set()
        };
        let tracked_parent = MaterializedLiveStateRow::from(fk_parent_row("parent-1", "version-a"));
        let mut untracked_child =
            MaterializedLiveStateRow::from(fk_child_row("child-1", "parent-1", "version-a"));
        mark_live_row_untracked(&mut untracked_child);
        let live_state = StaticLiveStateReader {
            rows: vec![tracked_parent, untracked_child],
        };

        let error =
            validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &live_state,
            ))
            .await
            .expect_err("tracked target delete must be blocked by committed untracked references");

        assert_eq!(error.code, LixError::CODE_FOREIGN_KEY);
    }

    #[tokio::test]
    async fn validation_rejects_foreign_key_target_committed_only_in_different_version() {
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: vec![fk_child_row("child-1", "parent-1", "version-a")],
            adopted_rows: Vec::new(),
            ..empty_staged_write_set()
        };
        let live_state = StaticLiveStateReader {
            rows: vec![MaterializedLiveStateRow::from(fk_parent_row(
                "parent-1",
                "version-b",
            ))],
        };

        let error =
            validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &live_state,
            ))
            .await
            .expect_err(
                "foreign key target in another committed version should not satisfy this version",
            );

        assert_eq!(error.code, LixError::CODE_FOREIGN_KEY);
    }

    #[tokio::test]
    async fn validation_rejects_foreign_key_target_tombstoned_by_transaction() {
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let mut parent_delete = fk_parent_row("parent-1", "version-a");
        parent_delete.snapshot = None;
        let staged_writes = PreparedWriteSet {
            state_rows: vec![
                parent_delete,
                fk_child_row("child-1", "parent-1", "version-a"),
            ],
            ..empty_staged_write_set()
        };
        let live_state = StaticLiveStateReader {
            rows: vec![MaterializedLiveStateRow::from(fk_parent_row(
                "parent-1",
                "version-a",
            ))],
        };

        let error =
            validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &live_state,
            ))
            .await
            .expect_err("same-transaction tombstone should hide the committed FK target");

        assert_eq!(error.code, LixError::CODE_FOREIGN_KEY);
    }

    #[tokio::test]
    async fn validation_allows_tracked_fk_target_when_untracked_tombstone_shadows_same_identity() {
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let mut untracked_parent_delete = fk_parent_row("parent-1", "version-a");
        untracked_parent_delete.snapshot = None;
        mark_prepared_row_untracked(&mut untracked_parent_delete);
        let staged_writes = PreparedWriteSet {
            state_rows: vec![
                untracked_parent_delete,
                fk_child_row("child-1", "parent-1", "version-a"),
            ],
            adopted_rows: Vec::new(),
            ..empty_staged_write_set()
        };
        let live_state = StaticLiveStateReader {
            rows: vec![MaterializedLiveStateRow::from(fk_parent_row(
                "parent-1",
                "version-a",
            ))],
        };

        validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
            &staged_writes,
            &visible_schemas,
            &live_state,
        ))
        .await
        .expect("untracked tombstone must not hide tracked FK target");
    }

    #[tokio::test]
    async fn validation_rejects_pending_reference_to_deleted_identity() {
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let mut parent_delete = fk_parent_row("parent-1", "version-a");
        parent_delete.snapshot = None;
        let staged_writes = PreparedWriteSet {
            state_rows: vec![
                parent_delete,
                fk_child_row("child-1", "parent-1", "version-a"),
            ],
            ..empty_staged_write_set()
        };
        let live_state = StaticLiveStateReader {
            rows: vec![MaterializedLiveStateRow::from(fk_parent_row(
                "parent-1",
                "version-a",
            ))],
        };

        let error =
            validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &live_state,
            ))
            .await
            .expect_err("pending child reference should block parent delete");

        assert_eq!(error.code, LixError::CODE_FOREIGN_KEY);
    }

    #[tokio::test]
    async fn validation_allows_delete_with_pending_reference_in_different_version() {
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let mut parent_delete = fk_parent_row("parent-1", "version-a");
        parent_delete.snapshot = None;
        let staged_writes = PreparedWriteSet {
            state_rows: vec![
                parent_delete,
                fk_parent_row("parent-1", "version-b"),
                fk_child_row("child-1", "parent-1", "version-b"),
            ],
            ..empty_staged_write_set()
        };

        validate_prepared_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect("pending references in another version should not block this delete");
    }

    #[tokio::test]
    async fn validation_allows_state_surface_fk_target_committed_by_exact_identity() {
        let visible_schemas = vec![fk_parent_schema(), state_surface_ref_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: vec![state_surface_ref_row(
                "ref-1",
                "target-1",
                "fk_parent_schema",
                "file-a",
            )],
            ..empty_staged_write_set()
        };
        let live_state = StaticLiveStateReader {
            rows: vec![MaterializedLiveStateRow::from(fk_parent_row(
                "target-1",
                "version-a",
            ))],
        };

        validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
            &staged_writes,
            &visible_schemas,
            &live_state,
        ))
        .await
        .expect("state FK should resolve against exact committed identity");
    }

    #[tokio::test]
    async fn validation_rejects_tracked_state_surface_fk_target_pending_only_as_untracked() {
        let visible_schemas = vec![
            fk_parent_schema(),
            state_surface_ref_schema(),
            file_descriptor_schema(),
            directory_descriptor_schema(),
        ];
        let mut untracked_target = fk_parent_row("target-1", "version-a");
        mark_prepared_row_untracked(&mut untracked_target);
        let mut untracked_file_descriptor = staged_file_descriptor_row("file-a", "version-a");
        mark_prepared_row_untracked(&mut untracked_file_descriptor);
        let staged_writes = PreparedWriteSet {
            state_rows: vec![
                untracked_file_descriptor,
                untracked_target,
                state_surface_ref_row("ref-1", "target-1", "fk_parent_schema", "file-a"),
            ],
            ..empty_staged_write_set()
        };

        let error = validate_prepared_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect_err(
                "tracked state-surface FK must not resolve through a pending untracked target",
            );

        assert_eq!(error.code, LixError::CODE_FOREIGN_KEY);
    }

    #[tokio::test]
    async fn validation_rejects_tracked_state_surface_fk_target_committed_only_as_untracked() {
        let visible_schemas = vec![fk_parent_schema(), state_surface_ref_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: vec![state_surface_ref_row(
                "ref-1",
                "target-1",
                "fk_parent_schema",
                "file-a",
            )],
            ..empty_staged_write_set()
        };
        let mut untracked_target =
            MaterializedLiveStateRow::from(fk_parent_row("target-1", "version-a"));
        mark_live_row_untracked(&mut untracked_target);
        let live_state = StaticLiveStateReader {
            rows: vec![untracked_target],
        };

        let error =
            validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &live_state,
            ))
            .await
            .expect_err(
                "tracked state-surface FK must not resolve through a committed untracked target",
            );

        assert_eq!(error.code, LixError::CODE_FOREIGN_KEY);
    }

    #[tokio::test]
    async fn validation_allows_untracked_state_surface_fk_target_committed_as_tracked() {
        let visible_schemas = vec![
            fk_parent_schema(),
            state_surface_ref_schema(),
            file_descriptor_schema(),
            directory_descriptor_schema(),
        ];
        let mut untracked_file_descriptor = staged_file_descriptor_row("file-a", "version-a");
        mark_prepared_row_untracked(&mut untracked_file_descriptor);
        let mut untracked_ref =
            state_surface_ref_row("ref-1", "target-1", "fk_parent_schema", "file-a");
        mark_prepared_row_untracked(&mut untracked_ref);
        let staged_writes = PreparedWriteSet {
            state_rows: vec![untracked_file_descriptor, untracked_ref],
            ..empty_staged_write_set()
        };
        let live_state = StaticLiveStateReader {
            rows: vec![
                committed_file_descriptor_row("file-a", "version-a"),
                MaterializedLiveStateRow::from(fk_parent_row("target-1", "version-a")),
            ],
        };

        validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
            &staged_writes,
            &visible_schemas,
            &live_state,
        ))
        .await
        .expect("untracked state-surface FK should reference committed tracked target");
    }

    #[tokio::test]
    async fn validation_allows_tracked_state_surface_fk_target_committed_behind_untracked_overlay()
    {
        let visible_schemas = vec![fk_parent_schema(), state_surface_ref_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: vec![state_surface_ref_row(
                "ref-1",
                "target-1",
                "fk_parent_schema",
                "file-a",
            )],
            ..empty_staged_write_set()
        };
        let tracked_target = MaterializedLiveStateRow::from(fk_parent_row("target-1", "version-a"));
        let mut untracked_overlay =
            MaterializedLiveStateRow::from(fk_parent_row("target-1", "version-a"));
        mark_live_row_untracked(&mut untracked_overlay);
        let live_state = OverlayingStaticLiveStateReader {
            rows: vec![tracked_target, untracked_overlay],
        };

        validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
            &staged_writes,
            &visible_schemas,
            &live_state,
        ))
        .await
        .expect(
            "tracked state-surface FK should resolve against tracked target behind untracked overlay",
        );
    }

    #[tokio::test]
    async fn validation_allows_state_surface_fk_target_with_composite_entity_id() {
        let visible_schemas = vec![composite_message_schema(), state_surface_ref_schema()];
        let staged_writes = PreparedWriteSet {
            state_rows: vec![state_surface_ref_row_with_target_entity_id(
                "ref-1",
                json!(["welcome.title", "en"]),
                "composite_message_schema",
                "file-a",
            )],
            ..empty_staged_write_set()
        };
        let live_state = StaticLiveStateReader {
            rows: vec![MaterializedLiveStateRow::from(composite_message_row(
                "welcome.title",
                "en",
                "version-a",
            ))],
        };

        validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
            &staged_writes,
            &visible_schemas,
            &live_state,
        ))
        .await
        .expect("state FK should resolve composite JSON-array entity ids");
    }

    #[tokio::test]
    async fn validation_rejects_delete_when_same_version_reference_exists() {
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let mut parent_delete = fk_parent_row("parent-1", "version-a");
        parent_delete.snapshot = None;
        let live_state = StaticLiveStateReader {
            rows: vec![
                MaterializedLiveStateRow::from(fk_parent_row("parent-1", "version-a")),
                MaterializedLiveStateRow::from(fk_child_row("child-1", "parent-1", "version-a")),
            ],
        };
        let staged_writes = PreparedWriteSet {
            state_rows: vec![parent_delete],
            adopted_rows: Vec::new(),
            ..empty_staged_write_set()
        };

        let error =
            validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &live_state,
            ))
            .await
            .expect_err("delete should be restricted by same-version references");

        assert_eq!(error.code, LixError::CODE_FOREIGN_KEY);
    }

    #[tokio::test]
    async fn validation_allows_delete_when_only_different_version_reference_exists() {
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let mut parent_delete = fk_parent_row("parent-1", "version-a");
        parent_delete.snapshot = None;
        let live_state = StaticLiveStateReader {
            rows: vec![
                MaterializedLiveStateRow::from(fk_parent_row("parent-1", "version-a")),
                MaterializedLiveStateRow::from(fk_child_row("child-1", "parent-1", "version-b")),
            ],
        };
        let staged_writes = PreparedWriteSet {
            state_rows: vec![parent_delete],
            adopted_rows: Vec::new(),
            ..empty_staged_write_set()
        };

        validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
            &staged_writes,
            &visible_schemas,
            &live_state,
        ))
        .await
        .expect("references in another version should not restrict this version");
    }

    #[tokio::test]
    async fn validation_allows_delete_when_committed_reference_is_also_deleted() {
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let mut parent_delete = fk_parent_row("parent-1", "version-a");
        parent_delete.snapshot = None;
        let mut child_delete = fk_child_row("child-1", "parent-1", "version-a");
        child_delete.snapshot = None;
        let live_state = StaticLiveStateReader {
            rows: vec![
                MaterializedLiveStateRow::from(fk_parent_row("parent-1", "version-a")),
                MaterializedLiveStateRow::from(fk_child_row("child-1", "parent-1", "version-a")),
            ],
        };
        let staged_writes = PreparedWriteSet {
            state_rows: vec![parent_delete, child_delete],
            adopted_rows: Vec::new(),
            ..empty_staged_write_set()
        };

        validate_prepared_writes(TransactionValidationInput::from_visible_schemas_for_tests(
            &staged_writes,
            &visible_schemas,
            &live_state,
        ))
        .await
        .expect("committed references deleted in the same transaction should not restrict delete");
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

        assert!(plan
            .1
            .compiled_schema
            .validate(&json!({ "key": "k", "value": "v" }))
            .is_ok());
    }

    #[test]
    fn pending_indexes_record_primary_key_fk_targets_by_exact_scope() {
        let mut indexes = PendingConstraintIndexes::default();
        let row = fk_parent_row("parent-1", "version-a");
        let snapshot = serde_json::from_str::<JsonValue>(
            row.snapshot
                .as_ref()
                .map(|snapshot| snapshot.normalized.as_ref())
                .expect("fixture should have snapshot"),
        )
        .expect("fixture JSON should parse");

        indexes
            .remember_row(
                PreparedValidationRow::State(&row),
                test_plan_from_schema(fk_parent_schema()),
                &snapshot,
            )
            .expect("parent row should index");

        assert!(indexes
            .has_fk_target(
                "fk_parent_schema",
                "version-a",
                Some("file-a"),
                &["/id"],
                UniqueConstraintValue::string_values(["parent-1"]),
            )
            .expect("lookup should build"));
        assert!(!indexes
            .has_fk_target(
                "fk_parent_schema",
                "version-b",
                Some("file-a"),
                &["/id"],
                UniqueConstraintValue::string_values(["parent-1"]),
            )
            .expect("lookup should build"));
    }

    #[test]
    fn pending_indexes_record_unique_fk_targets_by_exact_scope() {
        let mut indexes = PendingConstraintIndexes::default();
        let row = unique_row("post-1", "hello-world", "first");
        let snapshot = serde_json::from_str::<JsonValue>(
            row.snapshot
                .as_ref()
                .map(|snapshot| snapshot.normalized.as_ref())
                .expect("fixture should have snapshot"),
        )
        .expect("fixture JSON should parse");

        indexes
            .remember_row(
                PreparedValidationRow::State(&row),
                test_plan_from_schema(unique_schema()),
                &snapshot,
            )
            .expect("unique row should index");

        assert!(indexes
            .has_fk_target(
                "unique_schema",
                "version-a",
                Some("file-a"),
                &["/slug"],
                UniqueConstraintValue::string_values(["hello-world"]),
            )
            .expect("lookup should build"));
    }

    #[test]
    fn pending_indexes_record_normal_fk_references_by_exact_scope() {
        let mut indexes = PendingConstraintIndexes::default();
        let row = fk_child_row("child-1", "parent-1", "version-a");
        let snapshot = serde_json::from_str::<JsonValue>(
            row.snapshot
                .as_ref()
                .map(|snapshot| snapshot.normalized.as_ref())
                .expect("fixture should have snapshot"),
        )
        .expect("fixture JSON should parse");
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let staged_writes = empty_staged_write_set();
        let input = validation_input(&staged_writes, &visible_schemas);
        let _catalog = catalog_from_transaction_input(&input).expect("catalog should build");

        indexes
            .remember_foreign_key_references(
                PreparedValidationRow::State(&row),
                test_plan_from_schema(fk_child_schema()),
                &snapshot,
            )
            .expect("child row should index FK reference");

        assert!(indexes
            .has_fk_reference_to_key(
                "fk_parent_schema",
                "version-a",
                Some("file-a"),
                &["/id"],
                UniqueConstraintValue::string_values(["parent-1"]),
            )
            .expect("lookup should build"));
        assert!(!indexes
            .has_fk_reference_to_key(
                "fk_parent_schema",
                "version-b",
                Some("file-a"),
                &["/id"],
                UniqueConstraintValue::string_values(["parent-1"]),
            )
            .expect("lookup should build"));
    }

    #[test]
    fn pending_indexes_record_state_surface_fk_references_by_exact_identity() {
        let mut indexes = PendingConstraintIndexes::default();
        let row = state_surface_ref_row("ref-1", "target-1", "fk_parent_schema", "file-a");
        let snapshot = serde_json::from_str::<JsonValue>(
            row.snapshot
                .as_ref()
                .map(|snapshot| snapshot.normalized.as_ref())
                .expect("fixture should have snapshot"),
        )
        .expect("fixture JSON should parse");
        let visible_schemas = vec![state_surface_ref_schema()];
        let staged_writes = empty_staged_write_set();
        let input = validation_input(&staged_writes, &visible_schemas);
        let _catalog = catalog_from_transaction_input(&input).expect("catalog should build");

        indexes
            .remember_foreign_key_references(
                PreparedValidationRow::State(&row),
                test_plan_from_schema(state_surface_ref_schema()),
                &snapshot,
            )
            .expect("state-surface row should index FK reference");

        assert!(
            indexes.has_fk_reference_to_identity(DomainRowIdentity::exact(
                "version-a",
                false,
                Some("file-a".to_string()),
                "fk_parent_schema",
                EntityIdentity::single("target-1"),
            ))
        );
    }

    #[test]
    fn pending_delete_restrictions_ignore_tombstoned_referencing_rows() {
        let mut indexes = PendingConstraintIndexes::default();
        let mut parent_delete = fk_parent_row("parent-1", "version-a");
        parent_delete.snapshot = None;
        indexes.remember_tombstone(PreparedValidationRow::State(&parent_delete));

        let child = fk_child_row("child-1", "parent-1", "version-a");
        let child_snapshot = serde_json::from_str::<JsonValue>(
            child
                .snapshot
                .as_ref()
                .map(|snapshot| snapshot.normalized.as_ref())
                .expect("fixture should have snapshot"),
        )
        .expect("fixture JSON should parse");
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let staged_writes = empty_staged_write_set();
        let input = validation_input(&staged_writes, &visible_schemas);
        let catalog = catalog_from_transaction_input(&input).expect("catalog should build");
        indexes
            .remember_foreign_key_references(
                PreparedValidationRow::State(&child),
                test_plan_from_schema(fk_child_schema()),
                &child_snapshot,
            )
            .expect("child row should index FK reference");

        let mut child_delete = fk_child_row("child-1", "parent-1", "version-a");
        child_delete.snapshot = None;
        indexes.remember_tombstone(PreparedValidationRow::State(&child_delete));

        validate_pending_delete_restrictions(&catalog, &indexes)
            .expect("a row deleted in the same transaction should not block target delete");
    }

    #[test]
    fn pending_fk_validation_collects_unresolved_normal_fk_check() {
        let indexes = PendingConstraintIndexes::default();
        let row = fk_child_row("child-1", "parent-1", "version-a");
        let snapshot = serde_json::from_str::<JsonValue>(
            row.snapshot
                .as_ref()
                .map(|snapshot| snapshot.normalized.as_ref())
                .expect("fixture should have snapshot"),
        )
        .expect("fixture JSON should parse");
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let staged_writes = empty_staged_write_set();
        let input = validation_input(&staged_writes, &visible_schemas);
        let _catalog = catalog_from_transaction_input(&input).expect("catalog should build");

        let unresolved = validate_pending_foreign_keys(
            &indexes,
            &[(
                PreparedValidationRow::State(&row),
                test_plan_from_schema(fk_child_schema()),
                &snapshot,
            )],
        )
        .expect("FK validation should collect unresolved checks");

        assert_eq!(unresolved.len(), 1);
        assert_eq!(
            unresolved[0].source_identity,
            DomainRowIdentity::exact(
                "version-a",
                false,
                Some("file-a".to_string()),
                "fk_child_schema",
                EntityIdentity::single("child-1"),
            )
        );
        assert_eq!(unresolved[0].source_schema_key, "fk_child_schema");
        assert_eq!(
            unresolved[0].source_pointer_group,
            vec![vec!["parent_id".to_string()]]
        );
        let UnresolvedForeignKeyTarget::Key(target) = &unresolved[0].target else {
            panic!("normal FK should produce key target");
        };
        assert_eq!(target.schema_key, "fk_parent_schema");
        assert_eq!(target.domain.version_id(), "version-a");
        assert_eq!(
            target.domain.file_scope(),
            &DomainFileScope::Exact(Some("file-a".to_string()))
        );
        assert_eq!(target.pointer_group, vec![vec!["id".to_string()]]);
        assert_eq!(
            target.value,
            UniqueConstraintValue::string_values(["parent-1"])
        );
    }

    #[test]
    fn pending_fk_validation_resolves_normal_fk_against_pending_target() {
        let mut indexes = PendingConstraintIndexes::default();
        let parent = fk_parent_row("parent-1", "version-a");
        let parent_snapshot = serde_json::from_str::<JsonValue>(
            parent
                .snapshot
                .as_ref()
                .map(|snapshot| snapshot.normalized.as_ref())
                .expect("fixture should have snapshot"),
        )
        .expect("fixture JSON should parse");
        indexes
            .remember_row(
                PreparedValidationRow::State(&parent),
                test_plan_from_schema(fk_parent_schema()),
                &parent_snapshot,
            )
            .expect("parent should index as pending FK target");

        let child = fk_child_row("child-1", "parent-1", "version-a");
        let child_snapshot = serde_json::from_str::<JsonValue>(
            child
                .snapshot
                .as_ref()
                .map(|snapshot| snapshot.normalized.as_ref())
                .expect("fixture should have snapshot"),
        )
        .expect("fixture JSON should parse");
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let staged_writes = empty_staged_write_set();
        let input = validation_input(&staged_writes, &visible_schemas);
        let _catalog = catalog_from_transaction_input(&input).expect("catalog should build");

        let unresolved = validate_pending_foreign_keys(
            &indexes,
            &[(
                PreparedValidationRow::State(&child),
                test_plan_from_schema(fk_child_schema()),
                &child_snapshot,
            )],
        )
        .expect("FK validation should inspect pending targets");

        assert!(
            unresolved.is_empty(),
            "same-version pending parent should satisfy the child FK"
        );
    }

    #[test]
    fn pending_fk_validation_keeps_normal_fk_unresolved_across_versions() {
        let mut indexes = PendingConstraintIndexes::default();
        let parent = fk_parent_row("parent-1", "version-b");
        let parent_snapshot = serde_json::from_str::<JsonValue>(
            parent
                .snapshot
                .as_ref()
                .map(|snapshot| snapshot.normalized.as_ref())
                .expect("fixture should have snapshot"),
        )
        .expect("fixture JSON should parse");
        indexes
            .remember_row(
                PreparedValidationRow::State(&parent),
                test_plan_from_schema(fk_parent_schema()),
                &parent_snapshot,
            )
            .expect("parent should index as pending FK target");

        let child = fk_child_row("child-1", "parent-1", "version-a");
        let child_snapshot = serde_json::from_str::<JsonValue>(
            child
                .snapshot
                .as_ref()
                .map(|snapshot| snapshot.normalized.as_ref())
                .expect("fixture should have snapshot"),
        )
        .expect("fixture JSON should parse");
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let staged_writes = empty_staged_write_set();
        let input = validation_input(&staged_writes, &visible_schemas);
        let _catalog = catalog_from_transaction_input(&input).expect("catalog should build");

        let unresolved = validate_pending_foreign_keys(
            &indexes,
            &[(
                PreparedValidationRow::State(&child),
                test_plan_from_schema(fk_child_schema()),
                &child_snapshot,
            )],
        )
        .expect("FK validation should inspect pending targets");

        assert_eq!(unresolved.len(), 1);
        let UnresolvedForeignKeyTarget::Key(target) = &unresolved[0].target else {
            panic!("normal FK should produce key target");
        };
        assert_eq!(
            target.domain.version_id(),
            "version-a",
            "FK checks are exact-version scoped, not overlay scoped"
        );
    }

    #[test]
    fn pending_fk_validation_collects_unresolved_state_surface_check() {
        let indexes = PendingConstraintIndexes::default();
        let row = state_surface_ref_row("ref-1", "target-1", "fk_parent_schema", "file-a");
        let snapshot = serde_json::from_str::<JsonValue>(
            row.snapshot
                .as_ref()
                .map(|snapshot| snapshot.normalized.as_ref())
                .expect("fixture should have snapshot"),
        )
        .expect("fixture JSON should parse");
        let visible_schemas = vec![state_surface_ref_schema()];
        let staged_writes = empty_staged_write_set();
        let input = validation_input(&staged_writes, &visible_schemas);
        let _catalog = catalog_from_transaction_input(&input).expect("catalog should build");

        let unresolved = validate_pending_foreign_keys(
            &indexes,
            &[(
                PreparedValidationRow::State(&row),
                test_plan_from_schema(state_surface_ref_schema()),
                &snapshot,
            )],
        )
        .expect("FK validation should collect unresolved checks");

        assert_eq!(unresolved.len(), 1);
        assert_eq!(
            unresolved[0].source_identity,
            DomainRowIdentity::exact(
                "version-a",
                false,
                Some("file-a".to_string()),
                "state_surface_ref_schema",
                EntityIdentity::single("ref-1"),
            )
        );
        assert_eq!(unresolved[0].source_schema_key, "state_surface_ref_schema");
        assert_eq!(
            unresolved[0].source_pointer_group,
            vec![
                vec!["target_entity_id".to_string()],
                vec!["target_schema_key".to_string()],
                vec!["target_file_id".to_string()],
            ]
        );
        let UnresolvedForeignKeyTarget::StateSurfaceIdentity(target) = &unresolved[0].target else {
            panic!("state FK should produce state-surface identity target");
        };
        assert_eq!(target.domain().version_id(), "version-a");
        assert_eq!(target.schema_key(), "fk_parent_schema");
        assert_eq!(target.entity_id(), &EntityIdentity::single("target-1"));
        assert_eq!(
            target.domain().file_scope(),
            &DomainFileScope::Exact(Some("file-a".to_string()))
        );
    }

    #[tokio::test]
    async fn committed_fk_lookup_resolves_normal_fk_in_exact_scope() {
        let indexes = PendingConstraintIndexes::default();
        let child = fk_child_row("child-1", "parent-1", "version-a");
        let child_snapshot = serde_json::from_str::<JsonValue>(
            child
                .snapshot
                .as_ref()
                .map(|snapshot| snapshot.normalized.as_ref())
                .expect("fixture should have snapshot"),
        )
        .expect("fixture JSON should parse");
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let staged_writes = empty_staged_write_set();
        let input = validation_input(&staged_writes, &visible_schemas);
        let _catalog = catalog_from_transaction_input(&input).expect("catalog should build");
        let unresolved = validate_pending_foreign_keys(
            &indexes,
            &[(
                PreparedValidationRow::State(&child),
                test_plan_from_schema(fk_child_schema()),
                &child_snapshot,
            )],
        )
        .expect("pending FK validation should collect unresolved check");
        let live_state = StaticLiveStateReader {
            rows: vec![MaterializedLiveStateRow::from(fk_parent_row(
                "parent-1",
                "version-a",
            ))],
        };

        let still_unresolved = validate_committed_foreign_keys(
            &TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &live_state,
            ),
            &indexes,
            &unresolved,
        )
        .await
        .expect("committed FK lookup should scan live state");

        assert!(
            still_unresolved.is_empty(),
            "same-version committed parent should satisfy unresolved FK"
        );
    }

    #[tokio::test]
    async fn committed_fk_lookup_keeps_normal_fk_unresolved_across_versions() {
        let indexes = PendingConstraintIndexes::default();
        let child = fk_child_row("child-1", "parent-1", "version-a");
        let child_snapshot = serde_json::from_str::<JsonValue>(
            child
                .snapshot
                .as_ref()
                .map(|snapshot| snapshot.normalized.as_ref())
                .expect("fixture should have snapshot"),
        )
        .expect("fixture JSON should parse");
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let staged_writes = empty_staged_write_set();
        let input = validation_input(&staged_writes, &visible_schemas);
        let _catalog = catalog_from_transaction_input(&input).expect("catalog should build");
        let unresolved = validate_pending_foreign_keys(
            &indexes,
            &[(
                PreparedValidationRow::State(&child),
                test_plan_from_schema(fk_child_schema()),
                &child_snapshot,
            )],
        )
        .expect("pending FK validation should collect unresolved check");
        let live_state = StaticLiveStateReader {
            rows: vec![MaterializedLiveStateRow::from(fk_parent_row(
                "parent-1",
                "version-b",
            ))],
        };

        let still_unresolved = validate_committed_foreign_keys(
            &TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &live_state,
            ),
            &indexes,
            &unresolved,
        )
        .await
        .expect("committed FK lookup should scan live state");

        assert_eq!(
            still_unresolved.len(),
            1,
            "committed FK lookup is exact-version scoped"
        );
    }

    #[tokio::test]
    async fn committed_fk_lookup_resolves_state_surface_fk_by_exact_identity() {
        let indexes = PendingConstraintIndexes::default();
        let row = state_surface_ref_row("ref-1", "target-1", "fk_parent_schema", "file-a");
        let snapshot = serde_json::from_str::<JsonValue>(
            row.snapshot
                .as_ref()
                .map(|snapshot| snapshot.normalized.as_ref())
                .expect("fixture should have snapshot"),
        )
        .expect("fixture JSON should parse");
        let visible_schemas = vec![state_surface_ref_schema()];
        let staged_writes = empty_staged_write_set();
        let input = validation_input(&staged_writes, &visible_schemas);
        let _catalog = catalog_from_transaction_input(&input).expect("catalog should build");
        let unresolved = validate_pending_foreign_keys(
            &indexes,
            &[(
                PreparedValidationRow::State(&row),
                test_plan_from_schema(state_surface_ref_schema()),
                &snapshot,
            )],
        )
        .expect("pending FK validation should collect unresolved check");
        let live_state = StaticLiveStateReader {
            rows: vec![MaterializedLiveStateRow::from(fk_parent_row(
                "target-1",
                "version-a",
            ))],
        };

        let still_unresolved = validate_committed_foreign_keys(
            &TransactionValidationInput::from_visible_schemas_for_tests(
                &staged_writes,
                &visible_schemas,
                &live_state,
            ),
            &indexes,
            &unresolved,
        )
        .await
        .expect("committed FK lookup should load exact live-state row");

        assert!(
            still_unresolved.is_empty(),
            "committed state-surface target should satisfy unresolved FK"
        );
    }

    fn empty_staged_write_set() -> PreparedWriteSet {
        PreparedWriteSet {
            state_rows: Vec::new(),
            adopted_rows: Vec::new(),
            insert_identities: BTreeMap::new(),
            commit_members_by_version: BTreeMap::new(),
            extra_commit_parents_by_version: BTreeMap::new(),
            file_data_writes: Vec::new(),
            json_writer: crate::json_store::JsonStoreContext::new().writer(),
        }
    }

    fn live_state_row_matches_scan(
        row: &MaterializedLiveStateRow,
        request: &LiveStateScanRequest,
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
            && (request.filter.version_ids.is_empty()
                || request.filter.version_ids.contains(&row.version_id))
            && (request.filter.file_ids.is_empty()
                || request
                    .filter
                    .file_ids
                    .iter()
                    .any(|filter| filter.matches(row.file_id.as_ref())))
    }

    fn live_state_row_matches_load(
        row: &MaterializedLiveStateRow,
        request: &LiveStateRowRequest,
    ) -> bool {
        row.schema_key == request.schema_key
            && row.version_id == request.version_id
            && row.entity_id == request.entity_id
            && request.file_id.matches(row.file_id.as_ref())
    }

    fn test_file_descriptor_rows() -> Vec<MaterializedLiveStateRow> {
        vec![
            committed_file_descriptor_row("file-a", "version-a"),
            committed_file_descriptor_row("file-a", "version-b"),
            committed_file_descriptor_row("file-b", "version-a"),
            committed_file_descriptor_row("file-b", "version-b"),
        ]
    }

    fn pending_registered_schema_row(schema_key: &str) -> PreparedStateRow {
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

    fn pending_registered_schema_from_definition(schema: JsonValue) -> PreparedStateRow {
        let key = schema_key_from_definition(&schema).expect("test schema should have a key");
        PreparedStateRow {
            schema_plan_id: crate::schema_catalog::SchemaPlanId::for_test(0),
            facts: crate::transaction::types::PreparedRowFacts::default(),
            entity_id: registered_schema_entity_id(&key.schema_key),
            schema_key: REGISTERED_SCHEMA_KEY.to_string(),
            file_id: None,
            snapshot: Some(test_stage_json(&json!({ "value": schema }).to_string())),
            metadata: None,
            origin: None,
            created_at: "2026-04-29T00:00:00.000Z".to_string(),
            updated_at: "2026-04-29T00:00:00.000Z".to_string(),
            global: true,
            change_id: Some("change-registered-schema".to_string()),
            commit_id: Some("commit-registered-schema".to_string()),
            untracked: false,
            version_id: crate::GLOBAL_VERSION_ID.to_string(),
        }
    }

    fn registered_schema_entity_id(schema_key: &str) -> crate::entity_identity::EntityIdentity {
        crate::entity_identity::EntityIdentity::from_primary_key_paths(
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

    fn composite_message_schema() -> JsonValue {
        json!({
            "x-lix-key": "composite_message_schema",
            "x-lix-primary-key": ["/key", "/locale"],
            "type": "object",
            "properties": {
                "key": { "type": "string" },
                "locale": { "type": "string" },
                "text": { "type": "string" }
            },
            "required": ["key", "locale", "text"],
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

    fn state_surface_ref_schema() -> JsonValue {
        json!({
            "x-lix-key": "state_surface_ref_schema",
            "x-lix-primary-key": ["/id"],
            "x-lix-state-foreign-keys": [
                ["/target_entity_id", "/target_schema_key", "/target_file_id"]
            ],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "target_entity_id": {
                    "type": "array",
                    "items": { "type": "string" },
                    "minItems": 1
                },
                "target_schema_key": { "type": "string" },
                "target_file_id": { "type": ["string", "null"] }
            },
            "required": ["id", "target_entity_id", "target_schema_key", "target_file_id"],
            "additionalProperties": false
        })
    }

    fn unique_row(entity_id: &str, slug: &str, title: &str) -> PreparedStateRow {
        let mut row = staged_row(
            "unique_schema",
            Some(
                json!({
                    "id": entity_id,
                    "slug": slug,
                    "title": title,
                })
                .to_string(),
            ),
        );
        row.entity_id = crate::entity_identity::EntityIdentity::single(entity_id);
        row.file_id = Some("file-a".to_string());
        row.version_id = "version-a".to_string();
        row.global = false;
        row
    }

    fn nullable_unique_row(entity_id: &str, scope: Option<&str>, name: &str) -> PreparedStateRow {
        let mut row = staged_row(
            "nullable_unique_schema",
            Some(
                json!({
                    "id": entity_id,
                    "scope": scope,
                    "name": name,
                })
                .to_string(),
            ),
        );
        row.entity_id = crate::entity_identity::EntityIdentity::single(entity_id);
        row.file_id = Some("file-a".to_string());
        row.version_id = "version-a".to_string();
        row.global = false;
        row
    }

    fn fk_parent_row(entity_id: &str, version_id: &str) -> PreparedStateRow {
        let mut row = staged_row(
            "fk_parent_schema",
            Some(json!({ "id": entity_id }).to_string()),
        );
        row.entity_id = crate::entity_identity::EntityIdentity::single(entity_id);
        row.file_id = Some("file-a".to_string());
        row.version_id = version_id.to_string();
        row.global = false;
        row
    }

    fn fk_child_row(entity_id: &str, parent_id: &str, version_id: &str) -> PreparedStateRow {
        let mut row = staged_row(
            "fk_child_schema",
            Some(json!({ "id": entity_id, "parent_id": parent_id }).to_string()),
        );
        row.entity_id = crate::entity_identity::EntityIdentity::single(entity_id);
        row.file_id = Some("file-a".to_string());
        row.version_id = version_id.to_string();
        row.global = false;
        row
    }

    fn composite_message_row(key: &str, locale: &str, version_id: &str) -> PreparedStateRow {
        let snapshot = json!({
            "key": key,
            "locale": locale,
            "text": "Welcome",
        });
        let mut row = staged_row("composite_message_schema", Some(snapshot.to_string()));
        row.entity_id = EntityIdentity::from_primary_key_paths(
            &snapshot,
            &[vec!["key".to_string()], vec!["locale".to_string()]],
        )
        .expect("composite message identity should derive");
        row.file_id = Some("file-a".to_string());
        row.version_id = version_id.to_string();
        row.global = false;
        row
    }

    fn state_surface_ref_row(
        entity_id: &str,
        target_entity_id: &str,
        target_schema_key: &str,
        target_file_id: &str,
    ) -> PreparedStateRow {
        state_surface_ref_row_with_target_entity_id(
            entity_id,
            json!([target_entity_id]),
            target_schema_key,
            target_file_id,
        )
    }

    fn state_surface_ref_row_with_target_entity_id(
        entity_id: &str,
        target_entity_id: JsonValue,
        target_schema_key: &str,
        target_file_id: &str,
    ) -> PreparedStateRow {
        let mut row = staged_row(
            "state_surface_ref_schema",
            Some(
                json!({
                    "id": entity_id,
                    "target_entity_id": target_entity_id,
                    "target_schema_key": target_schema_key,
                    "target_file_id": target_file_id,
                })
                .to_string(),
            ),
        );
        row.entity_id = crate::entity_identity::EntityIdentity::single(entity_id);
        row.file_id = Some("file-a".to_string());
        row.version_id = "version-a".to_string();
        row.global = false;
        row
    }

    fn mark_prepared_row_untracked(row: &mut PreparedStateRow) {
        row.untracked = true;
        row.change_id = None;
        row.commit_id = None;
    }

    fn mark_live_row_untracked(row: &mut MaterializedLiveStateRow) {
        row.untracked = true;
        row.change_id = None;
        row.commit_id = None;
    }

    fn staged_file_descriptor_row(file_id: &str, version_id: &str) -> PreparedStateRow {
        let mut row = staged_row(
            FILE_DESCRIPTOR_SCHEMA_KEY,
            Some(
                json!({
                    "id": file_id,
                    "directory_id": null,
                    "name": file_id,
                    "hidden": false,
                })
                .to_string(),
            ),
        );
        row.entity_id = crate::entity_identity::EntityIdentity::single(file_id);
        row.file_id = None;
        row.version_id = version_id.to_string();
        row.global = version_id == crate::GLOBAL_VERSION_ID;
        row
    }

    fn committed_file_descriptor_row(file_id: &str, version_id: &str) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow::from(staged_file_descriptor_row(file_id, version_id))
    }

    fn directory_descriptor_row(
        directory_id: &str,
        parent_id: Option<&str>,
        name: &str,
        version_id: &str,
    ) -> PreparedStateRow {
        let mut row = staged_row(
            DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
            Some(
                json!({
                    "id": directory_id,
                    "parent_id": parent_id,
                    "name": name,
                    "hidden": false,
                })
                .to_string(),
            ),
        );
        row.entity_id = crate::entity_identity::EntityIdentity::single(directory_id);
        row.file_id = None;
        row.version_id = version_id.to_string();
        row.global = version_id == crate::GLOBAL_VERSION_ID;
        row
    }

    fn committed_unique_row(entity_id: &str, slug: &str, title: &str) -> MaterializedLiveStateRow {
        let row = unique_row(entity_id, slug, title);
        MaterializedLiveStateRow {
            entity_id: row.entity_id,
            schema_key: row.schema_key,
            file_id: row.file_id,
            snapshot_content: row.snapshot.as_ref().map(|snapshot| snapshot.materialize()),
            metadata: row.metadata.as_ref().map(|metadata| metadata.materialize()),
            created_at: row.created_at,
            updated_at: row.updated_at,
            global: row.global,
            change_id: row.change_id,
            commit_id: row.commit_id,
            untracked: row.untracked,
            version_id: row.version_id,
        }
    }

    fn committed_nullable_unique_row(
        entity_id: &str,
        scope: Option<&str>,
        name: &str,
    ) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow::from(nullable_unique_row(entity_id, scope, name))
    }

    fn staged_row(schema_key: &str, snapshot_content: Option<String>) -> PreparedStateRow {
        PreparedStateRow {
            schema_plan_id: crate::schema_catalog::SchemaPlanId::for_test(0),
            facts: crate::transaction::types::PreparedRowFacts::default(),
            entity_id: crate::entity_identity::EntityIdentity::single("entity-1"),
            schema_key: schema_key.to_string(),
            file_id: None,
            snapshot: snapshot_content.as_deref().map(test_stage_json),
            metadata: None,
            origin: None,
            created_at: "2026-04-29T00:00:00.000Z".to_string(),
            updated_at: "2026-04-29T00:00:00.000Z".to_string(),
            global: true,
            change_id: Some("change-1".to_string()),
            commit_id: Some("commit-1".to_string()),
            untracked: false,
            version_id: crate::GLOBAL_VERSION_ID.to_string(),
        }
    }
}
