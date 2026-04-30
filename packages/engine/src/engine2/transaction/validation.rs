use std::collections::BTreeMap;

use jsonschema::JSONSchema;
use serde_json::Value as JsonValue;

use crate::common::json_pointer_get;
use crate::engine2::entity_identity::{EntityIdentity, EntityIdentityError, EntityIdentityPart};
use crate::engine2::live_state::{
    LiveStateFilter, LiveStateReader, LiveStateRow, LiveStateRowIdentity, LiveStateRowRequest,
    LiveStateScanRequest,
};
use crate::engine2::transaction::staging::StagedWriteSet;
use crate::engine2::transaction::types::StagedStateRow;
use crate::schema::{
    builtin_schema_definition, compile_lix_schema, format_lix_schema_validation_errors,
    schema_from_registered_snapshot, schema_key_from_definition, validate_lix_schema,
    validate_lix_schema_definition, SchemaKey,
};
use crate::{LixError, NullableKeyFilter};

const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";
const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const STATE_SURFACE_SCHEMA_KEY: &str = "lix_state";

/// Immutable view of the final transaction write set before persistence.
///
/// Validation intentionally runs after staging has coalesced overwrites and
/// hydrated generated fields, but before changelog, tracked-state, untracked
/// state, or binary CAS writes are flushed.
pub(crate) struct TransactionValidationInput<'a> {
    staged_writes: &'a StagedWriteSet,
    visible_schemas: &'a [JsonValue],
    live_state: &'a dyn LiveStateReader,
}

impl<'a> TransactionValidationInput<'a> {
    pub(crate) fn new(
        staged_writes: &'a StagedWriteSet,
        visible_schemas: &'a [JsonValue],
        live_state: &'a dyn LiveStateReader,
    ) -> Self {
        Self {
            staged_writes,
            visible_schemas,
            live_state,
        }
    }
}

/// Validates the final transaction write set before durable persistence.
///
/// The validator owns semantic write correctness for every engine2 write
/// frontend. It builds one transaction-visible schema catalog, validates pending
/// schema registrations, checks exact schema existence, and validates each
/// non-tombstone snapshot against the compiled JSON Schema for its
/// `(schema_key, schema_version)`.
///
/// Cross-row constraints such as `x-lix-unique` and foreign keys should also
/// live here so they can share transaction-local indexes and see the final
/// coalesced staged write set.
pub(crate) async fn validate_staged_writes(
    input: TransactionValidationInput<'_>,
) -> Result<(), LixError> {
    let schema_catalog = SchemaCatalogSnapshot::from_transaction_input(&input)?;
    let pending_file_descriptors =
        PendingFileDescriptorIndex::from_staged_writes(input.staged_writes);
    let mut compiled_schemas = CompiledSchemaCatalog::new(&schema_catalog);
    let mut pending_constraints = PendingConstraintIndexes::default();
    let mut staged_snapshots = Vec::new();
    for row in &input.staged_writes.state_rows {
        validate_staged_row_shape(row)?;
        validate_schema_exists(row, &schema_catalog)?;
        let snapshot = validate_snapshot_content(row, &mut compiled_schemas)?;
        if let Some(snapshot) = snapshot.as_ref() {
            let schema = schema_catalog
                .schema(&row.schema_key, &row.schema_version)
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_SCHEMA_DEFINITION,
                        format!(
                            "schema '{}' version '{}' is not visible to this transaction",
                            row.schema_key, row.schema_version
                        ),
                    )
                })?;
            validate_file_owner_reference(&input, &pending_file_descriptors, row).await?;
            validate_primary_key_identity(row, schema, snapshot)?;
            pending_constraints.remember_row(row, schema, snapshot)?;
            pending_constraints.remember_foreign_key_references(
                &schema_catalog,
                row,
                schema,
                snapshot,
            )?;
            staged_snapshots.push((row, schema, snapshot.clone()));
        } else {
            pending_constraints.remember_tombstone(row);
        }
    }
    let unresolved_foreign_keys =
        validate_pending_foreign_keys(&schema_catalog, &pending_constraints, &staged_snapshots)?;
    validate_pending_delete_restrictions(&schema_catalog, &pending_constraints)?;
    let unresolved_foreign_keys =
        validate_committed_foreign_keys(&input, &pending_constraints, &unresolved_foreign_keys)
            .await?;
    reject_unresolved_foreign_keys(&unresolved_foreign_keys)?;
    validate_committed_delete_restrictions(&input, &schema_catalog, &pending_constraints).await?;
    validate_committed_unique_constraints(&input, &pending_constraints).await?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingFileDescriptorState {
    Present,
    Tombstone,
}

#[derive(Debug, Clone, Default)]
struct PendingFileDescriptorIndex {
    by_version_and_file_id: BTreeMap<(String, String), PendingFileDescriptorState>,
}

impl PendingFileDescriptorIndex {
    fn from_staged_writes(staged_writes: &StagedWriteSet) -> Self {
        let mut index = Self::default();
        for row in &staged_writes.state_rows {
            if row.schema_key != FILE_DESCRIPTOR_SCHEMA_KEY || row.file_id.is_some() {
                continue;
            }
            if let Ok(file_id) = row.entity_id.as_string() {
                let state = if row.snapshot_content.is_some() {
                    PendingFileDescriptorState::Present
                } else {
                    PendingFileDescriptorState::Tombstone
                };
                index
                    .by_version_and_file_id
                    .insert((row.version_id.clone(), file_id), state);
            }
        }
        index
    }

    fn state(&self, version_id: &str, file_id: &str) -> Option<PendingFileDescriptorState> {
        self.by_version_and_file_id
            .get(&(version_id.to_string(), file_id.to_string()))
            .copied()
    }
}

async fn validate_file_owner_reference(
    input: &TransactionValidationInput<'_>,
    pending_file_descriptors: &PendingFileDescriptorIndex,
    row: &StagedStateRow,
) -> Result<(), LixError> {
    let Some(file_id) = row.file_id.as_deref() else {
        return Ok(());
    };

    if pending_file_descriptor_exists(pending_file_descriptors, &row.version_id, file_id) {
        return Ok(());
    }

    if committed_file_descriptor_exists(input.live_state, &row.version_id, file_id).await? {
        return Ok(());
    }

    Err(missing_file_owner_reference_error(row, file_id)?)
}

fn pending_file_descriptor_exists(
    pending_file_descriptors: &PendingFileDescriptorIndex,
    version_id: &str,
    file_id: &str,
) -> bool {
    matches!(
        pending_file_descriptors.state(version_id, file_id),
        Some(PendingFileDescriptorState::Present)
    )
}

async fn committed_file_descriptor_exists(
    live_state: &dyn LiveStateReader,
    version_id: &str,
    file_id: &str,
) -> Result<bool, LixError> {
    committed_file_descriptor_exists_in_exact_version(live_state, version_id, file_id).await
}

async fn committed_file_descriptor_exists_in_exact_version(
    live_state: &dyn LiveStateReader,
    version_id: &str,
    file_id: &str,
) -> Result<bool, LixError> {
    let Some(row) = live_state
        .load_row(&LiveStateRowRequest {
            schema_key: FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
            version_id: version_id.to_string(),
            entity_id: EntityIdentity::single(file_id),
            file_id: NullableKeyFilter::Null,
        })
        .await?
    else {
        return Ok(false);
    };
    Ok(row.snapshot_content.is_some()
        && row.schema_key == FILE_DESCRIPTOR_SCHEMA_KEY
        && row.entity_id == EntityIdentity::single(file_id)
        && row.file_id.is_none()
        && committed_row_is_exact_version_scoped(&row, version_id))
}

fn missing_file_owner_reference_error(
    row: &StagedStateRow,
    file_id: &str,
) -> Result<LixError, LixError> {
    Ok(LixError::new(
        LixError::CODE_UNKNOWN,
        format!(
            "file ownership validation failed for schema '{}': entity '{}' references missing file_id '{}' in effective file scope for version '{}'",
            row.schema_key,
            row.entity_id.as_string()?,
            file_id,
            row.version_id
        ),
    ))
}

fn validate_staged_row_shape(row: &StagedStateRow) -> Result<(), LixError> {
    if row.schema_key.is_empty() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "engine2 transaction validation requires non-empty schema_key",
        ));
    }
    if row.schema_version.is_empty() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "engine2 transaction validation requires non-empty schema_version",
        ));
    }
    Ok(())
}

fn validate_schema_exists(
    row: &StagedStateRow,
    schema_catalog: &SchemaCatalogSnapshot,
) -> Result<(), LixError> {
    if !schema_catalog.contains(&row.schema_key, &row.schema_version) {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            format!(
                "schema '{}' version '{}' is not visible to this transaction",
                row.schema_key, row.schema_version
            ),
        ));
    }
    Ok(())
}

fn validate_snapshot_content(
    row: &StagedStateRow,
    compiled_schemas: &mut CompiledSchemaCatalog<'_>,
) -> Result<Option<JsonValue>, LixError> {
    let Some(snapshot_content) = row.snapshot_content.as_deref() else {
        return Ok(None);
    };
    let snapshot = serde_json::from_str::<JsonValue>(snapshot_content).map_err(|error| {
        LixError::new(
            LixError::CODE_SCHEMA_VALIDATION,
            format!(
                "snapshot_content for schema '{}' version '{}' is invalid JSON: {error}",
                row.schema_key, row.schema_version
            ),
        )
    })?;
    let compiled_schema = compiled_schemas.compiled_schema(&row.schema_key, &row.schema_version)?;
    if let Err(errors) = compiled_schema.validate(&snapshot) {
        let details = format_lix_schema_validation_errors(errors);
        return Err(LixError::new(
            LixError::CODE_SCHEMA_VALIDATION,
            format!(
                "snapshot_content validation failed for schema '{}' version '{}': {details}",
                row.schema_key, row.schema_version
            ),
        ));
    }
    Ok(Some(snapshot))
}

fn validate_primary_key_identity(
    row: &StagedStateRow,
    schema: &JsonValue,
    snapshot: &JsonValue,
) -> Result<(), LixError> {
    let Some(primary_key_paths) = pointer_groups(schema, "x-lix-primary-key")?
        .into_iter()
        .next()
    else {
        return Ok(());
    };
    let derived = EntityIdentity::from_primary_key_paths(snapshot, &primary_key_paths)
        .map_err(|error| primary_key_identity_error(row, &primary_key_paths, error))?;
    if row.entity_id != derived {
        return Err(LixError::new(
            LixError::CODE_UNIQUE,
            format!(
                "primary-key constraint violation on schema '{}' version '{}': entity_id '{}' does not match derived primary key '{}'",
                row.schema_key,
                row.schema_version,
                row.entity_id.as_string()?,
                derived.as_string()?
            ),
        ));
    }
    Ok(())
}

#[derive(Default)]
struct PendingConstraintIndexes {
    unique_values: BTreeMap<PendingUniqueKey, EntityIdentity>,
    identity_targets: Vec<LiveStateRowIdentity>,
    fk_targets: BTreeMap<PendingForeignKeyTargetKey, Vec<EntityIdentity>>,
    fk_references: BTreeMap<PendingForeignKeyReferenceTarget, Vec<LiveStateRowIdentity>>,
    tombstones: Vec<PendingTombstone>,
}

impl PendingConstraintIndexes {
    fn remember_tombstone(&mut self, row: &StagedStateRow) {
        self.tombstones.push(PendingTombstone {
            identity: LiveStateRowIdentity {
                version_id: row.version_id.clone(),
                schema_key: row.schema_key.clone(),
                entity_id: row.entity_id.clone(),
                file_id: row.file_id.clone(),
            },
            schema_version: row.schema_version.clone(),
        });
    }

    fn remember_row(
        &mut self,
        row: &StagedStateRow,
        schema: &JsonValue,
        snapshot: &JsonValue,
    ) -> Result<(), LixError> {
        self.remember_identity_target(row);
        self.remember_primary_key_target(row, schema, snapshot)?;
        self.remember_unique_targets(row, schema, snapshot)?;
        Ok(())
    }

    fn remember_identity_target(&mut self, row: &StagedStateRow) {
        self.identity_targets.push(LiveStateRowIdentity {
            version_id: row.version_id.clone(),
            schema_key: row.schema_key.clone(),
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
        });
    }

    fn remember_primary_key_target(
        &mut self,
        row: &StagedStateRow,
        schema: &JsonValue,
        snapshot: &JsonValue,
    ) -> Result<(), LixError> {
        for primary_key_paths in pointer_groups(schema, "x-lix-primary-key")? {
            self.remember_fk_target(row, &primary_key_paths, snapshot);
        }
        Ok(())
    }

    fn remember_unique_targets(
        &mut self,
        row: &StagedStateRow,
        schema: &JsonValue,
        snapshot: &JsonValue,
    ) -> Result<(), LixError> {
        for unique_paths in pointer_groups(schema, "x-lix-unique")? {
            let Some(value) = UniqueConstraintValue::from_snapshot(snapshot, &unique_paths) else {
                continue;
            };
            self.remember_fk_target(row, &unique_paths, snapshot);
            let key = PendingUniqueKey {
                schema_key: row.schema_key.clone(),
                schema_version: row.schema_version.clone(),
                version_id: row.version_id.clone(),
                untracked: row.untracked,
                file_id: row.file_id.clone(),
                pointer_group: unique_paths.clone(),
                value,
            };
            if let Some(existing_entity_id) = self
                .unique_values
                .insert(key.clone(), row.entity_id.clone())
            {
                if existing_entity_id != row.entity_id {
                    return Err(LixError::new(
                        LixError::CODE_UNIQUE,
                        format!(
                            "unique constraint violation on {}.{} for value {}: rows '{}' and '{}' conflict",
                            row.schema_key,
                            format_pointer_group(&key.pointer_group),
                            key.value.display(),
                            existing_entity_id.as_string()?,
                            row.entity_id.as_string()?
                        ),
                    ));
                }
            }
        }
        Ok(())
    }

    fn remember_fk_target(
        &mut self,
        row: &StagedStateRow,
        pointer_group: &[Vec<String>],
        snapshot: &JsonValue,
    ) {
        let Some(value) = UniqueConstraintValue::from_snapshot(snapshot, pointer_group) else {
            return;
        };
        self.fk_targets
            .entry(PendingForeignKeyTargetKey {
                schema_key: row.schema_key.clone(),
                schema_version: row.schema_version.clone(),
                version_id: row.version_id.clone(),
                file_id: row.file_id.clone(),
                pointer_group: pointer_group.to_vec(),
                value,
            })
            .or_default()
            .push(row.entity_id.clone());
    }

    fn remember_foreign_key_references(
        &mut self,
        schema_catalog: &SchemaCatalogSnapshot,
        row: &StagedStateRow,
        schema: &JsonValue,
        snapshot: &JsonValue,
    ) -> Result<(), LixError> {
        for foreign_key in foreign_key_definitions(schema)? {
            let Some(local_value) =
                UniqueConstraintValue::from_snapshot(snapshot, &foreign_key.local_properties)
            else {
                continue;
            };
            let target = if foreign_key.referenced_schema_key == STATE_SURFACE_SCHEMA_KEY {
                PendingForeignKeyReferenceTarget::StateSurfaceIdentity(
                    state_surface_target_identity(&row.version_id, &foreign_key, snapshot)?,
                )
            } else {
                let target_key = schema_catalog
                    .schema_key_by_key(&foreign_key.referenced_schema_key)
                    .ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_SCHEMA_DEFINITION,
                            format!(
                                "foreign key on schema '{}' references missing schema '{}'",
                                row.schema_key, foreign_key.referenced_schema_key
                            ),
                        )
                    })?;
                PendingForeignKeyReferenceTarget::Key(PendingForeignKeyTargetKey {
                    schema_key: target_key.schema_key,
                    schema_version: target_key.schema_version,
                    version_id: row.version_id.clone(),
                    file_id: row.file_id.clone(),
                    pointer_group: foreign_key.referenced_properties,
                    value: local_value,
                })
            };
            self.fk_references
                .entry(target)
                .or_default()
                .push(LiveStateRowIdentity {
                    version_id: row.version_id.clone(),
                    schema_key: row.schema_key.clone(),
                    entity_id: row.entity_id.clone(),
                    file_id: row.file_id.clone(),
                });
        }
        Ok(())
    }

    fn tombstones_identity(&self, row: &LiveStateRow) -> bool {
        let identity = LiveStateRowIdentity::from_row(row);
        self.tombstones
            .iter()
            .any(|tombstone| tombstone.identity == identity)
    }

    fn has_identity_target(&self, identity: &LiveStateRowIdentity) -> bool {
        self.identity_targets.contains(identity)
    }

    fn tombstones_target_identity(&self, identity: &LiveStateRowIdentity) -> bool {
        self.tombstones
            .iter()
            .any(|tombstone| tombstone.identity == *identity)
    }

    fn has_fk_target_key(&self, key: &PendingForeignKeyTargetKey) -> bool {
        self.fk_targets.contains_key(key)
    }

    fn active_references_to(
        &self,
        target: &PendingForeignKeyReferenceTarget,
    ) -> Vec<&LiveStateRowIdentity> {
        self.fk_references
            .get(target)
            .into_iter()
            .flat_map(|references| references.iter())
            .filter(|source_identity| !self.tombstones_target_identity(source_identity))
            .collect()
    }

    #[cfg(test)]
    fn has_fk_reference_to_key(
        &self,
        schema_key: &str,
        schema_version: &str,
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
            schema_version: schema_version.to_string(),
            version_id: version_id.to_string(),
            file_id: file_id.map(str::to_string),
            pointer_group,
            value,
        });
        Ok(self.fk_references.contains_key(&key))
    }

    #[cfg(test)]
    fn has_fk_reference_to_identity(&self, identity: LiveStateRowIdentity) -> bool {
        self.fk_references
            .contains_key(&PendingForeignKeyReferenceTarget::StateSurfaceIdentity(
                identity,
            ))
    }

    #[cfg(test)]
    fn has_fk_target(
        &self,
        schema_key: &str,
        schema_version: &str,
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
            schema_version: schema_version.to_string(),
            version_id: version_id.to_string(),
            file_id: file_id.map(str::to_string),
            pointer_group,
            value,
        };
        Ok(self.fk_targets.contains_key(&key))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingTombstone {
    identity: LiveStateRowIdentity,
    schema_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PendingUniqueKey {
    schema_key: String,
    schema_version: String,
    version_id: String,
    untracked: bool,
    file_id: Option<String>,
    pointer_group: Vec<Vec<String>>,
    value: UniqueConstraintValue,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PendingForeignKeyTargetKey {
    schema_key: String,
    schema_version: String,
    version_id: String,
    file_id: Option<String>,
    pointer_group: Vec<Vec<String>>,
    value: UniqueConstraintValue,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum PendingForeignKeyReferenceTarget {
    Key(PendingForeignKeyTargetKey),
    StateSurfaceIdentity(LiveStateRowIdentity),
}

fn validate_pending_delete_restrictions(
    schema_catalog: &SchemaCatalogSnapshot,
    pending_constraints: &PendingConstraintIndexes,
) -> Result<(), LixError> {
    for tombstone in &pending_constraints.tombstones {
        let identity_target =
            PendingForeignKeyReferenceTarget::StateSurfaceIdentity(tombstone.identity.clone());
        reject_pending_delete_references(
            &tombstone.identity,
            &identity_target,
            pending_constraints.active_references_to(&identity_target),
        )?;

        let Some(schema) =
            schema_catalog.schema(&tombstone.identity.schema_key, &tombstone.schema_version)
        else {
            continue;
        };
        for primary_key_paths in pointer_groups(schema, "x-lix-primary-key")? {
            let target = PendingForeignKeyReferenceTarget::Key(PendingForeignKeyTargetKey {
                schema_key: tombstone.identity.schema_key.clone(),
                schema_version: tombstone.schema_version.clone(),
                version_id: tombstone.identity.version_id.clone(),
                file_id: tombstone.identity.file_id.clone(),
                pointer_group: primary_key_paths,
                value: UniqueConstraintValue::from_entity_identity(&tombstone.identity.entity_id),
            });
            reject_pending_delete_references(
                &tombstone.identity,
                &target,
                pending_constraints.active_references_to(&target),
            )?;
        }
    }
    Ok(())
}

fn reject_pending_delete_references(
    deleted_identity: &LiveStateRowIdentity,
    target: &PendingForeignKeyReferenceTarget,
    references: Vec<&LiveStateRowIdentity>,
) -> Result<(), LixError> {
    let Some(reference) = references.first() else {
        return Ok(());
    };
    Err(LixError::new(
        LixError::CODE_FOREIGN_KEY,
        format!(
            "cannot delete '{}' row '{}' in version '{}' because pending row '{}' references it{}",
            deleted_identity.schema_key,
            deleted_identity.entity_id.as_string()?,
            deleted_identity.version_id,
            reference.entity_id.as_string()?,
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
            target.schema_key,
            target.entity_id.as_string()?
        )),
    }
}

async fn validate_committed_delete_restrictions(
    input: &TransactionValidationInput<'_>,
    schema_catalog: &SchemaCatalogSnapshot,
    pending_constraints: &PendingConstraintIndexes,
) -> Result<(), LixError> {
    for tombstone in &pending_constraints.tombstones {
        for (source_key, source_schema) in schema_catalog.schemas() {
            for foreign_key in foreign_key_definitions(source_schema)? {
                if foreign_key.referenced_schema_key == STATE_SURFACE_SCHEMA_KEY {
                    validate_committed_state_surface_delete_restriction(
                        input.live_state,
                        pending_constraints,
                        tombstone,
                        source_key,
                        &foreign_key,
                    )
                    .await?;
                } else if foreign_key.referenced_schema_key == tombstone.identity.schema_key {
                    validate_committed_normal_delete_restriction(
                        input.live_state,
                        pending_constraints,
                        tombstone,
                        source_key,
                        &foreign_key,
                    )
                    .await?;
                }
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
    foreign_key: &ForeignKeyDefinition,
) -> Result<(), LixError> {
    let Some(deleted_value) =
        committed_deleted_row_value(live_state, tombstone, &foreign_key.referenced_properties)
            .await?
    else {
        return Ok(());
    };
    let rows = live_state
        .scan_rows(&LiveStateScanRequest {
            filter: LiveStateFilter {
                schema_keys: vec![source_key.schema_key.clone()],
                version_ids: vec![tombstone.identity.version_id.clone()],
                file_ids: vec![nullable_filter_from_option(&tombstone.identity.file_id)],
                include_tombstones: false,
                ..Default::default()
            },
            ..Default::default()
        })
        .await?;

    for row in rows {
        if !committed_row_is_exact_version_scoped(&row, &tombstone.identity.version_id) {
            continue;
        }
        if row.schema_version != source_key.schema_version
            || row.file_id != tombstone.identity.file_id
            || pending_constraints.tombstones_identity(&row)
        {
            continue;
        }
        let Some(snapshot_content) = row.snapshot_content.as_deref() else {
            continue;
        };
        let snapshot = parse_committed_snapshot(&row, snapshot_content)?;
        if UniqueConstraintValue::from_snapshot(&snapshot, &foreign_key.local_properties).as_ref()
            == Some(&deleted_value)
        {
            return Err(committed_delete_restriction_error(
                &tombstone.identity,
                &row,
                &foreign_key.local_properties,
            )?);
        }
    }
    Ok(())
}

async fn validate_committed_state_surface_delete_restriction(
    live_state: &dyn LiveStateReader,
    pending_constraints: &PendingConstraintIndexes,
    tombstone: &PendingTombstone,
    source_key: &SchemaCatalogKey,
    foreign_key: &ForeignKeyDefinition,
) -> Result<(), LixError> {
    let rows = live_state
        .scan_rows(&LiveStateScanRequest {
            filter: LiveStateFilter {
                schema_keys: vec![source_key.schema_key.clone()],
                version_ids: vec![tombstone.identity.version_id.clone()],
                file_ids: vec![nullable_filter_from_option(&tombstone.identity.file_id)],
                include_tombstones: false,
                ..Default::default()
            },
            ..Default::default()
        })
        .await?;

    for row in rows {
        if !committed_row_is_exact_version_scoped(&row, &tombstone.identity.version_id) {
            continue;
        }
        if row.schema_version != source_key.schema_version
            || row.file_id != tombstone.identity.file_id
            || pending_constraints.tombstones_identity(&row)
        {
            continue;
        }
        let Some(snapshot_content) = row.snapshot_content.as_deref() else {
            continue;
        };
        let snapshot = parse_committed_snapshot(&row, snapshot_content)?;
        if state_surface_target_identity(&row.version_id, foreign_key, &snapshot)?
            == tombstone.identity
        {
            return Err(committed_delete_restriction_error(
                &tombstone.identity,
                &row,
                &foreign_key.local_properties,
            )?);
        }
    }
    Ok(())
}

async fn committed_deleted_row_value(
    live_state: &dyn LiveStateReader,
    tombstone: &PendingTombstone,
    referenced_properties: &[Vec<String>],
) -> Result<Option<UniqueConstraintValue>, LixError> {
    let Some(row) = live_state
        .load_row(&LiveStateRowRequest {
            schema_key: tombstone.identity.schema_key.clone(),
            version_id: tombstone.identity.version_id.clone(),
            entity_id: tombstone.identity.entity_id.clone(),
            file_id: nullable_filter_from_option(&tombstone.identity.file_id),
        })
        .await?
    else {
        return Ok(None);
    };
    if !committed_row_is_exact_version_scoped(&row, &tombstone.identity.version_id)
        || row.schema_version != tombstone.schema_version
        || row.file_id != tombstone.identity.file_id
    {
        return Ok(None);
    }
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
    deleted_identity: &LiveStateRowIdentity,
    referencing_row: &LiveStateRow,
    local_properties: &[Vec<String>],
) -> Result<LixError, LixError> {
    Ok(LixError::new(
        LixError::CODE_FOREIGN_KEY,
        format!(
            "cannot delete '{}' row '{}' in version '{}' because committed row '{}' references it through {}",
            deleted_identity.schema_key,
            deleted_identity.entity_id.as_string()?,
            deleted_identity.version_id,
            referencing_row.entity_id.as_string()?,
            format_pointer_group(local_properties)
        ),
    ))
}

fn parse_committed_snapshot(
    row: &LiveStateRow,
    snapshot_content: &str,
) -> Result<JsonValue, LixError> {
    serde_json::from_str::<JsonValue>(snapshot_content).map_err(|error| {
        LixError::new(
            LixError::CODE_SCHEMA_VALIDATION,
            format!(
                "committed snapshot_content for schema '{}' version '{}' is invalid JSON: {error}",
                row.schema_key, row.schema_version
            ),
        )
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct UnresolvedForeignKeyCheck {
    source_identity: LiveStateRowIdentity,
    source_schema_key: String,
    source_pointer_group: Vec<Vec<String>>,
    target: UnresolvedForeignKeyTarget,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum UnresolvedForeignKeyTarget {
    Key(PendingForeignKeyTargetKey),
    StateSurfaceIdentity(LiveStateRowIdentity),
}

fn validate_pending_foreign_keys(
    schema_catalog: &SchemaCatalogSnapshot,
    pending_constraints: &PendingConstraintIndexes,
    staged_snapshots: &[(&StagedStateRow, &JsonValue, JsonValue)],
) -> Result<Vec<UnresolvedForeignKeyCheck>, LixError> {
    let mut unresolved = Vec::new();
    for (row, schema, snapshot) in staged_snapshots {
        for foreign_key in foreign_key_definitions(schema)? {
            let Some(local_value) =
                UniqueConstraintValue::from_snapshot(snapshot, &foreign_key.local_properties)
            else {
                continue;
            };
            if foreign_key.referenced_schema_key == STATE_SURFACE_SCHEMA_KEY {
                if let Some(check) = validate_pending_state_surface_foreign_key(
                    row,
                    &foreign_key,
                    snapshot,
                    pending_constraints,
                )? {
                    unresolved.push(check);
                }
            } else {
                if let Some(check) = validate_pending_normal_foreign_key(
                    schema_catalog,
                    row,
                    &foreign_key,
                    local_value,
                    pending_constraints,
                )? {
                    unresolved.push(check);
                }
            }
        }
    }
    Ok(unresolved)
}

fn validate_pending_normal_foreign_key(
    schema_catalog: &SchemaCatalogSnapshot,
    row: &StagedStateRow,
    foreign_key: &ForeignKeyDefinition,
    local_value: UniqueConstraintValue,
    pending_constraints: &PendingConstraintIndexes,
) -> Result<Option<UnresolvedForeignKeyCheck>, LixError> {
    let target_key = schema_catalog
        .schema_key_by_key(&foreign_key.referenced_schema_key)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                format!(
                    "foreign key on schema '{}' references missing schema '{}'",
                    row.schema_key, foreign_key.referenced_schema_key
                ),
            )
        })?;
    let key = PendingForeignKeyTargetKey {
        schema_key: target_key.schema_key,
        schema_version: target_key.schema_version,
        version_id: row.version_id.clone(),
        file_id: row.file_id.clone(),
        pointer_group: foreign_key.referenced_properties.clone(),
        value: local_value,
    };
    if pending_constraints.has_fk_target_key(&key) {
        return Ok(None);
    }
    Ok(Some(UnresolvedForeignKeyCheck {
        source_identity: LiveStateRowIdentity {
            version_id: row.version_id.clone(),
            schema_key: row.schema_key.clone(),
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
        },
        source_schema_key: row.schema_key.clone(),
        source_pointer_group: foreign_key.local_properties.clone(),
        target: UnresolvedForeignKeyTarget::Key(key),
    }))
}

fn validate_pending_state_surface_foreign_key(
    row: &StagedStateRow,
    foreign_key: &ForeignKeyDefinition,
    snapshot: &JsonValue,
    pending_constraints: &PendingConstraintIndexes,
) -> Result<Option<UnresolvedForeignKeyCheck>, LixError> {
    let target_identity = state_surface_target_identity(&row.version_id, foreign_key, snapshot)?;
    if pending_constraints.tombstones_target_identity(&target_identity) {
        return Err(LixError::new(
            LixError::CODE_FOREIGN_KEY,
            format!(
                "foreign key on {}.{} references target deleted in this transaction",
                row.schema_key,
                format_pointer_group(&foreign_key.local_properties)
            ),
        ));
    }
    if pending_constraints.has_identity_target(&target_identity) {
        return Ok(None);
    }
    Ok(Some(UnresolvedForeignKeyCheck {
        source_identity: LiveStateRowIdentity {
            version_id: row.version_id.clone(),
            schema_key: row.schema_key.clone(),
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
        },
        source_schema_key: row.schema_key.clone(),
        source_pointer_group: foreign_key.local_properties.clone(),
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
            check.source_identity.entity_id.as_string()?,
            format_pointer_group(&check.source_pointer_group),
            check.source_identity.version_id,
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
            target.schema_key,
            target.entity_id.as_string()?
        )),
    }
}

async fn committed_normal_foreign_key_target_exists(
    live_state: &dyn LiveStateReader,
    pending_constraints: &PendingConstraintIndexes,
    target: &PendingForeignKeyTargetKey,
) -> Result<bool, LixError> {
    let rows = live_state
        .scan_rows(&LiveStateScanRequest {
            filter: LiveStateFilter {
                schema_keys: vec![target.schema_key.clone()],
                version_ids: vec![target.version_id.clone()],
                file_ids: vec![nullable_filter_from_option(&target.file_id)],
                include_tombstones: false,
                ..Default::default()
            },
            ..Default::default()
        })
        .await?;

    for row in rows {
        if !committed_row_is_exact_version_scoped(&row, &target.version_id) {
            continue;
        }
        if pending_constraints.tombstones_identity(&row) {
            continue;
        }
        if row.schema_key != target.schema_key
            || row.schema_version != target.schema_version
            || row.file_id != target.file_id
        {
            continue;
        }
        let Some(snapshot_content) = row.snapshot_content.as_deref() else {
            continue;
        };
        let snapshot = serde_json::from_str::<JsonValue>(snapshot_content).map_err(|error| {
            LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!(
                    "committed snapshot_content for schema '{}' version '{}' is invalid JSON: {error}",
                    row.schema_key, row.schema_version
                ),
            )
        })?;
        if UniqueConstraintValue::from_snapshot(&snapshot, &target.pointer_group).as_ref()
            == Some(&target.value)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn committed_state_surface_foreign_key_target_exists(
    live_state: &dyn LiveStateReader,
    pending_constraints: &PendingConstraintIndexes,
    target_identity: &LiveStateRowIdentity,
) -> Result<bool, LixError> {
    let Some(row) = live_state
        .load_row(&LiveStateRowRequest {
            schema_key: target_identity.schema_key.clone(),
            version_id: target_identity.version_id.clone(),
            entity_id: target_identity.entity_id.clone(),
            file_id: nullable_filter_from_option(&target_identity.file_id),
        })
        .await?
    else {
        return Ok(false);
    };
    if pending_constraints.tombstones_identity(&row) {
        return Ok(false);
    }
    Ok(
        committed_row_is_exact_version_scoped(&row, &target_identity.version_id)
            && row.schema_key == target_identity.schema_key
            && row.entity_id == target_identity.entity_id
            && row.file_id == target_identity.file_id,
    )
}

fn state_surface_target_identity(
    version_id: &str,
    foreign_key: &ForeignKeyDefinition,
    snapshot: &JsonValue,
) -> Result<LiveStateRowIdentity, LixError> {
    let entity_id =
        state_surface_local_value_for_referenced_pointer(foreign_key, snapshot, "/entity_id")?;
    let schema_key =
        state_surface_local_value_for_referenced_pointer(foreign_key, snapshot, "/schema_key")?;
    let file_id = state_surface_optional_local_value_for_referenced_pointer(
        foreign_key,
        snapshot,
        "/file_id",
    )?;
    Ok(LiveStateRowIdentity {
        version_id: version_id.to_string(),
        schema_key,
        entity_id: EntityIdentity::from_string(&entity_id).map_err(|error| {
            LixError::new(
                LixError::CODE_FOREIGN_KEY,
                format!("state-surface foreign key entity_id is invalid: {error}"),
            )
        })?,
        file_id,
    })
}

fn state_surface_local_value_for_referenced_pointer(
    foreign_key: &ForeignKeyDefinition,
    snapshot: &JsonValue,
    referenced_pointer: &str,
) -> Result<String, LixError> {
    state_surface_optional_local_value_for_referenced_pointer(
        foreign_key,
        snapshot,
        referenced_pointer,
    )?
    .ok_or_else(|| {
        LixError::new(
            LixError::CODE_FOREIGN_KEY,
            format!("state-surface foreign key target '{referenced_pointer}' is missing"),
        )
    })
}

fn state_surface_optional_local_value_for_referenced_pointer(
    foreign_key: &ForeignKeyDefinition,
    snapshot: &JsonValue,
    referenced_pointer: &str,
) -> Result<Option<String>, LixError> {
    let referenced_pointer = parse_json_pointer(referenced_pointer)?;
    let Some(index) = foreign_key
        .referenced_properties
        .iter()
        .position(|pointer| pointer == &referenced_pointer)
    else {
        return Ok(None);
    };
    let local_pointer = &foreign_key.local_properties[index];
    let Some(value) = json_pointer_get(snapshot, local_pointer) else {
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
                LixError::CODE_FOREIGN_KEY,
                format!(
                    "state-surface foreign key value at '{}' must be a string",
                    format_json_pointer(local_pointer)
                ),
            )
        })
}

async fn validate_committed_unique_constraints(
    input: &TransactionValidationInput<'_>,
    pending_constraints: &PendingConstraintIndexes,
) -> Result<(), LixError> {
    for (key, pending_entity_id) in &pending_constraints.unique_values {
        let committed_rows = input
            .live_state
            .scan_rows(&LiveStateScanRequest {
                filter: LiveStateFilter {
                    schema_keys: vec![key.schema_key.clone()],
                    version_ids: vec![key.version_id.clone()],
                    file_ids: vec![nullable_filter_from_option(&key.file_id)],
                    include_tombstones: false,
                    ..Default::default()
                },
                ..Default::default()
            })
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
            let snapshot = serde_json::from_str::<JsonValue>(snapshot_content).map_err(|error| {
                LixError::new(
                    LixError::CODE_SCHEMA_VALIDATION,
                    format!(
                        "committed snapshot_content for schema '{}' version '{}' is invalid JSON: {error}",
                        committed_row.schema_key, committed_row.schema_version
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
                        committed_row.entity_id.as_string()?,
                        pending_entity_id.as_string()?
                    ),
                ));
            }
        }
    }
    Ok(())
}

fn nullable_filter_from_option(value: &Option<String>) -> NullableKeyFilter<String> {
    match value {
        Some(value) => NullableKeyFilter::Value(value.clone()),
        None => NullableKeyFilter::Null,
    }
}

fn committed_row_is_in_exact_validation_scope(row: &LiveStateRow, key: &PendingUniqueKey) -> bool {
    // LiveStateReader may return serving projections such as global rows
    // projected into a requested version. Constraint validation is root-local:
    // only rows authored in the exact version participate.
    row.version_id == key.version_id
        && row.schema_key == key.schema_key
        && row.schema_version == key.schema_version
        && row.untracked == key.untracked
        && row.file_id == key.file_id
        && committed_row_is_exact_version_scoped(row, &key.version_id)
}

fn committed_row_is_exact_version_scoped(row: &LiveStateRow, version_id: &str) -> bool {
    row.version_id == version_id
        && row.global == (row.version_id == crate::version::GLOBAL_VERSION_ID)
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
                .map(|part| match part {
                    EntityIdentityPart::String(value) => format!("{value:?}"),
                    EntityIdentityPart::Bool(value) => value.to_string(),
                    EntityIdentityPart::Number(value) => value.clone(),
                })
                .collect(),
        )
    }

    fn from_snapshot(snapshot: &JsonValue, pointers: &[Vec<String>]) -> Option<Self> {
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
        JsonValue::Array(_) | JsonValue::Object(_) => value.to_string(),
    }
}

fn pointer_groups(schema: &JsonValue, field: &str) -> Result<Vec<Vec<Vec<String>>>, LixError> {
    let Some(value) = schema.get(field) else {
        return Ok(Vec::new());
    };
    let groups = if field == "x-lix-primary-key" {
        vec![value]
    } else {
        value
            .as_array()
            .map(|groups| groups.iter().collect())
            .unwrap_or_default()
    };
    groups
        .into_iter()
        .map(|group| {
            let group = group.as_array().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_SCHEMA_DEFINITION,
                    format!("schema {field} must contain arrays of JSON Pointers"),
                )
            })?;
            group
                .iter()
                .enumerate()
                .map(|(index, pointer)| {
                    let pointer = pointer.as_str().ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_SCHEMA_DEFINITION,
                            format!("schema {field} entry at index {index} must be a string"),
                        )
                    })?;
                    parse_json_pointer(pointer)
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .collect()
}

fn parse_json_pointer(pointer: &str) -> Result<Vec<String>, LixError> {
    if pointer.is_empty() {
        return Ok(Vec::new());
    }
    if !pointer.starts_with('/') {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            format!("invalid JSON pointer '{pointer}'"),
        ));
    }
    pointer[1..]
        .split('/')
        .map(unescape_json_pointer_segment)
        .collect()
}

fn unescape_json_pointer_segment(segment: &str) -> Result<String, LixError> {
    let mut output = String::new();
    let mut chars = segment.chars();
    while let Some(ch) = chars.next() {
        if ch == '~' {
            match chars.next() {
                Some('0') => output.push('~'),
                Some('1') => output.push('/'),
                _ => {
                    return Err(LixError::new(
                        LixError::CODE_SCHEMA_DEFINITION,
                        "invalid JSON pointer escape",
                    ));
                }
            }
        } else {
            output.push(ch);
        }
    }
    Ok(output)
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

fn format_json_pointer(pointer: &[String]) -> String {
    if pointer.is_empty() {
        return String::new();
    }
    format!(
        "/{}",
        pointer
            .iter()
            .map(|segment| segment.replace('~', "~0").replace('/', "~1"))
            .collect::<Vec<_>>()
            .join("/")
    )
}

fn primary_key_identity_error(
    row: &StagedStateRow,
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
        EntityIdentityError::NullPrimaryKeyValue { index } => {
            let pointer = format_json_pointer(&primary_key_paths[index]);
            format!("null value at primary-key pointer '{pointer}'")
        }
        EntityIdentityError::EmptyPrimaryKeyValue { index } => {
            let pointer = format_json_pointer(&primary_key_paths[index]);
            format!("empty value at primary-key pointer '{pointer}'")
        }
        EntityIdentityError::UnsupportedPrimaryKeyValue { index } => {
            let pointer = format_json_pointer(&primary_key_paths[index]);
            format!("unsupported non-scalar value at primary-key pointer '{pointer}'")
        }
        EntityIdentityError::InvalidEncodedEntityIdentity => {
            "invalid encoded entity identity".to_string()
        }
    };
    LixError::new(
        LixError::CODE_UNIQUE,
        format!(
            "primary-key constraint violation on schema '{}' version '{}': {reason}",
            row.schema_key, row.schema_version
        ),
    )
}

#[derive(Debug, Clone)]
struct ForeignKeyDefinition {
    local_properties: Vec<Vec<String>>,
    referenced_schema_key: String,
    referenced_properties: Vec<Vec<String>>,
}

fn foreign_key_definitions(schema: &JsonValue) -> Result<Vec<ForeignKeyDefinition>, LixError> {
    let Some(value) = schema.get("x-lix-foreign-keys") else {
        return Ok(Vec::new());
    };
    let Some(foreign_keys) = value.as_array() else {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            "schema x-lix-foreign-keys must be an array",
        ));
    };

    foreign_keys
        .iter()
        .enumerate()
        .map(|(index, foreign_key)| {
            let object = foreign_key.as_object().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_SCHEMA_DEFINITION,
                    format!("x-lix-foreign-keys[{index}] must be an object"),
                )
            })?;
            let references = object
                .get("references")
                .and_then(JsonValue::as_object)
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_SCHEMA_DEFINITION,
                        format!("x-lix-foreign-keys[{index}].references must be an object"),
                    )
                })?;
            let referenced_schema_key = references
                .get("schemaKey")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_SCHEMA_DEFINITION,
                        format!("x-lix-foreign-keys[{index}].references.schemaKey must be a string"),
                    )
                })?
                .to_string();
            let local_properties = pointer_array(
                object.get("properties"),
                &format!("x-lix-foreign-keys[{index}].properties"),
            )?;
            let referenced_properties = pointer_array(
                references.get("properties"),
                &format!("x-lix-foreign-keys[{index}].references.properties"),
            )?;
            if local_properties.len() != referenced_properties.len() {
                return Err(LixError::new(
                    LixError::CODE_SCHEMA_DEFINITION,
                    format!(
                        "x-lix-foreign-keys[{index}] must have the same number of local and referenced properties"
                    ),
                ));
            }
            Ok(ForeignKeyDefinition {
                local_properties,
                referenced_schema_key,
                referenced_properties,
            })
        })
        .collect()
}

fn pointer_array(value: Option<&JsonValue>, label: &str) -> Result<Vec<Vec<String>>, LixError> {
    let Some(value) = value else {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            format!("{label} is required"),
        ));
    };
    let Some(values) = value.as_array() else {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            format!("{label} must be an array"),
        ));
    };
    if values.is_empty() {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            format!("{label} must not be empty"),
        ));
    }
    values
        .iter()
        .enumerate()
        .map(|(index, pointer)| {
            let pointer = pointer.as_str().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_SCHEMA_DEFINITION,
                    format!("{label}[{index}] must be a string"),
                )
            })?;
            parse_json_pointer(pointer)
        })
        .collect()
}

fn validate_foreign_key_definition(
    catalog: &SchemaCatalogSnapshot,
    source_key: &SchemaCatalogKey,
    source_schema: &JsonValue,
    foreign_key: ForeignKeyDefinition,
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

    let target_schema = catalog
        .schema_by_key(&foreign_key.referenced_schema_key)
        .or_else(|| {
            (foreign_key.referenced_schema_key == STATE_SURFACE_SCHEMA_KEY)
                .then_some(state_surface_foreign_key_schema())
        })
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                format!(
                    "foreign key on schema '{}' references missing schema '{}'",
                    source_key.schema_key, foreign_key.referenced_schema_key
                ),
            )
        })?;

    for pointer in &foreign_key.referenced_properties {
        validate_schema_field_pointer(target_schema, pointer).map_err(|detail| {
            LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                format!(
                    "foreign key on schema '{}' references missing target property '{}.{}': {detail}",
                    source_key.schema_key,
                    foreign_key.referenced_schema_key,
                    format_json_pointer(pointer)
                ),
            )
        })?;
    }

    if foreign_key.referenced_schema_key == STATE_SURFACE_SCHEMA_KEY {
        validate_state_surface_foreign_key_target(source_key, &foreign_key)?;
    } else if !referenced_properties_are_keyed(target_schema, &foreign_key.referenced_properties)? {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            format!(
                "foreign key on schema '{}' references '{}.{}', but referenced properties must match the target primary key or a unique constraint",
                source_key.schema_key,
                foreign_key.referenced_schema_key,
                format_pointer_group(&foreign_key.referenced_properties)
            ),
        ));
    }

    Ok(())
}

fn state_surface_foreign_key_schema() -> &'static JsonValue {
    crate::schema::lix_state_surface_schema_definition()
}

fn validate_state_surface_foreign_key_target(
    source_key: &SchemaCatalogKey,
    foreign_key: &ForeignKeyDefinition,
) -> Result<(), LixError> {
    for required_pointer in ["/entity_id", "/schema_key"] {
        let required_pointer = parse_json_pointer(required_pointer)?;
        if !foreign_key
            .referenced_properties
            .iter()
            .any(|pointer| pointer == &required_pointer)
        {
            return Err(LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                format!(
                    "foreign key on schema '{}' references lix_state and must include '{}'",
                    source_key.schema_key,
                    format_json_pointer(&required_pointer)
                ),
            ));
        }
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
    target_schema: &JsonValue,
    referenced_properties: &[Vec<String>],
) -> Result<bool, LixError> {
    if let Some(primary_key) = pointer_groups(target_schema, "x-lix-primary-key")?
        .into_iter()
        .next()
    {
        if primary_key == referenced_properties {
            return Ok(true);
        }
    }
    Ok(pointer_groups(target_schema, "x-lix-unique")?
        .into_iter()
        .any(|unique_group| unique_group == referenced_properties))
}

/// Transaction-visible schema definitions indexed by exact schema identity.
///
/// The snapshot starts from schemas visible before this write, then applies
/// pending `lix_registered_schema` rows from the final staged write set. That
/// lets one transaction register a schema and write rows for it without
/// re-scanning schemas for every staged row.
#[derive(Debug, Clone, Default)]
struct SchemaCatalogSnapshot {
    schemas_by_key: BTreeMap<SchemaCatalogKey, JsonValue>,
}

impl SchemaCatalogSnapshot {
    fn from_transaction_input(input: &TransactionValidationInput<'_>) -> Result<Self, LixError> {
        let mut snapshot = Self::default();
        snapshot.remember_visible_schemas(input.visible_schemas)?;
        snapshot.remember_pending_registered_schemas(&input.staged_writes.state_rows)?;
        snapshot.validate_foreign_key_definitions()?;
        Ok(snapshot)
    }

    fn remember_visible_schemas(&mut self, visible_schemas: &[JsonValue]) -> Result<(), LixError> {
        for schema in visible_schemas {
            let key = schema_key_from_definition(schema)?;
            self.insert_schema(key, schema.clone());
        }
        Ok(())
    }

    fn remember_pending_registered_schemas(
        &mut self,
        rows: &[StagedStateRow],
    ) -> Result<(), LixError> {
        let mut pending_keys =
            BTreeMap::<SchemaCatalogKey, crate::engine2::entity_identity::EntityIdentity>::new();
        for row in rows {
            if row.schema_key != REGISTERED_SCHEMA_KEY {
                continue;
            }
            let (key, schema) = validate_pending_registered_schema(row)?;
            let catalog_key = SchemaCatalogKey::from_schema_key(key.clone());
            if let Some(existing_entity_id) =
                pending_keys.insert(catalog_key.clone(), row.entity_id.clone())
            {
                return Err(LixError::new(
                    LixError::CODE_SCHEMA_DEFINITION,
                    format!(
                        "duplicate pending registered schema '{}' version '{}' in transaction: rows '{}' and '{}'",
                        catalog_key.schema_key,
                        catalog_key.schema_version,
                        existing_entity_id.as_string()?,
                        row.entity_id.as_string()?
                    ),
                ));
            }
            self.insert_schema(key, schema);
        }
        Ok(())
    }

    fn insert_schema(&mut self, key: SchemaKey, schema: JsonValue) {
        self.schemas_by_key
            .insert(SchemaCatalogKey::from_schema_key(key), schema);
    }

    fn contains(&self, schema_key: &str, schema_version: &str) -> bool {
        self.schemas_by_key.contains_key(&SchemaCatalogKey {
            schema_key: schema_key.to_string(),
            schema_version: schema_version.to_string(),
        })
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.schemas_by_key.len()
    }

    fn schema(&self, schema_key: &str, schema_version: &str) -> Option<&JsonValue> {
        self.schemas_by_key.get(&SchemaCatalogKey {
            schema_key: schema_key.to_string(),
            schema_version: schema_version.to_string(),
        })
    }

    fn schema_by_key(&self, schema_key: &str) -> Option<&JsonValue> {
        self.schemas_by_key
            .iter()
            .find_map(|(key, schema)| (key.schema_key == schema_key).then_some(schema))
    }

    fn schema_key_by_key(&self, schema_key: &str) -> Option<SchemaCatalogKey> {
        self.schemas_by_key
            .keys()
            .find(|key| key.schema_key == schema_key)
            .cloned()
    }

    fn schemas(&self) -> impl Iterator<Item = (&SchemaCatalogKey, &JsonValue)> {
        self.schemas_by_key.iter()
    }

    fn validate_foreign_key_definitions(&self) -> Result<(), LixError> {
        for (key, schema) in &self.schemas_by_key {
            let foreign_keys = foreign_key_definitions(schema)?;
            for foreign_key in foreign_keys {
                validate_foreign_key_definition(self, key, schema, foreign_key)?;
            }
        }
        Ok(())
    }
}

/// Per-transaction compiled schema cache.
///
/// Compilation is lazy and keyed by exact `(schema_key, schema_version)`, so a
/// transaction that writes many rows for one schema pays the JSON Schema
/// compilation cost only once.
struct CompiledSchemaCatalog<'a> {
    schema_catalog: &'a SchemaCatalogSnapshot,
    compiled_by_key: BTreeMap<SchemaCatalogKey, JSONSchema>,
}

impl<'a> CompiledSchemaCatalog<'a> {
    fn new(schema_catalog: &'a SchemaCatalogSnapshot) -> Self {
        Self {
            schema_catalog,
            compiled_by_key: BTreeMap::new(),
        }
    }

    fn compiled_schema(
        &mut self,
        schema_key: &str,
        schema_version: &str,
    ) -> Result<&JSONSchema, LixError> {
        let key = SchemaCatalogKey {
            schema_key: schema_key.to_string(),
            schema_version: schema_version.to_string(),
        };
        if !self.compiled_by_key.contains_key(&key) {
            let schema = self
                .schema_catalog
                .schema(schema_key, schema_version)
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_SCHEMA_DEFINITION,
                        format!(
                            "schema '{schema_key}' version '{schema_version}' is not visible to this transaction"
                        ),
                    )
                })?;
            let compiled = compile_lix_schema(schema)?;
            self.compiled_by_key.insert(key.clone(), compiled);
        }
        self.compiled_by_key.get(&key).ok_or_else(|| {
            LixError::new(
                LixError::CODE_UNKNOWN,
                format!(
                    "compiled schema cache lookup failed for schema '{schema_key}' version '{schema_version}'"
                ),
            )
        })
    }

    #[cfg(test)]
    fn compiled_count(&self) -> usize {
        self.compiled_by_key.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SchemaCatalogKey {
    schema_key: String,
    schema_version: String,
}

impl SchemaCatalogKey {
    fn from_schema_key(key: SchemaKey) -> Self {
        Self {
            schema_key: key.schema_key,
            schema_version: key.schema_version,
        }
    }
}

fn validate_pending_registered_schema(
    row: &StagedStateRow,
) -> Result<(SchemaKey, JsonValue), LixError> {
    let snapshot_content = row.snapshot_content.as_deref().ok_or_else(|| {
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

    let registered_schema_definition = builtin_schema_definition(REGISTERED_SCHEMA_KEY)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                "missing builtin lix_registered_schema definition",
            )
        })?;
    validate_lix_schema(registered_schema_definition, &snapshot)?;

    // A registered-schema row stores the schema definition under `value`.
    // Validate both layers: the outer row must match the builtin
    // `lix_registered_schema` schema, and the inner definition must be a valid
    // Lix schema before it can extend the transaction-visible catalog.
    let (key, schema) = schema_from_registered_snapshot(&snapshot)?;
    validate_lix_schema_definition(&schema)?;
    Ok((key, schema))
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use serde_json::json;

    use super::*;
    use crate::engine2::live_state::{LiveStateRow, LiveStateRowRequest, LiveStateScanRequest};

    struct EmptyLiveStateReader;

    #[async_trait]
    impl LiveStateReader for EmptyLiveStateReader {
        async fn scan_rows(
            &self,
            request: &LiveStateScanRequest,
        ) -> Result<Vec<LiveStateRow>, LixError> {
            Ok(test_file_descriptor_rows()
                .into_iter()
                .filter(|row| live_state_row_matches_scan(row, request))
                .collect())
        }

        async fn load_row(
            &self,
            request: &LiveStateRowRequest,
        ) -> Result<Option<LiveStateRow>, LixError> {
            Ok(test_file_descriptor_rows()
                .into_iter()
                .find(|row| live_state_row_matches_load(row, request)))
        }
    }

    fn validation_input<'a>(
        staged_writes: &'a StagedWriteSet,
        visible_schemas: &'a [JsonValue],
    ) -> TransactionValidationInput<'a> {
        TransactionValidationInput::new(staged_writes, visible_schemas, &EmptyLiveStateReader)
    }

    struct StaticLiveStateReader {
        rows: Vec<LiveStateRow>,
    }

    #[async_trait]
    impl LiveStateReader for StaticLiveStateReader {
        async fn scan_rows(
            &self,
            request: &LiveStateScanRequest,
        ) -> Result<Vec<LiveStateRow>, LixError> {
            Ok(self
                .rows
                .iter()
                .cloned()
                .chain(test_file_descriptor_rows())
                .filter(|row| {
                    request.filter.schema_keys.is_empty()
                        || request.filter.schema_keys.contains(&row.schema_key)
                })
                .filter(|row| {
                    request.filter.version_ids.is_empty()
                        || request.filter.version_ids.contains(&row.version_id)
                })
                .filter(|row| {
                    request.filter.file_ids.is_empty()
                        || request
                            .filter
                            .file_ids
                            .iter()
                            .any(|filter| filter.matches(row.file_id.as_ref()))
                })
                .collect())
        }

        async fn load_row(
            &self,
            request: &LiveStateRowRequest,
        ) -> Result<Option<LiveStateRow>, LixError> {
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

    struct StrictEmptyLiveStateReader;

    #[async_trait]
    impl LiveStateReader for StrictEmptyLiveStateReader {
        async fn scan_rows(
            &self,
            _request: &LiveStateScanRequest,
        ) -> Result<Vec<LiveStateRow>, LixError> {
            Ok(Vec::new())
        }

        async fn load_row(
            &self,
            _request: &LiveStateRowRequest,
        ) -> Result<Option<LiveStateRow>, LixError> {
            Ok(None)
        }
    }

    struct StrictStaticLiveStateReader {
        rows: Vec<LiveStateRow>,
    }

    #[async_trait]
    impl LiveStateReader for StrictStaticLiveStateReader {
        async fn scan_rows(
            &self,
            request: &LiveStateScanRequest,
        ) -> Result<Vec<LiveStateRow>, LixError> {
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
        ) -> Result<Option<LiveStateRow>, LixError> {
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
            "x-lix-version": "1",
            "type": "object",
        })];
        let staged_writes = empty_staged_write_set();
        let input = validation_input(&staged_writes, &visible_schemas);

        let catalog = SchemaCatalogSnapshot::from_transaction_input(&input)
            .expect("schema catalog should build");

        assert_eq!(catalog.len(), 1);
        assert!(catalog.contains("visible_schema", "1"));
    }

    #[test]
    fn schema_catalog_includes_pending_registered_schema_rows() {
        let visible_schemas = vec![json!({
            "x-lix-key": "visible_schema",
            "x-lix-version": "1",
            "type": "object",
        })];
        let staged_writes = StagedWriteSet {
            state_rows: vec![pending_registered_schema_row("pending_schema", "2")],
            ..empty_staged_write_set()
        };
        let input = validation_input(&staged_writes, &visible_schemas);

        let catalog = SchemaCatalogSnapshot::from_transaction_input(&input)
            .expect("schema catalog should build");

        assert_eq!(catalog.len(), 2);
        assert!(catalog.contains("visible_schema", "1"));
        assert!(catalog.contains("pending_schema", "2"));
    }

    #[test]
    fn schema_catalog_pending_schema_overrides_same_visible_identity() {
        let visible_schemas = vec![json!({
            "x-lix-key": "same_schema",
            "x-lix-version": "1",
            "type": "object",
            "properties": {
                "old": { "type": "string" }
            }
        })];
        let staged_writes = StagedWriteSet {
            state_rows: vec![pending_registered_schema_row("same_schema", "1")],
            ..empty_staged_write_set()
        };
        let input = validation_input(&staged_writes, &visible_schemas);

        let catalog = SchemaCatalogSnapshot::from_transaction_input(&input)
            .expect("schema catalog should build");

        assert_eq!(catalog.len(), 1);
        assert!(catalog.contains("same_schema", "1"));
    }

    #[test]
    fn pending_registered_schema_requires_snapshot_content() {
        let mut row = pending_registered_schema_row("missing_snapshot", "1");
        row.snapshot_content = None;

        let error = validate_pending_registered_schema(&row)
            .expect_err("registered schema writes require snapshot_content");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(error.description.contains("snapshot_content"));
    }

    #[test]
    fn pending_registered_schema_rejects_invalid_snapshot_json() {
        let mut row = pending_registered_schema_row("invalid_json", "1");
        row.snapshot_content = Some("{not-json".to_string());

        let error = validate_pending_registered_schema(&row).expect_err("invalid JSON should fail");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(error.description.contains("invalid JSON"));
    }

    #[test]
    fn pending_registered_schema_uses_builtin_schema_for_outer_value_shape() {
        let mut row = pending_registered_schema_row("missing_value", "1");
        row.snapshot_content = Some(json!({}).to_string());

        let error = validate_pending_registered_schema(&row)
            .expect_err("builtin lix_registered_schema validation should fail");

        assert_eq!(error.code, LixError::CODE_SCHEMA_VALIDATION);
        assert!(error.description.contains("value"));
    }

    #[test]
    fn pending_registered_schema_rejects_malformed_nested_lix_schema_definition() {
        let mut row = pending_registered_schema_row("bad_schema_version", "v1");
        row.snapshot_content = Some(
            json!({
                "value": {
                    "x-lix-key": "bad_schema_version",
                    "x-lix-version": "v1",
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" }
                    },
                    "required": ["id"],
                    "additionalProperties": false,
                }
            })
            .to_string(),
        );

        let error = validate_pending_registered_schema(&row)
            .expect_err("nested Lix schema definition should be rejected");

        assert_eq!(error.code, LixError::CODE_SCHEMA_VALIDATION);
        assert!(error.description.contains("x-lix-version"));
    }

    #[test]
    fn schema_catalog_rejects_duplicate_pending_registered_schema_identity() {
        let mut duplicate = pending_registered_schema_row("duplicate_schema", "1");
        duplicate.entity_id = registered_schema_entity_id("duplicate_schema_duplicate", "1");
        let staged_writes = StagedWriteSet {
            state_rows: vec![
                pending_registered_schema_row("duplicate_schema", "1"),
                duplicate,
            ],
            ..empty_staged_write_set()
        };
        let visible_schemas = Vec::new();
        let input = validation_input(&staged_writes, &visible_schemas);

        let error = SchemaCatalogSnapshot::from_transaction_input(&input)
            .expect_err("duplicate pending schema keys should fail");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(error
            .description
            .contains("duplicate pending registered schema"));
    }

    #[test]
    fn schema_catalog_allows_pending_foreign_key_to_pending_schema() {
        let staged_writes = StagedWriteSet {
            state_rows: vec![
                pending_registered_schema_from_definition(fk_parent_schema()),
                pending_registered_schema_from_definition(fk_child_schema()),
            ],
            ..empty_staged_write_set()
        };
        let input = validation_input(&staged_writes, &[]);

        let catalog = SchemaCatalogSnapshot::from_transaction_input(&input)
            .expect("pending parent schema should satisfy pending child foreign key");

        assert!(catalog.contains("fk_parent_schema", "1"));
        assert!(catalog.contains("fk_child_schema", "1"));
    }

    #[test]
    fn schema_catalog_rejects_foreign_key_missing_target_schema() {
        let staged_writes = StagedWriteSet {
            state_rows: vec![pending_registered_schema_from_definition(fk_child_schema())],
            ..empty_staged_write_set()
        };
        let input = validation_input(&staged_writes, &[]);

        let error = SchemaCatalogSnapshot::from_transaction_input(&input)
            .expect_err("missing referenced schema should fail");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(error.description.contains("references missing schema"));
        assert!(error.description.contains("fk_parent_schema"));
    }

    #[test]
    fn schema_catalog_rejects_foreign_key_missing_local_field() {
        let mut child = fk_child_schema();
        child["x-lix-foreign-keys"][0]["properties"] = json!(["/missing_parent_id"]);
        let staged_writes = StagedWriteSet {
            state_rows: vec![
                pending_registered_schema_from_definition(fk_parent_schema()),
                pending_registered_schema_from_definition(child),
            ],
            ..empty_staged_write_set()
        };
        let input = validation_input(&staged_writes, &[]);

        let error = SchemaCatalogSnapshot::from_transaction_input(&input)
            .expect_err("missing local FK field should fail");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(error.description.contains("missing local property"));
        assert!(error.description.contains("/missing_parent_id"));
    }

    #[test]
    fn schema_catalog_rejects_foreign_key_missing_referenced_field() {
        let mut child = fk_child_schema();
        child["x-lix-foreign-keys"][0]["references"]["properties"] = json!(["/missing_id"]);
        let staged_writes = StagedWriteSet {
            state_rows: vec![
                pending_registered_schema_from_definition(fk_parent_schema()),
                pending_registered_schema_from_definition(child),
            ],
            ..empty_staged_write_set()
        };
        let input = validation_input(&staged_writes, &[]);

        let error = SchemaCatalogSnapshot::from_transaction_input(&input)
            .expect_err("missing referenced FK field should fail");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(error.description.contains("missing target property"));
        assert!(error.description.contains("/missing_id"));
    }

    #[test]
    fn schema_catalog_rejects_foreign_key_to_non_unique_target_field() {
        let mut parent = fk_parent_schema();
        parent["properties"]["name"] = json!({ "type": "string" });
        let mut child = fk_child_schema();
        child["x-lix-foreign-keys"][0]["references"]["properties"] = json!(["/name"]);
        let staged_writes = StagedWriteSet {
            state_rows: vec![
                pending_registered_schema_from_definition(parent),
                pending_registered_schema_from_definition(child),
            ],
            ..empty_staged_write_set()
        };
        let input = validation_input(&staged_writes, &[]);

        let error = SchemaCatalogSnapshot::from_transaction_input(&input)
            .expect_err("FK target must be primary-key or unique");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(error
            .description
            .contains("primary key or a unique constraint"));
        assert!(error.description.contains("/name"));
    }

    #[test]
    fn schema_catalog_allows_state_surface_foreign_key_target() {
        let staged_writes = StagedWriteSet {
            state_rows: vec![pending_registered_schema_from_definition(
                state_surface_ref_schema(),
            )],
            ..empty_staged_write_set()
        };
        let input = validation_input(&staged_writes, &[]);

        let catalog = SchemaCatalogSnapshot::from_transaction_input(&input)
            .expect("lix_state should validate as a state-surface FK target");

        assert!(catalog.contains("state_surface_ref_schema", "1"));
    }

    #[test]
    fn schema_catalog_rejects_state_surface_foreign_key_without_schema_key() {
        let mut schema = state_surface_ref_schema();
        schema["x-lix-foreign-keys"][0]["properties"] = json!(["/target_entity_id"]);
        schema["x-lix-foreign-keys"][0]["references"]["properties"] = json!(["/entity_id"]);
        let staged_writes = StagedWriteSet {
            state_rows: vec![pending_registered_schema_from_definition(schema)],
            ..empty_staged_write_set()
        };
        let input = validation_input(&staged_writes, &[]);

        let error = SchemaCatalogSnapshot::from_transaction_input(&input)
            .expect_err("lix_state FK target must include schema identity");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(error.description.contains("/schema_key"));
    }

    #[tokio::test]
    async fn validation_rejects_unknown_schema_key() {
        let visible_schemas = vec![key_value_schema()];
        let staged_writes = StagedWriteSet {
            state_rows: vec![staged_row(
                "unknown_schema",
                "1",
                Some(json!({}).to_string()),
            )],
            ..empty_staged_write_set()
        };

        let error = validate_staged_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect_err("unknown schema_key should fail");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(error.description.contains("unknown_schema"));
    }

    #[tokio::test]
    async fn validation_rejects_unknown_schema_version() {
        let visible_schemas = vec![key_value_schema()];
        let staged_writes = StagedWriteSet {
            state_rows: vec![staged_row(
                "lix_key_value",
                "2",
                Some(json!({ "key": "k", "value": "v" }).to_string()),
            )],
            ..empty_staged_write_set()
        };

        let error = validate_staged_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect_err("unknown schema_version should fail");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(error.description.contains("version '2'"));
    }

    #[tokio::test]
    async fn validation_checks_schema_existence_for_tombstones() {
        let visible_schemas = vec![key_value_schema()];
        let staged_writes = StagedWriteSet {
            state_rows: vec![staged_row("unknown_schema", "1", None)],
            ..empty_staged_write_set()
        };

        let error = validate_staged_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect_err("tombstone with unknown schema should fail");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(error.description.contains("unknown_schema"));
    }

    #[tokio::test]
    async fn validation_allows_pending_registered_schema_to_validate_later_rows() {
        let visible_schemas = vec![key_value_schema(), registered_schema()];
        let staged_writes = StagedWriteSet {
            state_rows: vec![
                pending_registered_schema_row("pending_schema", "1"),
                staged_row(
                    "pending_schema",
                    "1",
                    Some(json!({ "id": "entity-1" }).to_string()),
                ),
            ],
            ..empty_staged_write_set()
        };

        validate_staged_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect("pending registered schema should be visible to later staged rows");
    }

    #[tokio::test]
    async fn validation_validates_snapshot_content_against_schema() {
        let visible_schemas = vec![key_value_schema()];
        let staged_writes = StagedWriteSet {
            state_rows: vec![staged_row(
                "lix_key_value",
                "1",
                Some(json!({ "key": "k" }).to_string()),
            )],
            ..empty_staged_write_set()
        };

        let error = validate_staged_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect_err("missing required snapshot field should fail");

        assert_eq!(error.code, LixError::CODE_SCHEMA_VALIDATION);
        assert!(error.description.contains("value"));
    }

    #[tokio::test]
    async fn validation_rejects_invalid_snapshot_json() {
        let visible_schemas = vec![key_value_schema()];
        let staged_writes = StagedWriteSet {
            state_rows: vec![staged_row(
                "lix_key_value",
                "1",
                Some("{not-json".to_string()),
            )],
            ..empty_staged_write_set()
        };

        let error = validate_staged_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect_err("invalid snapshot JSON should fail");

        assert_eq!(error.code, LixError::CODE_SCHEMA_VALIDATION);
        assert!(error.description.contains("invalid JSON"));
    }

    #[tokio::test]
    async fn validation_skips_snapshot_validation_for_tombstones() {
        let visible_schemas = vec![key_value_schema()];
        let staged_writes = StagedWriteSet {
            state_rows: vec![staged_row("lix_key_value", "1", None)],
            ..empty_staged_write_set()
        };

        validate_staged_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect("tombstone should only require schema existence");
    }

    #[tokio::test]
    async fn validation_rejects_missing_file_owner_reference() {
        let visible_schemas = vec![unique_schema()];
        let staged_writes = StagedWriteSet {
            state_rows: vec![unique_row("post-1", "hello-world", "first")],
            ..empty_staged_write_set()
        };

        let error = validate_staged_writes(TransactionValidationInput::new(
            &staged_writes,
            &visible_schemas,
            &StrictEmptyLiveStateReader,
        ))
        .await
        .expect_err("non-null file_id should require a file descriptor");

        assert_eq!(error.code, LixError::CODE_UNKNOWN);
        assert!(error
            .description
            .contains("file ownership validation failed"));
        assert!(error.description.contains("file-a"));
    }

    #[tokio::test]
    async fn validation_allows_pending_file_owner_reference() {
        let visible_schemas = vec![unique_schema(), file_descriptor_schema()];
        let staged_writes = StagedWriteSet {
            state_rows: vec![
                staged_file_descriptor_row("file-a", "version-a"),
                unique_row("post-1", "hello-world", "first"),
            ],
            ..empty_staged_write_set()
        };

        validate_staged_writes(TransactionValidationInput::new(
            &staged_writes,
            &visible_schemas,
            &StrictEmptyLiveStateReader,
        ))
        .await
        .expect("same-transaction file descriptor should satisfy file ownership");
    }

    #[tokio::test]
    async fn validation_allows_committed_file_owner_reference() {
        let visible_schemas = vec![unique_schema()];
        let staged_writes = StagedWriteSet {
            state_rows: vec![unique_row("post-1", "hello-world", "first")],
            ..empty_staged_write_set()
        };
        let live_state = StaticLiveStateReader {
            rows: vec![committed_file_descriptor_row("file-a", "version-a")],
        };

        validate_staged_writes(TransactionValidationInput::new(
            &staged_writes,
            &visible_schemas,
            &live_state,
        ))
        .await
        .expect("committed file descriptor should satisfy file ownership");
    }

    #[tokio::test]
    async fn validation_rejects_file_owner_reference_that_exists_only_in_global() {
        let visible_schemas = vec![unique_schema()];
        let staged_writes = StagedWriteSet {
            state_rows: vec![unique_row("post-1", "hello-world", "first")],
            ..empty_staged_write_set()
        };
        let live_state = StrictStaticLiveStateReader {
            rows: vec![committed_file_descriptor_row(
                "file-a",
                crate::version::GLOBAL_VERSION_ID,
            )],
        };

        let error = validate_staged_writes(TransactionValidationInput::new(
            &staged_writes,
            &visible_schemas,
            &live_state,
        ))
        .await
        .expect_err("global file descriptor should not satisfy a version-local row");

        assert_eq!(error.code, LixError::CODE_UNKNOWN);
        assert!(error
            .description
            .contains("file ownership validation failed"));
    }

    #[tokio::test]
    async fn validation_rejects_primary_key_duplicate_with_different_identity() {
        let visible_schemas = vec![unique_schema()];
        let mut conflicting = unique_row("post-1", "hello-world", "first");
        conflicting.entity_id = crate::engine2::entity_identity::EntityIdentity::single("post-2");
        let staged_writes = StagedWriteSet {
            state_rows: vec![unique_row("post-1", "hello-world", "first"), conflicting],
            ..empty_staged_write_set()
        };

        let error = validate_staged_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect_err("same primary key under different identity should fail");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
        assert!(
            error
                .description
                .contains("primary-key constraint violation"),
            "error should explain primary-key conflict: {error:?}"
        );
    }

    #[tokio::test]
    async fn validation_rejects_pending_unique_value_duplicate() {
        let visible_schemas = vec![unique_schema()];
        let staged_writes = StagedWriteSet {
            state_rows: vec![
                unique_row("post-1", "hello-world", "first"),
                unique_row("post-2", "hello-world", "second"),
            ],
            ..empty_staged_write_set()
        };

        let error = validate_staged_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect_err("duplicate pending unique value should fail");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
        assert!(
            error
                .description
                .contains("unique constraint violation on unique_schema./slug"),
            "error should name the unique constraint: {error:?}"
        );
        assert!(
            error.description.contains("\"hello-world\""),
            "error should include the conflicting value: {error:?}"
        );
    }

    #[tokio::test]
    async fn validation_rejects_pending_unique_same_value_in_same_version() {
        let visible_schemas = vec![unique_schema()];
        let mut duplicate = unique_row("post-2", "hello-world", "second");
        duplicate.version_id = "version-a".to_string();
        let staged_writes = StagedWriteSet {
            state_rows: vec![unique_row("post-1", "hello-world", "first"), duplicate],
            ..empty_staged_write_set()
        };

        let error = validate_staged_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect_err("same unique value in the same version should fail");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
    }

    #[tokio::test]
    async fn validation_allows_pending_unique_same_value_in_different_versions() {
        let visible_schemas = vec![unique_schema()];
        let mut version_b = unique_row("post-2", "hello-world", "second");
        version_b.version_id = "version-b".to_string();
        let staged_writes = StagedWriteSet {
            state_rows: vec![unique_row("post-1", "hello-world", "first"), version_b],
            ..empty_staged_write_set()
        };

        validate_staged_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect("unique values should be scoped to the exact version_id");
    }

    #[tokio::test]
    async fn validation_allows_pending_unique_overwrite_of_same_identity() {
        let visible_schemas = vec![unique_schema()];
        let staged_writes = StagedWriteSet {
            state_rows: vec![
                unique_row("post-1", "hello-world", "first"),
                unique_row("post-1", "hello-world", "updated"),
            ],
            ..empty_staged_write_set()
        };

        validate_staged_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect("same identity should be treated as replacement, not duplicate");
    }

    #[tokio::test]
    async fn validation_skips_pending_unique_indexes_for_tombstones() {
        let visible_schemas = vec![unique_schema()];
        let mut tombstone = unique_row("post-1", "hello-world", "deleted");
        tombstone.snapshot_content = None;
        let staged_writes = StagedWriteSet {
            state_rows: vec![tombstone, unique_row("post-2", "hello-world", "second")],
            ..empty_staged_write_set()
        };

        validate_staged_writes(validation_input(&staged_writes, &visible_schemas))
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
        let staged_writes = StagedWriteSet {
            state_rows: vec![
                unique_row("post-1", "hello-world", "first"),
                different_file,
                different_version,
            ],
            ..empty_staged_write_set()
        };

        validate_staged_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect("unique values are scoped by file and version");
    }

    #[tokio::test]
    async fn validation_rejects_committed_visible_unique_value_duplicate() {
        let visible_schemas = vec![unique_schema()];
        let staged_writes = StagedWriteSet {
            state_rows: vec![unique_row("post-2", "hello-world", "second")],
            ..empty_staged_write_set()
        };
        let live_state = StaticLiveStateReader {
            rows: vec![committed_unique_row("post-1", "hello-world", "first")],
        };

        let error = validate_staged_writes(TransactionValidationInput::new(
            &staged_writes,
            &visible_schemas,
            &live_state,
        ))
        .await
        .expect_err("committed visible unique value should conflict");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
    }

    #[tokio::test]
    async fn validation_rejects_committed_unique_same_value_in_same_version() {
        let visible_schemas = vec![unique_schema()];
        let staged_writes = StagedWriteSet {
            state_rows: vec![unique_row("post-2", "hello-world", "second")],
            ..empty_staged_write_set()
        };
        let live_state = StaticLiveStateReader {
            rows: vec![committed_unique_row("post-1", "hello-world", "first")],
        };

        let error = validate_staged_writes(TransactionValidationInput::new(
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
        let staged_writes = StagedWriteSet {
            state_rows: vec![version_b],
            ..empty_staged_write_set()
        };
        let live_state = StaticLiveStateReader {
            rows: vec![committed_unique_row("post-1", "hello-world", "first")],
        };

        validate_staged_writes(TransactionValidationInput::new(
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
        let staged_writes = StagedWriteSet {
            state_rows: vec![unique_row("post-2", "hello-world", "second")],
            ..empty_staged_write_set()
        };
        let mut projected_overlay_row = committed_unique_row("post-1", "hello-world", "first");
        projected_overlay_row.version_id = "version-a".to_string();
        projected_overlay_row.global = true;
        let live_state = StaticLiveStateReader {
            rows: vec![projected_overlay_row],
        };

        validate_staged_writes(TransactionValidationInput::new(
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
        let staged_writes = StagedWriteSet {
            state_rows: vec![unique_row("post-1", "hello-world", "updated")],
            ..empty_staged_write_set()
        };
        let live_state = StaticLiveStateReader {
            rows: vec![committed_unique_row("post-1", "hello-world", "first")],
        };

        validate_staged_writes(TransactionValidationInput::new(
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
        tombstone.snapshot_content = None;
        let staged_writes = StagedWriteSet {
            state_rows: vec![tombstone, unique_row("post-2", "hello-world", "second")],
            ..empty_staged_write_set()
        };
        let live_state = StaticLiveStateReader {
            rows: vec![committed_unique_row("post-1", "hello-world", "first")],
        };

        validate_staged_writes(TransactionValidationInput::new(
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
        let staged_writes = StagedWriteSet {
            state_rows: vec![different_file, different_version],
            ..empty_staged_write_set()
        };
        let live_state = StaticLiveStateReader {
            rows: vec![committed_unique_row("post-1", "hello-world", "first")],
        };

        validate_staged_writes(TransactionValidationInput::new(
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
        let staged_writes = StagedWriteSet {
            state_rows: vec![fk_child_row("child-1", "parent-1", "version-a")],
            ..empty_staged_write_set()
        };

        let error = validate_staged_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect_err("foreign key must resolve in the same version");

        assert_eq!(error.code, LixError::CODE_FOREIGN_KEY);
        assert!(
            error.description.contains("foreign key on schema"),
            "error should explain unresolved FK: {error:?}"
        );
    }

    #[tokio::test]
    async fn validation_allows_foreign_key_target_in_same_version() {
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let staged_writes = StagedWriteSet {
            state_rows: vec![
                fk_parent_row("parent-1", "version-a"),
                fk_child_row("child-1", "parent-1", "version-a"),
            ],
            ..empty_staged_write_set()
        };

        validate_staged_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect("foreign key should resolve against pending rows in the same version");
    }

    #[tokio::test]
    async fn validation_rejects_foreign_key_target_that_exists_only_in_different_version() {
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let staged_writes = StagedWriteSet {
            state_rows: vec![
                fk_parent_row("parent-1", "version-b"),
                fk_child_row("child-1", "parent-1", "version-a"),
            ],
            ..empty_staged_write_set()
        };

        let error = validate_staged_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect_err("foreign key target in another version should not satisfy this version");

        assert_eq!(error.code, LixError::CODE_FOREIGN_KEY);
    }

    #[tokio::test]
    async fn validation_allows_foreign_key_target_committed_in_same_version() {
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let staged_writes = StagedWriteSet {
            state_rows: vec![fk_child_row("child-1", "parent-1", "version-a")],
            ..empty_staged_write_set()
        };
        let live_state = StaticLiveStateReader {
            rows: vec![LiveStateRow::from(fk_parent_row("parent-1", "version-a"))],
        };

        validate_staged_writes(TransactionValidationInput::new(
            &staged_writes,
            &visible_schemas,
            &live_state,
        ))
        .await
        .expect("foreign key should resolve against committed rows in the same version");
    }

    #[tokio::test]
    async fn validation_rejects_foreign_key_target_committed_only_in_different_version() {
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let staged_writes = StagedWriteSet {
            state_rows: vec![fk_child_row("child-1", "parent-1", "version-a")],
            ..empty_staged_write_set()
        };
        let live_state = StaticLiveStateReader {
            rows: vec![LiveStateRow::from(fk_parent_row("parent-1", "version-b"))],
        };

        let error = validate_staged_writes(TransactionValidationInput::new(
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
        parent_delete.snapshot_content = None;
        let staged_writes = StagedWriteSet {
            state_rows: vec![
                parent_delete,
                fk_child_row("child-1", "parent-1", "version-a"),
            ],
            ..empty_staged_write_set()
        };
        let live_state = StaticLiveStateReader {
            rows: vec![LiveStateRow::from(fk_parent_row("parent-1", "version-a"))],
        };

        let error = validate_staged_writes(TransactionValidationInput::new(
            &staged_writes,
            &visible_schemas,
            &live_state,
        ))
        .await
        .expect_err("same-transaction tombstone should hide the committed FK target");

        assert_eq!(error.code, LixError::CODE_FOREIGN_KEY);
    }

    #[tokio::test]
    async fn validation_rejects_pending_reference_to_deleted_identity() {
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let mut parent_delete = fk_parent_row("parent-1", "version-a");
        parent_delete.snapshot_content = None;
        let staged_writes = StagedWriteSet {
            state_rows: vec![
                parent_delete,
                fk_child_row("child-1", "parent-1", "version-a"),
            ],
            ..empty_staged_write_set()
        };
        let live_state = StaticLiveStateReader {
            rows: vec![LiveStateRow::from(fk_parent_row("parent-1", "version-a"))],
        };

        let error = validate_staged_writes(TransactionValidationInput::new(
            &staged_writes,
            &visible_schemas,
            &live_state,
        ))
        .await
        .expect_err("pending child reference should block parent delete");

        assert_eq!(error.code, LixError::CODE_FOREIGN_KEY);
        assert!(
            error.description.contains("cannot delete"),
            "error should explain delete restriction: {error:?}"
        );
    }

    #[tokio::test]
    async fn validation_allows_delete_with_pending_reference_in_different_version() {
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let mut parent_delete = fk_parent_row("parent-1", "version-a");
        parent_delete.snapshot_content = None;
        let staged_writes = StagedWriteSet {
            state_rows: vec![
                parent_delete,
                fk_parent_row("parent-1", "version-b"),
                fk_child_row("child-1", "parent-1", "version-b"),
            ],
            ..empty_staged_write_set()
        };

        validate_staged_writes(validation_input(&staged_writes, &visible_schemas))
            .await
            .expect("pending references in another version should not block this delete");
    }

    #[tokio::test]
    async fn validation_allows_state_surface_fk_target_committed_by_exact_identity() {
        let visible_schemas = vec![fk_parent_schema(), state_surface_ref_schema()];
        let staged_writes = StagedWriteSet {
            state_rows: vec![state_surface_ref_row(
                "ref-1",
                "target-1",
                "fk_parent_schema",
                "file-a",
            )],
            ..empty_staged_write_set()
        };
        let live_state = StaticLiveStateReader {
            rows: vec![LiveStateRow::from(fk_parent_row("target-1", "version-a"))],
        };

        validate_staged_writes(TransactionValidationInput::new(
            &staged_writes,
            &visible_schemas,
            &live_state,
        ))
        .await
        .expect("lix_state FK should resolve against exact committed identity");
    }

    #[tokio::test]
    async fn validation_rejects_delete_when_same_version_reference_exists() {
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let mut parent_delete = fk_parent_row("parent-1", "version-a");
        parent_delete.snapshot_content = None;
        let live_state = StaticLiveStateReader {
            rows: vec![
                LiveStateRow::from(fk_parent_row("parent-1", "version-a")),
                LiveStateRow::from(fk_child_row("child-1", "parent-1", "version-a")),
            ],
        };
        let staged_writes = StagedWriteSet {
            state_rows: vec![parent_delete],
            ..empty_staged_write_set()
        };

        let error = validate_staged_writes(TransactionValidationInput::new(
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
        parent_delete.snapshot_content = None;
        let live_state = StaticLiveStateReader {
            rows: vec![
                LiveStateRow::from(fk_parent_row("parent-1", "version-a")),
                LiveStateRow::from(fk_child_row("child-1", "parent-1", "version-b")),
            ],
        };
        let staged_writes = StagedWriteSet {
            state_rows: vec![parent_delete],
            ..empty_staged_write_set()
        };

        validate_staged_writes(TransactionValidationInput::new(
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
        parent_delete.snapshot_content = None;
        let mut child_delete = fk_child_row("child-1", "parent-1", "version-a");
        child_delete.snapshot_content = None;
        let live_state = StaticLiveStateReader {
            rows: vec![
                LiveStateRow::from(fk_parent_row("parent-1", "version-a")),
                LiveStateRow::from(fk_child_row("child-1", "parent-1", "version-a")),
            ],
        };
        let staged_writes = StagedWriteSet {
            state_rows: vec![parent_delete, child_delete],
            ..empty_staged_write_set()
        };

        validate_staged_writes(TransactionValidationInput::new(
            &staged_writes,
            &visible_schemas,
            &live_state,
        ))
        .await
        .expect("committed references deleted in the same transaction should not restrict delete");
    }

    #[test]
    fn compiled_schema_catalog_compiles_each_schema_once() {
        let visible_schemas = vec![key_value_schema()];
        let staged_writes = empty_staged_write_set();
        let input = validation_input(&staged_writes, &visible_schemas);
        let catalog = SchemaCatalogSnapshot::from_transaction_input(&input)
            .expect("schema catalog should build");
        let mut compiled = CompiledSchemaCatalog::new(&catalog);

        compiled
            .compiled_schema("lix_key_value", "1")
            .expect("schema should compile");
        compiled
            .compiled_schema("lix_key_value", "1")
            .expect("schema should be cached");

        assert_eq!(compiled.compiled_count(), 1);
    }

    #[test]
    fn pending_indexes_record_primary_key_fk_targets_by_exact_scope() {
        let mut indexes = PendingConstraintIndexes::default();
        let row = fk_parent_row("parent-1", "version-a");
        let snapshot = serde_json::from_str::<JsonValue>(
            row.snapshot_content
                .as_deref()
                .expect("fixture should have snapshot"),
        )
        .expect("fixture JSON should parse");

        indexes
            .remember_row(&row, &fk_parent_schema(), &snapshot)
            .expect("parent row should index");

        assert!(indexes
            .has_fk_target(
                "fk_parent_schema",
                "1",
                "version-a",
                Some("file-a"),
                &["/id"],
                UniqueConstraintValue::string_values(["parent-1"]),
            )
            .expect("lookup should build"));
        assert!(!indexes
            .has_fk_target(
                "fk_parent_schema",
                "1",
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
            row.snapshot_content
                .as_deref()
                .expect("fixture should have snapshot"),
        )
        .expect("fixture JSON should parse");

        indexes
            .remember_row(&row, &unique_schema(), &snapshot)
            .expect("unique row should index");

        assert!(indexes
            .has_fk_target(
                "unique_schema",
                "1",
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
            row.snapshot_content
                .as_deref()
                .expect("fixture should have snapshot"),
        )
        .expect("fixture JSON should parse");
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let staged_writes = empty_staged_write_set();
        let input = validation_input(&staged_writes, &visible_schemas);
        let catalog =
            SchemaCatalogSnapshot::from_transaction_input(&input).expect("catalog should build");

        indexes
            .remember_foreign_key_references(&catalog, &row, &fk_child_schema(), &snapshot)
            .expect("child row should index FK reference");

        assert!(indexes
            .has_fk_reference_to_key(
                "fk_parent_schema",
                "1",
                "version-a",
                Some("file-a"),
                &["/id"],
                UniqueConstraintValue::string_values(["parent-1"]),
            )
            .expect("lookup should build"));
        assert!(!indexes
            .has_fk_reference_to_key(
                "fk_parent_schema",
                "1",
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
            row.snapshot_content
                .as_deref()
                .expect("fixture should have snapshot"),
        )
        .expect("fixture JSON should parse");
        let visible_schemas = vec![state_surface_ref_schema()];
        let staged_writes = empty_staged_write_set();
        let input = validation_input(&staged_writes, &visible_schemas);
        let catalog =
            SchemaCatalogSnapshot::from_transaction_input(&input).expect("catalog should build");

        indexes
            .remember_foreign_key_references(&catalog, &row, &state_surface_ref_schema(), &snapshot)
            .expect("state-surface row should index FK reference");

        assert!(indexes.has_fk_reference_to_identity(LiveStateRowIdentity {
            version_id: "version-a".to_string(),
            schema_key: "fk_parent_schema".to_string(),
            entity_id: EntityIdentity::single("target-1"),
            file_id: Some("file-a".to_string()),
        }));
    }

    #[test]
    fn pending_delete_restrictions_ignore_tombstoned_referencing_rows() {
        let mut indexes = PendingConstraintIndexes::default();
        let mut parent_delete = fk_parent_row("parent-1", "version-a");
        parent_delete.snapshot_content = None;
        indexes.remember_tombstone(&parent_delete);

        let child = fk_child_row("child-1", "parent-1", "version-a");
        let child_snapshot = serde_json::from_str::<JsonValue>(
            child
                .snapshot_content
                .as_deref()
                .expect("fixture should have snapshot"),
        )
        .expect("fixture JSON should parse");
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let staged_writes = empty_staged_write_set();
        let input = validation_input(&staged_writes, &visible_schemas);
        let catalog =
            SchemaCatalogSnapshot::from_transaction_input(&input).expect("catalog should build");
        indexes
            .remember_foreign_key_references(&catalog, &child, &fk_child_schema(), &child_snapshot)
            .expect("child row should index FK reference");

        let mut child_delete = fk_child_row("child-1", "parent-1", "version-a");
        child_delete.snapshot_content = None;
        indexes.remember_tombstone(&child_delete);

        validate_pending_delete_restrictions(&catalog, &indexes)
            .expect("a row deleted in the same transaction should not block target delete");
    }

    #[test]
    fn pending_fk_validation_collects_unresolved_normal_fk_check() {
        let indexes = PendingConstraintIndexes::default();
        let row = fk_child_row("child-1", "parent-1", "version-a");
        let snapshot = serde_json::from_str::<JsonValue>(
            row.snapshot_content
                .as_deref()
                .expect("fixture should have snapshot"),
        )
        .expect("fixture JSON should parse");
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let staged_writes = empty_staged_write_set();
        let input = validation_input(&staged_writes, &visible_schemas);
        let catalog =
            SchemaCatalogSnapshot::from_transaction_input(&input).expect("catalog should build");

        let unresolved = validate_pending_foreign_keys(
            &catalog,
            &indexes,
            &[(&row, &fk_child_schema(), snapshot)],
        )
        .expect("FK validation should collect unresolved checks");

        assert_eq!(unresolved.len(), 1);
        assert_eq!(
            unresolved[0].source_identity,
            LiveStateRowIdentity {
                version_id: "version-a".to_string(),
                schema_key: "fk_child_schema".to_string(),
                entity_id: EntityIdentity::single("child-1"),
                file_id: Some("file-a".to_string()),
            }
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
        assert_eq!(target.schema_version, "1");
        assert_eq!(target.version_id, "version-a");
        assert_eq!(target.file_id.as_deref(), Some("file-a"));
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
                .snapshot_content
                .as_deref()
                .expect("fixture should have snapshot"),
        )
        .expect("fixture JSON should parse");
        indexes
            .remember_row(&parent, &fk_parent_schema(), &parent_snapshot)
            .expect("parent should index as pending FK target");

        let child = fk_child_row("child-1", "parent-1", "version-a");
        let child_snapshot = serde_json::from_str::<JsonValue>(
            child
                .snapshot_content
                .as_deref()
                .expect("fixture should have snapshot"),
        )
        .expect("fixture JSON should parse");
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let staged_writes = empty_staged_write_set();
        let input = validation_input(&staged_writes, &visible_schemas);
        let catalog =
            SchemaCatalogSnapshot::from_transaction_input(&input).expect("catalog should build");

        let unresolved = validate_pending_foreign_keys(
            &catalog,
            &indexes,
            &[(&child, &fk_child_schema(), child_snapshot)],
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
                .snapshot_content
                .as_deref()
                .expect("fixture should have snapshot"),
        )
        .expect("fixture JSON should parse");
        indexes
            .remember_row(&parent, &fk_parent_schema(), &parent_snapshot)
            .expect("parent should index as pending FK target");

        let child = fk_child_row("child-1", "parent-1", "version-a");
        let child_snapshot = serde_json::from_str::<JsonValue>(
            child
                .snapshot_content
                .as_deref()
                .expect("fixture should have snapshot"),
        )
        .expect("fixture JSON should parse");
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let staged_writes = empty_staged_write_set();
        let input = validation_input(&staged_writes, &visible_schemas);
        let catalog =
            SchemaCatalogSnapshot::from_transaction_input(&input).expect("catalog should build");

        let unresolved = validate_pending_foreign_keys(
            &catalog,
            &indexes,
            &[(&child, &fk_child_schema(), child_snapshot)],
        )
        .expect("FK validation should inspect pending targets");

        assert_eq!(unresolved.len(), 1);
        let UnresolvedForeignKeyTarget::Key(target) = &unresolved[0].target else {
            panic!("normal FK should produce key target");
        };
        assert_eq!(
            target.version_id, "version-a",
            "FK checks are exact-version scoped, not overlay scoped"
        );
    }

    #[test]
    fn pending_fk_validation_collects_unresolved_state_surface_check() {
        let indexes = PendingConstraintIndexes::default();
        let row = state_surface_ref_row("ref-1", "target-1", "fk_parent_schema", "file-a");
        let snapshot = serde_json::from_str::<JsonValue>(
            row.snapshot_content
                .as_deref()
                .expect("fixture should have snapshot"),
        )
        .expect("fixture JSON should parse");
        let visible_schemas = vec![state_surface_ref_schema()];
        let staged_writes = empty_staged_write_set();
        let input = validation_input(&staged_writes, &visible_schemas);
        let catalog =
            SchemaCatalogSnapshot::from_transaction_input(&input).expect("catalog should build");

        let unresolved = validate_pending_foreign_keys(
            &catalog,
            &indexes,
            &[(&row, &state_surface_ref_schema(), snapshot)],
        )
        .expect("FK validation should collect unresolved checks");

        assert_eq!(unresolved.len(), 1);
        assert_eq!(
            unresolved[0].source_identity,
            LiveStateRowIdentity {
                version_id: "version-a".to_string(),
                schema_key: "state_surface_ref_schema".to_string(),
                entity_id: EntityIdentity::single("ref-1"),
                file_id: Some("file-a".to_string()),
            }
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
            panic!("lix_state FK should produce state-surface identity target");
        };
        assert_eq!(target.version_id, "version-a");
        assert_eq!(target.schema_key, "fk_parent_schema");
        assert_eq!(target.entity_id, EntityIdentity::single("target-1"));
        assert_eq!(target.file_id.as_deref(), Some("file-a"));
    }

    #[tokio::test]
    async fn committed_fk_lookup_resolves_normal_fk_in_exact_scope() {
        let indexes = PendingConstraintIndexes::default();
        let child = fk_child_row("child-1", "parent-1", "version-a");
        let child_snapshot = serde_json::from_str::<JsonValue>(
            child
                .snapshot_content
                .as_deref()
                .expect("fixture should have snapshot"),
        )
        .expect("fixture JSON should parse");
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let staged_writes = empty_staged_write_set();
        let input = validation_input(&staged_writes, &visible_schemas);
        let catalog =
            SchemaCatalogSnapshot::from_transaction_input(&input).expect("catalog should build");
        let unresolved = validate_pending_foreign_keys(
            &catalog,
            &indexes,
            &[(&child, &fk_child_schema(), child_snapshot)],
        )
        .expect("pending FK validation should collect unresolved check");
        let live_state = StaticLiveStateReader {
            rows: vec![LiveStateRow::from(fk_parent_row("parent-1", "version-a"))],
        };

        let still_unresolved = validate_committed_foreign_keys(
            &TransactionValidationInput::new(&staged_writes, &visible_schemas, &live_state),
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
                .snapshot_content
                .as_deref()
                .expect("fixture should have snapshot"),
        )
        .expect("fixture JSON should parse");
        let visible_schemas = vec![fk_parent_schema(), fk_child_schema()];
        let staged_writes = empty_staged_write_set();
        let input = validation_input(&staged_writes, &visible_schemas);
        let catalog =
            SchemaCatalogSnapshot::from_transaction_input(&input).expect("catalog should build");
        let unresolved = validate_pending_foreign_keys(
            &catalog,
            &indexes,
            &[(&child, &fk_child_schema(), child_snapshot)],
        )
        .expect("pending FK validation should collect unresolved check");
        let live_state = StaticLiveStateReader {
            rows: vec![LiveStateRow::from(fk_parent_row("parent-1", "version-b"))],
        };

        let still_unresolved = validate_committed_foreign_keys(
            &TransactionValidationInput::new(&staged_writes, &visible_schemas, &live_state),
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
            row.snapshot_content
                .as_deref()
                .expect("fixture should have snapshot"),
        )
        .expect("fixture JSON should parse");
        let visible_schemas = vec![state_surface_ref_schema()];
        let staged_writes = empty_staged_write_set();
        let input = validation_input(&staged_writes, &visible_schemas);
        let catalog =
            SchemaCatalogSnapshot::from_transaction_input(&input).expect("catalog should build");
        let unresolved = validate_pending_foreign_keys(
            &catalog,
            &indexes,
            &[(&row, &state_surface_ref_schema(), snapshot)],
        )
        .expect("pending FK validation should collect unresolved check");
        let live_state = StaticLiveStateReader {
            rows: vec![LiveStateRow::from(fk_parent_row("target-1", "version-a"))],
        };

        let still_unresolved = validate_committed_foreign_keys(
            &TransactionValidationInput::new(&staged_writes, &visible_schemas, &live_state),
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

    fn empty_staged_write_set() -> StagedWriteSet {
        StagedWriteSet {
            state_rows: Vec::new(),
            commit_members_by_version: BTreeMap::new(),
            extra_commit_parents_by_version: BTreeMap::new(),
            file_data_writes: Vec::new(),
        }
    }

    fn live_state_row_matches_scan(row: &LiveStateRow, request: &LiveStateScanRequest) -> bool {
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

    fn live_state_row_matches_load(row: &LiveStateRow, request: &LiveStateRowRequest) -> bool {
        row.schema_key == request.schema_key
            && row.version_id == request.version_id
            && row.entity_id == request.entity_id
            && request.file_id.matches(row.file_id.as_ref())
    }

    fn test_file_descriptor_rows() -> Vec<LiveStateRow> {
        vec![
            committed_file_descriptor_row("file-a", "version-a"),
            committed_file_descriptor_row("file-a", "version-b"),
            committed_file_descriptor_row("file-b", "version-a"),
            committed_file_descriptor_row("file-b", "version-b"),
        ]
    }

    fn pending_registered_schema_row(schema_key: &str, schema_version: &str) -> StagedStateRow {
        pending_registered_schema_from_definition(json!({
            "x-lix-key": schema_key,
            "x-lix-version": schema_version,
            "type": "object",
            "properties": {
                "id": { "type": "string" }
            },
            "required": ["id"],
            "additionalProperties": false,
        }))
    }

    fn pending_registered_schema_from_definition(schema: JsonValue) -> StagedStateRow {
        let key = schema_key_from_definition(&schema).expect("test schema should have a key");
        StagedStateRow {
            entity_id: registered_schema_entity_id(&key.schema_key, &key.schema_version),
            schema_key: REGISTERED_SCHEMA_KEY.to_string(),
            file_id: None,
            snapshot_content: Some(json!({ "value": schema }).to_string()),
            metadata: None,
            schema_version: "1".to_string(),
            created_at: "2026-04-29T00:00:00.000Z".to_string(),
            updated_at: "2026-04-29T00:00:00.000Z".to_string(),
            global: true,
            change_id: Some("change-registered-schema".to_string()),
            commit_id: Some("commit-registered-schema".to_string()),
            untracked: false,
            version_id: crate::version::GLOBAL_VERSION_ID.to_string(),
        }
    }

    fn registered_schema_entity_id(
        schema_key: &str,
        schema_version: &str,
    ) -> crate::engine2::entity_identity::EntityIdentity {
        crate::engine2::entity_identity::EntityIdentity::from_primary_key_paths(
            &serde_json::json!({
                "value": {
                    "x-lix-key": schema_key,
                    "x-lix-version": schema_version,
                }
            }),
            &[
                vec!["value".to_string(), "x-lix-key".to_string()],
                vec!["value".to_string(), "x-lix-version".to_string()],
            ],
        )
        .expect("registered schema identity should derive")
    }

    fn key_value_schema() -> JsonValue {
        builtin_schema_definition("lix_key_value")
            .expect("lix_key_value builtin schema should exist")
            .clone()
    }

    fn registered_schema() -> JsonValue {
        builtin_schema_definition(REGISTERED_SCHEMA_KEY)
            .expect("lix_registered_schema builtin schema should exist")
            .clone()
    }

    fn file_descriptor_schema() -> JsonValue {
        builtin_schema_definition(FILE_DESCRIPTOR_SCHEMA_KEY)
            .expect("lix_file_descriptor builtin schema should exist")
            .clone()
    }

    fn unique_schema() -> JsonValue {
        json!({
            "x-lix-key": "unique_schema",
            "x-lix-version": "1",
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

    fn fk_parent_schema() -> JsonValue {
        json!({
            "x-lix-key": "fk_parent_schema",
            "x-lix-version": "1",
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
            "x-lix-version": "1",
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
            "x-lix-version": "1",
            "x-lix-primary-key": ["/id"],
            "x-lix-foreign-keys": [{
                "properties": ["/target_entity_id", "/target_schema_key", "/target_file_id"],
                "references": {
                    "schemaKey": "lix_state",
                    "properties": ["/entity_id", "/schema_key", "/file_id"]
                }
            }],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "target_entity_id": { "type": "string" },
                "target_schema_key": { "type": "string" },
                "target_file_id": { "type": ["string", "null"] }
            },
            "required": ["id", "target_entity_id", "target_schema_key", "target_file_id"],
            "additionalProperties": false
        })
    }

    fn unique_row(entity_id: &str, slug: &str, title: &str) -> StagedStateRow {
        let mut row = staged_row(
            "unique_schema",
            "1",
            Some(
                json!({
                    "id": entity_id,
                    "slug": slug,
                    "title": title,
                })
                .to_string(),
            ),
        );
        row.entity_id = crate::engine2::entity_identity::EntityIdentity::single(entity_id);
        row.file_id = Some("file-a".to_string());
        row.version_id = "version-a".to_string();
        row.global = false;
        row
    }

    fn fk_parent_row(entity_id: &str, version_id: &str) -> StagedStateRow {
        let mut row = staged_row(
            "fk_parent_schema",
            "1",
            Some(json!({ "id": entity_id }).to_string()),
        );
        row.entity_id = crate::engine2::entity_identity::EntityIdentity::single(entity_id);
        row.file_id = Some("file-a".to_string());
        row.version_id = version_id.to_string();
        row.global = false;
        row
    }

    fn fk_child_row(entity_id: &str, parent_id: &str, version_id: &str) -> StagedStateRow {
        let mut row = staged_row(
            "fk_child_schema",
            "1",
            Some(json!({ "id": entity_id, "parent_id": parent_id }).to_string()),
        );
        row.entity_id = crate::engine2::entity_identity::EntityIdentity::single(entity_id);
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
    ) -> StagedStateRow {
        let mut row = staged_row(
            "state_surface_ref_schema",
            "1",
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
        row.entity_id = crate::engine2::entity_identity::EntityIdentity::single(entity_id);
        row.file_id = Some("file-a".to_string());
        row.version_id = "version-a".to_string();
        row.global = false;
        row
    }

    fn staged_file_descriptor_row(file_id: &str, version_id: &str) -> StagedStateRow {
        let mut row = staged_row(
            FILE_DESCRIPTOR_SCHEMA_KEY,
            "1",
            Some(
                json!({
                    "id": file_id,
                    "directory_id": null,
                    "name": file_id,
                    "extension": null,
                    "hidden": false,
                })
                .to_string(),
            ),
        );
        row.entity_id = crate::engine2::entity_identity::EntityIdentity::single(file_id);
        row.file_id = None;
        row.version_id = version_id.to_string();
        row.global = version_id == crate::version::GLOBAL_VERSION_ID;
        row
    }

    fn committed_file_descriptor_row(file_id: &str, version_id: &str) -> LiveStateRow {
        LiveStateRow::from(staged_file_descriptor_row(file_id, version_id))
    }

    fn committed_unique_row(entity_id: &str, slug: &str, title: &str) -> LiveStateRow {
        let row = unique_row(entity_id, slug, title);
        LiveStateRow {
            entity_id: row.entity_id,
            schema_key: row.schema_key,
            file_id: row.file_id,
            snapshot_content: row.snapshot_content,
            metadata: row.metadata,
            schema_version: row.schema_version,
            created_at: row.created_at,
            updated_at: row.updated_at,
            global: row.global,
            change_id: row.change_id,
            commit_id: row.commit_id,
            untracked: row.untracked,
            version_id: row.version_id,
        }
    }

    fn staged_row(
        schema_key: &str,
        schema_version: &str,
        snapshot_content: Option<String>,
    ) -> StagedStateRow {
        StagedStateRow {
            entity_id: crate::engine2::entity_identity::EntityIdentity::single("entity-1"),
            schema_key: schema_key.to_string(),
            file_id: None,
            snapshot_content,
            metadata: None,
            schema_version: schema_version.to_string(),
            created_at: "2026-04-29T00:00:00.000Z".to_string(),
            updated_at: "2026-04-29T00:00:00.000Z".to_string(),
            global: true,
            change_id: Some("change-1".to_string()),
            commit_id: Some("commit-1".to_string()),
            untracked: false,
            version_id: crate::version::GLOBAL_VERSION_ID.to_string(),
        }
    }
}
