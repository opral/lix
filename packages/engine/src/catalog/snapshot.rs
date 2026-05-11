use std::{collections::BTreeMap, sync::Arc};

use jsonschema::JSONSchema;
use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::common::{format_json_pointer, parse_json_pointer};
use crate::domain::{Domain, DomainSchemaIdentity};
use crate::entity_identity::canonical_json_text;
use crate::functions::FunctionProviderHandle;
use crate::schema::{compile_lix_schema, validate_schema_amendment, SchemaKey};
use crate::LixError;

#[derive(Default)]
pub(crate) struct CatalogSnapshot {
    entries: Vec<CatalogEntry>,
    plans: Vec<SchemaPlan>,
    by_key: BTreeMap<SchemaCatalogKey, SchemaPlanId>,
    by_identity: BTreeMap<DomainSchemaIdentity, SchemaPlanId>,
    delete_references_by_target: BTreeMap<SchemaCatalogKey, Vec<DeleteReferencePlan>>,
    state_delete_references: Vec<StateDeleteReferencePlan>,
    fingerprint: CatalogFingerprint,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CatalogEntry {
    identity: DomainSchemaIdentity,
    key: SchemaCatalogKey,
    schema: JsonValue,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct CatalogFingerprint(String);

impl std::fmt::Debug for CatalogSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogSnapshot")
            .field("plan_count", &self.plans.len())
            .field("keys", &self.by_key.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl CatalogSnapshot {
    #[cfg(test)]
    pub(crate) fn from_visible_schemas(visible_schemas: &[JsonValue]) -> Result<Self, LixError> {
        let mut catalog = Self::default();
        for schema in visible_schemas {
            let key = crate::schema::schema_key_from_definition(schema)?;
            let catalog_key = SchemaCatalogKey::from_schema_key(key);
            let identity = DomainSchemaIdentity::new(
                Domain::schema_catalog(crate::GLOBAL_VERSION_ID, true),
                catalog_key.schema_key.clone(),
            );
            catalog.remember_schema_identity(identity, catalog_key, schema.clone())?;
        }
        catalog.rebuild_plans()?;
        Ok(catalog)
    }

    pub(crate) fn from_schema_facts(facts: &[SchemaCatalogFact]) -> Result<Self, LixError> {
        let entries = facts
            .iter()
            .map(|fact| CatalogEntry {
                identity: fact.identity.clone(),
                key: fact.catalog_key.clone(),
                schema: fact.schema.clone(),
            })
            .collect::<Vec<_>>();
        Self::from_entries(entries)
    }

    #[cfg(test)]
    pub(crate) fn fingerprint(&self) -> &CatalogFingerprint {
        &self.fingerprint
    }

    pub(crate) fn schema(&self, schema_key: &str) -> Option<&JsonValue> {
        self.plan_for_key(schema_key)
            .map(|(_, plan)| plan.schema.as_ref())
    }

    pub(crate) fn insert_schema_for_domain(
        &mut self,
        domain: Domain,
        key: SchemaKey,
        schema: JsonValue,
    ) -> Result<SchemaPlanId, LixError> {
        let key = SchemaCatalogKey::from_schema_key(key);
        let identity = DomainSchemaIdentity::new(domain, key.schema_key.clone());
        let mut entries = self.entries.clone();
        let mut candidate = Self::from_entries(entries.clone())?;
        let plan_id = candidate.remember_schema_identity(identity.clone(), key, schema)?;
        entries = candidate.entries.clone();
        let candidate = Self::from_entries(entries)?;
        *self = candidate;
        Ok(self.by_identity.get(&identity).copied().unwrap_or(plan_id))
    }

    fn from_entries(entries: Vec<CatalogEntry>) -> Result<Self, LixError> {
        let mut catalog = Self::default();
        for entry in entries {
            catalog.remember_schema_identity(entry.identity, entry.key, entry.schema)?;
        }
        catalog.rebuild_plans()?;
        Ok(catalog)
    }

    fn remember_schema_identity(
        &mut self,
        identity: DomainSchemaIdentity,
        key: SchemaCatalogKey,
        schema: JsonValue,
    ) -> Result<SchemaPlanId, LixError> {
        if let Some(existing) = self.by_identity.get(&identity).copied() {
            let existing_entry = &self.entries[existing.index()];
            if existing_entry.key == key && existing_entry.schema == schema {
                return Ok(existing);
            }
            if existing_entry.key == key {
                validate_schema_amendment(&existing_entry.schema, &schema)?;
                self.entries[existing.index()].schema = schema;
                return Ok(existing);
            }
            return Err(LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                format!("schema '{}' is already registered with a different definition in the same schema domain", key.schema_key),
            ));
        }
        if let Some(existing) = self.by_key.get(&key).copied() {
            let existing_entry = &self.entries[existing.index()];
            if existing_entry.identity == identity {
                return Ok(existing);
            }
            return Err(LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                format!("schema '{}' is visible from more than one schema domain", existing_entry.key.schema_key),
            )
            .with_hint("Schema references store schema_key, but not the schema domain. Remove the duplicate tracked/untracked schema registration or use a distinct schema key."));
        }

        let plan_id = SchemaPlanId(self.entries.len() as u32);
        self.by_key.insert(key.clone(), plan_id);
        self.by_identity.insert(identity.clone(), plan_id);
        self.entries.push(CatalogEntry {
            identity,
            key,
            schema,
        });
        Ok(plan_id)
    }

    fn rebuild_plans(&mut self) -> Result<(), LixError> {
        let schema_index = self
            .entries
            .iter()
            .map(|entry| (entry.key.clone(), &entry.schema))
            .collect::<BTreeMap<_, _>>();
        let plans = self
            .entries
            .iter()
            .map(|entry| {
                SchemaPlan::compile(
                    entry.key.clone(),
                    entry.schema.clone(),
                    &self.by_key,
                    &schema_index,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.plans = plans;
        self.rebuild_delete_plans();
        self.fingerprint = self.compute_fingerprint()?;
        Ok(())
    }

    fn rebuild_delete_plans(&mut self) {
        let mut delete_references_by_target =
            BTreeMap::<SchemaCatalogKey, Vec<DeleteReferencePlan>>::new();
        let mut state_delete_references = Vec::<StateDeleteReferencePlan>::new();
        for source_plan in &self.plans {
            for foreign_key in &source_plan.foreign_keys {
                delete_references_by_target
                    .entry(foreign_key.referenced_schema.clone())
                    .or_default()
                    .push(DeleteReferencePlan {
                        source_key: source_plan.key.clone(),
                        foreign_key: foreign_key.clone(),
                    });
            }
            for foreign_key in &source_plan.state_foreign_keys {
                state_delete_references.push(StateDeleteReferencePlan {
                    source_key: source_plan.key.clone(),
                    foreign_key: foreign_key.clone(),
                });
            }
        }
        self.delete_references_by_target = delete_references_by_target;
        self.state_delete_references = state_delete_references;
    }

    fn compute_fingerprint(&self) -> Result<CatalogFingerprint, LixError> {
        let mut hasher = blake3::Hasher::new();
        let mut entries = self.entries.iter().collect::<Vec<_>>();
        entries.sort_by(|left, right| left.identity.cmp(&right.identity));
        for entry in entries {
            hash_fingerprint_part(&mut hasher, &entry.identity.fingerprint_component());
            hash_fingerprint_part(&mut hasher, &entry.key.schema_key);
            let canonical_schema = canonical_json_text(&entry.schema).map_err(|error| {
                LixError::new(
                    LixError::CODE_SCHEMA_DEFINITION,
                    format!("failed to canonicalize schema for catalog fingerprint: {error}"),
                )
            })?;
            hash_fingerprint_part(&mut hasher, &canonical_schema);
        }
        Ok(CatalogFingerprint(hasher.finalize().to_hex().to_string()))
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, schema_key: &str) -> bool {
        self.plan_for_key(schema_key).is_some()
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.plans.len()
    }

    pub(crate) fn plans(&self) -> impl Iterator<Item = &SchemaPlan> {
        self.plans.iter()
    }

    pub(crate) fn plan(&self, plan_id: SchemaPlanId) -> Option<&SchemaPlan> {
        self.plans.get(plan_id.index())
    }

    pub(crate) fn plan_for_key(&self, schema_key: &str) -> Option<(SchemaPlanId, &SchemaPlan)> {
        let key = SchemaCatalogKey {
            schema_key: schema_key.to_string(),
        };
        let plan_id = *self.by_key.get(&key)?;
        let plan = self.plan(plan_id)?;
        Some((plan_id, plan))
    }

    pub(crate) fn delete_plan_for_key(&self, schema_key: &str) -> DeleteValidationPlan<'_> {
        let key = SchemaCatalogKey {
            schema_key: schema_key.to_string(),
        };
        DeleteValidationPlan {
            foreign_key_references: self
                .delete_references_by_target
                .get(&key)
                .map(Vec::as_slice)
                .unwrap_or(&[]),
            state_foreign_key_references: self.state_delete_references.as_slice(),
        }
    }
}

fn hash_fingerprint_part(hasher: &mut blake3::Hasher, value: &str) {
    hasher.update(&(value.len() as u64).to_le_bytes());
    hasher.update(value.as_bytes());
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct SchemaPlanId(u32);

impl SchemaPlanId {
    fn index(self) -> usize {
        self.0 as usize
    }

    #[cfg(test)]
    pub(crate) fn for_test(index: u32) -> Self {
        Self(index)
    }
}

pub(crate) type PointerGroup = Vec<Vec<String>>;

pub(crate) struct SchemaPlan {
    pub(crate) key: SchemaCatalogKey,
    pub(crate) schema: Arc<JsonValue>,
    pub(crate) compiled_schema: JSONSchema,
    pub(crate) defaults: DefaultPlan,
    pub(crate) primary_key: Option<PointerGroup>,
    pub(crate) uniques: Vec<PointerGroup>,
    pub(crate) foreign_keys: Vec<ForeignKeyPlan>,
    pub(crate) state_foreign_keys: Vec<StateForeignKeyPlan>,
}

impl SchemaPlan {
    fn compile(
        key: SchemaCatalogKey,
        schema: JsonValue,
        key_index: &BTreeMap<SchemaCatalogKey, SchemaPlanId>,
        schema_index: &BTreeMap<SchemaCatalogKey, &JsonValue>,
    ) -> Result<Self, LixError> {
        let compiled_schema = compile_lix_schema(&schema)?;
        let defaults = DefaultPlan::from_schema(&schema);
        let primary_key = primary_key_paths(&schema)?;
        let uniques = pointer_groups(&schema, "x-lix-unique")?;
        let foreign_keys = bind_foreign_key_plans(
            &key,
            &schema,
            foreign_key_plans(&schema)?,
            key_index,
            schema_index,
        )?;
        let state_foreign_keys = state_foreign_key_plans(&schema)?;
        Ok(Self {
            key,
            schema: Arc::new(schema),
            compiled_schema,
            defaults,
            primary_key,
            uniques,
            foreign_keys,
            state_foreign_keys,
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct DefaultPlan {
    properties: Vec<DefaultPropertyPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DefaultPropertyPlan {
    field_name: String,
    default: DefaultValuePlan,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DefaultValuePlan {
    Json(JsonValue),
    Cel(String),
}

impl DefaultPlan {
    fn from_schema(schema: &JsonValue) -> Self {
        let Some(properties) = schema.get("properties").and_then(JsonValue::as_object) else {
            return Self::default();
        };
        let mut ordered_properties = properties.iter().collect::<Vec<_>>();
        ordered_properties.sort_by(|(left_name, _), (right_name, _)| left_name.cmp(right_name));

        let properties = ordered_properties
            .into_iter()
            .filter_map(|(field_name, field_schema)| {
                if let Some(expression) = field_schema
                    .get("x-lix-default")
                    .and_then(JsonValue::as_str)
                {
                    return Some(DefaultPropertyPlan {
                        field_name: field_name.clone(),
                        default: DefaultValuePlan::Cel(expression.to_string()),
                    });
                }
                field_schema
                    .get("default")
                    .map(|value| DefaultPropertyPlan {
                        field_name: field_name.clone(),
                        default: DefaultValuePlan::Json(value.clone()),
                    })
            })
            .collect();
        Self { properties }
    }

    pub(crate) fn apply(
        &self,
        snapshot: &mut JsonMap<String, JsonValue>,
        functions: FunctionProviderHandle,
        schema_key: &str,
    ) -> Result<bool, LixError> {
        let mut changed = false;
        let mut cel_context = None::<JsonMap<String, JsonValue>>;
        for property in &self.properties {
            if snapshot.contains_key(&property.field_name) {
                continue;
            }
            let value = match &property.default {
                DefaultValuePlan::Json(value) => value.clone(),
                DefaultValuePlan::Cel(expression) => {
                    let context = cel_context.get_or_insert_with(|| snapshot.clone());
                    crate::cel::shared_runtime()
                        .evaluate_with_functions(expression, context, functions.clone())
                        .map_err(|err| LixError {
                            code: "LIX_ERROR_UNKNOWN".to_string(),
                            message: format!(
                                "failed to evaluate x-lix-default for '{}.{}': {}",
                                schema_key, property.field_name, err.message
                            ),
                            hint: None,
                            details: None,
                        })?
                }
            };
            snapshot.insert(property.field_name.clone(), value);
            changed = true;
        }
        Ok(changed)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ForeignKeyPlan {
    pub(crate) local_properties: PointerGroup,
    pub(crate) referenced_schema: SchemaCatalogKey,
    pub(crate) referenced_plan_id: SchemaPlanId,
    pub(crate) referenced_properties: PointerGroup,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeleteReferencePlan {
    pub(crate) source_key: SchemaCatalogKey,
    pub(crate) foreign_key: ForeignKeyPlan,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct StateDeleteReferencePlan {
    pub(crate) source_key: SchemaCatalogKey,
    pub(crate) foreign_key: StateForeignKeyPlan,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct DeleteValidationPlan<'a> {
    pub(crate) foreign_key_references: &'a [DeleteReferencePlan],
    pub(crate) state_foreign_key_references: &'a [StateDeleteReferencePlan],
}

impl DeleteValidationPlan<'_> {
    pub(crate) fn has_committed_checks(self) -> bool {
        !self.foreign_key_references.is_empty() || !self.state_foreign_key_references.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct UnboundForeignKeyPlan {
    local_properties: PointerGroup,
    referenced_schema: SchemaCatalogKey,
    referenced_properties: PointerGroup,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct StateForeignKeyPlan {
    /// Slot [0] in `x-lix-state-foreign-keys`: local pointer to the target entity_id.
    pub(crate) entity_id_property: Vec<String>,
    /// Slot [1] in `x-lix-state-foreign-keys`: local pointer to the target schema_key.
    pub(crate) schema_key_property: Vec<String>,
    /// Slot [2] in `x-lix-state-foreign-keys`: local pointer to the target file_id.
    pub(crate) file_id_property: Vec<String>,
}

impl StateForeignKeyPlan {
    pub(crate) fn local_properties(&self) -> PointerGroup {
        vec![
            self.entity_id_property.clone(),
            self.schema_key_property.clone(),
            self.file_id_property.clone(),
        ]
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct SchemaCatalogKey {
    pub(crate) schema_key: String,
}

impl SchemaCatalogKey {
    pub(crate) fn from_schema_key(key: SchemaKey) -> Self {
        Self {
            schema_key: key.schema_key,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SchemaCatalogFact {
    identity: DomainSchemaIdentity,
    catalog_key: SchemaCatalogKey,
    schema: JsonValue,
}

impl SchemaCatalogFact {
    pub(crate) fn new(domain: Domain, key: SchemaKey, schema: JsonValue) -> Self {
        let catalog_key = SchemaCatalogKey::from_schema_key(key);
        let identity = DomainSchemaIdentity::new(domain, catalog_key.schema_key.clone());
        Self {
            identity,
            catalog_key,
            schema,
        }
    }

    pub(crate) fn schema(&self) -> &JsonValue {
        &self.schema
    }

    pub(crate) fn catalog_key(&self) -> &SchemaCatalogKey {
        &self.catalog_key
    }
}

fn primary_key_paths(schema: &JsonValue) -> Result<Option<Vec<Vec<String>>>, LixError> {
    let Some(primary_key) = schema.get("x-lix-primary-key") else {
        return Ok(None);
    };
    let primary_key = primary_key.as_array().ok_or_else(|| {
        LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            "schema x-lix-primary-key must be an array of JSON Pointers",
        )
    })?;
    primary_key
        .iter()
        .enumerate()
        .map(|(index, pointer)| {
            let pointer = pointer.as_str().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_SCHEMA_DEFINITION,
                    format!("schema x-lix-primary-key entry at index {index} must be a string"),
                )
            })?;
            parse_json_pointer(pointer)
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

fn pointer_groups(schema: &JsonValue, field: &str) -> Result<Vec<PointerGroup>, LixError> {
    let Some(value) = schema.get(field) else {
        return Ok(Vec::new());
    };
    let groups = value
        .as_array()
        .map(|groups| groups.iter().collect::<Vec<_>>())
        .unwrap_or_default();
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

fn foreign_key_plans(schema: &JsonValue) -> Result<Vec<UnboundForeignKeyPlan>, LixError> {
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
                        format!(
                            "x-lix-foreign-keys[{index}].references.schemaKey must be a string"
                        ),
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
                        "x-lix-foreign-keys[{index}] properties and references.properties must have the same length"
                    ),
                ));
            }
            Ok(UnboundForeignKeyPlan {
                local_properties,
                referenced_schema: SchemaCatalogKey {
                    schema_key: referenced_schema_key,
                },
                referenced_properties,
            })
        })
        .collect()
}

fn bind_foreign_key_plans(
    source_key: &SchemaCatalogKey,
    source_schema: &JsonValue,
    unbound_foreign_keys: Vec<UnboundForeignKeyPlan>,
    key_index: &BTreeMap<SchemaCatalogKey, SchemaPlanId>,
    schema_index: &BTreeMap<SchemaCatalogKey, &JsonValue>,
) -> Result<Vec<ForeignKeyPlan>, LixError> {
    unbound_foreign_keys
        .into_iter()
        .map(|foreign_key| {
            if foreign_key.referenced_schema.schema_key == "lix_state" {
                return Err(LixError::new(
                    LixError::CODE_SCHEMA_DEFINITION,
                    format!(
                        "foreign key on schema '{}' must not reference schemaKey 'lix_state'; use x-lix-state-foreign-keys with pointers ordered as [entity_id, schema_key, file_id]",
                        source_key.schema_key
                    ),
                ));
            }

            let referenced_plan_id =
                *key_index.get(&foreign_key.referenced_schema).ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_SCHEMA_DEFINITION,
                        format!(
                            "foreign key on schema '{}' references missing schema '{}'",
                            source_key.schema_key,
                            foreign_key.referenced_schema.schema_key,
                        ),
                    )
                })?;
            let target_schema =
                schema_index
                    .get(&foreign_key.referenced_schema)
                    .copied()
                    .ok_or_else(|| {
                        LixError::new(
                        LixError::CODE_SCHEMA_DEFINITION,
                        format!(
                                "foreign key on schema '{}' references missing schema '{}'",
                                source_key.schema_key,
                                foreign_key.referenced_schema.schema_key,
                            ),
                    )
                })?;

            for (local_pointer, referenced_pointer) in foreign_key
                .local_properties
                .iter()
                .zip(foreign_key.referenced_properties.iter())
            {
                let local_field =
                    schema_field_at_pointer(source_schema, local_pointer).map_err(|detail| {
                        LixError::new(
                            LixError::CODE_SCHEMA_DEFINITION,
                            format!(
                                "foreign key on schema '{}' references missing local property '{}': {detail}",
                                source_key.schema_key,
                                format_json_pointer(local_pointer)
                            ),
                        )
                    })?;
                let referenced_field =
                    schema_field_at_pointer(target_schema, referenced_pointer).map_err(
                        |detail| {
                            LixError::new(
                                LixError::CODE_SCHEMA_DEFINITION,
                                format!(
                                    "foreign key on schema '{}' references missing target property '{}.{}': {detail}",
                                    source_key.schema_key,
                                    foreign_key.referenced_schema.schema_key,
                                    format_json_pointer(referenced_pointer)
                                ),
                            )
                        },
                    )?;
                validate_foreign_key_field_types(
                    source_key,
                    &foreign_key.referenced_schema,
                    local_pointer,
                    local_field,
                    referenced_pointer,
                    referenced_field,
                )?;
            }

            if !schema_properties_are_keyed(target_schema, &foreign_key.referenced_properties)? {
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

            Ok(ForeignKeyPlan {
                local_properties: foreign_key.local_properties,
                referenced_schema: foreign_key.referenced_schema,
                referenced_plan_id,
                referenced_properties: foreign_key.referenced_properties,
            })
        })
        .collect()
}

fn schema_field_at_pointer<'a>(
    schema: &'a JsonValue,
    pointer: &[String],
) -> Result<&'a JsonValue, String> {
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
    Ok(current)
}

fn validate_foreign_key_field_types(
    source_key: &SchemaCatalogKey,
    referenced_key: &SchemaCatalogKey,
    local_pointer: &[String],
    local_field: &JsonValue,
    referenced_pointer: &[String],
    referenced_field: &JsonValue,
) -> Result<(), LixError> {
    let local_type = compatible_json_schema_type(local_field).ok_or_else(|| {
        LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            format!(
                "foreign key on schema '{}' local property '{}' must declare an explicit JSON Schema type",
                source_key.schema_key,
                format_json_pointer(local_pointer)
            ),
        )
    })?;
    let referenced_type = compatible_json_schema_type(referenced_field).ok_or_else(|| {
        LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            format!(
                "foreign key on schema '{}' target property '{}.{}' must declare an explicit JSON Schema type",
                source_key.schema_key,
                referenced_key.schema_key,
                format_json_pointer(referenced_pointer)
            ),
        )
    })?;
    if local_type != referenced_type {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            format!(
                "foreign key on schema '{}' has incompatible field types: local '{}' is {}, but target '{}.{}' is {}",
                source_key.schema_key,
                format_json_pointer(local_pointer),
                local_type,
                referenced_key.schema_key,
                format_json_pointer(referenced_pointer),
                referenced_type
            ),
        ));
    }
    Ok(())
}

fn compatible_json_schema_type(field_schema: &JsonValue) -> Option<JsonValue> {
    match field_schema.get("type")? {
        JsonValue::Array(types) => {
            let non_null_types = types
                .iter()
                .filter(|value| value.as_str() != Some("null"))
                .cloned()
                .collect::<Vec<_>>();
            match non_null_types.as_slice() {
                [] => None,
                [single] => Some(single.clone()),
                _ => Some(JsonValue::Array(non_null_types)),
            }
        }
        value => Some(value.clone()),
    }
}

fn schema_properties_are_keyed(
    target_schema: &JsonValue,
    referenced_properties: &[Vec<String>],
) -> Result<bool, LixError> {
    if let Some(primary_key) = primary_key_paths(target_schema)? {
        if primary_key == referenced_properties {
            return Ok(true);
        }
    }
    Ok(pointer_groups(target_schema, "x-lix-unique")?
        .iter()
        .any(|unique_group| unique_group == referenced_properties))
}

fn state_foreign_key_plans(schema: &JsonValue) -> Result<Vec<StateForeignKeyPlan>, LixError> {
    let Some(value) = schema.get("x-lix-state-foreign-keys") else {
        return Ok(Vec::new());
    };
    let Some(foreign_keys) = value.as_array() else {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            "schema x-lix-state-foreign-keys must be an array",
        ));
    };

    foreign_keys
        .iter()
        .enumerate()
        .map(|(index, foreign_key)| {
            let local_properties = pointer_array(
                Some(foreign_key),
                &format!("x-lix-state-foreign-keys[{index}]"),
            )?;
            if local_properties.len() != 3 {
                return Err(LixError::new(
                    LixError::CODE_SCHEMA_DEFINITION,
                    format!(
                        "x-lix-state-foreign-keys[{index}] must contain exactly three JSON Pointers ordered as [entity_id, schema_key, file_id]"
                    ),
                ));
            }
            Ok(StateForeignKeyPlan {
                entity_id_property: local_properties[0].clone(),
                schema_key_property: local_properties[1].clone(),
                file_id_property: local_properties[2].clone(),
            })
        })
        .collect()
}

fn pointer_array(value: Option<&JsonValue>, context: &str) -> Result<PointerGroup, LixError> {
    let Some(value) = value else {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            format!("{context} must be an array of JSON Pointers"),
        ));
    };
    let Some(array) = value.as_array() else {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            format!("{context} must be an array of JSON Pointers"),
        ));
    };
    array
        .iter()
        .enumerate()
        .map(|(index, pointer)| {
            let pointer = pointer.as_str().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_SCHEMA_DEFINITION,
                    format!("{context}[{index}] must be a string"),
                )
            })?;
            parse_json_pointer(pointer)
        })
        .collect()
}

fn format_pointer_group(paths: &[Vec<String>]) -> String {
    paths
        .iter()
        .map(|path| format_json_pointer(path))
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn catalog_rejects_same_schema_key_from_multiple_domains() {
        let tracked = SchemaCatalogFact::new(
            Domain::schema_catalog("main", false),
            SchemaKey::new("example_schema"),
            schema_json("example_schema"),
        );
        let untracked = SchemaCatalogFact::new(
            Domain::schema_catalog("main", true),
            SchemaKey::new("example_schema"),
            schema_json("example_schema"),
        );

        let error = CatalogSnapshot::from_schema_facts(&[tracked, untracked])
            .expect_err("same schema key in two reachable domains is ambiguous");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(error.message.contains("more than one schema domain"));
    }

    #[test]
    fn insert_schema_for_domain_is_atomic_when_binding_fails() {
        let mut catalog = CatalogSnapshot::from_schema_facts(&[SchemaCatalogFact::new(
            Domain::schema_catalog("main", false),
            SchemaKey::new("base_schema"),
            schema_json("base_schema"),
        )])
        .expect("base catalog should bind");

        let error = catalog
            .insert_schema_for_domain(
                Domain::schema_catalog("main", false),
                SchemaKey::new("bad_child_schema"),
                child_schema_json("bad_child_schema", "missing_parent_schema"),
            )
            .expect_err("schema with missing FK target should fail");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(catalog.contains("base_schema"));
        assert!(
            !catalog.contains("bad_child_schema"),
            "failed catalog insert must not publish a partially bound schema"
        );
    }

    #[test]
    fn catalog_fingerprint_is_independent_of_fact_order() {
        let parent = SchemaCatalogFact::new(
            Domain::schema_catalog("main", false),
            SchemaKey::new("parent_schema"),
            schema_json("parent_schema"),
        );
        let child = SchemaCatalogFact::new(
            Domain::schema_catalog("main", false),
            SchemaKey::new("child_schema"),
            child_schema_json("child_schema", "parent_schema"),
        );

        let parent_first = CatalogSnapshot::from_schema_facts(&[parent.clone(), child.clone()])
            .expect("parent-first facts should bind");
        let child_first = CatalogSnapshot::from_schema_facts(&[child, parent])
            .expect("child-first facts should bind as the same domain snapshot");

        assert_eq!(parent_first.fingerprint(), child_first.fingerprint());
    }

    #[test]
    fn delete_plan_has_no_committed_checks_for_unreferenced_schema() {
        let catalog = CatalogSnapshot::from_schema_facts(&[SchemaCatalogFact::new(
            Domain::schema_catalog("main", false),
            SchemaKey::new("standalone_schema"),
            schema_json("standalone_schema"),
        )])
        .expect("catalog should bind");

        let delete_plan = catalog.delete_plan_for_key("standalone_schema");

        assert!(!delete_plan.has_committed_checks());
        assert!(delete_plan.foreign_key_references.is_empty());
        assert!(delete_plan.state_foreign_key_references.is_empty());
    }

    #[test]
    fn delete_plan_indexes_foreign_keys_by_referenced_schema() {
        let parent = SchemaCatalogFact::new(
            Domain::schema_catalog("main", false),
            SchemaKey::new("parent_schema"),
            schema_json("parent_schema"),
        );
        let child = SchemaCatalogFact::new(
            Domain::schema_catalog("main", false),
            SchemaKey::new("child_schema"),
            child_schema_json("child_schema", "parent_schema"),
        );
        let catalog =
            CatalogSnapshot::from_schema_facts(&[parent, child]).expect("catalog should bind");

        let parent_delete_plan = catalog.delete_plan_for_key("parent_schema");
        let child_delete_plan = catalog.delete_plan_for_key("child_schema");

        assert!(parent_delete_plan.has_committed_checks());
        assert_eq!(parent_delete_plan.foreign_key_references.len(), 1);
        assert_eq!(
            parent_delete_plan.foreign_key_references[0]
                .source_key
                .schema_key,
            "child_schema"
        );
        assert!(!child_delete_plan.has_committed_checks());
    }

    #[test]
    fn delete_plan_conservatively_applies_state_foreign_keys_to_every_schema() {
        let target = SchemaCatalogFact::new(
            Domain::schema_catalog("main", false),
            SchemaKey::new("target_schema"),
            schema_json("target_schema"),
        );
        let source = SchemaCatalogFact::new(
            Domain::schema_catalog("main", false),
            SchemaKey::new("state_fk_schema"),
            state_fk_schema_json("state_fk_schema"),
        );
        let catalog =
            CatalogSnapshot::from_schema_facts(&[target, source]).expect("catalog should bind");

        let target_delete_plan = catalog.delete_plan_for_key("target_schema");

        assert!(target_delete_plan.has_committed_checks());
        assert_eq!(target_delete_plan.state_foreign_key_references.len(), 1);
        assert_eq!(
            target_delete_plan.state_foreign_key_references[0]
                .source_key
                .schema_key,
            "state_fk_schema"
        );
    }

    fn schema_json(schema_key: &str) -> JsonValue {
        json!({
            "x-lix-key": schema_key,
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string" }
            },
            "required": ["id"],
            "additionalProperties": false
        })
    }

    fn child_schema_json(schema_key: &str, parent_schema_key: &str) -> JsonValue {
        json!({
            "x-lix-key": schema_key,
            "x-lix-primary-key": ["/id"],
            "x-lix-foreign-keys": [{
                "properties": ["/parent_id"],
                "references": {
                    "schemaKey": parent_schema_key,
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

    fn state_fk_schema_json(schema_key: &str) -> JsonValue {
        json!({
            "x-lix-key": schema_key,
            "x-lix-primary-key": ["/id"],
            "x-lix-state-foreign-keys": [["/target_id", "/target_schema", "/target_file"]],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "target_id": { "type": "string" },
                "target_schema": { "type": "string" },
                "target_file": { "type": ["string", "null"] }
            },
            "required": ["id", "target_id", "target_schema", "target_file"],
            "additionalProperties": false
        })
    }
}
