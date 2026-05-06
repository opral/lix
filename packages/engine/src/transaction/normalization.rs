use std::{collections::BTreeMap, sync::Arc};

use jsonschema::JSONSchema;
use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::common::normalize_path_segment;
use crate::entity_identity::{EntityIdentity, EntityIdentityError};
use crate::functions::FunctionProviderHandle;
use crate::schema::{
    compile_lix_schema, is_seed_schema_key, reject_unsupported_registered_schema_version,
    schema_from_registered_snapshot, schema_key_from_definition, validate_lix_schema,
    validate_lix_schema_definition, SchemaKey,
};
use crate::transaction::types::{PreparedRowFacts, TransactionJson, TransactionWriteRow};
use crate::LixError;

pub(crate) const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";
const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";
const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NormalizedTransactionWriteRow {
    pub(crate) row: TransactionWriteRow,
    pub(crate) snapshot: Option<TransactionJson>,
    pub(crate) schema_plan_id: SchemaPlanId,
    pub(crate) facts: PreparedRowFacts,
}

/// Transaction-local schema catalog used while transaction write rows are prepared.
///
/// Normalization has to happen before rows are keyed in the prepared-write map:
/// defaults may fill primary-key fields and primary keys may derive the final
/// entity id. The catalog starts with session-visible schemas and is updated as
/// pending `lix_registered_schema` rows are prepared, so later rows in the
/// same transaction can target newly registered schemas.
#[derive(Default)]
pub(crate) struct TransactionSchemaCatalog {
    plans: Vec<TransactionSchemaPlan>,
    by_key: BTreeMap<SchemaCatalogKey, SchemaPlanId>,
    by_schema_key: BTreeMap<String, SchemaPlanId>,
}

impl std::fmt::Debug for TransactionSchemaCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransactionSchemaCatalog")
            .field("plan_count", &self.plans.len())
            .field("keys", &self.by_key.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl TransactionSchemaCatalog {
    pub(crate) fn from_visible_schemas(visible_schemas: &[JsonValue]) -> Result<Self, LixError> {
        let mut catalog = Self::default();
        for schema in visible_schemas {
            let key = schema_key_from_definition(schema)?;
            catalog.insert_schema(key, schema.clone())?;
        }
        Ok(catalog)
    }

    pub(crate) fn schema(&self, schema_key: &str, schema_version: &str) -> Option<&JsonValue> {
        self.plan_for_key(schema_key, schema_version)
            .map(|(_, plan)| plan.schema.as_ref())
    }

    pub(crate) fn insert_schema(
        &mut self,
        key: SchemaKey,
        schema: JsonValue,
    ) -> Result<SchemaPlanId, LixError> {
        let key = SchemaCatalogKey::from_schema_key(key);
        let plan = TransactionSchemaPlan::compile(key.clone(), schema)?;
        if let Some(existing) = self.by_key.get(&key).copied() {
            self.plans[existing.index()] = plan;
            return Ok(existing);
        }
        let plan_id = SchemaPlanId(self.plans.len() as u32);
        self.by_schema_key
            .entry(key.schema_key.clone())
            .or_insert(plan_id);
        self.by_key.insert(key, plan_id);
        self.plans.push(plan);
        Ok(plan_id)
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, schema_key: &str, schema_version: &str) -> bool {
        self.plan_for_key(schema_key, schema_version).is_some()
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.plans.len()
    }

    pub(crate) fn schema_key_by_key(&self, schema_key: &str) -> Option<SchemaCatalogKey> {
        self.by_schema_key
            .get(schema_key)
            .and_then(|plan_id| self.plan(*plan_id))
            .map(|plan| plan.key.clone())
    }

    pub(crate) fn plan_by_schema_key(&self, schema_key: &str) -> Option<&TransactionSchemaPlan> {
        self.by_schema_key
            .get(schema_key)
            .and_then(|plan_id| self.plan(*plan_id))
    }

    pub(crate) fn plans(&self) -> impl Iterator<Item = &TransactionSchemaPlan> {
        self.plans.iter()
    }

    pub(crate) fn plan(&self, plan_id: SchemaPlanId) -> Option<&TransactionSchemaPlan> {
        self.plans.get(plan_id.index())
    }

    pub(crate) fn plan_for_key(
        &self,
        schema_key: &str,
        schema_version: &str,
    ) -> Option<(SchemaPlanId, &TransactionSchemaPlan)> {
        let key = SchemaCatalogKey {
            schema_key: schema_key.to_string(),
            schema_version: schema_version.to_string(),
        };
        let plan_id = *self.by_key.get(&key)?;
        let plan = self.plan(plan_id)?;
        Some((plan_id, plan))
    }
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

pub(crate) struct TransactionSchemaPlan {
    pub(crate) key: SchemaCatalogKey,
    pub(crate) schema: Arc<JsonValue>,
    pub(crate) compiled_schema: JSONSchema,
    pub(crate) defaults: DefaultPlan,
    pub(crate) primary_key: Option<PointerGroup>,
    pub(crate) uniques: Vec<PointerGroup>,
    pub(crate) foreign_keys: Vec<ForeignKeyPlan>,
}

impl TransactionSchemaPlan {
    fn compile(key: SchemaCatalogKey, schema: JsonValue) -> Result<Self, LixError> {
        let compiled_schema = compile_lix_schema(&schema)?;
        let defaults = DefaultPlan::from_schema(&schema);
        let primary_key = primary_key_paths(&schema)?;
        let uniques = pointer_groups(&schema, "x-lix-unique")?;
        let foreign_keys = foreign_key_plans(&schema)?;
        Ok(Self {
            key,
            schema: Arc::new(schema),
            compiled_schema,
            defaults,
            primary_key,
            uniques,
            foreign_keys,
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

    fn apply(
        &self,
        snapshot: &mut JsonMap<String, JsonValue>,
        functions: FunctionProviderHandle,
        schema_key: &str,
        schema_version: &str,
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
                                "failed to evaluate x-lix-default for '{}.{}' ({}): {}",
                                schema_key, property.field_name, schema_version, err.message
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
    pub(crate) referenced_schema_key: String,
    pub(crate) referenced_properties: PointerGroup,
}

/// Normalizes one incoming row into a row with final snapshot/entity identity.
///
/// This is the canonical schema-semantics boundary for transaction writes. It owns
/// schema default application, primary-key identity derivation, and explicit
/// identity mismatch validation. SQL providers should not pre-derive primary
/// keys for schemas that can be normalized here; they should pass decoded
/// snapshots and let this layer complete them.
///
/// This function intentionally does not assign timestamps, change ids, or
/// commit ids; those are prepared-row fields assigned after semantic
/// normalization has produced the final identity.
pub(crate) fn normalize_transaction_write_row(
    mut row: TransactionWriteRow,
    schema_catalog: &mut TransactionSchemaCatalog,
    functions: FunctionProviderHandle,
) -> Result<NormalizedTransactionWriteRow, LixError> {
    validate_transaction_write_row_schema_identity(&row)?;

    let Some((schema_plan_id, schema_plan)) =
        schema_catalog.plan_for_key(&row.schema_key, &row.schema_version)
    else {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            format!(
                "schema '{}' version '{}' is not visible to this transaction",
                row.schema_key, row.schema_version
            ),
        ));
    };

    let normalized_snapshot = if let Some(snapshot) = row.snapshot.take() {
        let (mut snapshot, normalized) = snapshot_object_from_transaction_json(snapshot, &row)?;
        let defaults_changed = apply_defaults(&mut snapshot, schema_plan, &row, functions)?;
        let descriptor_changed = normalize_filesystem_descriptor_snapshot(&row, &mut snapshot)?;
        let snapshot = JsonValue::Object(snapshot);
        row.entity_id = Some(resolve_entity_id(&row, schema_plan, &snapshot)?);
        if defaults_changed || descriptor_changed {
            Some(TransactionJson::from_value(
                snapshot,
                "normalized transaction snapshot_content",
            )?)
        } else {
            Some(TransactionJson::from_parts(Arc::new(snapshot), normalized))
        }
    } else if row.entity_id.is_none() {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_VALIDATION,
            format!(
                "tombstone for schema '{}' version '{}' requires entity_id",
                row.schema_key, row.schema_version
            ),
        ));
    } else {
        None
    };

    if row.schema_key == REGISTERED_SCHEMA_KEY {
        remember_pending_registered_schema(
            normalized_snapshot.as_ref().map(TransactionJson::value),
            schema_catalog,
        )?;
    }

    Ok(NormalizedTransactionWriteRow {
        row,
        snapshot: normalized_snapshot,
        schema_plan_id,
        facts: PreparedRowFacts::default(),
    })
}

fn validate_transaction_write_row_schema_identity(
    row: &TransactionWriteRow,
) -> Result<(), LixError> {
    if row.schema_key.is_empty() {
        return Err(LixError::new(
            LixError::CODE_UNKNOWN,
            "engine transaction staging requires non-empty schema_key",
        ));
    }
    if row.schema_version.is_empty() {
        return Err(LixError::new(
            LixError::CODE_UNKNOWN,
            "engine transaction staging requires non-empty schema_version",
        ));
    }
    Ok(())
}

fn snapshot_object_from_transaction_json(
    snapshot: TransactionJson,
    row: &TransactionWriteRow,
) -> Result<(JsonMap<String, JsonValue>, Arc<str>), LixError> {
    let (snapshot, normalized) = snapshot.into_parts();
    let snapshot = match Arc::try_unwrap(snapshot) {
        Ok(snapshot) => snapshot,
        Err(snapshot) => snapshot.as_ref().clone(),
    };
    match snapshot {
        JsonValue::Object(snapshot) => Ok((snapshot, normalized)),
        _ => Err(LixError::new(
            LixError::CODE_SCHEMA_VALIDATION,
            format!(
                "snapshot_content for schema '{}' version '{}' must be a JSON object",
                row.schema_key, row.schema_version
            ),
        )),
    }
}

fn apply_defaults(
    snapshot: &mut JsonMap<String, JsonValue>,
    schema_plan: &TransactionSchemaPlan,
    row: &TransactionWriteRow,
    functions: FunctionProviderHandle,
) -> Result<bool, LixError> {
    schema_plan
        .defaults
        .apply(snapshot, functions, &row.schema_key, &row.schema_version)
}

fn normalize_filesystem_descriptor_snapshot(
    row: &TransactionWriteRow,
    snapshot: &mut JsonMap<String, JsonValue>,
) -> Result<bool, LixError> {
    match row.schema_key.as_str() {
        DIRECTORY_DESCRIPTOR_SCHEMA_KEY => normalize_directory_descriptor_snapshot(row, snapshot),
        FILE_DESCRIPTOR_SCHEMA_KEY => normalize_file_descriptor_snapshot(row, snapshot),
        _ => Ok(false),
    }
}

fn normalize_directory_descriptor_snapshot(
    row: &TransactionWriteRow,
    snapshot: &mut JsonMap<String, JsonValue>,
) -> Result<bool, LixError> {
    let Some(name) = optional_string_field(snapshot, "name", row)? else {
        return Ok(false);
    };
    let normalized_name = normalize_path_segment(name)?;
    if name == normalized_name {
        return Ok(false);
    }
    snapshot.insert("name".to_string(), JsonValue::String(normalized_name));
    Ok(true)
}

fn normalize_file_descriptor_snapshot(
    row: &TransactionWriteRow,
    snapshot: &mut JsonMap<String, JsonValue>,
) -> Result<bool, LixError> {
    let Some(name) = optional_string_field(snapshot, "name", row)? else {
        return Ok(false);
    };
    let normalized_name = normalize_path_segment(name)?;
    if name == normalized_name {
        return Ok(false);
    }
    snapshot.insert("name".to_string(), JsonValue::String(normalized_name));
    Ok(true)
}

fn optional_string_field<'a>(
    snapshot: &'a JsonMap<String, JsonValue>,
    field: &str,
    row: &TransactionWriteRow,
) -> Result<Option<&'a str>, LixError> {
    let Some(value) = snapshot.get(field) else {
        return Ok(None);
    };
    value.as_str().map(Some).ok_or_else(|| {
        LixError::new(
            LixError::CODE_SCHEMA_VALIDATION,
            format!(
                "snapshot_content for schema '{}' version '{}' field '{}' must be a string",
                row.schema_key, row.schema_version, field
            ),
        )
    })
}

fn resolve_entity_id(
    row: &TransactionWriteRow,
    schema_plan: &TransactionSchemaPlan,
    snapshot: &JsonValue,
) -> Result<EntityIdentity, LixError> {
    let Some(primary_key_paths) = schema_plan.primary_key.as_ref() else {
        return row.entity_id.clone().ok_or_else(|| {
            LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!(
                    "write for schema '{}' version '{}' requires entity_id because the schema has no x-lix-primary-key",
                    row.schema_key, row.schema_version
                ),
            )
        });
    };
    let derived = EntityIdentity::from_primary_key_paths(snapshot, primary_key_paths)
        .map_err(|error| entity_id_derivation_error(row, primary_key_paths, error))?;
    if let Some(entity_id) = row.entity_id.as_ref() {
        if entity_id != &derived {
            return Err(LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!(
                    "entity_id '{}' does not match x-lix-primary-key derived entity_id '{}' for schema '{}' version '{}'",
                    entity_id.as_string()?, derived.as_string()?, row.schema_key, row.schema_version
                ),
            ));
        }
    }
    Ok(derived)
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

fn foreign_key_plans(schema: &JsonValue) -> Result<Vec<ForeignKeyPlan>, LixError> {
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
            Ok(ForeignKeyPlan {
                local_properties,
                referenced_schema_key,
                referenced_properties,
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
        .map(decode_json_pointer_segment)
        .collect()
}

fn decode_json_pointer_segment(segment: &str) -> Result<String, LixError> {
    let mut out = String::new();
    let mut chars = segment.chars();
    while let Some(ch) = chars.next() {
        if ch == '~' {
            match chars.next() {
                Some('0') => out.push('~'),
                Some('1') => out.push('/'),
                _ => {
                    return Err(LixError::new(
                        LixError::CODE_SCHEMA_DEFINITION,
                        "invalid JSON pointer escape",
                    ))
                }
            }
        } else {
            out.push(ch);
        }
    }
    Ok(out)
}

fn entity_id_derivation_error(
    row: &TransactionWriteRow,
    primary_key_paths: &[Vec<String>],
    error: EntityIdentityError,
) -> LixError {
    let detail = match error {
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
        LixError::CODE_SCHEMA_VALIDATION,
        format!(
            "failed to derive entity_id for schema '{}' version '{}': {detail}",
            row.schema_key, row.schema_version
        ),
    )
}

fn format_json_pointer(segments: &[String]) -> String {
    if segments.is_empty() {
        return String::new();
    }
    format!(
        "/{}",
        segments
            .iter()
            .map(|segment| segment.replace('~', "~0").replace('/', "~1"))
            .collect::<Vec<_>>()
            .join("/")
    )
}

pub(crate) fn remember_pending_registered_schema(
    snapshot: Option<&JsonValue>,
    schema_catalog: &mut TransactionSchemaCatalog,
) -> Result<(), LixError> {
    let Some(snapshot) = snapshot else {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            "lix_registered_schema rows cannot be deleted yet; schema deletion is not supported",
        ));
    };
    let (key, schema) = schema_from_registered_snapshot(&snapshot)?;
    reject_unsupported_registered_schema_version(&key)?;
    if is_seed_schema_key(&key.schema_key) {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            format!(
                "schema '{}' is a system schema and cannot be registered at runtime",
                key.schema_key
            ),
        ));
    }
    validate_lix_schema_definition(&schema)?;
    {
        let registered_schema_definition = schema_catalog
            .schema(REGISTERED_SCHEMA_KEY, "1")
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_SCHEMA_DEFINITION,
                    "lix_registered_schema schema is not visible to this transaction",
                )
            })?;
        if !snapshot.get("value").is_some_and(JsonValue::is_object) {
            validate_lix_schema(registered_schema_definition, &snapshot)?;
        }
        validate_lix_schema(registered_schema_definition, &snapshot)?;
    }
    schema_catalog.insert_schema(key, schema)?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct SchemaCatalogKey {
    pub(crate) schema_key: String,
    pub(crate) schema_version: String,
}

impl SchemaCatalogKey {
    pub(crate) fn from_schema_key(key: SchemaKey) -> Self {
        Self {
            schema_key: key.schema_key,
            schema_version: key.schema_version,
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::functions::{FunctionProvider, SharedFunctionProvider};
    use crate::schema::seed_schema_definition;

    #[test]
    fn normalization_derives_entity_id_from_primary_key() {
        let mut catalog = catalog_with(vec![schema_with_default_id()]);
        let row = TransactionWriteRow {
            entity_id: None,
            schema_key: "normalization_schema".to_string(),
            schema_version: "1".to_string(),
            snapshot: Some(snapshot_json(
                r#"{"id":"entity-from-snapshot","value":"hello"}"#,
            )),
            ..base_stage_row()
        };

        let row =
            normalize_transaction_write_row(row, &mut catalog, functions()).expect("normalize row");

        assert_eq!(
            row.row.entity_id.as_ref(),
            Some(&crate::entity_identity::EntityIdentity::single(
                "entity-from-snapshot"
            ))
        );
    }

    #[test]
    fn normalization_applies_json_and_cel_defaults_before_identity_derivation() {
        let mut catalog = catalog_with(vec![schema_with_default_id()]);
        let row = TransactionWriteRow {
            entity_id: None,
            schema_key: "normalization_schema".to_string(),
            schema_version: "1".to_string(),
            snapshot: Some(snapshot_json(r#"{}"#)),
            ..base_stage_row()
        };

        let row =
            normalize_transaction_write_row(row, &mut catalog, functions()).expect("normalize row");
        let snapshot = normalized_snapshot(&row);

        assert_eq!(
            row.row.entity_id.as_ref(),
            Some(&crate::entity_identity::EntityIdentity::single(
                "uuid-default"
            ))
        );
        assert_eq!(snapshot["id"], "uuid-default");
        assert_eq!(snapshot["value"], "literal-default");
    }

    #[test]
    fn normalization_applies_cel_defaults_from_snapshot_context() {
        let mut catalog = catalog_with(vec![schema_with_cel_field_default()]);
        let row = TransactionWriteRow {
            entity_id: None,
            schema_key: "cel_field_default_schema".to_string(),
            schema_version: "1".to_string(),
            snapshot: Some(snapshot_json(r#"{"id":"entity-1","name":"Sample"}"#)),
            ..base_stage_row()
        };

        let row =
            normalize_transaction_write_row(row, &mut catalog, functions()).expect("normalize row");
        let snapshot = normalized_snapshot(&row);

        assert_eq!(snapshot["slug"], "Sample-slug");
    }

    #[test]
    fn normalization_x_lix_default_overrides_json_default() {
        let mut catalog = catalog_with(vec![schema_with_overridden_default()]);
        let row = TransactionWriteRow {
            entity_id: None,
            schema_key: "overridden_default_schema".to_string(),
            schema_version: "1".to_string(),
            snapshot: Some(snapshot_json(r#"{"id":"entity-1"}"#)),
            ..base_stage_row()
        };

        let row =
            normalize_transaction_write_row(row, &mut catalog, functions()).expect("normalize row");
        let snapshot = normalized_snapshot(&row);

        assert_eq!(snapshot["status"], "computed");
    }

    #[test]
    fn normalization_does_not_overwrite_explicit_null_with_default() {
        let mut catalog = catalog_with(vec![schema_with_nullable_default()]);
        let row = TransactionWriteRow {
            entity_id: None,
            schema_key: "nullable_default_schema".to_string(),
            schema_version: "1".to_string(),
            snapshot: Some(snapshot_json(r#"{"id":"entity-1","status":null}"#)),
            ..base_stage_row()
        };

        let row =
            normalize_transaction_write_row(row, &mut catalog, functions()).expect("normalize row");
        let snapshot = normalized_snapshot(&row);

        assert_eq!(snapshot["status"], JsonValue::Null);
    }

    #[test]
    fn normalization_applies_timestamp_function_default() {
        let mut catalog = catalog_with(vec![schema_with_timestamp_default()]);
        let row = TransactionWriteRow {
            entity_id: None,
            schema_key: "timestamp_default_schema".to_string(),
            schema_version: "1".to_string(),
            snapshot: Some(snapshot_json(r#"{"id":"entity-1"}"#)),
            ..base_stage_row()
        };

        let row =
            normalize_transaction_write_row(row, &mut catalog, functions()).expect("normalize row");
        let snapshot = normalized_snapshot(&row);

        assert_eq!(snapshot["created_at"], "1970-01-01T00:00:00.000Z");
    }

    #[test]
    fn normalization_surfaces_cel_default_errors() {
        let mut catalog = catalog_with(vec![schema_with_unknown_cel_default()]);
        let row = TransactionWriteRow {
            entity_id: None,
            schema_key: "unknown_cel_default_schema".to_string(),
            schema_version: "1".to_string(),
            snapshot: Some(snapshot_json(r#"{"id":"entity-1"}"#)),
            ..base_stage_row()
        };

        let error = normalize_transaction_write_row(row, &mut catalog, functions())
            .expect_err("default should fail");

        assert!(error.message.contains("failed to evaluate x-lix-default"));
        assert!(error.message.contains("unknown_cel_default_schema.slug"));
    }

    #[test]
    fn normalization_rejects_entity_id_that_disagrees_with_primary_key() {
        let mut catalog = catalog_with(vec![schema_with_default_id()]);
        let row = TransactionWriteRow {
            entity_id: Some(crate::entity_identity::EntityIdentity::single("wrong-id")),
            schema_key: "normalization_schema".to_string(),
            schema_version: "1".to_string(),
            snapshot: Some(snapshot_json(r#"{"id":"right-id","value":"hello"}"#)),
            ..base_stage_row()
        };

        let error = normalize_transaction_write_row(row, &mut catalog, functions())
            .expect_err("id mismatch fails");

        assert!(error
            .message
            .contains("does not match x-lix-primary-key derived entity_id"));
    }

    #[test]
    fn normalization_derives_opaque_entity_id_for_composite_primary_key() {
        let mut catalog = catalog_with(vec![composite_key_schema()]);
        let row = TransactionWriteRow {
            entity_id: None,
            schema_key: "composite_key_schema".to_string(),
            schema_version: "1".to_string(),
            snapshot: Some(snapshot_json(r#"{"namespace":"a~b","key":"1"}"#)),
            ..base_stage_row()
        };

        let row =
            normalize_transaction_write_row(row, &mut catalog, functions()).expect("normalize row");
        let entity_id = row.row.entity_id.expect("composite entity id");
        let projected_entity_id = entity_id.as_string().expect("entity id should project");

        assert!(projected_entity_id.starts_with("pk:v1:"));
        assert_ne!(projected_entity_id, "a~b~1");
    }

    #[test]
    fn normalization_validates_explicit_composite_entity_id_against_projection() {
        let mut catalog = catalog_with(vec![composite_key_schema()]);
        let snapshot = json!({
            "namespace": "a~b",
            "key": "1",
        });
        let derived = EntityIdentity::from_primary_key_paths(
            &snapshot,
            &[vec!["namespace".to_string()], vec!["key".to_string()]],
        )
        .expect("identity should derive");
        let row = TransactionWriteRow {
            entity_id: Some(derived.clone()),
            schema_key: "composite_key_schema".to_string(),
            schema_version: "1".to_string(),
            snapshot: Some(transaction_json(snapshot.clone())),
            ..base_stage_row()
        };

        let row =
            normalize_transaction_write_row(row, &mut catalog, functions()).expect("normalize row");

        assert_eq!(row.row.entity_id.as_ref(), Some(&derived));
    }

    #[test]
    fn normalization_makes_pending_registered_schema_visible_to_later_rows() {
        let mut catalog = catalog_with(vec![seed_schema_definition(REGISTERED_SCHEMA_KEY)
            .expect("registered schema builtin")
            .clone()]);
        let registered = TransactionWriteRow {
            entity_id: None,
            schema_key: REGISTERED_SCHEMA_KEY.to_string(),
            schema_version: "1".to_string(),
            snapshot: Some(transaction_json(json!({
                "value": dynamic_schema_definition(),
            }))),
            ..base_stage_row()
        };

        normalize_transaction_write_row(registered, &mut catalog, functions())
            .expect("register schema");

        let dynamic = TransactionWriteRow {
            entity_id: None,
            schema_key: "dynamic_schema".to_string(),
            schema_version: "1".to_string(),
            snapshot: Some(snapshot_json(r#"{"id":"dynamic-1"}"#)),
            ..base_stage_row()
        };
        let dynamic = normalize_transaction_write_row(dynamic, &mut catalog, functions())
            .expect("dynamic row");

        assert_eq!(
            dynamic.row.entity_id.as_ref(),
            Some(&crate::entity_identity::EntityIdentity::single("dynamic-1"))
        );
    }

    #[test]
    fn normalization_canonicalizes_filesystem_descriptor_segments() {
        let mut catalog = catalog_with(vec![
            builtin_schema(FILE_DESCRIPTOR_SCHEMA_KEY),
            builtin_schema(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
        ]);

        let file = TransactionWriteRow {
            entity_id: None,
            schema_key: FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
            snapshot: Some(transaction_json(json!({
                "id": "file-cafe",
                "directory_id": null,
                "name": "Cafe\u{301}.txt",
            }))),
            global: false,
            ..base_stage_row()
        };
        let file = normalize_transaction_write_row(file, &mut catalog, functions())
            .expect("normalize file");
        let file_snapshot = normalized_snapshot(&file);
        assert_eq!(file_snapshot["name"], "Café.txt");

        let directory = TransactionWriteRow {
            entity_id: None,
            schema_key: DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string(),
            snapshot: Some(transaction_json(json!({
                "id": "dir-cafe",
                "parent_id": null,
                "name": "Cafe\u{301}",
            }))),
            global: false,
            ..base_stage_row()
        };
        let directory = normalize_transaction_write_row(directory, &mut catalog, functions())
            .expect("normalize directory");
        let directory_snapshot = normalized_snapshot(&directory);
        assert_eq!(directory_snapshot["name"], "Café");
    }

    #[test]
    fn normalization_rejects_invalid_filesystem_descriptor_segments() {
        let mut catalog = catalog_with(vec![
            builtin_schema(FILE_DESCRIPTOR_SCHEMA_KEY),
            builtin_schema(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
        ]);

        let dot_segment = normalize_transaction_write_row(
            TransactionWriteRow {
                entity_id: None,
                schema_key: FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
                snapshot: Some(transaction_json(json!({
                    "id": "file-dotdot",
                    "directory_id": null,
                    "name": "..",
                }))),
                global: false,
                ..base_stage_row()
            },
            &mut catalog,
            functions(),
        )
        .expect_err("file descriptor name should reject dot segments");
        assert_eq!(dot_segment.code, "LIX_ERROR_PATH_DOT_SEGMENT");

        let bidi = normalize_transaction_write_row(
            TransactionWriteRow {
                entity_id: None,
                schema_key: FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
                snapshot: Some(transaction_json(json!({
                    "id": "file-bidi",
                    "directory_id": null,
                    "name": "safe\u{202E}txt",
                }))),
                global: false,
                ..base_stage_row()
            },
            &mut catalog,
            functions(),
        )
        .expect_err("file descriptor name should reject bidi formatting characters");
        assert_eq!(bidi.code, "LIX_ERROR_PATH_INVALID_SEGMENT_CODE_POINT");

        let zero_width = normalize_transaction_write_row(
            TransactionWriteRow {
                entity_id: None,
                schema_key: DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string(),
                snapshot: Some(transaction_json(json!({
                    "id": "dir-zero-width",
                    "parent_id": null,
                    "name": "zero\u{200D}width",
                }))),
                global: false,
                ..base_stage_row()
            },
            &mut catalog,
            functions(),
        )
        .expect_err("directory descriptor name should reject zero-width characters");
        assert_eq!(zero_width.code, "LIX_ERROR_PATH_INVALID_SEGMENT_CODE_POINT");
    }

    #[test]
    fn normalization_keeps_file_descriptor_name_opaque() {
        let mut catalog = catalog_with(vec![builtin_schema(FILE_DESCRIPTOR_SCHEMA_KEY)]);

        let row = normalize_transaction_write_row(
            TransactionWriteRow {
                entity_id: None,
                schema_key: FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
                snapshot: Some(transaction_json(json!({
                    "id": "file-opaque-name",
                    "directory_id": null,
                    "name": "foo.bar",
                }))),
                global: false,
                ..base_stage_row()
            },
            &mut catalog,
            functions(),
        )
        .expect("file descriptor name should be an opaque basename");

        let snapshot = normalized_snapshot(&row);
        assert_eq!(snapshot["name"], "foo.bar");
    }

    fn normalized_snapshot(row: &NormalizedTransactionWriteRow) -> &JsonValue {
        row.snapshot
            .as_ref()
            .expect("normalized test row should have a snapshot")
            .value()
    }

    fn catalog_with(schemas: Vec<JsonValue>) -> TransactionSchemaCatalog {
        TransactionSchemaCatalog::from_visible_schemas(&schemas).expect("catalog")
    }

    fn builtin_schema(schema_key: &str) -> JsonValue {
        seed_schema_definition(schema_key)
            .unwrap_or_else(|| panic!("{schema_key} builtin schema should exist"))
            .clone()
    }

    fn transaction_json(value: JsonValue) -> TransactionJson {
        TransactionJson::from_value_for_test(value)
    }

    fn snapshot_json(value: &str) -> TransactionJson {
        transaction_json(serde_json::from_str(value).expect("test snapshot should parse"))
    }

    fn base_stage_row() -> TransactionWriteRow {
        TransactionWriteRow {
            entity_id: Some(crate::entity_identity::EntityIdentity::single("entity-1")),
            schema_key: "normalization_schema".to_string(),
            file_id: None,
            snapshot: Some(snapshot_json(r#"{"id":"entity-1","value":"hello"}"#)),
            metadata: None,
            origin: None,
            schema_version: "1".to_string(),
            created_at: None,
            updated_at: None,
            global: true,
            change_id: None,
            commit_id: None,
            untracked: false,
            version_id: crate::GLOBAL_VERSION_ID.to_string(),
        }
    }

    fn schema_with_default_id() -> JsonValue {
        json!({
            "x-lix-key": "normalization_schema",
            "x-lix-version": "1",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string", "x-lix-default": "lix_uuid_v7()" },
                "value": { "type": "string", "default": "literal-default" }
            },
            "required": ["id", "value"],
            "additionalProperties": false
        })
    }

    fn schema_with_cel_field_default() -> JsonValue {
        json!({
            "x-lix-key": "cel_field_default_schema",
            "x-lix-version": "1",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "name": { "type": "string" },
                "slug": { "type": "string", "x-lix-default": "name + '-slug'" }
            },
            "required": ["id", "name"],
            "additionalProperties": false
        })
    }

    fn schema_with_overridden_default() -> JsonValue {
        json!({
            "x-lix-key": "overridden_default_schema",
            "x-lix-version": "1",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "status": {
                    "type": "string",
                    "default": "literal",
                    "x-lix-default": "'computed'"
                }
            },
            "required": ["id"],
            "additionalProperties": false
        })
    }

    fn schema_with_nullable_default() -> JsonValue {
        json!({
            "x-lix-key": "nullable_default_schema",
            "x-lix-version": "1",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "status": {
                    "anyOf": [{ "type": "string" }, { "type": "null" }],
                    "x-lix-default": "'computed'"
                }
            },
            "required": ["id"],
            "additionalProperties": false
        })
    }

    fn schema_with_timestamp_default() -> JsonValue {
        json!({
            "x-lix-key": "timestamp_default_schema",
            "x-lix-version": "1",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "created_at": { "type": "string", "x-lix-default": "lix_timestamp()" }
            },
            "required": ["id"],
            "additionalProperties": false
        })
    }

    fn schema_with_unknown_cel_default() -> JsonValue {
        json!({
            "x-lix-key": "unknown_cel_default_schema",
            "x-lix-version": "1",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "slug": { "type": "string", "x-lix-default": "missing_var + '-slug'" }
            },
            "required": ["id"],
            "additionalProperties": false
        })
    }

    fn composite_key_schema() -> JsonValue {
        json!({
            "x-lix-key": "composite_key_schema",
            "x-lix-version": "1",
            "x-lix-primary-key": ["/namespace", "/key"],
            "type": "object",
            "properties": {
                "namespace": { "type": "string" },
                "key": { "type": "string" }
            },
            "required": ["namespace", "key"],
            "additionalProperties": false
        })
    }

    fn dynamic_schema_definition() -> JsonValue {
        json!({
            "x-lix-key": "dynamic_schema",
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

    fn functions() -> FunctionProviderHandle {
        SharedFunctionProvider::new(Box::new(FixedFunctions) as Box<dyn FunctionProvider + Send>)
    }

    struct FixedFunctions;

    impl FunctionProvider for FixedFunctions {
        fn uuid_v7(&mut self) -> String {
            "uuid-default".to_string()
        }

        fn timestamp(&mut self) -> String {
            "1970-01-01T00:00:00.000Z".to_string()
        }
    }
}
