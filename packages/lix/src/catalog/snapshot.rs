use std::{
    collections::BTreeMap,
    sync::{Arc, OnceLock},
};

use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::LixError;
use crate::common::format_json_pointer;
use crate::domain::{Domain, DomainSchemaIdentity};
use crate::functions::FunctionProviderHandle;
use crate::row_pk::canonical_json_text;
use crate::schema::{SchemaKey, compile_lix_schema, validate_schema_amendment};

#[derive(Default)]
pub(crate) struct CatalogSnapshot {
    entries: Vec<CatalogEntry>,
    plans: Vec<SchemaPlan>,
    by_key: BTreeMap<SchemaCatalogKey, SchemaPlanId>,
    by_identity: BTreeMap<DomainSchemaIdentity, SchemaPlanId>,
    delete_references_by_target: BTreeMap<SchemaCatalogKey, Vec<DeleteReferencePlan>>,
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

#[expect(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for CatalogSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogSnapshot")
            .field("plan_count", &self.plans.len())
            .field("keys", &self.by_key.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl CatalogSnapshot {
    pub(crate) fn from_visible_schemas(visible_schemas: &[JsonValue]) -> Result<Self, LixError> {
        let mut catalog = Self::default();
        for schema in visible_schemas {
            let schema = schema.clone();
            let key = crate::schema::schema_key_from_definition(&schema)?;
            let catalog_key = SchemaCatalogKey::from_schema_key(key);
            let identity = DomainSchemaIdentity::new(
                Domain::schema_catalog(crate::GLOBAL_BRANCH_ID, true),
                catalog_key.schema_key.clone(),
            );
            catalog.remember_schema_identity(identity, catalog_key, schema)?;
        }
        catalog.rebuild_plans()?;
        Ok(catalog)
    }

    /// Compiled catalog for schemas shipped with the engine.
    ///
    /// These schemas are engine authority, not repository authority. Every
    /// transaction catalog starts with this immutable base; persisted
    /// `lix_registered_schema` rows for the same keys are only discoverability
    /// projections retained in repository history.
    pub(crate) fn builtin() -> &'static Self {
        Self::builtin_arc().as_ref()
    }

    pub(crate) fn builtin_shared() -> Arc<Self> {
        Arc::clone(Self::builtin_arc())
    }

    fn builtin_arc() -> &'static Arc<Self> {
        static BUILTIN: OnceLock<Arc<CatalogSnapshot>> = OnceLock::new();
        BUILTIN.get_or_init(|| {
            let schemas = crate::schema::seed_schema_definitions()
                .into_iter()
                .cloned()
                .collect::<Vec<_>>();
            Arc::new(
                Self::from_visible_schemas(&schemas)
                    .expect("embedded Schema v1 definitions must compile as one catalog"),
            )
        })
    }

    pub(crate) fn from_schema_facts(facts: &[SchemaCatalogFact]) -> Result<Self, LixError> {
        let mut entries = Self::builtin().entries.clone();
        entries.extend(facts.iter().filter_map(|fact| {
            // Repository history can retain the old bootstrap projections, but
            // it can no longer make an engine schema appear, disappear, or
            // change definition. Runtime registration already reserves every
            // `lix_*` key; filtering all known built-ins here also lets the
            // migration chain read historical definitions while an older
            // schema amendment is still being upgraded.
            crate::schema::seed_schema_definition(&fact.catalog_key.schema_key)
                .is_none()
                .then(|| CatalogEntry {
                    identity: fact.identity.clone(),
                    key: fact.catalog_key.clone(),
                    schema: fact.schema.clone(),
                })
        }));
        Self::from_entries(entries)
    }

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
        // Registration rows are deliberately made visible before the rest of
        // their transaction is normalized, then encountered again in normal
        // row order. Avoid rebuilding and recompiling the entire catalog for
        // that exact replay. Amendments still take the atomic candidate path
        // below.
        if let Some(existing) = self.by_identity.get(&identity).copied() {
            let existing_entry = &self.entries[existing.index()];
            if existing_entry.key == key && existing_entry.schema == schema {
                return Ok(existing);
            }
        }
        let (candidate, plan_id) = self.with_inserted_schema(identity, key, schema)?;
        *self = candidate;
        Ok(plan_id)
    }

    /// Builds one atomic candidate without recompiling the already-valid
    /// source catalog. Entry ordinals and both lookup maps are stable across
    /// the clone; only the final candidate needs fresh compiled plans.
    fn with_inserted_schema(
        &self,
        identity: DomainSchemaIdentity,
        key: SchemaCatalogKey,
        schema: JsonValue,
    ) -> Result<(Self, SchemaPlanId), LixError> {
        let mut candidate = Self {
            entries: self.entries.clone(),
            plans: Vec::new(),
            by_key: self.by_key.clone(),
            by_identity: self.by_identity.clone(),
            delete_references_by_target: BTreeMap::new(),
            fingerprint: CatalogFingerprint::default(),
        };
        let plan_id = candidate.remember_schema_identity(identity, key, schema)?;
        candidate.rebuild_plans()?;
        Ok((candidate, plan_id))
    }

    fn from_entries(entries: Vec<CatalogEntry>) -> Result<Self, LixError> {
        let mut catalog = Self::default();
        for entry in entries {
            catalog.remember_schema_identity(entry.identity, entry.key, entry.schema)?;
        }
        catalog.rebuild_plans()?;
        Ok(catalog)
    }

    #[expect(clippy::cast_possible_truncation)]
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
                format!(
                    "schema '{}' is already registered with a different definition in the same schema domain",
                    key.schema_key
                ),
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
        }
        self.delete_references_by_target = delete_references_by_target;
    }

    fn compute_fingerprint(&self) -> Result<CatalogFingerprint, LixError> {
        let mut hasher = blake3::Hasher::new();
        let mut entries = self.entries.iter().collect::<Vec<_>>();
        entries.sort_by(|left, right| left.identity.cmp(&right.identity));
        for entry in entries {
            hash_catalog_fact(
                &mut hasher,
                &entry.identity,
                &entry.key.schema_key,
                &entry.schema,
            )?;
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

    /// Returns the schema definitions represented by this compiled snapshot.
    ///
    /// SQL surface binding needs the authoritative catalog snapshot captured
    /// when a transaction opens. Project it from that snapshot instead of
    /// rescanning durable schema rows through live state.
    pub(crate) fn schema_jsons(&self) -> Vec<JsonValue> {
        self.by_key
            .values()
            .map(|plan_id| self.plans[plan_id.index()].schema.as_ref().clone())
            .collect()
    }

    pub(crate) fn plan(&self, plan_id: SchemaPlanId) -> Option<&SchemaPlan> {
        self.plans.get(plan_id.index())
    }

    pub(crate) fn plan_for_key(&self, schema_key: &str) -> Option<(SchemaPlanId, &SchemaPlan)> {
        let plan_id = *self.by_key.get(schema_key)?;
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
        }
    }
}

pub(super) fn hash_fingerprint_part(hasher: &mut blake3::Hasher, value: &str) {
    hasher.update(&(value.len() as u64).to_le_bytes());
    hasher.update(value.as_bytes());
}

/// Hashes one catalog fact as three length-prefixed parts.
///
/// `CatalogSnapshot::compute_fingerprint` and `fingerprint_schema_facts` must
/// hash the identical part stream: the facts fingerprint keys the compiled
/// snapshot cache, so any drift between the two would silently split or merge
/// cache entries. The schema key is hashed as its own part even though the
/// identity component embeds it; the standalone part keeps the stream
/// injective regardless of separator characters inside identity fields.
fn hash_catalog_fact(
    hasher: &mut blake3::Hasher,
    identity: &DomainSchemaIdentity,
    schema_key: &str,
    schema: &JsonValue,
) -> Result<(), LixError> {
    hash_fingerprint_part(hasher, &identity.fingerprint_component());
    hash_fingerprint_part(hasher, schema_key);
    let canonical_schema = canonical_json_text(schema).map_err(|error| {
        LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            format!("failed to canonicalize schema for catalog fingerprint: {error}"),
        )
    })?;
    hash_fingerprint_part(hasher, &canonical_schema);
    Ok(())
}

/// Content fingerprint of raw schema facts, before any snapshot is built.
///
/// Identical fact sets always produce the same fingerprint, so it can key a
/// cache of compiled snapshots without an invalidation protocol.
pub(crate) fn fingerprint_schema_facts(
    facts: &[SchemaCatalogFact],
) -> Result<CatalogFingerprint, LixError> {
    let mut hasher = blake3::Hasher::new();
    let mut effective_facts = CatalogSnapshot::builtin()
        .entries
        .iter()
        .map(|entry| (&entry.identity, &entry.key.schema_key, &entry.schema))
        .chain(facts.iter().filter_map(|fact| {
            crate::schema::seed_schema_definition(&fact.catalog_key.schema_key)
                .is_none()
                .then_some((&fact.identity, &fact.catalog_key.schema_key, &fact.schema))
        }))
        .collect::<Vec<_>>();
    effective_facts.sort_by(|left, right| left.0.cmp(right.0));
    for (identity, schema_key, schema) in effective_facts {
        hash_catalog_fact(&mut hasher, identity, schema_key, schema)?;
    }
    Ok(CatalogFingerprint(hasher.finalize().to_hex().to_string()))
}

/// Copy-on-write catalog handle for one transaction schema scope.
///
/// Transactions normally share an immutable compiled snapshot from the
/// engine-wide cache. Registering a schema inside the transaction switches the
/// handle to a private rebuilt snapshot, so pending registrations are never
/// observable outside the transaction that staged them.
pub(crate) enum TransactionCatalog {
    Shared(Arc<CatalogSnapshot>),
    Owned(CatalogSnapshot),
}

impl TransactionCatalog {
    pub(crate) fn snapshot(&self) -> &CatalogSnapshot {
        match self {
            Self::Shared(snapshot) => snapshot,
            Self::Owned(snapshot) => snapshot,
        }
    }

    pub(crate) fn insert_schema_for_domain(
        &mut self,
        domain: Domain,
        key: SchemaKey,
        schema: JsonValue,
    ) -> Result<SchemaPlanId, LixError> {
        match self {
            Self::Shared(snapshot) => {
                let catalog_key = SchemaCatalogKey::from_schema_key(key);
                let identity = DomainSchemaIdentity::new(domain, catalog_key.schema_key.clone());
                let (candidate, plan_id) =
                    snapshot.with_inserted_schema(identity, catalog_key, schema)?;
                *self = Self::Owned(candidate);
                Ok(plan_id)
            }
            Self::Owned(snapshot) => snapshot.insert_schema_for_domain(domain, key, schema),
        }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct SchemaPlanFingerprint([u8; 32]);

impl SchemaPlanFingerprint {
    pub(crate) fn matches_bytes(&self, bytes: &[u8; 32]) -> bool {
        &self.0 == bytes
    }

    pub(crate) fn bytes(self) -> [u8; 32] {
        self.0
    }
}

pub(crate) type PointerGroup = Vec<Vec<String>>;

pub(crate) struct SchemaPlan {
    pub(crate) key: SchemaCatalogKey,
    pub(crate) schema: Arc<JsonValue>,
    fingerprint: Arc<SchemaPlanFingerprint>,
    pub(crate) compiled_schema: lix_schema::CompiledSchema,
    fast_object_validation: Option<FastObjectValidationPlan>,
    pub(crate) defaults: DefaultPlan,
    pub(crate) primary_key: Option<PointerGroup>,
    pub(crate) primary_key_component_types: Option<Vec<crate::row_pk::RowPkComponentType>>,
    pub(crate) uniques: Vec<PointerGroup>,
    pub(crate) foreign_keys: Vec<ForeignKeyPlan>,
}

impl SchemaPlan {
    pub(crate) fn fingerprint(&self) -> &SchemaPlanFingerprint {
        self.fingerprint.as_ref()
    }

    pub(crate) fn accepts_row_content_fast(&self, value: &JsonValue) -> bool {
        self.fast_object_validation
            .as_ref()
            .is_some_and(|plan| plan.accepts(value))
    }

    pub(crate) fn accepts_canonical_certificate(&self) -> bool {
        self.fast_object_validation
            .as_ref()
            .is_some_and(FastObjectValidationPlan::supports_row_certificate)
            && self
                .primary_key
                .as_ref()
                .is_some_and(|paths| paths.len() <= 128 && paths.iter().all(|path| path.len() == 1))
            && self.primary_key_component_types.is_some()
            && self.uniques.is_empty()
            && self.foreign_keys.is_empty()
    }

    /// Compiles one standalone plan for tests that need the same constraint
    /// projection production uses, without building a whole catalog.
    #[cfg(test)]
    pub(crate) fn compile_standalone_for_test(
        key: SchemaCatalogKey,
        schema: JsonValue,
        key_index: &BTreeMap<SchemaCatalogKey, SchemaPlanId>,
        schema_index: &BTreeMap<SchemaCatalogKey, &JsonValue>,
    ) -> Result<Self, LixError> {
        Self::compile(key, schema, key_index, schema_index)
    }

    fn compile(
        key: SchemaCatalogKey,
        schema: JsonValue,
        key_index: &BTreeMap<SchemaCatalogKey, SchemaPlanId>,
        schema_index: &BTreeMap<SchemaCatalogKey, &JsonValue>,
    ) -> Result<Self, LixError> {
        let parsed_schema = crate::schema::parse_lix_schema(&schema)?;
        let fingerprint = Arc::new(SchemaPlanFingerprint(
            *parsed_schema
                .wire_fingerprint()
                .map_err(|error| {
                    LixError::new(
                        LixError::CODE_SCHEMA_DEFINITION,
                        format!("failed to fingerprint compiled schema plan: {error}"),
                    )
                })?
                .as_bytes(),
        ));
        let compiled_schema = compile_lix_schema(&schema)?;
        let fast_object_validation = FastObjectValidationPlan::compile_v1(&schema);
        let defaults = DefaultPlan::from_schema(&schema);
        let primary_key = primary_key_paths(&schema)?;
        let primary_key_component_types = primary_key
            .as_ref()
            .map(|paths| primary_key_component_types(&schema, paths))
            .transpose()?;
        let uniques = pointer_groups(&schema, "x-lix-unique")?;
        let foreign_keys = bind_foreign_key_plans(
            &key,
            &schema,
            foreign_key_plans(&schema)?,
            key_index,
            schema_index,
        )?;
        Ok(Self {
            key,
            schema: Arc::new(schema),
            fingerprint,
            compiled_schema,
            fast_object_validation,
            defaults,
            primary_key,
            primary_key_component_types,
            uniques,
            foreign_keys,
        })
    }
}

fn primary_key_component_types(
    schema: &JsonValue,
    paths: &[Vec<String>],
) -> Result<Vec<crate::row_pk::RowPkComponentType>, LixError> {
    let schema = crate::schema::parse_lix_schema(schema)?;
    paths
        .iter()
        .enumerate()
        .map(|(index, path)| {
            let [name] = path.as_slice() else {
                return Err(LixError::new(
                    LixError::CODE_SCHEMA_DEFINITION,
                    format!("primary-key path at index {index} must name one column"),
                ));
            };
            let column = schema
                .columns
                .iter()
                .find(|column| &column.name == name)
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_SCHEMA_DEFINITION,
                        format!("primary-key column '{name}' does not exist"),
                    )
                })?;
            match column.data_type {
                lix_schema::DataType::Int8 => Ok(crate::row_pk::RowPkComponentType::Integer),
                lix_schema::DataType::Uuid => Ok(crate::row_pk::RowPkComponentType::Uuid),
                lix_schema::DataType::Text => Ok(crate::row_pk::RowPkComponentType::String),
                _ => Err(LixError::new(
                    LixError::CODE_SCHEMA_DEFINITION,
                    format!("primary-key column at index {index} must be bigint, text, or uuid"),
                )),
            }
        })
        .collect()
}

#[derive(Debug)]
struct FastObjectValidationPlan {
    properties: BTreeMap<String, FastValueValidation>,
    required: Vec<String>,
    additional_properties: bool,
    min_properties: usize,
}

impl FastObjectValidationPlan {
    fn compile_v1(schema: &JsonValue) -> Option<Self> {
        let schema = crate::schema::parse_lix_schema(schema).ok()?;
        let mut properties = BTreeMap::new();
        let mut required = Vec::new();
        for column in schema.columns {
            let nullable = column.nullable;
            let validation = match column.data_type {
                lix_schema::DataType::Text => FastValueValidation::Types(FastJsonTypes(
                    FastJsonTypes::STRING | if nullable { FastJsonTypes::NULL } else { 0 },
                )),
                lix_schema::DataType::Uuid => {
                    let validation = FastStringValidation::Uuid;
                    if nullable {
                        FastValueValidation::StringOrNull(validation)
                    } else {
                        FastValueValidation::String(validation)
                    }
                }
                lix_schema::DataType::Int8 => FastValueValidation::Types(FastJsonTypes(
                    FastJsonTypes::INTEGER | if nullable { FastJsonTypes::NULL } else { 0 },
                )),
                lix_schema::DataType::Float8 => FastValueValidation::Types(FastJsonTypes(
                    FastJsonTypes::NUMBER | if nullable { FastJsonTypes::NULL } else { 0 },
                )),
                lix_schema::DataType::Boolean => FastValueValidation::Types(FastJsonTypes(
                    FastJsonTypes::BOOLEAN | if nullable { FastJsonTypes::NULL } else { 0 },
                )),
                lix_schema::DataType::Jsonb => FastValueValidation::Types(FastJsonTypes::ANY),
                lix_schema::DataType::Timestamptz => {
                    let validation = FastStringValidation::Timestamptz;
                    if nullable {
                        FastValueValidation::StringOrNull(validation)
                    } else {
                        FastValueValidation::String(validation)
                    }
                }
            };
            if !nullable && column.default_value.is_none() && column.default_expression.is_none() {
                required.push(column.name.clone());
            }
            properties.insert(column.name, validation);
        }
        Some(Self {
            properties,
            required,
            additional_properties: false,
            min_properties: 0,
        })
    }

    fn accepts(&self, value: &JsonValue) -> bool {
        let Some(value) = value.as_object() else {
            return false;
        };
        if value.len() < self.min_properties {
            return false;
        }
        if self
            .required
            .iter()
            .any(|required| !value.contains_key(required))
        {
            return false;
        }
        value.iter().all(|(name, value)| {
            self.properties
                .get(name)
                .map_or(self.additional_properties, |types| types.accepts(value))
        })
    }

    fn supports_row_certificate(&self) -> bool {
        self.required.len() <= 128
    }
}

#[derive(Debug)]
enum FastValueValidation {
    Types(FastJsonTypes),
    String(FastStringValidation),
    StringOrNull(FastStringValidation),
}

impl FastValueValidation {
    fn accepts(&self, value: &JsonValue) -> bool {
        match self {
            Self::Types(types) => types.accepts(value),
            Self::String(validation) => value
                .as_str()
                .is_some_and(|value| validation.accepts(value)),
            Self::StringOrNull(validation) => {
                value.is_null()
                    || value
                        .as_str()
                        .is_some_and(|value| validation.accepts(value))
            }
        }
    }
}

#[derive(Debug)]
enum FastStringValidation {
    Uuid,
    Timestamptz,
}

impl FastStringValidation {
    fn accepts(&self, value: &str) -> bool {
        match self {
            Self::Uuid => uuid::Uuid::parse_str(value).is_ok(),
            Self::Timestamptz => chrono::DateTime::parse_from_rfc3339(value).is_ok(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct FastJsonTypes(u16);

impl FastJsonTypes {
    const NULL: u16 = 1 << 0;
    const BOOLEAN: u16 = 1 << 1;
    const NUMBER: u16 = 1 << 2;
    const INTEGER: u16 = 1 << 3;
    const STRING: u16 = 1 << 4;
    const ARRAY: u16 = 1 << 5;
    const OBJECT: u16 = 1 << 6;
    const ANY: Self = Self(
        Self::NULL
            | Self::BOOLEAN
            | Self::NUMBER
            | Self::INTEGER
            | Self::STRING
            | Self::ARRAY
            | Self::OBJECT,
    );

    fn accepts(self, value: &JsonValue) -> bool {
        let bit = match value {
            JsonValue::Null => Self::NULL,
            JsonValue::Bool(_) => Self::BOOLEAN,
            JsonValue::Number(number) if number.is_i64() || number.is_u64() => {
                Self::NUMBER | Self::INTEGER
            }
            JsonValue::Number(_) => Self::NUMBER,
            JsonValue::String(_) => Self::STRING,
            JsonValue::Array(_) => Self::ARRAY,
            JsonValue::Object(_) => Self::OBJECT,
        };
        self.0 & bit != 0
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
    UuidV7,
    CurrentTimestamp,
}

impl DefaultPlan {
    pub(crate) fn is_empty(&self) -> bool {
        self.properties.is_empty()
    }

    pub(crate) fn from_schema(schema: &JsonValue) -> Self {
        let Ok(schema) = crate::schema::parse_lix_schema(schema) else {
            return Self::default();
        };
        let properties = schema
            .columns
            .into_iter()
            .filter_map(|column| {
                if let Some(expression) = column.default_expression {
                    let default = match expression.trim() {
                        "uuidv7()" => DefaultValuePlan::UuidV7,
                        "CURRENT_TIMESTAMP" => DefaultValuePlan::CurrentTimestamp,
                        _ => unreachable!("Schema v1 rejects unsupported default expressions"),
                    };
                    return Some(DefaultPropertyPlan {
                        field_name: column.name,
                        default,
                    });
                }
                column.default_value.map(|value| DefaultPropertyPlan {
                    field_name: column.name,
                    default: DefaultValuePlan::Json(value),
                })
            })
            .collect();
        Self { properties }
    }

    pub(crate) fn apply<F>(
        &self,
        snapshot: &mut JsonMap<String, JsonValue>,
        functions: FunctionProviderHandle,
        _schema_key: &str,
        mut current_timestamp: F,
    ) -> Result<bool, LixError>
    where
        F: FnMut() -> Result<crate::common::LixTimestamp, LixError>,
    {
        let mut changed = false;
        for property in &self.properties {
            if snapshot.contains_key(&property.field_name) {
                continue;
            }
            let value = match &property.default {
                DefaultValuePlan::Json(value) => value.clone(),
                DefaultValuePlan::UuidV7 => JsonValue::String(functions.call_uuid_v7().to_string()),
                DefaultValuePlan::CurrentTimestamp => {
                    JsonValue::String(current_timestamp()?.to_string())
                }
            };
            snapshot.insert(property.field_name.clone(), value);
            changed = true;
        }
        Ok(changed)
    }

    /// Returns whether applying this plan would mutate `snapshot`.
    ///
    /// Batch-backed canonical rows use this check to keep their parsed value
    /// and normalized bytes in the transition arena when every defaulted
    /// property is already present. The uncommon missing-default case is
    /// materialized into an owned object before evaluation.
    pub(crate) fn would_apply(&self, snapshot: &JsonMap<String, JsonValue>) -> bool {
        self.properties
            .iter()
            .any(|property| !snapshot.contains_key(&property.field_name))
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

#[derive(Debug, Clone, Copy)]
pub(crate) struct DeleteValidationPlan<'a> {
    pub(crate) foreign_key_references: &'a [DeleteReferencePlan],
}

impl DeleteValidationPlan<'_> {
    pub(crate) fn has_committed_checks(self) -> bool {
        !self.foreign_key_references.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct UnboundForeignKeyPlan {
    local_properties: PointerGroup,
    referenced_schema: SchemaCatalogKey,
    referenced_properties: PointerGroup,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct SchemaCatalogKey {
    pub(crate) schema_key: String,
}

impl std::borrow::Borrow<str> for SchemaCatalogKey {
    fn borrow(&self) -> &str {
        &self.schema_key
    }
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

    #[cfg(test)]
    pub(crate) fn schema(&self) -> &JsonValue {
        &self.schema
    }

    #[cfg(test)]
    pub(crate) fn catalog_key(&self) -> &SchemaCatalogKey {
        &self.catalog_key
    }
}

fn primary_key_paths(schema: &JsonValue) -> Result<Option<Vec<Vec<String>>>, LixError> {
    let schema = crate::schema::parse_lix_schema(schema)?;
    Ok(Some(
        schema
            .primary_key
            .into_iter()
            .map(|column| vec![column])
            .collect(),
    ))
}

fn pointer_groups(schema: &JsonValue, field: &str) -> Result<Vec<PointerGroup>, LixError> {
    let schema = crate::schema::parse_lix_schema(schema)?;
    if field != "unique" && field != "x-lix-unique" {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("unsupported Schema v1 pointer group '{field}'"),
        ));
    }
    Ok(schema
        .unique
        .into_iter()
        .map(|group| group.into_iter().map(|column| vec![column]).collect())
        .collect())
}

fn foreign_key_plans(schema: &JsonValue) -> Result<Vec<UnboundForeignKeyPlan>, LixError> {
    let schema = crate::schema::parse_lix_schema(schema)?;
    Ok(schema
        .foreign_keys
        .into_iter()
        .map(|foreign_key| UnboundForeignKeyPlan {
            local_properties: foreign_key
                .columns
                .into_iter()
                .map(|column| vec![column])
                .collect(),
            referenced_schema: SchemaCatalogKey {
                schema_key: foreign_key.references.schema_key,
            },
            referenced_properties: foreign_key
                .references
                .columns
                .into_iter()
                .map(|column| vec![column])
                .collect(),
        })
        .collect())
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
                validate_foreign_key_field_types(
                    source_key,
                    source_schema,
                    &foreign_key.referenced_schema,
                    target_schema,
                    local_pointer,
                    referenced_pointer,
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

fn validate_foreign_key_field_types(
    source_key: &SchemaCatalogKey,
    source_schema: &JsonValue,
    referenced_key: &SchemaCatalogKey,
    referenced_schema: &JsonValue,
    local_pointer: &[String],
    referenced_pointer: &[String],
) -> Result<(), LixError> {
    let source = crate::schema::parse_lix_schema(source_schema)?;
    let referenced = crate::schema::parse_lix_schema(referenced_schema)?;
    let [local_name] = local_pointer else {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            "Schema v1 foreign keys require top-level columns",
        ));
    };
    let [referenced_name] = referenced_pointer else {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            "Schema v1 foreign keys require top-level columns",
        ));
    };
    let local_type = source
        .columns
        .iter()
        .find(|column| &column.name == local_name)
        .map(|column| column.data_type)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                format!("foreign key references missing local column '{local_name}'"),
            )
        })?;
    let referenced_type = referenced
        .columns
        .iter()
        .find(|column| &column.name == referenced_name)
        .map(|column| column.data_type)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                format!(
                    "foreign key references missing target column '{}.{referenced_name}'",
                    referenced_key.schema_key
                ),
            )
        })?;
    if local_type != referenced_type {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            format!(
                "foreign key on schema '{}' has incompatible types: '{}' is {}, but '{}.{}' is {}",
                source_key.schema_key,
                local_name,
                local_type.postgres_name(),
                referenced_key.schema_key,
                referenced_name,
                referenced_type.postgres_name()
            ),
        ));
    }
    Ok(())
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
    fn schema_rejects_removed_columnar_storage_policy() {
        let mut schema = crate::schema::seed_schema_definition("lix_key_value")
            .expect("key-value schema should exist")
            .clone();
        schema["x-lix-columnar"] = json!(false);
        let error = crate::schema::validate_lix_schema_definition(&schema)
            .expect_err("physical storage policy must not be part of a public schema");
        assert!(error.message.contains("unknown field `x-lix-columnar`"));
    }

    #[test]
    fn fast_object_validation_accepts_key_value_rows_and_rejects_invalid_shapes() {
        let schema = crate::schema::seed_schema_definition("lix_key_value")
            .expect("key-value schema should exist");
        let plan = SchemaPlan::compile(
            SchemaCatalogKey {
                schema_key: "lix_key_value".to_string(),
            },
            schema.clone(),
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
        .expect("key-value schema should compile");

        for value in [
            json!({"key": "a", "value": null}),
            json!({"key": "a", "value": {"nested": true}}),
            json!({"key": "a", "value": [1, 2, 3]}),
        ] {
            assert!(plan.accepts_row_content_fast(&value));
            assert!(plan.compiled_schema.is_valid(&value));
        }
        assert!(plan.accepts_row_content_fast(&json!({"key": "a"})));
        for value in [
            json!({"key": 1, "value": null}),
            json!({"key": "a", "value": null, "extra": true}),
        ] {
            assert!(!plan.accepts_row_content_fast(&value));
            assert!(!plan.compiled_schema.is_valid(&value));
        }
    }

    #[test]
    fn default_plan_compiles_postgresql_uuid_expression_without_cel() {
        let plan = DefaultPlan::from_schema(&json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "default_probe",
            "columns": [
                {"name": "id", "type": "uuid", "nullable": false, "default_expression": "uuidv7()"}
            ],
            "primary_key": ["id"]
        }));

        assert_eq!(plan.properties[0].field_name, "id");
        assert_eq!(plan.properties[0].default, DefaultValuePlan::UuidV7);
    }

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
    fn transaction_catalog_always_contains_engine_builtin_schemas() {
        let catalog = CatalogSnapshot::from_schema_facts(&[])
            .expect("an empty repository catalog should still bind engine schemas");

        for schema in crate::schema::seed_schema_definitions() {
            let key = crate::schema::schema_key_from_definition(schema)
                .expect("embedded schema should have a key");
            assert!(catalog.contains(&key.schema_key), "{}", key.schema_key);
        }
    }

    #[test]
    fn persisted_builtin_projection_cannot_override_engine_authority() {
        let mut stored = crate::schema::seed_schema_definition("lix_file_descriptor")
            .expect("file descriptor schema should be embedded")
            .clone();
        stored["description"] = json!("repository-controlled definition");
        let custom = SchemaCatalogFact::new(
            Domain::schema_catalog("main", false),
            SchemaKey::new("acme_note"),
            schema_json("acme_note"),
        );
        let catalog_without_projection = CatalogSnapshot::from_schema_facts(&[custom.clone()])
            .expect("custom catalog should bind");
        let catalog = CatalogSnapshot::from_schema_facts(&[
            SchemaCatalogFact::new(
                Domain::schema_catalog("main", false),
                SchemaKey::new("lix_file_descriptor"),
                stored,
            ),
            custom,
        ])
        .expect("persisted built-in projections should be ignored as catalog authority");

        assert_eq!(
            catalog.schema("lix_file_descriptor"),
            crate::schema::seed_schema_definition("lix_file_descriptor")
        );
        assert!(catalog.contains("acme_note"));
        assert_eq!(
            catalog.fingerprint(),
            catalog_without_projection.fingerprint()
        );
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
    fn facts_fingerprint_matches_built_snapshot_fingerprint() {
        let facts = vec![
            SchemaCatalogFact::new(
                Domain::schema_catalog("main", false),
                SchemaKey::new("parent_schema"),
                schema_json("parent_schema"),
            ),
            SchemaCatalogFact::new(
                Domain::schema_catalog("main", false),
                SchemaKey::new("child_schema"),
                child_schema_json("child_schema", "parent_schema"),
            ),
        ];

        let facts_fingerprint =
            fingerprint_schema_facts(&facts).expect("facts fingerprint should hash");
        let snapshot = CatalogSnapshot::from_schema_facts(&facts).expect("catalog should bind");

        assert_eq!(
            &facts_fingerprint,
            snapshot.fingerprint(),
            "cache key and snapshot fingerprint must use the same hashing scheme"
        );
    }

    #[test]
    fn transaction_catalog_copy_on_write_isolates_shared_snapshot() {
        let shared = Arc::new(
            CatalogSnapshot::from_schema_facts(&[SchemaCatalogFact::new(
                Domain::schema_catalog("main", false),
                SchemaKey::new("base_schema"),
                schema_json("base_schema"),
            )])
            .expect("base catalog should bind"),
        );
        let (base_plan_id, _) = shared
            .plan_for_key("base_schema")
            .expect("base schema plan should exist");
        let mut handle = TransactionCatalog::Shared(Arc::clone(&shared));

        handle
            .insert_schema_for_domain(
                Domain::schema_catalog("main", false),
                SchemaKey::new("registered_schema"),
                schema_json("registered_schema"),
            )
            .expect("registration should rebuild an owned catalog");

        assert!(matches!(handle, TransactionCatalog::Owned(_)));
        assert!(handle.snapshot().contains("registered_schema"));
        assert!(
            !shared.contains("registered_schema"),
            "pending registrations must not mutate the shared snapshot"
        );
        let (rebuilt_plan_id, _) = handle
            .snapshot()
            .plan_for_key("base_schema")
            .expect("base schema plan should survive the rebuild");
        assert_eq!(
            base_plan_id, rebuilt_plan_id,
            "plan ids issued by the shared snapshot must stay valid after copy-on-write"
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
    fn schema_jsons_project_compiled_catalog_in_schema_key_order() {
        let zeta = schema_json("zeta_schema");
        let alpha = schema_json("alpha_schema");
        let catalog = CatalogSnapshot::from_schema_facts(&[
            SchemaCatalogFact::new(
                Domain::schema_catalog("main", false),
                SchemaKey::new("zeta_schema"),
                zeta.clone(),
            ),
            SchemaCatalogFact::new(
                Domain::schema_catalog("main", false),
                SchemaKey::new("alpha_schema"),
                alpha.clone(),
            ),
        ])
        .expect("catalog should bind");

        let mut expected = crate::schema::seed_schema_definitions()
            .into_iter()
            .cloned()
            .chain([alpha, zeta])
            .collect::<Vec<_>>();
        expected.sort_by_key(|schema| {
            crate::schema::schema_key_from_definition(schema)
                .expect("test schema should have a key")
                .schema_key
        });
        assert_eq!(catalog.schema_jsons(), expected);
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

    fn schema_json(schema_key: &str) -> JsonValue {
        json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": schema_key,
            "columns": [{ "name": "id", "type": "text", "nullable": false }],
            "primary_key": ["id"]
        })
    }

    fn child_schema_json(schema_key: &str, parent_schema_key: &str) -> JsonValue {
        json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": schema_key,
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "parent_id", "type": "text", "nullable": false }
            ],
            "primary_key": ["id"],
            "foreign_keys": [{
                "columns": ["parent_id"],
                "references": {
                    "schema_key": parent_schema_key,
                    "columns": ["id"]
                }
            }]
        })
    }
}
