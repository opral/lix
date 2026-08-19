use std::{cmp::Ordering, collections::BTreeMap, sync::Arc};

use serde_json::{Map as JsonMap, Value as JsonValue};
use smallvec::SmallVec;

use crate::LixError;
use crate::common::format_json_pointer;
use crate::domain::{Domain, DomainSchemaIdentity};
use crate::functions::FunctionProviderHandle;
use crate::row_pk::{RowPk, canonical_json_text};
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
    #[cfg(test)]
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

    /// Rebuilds an owned snapshot with the same entries.
    ///
    /// Compiled plans are not clonable, so a transaction that needs a private
    /// mutable catalog recompiles from the recorded entries. Entry order is
    /// preserved, so `SchemaPlanId`s issued by the source snapshot remain
    /// valid against the rebuilt one.
    pub(crate) fn rebuild_owned(&self) -> Result<Self, LixError> {
        Self::from_entries(self.entries.clone())
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
        let mut entries = self.entries.clone();
        let mut candidate = Self::from_entries(entries.clone())?;
        let plan_id = candidate.remember_schema_identity(identity.clone(), key, schema)?;
        entries.clone_from(&candidate.entries);
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
    let mut facts = facts.iter().collect::<Vec<_>>();
    facts.sort_by(|left, right| left.identity.cmp(&right.identity));
    for fact in facts {
        hash_catalog_fact(
            &mut hasher,
            &fact.identity,
            &fact.catalog_key.schema_key,
            &fact.schema,
        )?;
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
        if let Self::Shared(snapshot) = self {
            *self = Self::Owned(snapshot.rebuild_owned()?);
        }
        let Self::Owned(snapshot) = self else {
            unreachable!("transaction catalog is owned after copy-on-write");
        };
        snapshot.insert_schema_for_domain(domain, key, schema)
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

#[derive(Clone, Copy, Debug)]
pub(crate) enum TypedJsonScalarRef<'a> {
    Null,
    Boolean,
    String(&'a str),
}

/// Batch-stable validation compiled once for a fixed typed object layout.
///
/// SQL and future typed producers reuse the resolved property validators and
/// primary-key column positions for every row instead of repeating structural
/// schema work in the hot loop.
pub(crate) struct TypedJsonObjectLayoutCertificate<'a> {
    schema_key: &'a str,
    field_names: Vec<String>,
    field_validations: Vec<Option<&'a FastValueValidation>>,
    primary_key_field_indices: Vec<usize>,
    primary_key_component_types: &'a [crate::row_pk::RowPkComponentType],
}

impl TypedJsonObjectLayoutCertificate<'_> {
    pub(crate) fn certify_row(
        &self,
        values: &[TypedJsonScalarRef<'_>],
        row_pk: &[&str],
    ) -> Result<(), LixError> {
        if values.len() != self.field_validations.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "typed object row does not match its certified layout",
            ));
        }
        for ((name, validation), value) in self
            .field_names
            .iter()
            .zip(&self.field_validations)
            .zip(values.iter().copied())
        {
            if validation.is_some_and(|validation| !validation.accepts_typed(value)) {
                return Err(typed_object_validation_error(
                    self.schema_key,
                    &format!("property '{name}' does not satisfy its schema"),
                ));
            }
        }
        if row_pk.len() != self.primary_key_field_indices.len() {
            return Err(typed_object_validation_error(
                self.schema_key,
                "snapshot primary-key component count does not match the emitted row_pk",
            ));
        }
        for (&field_index, expected) in self.primary_key_field_indices.iter().zip(row_pk) {
            let actual = match values[field_index] {
                TypedJsonScalarRef::String(value) => Some(value),
                TypedJsonScalarRef::Null | TypedJsonScalarRef::Boolean => None,
            };
            if actual != Some(*expected) {
                return Err(typed_object_validation_error(
                    self.schema_key,
                    &format!(
                        "snapshot primary-key property '{}' does not match the emitted row_pk",
                        self.field_names[field_index]
                    ),
                ));
            }
        }
        RowPk::validate_external_parts(row_pk, self.primary_key_component_types).map_err(|error| {
            LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!(
                    "typed row_pk is invalid for schema '{}': {error}",
                    self.schema_key
                ),
            )
        })
    }
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

    pub(crate) fn certify_typed_object_layout<'a>(
        &'a self,
        schema_key: &str,
        field_names: &[&str],
    ) -> Result<TypedJsonObjectLayoutCertificate<'a>, LixError> {
        if !self.accepts_canonical_certificate() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "typed object certification requires a canonical schema plan",
            ));
        }
        if schema_key != self.key.schema_key {
            return Err(LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!(
                    "typed snapshot schema '{schema_key}' does not match schema plan '{}'",
                    self.key.schema_key
                ),
            ));
        }
        let validation = self
            .fast_object_validation
            .as_ref()
            .expect("certificate eligibility requires fast object validation");
        if field_names.len() < validation.min_properties {
            return Err(typed_object_validation_error(
                &self.key.schema_key,
                "object has fewer properties than minProperties",
            ));
        }
        if let Some(required) = validation
            .required
            .iter()
            .find(|required| !field_names.contains(&required.as_str()))
        {
            return Err(typed_object_validation_error(
                &self.key.schema_key,
                &format!("required property '{required}' is missing"),
            ));
        }
        let mut field_validations = Vec::with_capacity(field_names.len());
        for &name in field_names {
            match validation.properties.get(name) {
                Some(property) => field_validations.push(Some(property)),
                None if validation.additional_properties => field_validations.push(None),
                None => {
                    return Err(typed_object_validation_error(
                        &self.key.schema_key,
                        &format!("property '{name}' does not satisfy its schema"),
                    ));
                }
            }
        }
        let primary_key_paths = self
            .primary_key
            .as_deref()
            .expect("certificate eligibility requires a primary key");
        let mut primary_key_field_indices = Vec::with_capacity(primary_key_paths.len());
        for path in primary_key_paths {
            let [name] = path.as_slice() else {
                unreachable!("certificate eligibility requires top-level primary keys");
            };
            let Some(field_index) = field_names.iter().position(|field| field == name) else {
                return Err(typed_object_validation_error(
                    &self.key.schema_key,
                    &format!("required primary-key property '{name}' is missing"),
                ));
            };
            primary_key_field_indices.push(field_index);
        }
        Ok(TypedJsonObjectLayoutCertificate {
            schema_key: &self.key.schema_key,
            field_names: field_names.iter().map(|name| (*name).to_string()).collect(),
            field_validations,
            primary_key_field_indices,
            primary_key_component_types: self
                .primary_key_component_types
                .as_deref()
                .expect("certificate eligibility requires typed primary-key components"),
        })
    }

    /// Parses, validates, and, only when necessary, canonicalizes an internal
    /// engine JSON row in one structural pass. Plugin-owned schemas never use
    /// this path; their rows cross the host boundary as typed values.
    pub(crate) fn certify_or_normalize_json_row_parts(
        &self,
        bytes: &[u8],
        schema_key: &str,
        row_pk: &[&str],
    ) -> Result<Option<Option<Vec<u8>>>, LixError> {
        if !self.accepts_canonical_certificate() {
            return Ok(None);
        }
        let validation = self
            .fast_object_validation
            .as_ref()
            .expect("certificate eligibility requires fast object validation");
        let primary_key_paths = self
            .primary_key
            .as_deref()
            .expect("certificate eligibility requires a primary key");
        let component_types = self
            .primary_key_component_types
            .as_deref()
            .expect("certificate eligibility requires typed primary-key components");
        if row_pk.len() != primary_key_paths.len() {
            return Ok(None);
        }
        if schema_key != self.key.schema_key {
            return Err(LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!(
                    "row schema '{}' does not match schema plan '{}'",
                    schema_key, self.key.schema_key
                ),
            ));
        }

        let mut parser = CanonicalPluginRowParser::new(bytes)?;
        let mut primary_key = CanonicalPrimaryKeyMatcher::new(primary_key_paths, row_pk);
        let normalized = match parser.parse_root_object(validation, &mut primary_key) {
            Ok(normalized) => normalized,
            Err(CanonicalPluginRowError::InvalidPlugin(message)) => {
                return Err(LixError::new(LixError::CODE_INVALID_PLUGIN, message));
            }
            Err(CanonicalPluginRowError::Schema(message)) => {
                return Err(LixError::new(
                    LixError::CODE_SCHEMA_VALIDATION,
                    format!(
                        "row content validation failed for schema '{}': {message}",
                        self.key.schema_key
                    ),
                ));
            }
        };

        RowPk::validate_external_parts(row_pk, component_types)
            .map(|()| Some(normalized))
            .map_err(|error| {
                LixError::new(
                    LixError::CODE_SCHEMA_VALIDATION,
                    format!(
                        "row primary key is invalid for schema '{}': {error}",
                        self.key.schema_key
                    ),
                )
            })
    }

    pub(crate) fn accepts_canonical_certificate(&self) -> bool {
        self.fast_object_validation
            .as_ref()
            .is_some_and(FastObjectValidationPlan::supports_canonical_streaming)
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

    fn supports_canonical_streaming(&self) -> bool {
        self.required.len() <= 128
            && self
                .properties
                .values()
                .all(FastValueValidation::supports_canonical_streaming)
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

    fn accepts_typed(&self, value: TypedJsonScalarRef<'_>) -> bool {
        match (self, value) {
            (Self::Types(types), TypedJsonScalarRef::Null) => {
                types.accepts_canonical_kind(CanonicalJsonKind::Null)
            }
            (Self::Types(types), TypedJsonScalarRef::Boolean) => {
                types.accepts_canonical_kind(CanonicalJsonKind::Boolean)
            }
            (Self::Types(types), TypedJsonScalarRef::String(_)) => {
                types.accepts_canonical_kind(CanonicalJsonKind::String)
            }
            (Self::String(validation), TypedJsonScalarRef::String(value)) => {
                validation.accepts(value)
            }
            (Self::StringOrNull(_), TypedJsonScalarRef::Null) => true,
            (Self::StringOrNull(validation), TypedJsonScalarRef::String(value)) => {
                validation.accepts(value)
            }
            (
                Self::String(_) | Self::StringOrNull(_),
                TypedJsonScalarRef::Boolean | TypedJsonScalarRef::Null,
            ) => false,
        }
    }

    fn supports_canonical_streaming(&self) -> bool {
        match self {
            Self::Types(_) | Self::String(_) | Self::StringOrNull(_) => true,
        }
    }
}

fn typed_object_validation_error(schema_key: &str, message: &str) -> LixError {
    LixError::new(
        LixError::CODE_SCHEMA_VALIDATION,
        format!("snapshot_content validation failed for schema '{schema_key}': {message}"),
    )
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

    fn accepts_canonical(&self, value: CanonicalJsonString<'_>) -> bool {
        self.accepts(value.encoded)
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

    fn accepts_canonical_kind(self, kind: CanonicalJsonKind) -> bool {
        let bit = match kind {
            CanonicalJsonKind::Null => Self::NULL,
            CanonicalJsonKind::Boolean => Self::BOOLEAN,
            CanonicalJsonKind::String => Self::STRING,
            CanonicalJsonKind::Array => Self::ARRAY,
            CanonicalJsonKind::Object => Self::OBJECT,
        };
        self.0 & bit != 0
    }
}

#[derive(Debug)]
enum CanonicalPluginRowError {
    InvalidPlugin(String),
    Schema(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CanonicalJsonKind {
    Null,
    Boolean,
    String,
    Array,
    Object,
}

/// A decoded view over one string token in the engine's exact canonical JSON
/// spelling. Iteration decodes the only escapes the canonical encoder emits
/// (`\"`, `\\`, the five short control escapes, and lowercase `\u00xx`
/// for remaining control scalars) without allocating.
#[derive(Clone, Copy, Debug)]
struct CanonicalJsonString<'a> {
    encoded: &'a str,
}

impl<'a> CanonicalJsonString<'a> {
    fn chars(self) -> CanonicalJsonStringChars<'a> {
        CanonicalJsonStringChars {
            encoded: self.encoded,
            offset: 0,
        }
    }

    fn eq_str(self, expected: &str) -> bool {
        self.chars().eq(expected.chars())
    }

    fn cmp(self, other: Self) -> Ordering {
        self.chars().cmp(other.chars())
    }
}

struct CanonicalJsonStringChars<'a> {
    encoded: &'a str,
    offset: usize,
}

impl Iterator for CanonicalJsonStringChars<'_> {
    type Item = char;

    fn next(&mut self) -> Option<Self::Item> {
        let remaining = self.encoded.get(self.offset..)?;
        let first = *remaining.as_bytes().first()?;
        if first != b'\\' {
            let scalar = remaining.chars().next()?;
            self.offset += scalar.len_utf8();
            return Some(scalar);
        }
        let escaped = remaining.as_bytes()[1];
        match escaped {
            b'"' => {
                self.offset += 2;
                Some('"')
            }
            b'\\' => {
                self.offset += 2;
                Some('\\')
            }
            b'b' => {
                self.offset += 2;
                Some('\u{08}')
            }
            b't' => {
                self.offset += 2;
                Some('\t')
            }
            b'n' => {
                self.offset += 2;
                Some('\n')
            }
            b'f' => {
                self.offset += 2;
                Some('\u{0c}')
            }
            b'r' => {
                self.offset += 2;
                Some('\r')
            }
            b'u' => {
                let high = canonical_hex_value(remaining.as_bytes()[4]);
                let low = canonical_hex_value(remaining.as_bytes()[5]);
                self.offset += 6;
                char::from_u32(u32::from((high << 4) | low))
            }
            _ => unreachable!("canonical string parser admitted an unsupported escape"),
        }
    }
}

fn canonical_hex_value(value: u8) -> u8 {
    match value {
        b'0'..=b'9' => value - b'0',
        b'a'..=b'f' => value - b'a' + 10,
        _ => unreachable!("canonical string parser admitted a non-lowercase hexadecimal digit"),
    }
}

enum ParsedJsonString<'a> {
    Canonical(CanonicalJsonString<'a>),
    Decoded(String),
}

impl ParsedJsonString<'_> {
    fn is_canonical(&self) -> bool {
        matches!(self, Self::Canonical(_))
    }

    fn eq_str(&self, expected: &str) -> bool {
        match self {
            Self::Canonical(value) => value.eq_str(expected),
            Self::Decoded(value) => value == expected,
        }
    }

    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Canonical(left), Self::Canonical(right)) => left.cmp(*right),
            (Self::Canonical(left), Self::Decoded(right)) => left.chars().cmp(right.chars()),
            (Self::Decoded(left), Self::Canonical(right)) => left.chars().cmp(right.chars()),
            (Self::Decoded(left), Self::Decoded(right)) => left.cmp(right),
        }
    }

    fn write_canonical(&self, output: &mut Vec<u8>) {
        output.push(b'"');
        match self {
            Self::Canonical(value) => output.extend_from_slice(value.encoded.as_bytes()),
            Self::Decoded(value) => write_canonical_json_string_contents(value, output),
        }
        output.push(b'"');
    }
}

#[derive(Clone, Copy)]
struct ParsedJsonValue(usize);

enum ParsedJsonNode<'a> {
    Exact {
        encoded: &'a str,
        kind: CanonicalJsonKind,
    },
    String(ParsedJsonString<'a>),
    Array {
        first: Option<usize>,
    },
    Object {
        first: Option<usize>,
    },
}

struct ParsedJsonProperty<'a> {
    key: ParsedJsonString<'a>,
    value: ParsedJsonValue,
    next: Option<usize>,
}

struct ParsedJsonElement {
    value: ParsedJsonValue,
    next: Option<usize>,
}

fn write_canonical_json_string_contents(value: &str, output: &mut Vec<u8>) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for scalar in value.chars() {
        match scalar {
            '"' => output.extend_from_slice(br#"\""#),
            '\\' => output.extend_from_slice(br#"\\"#),
            '\u{08}' => output.extend_from_slice(br"\b"),
            '\t' => output.extend_from_slice(br"\t"),
            '\n' => output.extend_from_slice(br"\n"),
            '\u{0c}' => output.extend_from_slice(br"\f"),
            '\r' => output.extend_from_slice(br"\r"),
            '\u{00}'..='\u{07}' | '\u{0b}' | '\u{0e}'..='\u{1f}' => {
                let byte = scalar as u8;
                output.extend_from_slice(br"\u00");
                output.push(HEX[usize::from(byte >> 4)]);
                output.push(HEX[usize::from(byte & 0x0f)]);
            }
            _ => {
                let mut encoded = [0_u8; 4];
                output.extend_from_slice(scalar.encode_utf8(&mut encoded).as_bytes());
            }
        }
    }
}

struct CanonicalPrimaryKeyMatcher<'a> {
    paths: &'a [Vec<String>],
    emitted: &'a [&'a str],
    seen: u128,
}

impl<'a> CanonicalPrimaryKeyMatcher<'a> {
    fn new(paths: &'a [Vec<String>], emitted: &'a [&'a str]) -> Self {
        Self {
            paths,
            emitted,
            seen: 0,
        }
    }

    fn component_for_key(&self, key: &ParsedJsonString<'_>) -> Option<usize> {
        self.paths.iter().position(|path| key.eq_str(&path[0]))
    }

    fn observe(&mut self, index: usize, actual: Option<&ParsedJsonString<'_>>) -> Option<String> {
        self.seen |= 1_u128 << index;
        let Some(actual) = actual else {
            return Some(format!(
                "primary-key property '{}' must be a string in production v2 snapshots",
                self.paths[index][0]
            ));
        };
        if actual.eq_str(self.emitted[index]) {
            None
        } else {
            Some(format!(
                "snapshot primary-key property '{}' does not match the emitted row_pk",
                self.paths[index][0]
            ))
        }
    }

    fn finish(&self) -> Option<String> {
        let expected = if self.paths.len() == 128 {
            u128::MAX
        } else {
            (1_u128 << self.paths.len()) - 1
        };
        if self.seen == expected {
            None
        } else {
            Some("snapshot is missing one or more primary-key properties".to_owned())
        }
    }
}

/// One-pass parser for canonical and compatibility-spelled, number-free v2
/// snapshots.
///
/// Exact values collapse to borrowed source slices. A noncanonical container
/// keeps only typed child fragments, using inline storage for the common row
/// shapes. At the root those fragments are emitted once into a single
/// canonical buffer. No `serde_json::Value` is constructed.
struct CanonicalPluginRowParser<'a> {
    input: &'a str,
    offset: usize,
    semantic_error: Option<String>,
    nodes: SmallVec<[ParsedJsonNode<'a>; 64]>,
    properties: SmallVec<[ParsedJsonProperty<'a>; 16]>,
    elements: SmallVec<[ParsedJsonElement; 64]>,
}

impl<'a> CanonicalPluginRowParser<'a> {
    const MAX_DEPTH: usize = 128;

    fn new(bytes: &'a [u8]) -> Result<Self, LixError> {
        let input = std::str::from_utf8(bytes).map_err(|error| {
            LixError::new(
                LixError::CODE_INVALID_PLUGIN,
                format!("v2 snapshot must be UTF-8 JSON: {error}"),
            )
        })?;
        Ok(Self {
            input,
            offset: 0,
            semantic_error: None,
            nodes: SmallVec::new(),
            properties: SmallVec::new(),
            elements: SmallVec::new(),
        })
    }

    fn parse_root_object(
        &mut self,
        validation: &FastObjectValidationPlan,
        primary_key: &mut CanonicalPrimaryKeyMatcher<'_>,
    ) -> Result<Option<Vec<u8>>, CanonicalPluginRowError> {
        let leading_whitespace = self.skip_whitespace();
        if self.peek() != Some(b'{') {
            return Err(CanonicalPluginRowError::InvalidPlugin(
                "v2 row snapshots must be JSON objects".to_owned(),
            ));
        }
        let value = self.parse_object(Some(validation), Some(primary_key), 0)?;
        let trailing_whitespace = self.skip_whitespace();
        if self.offset != self.input.len() {
            return Err(CanonicalPluginRowError::InvalidPlugin(
                "v2 snapshot contains trailing or invalid JSON input".to_owned(),
            ));
        }
        if let Some(message) = primary_key.finish() {
            self.record_schema(message);
        }
        if let Some(message) = self.semantic_error.take() {
            return Err(CanonicalPluginRowError::Schema(message));
        }
        if self.value_is_canonical(value) && !leading_whitespace && !trailing_whitespace {
            return Ok(None);
        }
        let mut normalized = Vec::with_capacity(self.input.len());
        self.write_value(value, &mut normalized);
        Ok(Some(normalized))
    }

    fn parse_value(
        &mut self,
        validation: Option<&FastValueValidation>,
        depth: usize,
    ) -> Result<ParsedJsonValue, CanonicalPluginRowError> {
        if depth > Self::MAX_DEPTH {
            return Err(CanonicalPluginRowError::InvalidPlugin(
                "v2 snapshot JSON nesting exceeds the host limit".to_owned(),
            ));
        }
        let start = self.offset;
        let value = match self.peek() {
            Some(b'"') => {
                let string = self.parse_string()?;
                self.push_node(ParsedJsonNode::String(string))
            }
            Some(b'{') => self.parse_object(None, None, depth)?,
            Some(b'[') => self.parse_array(depth)?,
            Some(b'n') => {
                self.parse_literal(b"null")?;
                self.push_node(ParsedJsonNode::Exact {
                    encoded: &self.input[start..self.offset],
                    kind: CanonicalJsonKind::Null,
                })
            }
            Some(b't') => {
                self.parse_literal(b"true")?;
                self.push_node(ParsedJsonNode::Exact {
                    encoded: &self.input[start..self.offset],
                    kind: CanonicalJsonKind::Boolean,
                })
            }
            Some(b'f') => {
                self.parse_literal(b"false")?;
                self.push_node(ParsedJsonNode::Exact {
                    encoded: &self.input[start..self.offset],
                    kind: CanonicalJsonKind::Boolean,
                })
            }
            Some(b'-' | b'0'..=b'9') => {
                return Err(CanonicalPluginRowError::InvalidPlugin(
                    "JSON numbers are not enabled for production v2".to_owned(),
                ));
            }
            Some(_) => {
                return Err(CanonicalPluginRowError::InvalidPlugin(
                    "v2 snapshot contains malformed JSON".to_owned(),
                ));
            }
            None => {
                return Err(CanonicalPluginRowError::InvalidPlugin(
                    "v2 snapshot ended before a JSON value".to_owned(),
                ));
            }
        };
        self.validate_value(validation, value);
        Ok(value)
    }

    fn parse_object(
        &mut self,
        validation: Option<&FastObjectValidationPlan>,
        mut primary_key: Option<&mut CanonicalPrimaryKeyMatcher<'_>>,
        depth: usize,
    ) -> Result<ParsedJsonValue, CanonicalPluginRowError> {
        let start = self.offset;
        self.expect_byte(b'{')?;
        let mut canonical = !self.skip_whitespace();
        let mut first_property = None;
        let mut last_property = None::<usize>;
        let mut required_seen = 0_u128;
        let mut property_count = 0_usize;
        if self.consume_byte(b'}') {
            self.validate_object_end(validation, required_seen, property_count);
            return Ok(if canonical {
                self.push_node(ParsedJsonNode::Exact {
                    encoded: &self.input[start..self.offset],
                    kind: CanonicalJsonKind::Object,
                })
            } else {
                self.push_node(ParsedJsonNode::Object { first: None })
            });
        }

        loop {
            if self.peek() != Some(b'"') {
                return Err(CanonicalPluginRowError::InvalidPlugin(
                    "v2 snapshot object keys must be JSON strings".to_owned(),
                ));
            }
            let key = self.parse_string()?;
            canonical &= key.is_canonical();
            if let Some(previous) = last_property {
                match self.properties[previous].key.cmp(&key) {
                    Ordering::Less => {}
                    Ordering::Equal => {
                        return Err(CanonicalPluginRowError::InvalidPlugin(
                            "v2 snapshot contains a duplicate decoded JSON object key".to_owned(),
                        ));
                    }
                    Ordering::Greater => canonical = false,
                }
            }
            property_count += 1;

            let property_validation = validation.and_then(|plan| {
                plan.properties
                    .iter()
                    .find_map(|(name, validation)| key.eq_str(name).then_some(validation))
            });
            if let Some(plan) = validation {
                if let Some(index) = plan
                    .required
                    .iter()
                    .position(|required| key.eq_str(required))
                {
                    required_seen |= 1_u128 << index;
                }
                if property_validation.is_none() && !plan.additional_properties {
                    self.record_schema("snapshot contains an undeclared property".to_owned());
                }
            }

            canonical &= !self.skip_whitespace();
            if self.peek() != Some(b':') {
                return Err(CanonicalPluginRowError::InvalidPlugin(
                    "v2 snapshot object key is not followed by ':'".to_owned(),
                ));
            }
            self.offset += 1;
            canonical &= !self.skip_whitespace();
            let primary_component = primary_key
                .as_deref()
                .and_then(|matcher| matcher.component_for_key(&key));
            let value = self.parse_value(property_validation, depth + 1)?;
            canonical &= self.value_is_canonical(value);
            if let (Some(index), Some(matcher)) = (primary_component, primary_key.as_deref_mut()) {
                if let Some(message) = matcher.observe(index, self.value_string(value)) {
                    self.record_schema(message);
                }
            }
            let property = self.properties.len();
            self.properties.push(ParsedJsonProperty {
                key,
                value,
                next: None,
            });
            if let Some(previous) = last_property {
                self.properties[previous].next = Some(property);
            } else {
                first_property = Some(property);
            }
            last_property = Some(property);

            canonical &= !self.skip_whitespace();
            match self.peek() {
                Some(b',') => {
                    self.offset += 1;
                    canonical &= !self.skip_whitespace();
                }
                Some(b'}') => {
                    self.offset += 1;
                    break;
                }
                _ => {
                    return Err(CanonicalPluginRowError::InvalidPlugin(
                        "v2 snapshot object contains malformed JSON".to_owned(),
                    ));
                }
            }
        }
        self.validate_object_end(validation, required_seen, property_count);
        if canonical {
            return Ok(self.push_node(ParsedJsonNode::Exact {
                encoded: &self.input[start..self.offset],
                kind: CanonicalJsonKind::Object,
            }));
        }
        let first = self.sort_object_properties(first_property)?;
        Ok(self.push_node(ParsedJsonNode::Object { first }))
    }

    fn validate_object_end(
        &mut self,
        validation: Option<&FastObjectValidationPlan>,
        required_seen: u128,
        property_count: usize,
    ) {
        let Some(validation) = validation else {
            return;
        };
        let expected_required = if validation.required.len() == 128 {
            u128::MAX
        } else {
            (1_u128 << validation.required.len()) - 1
        };
        if required_seen != expected_required {
            self.record_schema("snapshot is missing one or more required properties".to_owned());
        }
        if property_count < validation.min_properties {
            self.record_schema(format!(
                "snapshot object has fewer than {} properties",
                validation.min_properties
            ));
        }
    }

    fn parse_array(&mut self, depth: usize) -> Result<ParsedJsonValue, CanonicalPluginRowError> {
        let start = self.offset;
        self.expect_byte(b'[')?;
        let mut canonical = !self.skip_whitespace();
        let mut first_element = None;
        let mut last_element = None::<usize>;
        if self.consume_byte(b']') {
            return Ok(if canonical {
                self.push_node(ParsedJsonNode::Exact {
                    encoded: &self.input[start..self.offset],
                    kind: CanonicalJsonKind::Array,
                })
            } else {
                self.push_node(ParsedJsonNode::Array { first: None })
            });
        }
        loop {
            let value = self.parse_value(None, depth + 1)?;
            canonical &= self.value_is_canonical(value);
            let element = self.elements.len();
            self.elements.push(ParsedJsonElement { value, next: None });
            if let Some(previous) = last_element {
                self.elements[previous].next = Some(element);
            } else {
                first_element = Some(element);
            }
            last_element = Some(element);
            canonical &= !self.skip_whitespace();
            match self.peek() {
                Some(b',') => {
                    self.offset += 1;
                    canonical &= !self.skip_whitespace();
                }
                Some(b']') => {
                    self.offset += 1;
                    break;
                }
                _ => {
                    return Err(CanonicalPluginRowError::InvalidPlugin(
                        "v2 snapshot array contains malformed JSON".to_owned(),
                    ));
                }
            }
        }
        if canonical {
            Ok(self.push_node(ParsedJsonNode::Exact {
                encoded: &self.input[start..self.offset],
                kind: CanonicalJsonKind::Array,
            }))
        } else {
            Ok(self.push_node(ParsedJsonNode::Array {
                first: first_element,
            }))
        }
    }

    fn parse_string(&mut self) -> Result<ParsedJsonString<'a>, CanonicalPluginRowError> {
        self.expect_byte(b'"')?;
        let start = self.offset;
        let mut decoded = None::<String>;
        while let Some(byte) = self.peek() {
            match byte {
                b'"' => {
                    let end = self.offset;
                    self.offset += 1;
                    return Ok(match decoded {
                        Some(decoded) => ParsedJsonString::Decoded(decoded),
                        None => ParsedJsonString::Canonical(CanonicalJsonString {
                            encoded: &self.input[start..end],
                        }),
                    });
                }
                b'\\' => {
                    let (scalar, consumed, canonical_escape) =
                        self.parse_string_escape(self.offset)?;
                    if !canonical_escape && decoded.is_none() {
                        decoded = Some(
                            CanonicalJsonString {
                                encoded: &self.input[start..self.offset],
                            }
                            .chars()
                            .collect(),
                        );
                    }
                    if let Some(decoded) = decoded.as_mut() {
                        decoded.push(scalar);
                    }
                    self.offset += consumed;
                }
                0x00..=0x1f => {
                    return Err(CanonicalPluginRowError::InvalidPlugin(
                        "v2 snapshot string contains an unescaped control byte".to_owned(),
                    ));
                }
                byte if byte.is_ascii() => {
                    if let Some(decoded) = decoded.as_mut() {
                        decoded.push(char::from(byte));
                    }
                    self.offset += 1;
                }
                _ => {
                    let remaining = &self.input[self.offset..];
                    let scalar = remaining
                        .chars()
                        .next()
                        .expect("input was validated as UTF-8");
                    if let Some(decoded) = decoded.as_mut() {
                        decoded.push(scalar);
                    }
                    self.offset += scalar.len_utf8();
                }
            }
        }
        Err(CanonicalPluginRowError::InvalidPlugin(
            "v2 snapshot contains an unterminated JSON string".to_owned(),
        ))
    }

    fn parse_string_escape(
        &self,
        offset: usize,
    ) -> Result<(char, usize, bool), CanonicalPluginRowError> {
        let remaining = &self.input.as_bytes()[offset..];
        let Some(escaped) = remaining.get(1).copied() else {
            return Err(CanonicalPluginRowError::InvalidPlugin(
                "v2 snapshot contains an unterminated JSON string escape".to_owned(),
            ));
        };
        let simple = match escaped {
            b'"' => Some(('"', true)),
            b'\\' => Some(('\\', true)),
            b'/' => Some(('/', false)),
            b'b' => Some(('\u{08}', true)),
            b't' => Some(('\t', true)),
            b'n' => Some(('\n', true)),
            b'f' => Some(('\u{0c}', true)),
            b'r' => Some(('\r', true)),
            _ => None,
        };
        if let Some((scalar, canonical)) = simple {
            return Ok((scalar, 2, canonical));
        }
        if escaped != b'u' {
            return Err(CanonicalPluginRowError::InvalidPlugin(
                "v2 snapshot string contains an invalid JSON escape".to_owned(),
            ));
        }
        let high = parse_json_hex_quad(remaining.get(2..6).ok_or_else(|| {
            CanonicalPluginRowError::InvalidPlugin(
                "v2 snapshot contains an incomplete unicode escape".to_owned(),
            )
        })?)
        .ok_or_else(|| {
            CanonicalPluginRowError::InvalidPlugin(
                "v2 snapshot string contains an invalid unicode escape".to_owned(),
            )
        })?;
        if (0xd800..=0xdbff).contains(&high) {
            if remaining.get(6..8) != Some(br"\u") {
                return Err(CanonicalPluginRowError::InvalidPlugin(
                    "v2 snapshot string contains an unpaired unicode surrogate".to_owned(),
                ));
            }
            let low = parse_json_hex_quad(remaining.get(8..12).ok_or_else(|| {
                CanonicalPluginRowError::InvalidPlugin(
                    "v2 snapshot contains an incomplete unicode surrogate pair".to_owned(),
                )
            })?)
            .ok_or_else(|| {
                CanonicalPluginRowError::InvalidPlugin(
                    "v2 snapshot string contains an invalid unicode surrogate pair".to_owned(),
                )
            })?;
            if !(0xdc00..=0xdfff).contains(&low) {
                return Err(CanonicalPluginRowError::InvalidPlugin(
                    "v2 snapshot string contains an unpaired unicode surrogate".to_owned(),
                ));
            }
            let scalar = 0x1_0000 + ((u32::from(high) - 0xd800) << 10) + (u32::from(low) - 0xdc00);
            return Ok((
                char::from_u32(scalar).expect("valid surrogate pair forms a unicode scalar"),
                12,
                false,
            ));
        }
        if (0xdc00..=0xdfff).contains(&high) {
            return Err(CanonicalPluginRowError::InvalidPlugin(
                "v2 snapshot string contains an unpaired unicode surrogate".to_owned(),
            ));
        }
        let scalar =
            char::from_u32(u32::from(high)).expect("non-surrogate u16 is a unicode scalar");
        let canonical = matches!(
            high,
            0x00..=0x07 | 0x0b | 0x0e..=0x1f
        ) && remaining.get(2..4) == Some(b"00")
            && remaining[4..6]
                .iter()
                .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'));
        Ok((scalar, 6, canonical))
    }

    fn parse_literal(&mut self, literal: &[u8]) -> Result<(), CanonicalPluginRowError> {
        if self
            .input
            .as_bytes()
            .get(self.offset..self.offset.saturating_add(literal.len()))
            == Some(literal)
        {
            self.offset += literal.len();
            Ok(())
        } else {
            Err(CanonicalPluginRowError::InvalidPlugin(
                "v2 snapshot contains a malformed JSON literal".to_owned(),
            ))
        }
    }

    fn push_node(&mut self, node: ParsedJsonNode<'a>) -> ParsedJsonValue {
        let value = ParsedJsonValue(self.nodes.len());
        self.nodes.push(node);
        value
    }

    fn value_kind(&self, value: ParsedJsonValue) -> CanonicalJsonKind {
        match &self.nodes[value.0] {
            ParsedJsonNode::Exact { kind, .. } => *kind,
            ParsedJsonNode::String(_) => CanonicalJsonKind::String,
            ParsedJsonNode::Array { .. } => CanonicalJsonKind::Array,
            ParsedJsonNode::Object { .. } => CanonicalJsonKind::Object,
        }
    }

    fn value_string(&self, value: ParsedJsonValue) -> Option<&ParsedJsonString<'a>> {
        match &self.nodes[value.0] {
            ParsedJsonNode::String(value) => Some(value),
            _ => None,
        }
    }

    fn value_is_canonical(&self, value: ParsedJsonValue) -> bool {
        match &self.nodes[value.0] {
            ParsedJsonNode::Exact { .. } => true,
            ParsedJsonNode::String(value) => value.is_canonical(),
            ParsedJsonNode::Array { .. } | ParsedJsonNode::Object { .. } => false,
        }
    }

    fn write_value(&self, value: ParsedJsonValue, output: &mut Vec<u8>) {
        match &self.nodes[value.0] {
            ParsedJsonNode::Exact { encoded, .. } => {
                output.extend_from_slice(encoded.as_bytes());
            }
            ParsedJsonNode::String(value) => value.write_canonical(output),
            ParsedJsonNode::Array { first } => {
                output.push(b'[');
                let mut element = *first;
                let mut first_output = true;
                while let Some(index) = element {
                    if !first_output {
                        output.push(b',');
                    }
                    first_output = false;
                    let value = self.elements[index].value;
                    element = self.elements[index].next;
                    self.write_value(value, output);
                }
                output.push(b']');
            }
            ParsedJsonNode::Object { first } => {
                output.push(b'{');
                let mut property = *first;
                let mut first_output = true;
                while let Some(index) = property {
                    if !first_output {
                        output.push(b',');
                    }
                    first_output = false;
                    self.properties[index].key.write_canonical(output);
                    output.push(b':');
                    let value = self.properties[index].value;
                    property = self.properties[index].next;
                    self.write_value(value, output);
                }
                output.push(b'}');
            }
        }
    }

    /// Sorting storage exists only after a row has diverged from canonical
    /// spelling. The canonical lane compares each key with the prior key and
    /// never constructs this list.
    fn sort_object_properties(
        &mut self,
        first: Option<usize>,
    ) -> Result<Option<usize>, CanonicalPluginRowError> {
        let mut ordered = SmallVec::<[usize; 8]>::new();
        let mut property = first;
        while let Some(index) = property {
            ordered.push(index);
            property = self.properties[index].next;
        }
        ordered.sort_unstable_by(|left, right| {
            self.properties[*left].key.cmp(&self.properties[*right].key)
        });
        if ordered.windows(2).any(|pair| {
            self.properties[pair[0]]
                .key
                .cmp(&self.properties[pair[1]].key)
                == Ordering::Equal
        }) {
            return Err(CanonicalPluginRowError::InvalidPlugin(
                "v2 snapshot contains a duplicate decoded JSON object key".to_owned(),
            ));
        }
        for pair in ordered.windows(2) {
            self.properties[pair[0]].next = Some(pair[1]);
        }
        if let Some(last) = ordered.last().copied() {
            self.properties[last].next = None;
        }
        Ok(ordered.first().copied())
    }

    fn validate_value(&mut self, validation: Option<&FastValueValidation>, value: ParsedJsonValue) {
        let Some(validation) = validation else {
            return;
        };
        let accepted = match validation {
            FastValueValidation::Types(types) => {
                types.accepts_canonical_kind(self.value_kind(value))
            }
            FastValueValidation::String(validation) => {
                self.value_string(value).is_some_and(|value| match value {
                    ParsedJsonString::Canonical(value) => validation.accepts_canonical(*value),
                    ParsedJsonString::Decoded(value) => validation.accepts(value),
                })
            }
            FastValueValidation::StringOrNull(validation) => {
                self.value_kind(value) == CanonicalJsonKind::Null
                    || self.value_string(value).is_some_and(|value| match value {
                        ParsedJsonString::Canonical(value) => validation.accepts_canonical(*value),
                        ParsedJsonString::Decoded(value) => validation.accepts(value),
                    })
            }
        };
        if !accepted {
            self.record_schema("snapshot property does not satisfy its schema".to_owned());
        }
    }

    fn record_schema(&mut self, message: String) {
        if self.semantic_error.is_none() {
            self.semantic_error = Some(message);
        }
    }

    fn expect_byte(&mut self, expected: u8) -> Result<(), CanonicalPluginRowError> {
        if self.consume_byte(expected) {
            Ok(())
        } else {
            Err(CanonicalPluginRowError::InvalidPlugin(
                "v2 snapshot contains malformed JSON".to_owned(),
            ))
        }
    }

    fn consume_byte(&mut self, expected: u8) -> bool {
        if self.peek() == Some(expected) {
            self.offset += 1;
            true
        } else {
            false
        }
    }

    fn peek(&self) -> Option<u8> {
        self.input.as_bytes().get(self.offset).copied()
    }

    fn skip_whitespace(&mut self) -> bool {
        let start = self.offset;
        while self
            .peek()
            .is_some_and(|byte| matches!(byte, b' ' | b'\t' | b'\n' | b'\r'))
        {
            self.offset += 1;
        }
        self.offset != start
    }
}

fn parse_json_hex_quad(bytes: &[u8]) -> Option<u16> {
    if bytes.len() != 4 {
        return None;
    }
    bytes.iter().try_fold(0_u16, |value, byte| {
        let digit = match byte {
            b'0'..=b'9' => u16::from(byte - b'0'),
            b'a'..=b'f' => u16::from(byte - b'a' + 10),
            b'A'..=b'F' => u16::from(byte - b'A' + 10),
            _ => return None,
        };
        Some((value << 4) | digit)
    })
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

    const UUID_A: &str = "019a0000-0000-7000-8000-000000000001";

    fn compile_actual_fast_schema(schema_json: &str) -> SchemaPlan {
        let schema: JsonValue =
            serde_json::from_str(schema_json).expect("built-in schema JSON should parse");
        let schema_key = schema
            .get("key")
            .and_then(JsonValue::as_str)
            .expect("built-in schema should declare key")
            .to_owned();
        let plan = SchemaPlan::compile(
            SchemaCatalogKey {
                schema_key: schema_key.clone(),
            },
            schema,
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
        .expect("built-in schema should compile");
        assert!(
            plan.fast_object_validation.is_some(),
            "built-in schema '{schema_key}' should have a fast validation plan"
        );
        plan
    }

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

        assert_eq!(catalog.schema_jsons(), vec![alpha, zeta]);
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
