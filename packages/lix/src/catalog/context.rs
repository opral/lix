use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use blake3::Hasher;
use serde_json::Value as JsonValue;

use crate::LixError;
use crate::catalog::snapshot::{
    CatalogFingerprint, fingerprint_schema_facts, hash_fingerprint_part,
};
use crate::catalog::{CatalogSnapshot, SchemaCatalogFact};
use crate::domain::Domain;
use crate::entity_pk::{EntityPk, EntityPkComponents};
use crate::schema::schema_key_from_definition;
use crate::state::{StateRow, TransactionStateView};

const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";
const COMPILED_CATALOG_CACHE_LIMIT: usize = 64;

/// Engine schema visibility boundary over one authenticated ForkTree view.
pub(crate) struct CatalogContext {
    compiled_catalogs: Mutex<HashMap<CatalogFingerprint, Arc<CatalogSnapshot>>>,
    compiled_catalogs_by_rows: Mutex<HashMap<CatalogRowsFingerprint, Arc<CatalogSnapshot>>>,
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct CatalogRowsFingerprint(String);

impl CatalogContext {
    pub(crate) fn new() -> Self {
        Self {
            compiled_catalogs: Mutex::new(HashMap::new()),
            compiled_catalogs_by_rows: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) async fn compiled_catalog_for_transaction_state<R>(
        &self,
        state: &TransactionStateView<R>,
        domain: &Domain,
    ) -> Result<Arc<CatalogSnapshot>, LixError>
    where
        R: crate::storage_adapter::StorageAdapterRead,
    {
        let catalog_rows = scan_transaction_catalog_rows(state, domain).await?;
        self.compiled_catalog_for_rows(&catalog_rows)
    }

    fn compiled_catalog_for_rows(
        &self,
        catalog_rows: &CatalogRows,
    ) -> Result<Arc<CatalogSnapshot>, LixError> {
        let mut hasher = Hasher::new();
        for (schema_domain, row) in catalog_rows.iter() {
            hash_fingerprint_part(&mut hasher, &schema_domain.fingerprint_component());
            let snapshot_content = row.snapshot_content.as_deref().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    "catalog row is missing its authenticated snapshot content",
                )
            })?;
            hash_fingerprint_part(&mut hasher, snapshot_content);
        }
        let fingerprint = CatalogRowsFingerprint(hasher.finalize().to_hex().to_string());

        if let Some(snapshot) = self
            .compiled_catalogs_by_rows
            .lock()
            .expect("compiled catalog rows cache lock should not be poisoned")
            .get(&fingerprint)
        {
            return Ok(Arc::clone(snapshot));
        }

        let facts = facts_from_catalog_rows(catalog_rows)?;
        let snapshot = self.compiled_catalog_for_facts(&facts)?;
        let mut cache = self
            .compiled_catalogs_by_rows
            .lock()
            .expect("compiled catalog rows cache lock should not be poisoned");
        if cache.len() >= COMPILED_CATALOG_CACHE_LIMIT {
            if let Some(evicted) = cache.keys().find(|key| **key != fingerprint).cloned() {
                cache.remove(&evicted);
            }
        }
        cache.insert(fingerprint, Arc::clone(&snapshot));
        Ok(snapshot)
    }

    pub(crate) fn compiled_catalog_for_facts(
        &self,
        facts: &[SchemaCatalogFact],
    ) -> Result<Arc<CatalogSnapshot>, LixError> {
        let fingerprint = fingerprint_schema_facts(facts)?;
        if let Some(snapshot) = self
            .compiled_catalogs
            .lock()
            .expect("compiled catalog cache lock should not be poisoned")
            .get(&fingerprint)
        {
            return Ok(Arc::clone(snapshot));
        }
        let snapshot = Arc::new(CatalogSnapshot::from_schema_facts(facts)?);
        #[cfg(feature = "storage-benches")]
        crate::storage_bench::record_transaction_schema_catalog_compile();
        let mut cache = self
            .compiled_catalogs
            .lock()
            .expect("compiled catalog cache lock should not be poisoned");
        if cache.len() >= COMPILED_CATALOG_CACHE_LIMIT {
            if let Some(evicted) = cache.keys().find(|key| **key != fingerprint).cloned() {
                cache.remove(&evicted);
            }
        }
        cache.insert(fingerprint, Arc::clone(&snapshot));
        Ok(snapshot)
    }
}

struct CatalogRows {
    domains: Vec<CatalogDomainRows>,
}

impl CatalogRows {
    fn iter(&self) -> impl Iterator<Item = (&Domain, &CatalogRow)> {
        self.domains.iter().flat_map(|domain_rows| {
            domain_rows.rows.iter().filter_map(move |row| {
                row_belongs_to_schema_catalog_domain(row).then_some((&domain_rows.domain, row))
            })
        })
    }
}

struct CatalogDomainRows {
    domain: Domain,
    rows: Vec<CatalogRow>,
}

struct CatalogRow {
    key: crate::forktree::StateKey,
    snapshot_content: Option<crate::common::SharedStr>,
}

fn catalog_row_from_state(row: StateRow) -> CatalogRow {
    let key = crate::forktree::decode_state_key(&row.key)
        .expect("authenticated state view must return canonical state keys");
    let snapshot_content = match row.value.cell {
        crate::forktree::StateCell::Value(value) => Some(value),
        crate::forktree::StateCell::Null | crate::forktree::StateCell::Tombstone => None,
    };
    CatalogRow {
        key,
        snapshot_content,
    }
}

async fn scan_transaction_catalog_rows<R>(
    state: &TransactionStateView<R>,
    domain: &Domain,
) -> Result<CatalogRows, LixError>
where
    R: crate::storage_adapter::StorageAdapterRead,
{
    let lower = crate::forktree::encode_state_entity_prefix(
        REGISTERED_SCHEMA_KEY,
        &EntityPk {
            components: EntityPkComponents::Empty,
        },
    );
    let upper = crate::forktree::exclusive_prefix_upper_bound(&lower);
    let schema_domains = domain.schema_catalog_domains();
    let mut catalog_rows = Vec::with_capacity(schema_domains.len());
    for schema_domain in schema_domains {
        let rows = state
            .branch_range(
                schema_domain.branch_id(),
                Some(&lower),
                upper.as_deref(),
                None,
                false,
            )
            .await?
            .into_iter()
            .map(catalog_row_from_state)
            .collect();
        catalog_rows.push(CatalogDomainRows {
            domain: schema_domain,
            rows,
        });
    }
    Ok(CatalogRows {
        domains: catalog_rows,
    })
}

fn facts_from_catalog_rows(catalog_rows: &CatalogRows) -> Result<Vec<SchemaCatalogFact>, LixError> {
    let row_count = catalog_rows
        .domains
        .iter()
        .map(|domain| domain.rows.len())
        .sum();
    let mut facts = Vec::with_capacity(row_count);
    for (schema_domain, row) in catalog_rows.iter() {
        let Some((key, schema)) = decode_registered_schema_row(row)? else {
            continue;
        };
        facts.push(SchemaCatalogFact::new(schema_domain.clone(), key, schema));
    }
    Ok(facts)
}

fn row_belongs_to_schema_catalog_domain(row: &CatalogRow) -> bool {
    row.key.schema_key == REGISTERED_SCHEMA_KEY
        && row.key.file_id.is_none()
        && row.snapshot_content.is_some()
}

fn decode_registered_schema_row(
    row: &CatalogRow,
) -> Result<Option<(crate::schema::SchemaKey, JsonValue)>, LixError> {
    if row.key.schema_key != REGISTERED_SCHEMA_KEY {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "expected lix_registered_schema row, got schema_key={}",
                row.key.schema_key
            ),
        ));
    }
    let Some(snapshot_content) = row.snapshot_content.as_ref() else {
        return Ok(None);
    };
    let snapshot: JsonValue = serde_json::from_str(snapshot_content).map_err(|err| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("invalid registered schema snapshot JSON: {err}"),
        )
    })?;
    let schema = snapshot.get("value").cloned().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "registered schema snapshot missing value",
        )
    })?;
    let key = schema_key_from_definition(&schema)?;
    Ok(Some((key, schema)))
}
