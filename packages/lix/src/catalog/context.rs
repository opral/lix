use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use blake3::Hasher;
use serde_json::Value as JsonValue;

use crate::catalog::revision::CatalogRevision;
use crate::catalog::snapshot::{
    CatalogFingerprint, fingerprint_schema_facts, hash_fingerprint_part,
};
use crate::catalog::{CatalogSnapshot, SchemaCatalogFact};
use crate::domain::Domain;
use crate::schema::schema_key_from_definition;
use crate::state::{ForkTreeStateView, StateRow};
use crate::{LixError, NullableKeyFilter};

const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";
const COMPILED_CATALOG_CACHE_LIMIT: usize = 64;

/// Engine schema visibility boundary over one authenticated ForkTree view.
pub(crate) struct CatalogContext {
    compiled_catalogs: Mutex<HashMap<CatalogFingerprint, Arc<CatalogSnapshot>>>,
    compiled_catalogs_by_rows: Mutex<HashMap<CatalogRowsFingerprint, Arc<CatalogSnapshot>>>,
    transaction_opening_catalogs:
        Mutex<HashMap<TransactionOpeningCatalogKey, Arc<CatalogSnapshot>>>,
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct CatalogRowsFingerprint(String);

#[derive(Clone, PartialEq, Eq, Hash)]
struct TransactionOpeningCatalogKey {
    domain: Domain,
    revision: CatalogRevision,
}

impl CatalogContext {
    pub(crate) fn new() -> Self {
        Self {
            compiled_catalogs: Mutex::new(HashMap::new()),
            compiled_catalogs_by_rows: Mutex::new(HashMap::new()),
            transaction_opening_catalogs: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) async fn compiled_catalog_for_transaction_open<R>(
        &self,
        state: &ForkTreeStateView<R>,
        domain: &Domain,
        revision: Option<&CatalogRevision>,
    ) -> Result<Arc<CatalogSnapshot>, LixError>
    where
        R: crate::storage_adapter::StorageAdapterRead,
    {
        let Some(revision) = revision else {
            return self.compiled_catalog_for_domain(state, domain).await;
        };
        let key = TransactionOpeningCatalogKey {
            domain: domain.clone(),
            revision: revision.clone(),
        };
        if let Some(snapshot) = self
            .transaction_opening_catalogs
            .lock()
            .expect("transaction opening catalog cache lock should not be poisoned")
            .get(&key)
        {
            return Ok(Arc::clone(snapshot));
        }

        let snapshot = self.compiled_catalog_for_domain(state, domain).await?;
        let mut cache = self
            .transaction_opening_catalogs
            .lock()
            .expect("transaction opening catalog cache lock should not be poisoned");
        if cache.len() >= COMPILED_CATALOG_CACHE_LIMIT {
            if let Some(evicted) = cache.keys().find(|entry| **entry != key).cloned() {
                cache.remove(&evicted);
            }
        }
        cache.insert(key, Arc::clone(&snapshot));
        Ok(snapshot)
    }

    pub(crate) async fn compiled_catalog_for_domain<R>(
        &self,
        state: &ForkTreeStateView<R>,
        domain: &Domain,
    ) -> Result<Arc<CatalogSnapshot>, LixError>
    where
        R: crate::storage_adapter::StorageAdapterRead,
    {
        let catalog_rows = scan_catalog_rows(state, domain).await?;
        self.compiled_catalog_for_rows(&catalog_rows)
    }

    pub(crate) fn compiled_catalog_for_state_rows(
        &self,
        catalog_rows: Vec<(Domain, Vec<StateRow>)>,
    ) -> Result<Arc<CatalogSnapshot>, LixError> {
        let catalog_rows = CatalogRows {
            domains: catalog_rows
                .into_iter()
                .map(|(domain, rows)| CatalogDomainRows { domain, rows })
                .collect(),
        };
        self.compiled_catalog_for_rows(&catalog_rows)
    }

    fn compiled_catalog_for_rows(
        &self,
        catalog_rows: &CatalogRows,
    ) -> Result<Arc<CatalogSnapshot>, LixError> {
        let mut hasher = Hasher::new();
        for (schema_domain, row) in catalog_rows.iter() {
            hash_fingerprint_part(&mut hasher, &schema_domain.fingerprint_component());
            let snapshot_content = state_snapshot_content(row).ok_or_else(|| {
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
    fn iter(&self) -> impl Iterator<Item = (&Domain, &StateRow)> {
        self.domains.iter().flat_map(|domain_rows| {
            domain_rows.rows.iter().filter_map(move |row| {
                row_belongs_to_schema_catalog_domain(row, &domain_rows.domain)
                    .then_some((&domain_rows.domain, row))
            })
        })
    }
}

struct CatalogDomainRows {
    domain: Domain,
    rows: Vec<StateRow>,
}

async fn scan_catalog_rows<R>(
    state: &ForkTreeStateView<R>,
    domain: &Domain,
) -> Result<CatalogRows, LixError>
where
    R: crate::storage_adapter::StorageAdapterRead,
{
    let schema_domains = domain.schema_catalog_domains();
    let mut catalog_rows = Vec::with_capacity(schema_domains.len());
    for schema_domain in schema_domains {
        if schema_domain.untracked() {
            return Err(LixError::new(
                LixError::CODE_STORAGE_ERROR,
                "untracked schema catalogs require the TransactionStateView untracked seam",
            ));
        }
        let rows = state.range(None, None, None, false).await?;
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

fn row_belongs_to_schema_catalog_domain(row: &StateRow, domain: &Domain) -> bool {
    let Ok(key) = crate::forktree::decode_state_key(&row.key) else {
        return false;
    };
    key.schema_key == REGISTERED_SCHEMA_KEY
        && key.file_id.is_none()
        && matches!(&row.value.cell, crate::forktree::StateCell::Value(_))
        && !domain.untracked()
}

fn decode_registered_schema_row(
    row: &StateRow,
) -> Result<Option<(crate::schema::SchemaKey, JsonValue)>, LixError> {
    let key = crate::forktree::decode_state_key(&row.key)?;
    if key.schema_key != REGISTERED_SCHEMA_KEY {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "expected lix_registered_schema row, got schema_key={}",
                key.schema_key
            ),
        ));
    }
    let crate::forktree::StateCell::Value(snapshot_content) = &row.value.cell else {
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

fn state_snapshot_content(row: &StateRow) -> Option<&str> {
    match &row.value.cell {
        crate::forktree::StateCell::Value(value) => Some(value.as_str()),
        crate::forktree::StateCell::Null | crate::forktree::StateCell::Tombstone => None,
    }
}
