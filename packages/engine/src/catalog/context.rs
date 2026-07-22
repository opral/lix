use std::collections::{BTreeMap, HashMap};
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::Value as JsonValue;

use crate::catalog::revision::CatalogRevision;
use crate::catalog::snapshot::{
    CatalogFingerprint, fingerprint_schema_facts, hash_fingerprint_part,
};
use crate::catalog::{CatalogSnapshot, SchemaCatalogFact};
use crate::domain::{Domain, committed_row_is_exact_branch_scoped};
use crate::live_state::MaterializedLiveStateRow;
use crate::live_state::{LiveStateFilter, LiveStateReader, LiveStateScanRequest};
use crate::schema::schema_key_from_definition;
use crate::{LixError, NullableKeyFilter};

const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";

/// Compiled catalog snapshots are cached at most this many fact sets deep.
/// Schema catalogs churn rarely; the bound only guards against pathological
/// schema-mutation workloads growing the cache without limit.
const COMPILED_CATALOG_CACHE_LIMIT: usize = 64;

/// Engine schema visibility boundary.
///
/// Dynamic SQL planning receives a schema snapshot from live state. System
/// schemas are also seeded as ordinary `lix_registered_schema` rows for
/// validation and introspection, while fixed-surface reads may use their
/// compile-time definitions without loading those rows. The context also owns
/// engine-wide caches of compiled snapshots, keyed by either fact content or
/// the atomic storage revision captured when a transaction opens.
pub(crate) struct CatalogContext {
    compiled_catalogs: Mutex<HashMap<CatalogFingerprint, Arc<CatalogSnapshot>>>,
    compiled_catalogs_by_rows: Mutex<HashMap<CatalogRowsFingerprint, Arc<CatalogSnapshot>>>,
    transaction_opening_catalogs:
        Mutex<HashMap<TransactionOpeningCatalogKey, Arc<CatalogSnapshot>>>,
    #[cfg(test)]
    sql_read_schema_loads: AtomicUsize,
}

/// Fingerprint of the raw catalog rows visible to a domain, hashed before any
/// JSON decoding. The raw `snapshot_content` bytes uniquely determine the
/// decoded facts, so this key is at least as discriminating as the facts
/// fingerprint: textual or ordering variations can only cause a conservative
/// cache miss, never a wrong hit.
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
            #[cfg(test)]
            sql_read_schema_loads: AtomicUsize::new(0),
        }
    }

    /// Returns the catalog captured by a transaction-opening storage snapshot.
    ///
    /// The revision is loaded from the same pinned read as `live_state`. A
    /// matching `(domain, revision)` therefore proves that registered-schema
    /// visibility is identical without rescanning its tracked and untracked
    /// rows. Missing revisions conservatively retain the scan path for stores
    /// initialized by older engines.
    pub(crate) async fn compiled_catalog_for_transaction_open<R>(
        &self,
        live_state: &R,
        domain: &Domain,
        revision: Option<&CatalogRevision>,
    ) -> Result<Arc<CatalogSnapshot>, LixError>
    where
        R: LiveStateReader + ?Sized,
    {
        let Some(revision) = revision else {
            return self.compiled_catalog_for_domain(live_state, domain).await;
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

        let snapshot = self.compiled_catalog_for_domain(live_state, domain).await?;
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

    /// Returns the compiled snapshot for the catalog rows visible to `domain`.
    ///
    /// This is the transaction-opening cache-miss path and the authoritative
    /// path for validation overlays. Identical raw rows avoid JSON decoding and
    /// canonicalization, but still require the live-state scans.
    pub(crate) async fn compiled_catalog_for_domain<R>(
        &self,
        live_state: &R,
        domain: &Domain,
    ) -> Result<Arc<CatalogSnapshot>, LixError>
    where
        R: LiveStateReader + ?Sized,
    {
        let catalog_rows = scan_catalog_rows(live_state, domain).await?;
        let mut hasher = blake3::Hasher::new();
        for (schema_domain, row) in &catalog_rows {
            hash_fingerprint_part(&mut hasher, &schema_domain.fingerprint_component());
            let snapshot_content = row
                .snapshot_content
                .as_deref()
                .expect("catalog rows are filtered to rows with snapshot_content");
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

        let facts = facts_from_catalog_rows(&catalog_rows)?;
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

    /// Returns the compiled snapshot for `facts`, building it on first use.
    ///
    /// The cache key is the content fingerprint of the facts, so a cached
    /// snapshot can never go stale: changed schema rows produce different
    /// facts and therefore a different key. The lock is not held while
    /// compiling, so two racing callers may both compile the same facts; the
    /// results are identical and the last insert wins.
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
            // Evict one other entry instead of clearing: identical schema
            // content on N branches occupies N keys, so a full clear would
            // recompile every hot catalog whenever the bound is reached.
            if let Some(evicted) = cache.keys().find(|key| **key != fingerprint).cloned() {
                cache.remove(&evicted);
            }
        }
        cache.insert(fingerprint, Arc::clone(&snapshot));
        Ok(snapshot)
    }

    /// Loads schema definitions for SQL surface planning at `branch_id`.
    ///
    /// SQL surfaces are a read-planning projection over the active untracked
    /// schema catalog. Validation must use `schema_facts_for_domain` instead so
    /// schema durability remains explicit.
    pub(crate) async fn schema_jsons_for_sql_read_planning<R>(
        &self,
        live_state: &R,
        branch_id: &str,
    ) -> Result<Vec<JsonValue>, LixError>
    where
        R: LiveStateReader + ?Sized,
    {
        #[cfg(test)]
        self.sql_read_schema_loads.fetch_add(1, Ordering::Relaxed);
        let facts = self
            .schema_facts_for_domain(live_state, &Domain::schema_catalog(branch_id, true))
            .await?;
        let mut schemas = BTreeMap::<String, JsonValue>::new();
        for fact in facts {
            let schema_key = fact.catalog_key().schema_key.clone();
            if schemas
                .insert(schema_key.clone(), fact.schema().clone())
                .is_some()
            {
                return Err(LixError::new(
                    LixError::CODE_SCHEMA_DEFINITION,
                    format!(
                        "SQL surface schema '{schema_key}' is visible from more than one schema catalog fact"
                    ),
                )
                .with_hint("SQL entity surfaces are named by schema_key. Keep exactly one visible schema per schema_key for SQL planning."));
            }
        }
        Ok(schemas.into_values().collect())
    }

    #[cfg(test)]
    pub(crate) fn sql_read_schema_load_count_for_test(&self) -> usize {
        self.sql_read_schema_loads.load(Ordering::Relaxed)
    }

    /// Loads schema facts reachable from a row domain.
    pub(crate) async fn schema_facts_for_domain<R>(
        &self,
        live_state: &R,
        domain: &Domain,
    ) -> Result<Vec<SchemaCatalogFact>, LixError>
    where
        R: LiveStateReader + ?Sized,
    {
        let catalog_rows = scan_catalog_rows(live_state, domain).await?;
        facts_from_catalog_rows(&catalog_rows)
    }
}

/// Scans the raw registered-schema rows reachable from `domain`, without
/// decoding their snapshots.
async fn scan_catalog_rows<R>(
    live_state: &R,
    domain: &Domain,
) -> Result<Vec<(Domain, MaterializedLiveStateRow)>, LixError>
where
    R: LiveStateReader + ?Sized,
{
    let mut catalog_rows = Vec::new();
    for schema_domain in domain.schema_catalog_domains() {
        let request = LiveStateScanRequest {
            filter: LiveStateFilter {
                schema_keys: vec![REGISTERED_SCHEMA_KEY.to_string()],
                branch_ids: vec![schema_domain.branch_id().to_string()],
                file_ids: vec![NullableKeyFilter::Null],
                untracked: Some(schema_domain.untracked()),
                include_tombstones: false,
                ..LiveStateFilter::default()
            },
            ..LiveStateScanRequest::default()
        };
        let rows = if schema_domain.untracked() {
            live_state.scan_rows(&request).await?
        } else {
            live_state.scan_tracked_rows(&request).await?
        };
        catalog_rows.extend(
            rows.into_iter()
                .filter(|row| row_belongs_to_schema_catalog_domain(row, &schema_domain))
                .map(|row| (schema_domain.clone(), row)),
        );
    }
    Ok(catalog_rows)
}

fn facts_from_catalog_rows(
    catalog_rows: &[(Domain, MaterializedLiveStateRow)],
) -> Result<Vec<SchemaCatalogFact>, LixError> {
    let mut facts = Vec::new();
    for (schema_domain, row) in catalog_rows {
        let Some((key, schema)) = decode_registered_schema_row(row)? else {
            continue;
        };
        facts.push(SchemaCatalogFact::new(schema_domain.clone(), key, schema));
    }
    Ok(facts)
}

fn row_belongs_to_schema_catalog_domain(row: &MaterializedLiveStateRow, domain: &Domain) -> bool {
    row.schema_key == REGISTERED_SCHEMA_KEY
        && row.file_id.is_none()
        && row.snapshot_content.is_some()
        && row.branch_id == domain.branch_id()
        && row.untracked == domain.untracked()
        && committed_row_is_exact_branch_scoped(row, domain.branch_id())
}

fn decode_registered_schema_row(
    row: &MaterializedLiveStateRow,
) -> Result<Option<(crate::schema::SchemaKey, JsonValue)>, LixError> {
    if row.schema_key != REGISTERED_SCHEMA_KEY {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "expected lix_registered_schema row, got schema_key={}",
                row.schema_key
            ),
        ));
    }

    let Some(snapshot_content) = row.snapshot_content.as_deref() else {
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

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use serde_json::json;

    use super::*;
    use crate::GLOBAL_BRANCH_ID;
    use crate::changelog::ChangeId;
    use crate::live_state::LiveStateRowRequest;

    #[tokio::test]
    async fn compiled_catalog_for_domain_hits_cache_without_decoding() {
        let context = CatalogContext::new();
        let domain = Domain::schema_catalog("global", true);
        let reader = RowsLiveStateReader::new(vec![
            registered_schema_row("alpha_schema"),
            registered_schema_row("beta_schema"),
        ]);

        let first = context
            .compiled_catalog_for_domain(&reader, &domain)
            .await
            .expect("catalog should compile");
        let second = context
            .compiled_catalog_for_domain(&reader, &domain)
            .await
            .expect("catalog should hit the raw-rows cache");
        assert!(
            Arc::ptr_eq(&first, &second),
            "identical raw rows must return the cached snapshot"
        );

        let changed_reader = RowsLiveStateReader::new(vec![
            registered_schema_row("alpha_schema"),
            registered_schema_row("gamma_schema"),
        ]);
        let changed = context
            .compiled_catalog_for_domain(&changed_reader, &domain)
            .await
            .expect("changed catalog should compile");
        assert!(
            !Arc::ptr_eq(&first, &changed),
            "changed raw rows must compile a different snapshot"
        );
        assert!(changed.contains("gamma_schema"));
        assert!(!first.contains("gamma_schema"));
    }

    #[tokio::test]
    async fn transaction_opening_revision_skips_catalog_row_scans_until_it_changes() {
        let context = CatalogContext::new();
        let domain = Domain::schema_catalog("global", true);
        let revision = CatalogRevision::for_test(b"revision-one");
        let reader = RowsLiveStateReader::new(vec![registered_schema_row("alpha_schema")]);

        let first = context
            .compiled_catalog_for_transaction_open(&reader, &domain, Some(&revision))
            .await
            .expect("opening catalog should compile");
        assert_eq!(
            reader.scan_count(),
            2,
            "cold open scans both durability scopes"
        );
        let second = context
            .compiled_catalog_for_transaction_open(&reader, &domain, Some(&revision))
            .await
            .expect("opening catalog should hit by revision");
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(
            reader.scan_count(),
            2,
            "hot open must not rescan registered-schema rows"
        );

        let changed_reader = RowsLiveStateReader::new(vec![registered_schema_row("beta_schema")]);
        let changed = context
            .compiled_catalog_for_transaction_open(
                &changed_reader,
                &domain,
                Some(&CatalogRevision::for_test(b"revision-two")),
            )
            .await
            .expect("changed revision should reload the catalog");
        assert_eq!(changed_reader.scan_count(), 2);
        assert!(changed.contains("beta_schema"));
        assert!(!changed.contains("alpha_schema"));
    }

    #[tokio::test]
    async fn missing_transaction_opening_revision_conservatively_rescans() {
        let context = CatalogContext::new();
        let domain = Domain::schema_catalog("global", true);
        let reader = RowsLiveStateReader::new(vec![registered_schema_row("alpha_schema")]);

        context
            .compiled_catalog_for_transaction_open(&reader, &domain, None)
            .await
            .expect("first opening catalog should compile");
        context
            .compiled_catalog_for_transaction_open(&reader, &domain, None)
            .await
            .expect("second opening catalog should compile");

        assert_eq!(reader.scan_count(), 4);
    }

    #[test]
    fn compiled_catalog_cache_shares_snapshots_for_equal_facts() {
        let context = CatalogContext::new();
        let parent = catalog_fact("parent_schema");
        let child = catalog_fact("child_schema");

        let first = context
            .compiled_catalog_for_facts(&[parent.clone(), child.clone()])
            .expect("catalog should compile");
        let reordered = context
            .compiled_catalog_for_facts(&[child, parent.clone()])
            .expect("catalog should compile");
        let different = context
            .compiled_catalog_for_facts(&[parent])
            .expect("catalog should compile");

        assert!(
            Arc::ptr_eq(&first, &reordered),
            "equal facts in any order must hit the same cached snapshot"
        );
        assert!(
            !Arc::ptr_eq(&first, &different),
            "different facts must compile a different snapshot"
        );
    }

    fn catalog_fact(schema_key: &str) -> SchemaCatalogFact {
        SchemaCatalogFact::new(
            Domain::schema_catalog("main", false),
            crate::schema::SchemaKey::new(schema_key),
            json!({
                "x-lix-key": schema_key,
                "x-lix-primary-key": ["/id"],
                "type": "object",
                "properties": {
                    "id": { "type": "string" }
                },
                "required": ["id"],
                "additionalProperties": false
            }),
        )
    }

    #[tokio::test]
    async fn visible_schemas_are_loaded_from_registered_schema_rows() {
        let context = CatalogContext::new();

        let schemas = context
            .schema_jsons_for_sql_read_planning(
                &RowsLiveStateReader::new(vec![
                    registered_schema_row("lix_registered_schema"),
                    registered_schema_row("lix_key_value"),
                ]),
                "global",
            )
            .await
            .expect("schema visibility should load");

        assert!(schemas.iter().any(|schema| {
            schema.get("x-lix-key").and_then(JsonValue::as_str) == Some("lix_registered_schema")
        }));
        assert!(schemas.iter().any(|schema| {
            schema.get("x-lix-key").and_then(JsonValue::as_str) == Some("lix_key_value")
        }));
    }

    #[tokio::test]
    async fn compiled_catalog_projects_the_same_sql_visible_schemas() {
        let context = CatalogContext::new();
        let mut tracked = registered_schema_row("zeta_tracked_schema");
        tracked.untracked = false;
        let reader = RowsLiveStateReader::new(vec![
            registered_schema_row("alpha_untracked_schema"),
            tracked,
        ]);
        let domain = Domain::schema_catalog("global", true);

        let durable_projection = context
            .schema_jsons_for_sql_read_planning(&reader, "global")
            .await
            .expect("SQL schema visibility should load");
        let compiled_projection = context
            .compiled_catalog_for_domain(&reader, &domain)
            .await
            .expect("catalog should compile")
            .schema_jsons();

        assert_eq!(compiled_projection, durable_projection);
    }

    #[tokio::test]
    async fn visible_schemas_include_registered_schema_rows() {
        let context = CatalogContext::new();

        let schemas = context
            .schema_jsons_for_sql_read_planning(
                &RowsLiveStateReader::new(vec![registered_schema_row("engine_dynamic_schema")]),
                "global",
            )
            .await
            .expect("schema visibility should load");

        assert!(schemas.iter().any(|schema| {
            schema.get("x-lix-key").and_then(JsonValue::as_str) == Some("engine_dynamic_schema")
        }));
    }

    #[tokio::test]
    async fn sql_read_planning_rejects_multiple_visible_schemas_for_same_surface() {
        let context = CatalogContext::new();
        let error = context
            .schema_jsons_for_sql_read_planning(
                &RowsLiveStateReader::new(vec![
                    registered_schema_row("engine_dynamic_schema"),
                    registered_schema_row("engine_dynamic_schema"),
                ]),
                "global",
            )
            .await
            .expect_err("SQL surfaces must not choose a schema identity implicitly");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(error.message.contains("SQL surface schema"));
    }

    #[tokio::test]
    async fn tracked_domain_sees_tracked_seed_schemas_but_not_user_untracked_schemas() {
        let context = CatalogContext::new();
        let mut seed_schema = registered_schema_row("lix_key_value");
        seed_schema.untracked = false;

        let facts = context
            .schema_facts_for_domain(
                &RowsLiveStateReader::new(vec![
                    seed_schema,
                    registered_schema_row("engine_dynamic_schema"),
                ]),
                &Domain::schema_catalog("global", false),
            )
            .await
            .expect("schema visibility should load");
        let schemas = facts
            .iter()
            .map(SchemaCatalogFact::schema)
            .collect::<Vec<_>>();

        assert!(schemas.iter().any(|schema| {
            schema.get("x-lix-key").and_then(JsonValue::as_str) == Some("lix_key_value")
        }));
        assert!(!schemas.iter().any(|schema| {
            schema.get("x-lix-key").and_then(JsonValue::as_str) == Some("engine_dynamic_schema")
        }));
    }

    #[tokio::test]
    async fn tracked_domain_does_not_see_untracked_seed_schemas() {
        let context = CatalogContext::new();

        let facts = context
            .schema_facts_for_domain(
                &RowsLiveStateReader::new(vec![registered_schema_row("lix_key_value")]),
                &Domain::schema_catalog("global", false),
            )
            .await
            .expect("schema visibility should load");
        let schemas = facts
            .iter()
            .map(SchemaCatalogFact::schema)
            .collect::<Vec<_>>();

        assert!(!schemas.iter().any(|schema| {
            schema.get("x-lix-key").and_then(JsonValue::as_str) == Some("lix_key_value")
        }));
    }

    #[tokio::test]
    async fn visible_schemas_ignore_projected_global_schema_rows_for_branch_scope() {
        let context = CatalogContext::new();
        let mut global_only = registered_schema_row("global_only_schema");
        global_only.global = true;
        global_only.branch_id = "main".to_string();

        let schemas = context
            .schema_jsons_for_sql_read_planning(
                &RowsLiveStateReader::new(vec![global_only]),
                "main",
            )
            .await
            .expect("schema visibility should load");

        assert!(schemas.is_empty());
    }

    #[tokio::test]
    async fn schema_facts_post_filter_non_catalog_rows_even_if_reader_returns_them() {
        let context = CatalogContext::new();
        let valid_schema = registered_schema_row("valid_schema");
        let mut file_scoped_schema = registered_schema_row("file_scoped_schema");
        file_scoped_schema.file_id = Some("file-a".to_string());
        let mut tombstoned_schema = registered_schema_row("tombstoned_schema");
        tombstoned_schema.snapshot_content = None;

        let facts = context
            .schema_facts_for_domain(
                &RowsLiveStateReader::new(vec![
                    valid_schema,
                    file_scoped_schema,
                    tombstoned_schema,
                ]),
                &Domain::schema_catalog("global", true),
            )
            .await
            .expect("schema facts should load");
        let schema_keys = facts
            .iter()
            .filter_map(|fact| fact.schema().get("x-lix-key").and_then(JsonValue::as_str))
            .collect::<Vec<_>>();

        assert_eq!(schema_keys, vec!["valid_schema"]);
    }

    #[tokio::test]
    async fn visible_schemas_are_empty_when_no_schema_rows_are_visible() {
        let context = CatalogContext::new();

        let schemas = context
            .schema_jsons_for_sql_read_planning(&RowsLiveStateReader::new(Vec::new()), "global")
            .await
            .expect("schema visibility should load");

        assert!(schemas.is_empty());
    }

    struct RowsLiveStateReader {
        rows: Vec<MaterializedLiveStateRow>,
        scan_count: AtomicUsize,
    }

    impl RowsLiveStateReader {
        fn new(rows: Vec<MaterializedLiveStateRow>) -> Self {
            Self {
                rows,
                scan_count: AtomicUsize::new(0),
            }
        }

        fn scan_count(&self) -> usize {
            self.scan_count.load(Ordering::Relaxed)
        }
    }

    #[async_trait]
    impl LiveStateReader for RowsLiveStateReader {
        async fn load_exact_rows(
            &self,
            request: &crate::live_state::LiveStateExactBatchRequest,
        ) -> Result<Vec<Option<MaterializedLiveStateRow>>, LixError> {
            crate::live_state::load_exact_rows_via_scan_for_test(self, request).await
        }

        async fn scan_rows(
            &self,
            request: &LiveStateScanRequest,
        ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
            self.scan_count.fetch_add(1, Ordering::Relaxed);
            Ok(self
                .rows
                .iter()
                .filter(|row| {
                    request.filter.schema_keys.is_empty()
                        || request.filter.schema_keys.contains(&row.schema_key)
                })
                .filter(|row| {
                    request.filter.branch_ids.is_empty()
                        || request.filter.branch_ids.contains(&row.branch_id)
                })
                .filter(|row| {
                    request
                        .filter
                        .untracked
                        .is_none_or(|untracked| row.untracked == untracked)
                })
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
                .find(|row| {
                    row.schema_key == request.schema_key
                        && row.branch_id == request.branch_id
                        && row.entity_pk == request.entity_pk
                })
                .cloned())
        }
    }

    fn registered_schema_row(schema_key: &str) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            entity_pk: registered_schema_entity_pk(schema_key),
            file_id: None,
            schema_key: REGISTERED_SCHEMA_KEY.to_string(),
            branch_id: GLOBAL_BRANCH_ID.to_string(),
            metadata: None,
            deleted: false,
            change_id: Some(ChangeId::for_test_label("change-registered-schema")),
            commit_id: None,
            global: true,
            untracked: true,
            created_at: "2026-04-23T00:00:00Z".to_string(),
            updated_at: "2026-04-23T01:00:00Z".to_string(),
            snapshot_content: Some(
                json!({
                    "value": {
                        "x-lix-key": schema_key,
                        "type": "object",
                        "properties": {
                            "id": { "type": "string" }
                        },
                        "required": ["id"],
                        "additionalProperties": false
                    }
                })
                .to_string(),
            ),
        }
    }

    fn registered_schema_entity_pk(schema_key: &str) -> crate::entity_pk::EntityPk {
        crate::entity_pk::EntityPk::from_primary_key_paths(
            &json!({
                "value": {
                    "x-lix-key": schema_key,
                }
            }),
            &[vec!["value".to_string(), "x-lix-key".to_string()]],
        )
        .expect("registered schema identity should derive")
    }
}
