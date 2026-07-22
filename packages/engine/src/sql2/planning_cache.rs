use std::hash::Hash;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::LixError;
use crate::sql2::catalog::PublicCatalog;
use crate::sql2::plan::LogicalWritePlan;
use datafusion::sql::parser::Statement as DataFusionStatement;
use lru::LruCache;

const PARSED_STATEMENT_CAPACITY: usize = 256;
const PUBLIC_CATALOG_CAPACITY: usize = 16;
const WRITE_PLAN_CAPACITY: usize = 256;

/// Bounded, engine-owned cache for snapshot-independent SQL planning templates.
///
/// Parsed statements depend only on the exact SQL text. Bound write plans also
/// depend on the visible catalog and active branch because binding resolves
/// public surfaces and injects branch scope. DataFusion plans deliberately do
/// not belong here: they capture providers tied to one storage snapshot.
pub(crate) struct SqlPlanningCache<CatalogKey> {
    parsed_statements: Mutex<LruCache<Arc<str>, Arc<DataFusionStatement>>>,
    public_catalogs: Mutex<LruCache<CatalogKey, Arc<PublicCatalog>>>,
    write_plans: Mutex<LruCache<WritePlanCacheKey<CatalogKey>, Arc<LogicalWritePlan>>>,
}

impl<CatalogKey> std::fmt::Debug for SqlPlanningCache<CatalogKey> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SqlPlanningCache")
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct WritePlanCacheKey<CatalogKey> {
    sql: Arc<str>,
    catalog: CatalogKey,
    active_branch_id: Arc<str>,
}

impl<CatalogKey> Default for SqlPlanningCache<CatalogKey>
where
    CatalogKey: Clone + Eq + Hash,
{
    fn default() -> Self {
        Self::with_capacities(
            PARSED_STATEMENT_CAPACITY,
            PUBLIC_CATALOG_CAPACITY,
            WRITE_PLAN_CAPACITY,
        )
    }
}

impl<CatalogKey> SqlPlanningCache<CatalogKey>
where
    CatalogKey: Clone + Eq + Hash,
{
    /// Parses exact SQL once and returns an owned clone of the cached AST.
    ///
    /// Parse failures are intentionally not cached. Two concurrent cold calls
    /// may do duplicate parsing, but parsing happens outside the mutex and the
    /// second insertion reuses the first successful template.
    pub(crate) fn parse_statement(&self, sql: &str) -> Result<DataFusionStatement, LixError> {
        let cached = lock_or_recover(&self.parsed_statements).get(sql).cloned();
        if let Some(statement) = cached {
            return Ok(statement.as_ref().clone());
        }

        let parsed = crate::sql2::parse::parse_statement(sql)?;
        let mut statements = lock_or_recover(&self.parsed_statements);
        if let Some(statement) = statements.get(sql).cloned() {
            drop(statements);
            return Ok(statement.as_ref().clone());
        }
        statements.put(Arc::from(sql), Arc::new(parsed.clone()));
        Ok(parsed)
    }

    /// Returns stable public-surface metadata for one catalog generation.
    ///
    /// Callers are responsible for using a key that changes whenever any
    /// visible schema changes. Invalid catalogs are not cached.
    pub(crate) fn public_catalog<F>(
        &self,
        catalog_key: &CatalogKey,
        visible_schemas: F,
    ) -> Result<Arc<PublicCatalog>, LixError>
    where
        F: FnOnce() -> Result<Vec<serde_json::Value>, LixError>,
    {
        if let Some(catalog) = lock_or_recover(&self.public_catalogs).get(catalog_key) {
            return Ok(Arc::clone(catalog));
        }

        let visible_schemas = visible_schemas()?;
        let built = Arc::new(PublicCatalog::from_visible_schemas(&visible_schemas)?);
        let mut catalogs = lock_or_recover(&self.public_catalogs);
        if let Some(catalog) = catalogs.get(catalog_key) {
            return Ok(Arc::clone(catalog));
        }
        catalogs.put(catalog_key.clone(), Arc::clone(&built));
        Ok(built)
    }

    /// Returns a cloned write template for this exact planning environment.
    ///
    /// Execution may resolve parameterized branch scopes by mutating its owned
    /// plan, so cache hits must never expose the stored template by reference.
    pub(crate) fn write_plan(
        &self,
        sql: &str,
        catalog_key: &CatalogKey,
        active_branch_id: &str,
    ) -> Option<LogicalWritePlan> {
        let key = WritePlanCacheKey::new(sql, catalog_key.clone(), active_branch_id);
        let cached = lock_or_recover(&self.write_plans).get(&key).cloned();
        cached.map(|plan| plan.as_ref().clone())
    }

    pub(crate) fn remember_write_plan(
        &self,
        sql: &str,
        catalog_key: CatalogKey,
        active_branch_id: &str,
        plan: &LogicalWritePlan,
    ) {
        let key = WritePlanCacheKey::new(sql, catalog_key, active_branch_id);
        lock_or_recover(&self.write_plans).put(key, Arc::new(plan.clone()));
    }

    fn with_capacities(
        parsed_statement_capacity: usize,
        public_catalog_capacity: usize,
        write_plan_capacity: usize,
    ) -> Self {
        Self {
            parsed_statements: Mutex::new(LruCache::new(non_zero(parsed_statement_capacity))),
            public_catalogs: Mutex::new(LruCache::new(non_zero(public_catalog_capacity))),
            write_plans: Mutex::new(LruCache::new(non_zero(write_plan_capacity))),
        }
    }
}

impl<CatalogKey> WritePlanCacheKey<CatalogKey> {
    fn new(sql: &str, catalog: CatalogKey, active_branch_id: &str) -> Self {
        Self {
            sql: Arc::from(sql),
            catalog,
            active_branch_id: Arc::from(active_branch_id),
        }
    }
}

fn non_zero(capacity: usize) -> NonZeroUsize {
    NonZeroUsize::new(capacity.max(1)).expect("SQL planning cache capacity is non-zero")
}

fn lock_or_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql2::bind_statement;
    use crate::sql2::plan::branch_scope::BranchScope;
    use serde_json::json;

    fn test_cache(capacity: usize) -> SqlPlanningCache<String> {
        SqlPlanningCache::with_capacities(capacity, capacity, capacity)
    }

    fn write_plan(sql: &str, active_branch_id: &str) -> LogicalWritePlan {
        let statement = crate::sql2::parse_statement(sql).expect("SQL parses");
        let bound = bind_statement(&statement, &[], active_branch_id).expect("SQL binds");
        crate::sql2::plan_write(bound).expect("SQL plans")
    }

    #[test]
    fn parsed_statements_are_keyed_by_exact_sql_and_bounded() {
        let cache = test_cache(2);
        let sql = "SELECT $1";

        let first = cache.parse_statement(sql).expect("first parse");
        let second = cache.parse_statement(sql).expect("cached parse");
        assert_eq!(first, second);
        assert_eq!(lock_or_recover(&cache.parsed_statements).len(), 1);

        cache
            .parse_statement("SELECT  $1")
            .expect("whitespace variant parses");
        assert_eq!(lock_or_recover(&cache.parsed_statements).len(), 2);

        cache.parse_statement("SELECT 2").expect("third SQL parses");
        let statements = lock_or_recover(&cache.parsed_statements);
        assert_eq!(statements.len(), 2);
        assert!(!statements.contains(sql));
    }

    #[test]
    fn parse_failures_are_not_cached() {
        let cache = test_cache(2);

        assert!(cache.parse_statement("SELECT (").is_err());
        assert!(lock_or_recover(&cache.parsed_statements).is_empty());
    }

    #[test]
    fn public_catalogs_reuse_stable_metadata_by_catalog_key() {
        let cache = test_cache(2);
        let catalog_a = "catalog-a".to_string();
        let catalog_b = "catalog-b".to_string();
        let schema = json!({
            "x-lix-key": "app_note",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "text": { "type": "string" }
            },
            "required": ["id", "text"]
        });

        let first = cache
            .public_catalog(&catalog_a, || Ok(vec![schema.clone()]))
            .expect("first catalog builds");
        let second = cache
            .public_catalog(&catalog_a, || Ok(vec![schema]))
            .expect("catalog cache hit");
        assert!(Arc::ptr_eq(&first, &second));
        assert!(first.surface("app_note").is_some());

        let system_only = cache
            .public_catalog(&catalog_b, || Ok(Vec::new()))
            .expect("second catalog builds");
        assert!(system_only.surface("app_note").is_none());
    }

    #[test]
    fn write_plans_are_keyed_by_sql_catalog_and_active_branch() {
        let cache = test_cache(8);
        let catalog_a = "catalog-a".to_string();
        let catalog_b = "catalog-b".to_string();
        let sql = "DELETE FROM lix_file WHERE id = $1";
        let plan = write_plan(sql, "branch-a");
        cache.remember_write_plan(sql, catalog_a.clone(), "branch-a", &plan);

        assert_eq!(cache.write_plan(sql, &catalog_a, "branch-a"), Some(plan));
        assert!(
            cache
                .write_plan(
                    "DELETE  FROM lix_file WHERE id = $1",
                    &catalog_a,
                    "branch-a"
                )
                .is_none()
        );
        assert!(cache.write_plan(sql, &catalog_b, "branch-a").is_none());
        assert!(cache.write_plan(sql, &catalog_a, "branch-b").is_none());
    }

    #[test]
    fn write_plan_hits_clone_the_immutable_template() {
        let cache = test_cache(2);
        let catalog = "catalog-a".to_string();
        let sql = "DELETE FROM lix_file WHERE id = $1";
        let plan = write_plan(sql, "branch-a");
        cache.remember_write_plan(sql, catalog.clone(), "branch-a", &plan);

        let mut first = cache
            .write_plan(sql, &catalog, "branch-a")
            .expect("first cache hit");
        first.bound.branch_scope = BranchScope::Empty;

        let second = cache
            .write_plan(sql, &catalog, "branch-a")
            .expect("second cache hit");
        assert_eq!(second, plan);
    }

    #[test]
    fn write_plan_cache_evicts_least_recently_used_template() {
        let cache = test_cache(2);
        let catalog = "catalog-a".to_string();
        let first_sql = "DELETE FROM lix_file WHERE id = 'first'";
        let second_sql = "DELETE FROM lix_file WHERE id = 'second'";
        let third_sql = "DELETE FROM lix_file WHERE id = 'third'";
        for sql in [first_sql, second_sql] {
            cache.remember_write_plan(
                sql,
                catalog.clone(),
                "branch-a",
                &write_plan(sql, "branch-a"),
            );
        }
        cache
            .write_plan(first_sql, &catalog, "branch-a")
            .expect("first entry is promoted");
        cache.remember_write_plan(
            third_sql,
            catalog.clone(),
            "branch-a",
            &write_plan(third_sql, "branch-a"),
        );

        assert!(cache.write_plan(first_sql, &catalog, "branch-a").is_some());
        assert!(cache.write_plan(second_sql, &catalog, "branch-a").is_none());
        assert!(cache.write_plan(third_sql, &catalog, "branch-a").is_some());
    }
}
