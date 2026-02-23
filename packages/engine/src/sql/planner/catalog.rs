use std::collections::{BTreeMap, BTreeSet};

use crate::{LixBackend, LixError, SqlDialect, Value};

const MATERIALIZED_PREFIX: &str = "lix_internal_state_materialized_v1_";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct PlannerCatalogSnapshot {
    pub(crate) materialized_schema_keys: Vec<String>,
    pub(crate) schema_keys_by_plugin: BTreeMap<String, Vec<String>>,
}

impl PlannerCatalogSnapshot {
    pub(crate) fn schema_keys_for_plugins(&self, plugin_keys: &[String]) -> Vec<String> {
        let mut keys = BTreeSet::new();
        for plugin_key in plugin_keys {
            let normalized = plugin_key.to_ascii_lowercase();
            if let Some(schema_keys) = self.schema_keys_by_plugin.get(&normalized) {
                keys.extend(schema_keys.iter().cloned());
            }
        }
        keys.into_iter().collect()
    }
}

pub(crate) async fn load_planner_catalog_snapshot(
    backend: &dyn LixBackend,
) -> Result<PlannerCatalogSnapshot, LixError> {
    let materialized_schema_keys = load_materialized_schema_keys(backend).await?;
    let schema_keys_by_plugin = load_schema_keys_by_plugin(backend).await?;
    Ok(PlannerCatalogSnapshot {
        materialized_schema_keys,
        schema_keys_by_plugin,
    })
}

async fn load_materialized_schema_keys(backend: &dyn LixBackend) -> Result<Vec<String>, LixError> {
    let sql = match backend.dialect() {
        SqlDialect::Sqlite => {
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'lix_internal_state_materialized_v1_%'"
        }
        SqlDialect::Postgres => {
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = current_schema() \
               AND table_type = 'BASE TABLE' \
               AND table_name LIKE 'lix_internal_state_materialized_v1_%'"
        }
    };
    let result = backend.execute(sql, &[]).await?;

    let mut keys = BTreeSet::new();
    for row in &result.rows {
        let Some(Value::Text(name)) = row.first() else {
            continue;
        };
        let Some(schema_key) = name.strip_prefix(MATERIALIZED_PREFIX) else {
            continue;
        };
        if schema_key.is_empty() {
            continue;
        }
        keys.insert(schema_key.to_string());
    }
    Ok(keys.into_iter().collect())
}

async fn load_schema_keys_by_plugin(
    backend: &dyn LixBackend,
) -> Result<BTreeMap<String, Vec<String>>, LixError> {
    let mut selects = Vec::new();
    if internal_table_exists(backend, "lix_internal_change").await? {
        selects.push(
            "SELECT plugin_key, schema_key \
             FROM lix_internal_change \
             WHERE plugin_key IS NOT NULL AND schema_key IS NOT NULL"
                .to_string(),
        );
    }
    if internal_table_exists(backend, "lix_internal_state_untracked").await? {
        selects.push(
            "SELECT plugin_key, schema_key \
             FROM lix_internal_state_untracked \
             WHERE plugin_key IS NOT NULL AND schema_key IS NOT NULL"
                .to_string(),
        );
    }
    if selects.is_empty() {
        return Ok(BTreeMap::new());
    }

    let sql = selects.join(" UNION ");
    let result = backend
        .execute(&sql, &[])
        .await?;

    let mut map = BTreeMap::<String, BTreeSet<String>>::new();
    for row in &result.rows {
        let (Some(Value::Text(plugin_key)), Some(Value::Text(schema_key))) = (row.first(), row.get(1))
        else {
            continue;
        };
        if plugin_key.is_empty() || schema_key.is_empty() {
            continue;
        }
        map.entry(plugin_key.to_ascii_lowercase())
            .or_default()
            .insert(schema_key.clone());
    }

    Ok(map
        .into_iter()
        .map(|(plugin_key, schema_keys)| (plugin_key, schema_keys.into_iter().collect()))
        .collect())
}

async fn internal_table_exists(
    backend: &dyn LixBackend,
    table_name: &str,
) -> Result<bool, LixError> {
    let sql = match backend.dialect() {
        SqlDialect::Sqlite => format!(
            "SELECT 1 FROM sqlite_master \
             WHERE type = 'table' \
               AND name = '{table_name}' \
             LIMIT 1",
        ),
        SqlDialect::Postgres => format!(
            "SELECT 1 FROM information_schema.tables \
             WHERE table_schema = current_schema() \
               AND table_name = '{table_name}' \
             LIMIT 1",
        ),
    };
    let result = backend.execute(&sql, &[]).await?;
    Ok(!result.rows.is_empty())
}
