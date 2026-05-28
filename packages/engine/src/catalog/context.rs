use std::collections::BTreeMap;

use serde_json::Value as JsonValue;

use crate::catalog::SchemaCatalogFact;
use crate::domain::{committed_row_is_exact_branch_scoped, Domain};
use crate::live_state::MaterializedLiveStateRow;
use crate::live_state::{LiveStateFilter, LiveStateReader, LiveStateScanRequest};
use crate::schema::schema_key_from_definition;
use crate::{LixError, NullableKeyFilter};

const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";

/// Engine schema visibility boundary.
///
/// SQL planning receives a schema snapshot from live state. System schemas are
/// seeded as ordinary `lix_registered_schema` rows during initialization, so
/// runtime schema visibility has one source of truth.
pub(crate) struct CatalogContext;

impl CatalogContext {
    pub(crate) fn new() -> Self {
        Self
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
                        "SQL surface schema '{}' is visible from more than one schema catalog fact",
                        schema_key
                    ),
                )
                .with_hint("SQL entity surfaces are named by schema_key. Keep exactly one visible schema per schema_key for SQL planning."));
            }
        }
        Ok(schemas.into_values().collect())
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
        let mut facts = Vec::new();
        for schema_domain in domain.schema_catalog_domains() {
            let rows = live_state
                .scan_rows(&LiveStateScanRequest {
                    filter: LiveStateFilter {
                        schema_keys: vec![REGISTERED_SCHEMA_KEY.to_string()],
                        branch_ids: vec![schema_domain.branch_id().to_string()],
                        file_ids: vec![NullableKeyFilter::Null],
                        untracked: Some(schema_domain.untracked()),
                        include_tombstones: false,
                        ..LiveStateFilter::default()
                    },
                    ..LiveStateScanRequest::default()
                })
                .await?;
            for row in rows
                .into_iter()
                .filter(|row| row_belongs_to_schema_catalog_domain(row, &schema_domain))
            {
                let Some((key, schema)) = decode_registered_schema_row(&row)? else {
                    continue;
                };
                facts.push(SchemaCatalogFact::new(schema_domain.clone(), key, schema));
            }
        }
        Ok(facts)
    }
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
    use crate::changelog::ChangeId;
    use crate::live_state::LiveStateRowRequest;
    use crate::GLOBAL_BRANCH_ID;

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
    }

    impl RowsLiveStateReader {
        fn new(rows: Vec<MaterializedLiveStateRow>) -> Self {
            Self { rows }
        }
    }

    #[async_trait]
    impl LiveStateReader for RowsLiveStateReader {
        async fn scan_rows(
            &self,
            request: &LiveStateScanRequest,
        ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
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
