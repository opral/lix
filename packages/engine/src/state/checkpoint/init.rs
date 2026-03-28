use crate::init::seed::{quote_ident, text_value};
use crate::init::tables::execute_init_statements;
use crate::init::InitExecutor;
use crate::live_state::schema_access::{payload_column_name_for_schema, tracked_relation_name};
use crate::Value;
use crate::{LixBackend, LixError};

const CHECKPOINT_INIT_STATEMENTS: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS lix_internal_last_checkpoint (\
     version_id TEXT PRIMARY KEY,\
     checkpoint_commit_id TEXT NOT NULL\
     )",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_last_checkpoint_commit \
     ON lix_internal_last_checkpoint (checkpoint_commit_id)",
];

pub(crate) async fn init(backend: &dyn LixBackend) -> Result<(), LixError> {
    execute_init_statements(backend, "state::checkpoint", CHECKPOINT_INIT_STATEMENTS).await
}

pub(crate) async fn seed_bootstrap(executor: &mut InitExecutor<'_, '_>) -> Result<(), LixError> {
    executor.seed_default_checkpoint_label().await?;
    executor.rebuild_internal_last_checkpoint().await
}

impl<'engine, 'tx> InitExecutor<'engine, 'tx> {
    pub(crate) async fn seed_default_checkpoint_label(&mut self) -> Result<(), LixError> {
        let bootstrap_commit_id = self.load_global_version_commit_id().await?;
        let existing = self
            .execute_internal(
                "SELECT snapshot_content \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_label' \
                   AND entity_id = $1 \
                   AND file_id = 'lix' \
                   AND version_id = 'global' \
                   AND snapshot_content IS NOT NULL \
                 ORDER BY updated_at DESC, created_at DESC, change_id DESC \
                 LIMIT 1",
                &[Value::Text(super::CHECKPOINT_LABEL_ID.to_string())],
            )
            .await?;
        let [statement] = existing.statements.as_slice() else {
            return Err(crate::errors::unexpected_statement_count_error(
                "default checkpoint label query",
                1,
                existing.statements.len(),
            ));
        };
        if let Some(row) = statement.rows.first() {
            let Some(Value::Text(snapshot_content)) = row.first() else {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "checkpoint label snapshot_content must be text",
                ));
            };
            let parsed: serde_json::Value = serde_json::from_str(snapshot_content.as_str())
                .map_err(|error| LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: format!("checkpoint label snapshot invalid JSON: {error}"),
                })?;
            let id = parsed.get("id").and_then(serde_json::Value::as_str);
            let name = parsed.get("name").and_then(serde_json::Value::as_str);
            if id != Some(super::CHECKPOINT_LABEL_ID) || name != Some(super::CHECKPOINT_LABEL_NAME)
            {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "checkpoint label canonical row is present but invalid",
                ));
            }
            self.ensure_checkpoint_label_on_bootstrap_commit(
                &bootstrap_commit_id,
                super::CHECKPOINT_LABEL_ID,
            )
            .await?;
            return Ok(());
        }

        let snapshot_content = serde_json::json!({
            "id": super::CHECKPOINT_LABEL_ID,
            "name": super::CHECKPOINT_LABEL_NAME,
        })
        .to_string();
        self.insert_bootstrap_tracked_row(
            Some(&bootstrap_commit_id),
            super::CHECKPOINT_LABEL_ID,
            "lix_label",
            "1",
            "lix",
            "global",
            "lix",
            &snapshot_content,
        )
        .await?;

        self.ensure_checkpoint_label_on_bootstrap_commit(
            &bootstrap_commit_id,
            super::CHECKPOINT_LABEL_ID,
        )
        .await?;
        Ok(())
    }

    async fn ensure_checkpoint_label_on_bootstrap_commit(
        &mut self,
        bootstrap_commit_id: &str,
        label_id: &str,
    ) -> Result<(), LixError> {
        let entity_label_id = super::checkpoint_commit_label_entity_id(bootstrap_commit_id);
        let existing = self
            .execute_internal(
                "SELECT 1 \
                 FROM lix_state_by_version \
                 WHERE entity_id = $1 \
                   AND schema_key = 'lix_entity_label' \
                   AND file_id = 'lix' \
                   AND version_id = 'global' \
                   AND snapshot_content IS NOT NULL \
                 LIMIT 1",
                &[Value::Text(entity_label_id.clone())],
            )
            .await?;
        let [statement] = existing.statements.as_slice() else {
            return Err(crate::errors::unexpected_statement_count_error(
                "checkpoint label bootstrap link existence query",
                1,
                existing.statements.len(),
            ));
        };
        if !statement.rows.is_empty() {
            return Ok(());
        }

        if label_id != super::CHECKPOINT_LABEL_ID {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("unexpected checkpoint label id '{label_id}'"),
            ));
        }
        let snapshot_content = super::checkpoint_commit_label_snapshot(bootstrap_commit_id);
        self.insert_bootstrap_tracked_row(
            Some(bootstrap_commit_id),
            &entity_label_id,
            "lix_entity_label",
            "1",
            "lix",
            "global",
            "lix",
            &snapshot_content,
        )
        .await?;

        Ok(())
    }

    pub(crate) async fn insert_last_checkpoint_for_version(
        &mut self,
        version_id: &str,
        checkpoint_commit_id: &str,
    ) -> Result<(), LixError> {
        self.execute_backend(
            "INSERT INTO lix_internal_last_checkpoint (version_id, checkpoint_commit_id) \
             VALUES ($1, $2)",
            &[
                Value::Text(version_id.to_string()),
                Value::Text(checkpoint_commit_id.to_string()),
            ],
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn rebuild_internal_last_checkpoint(&mut self) -> Result<(), LixError> {
        let versions = self
            .execute_internal(
                "SELECT id, commit_id \
                 FROM lix_version \
                 ORDER BY id",
                &[],
            )
            .await?;
        let [statement] = versions.statements.as_slice() else {
            return Err(crate::errors::unexpected_statement_count_error(
                "rebuild_internal_last_checkpoint query",
                1,
                versions.statements.len(),
            ));
        };

        self.execute_backend("DELETE FROM lix_internal_last_checkpoint", &[])
            .await?;

        let global_commit_id = self.load_global_version_commit_id().await?;
        let global_checkpoint_commit_id = self
            .resolve_last_checkpoint_commit_id_for_tip(&global_commit_id)
            .await?
            .unwrap_or_else(|| global_commit_id.clone());
        self.insert_last_checkpoint_for_version(
            crate::version::GLOBAL_VERSION_ID,
            &global_checkpoint_commit_id,
        )
        .await?;

        for row in &statement.rows {
            let version_id = text_value(row.get(0), "lix_version.id")?;
            if version_id == crate::version::GLOBAL_VERSION_ID {
                continue;
            }
            let commit_id = text_value(row.get(1), "lix_version.commit_id")?;
            let checkpoint_commit_id = self
                .resolve_last_checkpoint_commit_id_for_tip(&commit_id)
                .await?
                .unwrap_or_else(|| commit_id.clone());
            self.insert_last_checkpoint_for_version(&version_id, &checkpoint_commit_id)
                .await?;
        }

        Ok(())
    }

    async fn resolve_last_checkpoint_commit_id_for_tip(
        &mut self,
        head_commit_id: &str,
    ) -> Result<Option<String>, LixError> {
        let commit_edge_parent =
            payload_column_name_for_schema("lix_commit_edge", None, "parent_id")?;
        let commit_edge_child =
            payload_column_name_for_schema("lix_commit_edge", None, "child_id")?;
        let entity_label_entity_id =
            payload_column_name_for_schema("lix_entity_label", None, "entity_id")?;
        let entity_label_schema_key =
            payload_column_name_for_schema("lix_entity_label", None, "schema_key")?;
        let entity_label_label_id =
            payload_column_name_for_schema("lix_entity_label", None, "label_id")?;
        let commit_edge_table = tracked_relation_name("lix_commit_edge");
        let entity_label_table = tracked_relation_name("lix_entity_label");
        let commit_table = tracked_relation_name("lix_commit");
        let rows = self
            .execute_internal(
                &format!(
                    "WITH RECURSIVE reachable(commit_id, depth) AS ( \
                       SELECT $1 AS commit_id, 0 AS depth \
                       UNION ALL \
                       SELECT \
                         edge.__PARENT_ID__ AS commit_id, \
                         reachable.depth + 1 AS depth \
                       FROM reachable \
                       JOIN {commit_edge_table} edge \
                         ON edge.__CHILD_ID__ = reachable.commit_id \
                       WHERE edge.schema_key = 'lix_commit_edge' \
                         AND edge.version_id = 'global' \
                         AND edge.is_tombstone = 0 \
                         AND edge.__PARENT_ID__ IS NOT NULL \
                     ) \
                     SELECT reachable.commit_id \
                     FROM reachable \
                     JOIN ( \
                       SELECT \
                         {entity_label_entity_id} AS entity_id, \
                         {entity_label_schema_key} AS schema_key, \
                         {entity_label_label_id} AS label_id \
                       FROM {entity_label_table} \
                       WHERE schema_key = 'lix_entity_label' \
                         AND file_id = 'lix' \
                         AND version_id = 'global' \
                         AND is_tombstone = 0 \
                         AND {entity_label_entity_id} IS NOT NULL \
                         AND {entity_label_schema_key} IS NOT NULL \
                         AND {entity_label_label_id} IS NOT NULL \
                     ) el \
                       ON el.entity_id = reachable.commit_id \
                      AND el.schema_key = 'lix_commit' \
                      AND el.label_id = '{checkpoint_label_id}' \
                     LEFT JOIN ( \
                       SELECT entity_id AS id, created_at \
                       FROM {commit_table} \
                       WHERE schema_key = 'lix_commit' \
                         AND file_id = 'lix' \
                         AND version_id = 'global' \
                         AND is_tombstone = 0 \
                     ) c ON c.id = reachable.commit_id \
                     ORDER BY \
                       reachable.depth ASC, \
                       c.created_at DESC, \
                       reachable.commit_id DESC \
                     LIMIT 1",
                    checkpoint_label_id =
                        crate::sql_support::text::escape_sql_string(super::CHECKPOINT_LABEL_ID),
                    entity_label_entity_id = quote_ident(&entity_label_entity_id),
                    entity_label_schema_key = quote_ident(&entity_label_schema_key),
                    entity_label_label_id = quote_ident(&entity_label_label_id),
                    entity_label_table = quote_ident(&entity_label_table),
                    commit_table = quote_ident(&commit_table),
                )
                .replace("__PARENT_ID__", &commit_edge_parent)
                .replace("__CHILD_ID__", &commit_edge_child),
                &[Value::Text(head_commit_id.to_string())],
            )
            .await?;
        let [statement] = rows.statements.as_slice() else {
            return Err(crate::errors::unexpected_statement_count_error(
                "resolve checkpoint ancestor query",
                1,
                rows.statements.len(),
            ));
        };
        let Some(first) = statement.rows.first() else {
            return Ok(None);
        };
        Ok(Some(text_value(first.get(0), "checkpoint ancestor id")?))
    }
}
