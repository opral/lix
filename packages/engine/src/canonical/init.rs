use crate::errors;
use crate::init::seed::read_scalar_count;
use crate::init::tables::execute_init_statements;
use crate::init::InitExecutor;
use crate::Value;
use crate::{LixBackend, LixError};

const CANONICAL_INIT_STATEMENTS: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS lix_internal_commit_graph_node (\
     commit_id TEXT PRIMARY KEY,\
     generation BIGINT NOT NULL\
     )",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_commit_graph_node_generation \
     ON lix_internal_commit_graph_node (generation)",
    "CREATE TABLE IF NOT EXISTS lix_internal_entity_state_timeline_breakpoint (\
     root_commit_id TEXT NOT NULL,\
     entity_id TEXT NOT NULL,\
     schema_key TEXT NOT NULL,\
     file_id TEXT NOT NULL,\
     from_depth BIGINT NOT NULL,\
     plugin_key TEXT NOT NULL,\
     schema_version TEXT NOT NULL,\
     metadata TEXT,\
     snapshot_id TEXT NOT NULL,\
     change_id TEXT NOT NULL,\
     PRIMARY KEY (root_commit_id, entity_id, schema_key, file_id, from_depth)\
     )",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_entity_state_timeline_breakpoint_root_depth \
     ON lix_internal_entity_state_timeline_breakpoint (root_commit_id, from_depth)",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_entity_state_timeline_breakpoint_lookup \
     ON lix_internal_entity_state_timeline_breakpoint (root_commit_id, entity_id, file_id, schema_key, from_depth)",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_entity_state_timeline_breakpoint_filters \
     ON lix_internal_entity_state_timeline_breakpoint (root_commit_id, file_id, plugin_key, schema_key, entity_id, from_depth)",
    "CREATE TABLE IF NOT EXISTS lix_internal_timeline_status (\
     root_commit_id TEXT PRIMARY KEY,\
     built_max_depth BIGINT NOT NULL,\
     built_at TEXT NOT NULL\
     )",
];

pub(crate) async fn init(backend: &dyn LixBackend) -> Result<(), LixError> {
    execute_init_statements(backend, "canonical", CANONICAL_INIT_STATEMENTS).await
}

pub(crate) async fn seed_bootstrap(executor: &mut InitExecutor<'_, '_>) -> Result<(), LixError> {
    executor.seed_commit_graph_nodes().await
}

impl<'engine, 'tx> InitExecutor<'engine, 'tx> {
    pub(crate) async fn seed_commit_graph_nodes(&mut self) -> Result<(), LixError> {
        let graph_count_result = self
            .execute_backend(
                &format!(
                    "SELECT COUNT(*) FROM {}",
                    crate::canonical::graph::COMMIT_GRAPH_NODE_TABLE
                ),
                &[],
            )
            .await?;
        let graph_count =
            read_scalar_count(&graph_count_result, "lix_internal_commit_graph_node count")?;
        if graph_count > 0 {
            return Ok(());
        }

        let commit_table = crate::live_state::schema_access::tracked_relation_name("lix_commit");
        let commit_count_result = self
            .execute_backend(
                &format!(
                    "SELECT COUNT(*) \
                     FROM {commit_table} \
                     WHERE schema_key = 'lix_commit' \
                       AND version_id = 'global' \
                       AND is_tombstone = 0"
                ),
                &[],
            )
            .await?;
        let commit_count = read_scalar_count(&commit_count_result, "lix_commit count")?;
        if commit_count == 0 {
            return Ok(());
        }

        self.execute_backend(
            &crate::canonical::graph::build_commit_generation_seed_sql(),
            &[],
        )
        .await?;

        Ok(())
    }

    pub(crate) async fn seed_canonical_version_descriptor(
        &mut self,
        bootstrap_commit_id: &str,
        entity_id: &str,
        name: &str,
    ) -> Result<String, LixError> {
        let snapshot_content = crate::version::version_descriptor_snapshot_content(
            entity_id,
            name,
            entity_id == crate::version::GLOBAL_VERSION_ID,
        );
        let change_id = self.generate_runtime_uuid().await?;
        let timestamp = self.generate_runtime_timestamp().await?;
        self.insert_change_row_for_snapshot(
            entity_id,
            crate::version::version_descriptor_schema_key(),
            crate::version::version_descriptor_schema_version(),
            crate::version::version_descriptor_file_id(),
            crate::version::version_descriptor_plugin_key(),
            &snapshot_content,
            &change_id,
            &timestamp,
        )
        .await?;
        self.add_change_id_to_commit(bootstrap_commit_id, &change_id)
            .await?;
        Ok(change_id)
    }

    pub(crate) async fn seed_bootstrap_commit(
        &mut self,
        commit_id: &str,
        change_set_id: &str,
    ) -> Result<(), LixError> {
        let existing = self
            .execute_internal(
                "SELECT 1 \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_commit' \
                   AND entity_id = $1 \
                   AND file_id = 'lix' \
                   AND version_id = 'global' \
                   AND snapshot_content IS NOT NULL \
                 LIMIT 1",
                &[Value::Text(commit_id.to_string())],
            )
            .await?;
        let [statement] = existing.statements.as_slice() else {
            return Err(errors::unexpected_statement_count_error(
                "seed_bootstrap_commit existence query",
                1,
                existing.statements.len(),
            ));
        };
        if !statement.rows.is_empty() {
            return Ok(());
        }

        let snapshot_content = serde_json::json!({
            "id": commit_id,
            "change_set_id": change_set_id,
            "parent_commit_ids": [],
            "change_ids": [],
        })
        .to_string();
        self.insert_bootstrap_tracked_row(
            None,
            commit_id,
            "lix_commit",
            "1",
            "lix",
            "global",
            "lix",
            &snapshot_content,
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn seed_bootstrap_change_set(
        &mut self,
        change_set_id: &str,
    ) -> Result<(), LixError> {
        let existing = self
            .execute_internal(
                "SELECT 1 \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_change_set' \
                   AND entity_id = $1 \
                   AND file_id = 'lix' \
                   AND version_id = 'global' \
                   AND snapshot_content IS NOT NULL \
                 LIMIT 1",
                &[Value::Text(change_set_id.to_string())],
            )
            .await?;
        let [statement] = existing.statements.as_slice() else {
            return Err(errors::unexpected_statement_count_error(
                "seed_bootstrap_change_set existence query",
                1,
                existing.statements.len(),
            ));
        };
        if !statement.rows.is_empty() {
            return Ok(());
        }

        let snapshot_content = serde_json::json!({ "id": change_set_id }).to_string();
        self.insert_bootstrap_tracked_row(
            None,
            change_set_id,
            "lix_change_set",
            "1",
            "lix",
            "global",
            "lix",
            &snapshot_content,
        )
        .await?;
        Ok(())
    }
}
