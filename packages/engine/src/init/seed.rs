use std::collections::BTreeMap;

use crate::backend::{transaction_backend_view, QueryExecutor};
use crate::canonical::{
    append_changes, load_exact_committed_change_from_commit_with_executor,
    ExactCommittedStateRowRequest, UpdatedVersionRef,
};
use crate::functions::FunctionBindings;
use crate::functions::LixFunctionProvider;
use crate::live_state::{
    key_value_schema_key, key_value_schema_version, load_exact_live_row,
    load_version_head_commit_id_with_executor, load_version_head_commit_map_with_executor,
    write_live_rows, ExactLiveRowQuery, LiveRow, LiveRowSource,
};
use crate::session::{
    canonical_changes_from_updated_version_refs, untracked_live_rows_from_updated_version_refs,
};
use crate::transaction::{
    upsert_registered_schema_mirror_row_in_transaction, RegisteredSchemaMirrorRow,
};
use crate::transaction::{SessionCompilerCache, SessionCompilerState};
use crate::version::GLOBAL_VERSION_ID;
use crate::version::{
    parse_version_descriptor_snapshot, version_descriptor_file_id, version_descriptor_plugin_key,
    version_descriptor_schema_key, version_descriptor_schema_version,
};
use crate::{Lix, LixBackendTransaction, LixError, NullableKeyFilter, QueryResult, Value};
use serde_json::Value as JsonValue;

pub(crate) const LIX_ID_KEY: &str = "lix_id";
const BOOTSTRAP_REGISTERED_SCHEMA_TIMESTAMP: &str = "1970-01-01T00:00:00Z";

pub(crate) struct InitExecutor<'engine, 'tx> {
    lix: &'engine Lix,
    backend_transaction: &'tx mut dyn LixBackendTransaction,
    context: SessionCompilerState,
}

impl<'engine, 'tx> InitExecutor<'engine, 'tx> {
    pub(crate) fn new(
        lix: &'engine Lix,
        transaction: &'tx mut dyn LixBackendTransaction,
    ) -> Result<Self, LixError> {
        Ok(Self {
            lix,
            backend_transaction: transaction,
            context: SessionCompilerState::new(
                None,
                lix.engine().public_surface_registry(),
                SessionCompilerCache::new(),
                GLOBAL_VERSION_ID.to_string(),
                Vec::new(),
            ),
        })
    }

    pub(crate) fn boot_key_values(&self) -> &[crate::BootKeyValue] {
        self.lix.boot_key_values()
    }

    pub(crate) async fn execute_backend(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<QueryResult, LixError> {
        self.backend_transaction.execute(sql, params).await
    }

    pub(crate) fn backend_transaction_mut(
        &mut self,
    ) -> Result<&mut dyn LixBackendTransaction, LixError> {
        Ok(&mut *self.backend_transaction)
    }

    pub(crate) async fn generate_runtime_uuid(&mut self) -> Result<String, LixError> {
        let function_bindings = self.ensure_function_bindings().await?;
        let mut runtime_functions = function_bindings.provider().clone();
        crate::transaction::ensure_runtime_sequence_initialized_in_transaction(
            self.backend_transaction,
            &mut runtime_functions,
        )
        .await?;
        Ok(function_bindings.provider().call_uuid_v7())
    }

    pub(crate) async fn generate_runtime_timestamp(&mut self) -> Result<String, LixError> {
        let function_bindings = self.ensure_function_bindings().await?;
        let mut runtime_functions = function_bindings.provider().clone();
        crate::transaction::ensure_runtime_sequence_initialized_in_transaction(
            self.backend_transaction,
            &mut runtime_functions,
        )
        .await?;
        Ok(function_bindings.provider().call_timestamp())
    }

    pub(crate) async fn persist_runtime_state(&mut self) -> Result<(), LixError> {
        let Some(function_bindings) = self.context.function_bindings().cloned() else {
            return Ok(());
        };
        crate::transaction::persist_runtime_sequence_in_transaction(
            self.backend_transaction,
            function_bindings.provider(),
        )
        .await
    }

    async fn ensure_function_bindings(&mut self) -> Result<FunctionBindings, LixError> {
        if let Some(function_bindings) = self.context.function_bindings().cloned() {
            return Ok(function_bindings);
        }
        let backend = crate::backend::transaction_backend_view(self.backend_transaction);
        let functions = self
            .lix
            .engine()
            .prepare_runtime_functions_with_backend(&backend)
            .await?;
        let function_bindings = FunctionBindings::from_prepared_parts(
            functions.deterministic_sequence_enabled(),
            &functions,
        );
        self.context
            .set_function_bindings(function_bindings.clone());
        Ok(function_bindings)
    }

    pub(crate) async fn load_latest_commit_id(&mut self) -> Result<Option<String>, LixError> {
        let mut backend = crate::backend::transaction_backend_view(self.backend_transaction);
        if let Some(commit_id) =
            load_version_head_commit_id_with_executor(&mut backend, GLOBAL_VERSION_ID).await?
        {
            return Ok(Some(commit_id));
        }

        let has_commits = self
            .execute_backend(
                "SELECT 1 \
                 FROM lix_internal_change c \
                 JOIN lix_internal_snapshot s ON s.id = c.snapshot_id \
                 WHERE c.schema_key = 'lix_commit' \
                   AND c.file_id IS NULL \
                   AND c.plugin_key IS NULL \
                   AND s.content IS NOT NULL \
                 LIMIT 1",
                &[],
            )
            .await?
            .rows
            .first()
            .is_some();
        if has_commits {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description:
                    "init invariant violation: commits exist but the local global version head is missing"
                        .to_string(),
            });
        }

        Ok(None)
    }

    pub(crate) async fn load_global_version_commit_id(&mut self) -> Result<String, LixError> {
        let mut backend = crate::backend::transaction_backend_view(self.backend_transaction);
        let Some(commit_id) =
            load_version_head_commit_id_with_executor(&mut backend, GLOBAL_VERSION_ID).await?
        else {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "init invariant violation: local global version head is missing"
                    .to_string(),
            });
        };
        Ok(commit_id)
    }

    pub(crate) async fn resolve_last_checkpoint_commit_id_for_tip(
        &mut self,
        commit_id: &str,
    ) -> Result<Option<String>, LixError> {
        let mut backend = crate::backend::transaction_backend_view(self.backend_transaction);
        crate::canonical::resolve_last_checkpoint_commit_id_for_tip_with_executor(
            &mut backend,
            commit_id,
        )
        .await
    }

    async fn load_exact_bootstrap_live_row(
        &mut self,
        source: LiveRowSource,
        schema_key: &str,
        entity_id: &str,
        version_id: &str,
        file_id: Option<&str>,
    ) -> Result<Option<LiveRow>, LixError> {
        let backend = transaction_backend_view(self.backend_transaction_mut()?);
        load_exact_live_row(
            &backend,
            &ExactLiveRowQuery {
                source,
                schema_key: schema_key.to_string(),
                version_id: version_id.to_string(),
                entity_id: entity_id.to_string(),
                file_id: NullableKeyFilter::from_nullable(file_id.map(str::to_string)),
                schema_version: None,
                plugin_key: NullableKeyFilter::Any,
                writer_key: None,
                global: Some(version_id == GLOBAL_VERSION_ID),
                untracked: Some(matches!(source, LiveRowSource::Untracked)),
                include_tombstones: false,
                include_global_overlay: true,
                include_untracked_overlay: true,
            },
        )
        .await
    }

    pub(crate) async fn seed_builtin_registered_schemas(&mut self) -> Result<(), LixError> {
        for schema_key in crate::schema::builtin_schema_keys() {
            let schema =
                crate::schema::builtin_schema_definition(schema_key).ok_or_else(|| LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: format!("builtin schema '{schema_key}' is not available"),
                })?;
            let entity_id = builtin_schema_entity_id(schema)?;

            if self
                .load_exact_bootstrap_live_row(
                    LiveRowSource::Untracked,
                    "lix_registered_schema",
                    &entity_id,
                    GLOBAL_VERSION_ID,
                    None,
                )
                .await?
                .is_some()
            {
                continue;
            }

            let snapshot_content = serde_json::json!({
                "value": schema
            })
            .to_string();
            let change_id = self.generate_runtime_uuid().await?;
            self.insert_change_row_for_snapshot(
                &entity_id,
                "lix_registered_schema",
                "1",
                None,
                None,
                &snapshot_content,
                &change_id,
                BOOTSTRAP_REGISTERED_SCHEMA_TIMESTAMP,
                true,
            )
            .await?;

            write_live_rows(
                self.backend_transaction_mut()?,
                &[LiveRow {
                    entity_id: entity_id.clone(),
                    schema_key: "lix_registered_schema".to_string(),
                    schema_version: "1".to_string(),
                    file_id: None,
                    version_id: GLOBAL_VERSION_ID.to_string(),
                    plugin_key: None,
                    metadata: None,
                    change_id: Some(change_id.clone()),
                    writer_key: None,
                    global: true,
                    untracked: true,
                    created_at: Some(BOOTSTRAP_REGISTERED_SCHEMA_TIMESTAMP.to_string()),
                    updated_at: Some(BOOTSTRAP_REGISTERED_SCHEMA_TIMESTAMP.to_string()),
                    snapshot_content: Some(snapshot_content.clone()),
                }],
            )
            .await?;

            upsert_registered_schema_mirror_row_in_transaction(
                self.backend_transaction_mut()?,
                RegisteredSchemaMirrorRow {
                    entity_id: &entity_id,
                    schema_version: "1",
                    file_id: None,
                    version_id: GLOBAL_VERSION_ID,
                    plugin_key: None,
                    snapshot_content: Some(&snapshot_content),
                    metadata: None,
                    change_id: &change_id,
                    untracked: true,
                    created_at: BOOTSTRAP_REGISTERED_SCHEMA_TIMESTAMP,
                },
            )
            .await?;
        }

        Ok(())
    }

    pub(crate) async fn seed_default_versions(&mut self) -> Result<String, LixError> {
        let initial_commit_id = match self.load_latest_commit_id().await? {
            Some(commit_id) => commit_id,
            None => {
                let initial_change_set_id = self.generate_runtime_uuid().await?;
                let initial_commit_id = self.generate_runtime_uuid().await?;
                self.seed_initial_change_set(&initial_change_set_id).await?;
                self.seed_initial_commit(&initial_commit_id, &initial_change_set_id)
                    .await?;
                initial_commit_id
            }
        };
        self.assert_commit_change_set_integrity(&initial_commit_id)
            .await?;

        let main_version_id = match self
            .find_version_id_by_name(crate::session::DEFAULT_ACTIVE_VERSION_NAME)
            .await?
        {
            Some(version_id) => version_id,
            None => {
                let generated_main_id = self.generate_runtime_uuid().await?;
                self.seed_canonical_version_descriptor(
                    &initial_commit_id,
                    &generated_main_id,
                    crate::session::DEFAULT_ACTIVE_VERSION_NAME,
                )
                .await?;
                generated_main_id
            }
        };

        self.seed_canonical_version_descriptor(
            &initial_commit_id,
            crate::version::GLOBAL_VERSION_ID,
            crate::version::GLOBAL_VERSION_ID,
        )
        .await?;
        self.seed_local_version_head(crate::version::GLOBAL_VERSION_ID, &initial_commit_id)
            .await?;
        self.seed_local_version_head(&main_version_id, &initial_commit_id)
            .await?;

        Ok(main_version_id)
    }

    pub(crate) async fn find_version_id_by_name(
        &mut self,
        name: &str,
    ) -> Result<Option<String>, LixError> {
        let mut executor =
            crate::backend::transaction_backend_view(self.backend_transaction_mut()?);
        find_version_id_by_name_with_executor(&mut executor, name).await
    }

    pub(crate) async fn assert_commit_change_set_integrity(
        &mut self,
        commit_id: &str,
    ) -> Result<(), LixError> {
        let commit_row = self
            .execute_backend(
                "SELECT s.content \
                 FROM lix_internal_change c \
                 JOIN lix_internal_snapshot s ON s.id = c.snapshot_id \
                 WHERE c.schema_key = 'lix_commit' \
                   AND c.entity_id = $1 \
                   AND c.file_id IS NULL \
                   AND c.plugin_key IS NULL \
                   AND s.content IS NOT NULL",
                &[Value::Text(commit_id.to_string())],
            )
            .await?;
        let [row] = commit_row.rows.as_slice() else {
            return Err(if commit_row.rows.is_empty() {
                LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: format!(
                        "init invariant violation: commit '{commit_id}' is missing from canonical lix_commit facts"
                    ),
                }
            } else {
                LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: format!(
                        "init invariant violation: expected exactly one canonical lix_commit fact for '{commit_id}', got {}",
                        commit_row.rows.len()
                    ),
                }
            });
        };
        let Some(Value::Text(raw_snapshot)) = row.first() else {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "init invariant violation: commit '{commit_id}' canonical snapshot must be text"
                ),
            });
        };
        let commit_snapshot: crate::schema::LixCommit =
            serde_json::from_str(raw_snapshot).map_err(|error| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "init invariant violation: commit '{commit_id}' canonical snapshot is invalid JSON: {error}"
                ),
            })?;
        let Some(change_set_id) = commit_snapshot
            .change_set_id
            .filter(|change_set_id| !change_set_id.is_empty())
        else {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "init invariant violation: commit '{commit_id}' has empty change_set_id"
                ),
            });
        };

        let existing = self
            .execute_backend(
                "SELECT 1 \
                 FROM lix_internal_change c \
                 JOIN lix_internal_snapshot s ON s.id = c.snapshot_id \
                 WHERE c.schema_key = 'lix_change_set' \
                   AND c.entity_id = $1 \
                   AND c.file_id IS NULL \
                   AND c.plugin_key IS NULL \
                   AND s.content IS NOT NULL \
                 LIMIT 1",
                &[Value::Text(change_set_id.clone())],
            )
            .await?;
        if existing.rows.is_empty() {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "init invariant violation: commit '{commit_id}' references missing change_set '{change_set_id}'"
                ),
            });
        }

        Ok(())
    }

    async fn seed_local_version_head(
        &mut self,
        version_id: &str,
        commit_id: &str,
    ) -> Result<(), LixError> {
        let change_id = self.generate_runtime_uuid().await?;
        let timestamp = self.generate_runtime_timestamp().await?;
        let update = UpdatedVersionRef {
            version_id: version_id
                .to_string()
                .try_into()
                .map_err(|error: LixError| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "init bootstrap version_ref version_id '{version_id}' is invalid: {}",
                            error.description
                        ),
                    )
                })?,
            commit_id: commit_id.to_string(),
            change_id,
            created_at: timestamp,
        };
        let canonical_changes = canonical_changes_from_updated_version_refs(&[update.clone()])?;
        let live_rows = untracked_live_rows_from_updated_version_refs(&[update]);
        let function_bindings = self.ensure_function_bindings().await?;
        let mut runtime_functions = function_bindings.provider().clone();
        append_changes(
            self.backend_transaction_mut()?,
            &canonical_changes,
            &mut runtime_functions,
        )
        .await?;
        write_live_rows(self.backend_transaction_mut()?, &live_rows).await?;
        Ok(())
    }

    pub(crate) async fn seed_commit_graph_nodes(&mut self) -> Result<(), LixError> {
        let graph_count_result = self
            .execute_backend(
                &format!(
                    "SELECT COUNT(*) FROM {}",
                    crate::canonical::COMMIT_GRAPH_NODE_TABLE
                ),
                &[],
            )
            .await?;
        let graph_count =
            read_scalar_count(&graph_count_result, "lix_internal_commit_graph_node count")?;
        if graph_count > 0 {
            return Ok(());
        }

        let commit_count_result = self
            .execute_backend(
                "SELECT COUNT(*) \
                 FROM lix_internal_change c \
                 JOIN lix_internal_snapshot s ON s.id = c.snapshot_id \
                 WHERE c.schema_key = 'lix_commit' \
                   AND c.file_id IS NULL \
                   AND c.plugin_key IS NULL \
                   AND s.content IS NOT NULL",
                &[],
            )
            .await?;
        let commit_count = read_scalar_count(&commit_count_result, "lix_commit count")?;
        if commit_count == 0 {
            return Ok(());
        }

        let dialect = self.backend_transaction_mut()?.dialect();
        self.execute_backend(
            &crate::canonical::build_commit_generation_seed_sql(dialect),
            &[],
        )
        .await?;

        Ok(())
    }

    pub(crate) async fn seed_canonical_version_descriptor(
        &mut self,
        initial_commit_id: &str,
        entity_id: &str,
        name: &str,
    ) -> Result<String, LixError> {
        let snapshot_content = crate::version::version_descriptor_snapshot_content(
            entity_id,
            name,
            entity_id == GLOBAL_VERSION_ID,
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
            false,
        )
        .await?;
        self.add_change_id_to_commit(initial_commit_id, &change_id)
            .await?;
        Ok(change_id)
    }

    pub(crate) async fn seed_initial_commit(
        &mut self,
        commit_id: &str,
        change_set_id: &str,
    ) -> Result<(), LixError> {
        let existing = self
            .execute_backend(
                "SELECT 1 \
                 FROM lix_internal_change c \
                 JOIN lix_internal_snapshot s ON s.id = c.snapshot_id \
                 WHERE c.schema_key = 'lix_commit' \
                   AND c.entity_id = $1 \
                   AND c.file_id IS NULL \
                   AND s.content IS NOT NULL \
                 LIMIT 1",
                &[Value::Text(commit_id.to_string())],
            )
            .await?;
        if !existing.rows.is_empty() {
            return Ok(());
        }

        let snapshot_content = serde_json::json!({
            "id": commit_id,
            "change_set_id": change_set_id,
            "parent_commit_ids": [],
            "change_ids": [],
        })
        .to_string();
        let change_id = self.generate_runtime_uuid().await?;
        let timestamp = self.generate_runtime_timestamp().await?;
        self.insert_change_row_for_snapshot(
            commit_id,
            "lix_commit",
            "1",
            None,
            None,
            &snapshot_content,
            &change_id,
            &timestamp,
            false,
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn seed_initial_change_set(
        &mut self,
        change_set_id: &str,
    ) -> Result<(), LixError> {
        let existing = self
            .execute_backend(
                "SELECT 1 \
                 FROM lix_internal_change c \
                 JOIN lix_internal_snapshot s ON s.id = c.snapshot_id \
                 WHERE c.schema_key = 'lix_change_set' \
                   AND c.entity_id = $1 \
                   AND c.file_id IS NULL \
                   AND s.content IS NOT NULL \
                 LIMIT 1",
                &[Value::Text(change_set_id.to_string())],
            )
            .await?;
        if !existing.rows.is_empty() {
            return Ok(());
        }

        let snapshot_content = serde_json::json!({ "id": change_set_id }).to_string();
        let change_id = self.generate_runtime_uuid().await?;
        let timestamp = self.generate_runtime_timestamp().await?;
        self.insert_change_row_for_snapshot(
            change_set_id,
            "lix_change_set",
            "1",
            None,
            None,
            &snapshot_content,
            &change_id,
            &timestamp,
            false,
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn seed_checkpoint_labels_bootstrap(
        &mut self,
        version_heads: &[crate::canonical::CheckpointVersionHeadFact],
    ) -> Result<(), LixError> {
        self.seed_default_checkpoint_label().await?;
        self.rebuild_internal_last_checkpoint_from_heads(version_heads)
            .await
    }

    pub(crate) async fn seed_default_checkpoint_label(&mut self) -> Result<(), LixError> {
        let bootstrap_commit_id = self.load_global_version_commit_id().await?;
        if let Some(row) = self
            .load_exact_bootstrap_live_row(
                LiveRowSource::Tracked,
                crate::canonical::CHECKPOINT_LABEL_SCHEMA_KEY,
                crate::canonical::CHECKPOINT_LABEL_ID,
                GLOBAL_VERSION_ID,
                None,
            )
            .await?
        {
            let Some(snapshot_content) = row.snapshot_content.as_deref() else {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "checkpoint label snapshot_content must be text",
                ));
            };
            let parsed: serde_json::Value =
                serde_json::from_str(snapshot_content).map_err(|error| LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: format!("checkpoint label snapshot invalid JSON: {error}"),
                })?;
            let id = parsed.get("id").and_then(serde_json::Value::as_str);
            let name = parsed.get("name").and_then(serde_json::Value::as_str);
            if id != Some(crate::canonical::CHECKPOINT_LABEL_ID)
                || name != Some(crate::canonical::CHECKPOINT_LABEL_NAME)
            {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "checkpoint label canonical row is present but invalid",
                ));
            }
            self.ensure_checkpoint_label_on_bootstrap_commit(
                &bootstrap_commit_id,
                crate::canonical::CHECKPOINT_LABEL_ID,
            )
            .await?;
            return Ok(());
        }

        let snapshot_content = crate::canonical::checkpoint_label_snapshot();
        self.insert_bootstrap_tracked_row(
            Some(&bootstrap_commit_id),
            crate::canonical::CHECKPOINT_LABEL_ID,
            crate::canonical::CHECKPOINT_LABEL_SCHEMA_KEY,
            "1",
            None,
            "global",
            None,
            &snapshot_content,
        )
        .await?;

        self.ensure_checkpoint_label_on_bootstrap_commit(
            &bootstrap_commit_id,
            crate::canonical::CHECKPOINT_LABEL_ID,
        )
        .await?;
        Ok(())
    }

    async fn ensure_checkpoint_label_on_bootstrap_commit(
        &mut self,
        bootstrap_commit_id: &str,
        label_id: &str,
    ) -> Result<(), LixError> {
        let entity_label_id =
            crate::canonical::checkpoint_commit_label_entity_id(bootstrap_commit_id);
        if self
            .load_exact_bootstrap_live_row(
                LiveRowSource::Tracked,
                crate::canonical::CHECKPOINT_COMMIT_LABEL_SCHEMA_KEY,
                &entity_label_id,
                GLOBAL_VERSION_ID,
                None,
            )
            .await?
            .is_some()
        {
            return Ok(());
        }

        if label_id != crate::canonical::CHECKPOINT_LABEL_ID {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("unexpected checkpoint label id '{label_id}'"),
            ));
        }
        let snapshot_content =
            crate::canonical::checkpoint_commit_label_snapshot(bootstrap_commit_id);
        self.insert_bootstrap_tracked_row(
            Some(bootstrap_commit_id),
            &entity_label_id,
            crate::canonical::CHECKPOINT_COMMIT_LABEL_SCHEMA_KEY,
            "1",
            None,
            "global",
            None,
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

    pub(crate) async fn rebuild_internal_last_checkpoint_from_heads(
        &mut self,
        version_heads: &[crate::canonical::CheckpointVersionHeadFact],
    ) -> Result<(), LixError> {
        self.execute_backend("DELETE FROM lix_internal_last_checkpoint", &[])
            .await?;

        for version_head in version_heads {
            let version_id = version_head.version_id.as_str();
            let commit_id = version_head.head_commit_id.as_str();
            let checkpoint_commit_id = self
                .resolve_last_checkpoint_commit_id_for_tip(commit_id)
                .await?
                .unwrap_or_else(|| commit_id.to_string());
            self.insert_last_checkpoint_for_version(version_id, &checkpoint_commit_id)
                .await?;
        }

        Ok(())
    }

    pub(crate) async fn insert_bootstrap_key_value(
        &mut self,
        key: &str,
        value: &JsonValue,
        version_id: &str,
        untracked: bool,
        tracked_commit_id: Option<&str>,
    ) -> Result<(), LixError> {
        let snapshot_content = serde_json::json!({
            "key": key,
            "value": value,
        })
        .to_string();

        if untracked {
            self.insert_bootstrap_untracked_row(
                key,
                key_value_schema_key(),
                key_value_schema_version(),
                None,
                version_id,
                None,
                &snapshot_content,
            )
            .await
        } else {
            self.insert_bootstrap_tracked_row(
                tracked_commit_id,
                key,
                key_value_schema_key(),
                key_value_schema_version(),
                None,
                version_id,
                None,
                &snapshot_content,
            )
            .await
        }
    }

    pub(crate) async fn seed_boot_config_key_values(
        &mut self,
        default_active_version_id: &str,
    ) -> Result<(), LixError> {
        if self
            .boot_key_values()
            .iter()
            .any(|key_value| key_value.key == LIX_ID_KEY)
        {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("boot key `{LIX_ID_KEY}` is reserved for engine-owned identity state"),
            ));
        }

        let mut bootstrap_commit_id: Option<String> = None;
        for key_value in self.boot_key_values().to_vec() {
            let version_id = if key_value.lixcol_global.unwrap_or(false) {
                GLOBAL_VERSION_ID.to_string()
            } else {
                default_active_version_id.to_string()
            };
            let untracked = key_value.lixcol_untracked.unwrap_or(true);

            let tracked_commit_id = if untracked {
                None
            } else {
                Some(match &bootstrap_commit_id {
                    Some(commit_id) => commit_id.clone(),
                    None => {
                        let commit_id = self.load_global_version_commit_id().await?;
                        bootstrap_commit_id = Some(commit_id.clone());
                        commit_id
                    }
                })
            };

            self.insert_bootstrap_key_value(
                &key_value.key,
                &key_value.value,
                &version_id,
                untracked,
                tracked_commit_id.as_deref(),
            )
            .await?;
        }

        Ok(())
    }

    pub(crate) async fn seed_lix_id_key(&mut self) -> Result<(), LixError> {
        let lix_id_value = self.generate_runtime_uuid().await?;
        self.insert_bootstrap_key_value(
            LIX_ID_KEY,
            &JsonValue::String(lix_id_value),
            GLOBAL_VERSION_ID,
            false,
            None,
        )
        .await
    }

    pub(crate) async fn add_change_id_to_commit(
        &mut self,
        commit_id: &str,
        change_id: &str,
    ) -> Result<(), LixError> {
        let snapshot_rows = self
            .execute_backend(
                "SELECT c.snapshot_id, s.content \
                 FROM lix_internal_change c \
                 JOIN lix_internal_snapshot s ON s.id = c.snapshot_id \
                 WHERE c.entity_id = $1 \
                   AND c.schema_key = 'lix_commit' \
                   AND c.file_id IS NULL \
                   AND c.plugin_key IS NULL \
                   AND s.content IS NOT NULL",
                &[Value::Text(commit_id.to_string())],
            )
            .await?;

        let [snapshot_row] = snapshot_rows.rows.as_slice() else {
            return Err(if snapshot_rows.rows.is_empty() {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "add_change_id_to_commit: commit '{commit_id}' canonical snapshot not found"
                    ),
                )
            } else {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "add_change_id_to_commit: expected exactly one canonical snapshot for commit '{commit_id}', got {}",
                        snapshot_rows.rows.len()
                    ),
                )
            });
        };
        let snapshot_id = match snapshot_row.first() {
            Some(Value::Text(text)) if !text.is_empty() => text.clone(),
            Some(other) => {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "add_change_id_to_commit: commit '{commit_id}' snapshot_id must be text, got {other:?}"
                    ),
                ));
            }
            None => {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "add_change_id_to_commit: commit '{commit_id}' canonical snapshot row missing snapshot_id"
                    ),
                ));
            }
        };
        let current_snapshot = match snapshot_row.get(1) {
            Some(Value::Text(text)) => text.as_str(),
            Some(other) => {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "add_change_id_to_commit: commit '{commit_id}' snapshot content must be text, got {other:?}"
                    ),
                ));
            }
            None => {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "add_change_id_to_commit: commit '{commit_id}' canonical snapshot row missing content"
                    ),
                ));
            }
        };

        let mut parsed: JsonValue =
            serde_json::from_str(current_snapshot).map_err(|error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "add_change_id_to_commit: invalid JSON in commit '{commit_id}' snapshot: {error}"
                    ),
                )
            })?;

        let change_ids = parsed
            .get_mut("change_ids")
            .and_then(JsonValue::as_array_mut)
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "add_change_id_to_commit: commit '{commit_id}' snapshot missing change_ids array"
                    ),
                )
            })?;
        if !change_ids
            .iter()
            .any(|existing| existing.as_str() == Some(change_id))
        {
            change_ids.push(JsonValue::String(change_id.to_string()));
        }

        let updated_snapshot = parsed.to_string();

        self.execute_backend(
            "UPDATE lix_internal_snapshot SET content = $1 WHERE id = $2",
            &[
                Value::Text(updated_snapshot.clone()),
                Value::Text(snapshot_id),
            ],
        )
        .await?;

        Ok(())
    }

    pub(crate) async fn insert_bootstrap_tracked_row(
        &mut self,
        attach_to_commit_id: Option<&str>,
        entity_id: &str,
        schema_key: &str,
        schema_version: &str,
        file_id: Option<&str>,
        version_id: &str,
        plugin_key: Option<&str>,
        snapshot_content: &str,
    ) -> Result<(), LixError> {
        let change_id = self.generate_runtime_uuid().await?;
        let timestamp = self.generate_runtime_timestamp().await?;
        let row = LiveRow {
            entity_id: entity_id.to_string(),
            file_id: file_id.map(str::to_string),
            schema_key: schema_key.to_string(),
            schema_version: schema_version.to_string(),
            version_id: version_id.to_string(),
            plugin_key: plugin_key.map(str::to_string),
            metadata: None,
            change_id: Some(change_id.clone()),
            writer_key: None,
            global: version_id == GLOBAL_VERSION_ID,
            untracked: false,
            created_at: Some(timestamp.clone()),
            updated_at: Some(timestamp.clone()),
            snapshot_content: Some(snapshot_content.to_string()),
        };
        write_live_rows(self.backend_transaction_mut()?, &[row]).await?;

        self.insert_change_row_for_snapshot(
            entity_id,
            schema_key,
            schema_version,
            file_id,
            plugin_key,
            snapshot_content,
            &change_id,
            &timestamp,
            false,
        )
        .await?;

        if let Some(commit_id) = attach_to_commit_id {
            self.add_change_id_to_commit(commit_id, &change_id).await?;
        }

        Ok(())
    }

    pub(crate) async fn insert_bootstrap_untracked_row(
        &mut self,
        entity_id: &str,
        schema_key: &str,
        schema_version: &str,
        file_id: Option<&str>,
        version_id: &str,
        plugin_key: Option<&str>,
        snapshot_content: &str,
    ) -> Result<(), LixError> {
        let change_id = self.generate_runtime_uuid().await?;
        let timestamp = self.generate_runtime_timestamp().await?;
        let row = LiveRow {
            entity_id: entity_id.to_string(),
            file_id: file_id.map(str::to_string),
            schema_key: schema_key.to_string(),
            schema_version: schema_version.to_string(),
            version_id: version_id.to_string(),
            plugin_key: plugin_key.map(str::to_string),
            metadata: None,
            change_id: Some(change_id.clone()),
            writer_key: None,
            global: version_id == GLOBAL_VERSION_ID,
            untracked: true,
            created_at: Some(timestamp.clone()),
            updated_at: Some(timestamp),
            snapshot_content: Some(snapshot_content.to_string()),
        };
        self.insert_change_row_for_snapshot(
            entity_id,
            schema_key,
            schema_version,
            file_id,
            plugin_key,
            snapshot_content,
            &change_id,
            row.created_at
                .as_deref()
                .expect("bootstrap untracked timestamp should exist"),
            true,
        )
        .await?;
        write_live_rows(self.backend_transaction_mut()?, &[row]).await?;
        Ok(())
    }

    pub(crate) async fn insert_change_row_for_snapshot(
        &mut self,
        entity_id: &str,
        schema_key: &str,
        schema_version: &str,
        file_id: Option<&str>,
        plugin_key: Option<&str>,
        snapshot_content: &str,
        change_id: &str,
        created_at: &str,
        untracked: bool,
    ) -> Result<(), LixError> {
        let snapshot_id = format!("{change_id}~snapshot");
        self.execute_backend(
            "INSERT INTO lix_internal_snapshot (id, content) \
             SELECT $1, $2 \
             WHERE NOT EXISTS (SELECT 1 FROM lix_internal_snapshot WHERE id = $1)",
            &[
                Value::Text(snapshot_id.clone()),
                Value::Text(snapshot_content.to_string()),
            ],
        )
        .await?;
        self.execute_backend(
            "INSERT INTO lix_internal_change (\
             id, entity_id, schema_key, schema_version, file_id, plugin_key, snapshot_id, metadata, untracked, created_at\
             ) \
             SELECT $1, $2, $3, $4, $5, $6, $7, NULL, $8, $9 \
             WHERE NOT EXISTS (SELECT 1 FROM lix_internal_change WHERE id = $1)",
            &[
                Value::Text(change_id.to_string()),
                Value::Text(entity_id.to_string()),
                Value::Text(schema_key.to_string()),
                Value::Text(schema_version.to_string()),
                file_id.map(|value| Value::Text(value.to_string())).unwrap_or(Value::Null),
                plugin_key
                    .map(|value| Value::Text(value.to_string()))
                    .unwrap_or(Value::Null),
                Value::Text(snapshot_id),
                Value::Boolean(untracked),
                Value::Text(created_at.to_string()),
            ],
        )
        .await?;
        Ok(())
    }
}

async fn find_version_id_by_name_with_executor(
    executor: &mut dyn QueryExecutor,
    name: &str,
) -> Result<Option<String>, LixError> {
    let Some(global_head_commit_id) =
        load_version_head_commit_id_with_executor(executor, GLOBAL_VERSION_ID).await?
    else {
        return Ok(None);
    };
    let Some(version_heads) = load_version_head_commit_map_with_executor(executor).await? else {
        return Ok(None);
    };

    for version_id in version_heads.keys() {
        let Some(row) = load_exact_committed_change_from_commit_with_executor(
            executor,
            &global_head_commit_id,
            &ExactCommittedStateRowRequest {
                entity_id: version_id.to_string(),
                schema_key: version_descriptor_schema_key().to_string(),
                version_id: GLOBAL_VERSION_ID.to_string(),
                exact_filters: BTreeMap::from([
                    (
                        "file_id".to_string(),
                        version_descriptor_file_id()
                            .map(|value| Value::Text(value.to_string()))
                            .unwrap_or(Value::Null),
                    ),
                    (
                        "plugin_key".to_string(),
                        version_descriptor_plugin_key()
                            .map(|value| Value::Text(value.to_string()))
                            .unwrap_or(Value::Null),
                    ),
                    (
                        "schema_version".to_string(),
                        Value::Text(version_descriptor_schema_version().to_string()),
                    ),
                ]),
            },
        )
        .await?
        else {
            continue;
        };
        let Some(snapshot_content) = row.snapshot_content.as_deref() else {
            continue;
        };
        let descriptor = parse_version_descriptor_snapshot(snapshot_content)?;
        if descriptor.name.as_deref() == Some(name) {
            return Ok(Some(descriptor.id));
        }
    }

    Ok(None)
}

pub(super) fn read_scalar_count(result: &crate::QueryResult, label: &str) -> Result<i64, LixError> {
    let value = result
        .rows
        .first()
        .and_then(|row| row.first())
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("{label} query returned no rows"),
        })?;
    match value {
        Value::Integer(number) => Ok(*number),
        Value::Text(raw) => raw.parse::<i64>().map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("{label} query returned invalid integer '{raw}': {error}"),
        }),
        other => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("{label} query returned non-integer value: {other:?}"),
        }),
    }
}

pub(super) fn text_value(value: Option<&Value>, label: &str) -> Result<String, LixError> {
    match value {
        Some(Value::Text(text)) if !text.is_empty() => Ok(text.clone()),
        Some(Value::Text(_)) => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("{label} must not be empty"),
        }),
        Some(Value::Integer(number)) => Ok(number.to_string()),
        Some(other) => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("{label} must be text-like, got {other:?}"),
        }),
        None => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("missing {label}"),
        }),
    }
}

pub(super) fn system_directory_name(path: &str) -> String {
    let trimmed = path.trim_end_matches('/');
    trimmed
        .rsplit('/')
        .next()
        .filter(|segment| !segment.is_empty())
        .unwrap_or(".lix")
        .to_string()
}

fn builtin_schema_entity_id(schema: &JsonValue) -> Result<String, LixError> {
    let schema_key = schema
        .get("x-lix-key")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "builtin schema must define string x-lix-key".to_string(),
        })?;
    let schema_version = schema
        .get("x-lix-version")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "builtin schema must define string x-lix-version".to_string(),
        })?;

    Ok(format!("{schema_key}~{schema_version}"))
}
