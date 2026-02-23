use std::collections::BTreeSet;

use super::super::*;
use super::execution::{apply_effects_post_commit, apply_effects_tx, run};
use super::planning::derive_requirements::derive_plan_requirements;
use super::planning::parse::parse_sql;
use super::planning::plan::build_execution_plan;

pub(crate) fn sql2_routing_enabled() -> bool {
    std::env::var("LIX_SQL2_ROUTING")
        .map(|value| {
            let value = value.trim().to_ascii_lowercase();
            matches!(value.as_str(), "1" | "true" | "yes" | "on")
        })
        .unwrap_or(false)
}

impl Engine {
    pub(crate) async fn execute_impl_sql2(
        &self,
        sql: &str,
        params: &[Value],
        options: ExecuteOptions,
        allow_internal_tables: bool,
    ) -> Result<QueryResult, LixError> {
        if !allow_internal_tables && !self.access_to_internal {
            reject_internal_table_access(sql)?;
        }

        let parsed_statements = parse_sql(sql).map_err(LixError::from)?;
        if let Some(statements) =
            extract_explicit_transaction_script_from_statements(&parsed_statements, params)?
        {
            return self
                .execute_transaction_script_with_options(statements, params, options)
                .await;
        }

        if parsed_statements.len() > 1 {
            return self
                .execute_statement_script_with_options(parsed_statements, params, &options)
                .await;
        }

        let requirements = derive_plan_requirements(&parsed_statements);
        let active_version_id = self.active_version_id.read().unwrap().clone();
        let writer_key = options.writer_key.as_deref();

        if requirements.read_only_query {
            self.maybe_refresh_working_change_projection_for_read_query(
                self.backend.as_ref(),
                &active_version_id,
            )
            .await?;
        }

        self.maybe_materialize_reads_with_backend_from_statements(
            self.backend.as_ref(),
            &parsed_statements,
            &active_version_id,
        )
        .await?;

        let CollectedExecutionSideEffects {
            pending_file_writes,
            pending_file_delete_targets,
            detected_file_domain_changes_by_statement,
            detected_file_domain_changes,
            untracked_filesystem_update_domain_changes,
        } = if requirements.read_only_query {
            CollectedExecutionSideEffects {
                pending_file_writes: Vec::new(),
                pending_file_delete_targets: BTreeSet::new(),
                detected_file_domain_changes_by_statement: Vec::new(),
                detected_file_domain_changes: Vec::new(),
                untracked_filesystem_update_domain_changes: Vec::new(),
            }
        } else {
            self.collect_execution_side_effects_with_backend_from_statements(
                self.backend.as_ref(),
                &parsed_statements,
                params,
                &active_version_id,
                writer_key,
                true,
                true,
            )
            .await?
        };

        let (settings, sequence_start, functions) = self
            .prepare_runtime_functions_with_backend(self.backend.as_ref())
            .await?;

        let plan = build_execution_plan(
            self.backend.as_ref(),
            &self.cel_evaluator,
            parsed_statements.clone(),
            params,
            functions.clone(),
            &detected_file_domain_changes_by_statement,
            writer_key,
        )
        .await
        .map_err(LixError::from)?;

        if !plan.preprocess.mutations.is_empty() {
            validate_inserts(
                self.backend.as_ref(),
                &self.schema_cache,
                &plan.preprocess.mutations,
            )
            .await?;
        }
        if !plan.preprocess.update_validations.is_empty() {
            validate_updates(
                self.backend.as_ref(),
                &self.schema_cache,
                &plan.preprocess.update_validations,
                params,
            )
            .await?;
        }

        let execution = run::execute_plan_sql(
            self,
            &plan,
            &detected_file_domain_changes,
            plan.requirements.should_refresh_file_cache,
            &functions,
            writer_key,
        )
        .await
        .map_err(LixError::from)?;

        run::persist_runtime_sequence(self, settings, sequence_start, &functions).await?;

        if let Some(version_id) = &plan.effects.next_active_version_id {
            self.set_active_version_id(version_id.clone());
        }

        let file_cache_refresh_targets = if plan.requirements.should_refresh_file_cache {
            let mut targets = direct_state_file_cache_refresh_targets(&plan.preprocess.mutations);
            targets.extend(execution.postprocess_file_cache_targets);
            targets
        } else {
            BTreeSet::new()
        };
        let descriptor_cache_eviction_targets =
            file_descriptor_cache_eviction_targets(&plan.preprocess.mutations);
        let mut file_cache_invalidation_targets = file_cache_refresh_targets.clone();
        file_cache_invalidation_targets.extend(descriptor_cache_eviction_targets);
        file_cache_invalidation_targets.extend(pending_file_delete_targets.clone());

        apply_effects_tx::apply_sql_backed_effects(
            self,
            &plan.preprocess.mutations,
            &pending_file_writes,
            &pending_file_delete_targets,
            &detected_file_domain_changes,
            &untracked_filesystem_update_domain_changes,
            execution.plugin_changes_committed,
            &file_cache_invalidation_targets,
        )
        .await?;

        apply_effects_post_commit::apply_runtime_post_commit_effects(
            self,
            file_cache_refresh_targets,
            plan.requirements.should_invalidate_installed_plugins_cache,
            plan.effects.state_commit_stream_changes,
        )
        .await?;

        Ok(execution.result)
    }
}
