use crate::cel::CelEvaluator;
use crate::init::init_backend;
use crate::key_value::{key_value_schema_entity_id, key_value_schema_seed_insert_sql};
use crate::schema_registry::register_schema;
use crate::sql::{
    build_delete_followup_sql, build_update_followup_sql, preprocess_sql, PostprocessPlan,
};
use crate::validation::{validate_inserts, validate_updates, SchemaCache};
use crate::{LixBackend, LixError, QueryResult, Value};

pub struct Engine {
    backend: Box<dyn LixBackend + Send + Sync>,
    cel_evaluator: CelEvaluator,
    schema_cache: SchemaCache,
}

pub fn boot(backend: Box<dyn LixBackend + Send + Sync>) -> Engine {
    Engine {
        backend,
        cel_evaluator: CelEvaluator::new(),
        schema_cache: SchemaCache::new(),
    }
}

impl Engine {
    pub async fn init(&self) -> Result<(), LixError> {
        init_backend(self.backend.as_ref()).await?;
        self.ensure_key_value_schema_installed().await
    }

    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        let output =
            preprocess_sql(self.backend.as_ref(), &self.cel_evaluator, sql, params).await?;
        if !output.mutations.is_empty() {
            validate_inserts(self.backend.as_ref(), &self.schema_cache, &output.mutations).await?;
        }
        if !output.update_validations.is_empty() {
            validate_updates(
                self.backend.as_ref(),
                &self.schema_cache,
                &output.update_validations,
            )
            .await?;
        }
        for registration in output.registrations {
            register_schema(self.backend.as_ref(), &registration.schema_key).await?;
        }
        match output.postprocess {
            None => self.backend.execute(&output.sql, &output.params).await,
            Some(PostprocessPlan::VtableUpdate(plan)) => {
                let result = self.backend.execute(&output.sql, &output.params).await?;
                let followup_sql = build_update_followup_sql(&plan, &result.rows)?;
                if !followup_sql.is_empty() {
                    self.backend.execute(&followup_sql, &[]).await?;
                }
                Ok(result)
            }
            Some(PostprocessPlan::VtableDelete(plan)) => {
                let result = self.backend.execute(&output.sql, &output.params).await?;
                let followup_sql = build_delete_followup_sql(&plan, &result.rows)?;
                if !followup_sql.is_empty() {
                    self.backend.execute(&followup_sql, &[]).await?;
                }
                Ok(result)
            }
        }
    }

    async fn ensure_key_value_schema_installed(&self) -> Result<(), LixError> {
        let entity_id = key_value_schema_entity_id();
        let exists_sql = format!(
            "SELECT 1 FROM lix_internal_state_vtable \
             WHERE schema_key = 'lix_stored_schema' \
               AND entity_id = '{entity_id}' \
               AND version_id = 'global' \
               AND snapshot_content IS NOT NULL \
             LIMIT 1"
        );

        let existing = self.execute(&exists_sql, &[]).await?;
        if existing.rows.is_empty() {
            let insert_sql = key_value_schema_seed_insert_sql()?;
            self.execute(&insert_sql, &[]).await?;
        }

        Ok(())
    }
}
