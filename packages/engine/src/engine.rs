use crate::init::init_backend;
use crate::schema_registry::register_schema;
use crate::sql::{
    build_delete_followup_sql, build_update_followup_sql, preprocess_sql, PostprocessPlan,
};
use crate::{LixBackend, LixError, QueryResult, Value};

pub struct Engine {
    backend: Box<dyn LixBackend + Send + Sync>,
}

pub fn boot(backend: Box<dyn LixBackend + Send + Sync>) -> Engine {
    Engine { backend }
}

impl Engine {
    pub async fn init(&self) -> Result<(), LixError> {
        init_backend(self.backend.as_ref()).await
    }

    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        let output = preprocess_sql(sql)?;
        for registration in output.registrations {
            register_schema(self.backend.as_ref(), &registration.schema_key).await?;
        }
        match output.postprocess {
            None => self.backend.execute(&output.sql, params).await,
            Some(PostprocessPlan::VtableUpdate(plan)) => {
                let result = self.backend.execute(&output.sql, params).await?;
                let followup_sql = build_update_followup_sql(&plan, &result.rows)?;
                if !followup_sql.is_empty() {
                    self.backend.execute(&followup_sql, &[]).await?;
                }
                Ok(result)
            }
            Some(PostprocessPlan::VtableDelete(plan)) => {
                let result = self.backend.execute(&output.sql, params).await?;
                let followup_sql = build_delete_followup_sql(&plan, &result.rows)?;
                if !followup_sql.is_empty() {
                    self.backend.execute(&followup_sql, &[]).await?;
                }
                Ok(result)
            }
        }
    }
}
