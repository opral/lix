use crate::{LixError, Plan, QueryResult, Value};

pub struct Engine;

pub fn boot() -> Engine {
    Engine
}

impl Engine {
    pub fn preprocess(&self, sql: &str, params: &[Value]) -> Result<Plan, LixError> {
        Ok(Plan {
            sql: sql.to_string(),
            params: params.to_vec(),
        })
    }

    pub fn postprocess(&self, _plan: &Plan, _result: &QueryResult) -> Result<(), LixError> {
        Ok(())
    }
}
