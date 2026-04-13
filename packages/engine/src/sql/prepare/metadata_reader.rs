use async_trait::async_trait;
use std::collections::BTreeMap;

use crate::common::LixError;
use crate::common::{QueryResult, Value};

#[async_trait(?Send)]
pub trait SqlPreparationMetadataReader {
    async fn execute_preparation_query(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<QueryResult, LixError>;

    async fn load_current_version_heads_for_preparation(
        &mut self,
    ) -> Result<Option<BTreeMap<String, String>>, LixError>;

    async fn load_active_history_root_commit_id_for_preparation(
        &mut self,
        active_version_id: &str,
    ) -> Result<Option<String>, LixError>;
}
