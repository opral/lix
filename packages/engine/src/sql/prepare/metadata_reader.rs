use async_trait::async_trait;
use std::collections::BTreeMap;

use crate::common::LixError;
use crate::common::{QueryResult, Value};
use crate::history::load_history_root_commit_id_for_lineage_version_with_executor;
use crate::live_state::load_version_head_commit_map_with_executor;
use crate::{LixBackend, LixBackendTransaction};

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

#[async_trait(?Send)]
impl<T> SqlPreparationMetadataReader for &T
where
    T: LixBackend + ?Sized,
{
    async fn execute_preparation_query(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<QueryResult, LixError> {
        (*self).execute(sql, params).await
    }

    async fn load_current_version_heads_for_preparation(
        &mut self,
    ) -> Result<Option<BTreeMap<String, String>>, LixError> {
        match load_version_head_commit_map_with_executor(self).await {
            Ok(heads) => Ok(heads),
            Err(error)
                if error
                    .description
                    .contains("schema 'lix_version' is not stored") =>
            {
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    async fn load_active_history_root_commit_id_for_preparation(
        &mut self,
        active_version_id: &str,
    ) -> Result<Option<String>, LixError> {
        match load_history_root_commit_id_for_lineage_version_with_executor(self, active_version_id)
            .await
        {
            Ok(commit_id) => Ok(commit_id),
            Err(error)
                if error
                    .description
                    .contains("schema 'lix_version' is not stored") =>
            {
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }
}

#[async_trait(?Send)]
impl SqlPreparationMetadataReader for Box<dyn LixBackendTransaction + '_> {
    async fn execute_preparation_query(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<QueryResult, LixError> {
        self.as_mut().execute(sql, params).await
    }

    async fn load_current_version_heads_for_preparation(
        &mut self,
    ) -> Result<Option<BTreeMap<String, String>>, LixError> {
        match load_version_head_commit_map_with_executor(self).await {
            Ok(heads) => Ok(heads),
            Err(error)
                if error
                    .description
                    .contains("schema 'lix_version' is not stored") =>
            {
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    async fn load_active_history_root_commit_id_for_preparation(
        &mut self,
        active_version_id: &str,
    ) -> Result<Option<String>, LixError> {
        match load_history_root_commit_id_for_lineage_version_with_executor(self, active_version_id)
            .await
        {
            Ok(commit_id) => Ok(commit_id),
            Err(error)
                if error
                    .description
                    .contains("schema 'lix_version' is not stored") =>
            {
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }
}

#[async_trait(?Send)]
impl<T> SqlPreparationMetadataReader for &mut T
where
    T: LixBackendTransaction + ?Sized,
{
    async fn execute_preparation_query(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<QueryResult, LixError> {
        (**self).execute(sql, params).await
    }

    async fn load_current_version_heads_for_preparation(
        &mut self,
    ) -> Result<Option<BTreeMap<String, String>>, LixError> {
        match load_version_head_commit_map_with_executor(self).await {
            Ok(heads) => Ok(heads),
            Err(error)
                if error
                    .description
                    .contains("schema 'lix_version' is not stored") =>
            {
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    async fn load_active_history_root_commit_id_for_preparation(
        &mut self,
        active_version_id: &str,
    ) -> Result<Option<String>, LixError> {
        match load_history_root_commit_id_for_lineage_version_with_executor(self, active_version_id)
            .await
        {
            Ok(commit_id) => Ok(commit_id),
            Err(error)
                if error
                    .description
                    .contains("schema 'lix_version' is not stored") =>
            {
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }
}
