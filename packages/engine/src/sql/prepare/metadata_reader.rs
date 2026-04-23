use async_trait::async_trait;
use std::collections::BTreeMap;

use crate::common::LixError;
use crate::history::load_history_root_commit_id_for_lineage_version_with_executor;
use crate::live_state::{
    decode_registered_schema_row, load_version_head_commit_map_with_executor, scan_live_rows,
    LiveRowQuery, LiveRowSource,
};
use crate::schema::SchemaKey;
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixBackend, LixBackendTransaction};
use serde_json::Value as JsonValue;

#[async_trait(?Send)]
pub trait SqlPreparationMetadataReader {
    async fn load_current_version_heads_for_preparation(
        &mut self,
    ) -> Result<Option<BTreeMap<String, String>>, LixError>;

    async fn load_active_history_root_commit_id_for_preparation(
        &mut self,
        active_version_id: &str,
    ) -> Result<Option<String>, LixError>;

    async fn load_latest_registered_schema_entry_for_preparation(
        &mut self,
        schema_key: &str,
        current_version_heads: Option<&BTreeMap<String, String>>,
    ) -> Result<Option<(SchemaKey, JsonValue)>, LixError>;
}

#[async_trait(?Send)]
impl<T> SqlPreparationMetadataReader for &T
where
    T: LixBackend,
{
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

    async fn load_latest_registered_schema_entry_for_preparation(
        &mut self,
        schema_key: &str,
        current_version_heads: Option<&BTreeMap<String, String>>,
    ) -> Result<Option<(SchemaKey, JsonValue)>, LixError> {
        load_latest_registered_schema_entry_with_backend(*self, schema_key, current_version_heads)
            .await
    }
}

#[async_trait(?Send)]
impl SqlPreparationMetadataReader for &dyn LixBackend {
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

    async fn load_latest_registered_schema_entry_for_preparation(
        &mut self,
        schema_key: &str,
        current_version_heads: Option<&BTreeMap<String, String>>,
    ) -> Result<Option<(SchemaKey, JsonValue)>, LixError> {
        load_latest_registered_schema_entry_with_backend(*self, schema_key, current_version_heads)
            .await
    }
}

#[async_trait(?Send)]
impl SqlPreparationMetadataReader for Box<dyn LixBackendTransaction + '_> {
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

    async fn load_latest_registered_schema_entry_for_preparation(
        &mut self,
        schema_key: &str,
        current_version_heads: Option<&BTreeMap<String, String>>,
    ) -> Result<Option<(SchemaKey, JsonValue)>, LixError> {
        let backend = crate::backend::transaction_backend_view(self.as_mut());
        load_latest_registered_schema_entry_with_backend(
            &backend,
            schema_key,
            current_version_heads,
        )
        .await
    }
}

#[async_trait(?Send)]
impl<T> SqlPreparationMetadataReader for &mut T
where
    T: LixBackendTransaction,
{
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

    async fn load_latest_registered_schema_entry_for_preparation(
        &mut self,
        schema_key: &str,
        current_version_heads: Option<&BTreeMap<String, String>>,
    ) -> Result<Option<(SchemaKey, JsonValue)>, LixError> {
        let backend = crate::backend::transaction_backend_view(&mut **self);
        load_latest_registered_schema_entry_with_backend(
            &backend,
            schema_key,
            current_version_heads,
        )
        .await
    }
}

#[async_trait(?Send)]
impl SqlPreparationMetadataReader for &mut dyn LixBackendTransaction {
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

    async fn load_latest_registered_schema_entry_for_preparation(
        &mut self,
        schema_key: &str,
        current_version_heads: Option<&BTreeMap<String, String>>,
    ) -> Result<Option<(SchemaKey, JsonValue)>, LixError> {
        let backend = crate::backend::transaction_backend_view(*self);
        load_latest_registered_schema_entry_with_backend(
            &backend,
            schema_key,
            current_version_heads,
        )
        .await
    }
}

async fn load_latest_registered_schema_entry_with_backend(
    backend: &dyn LixBackend,
    schema_key: &str,
    current_version_heads: Option<&BTreeMap<String, String>>,
) -> Result<Option<(SchemaKey, JsonValue)>, LixError> {
    let mut latest = None::<(SchemaKey, JsonValue)>;
    for version_id in visible_registered_schema_version_ids(current_version_heads) {
        let rows = scan_live_rows(
            backend,
            &LiveRowQuery {
                schema_key: "lix_registered_schema".to_string(),
                version_id,
                // Registered-schema preparation must follow the same committed
                // visibility source as registry rebuilds and validation.
                // Using `Effective` here can disagree with those paths and
                // leave preparation thinking a schema is missing even though
                // the committed registry already exposed it.
                source: LiveRowSource::Tracked,
                constraints: Vec::new(),
                include_tombstones: false,
            },
        )
        .await?;

        for row in &rows {
            let Some((key, schema)) = decode_registered_schema_row(row)? else {
                continue;
            };
            if key.schema_key != schema_key {
                continue;
            }
            let replace = latest
                .as_ref()
                .is_none_or(|(existing, _)| schema_key_is_not_older(&key, existing));
            if replace {
                latest = Some((key, schema));
            }
        }
    }
    Ok(latest)
}

fn visible_registered_schema_version_ids(
    current_version_heads: Option<&BTreeMap<String, String>>,
) -> Vec<String> {
    let mut version_ids = std::collections::BTreeSet::from([GLOBAL_VERSION_ID.to_string()]);
    if let Some(heads) = current_version_heads {
        version_ids.extend(heads.keys().cloned());
    }
    version_ids.into_iter().collect()
}

fn schema_key_is_not_older(candidate: &SchemaKey, existing: &SchemaKey) -> bool {
    match (candidate.version_number(), existing.version_number()) {
        (Some(candidate_version), Some(existing_version)) => candidate_version >= existing_version,
        _ => candidate.schema_version >= existing.schema_version,
    }
}
