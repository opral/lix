use crate::plugin::{
    parse_plugin_archive_for_install, plugin_storage_archive_file_id, plugin_storage_archive_path,
};
use crate::sql2::filesystem_planner::{
    plan_file_path_write, DirectoryPathResolver, FilePathWriteInput, FilesystemRowContext,
};
use crate::transaction::types::{TransactionJson, TransactionWriteRow};
use crate::transaction::types::{TransactionWrite, TransactionWriteMode};
use crate::LixError;

use super::SessionContext;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisterPluginOptions {
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisterPluginReceipt {
    pub plugin_key: String,
}

impl SessionContext {
    pub async fn register_plugin(
        &self,
        options: RegisterPluginOptions,
    ) -> Result<RegisterPluginReceipt, LixError> {
        self.ensure_open()?;

        let parsed = parse_plugin_archive_for_install(&options.bytes)?;
        let plugin_key = parsed.manifest.key.clone();
        let archive_id = plugin_storage_archive_file_id(&plugin_key);
        let archive_path = plugin_storage_archive_path(&plugin_key)?;
        let archive_bytes = options.bytes;
        let schemas = parsed.schemas;

        self.with_write_transaction(|transaction| {
            Box::pin(async move {
                let version_id = transaction.active_version_id().to_string();
                let mut resolver = DirectoryPathResolver::from_existing(std::iter::empty())?;
                let mut generate_directory_id = || transaction.functions().call_uuid_v7();
                let plan = plan_file_path_write(
                    &mut resolver,
                    FilePathWriteInput {
                        id: Some(archive_id),
                        path: archive_path,
                        data: Some(archive_bytes),
                        hidden: Some(false),
                        context: FilesystemRowContext {
                            version_id: version_id.clone(),
                            global: false,
                            untracked: false,
                            file_id: None,
                            metadata: None,
                        },
                    },
                    &mut generate_directory_id,
                )?;

                transaction
                    .stage_write(TransactionWrite::RowsWithFileData {
                        mode: TransactionWriteMode::Insert,
                        rows: plan.rows,
                        file_data: plan.file_data,
                        count: plan.count,
                    })
                    .await?;

                let schema_rows = schemas
                    .into_iter()
                    .map(|schema| {
                        Ok(TransactionWriteRow {
                            entity_id: None,
                            schema_key: "lix_registered_schema".to_string(),
                            file_id: None,
                            snapshot: Some(TransactionJson::from_value(
                                serde_json::json!({ "value": schema }),
                                "plugin registered schema",
                            )?),
                            metadata: None,
                            origin: None,
                            created_at: None,
                            updated_at: None,
                            global: false,
                            change_id: None,
                            commit_id: None,
                            untracked: false,
                            version_id: version_id.clone(),
                        })
                    })
                    .collect::<Result<Vec<_>, LixError>>()?;

                if !schema_rows.is_empty() {
                    transaction
                        .stage_write(TransactionWrite::Rows {
                            mode: TransactionWriteMode::Insert,
                            rows: schema_rows,
                        })
                        .await?;
                }

                Ok(())
            })
        })
        .await?;

        Ok(RegisterPluginReceipt { plugin_key })
    }
}
