use std::sync::Arc;

use crate::binary_cas::BlobDataReader;
use crate::live_state::LiveStateReader;
use crate::sql2::filesystem_planner::{
    plan_file_path_write, DirectoryPathResolver, FilePathWriteInput, FilesystemRowContext,
};
use crate::transaction::types::{
    TransactionJson, TransactionWrite, TransactionWriteMode, TransactionWriteRow,
};
use crate::transaction::Transaction;
use crate::wasm::{NoopWasmRuntime, WasmRuntime};
use crate::LixError;

use super::component::{CachedPluginComponent, PluginComponentHost};
use super::detect_changes::{detect_changes_with_plugin, PluginDetectChangesInput};
use super::registry::load_installed_plugins_for_version;
use super::{
    parse_plugin_archive_for_install, plugin_storage_archive_file_id, plugin_storage_archive_path,
    select_plugin_for_file, InstalledPlugin, PluginContentType,
};
use crate::session::{RegisterPluginOptions, RegisterPluginReceipt};

pub(crate) struct PluginContext {
    wasm_runtime: Arc<dyn WasmRuntime>,
    component_cache: std::sync::Mutex<std::collections::BTreeMap<String, CachedPluginComponent>>,
}

impl PluginContext {
    pub(crate) fn new() -> Self {
        Self::new_with_wasm_runtime(Arc::new(NoopWasmRuntime))
    }

    pub(crate) fn new_with_wasm_runtime(wasm_runtime: Arc<dyn WasmRuntime>) -> Self {
        Self {
            wasm_runtime,
            component_cache: std::sync::Mutex::new(Default::default()),
        }
    }

    pub(crate) async fn register_plugin(
        &self,
        transaction: &mut Transaction,
        options: RegisterPluginOptions,
    ) -> Result<RegisterPluginReceipt, LixError> {
        let parsed = parse_plugin_archive_for_install(&options.bytes)?;
        let plugin_key = parsed.manifest.key.clone();
        let archive_id = plugin_storage_archive_file_id(&plugin_key);
        let archive_path = plugin_storage_archive_path(&plugin_key)?;
        let archive_bytes = options.bytes;
        let schemas = parsed.schemas;

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

        Ok(RegisterPluginReceipt { plugin_key })
    }

    pub(crate) async fn load_installed_plugins_for_version(
        &self,
        live_state: Arc<dyn LiveStateReader>,
        blob_reader: Arc<dyn BlobDataReader>,
        version_id: &str,
    ) -> Result<Vec<InstalledPlugin>, LixError> {
        load_installed_plugins_for_version(live_state, blob_reader, version_id).await
    }

    pub(crate) fn select_plugin_for_file<'a>(
        &self,
        plugins: &'a [InstalledPlugin],
        path: &str,
        content_type: Option<PluginContentType>,
    ) -> Option<&'a InstalledPlugin> {
        select_plugin_for_file(plugins, path, content_type)
    }

    pub(crate) async fn detect_changes_with_plugin(
        &self,
        plugin: &InstalledPlugin,
        input: PluginDetectChangesInput,
    ) -> Result<Vec<super::PluginEntityChange>, LixError> {
        detect_changes_with_plugin(self, plugin, input).await
    }
}

impl Default for PluginContext {
    fn default() -> Self {
        Self::new()
    }
}

impl PluginComponentHost for PluginContext {
    fn plugin_component_cache(
        &self,
    ) -> &std::sync::Mutex<std::collections::BTreeMap<String, CachedPluginComponent>> {
        &self.component_cache
    }

    fn wasm_runtime(&self) -> &Arc<dyn WasmRuntime> {
        &self.wasm_runtime
    }
}
