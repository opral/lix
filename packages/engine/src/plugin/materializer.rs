use std::collections::BTreeSet;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;

use crate::common::LixError;
use crate::live_state::{list_installed_plugin_archive_refs, PluginArchiveRef};
use crate::LixBackend;

use super::component::{apply_changes_with_plugin, PluginComponentHost};
use super::{
    load_installed_plugin_from_archive_bytes, plugin_key_from_archive_path, PluginContentType,
    PluginRuntime,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstalledPlugin {
    pub key: String,
    pub runtime: PluginRuntime,
    pub api_version: String,
    pub path_glob: String,
    pub content_type: Option<PluginContentType>,
    pub entry: String,
    pub manifest_json: String,
    pub wasm: Vec<u8>,
}

#[async_trait(?Send)]
pub trait FilesystemPluginMaterializer {
    async fn load_installed_plugins(&self) -> Result<Vec<InstalledPlugin>, LixError>;

    async fn apply_plugin_changes(
        &self,
        plugin: &InstalledPlugin,
        payload: &[u8],
    ) -> Result<Vec<u8>, LixError>;
}

pub(crate) trait PluginMaterializationHost: PluginComponentHost {
    fn plugin_backend(&self) -> &Arc<dyn LixBackend + Send + Sync>;

    fn installed_plugins_cache(&self) -> &RwLock<Option<Vec<InstalledPlugin>>>;
}

pub(crate) async fn load_installed_plugins_with_runtime_cache(
    host: &impl PluginMaterializationHost,
) -> Result<Vec<InstalledPlugin>, LixError> {
    if let Some(cached) = host
        .installed_plugins_cache()
        .read()
        .map_err(|_| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "installed plugin cache lock poisoned".to_string(),
            hint: None,
        })?
        .clone()
    {
        return Ok(cached);
    }

    let plugins = load_installed_plugins_from_backend(host).await?;
    let mut guard = host
        .installed_plugins_cache()
        .write()
        .map_err(|_| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "installed plugin cache lock poisoned".to_string(),
            hint: None,
        })?;
    *guard = Some(plugins.clone());
    Ok(plugins)
}

pub(crate) async fn load_installed_plugins_from_backend(
    host: &impl PluginMaterializationHost,
) -> Result<Vec<InstalledPlugin>, LixError> {
    load_installed_plugins_from_backend_state(host.plugin_backend().as_ref()).await
}

pub(crate) async fn load_installed_plugins_from_backend_state(
    backend: &dyn LixBackend,
) -> Result<Vec<InstalledPlugin>, LixError> {
    let archive_refs = list_installed_plugin_archive_refs(backend).await?;
    let mut plugins = Vec::with_capacity(archive_refs.len());
    for archive_ref in archive_refs {
        plugins.push(
            load_installed_plugin_from_archive_ref_with_backend(backend, &archive_ref).await?,
        );
    }
    Ok(plugins)
}

pub(crate) async fn load_installed_plugin_from_archive_ref_with_backend(
    backend: &dyn LixBackend,
    archive_ref: &PluginArchiveRef,
) -> Result<InstalledPlugin, LixError> {
    let Some(plugin_key) = plugin_key_from_archive_path(&archive_ref.path) else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "plugin materialization: unsupported plugin archive path '{}'",
                archive_ref.path
            ),
            hint: None,
        });
    };
    let binary_cas = crate::binary_cas::BinaryCasContext::new();
    let mut reader = binary_cas.reader(backend);
    let archive_bytes = reader
        .load_blob_data_by_hash(&archive_ref.blob_hash)
        .await?
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "plugin materialization: missing plugin archive blob '{}' for file '{}' ({})",
                archive_ref.blob_hash, archive_ref.path, archive_ref.file_id
            ),
            hint: None,
        })?;
    if archive_bytes.is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "plugin materialization: archive '{}' is empty",
                archive_ref.path
            ),
            hint: None,
        });
    }
    load_installed_plugin_from_archive_bytes(&plugin_key, &archive_ref.path, &archive_bytes)
}

pub(crate) async fn list_installed_plugin_manifest_keys(
    backend: &dyn LixBackend,
) -> Result<BTreeSet<String>, LixError> {
    Ok(load_installed_plugins_from_backend_state(backend)
        .await?
        .into_iter()
        .map(|plugin| plugin.key)
        .collect())
}

#[allow(dead_code)]
pub(crate) async fn installed_plugin_manifest_key_exists(
    backend: &dyn LixBackend,
    plugin_key: &str,
) -> Result<bool, LixError> {
    Ok(list_installed_plugin_manifest_keys(backend)
        .await?
        .contains(plugin_key))
}

pub(crate) fn invalidate_installed_plugins_cache(
    host: &impl PluginMaterializationHost,
) -> Result<(), LixError> {
    let mut guard = host
        .installed_plugins_cache()
        .write()
        .map_err(|_| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "installed plugin cache lock poisoned".to_string(),
            hint: None,
        })?;
    *guard = None;
    let mut component_guard = host.plugin_component_cache().lock().map_err(|_| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: "plugin component cache lock poisoned".to_string(),
        hint: None,
    })?;
    component_guard.clear();
    Ok(())
}

#[async_trait(?Send)]
impl<T> FilesystemPluginMaterializer for T
where
    T: PluginMaterializationHost,
{
    async fn load_installed_plugins(&self) -> Result<Vec<InstalledPlugin>, LixError> {
        load_installed_plugins_with_runtime_cache(self).await
    }

    async fn apply_plugin_changes(
        &self,
        plugin: &InstalledPlugin,
        payload: &[u8],
    ) -> Result<Vec<u8>, LixError> {
        apply_changes_with_plugin(self, plugin, payload).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binary_cas::kv::{
        KvBlobManifest, KvBlobManifestChunk, KvChunk, BINARY_CAS_CHUNK_NAMESPACE,
        BINARY_CAS_MANIFEST_CHUNK_NAMESPACE, BINARY_CAS_MANIFEST_NAMESPACE,
    };
    use crate::{KvPair, KvScanRange, LixBackendTransaction, QueryResult, SqlDialect, Value};
    use async_trait::async_trait;
    use std::io::{Cursor, Write};
    use zip::write::SimpleFileOptions;
    use zip::{CompressionMethod, ZipWriter};

    struct InstalledPluginLookupBackend {
        archive_bytes: Vec<u8>,
    }

    struct UnusedTransaction;

    #[async_trait]
    impl LixBackend for InstalledPluginLookupBackend {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            if sql.contains("LIKE '/.lix/plugins/%.lixplugin'") {
                return Ok(QueryResult {
                    rows: vec![vec![
                        Value::Text("lix_plugin_archive::plugin_json".to_string()),
                        Value::Text("global".to_string()),
                        Value::Text("/.lix/plugins/plugin_json.lixplugin".to_string()),
                        Value::Text("blob-plugin-json".to_string()),
                    ]],
                    columns: Vec::new(),
                });
            }
            Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            })
        }

        async fn kv_get(&self, namespace: &str, key: &[u8]) -> Result<Option<Vec<u8>>, LixError> {
            match (namespace, key) {
                (BINARY_CAS_MANIFEST_NAMESPACE, b"blob-plugin-json") => {
                    serde_json::to_vec(&KvBlobManifest {
                        size_bytes: self.archive_bytes.len() as u64,
                        chunk_count: 1,
                    })
                    .map(Some)
                    .map_err(|error| {
                        LixError::new(
                            "LIX_ERROR_UNKNOWN",
                            format!("test manifest encode failed: {error}"),
                        )
                    })
                }
                (BINARY_CAS_CHUNK_NAMESPACE, b"chunk-plugin-json") => {
                    serde_json::to_vec(&KvChunk {
                        codec: "raw".to_string(),
                        codec_dict_id: None,
                        data: self.archive_bytes.clone(),
                    })
                    .map(Some)
                    .map_err(|error| {
                        LixError::new(
                            "LIX_ERROR_UNKNOWN",
                            format!("test chunk encode failed: {error}"),
                        )
                    })
                }
                _ => Ok(None),
            }
        }

        async fn kv_scan(
            &self,
            namespace: &str,
            range: KvScanRange,
            _limit: Option<usize>,
        ) -> Result<Vec<KvPair>, LixError> {
            if namespace != BINARY_CAS_MANIFEST_CHUNK_NAMESPACE {
                return Ok(Vec::new());
            }
            let key = b"blob-plugin-json/00000000000000000000".to_vec();
            let include = match range {
                KvScanRange::Prefix(prefix) => key.starts_with(&prefix),
                KvScanRange::Range { start, end } => key >= start && key < end,
            };
            if !include {
                return Ok(Vec::new());
            }
            let value = serde_json::to_vec(&KvBlobManifestChunk {
                chunk_hash: "chunk-plugin-json".to_string(),
                chunk_size: self.archive_bytes.len() as u64,
            })
            .map_err(|error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("test manifest chunk encode failed: {error}"),
                )
            })?;
            Ok(vec![KvPair::new(key, value)])
        }

        async fn begin_transaction(
            &self,
            _mode: crate::backend::TransactionBeginMode,
        ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
            Ok(Box::new(UnusedTransaction))
        }

        async fn begin_savepoint(
            &self,
            _name: &str,
        ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
            self.begin_transaction(crate::backend::TransactionBeginMode::Write)
                .await
        }
    }

    #[async_trait]
    impl LixBackendTransaction for UnusedTransaction {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        fn mode(&self) -> crate::backend::TransactionBeginMode {
            crate::backend::TransactionBeginMode::Write
        }

        async fn execute(
            &mut self,
            _sql: &str,
            _params: &[Value],
        ) -> Result<QueryResult, LixError> {
            Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            })
        }

        async fn commit(self: Box<Self>) -> Result<(), LixError> {
            Ok(())
        }

        async fn rollback(self: Box<Self>) -> Result<(), LixError> {
            Ok(())
        }
    }

    fn build_archive(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let options = SimpleFileOptions::default().compression_method(CompressionMethod::Stored);
        let cursor = Cursor::new(Vec::new());
        let mut writer = ZipWriter::new(cursor);
        for (path, bytes) in entries {
            writer
                .start_file(*path, options)
                .expect("archive entry start should succeed");
            writer
                .write_all(bytes)
                .expect("archive entry write should succeed");
        }
        writer
            .finish()
            .expect("archive finish should succeed")
            .into_inner()
    }

    fn build_plugin_archive(manifest_json: &str) -> Vec<u8> {
        let wasm = [0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];
        build_archive(&[
            ("manifest.json", manifest_json.as_bytes()),
            ("plugin.wasm", &wasm),
        ])
    }

    fn plugin_manifest_json(key: &str) -> String {
        format!(
            r#"{{
  "key":"{key}",
  "runtime":"wasm-component-v1",
  "api_version":"0.1.0",
  "match":{{"path_glob":"*.json"}},
  "entry":"plugin.wasm",
  "schemas":["schema/plugin_json_schema.json"]
}}"#
        )
    }

    #[tokio::test]
    async fn installed_plugin_manifest_key_exists_reads_installed_manifest_keys() {
        let backend = InstalledPluginLookupBackend {
            archive_bytes: build_plugin_archive(&plugin_manifest_json("plugin_json")),
        };

        assert!(
            installed_plugin_manifest_key_exists(&backend, "plugin_json")
                .await
                .expect("installed manifest key lookup should succeed")
        );
        assert!(
            !installed_plugin_manifest_key_exists(&backend, "missing_plugin")
                .await
                .expect("missing manifest key lookup should succeed")
        );
    }
}
