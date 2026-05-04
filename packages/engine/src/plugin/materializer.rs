use std::collections::BTreeSet;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;

use crate::common::LixError;
use crate::live_state::{list_installed_plugin_archive_refs, PluginArchiveRef};
use crate::Backend;

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
    fn plugin_backend(&self) -> &Arc<dyn Backend + Send + Sync>;

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
            message: "installed plugin cache lock poisoned".to_string(),
            hint: None,
            details: None,
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
            message: "installed plugin cache lock poisoned".to_string(),
            hint: None,
            details: None,
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
    backend: &dyn Backend,
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
    backend: &dyn Backend,
    archive_ref: &PluginArchiveRef,
) -> Result<InstalledPlugin, LixError> {
    let Some(plugin_key) = plugin_key_from_archive_path(&archive_ref.path) else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            message: format!(
                "plugin materialization: unsupported plugin archive path '{}'",
                archive_ref.path
            ),
            hint: None,
            details: None,
        });
    };
    let binary_cas = crate::binary_cas::BinaryCasContext::new();
    let mut reader = binary_cas.reader(backend);
    let archive_bytes = reader
        .load_blob_data_by_hash(&archive_ref.blob_hash)
        .await?
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            message: format!(
                "plugin materialization: missing plugin archive blob '{}' for file '{}' ({})",
                archive_ref.blob_hash, archive_ref.path, archive_ref.file_id
            ),
            hint: None,
            details: None,
        })?;
    if archive_bytes.is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            message: format!(
                "plugin materialization: archive '{}' is empty",
                archive_ref.path
            ),
            hint: None,
            details: None,
        });
    }
    load_installed_plugin_from_archive_bytes(&plugin_key, &archive_ref.path, &archive_bytes)
}

pub(crate) async fn list_installed_plugin_manifest_keys(
    backend: &dyn Backend,
) -> Result<BTreeSet<String>, LixError> {
    Ok(load_installed_plugins_from_backend_state(backend)
        .await?
        .into_iter()
        .map(|plugin| plugin.key)
        .collect())
}

#[allow(dead_code)]
pub(crate) async fn installed_plugin_manifest_key_exists(
    backend: &dyn Backend,
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
            message: "installed plugin cache lock poisoned".to_string(),
            hint: None,
            details: None,
        })?;
    *guard = None;
    let mut component_guard = host.plugin_component_cache().lock().map_err(|_| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        message: "plugin component cache lock poisoned".to_string(),
        hint: None,
            details: None,
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
    use crate::binary_cas::codec::{
        binary_blob_hash_bytes, encode_binary_cas_chunk, encode_binary_cas_manifest,
        encode_binary_cas_manifest_chunk, BinaryCasManifest, BinaryChunkCodec,
    };
    use crate::binary_cas::kv::{
        BINARY_CAS_CHUNK_NAMESPACE, BINARY_CAS_MANIFEST_CHUNK_NAMESPACE,
        BINARY_CAS_MANIFEST_NAMESPACE,
    };
    use crate::{
        BackendKvEntryPage, BackendKvExistsBatch, BackendKvExistsGroup,
        BackendKvGetRequest, BackendKvKeyPage, BackendKvScanRange, BackendKvScanRequest,
        BackendKvValueBatch, BackendKvValueGroup, BackendKvValuePage, BackendKvWriteBatch,
        BackendKvWriteStats, BackendReadTransaction, BackendWriteTransaction, BytePageBuilder,
    };
    use async_trait::async_trait;
    use std::io::{Cursor, Write};
    use zip::write::SimpleFileOptions;
    use zip::{CompressionMethod, ZipWriter};

    struct InstalledPluginLookupBackend {
        archive_bytes: Vec<u8>,
    }

    struct PluginLookupTransaction {
        archive_bytes: Vec<u8>,
    }

    #[async_trait]
    impl Backend for InstalledPluginLookupBackend {
        async fn begin_read_transaction(
            &self,
        ) -> Result<Box<dyn BackendReadTransaction + Send + Sync + 'static>, LixError> {
            Ok(Box::new(PluginLookupTransaction {
                archive_bytes: self.archive_bytes.clone(),
            }))
        }

        async fn begin_write_transaction(
            &self,
        ) -> Result<Box<dyn BackendWriteTransaction + Send + Sync + 'static>, LixError> {
            Ok(Box::new(PluginLookupTransaction {
                archive_bytes: self.archive_bytes.clone(),
            }))
        }
    }

    #[async_trait]
    impl BackendReadTransaction for PluginLookupTransaction {
        async fn get_values(
            &mut self,
            request: BackendKvGetRequest,
        ) -> Result<BackendKvValueBatch, LixError> {
            let mut groups = Vec::with_capacity(request.groups.len());
            for group in request.groups {
                let values = group
                    .keys
                    .into_iter()
                    .map(|key| test_kv_get(&self.archive_bytes, &group.namespace, &key))
                    .collect::<Result<Vec<_>, LixError>>()?;
                groups.push(BackendKvValueGroup {
                    namespace: group.namespace,
                    values,
                });
            }
            Ok(BackendKvValueBatch { groups })
        }

        async fn exists_many(
            &mut self,
            request: BackendKvGetRequest,
        ) -> Result<BackendKvExistsBatch, LixError> {
            let mut groups = Vec::with_capacity(request.groups.len());
            for group in request.groups {
                let exists = group
                    .keys
                    .iter()
                    .map(|key| test_kv_get(&self.archive_bytes, &group.namespace, key))
                    .collect::<Result<Vec<_>, LixError>>()?
                    .into_iter()
                    .map(|value| value.is_some())
                    .collect();
                groups.push(BackendKvExistsGroup {
                    namespace: group.namespace,
                    exists,
                });
            }
            Ok(BackendKvExistsBatch { groups })
        }

        async fn scan_keys(
            &mut self,
            request: BackendKvScanRequest,
        ) -> Result<BackendKvKeyPage, LixError> {
            let entries = test_kv_scan(&self.archive_bytes, request)?;
            Ok(BackendKvKeyPage {
                keys: entries.keys,
                resume_after: entries.resume_after,
            })
        }

        async fn scan_values(
            &mut self,
            request: BackendKvScanRequest,
        ) -> Result<BackendKvValuePage, LixError> {
            let entries = test_kv_scan(&self.archive_bytes, request)?;
            Ok(BackendKvValuePage {
                values: entries.values,
                resume_after: entries.resume_after,
            })
        }

        async fn scan_entries(
            &mut self,
            request: BackendKvScanRequest,
        ) -> Result<BackendKvEntryPage, LixError> {
            test_kv_scan(&self.archive_bytes, request)
        }

        async fn rollback(self: Box<Self>) -> Result<(), LixError> {
            Ok(())
        }
    }

    #[async_trait]
    impl BackendWriteTransaction for PluginLookupTransaction {
        async fn write_kv_batch(&mut self, _batch: BackendKvWriteBatch) -> Result<BackendKvWriteStats, LixError> {
            Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "plugin lookup test backend is read-only",
            ))
        }

        async fn commit(self: Box<Self>) -> Result<(), LixError> {
            Ok(())
        }
    }

    fn test_kv_get(
        archive_bytes: &[u8],
        namespace: &str,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, LixError> {
        match (namespace, key) {
            (BINARY_CAS_MANIFEST_NAMESPACE, key)
                if key == binary_blob_hash_bytes(archive_bytes).as_slice() =>
            {
                Ok(Some(encode_binary_cas_manifest(
                    &BinaryCasManifest::Chunked {
                        size_bytes: archive_bytes.len() as u64,
                        chunk_count: 1,
                    },
                )))
            }
            (BINARY_CAS_CHUNK_NAMESPACE, key)
                if key == binary_blob_hash_bytes(archive_bytes).as_slice() =>
            {
                Ok(Some(encode_binary_cas_chunk(
                    BinaryChunkCodec::Raw,
                    archive_bytes.len() as u64,
                    archive_bytes,
                )))
            }
            _ => Ok(None),
        }
    }

    fn test_kv_scan(
        archive_bytes: &[u8],
        request: BackendKvScanRequest,
    ) -> Result<BackendKvEntryPage, LixError> {
        if request.namespace != BINARY_CAS_MANIFEST_CHUNK_NAMESPACE {
            return Ok(BackendKvEntryPage {
                keys: BytePageBuilder::new().finish(),
                values: BytePageBuilder::new().finish(),
                resume_after: None,
            });
        }
        let blob_hash = binary_blob_hash_bytes(archive_bytes);
        let chunk_hash = binary_blob_hash_bytes(archive_bytes);
        let mut key = blob_hash.to_vec();
        key.extend_from_slice(&0u64.to_be_bytes());
        let include = match request.range {
            BackendKvScanRange::Prefix(prefix) => key.starts_with(&prefix),
            BackendKvScanRange::Range { start, end } => key >= start && key < end,
        };
        if !include || request.after.as_deref().is_some_and(|after| key.as_slice() <= after) {
            return Ok(BackendKvEntryPage {
                keys: BytePageBuilder::new().finish(),
                values: BytePageBuilder::new().finish(),
                resume_after: None,
            });
        }
        let value = encode_binary_cas_manifest_chunk(&chunk_hash, archive_bytes.len() as u64);
        let mut keys = BytePageBuilder::with_capacity(1, key.len());
        let mut values = BytePageBuilder::with_capacity(1, value.len());
        let mut resume_after = None;
        if request.limit > 0 {
            resume_after = Some(key.clone());
            keys.push(&key);
            values.push(&value);
        }
        let resume_after = (request.limit == 0).then_some(resume_after).flatten();
        Ok(BackendKvEntryPage {
            keys: keys.finish(),
            values: values.finish(),
            resume_after,
        })
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
