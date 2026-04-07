use crate::binary_cas::read::load_binary_blob_data_by_hash;
use crate::binary_cas::schema::INTERNAL_BINARY_FILE_VERSION_REF;
use crate::contracts::plugin::{
    parse_plugin_manifest_json, plugin_key_from_archive_path, InstalledPlugin,
};
#[cfg(test)]
use crate::runtime::wasm::WasmRuntime;
use crate::runtime::wasm::{WasmComponentInstance, WasmLimits};
use crate::runtime::Runtime;
use crate::{LixBackend, LixError, Value};
use std::collections::BTreeMap;
use std::io::{Cursor, Read};
use std::path::{Component, Path};
use std::sync::Arc;
use zip::read::ZipArchive;

const APPLY_CHANGES_EXPORTS: &[&str] = &["apply-changes", "api#apply-changes"];

#[derive(Clone)]
pub(crate) struct CachedPluginComponent {
    pub wasm: Vec<u8>,
    pub instance: Arc<dyn WasmComponentInstance>,
}

#[cfg(test)]
async fn load_or_init_plugin_component_with_loaded_instances(
    runtime: &dyn WasmRuntime,
    loaded_instances: &mut BTreeMap<String, CachedPluginComponent>,
    plugin: &InstalledPlugin,
) -> Result<Arc<dyn WasmComponentInstance>, LixError> {
    if let Some(cached) = loaded_instances.get(&plugin.key) {
        if cached.wasm == plugin.wasm {
            return Ok(cached.instance.clone());
        }
    }

    let loaded = runtime
        .init_component(plugin.wasm.clone(), WasmLimits::default())
        .await?;
    loaded_instances.insert(
        plugin.key.clone(),
        CachedPluginComponent {
            wasm: plugin.wasm.clone(),
            instance: loaded.clone(),
        },
    );
    Ok(loaded)
}

async fn load_or_init_plugin_component(
    runtime: &Runtime,
    plugin: &InstalledPlugin,
) -> Result<Arc<dyn WasmComponentInstance>, LixError> {
    {
        let guard = runtime
            .plugin_component_cache
            .lock()
            .map_err(|_| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "plugin component cache lock poisoned".to_string(),
            })?;
        if let Some(cached) = guard.get(&plugin.key) {
            if cached.wasm == plugin.wasm {
                return Ok(cached.instance.clone());
            }
        }
    }

    let initialized = runtime
        .wasm_runtime_ref()
        .init_component(plugin.wasm.clone(), WasmLimits::default())
        .await?;
    let mut guard = runtime
        .plugin_component_cache
        .lock()
        .map_err(|_| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "plugin component cache lock poisoned".to_string(),
        })?;
    if let Some(cached) = guard.get(&plugin.key) {
        if cached.wasm == plugin.wasm {
            return Ok(cached.instance.clone());
        }
    }
    guard.insert(
        plugin.key.clone(),
        CachedPluginComponent {
            wasm: plugin.wasm.clone(),
            instance: initialized.clone(),
        },
    );
    Ok(initialized)
}

pub(crate) async fn apply_changes_with_plugin(
    runtime: &Runtime,
    plugin: &InstalledPlugin,
    payload: &[u8],
) -> Result<Vec<u8>, LixError> {
    let instance = load_or_init_plugin_component(runtime, plugin).await?;
    call_apply_changes(instance.as_ref(), payload).await
}

pub(crate) async fn load_installed_plugins_with_runtime_cache(
    runtime: &Runtime,
) -> Result<Vec<InstalledPlugin>, LixError> {
    if let Some(cached) = runtime
        .installed_plugins_cache
        .read()
        .map_err(|_| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "installed plugin cache lock poisoned".to_string(),
        })?
        .clone()
    {
        return Ok(cached);
    }

    let plugins = load_installed_plugins_from_backend(runtime.backend().as_ref()).await?;
    let mut guard = runtime
        .installed_plugins_cache
        .write()
        .map_err(|_| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "installed plugin cache lock poisoned".to_string(),
        })?;
    *guard = Some(plugins.clone());
    Ok(plugins)
}

async fn call_apply_changes(
    instance: &dyn WasmComponentInstance,
    payload: &[u8],
) -> Result<Vec<u8>, LixError> {
    let mut errors = Vec::new();
    for export in APPLY_CHANGES_EXPORTS {
        match instance.call(export, payload).await {
            Ok(output) => return Ok(output),
            Err(error) => errors.push(format!("{export}: {}", error.description)),
        }
    }

    Err(LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!(
            "plugin materialization: failed to call apply-changes export ({})",
            errors.join("; ")
        ),
    })
}

async fn load_installed_plugins_from_backend(
    backend: &dyn LixBackend,
) -> Result<Vec<InstalledPlugin>, LixError> {
    let rows = backend
        .execute(
            &format!(
                "SELECT binary_ref.file_id, path_cache.path, binary_ref.blob_hash \
                 FROM {binary_file_version_ref} AS binary_ref \
                 INNER JOIN lix_internal_file_path_cache AS path_cache \
                     ON path_cache.file_id = binary_ref.file_id \
                    AND path_cache.version_id = binary_ref.version_id \
                 WHERE binary_ref.version_id = 'global' \
                   AND path_cache.path LIKE '/.lix/plugins/%.lixplugin' \
                   AND path_cache.path NOT LIKE '/.lix/plugins/%/%' \
                 ORDER BY path_cache.path",
                binary_file_version_ref = INTERNAL_BINARY_FILE_VERSION_REF,
            ),
            &[],
        )
        .await?;

    let mut plugins = Vec::with_capacity(rows.rows.len());
    for row in rows.rows {
        plugins.push(load_installed_plugin_from_blob_ref_row(backend, &row).await?);
    }
    Ok(plugins)
}

async fn load_installed_plugin_from_blob_ref_row(
    backend: &dyn LixBackend,
    row: &[Value],
) -> Result<InstalledPlugin, LixError> {
    let file_id = text_required(row, 0, "file_id")?;
    let archive_path = text_required(row, 1, "path")?;
    let blob_hash = text_required(row, 2, "blob_hash")?;
    let Some(plugin_key) = plugin_key_from_archive_path(&archive_path) else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "plugin materialization: unsupported plugin archive path '{}'",
                archive_path
            ),
        });
    };
    let archive_bytes = load_binary_blob_data_by_hash(backend, &blob_hash)
        .await?
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "plugin materialization: missing plugin archive blob '{}' for file '{}' ({})",
                blob_hash, archive_path, file_id
            ),
        })?;
    if archive_bytes.is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "plugin materialization: archive '{}' is empty",
                archive_path
            ),
        });
    }
    parse_installed_plugin_from_archive_bytes(&plugin_key, &archive_path, &archive_bytes)
}

fn parse_installed_plugin_from_archive_bytes(
    plugin_key: &str,
    archive_path: &str,
    archive_bytes: &[u8],
) -> Result<InstalledPlugin, LixError> {
    let files = read_plugin_archive_files(archive_path, archive_bytes)?;
    let manifest_bytes = files.get("manifest.json").ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!(
            "plugin materialization: archive '{}' is missing manifest.json",
            archive_path
        ),
    })?;
    let manifest_raw = std::str::from_utf8(manifest_bytes).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!(
            "plugin materialization: archive '{}' manifest.json must be UTF-8: {error}",
            archive_path
        ),
    })?;
    let validated_manifest = parse_plugin_manifest_json(manifest_raw)?;
    if validated_manifest.manifest.key != plugin_key {
        return Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), description: format!(
                "plugin materialization: archive '{}' key mismatch: path key '{}' vs manifest key '{}'",
                archive_path, plugin_key, validated_manifest.manifest.key
            ),
        });
    }

    let entry_path = normalize_plugin_archive_path(&validated_manifest.manifest.entry)?;
    let wasm = files.get(&entry_path).ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!(
            "plugin materialization: archive '{}' is missing entry file '{}'",
            archive_path, validated_manifest.manifest.entry
        ),
    })?;
    ensure_valid_plugin_wasm(wasm)?;

    let manifest = validated_manifest.manifest;
    let content_type = manifest.file_match.content_type;

    Ok(InstalledPlugin {
        key: manifest.key,
        runtime: manifest.runtime,
        api_version: manifest.api_version,
        path_glob: manifest.file_match.path_glob,
        content_type,
        entry: manifest.entry,
        manifest_json: validated_manifest.normalized_json,
        wasm: wasm.clone(),
    })
}

fn read_plugin_archive_files(
    archive_path: &str,
    archive_bytes: &[u8],
) -> Result<BTreeMap<String, Vec<u8>>, LixError> {
    if archive_bytes.is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "plugin materialization: archive '{}' is empty",
                archive_path
            ),
        });
    }

    let mut archive = ZipArchive::new(Cursor::new(archive_bytes)).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!(
            "plugin materialization: archive '{}' is not a valid zip file: {error}",
            archive_path
        ),
    })?;
    let mut files = BTreeMap::<String, Vec<u8>>::new();

    for index in 0..archive.len() {
        let mut entry = archive.by_index(index).map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "plugin materialization: failed to read archive '{}' entry index {}: {error}",
                archive_path, index
            ),
        })?;
        let raw_name = entry.name().to_string();
        if entry.is_dir() {
            continue;
        }
        if is_plugin_archive_symlink(entry.unix_mode()) {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "plugin materialization: archive '{}' entry '{}' must not be a symlink",
                    archive_path, raw_name
                ),
            });
        }
        let normalized_name = normalize_plugin_archive_path(&raw_name)?;
        let mut bytes = Vec::new();
        entry.read_to_end(&mut bytes).map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "plugin materialization: failed to read archive '{}' entry '{}': {error}",
                archive_path, raw_name
            ),
        })?;
        if files.insert(normalized_name.clone(), bytes).is_some() {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "plugin materialization: archive '{}' contains duplicate entry '{}'",
                    archive_path, normalized_name
                ),
            });
        }
    }

    Ok(files)
}

fn is_plugin_archive_symlink(mode: Option<u32>) -> bool {
    const MODE_FILE_TYPE_MASK: u32 = 0o170000;
    const MODE_SYMLINK: u32 = 0o120000;
    mode.is_some_and(|value| (value & MODE_FILE_TYPE_MASK) == MODE_SYMLINK)
}

fn normalize_plugin_archive_path(path: &str) -> Result<String, LixError> {
    if path.is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "plugin archive path must not be empty".to_string(),
        });
    }
    if path.starts_with('/') || path.starts_with('\\') {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("plugin archive path '{}' must be relative", path),
        });
    }
    if path.contains('\\') {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "plugin archive path '{}' must use forward slash separators",
                path
            ),
        });
    }

    let mut segments = Vec::<String>::new();
    for component in Path::new(path).components() {
        match component {
            Component::Normal(value) => {
                let segment = value.to_str().ok_or_else(|| LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: format!(
                        "plugin archive path '{}' contains non-UTF-8 components",
                        path
                    ),
                })?;
                if segment.is_empty() {
                    return Err(LixError {
                        code: "LIX_ERROR_UNKNOWN".to_string(),
                        description: format!("plugin archive path '{}' is invalid", path),
                    });
                }
                segments.push(segment.to_string());
            }
            Component::CurDir
            | Component::ParentDir
            | Component::RootDir
            | Component::Prefix(_) => {
                return Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), description: format!(
                        "plugin archive path '{}' must not contain traversal or absolute components",
                        path
                    ),
                });
            }
        }
    }

    if segments.is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("plugin archive path '{}' is invalid", path),
        });
    }
    Ok(segments.join("/"))
}

fn ensure_valid_plugin_wasm(wasm_bytes: &[u8]) -> Result<(), LixError> {
    if wasm_bytes.is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "plugin materialization: wasm bytes must not be empty".to_string(),
        });
    }
    if wasm_bytes.len() < 8 || !wasm_bytes.starts_with(&[0x00, 0x61, 0x73, 0x6d]) {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "plugin materialization: wasm bytes must start with a valid wasm header"
                .to_string(),
        });
    }
    Ok(())
}

fn text_required(row: &[Value], index: usize, column: &str) -> Result<String, LixError> {
    let Some(value) = row.get(index) else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "plugin materialization: row missing column '{column}' at index {index}"
            ),
        });
    };
    match value {
        Value::Text(text) => Ok(text.clone()),
        other => Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), description: format!(
                "plugin materialization: expected text column '{column}' at index {index}, got {other:?}"
            ),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::{load_or_init_plugin_component_with_loaded_instances, CachedPluginComponent};
    use crate::contracts::plugin::{InstalledPlugin, PluginRuntime};
    use crate::runtime::wasm::{WasmComponentInstance, WasmLimits, WasmRuntime};
    use crate::LixError;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[derive(Default)]
    struct CountingRuntime {
        init_calls: Arc<AtomicUsize>,
    }

    struct NoopComponent;

    #[async_trait(?Send)]
    impl WasmRuntime for CountingRuntime {
        async fn init_component(
            &self,
            _bytes: Vec<u8>,
            _limits: WasmLimits,
        ) -> Result<Arc<dyn WasmComponentInstance>, LixError> {
            self.init_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Arc::new(NoopComponent))
        }
    }

    #[async_trait(?Send)]
    impl WasmComponentInstance for NoopComponent {
        async fn call(&self, _export: &str, _input: &[u8]) -> Result<Vec<u8>, LixError> {
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn component_cache_reinitializes_when_same_key_wasm_changes() {
        let runtime = CountingRuntime::default();
        let mut loaded = std::collections::BTreeMap::<String, CachedPluginComponent>::new();
        let mut plugin = InstalledPlugin {
            key: "k".to_string(),
            runtime: PluginRuntime::WasmComponentV1,
            api_version: "0.1.0".to_string(),
            path_glob: "*.json".to_string(),
            content_type: None,
            entry: "plugin.wasm".to_string(),
            manifest_json: "{}".to_string(),
            wasm: vec![1],
        };

        load_or_init_plugin_component_with_loaded_instances(&runtime, &mut loaded, &plugin)
            .await
            .expect("first init should succeed");
        load_or_init_plugin_component_with_loaded_instances(&runtime, &mut loaded, &plugin)
            .await
            .expect("second lookup should reuse cache");
        assert_eq!(runtime.init_calls.load(Ordering::SeqCst), 1);

        plugin.wasm = vec![2];
        load_or_init_plugin_component_with_loaded_instances(&runtime, &mut loaded, &plugin)
            .await
            .expect("changed wasm should reinitialize instance");
        assert_eq!(runtime.init_calls.load(Ordering::SeqCst), 2);
    }
}
