use super::*;

impl Engine {
    pub async fn install_plugin(
        &self,
        manifest_json: &str,
        wasm_bytes: &[u8],
    ) -> Result<(), LixError> {
        let validated = parse_plugin_manifest_json(manifest_json)?;
        ensure_valid_wasm_binary(wasm_bytes)?;
        let now = crate::functions::timestamp::timestamp();
        upsert_plugin_record(
            self.backend.as_ref(),
            &validated.manifest,
            &validated.normalized_json,
            wasm_bytes,
            &now,
        )
        .await?;
        self.invalidate_installed_plugins_cache()?;
        Ok(())
    }
}

async fn upsert_plugin_record(
    backend: &dyn LixBackend,
    manifest: &PluginManifest,
    manifest_json: &str,
    wasm_bytes: &[u8],
    timestamp: &str,
) -> Result<(), LixError> {
    backend
        .execute(
            "INSERT INTO lix_internal_plugin (\
             key, runtime, api_version, match_path_glob, entry, manifest_json, wasm, created_at, updated_at\
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8) \
             ON CONFLICT (key) DO UPDATE SET \
             runtime = EXCLUDED.runtime, \
             api_version = EXCLUDED.api_version, \
             match_path_glob = EXCLUDED.match_path_glob, \
             entry = EXCLUDED.entry, \
             manifest_json = EXCLUDED.manifest_json, \
             wasm = EXCLUDED.wasm, \
             updated_at = EXCLUDED.updated_at",
            &[
                Value::Text(manifest.key.clone()),
                Value::Text(manifest.runtime.as_str().to_string()),
                Value::Text(manifest.api_version.clone()),
                Value::Text(manifest.file_match.path_glob.clone()),
                Value::Text(manifest.entry_or_default().to_string()),
                Value::Text(manifest_json.to_string()),
                Value::Blob(wasm_bytes.to_vec()),
                Value::Text(timestamp.to_string()),
            ],
        )
        .await?;

    Ok(())
}

fn ensure_valid_wasm_binary(wasm_bytes: &[u8]) -> Result<(), LixError> {
    if wasm_bytes.is_empty() {
        return Err(LixError {
            message: "Plugin wasm bytes must not be empty".to_string(),
        });
    }
    if wasm_bytes.len() < 8 || !wasm_bytes.starts_with(&[0x00, 0x61, 0x73, 0x6d]) {
        return Err(LixError {
            message: "Plugin wasm bytes must start with a valid wasm header".to_string(),
        });
    }
    Ok(())
}
