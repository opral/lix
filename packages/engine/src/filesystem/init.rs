use crate::init::seed::{system_directory_name, text_value};
use crate::init::tables::{add_column_if_missing, execute_init_statements};
use crate::init::InitExecutor;
use crate::Value;
use crate::{LixBackend, LixError};

const FILESYSTEM_INIT_STATEMENTS: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS lix_internal_file_data_cache (\
     file_id TEXT NOT NULL,\
     version_id TEXT NOT NULL,\
     data BYTEA NOT NULL,\
     PRIMARY KEY (file_id, version_id)\
     )",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_file_data_cache_version_id \
     ON lix_internal_file_data_cache (version_id)",
    "CREATE TABLE IF NOT EXISTS lix_internal_binary_blob_store (\
     blob_hash TEXT PRIMARY KEY,\
     data BYTEA NOT NULL,\
     size_bytes BIGINT NOT NULL,\
     created_at TEXT NOT NULL\
     )",
    "CREATE TABLE IF NOT EXISTS lix_internal_binary_blob_manifest (\
     blob_hash TEXT PRIMARY KEY,\
     size_bytes BIGINT NOT NULL,\
     chunk_count BIGINT NOT NULL,\
     created_at TEXT NOT NULL\
     )",
    "CREATE TABLE IF NOT EXISTS lix_internal_binary_chunk_store (\
     chunk_hash TEXT PRIMARY KEY,\
     data BYTEA NOT NULL,\
     size_bytes BIGINT NOT NULL,\
     codec TEXT NOT NULL DEFAULT 'legacy',\
     codec_dict_id TEXT,\
     created_at TEXT NOT NULL\
     )",
    "CREATE TABLE IF NOT EXISTS lix_internal_binary_blob_manifest_chunk (\
     blob_hash TEXT NOT NULL,\
     chunk_index BIGINT NOT NULL,\
     chunk_hash TEXT NOT NULL,\
     chunk_size BIGINT NOT NULL,\
     PRIMARY KEY (blob_hash, chunk_index),\
     FOREIGN KEY (blob_hash) REFERENCES lix_internal_binary_blob_manifest (blob_hash) ON DELETE RESTRICT,\
     FOREIGN KEY (chunk_hash) REFERENCES lix_internal_binary_chunk_store (chunk_hash) ON DELETE RESTRICT\
     )",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_binary_blob_manifest_chunk_hash \
     ON lix_internal_binary_blob_manifest_chunk (chunk_hash)",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_binary_blob_manifest_chunk_blob_hash \
     ON lix_internal_binary_blob_manifest_chunk (blob_hash)",
    "CREATE TABLE IF NOT EXISTS lix_internal_binary_file_version_ref (\
     file_id TEXT NOT NULL,\
     version_id TEXT NOT NULL,\
     blob_hash TEXT NOT NULL,\
     size_bytes BIGINT NOT NULL,\
     updated_at TEXT NOT NULL,\
     PRIMARY KEY (file_id, version_id),\
     FOREIGN KEY (blob_hash) REFERENCES lix_internal_binary_blob_manifest (blob_hash) ON DELETE RESTRICT\
     )",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_binary_file_version_ref_blob_hash \
     ON lix_internal_binary_file_version_ref (blob_hash)",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_binary_file_version_ref_version_id \
     ON lix_internal_binary_file_version_ref (version_id)",
    "CREATE TABLE IF NOT EXISTS lix_internal_file_path_cache (\
     file_id TEXT NOT NULL,\
     version_id TEXT NOT NULL,\
     directory_id TEXT,\
     name TEXT NOT NULL,\
     extension TEXT,\
     path TEXT NOT NULL,\
     PRIMARY KEY (file_id, version_id)\
     )",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_file_path_cache_version_path \
     ON lix_internal_file_path_cache (version_id, path, file_id)",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_file_path_cache_version_directory \
     ON lix_internal_file_path_cache (version_id, directory_id)",
    "CREATE TABLE IF NOT EXISTS lix_internal_file_lixcol_cache (\
     file_id TEXT NOT NULL,\
     version_id TEXT NOT NULL,\
     latest_change_id TEXT,\
     latest_commit_id TEXT,\
     created_at TEXT,\
     updated_at TEXT,\
     writer_key TEXT,\
     PRIMARY KEY (file_id, version_id)\
     )",
    "CREATE INDEX IF NOT EXISTS idx_file_lixcol_cache_lookup \
     ON lix_internal_file_lixcol_cache (file_id, version_id)",
];

pub(crate) async fn init(backend: &dyn LixBackend) -> Result<(), LixError> {
    execute_init_statements(backend, "filesystem", FILESYSTEM_INIT_STATEMENTS).await?;
    add_column_if_missing(
        backend,
        "lix_internal_binary_chunk_store",
        "codec",
        "TEXT NOT NULL DEFAULT 'legacy'",
    )
    .await?;
    add_column_if_missing(
        backend,
        "lix_internal_binary_chunk_store",
        "codec_dict_id",
        "TEXT",
    )
    .await?;
    Ok(())
}

pub(crate) async fn seed_bootstrap(executor: &mut InitExecutor<'_, '_>) -> Result<(), LixError> {
    executor.seed_global_system_directories().await
}

const SYSTEM_ROOT_DIRECTORY_PATH: &str = "/.lix/";
const SYSTEM_APP_DATA_DIRECTORY_PATH: &str = "/.lix/app_data/";
const SYSTEM_PLUGIN_DIRECTORY_PATH: &str = "/.lix/plugins/";

impl<'engine, 'tx> InitExecutor<'engine, 'tx> {
    pub(crate) async fn seed_global_system_directories(&mut self) -> Result<(), LixError> {
        let bootstrap_commit_id = self.load_global_version_commit_id().await?;
        let root_id = self
            .ensure_seeded_system_directory(&bootstrap_commit_id, SYSTEM_ROOT_DIRECTORY_PATH, None)
            .await?;
        self.ensure_seeded_system_directory(
            &bootstrap_commit_id,
            SYSTEM_APP_DATA_DIRECTORY_PATH,
            Some(root_id.as_str()),
        )
        .await?;
        self.ensure_seeded_system_directory(
            &bootstrap_commit_id,
            SYSTEM_PLUGIN_DIRECTORY_PATH,
            Some(root_id.as_str()),
        )
        .await?;

        Ok(())
    }

    async fn ensure_seeded_system_directory(
        &mut self,
        bootstrap_commit_id: &str,
        path: &str,
        parent_id: Option<&str>,
    ) -> Result<String, LixError> {
        let name = system_directory_name(path);
        let existing = match parent_id {
            Some(parent_id) => {
                self.execute_internal(
                    "SELECT id \
                     FROM lix_directory_by_version \
                     WHERE lixcol_version_id = 'global' \
                       AND name = $1 \
                       AND parent_id = $2 \
                     LIMIT 1",
                    &[
                        Value::Text(name.clone()),
                        Value::Text(parent_id.to_string()),
                    ],
                )
                .await?
            }
            None => {
                self.execute_internal(
                    "SELECT id \
                     FROM lix_directory_by_version \
                     WHERE lixcol_version_id = 'global' \
                       AND name = $1 \
                       AND parent_id IS NULL \
                     LIMIT 1",
                    &[Value::Text(name.clone())],
                )
                .await?
            }
        };
        let [statement] = existing.statements.as_slice() else {
            return Err(crate::errors::unexpected_statement_count_error(
                "system directory existence query",
                1,
                existing.statements.len(),
            ));
        };
        if let Some(row) = statement.rows.first() {
            return text_value(row.first(), "system directory entity_id");
        }

        let entity_id = self.generate_runtime_uuid().await?;
        let parent_id_json = parent_id.map(ToString::to_string);
        let snapshot_content = serde_json::json!({
            "id": entity_id,
            "parent_id": parent_id_json,
            "name": name,
            "hidden": true,
        })
        .to_string();
        self.insert_bootstrap_tracked_row(
            Some(bootstrap_commit_id),
            &entity_id,
            "lix_directory_descriptor",
            "1",
            "lix",
            "global",
            "lix",
            &snapshot_content,
        )
        .await?;

        Ok(entity_id)
    }
}
