use std::sync::Arc;

use crate::binary_cas::{BlobDataReader, BlobHash};
use crate::live_state::LiveStateReader;
use crate::sql2::filesystem_visibility::VisibleFilesystem;
use crate::LixError;

use super::{
    load_installed_plugin_from_archive_bytes, plugin_key_from_archive_path, InstalledPlugin,
};

pub(crate) async fn load_installed_plugins_for_version(
    live_state: Arc<dyn LiveStateReader>,
    blob_reader: Arc<dyn BlobDataReader>,
    version_id: &str,
) -> Result<Vec<InstalledPlugin>, LixError> {
    let filesystem = VisibleFilesystem::load(live_state, version_id).await?;
    let mut plugins = Vec::new();

    for files in filesystem.files_by_directory_id.values() {
        for file in files.values() {
            let path = filesystem.file_path(file)?;
            let Some(plugin_key) = plugin_key_from_archive_path(&path) else {
                continue;
            };
            let blob_ref = filesystem
                .blob_refs_by_file_id
                .get(&file.id)
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_UNKNOWN,
                        format!("plugin archive '{path}' is missing file data"),
                    )
                })?;
            let archive_hash = BlobHash::from_hex(&blob_ref.blob_hash)?;
            let archive_bytes = blob_reader
                .load_bytes_many(&[archive_hash])
                .await?
                .into_vec()
                .into_iter()
                .next()
                .flatten()
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_UNKNOWN,
                        format!(
                            "plugin archive '{}' is missing blob '{}'",
                            path, blob_ref.blob_hash
                        ),
                    )
                })?;
            plugins.push(load_installed_plugin_from_archive_bytes(
                &plugin_key,
                &path,
                &archive_bytes,
            )?);
        }
    }

    plugins.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(plugins)
}
