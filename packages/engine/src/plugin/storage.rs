use crate::LixError;

pub const PLUGIN_STORAGE_ROOT_DIRECTORY_PATH: &str = "/.lix/plugins/";
const PLUGIN_STORAGE_ROOT_PATH: &str = "/.lix/plugins";
pub const PLUGIN_ARCHIVE_FILE_EXTENSION: &str = ".lixplugin";
const PLUGIN_ARCHIVE_FILE_ID_PREFIX: &str = "lix_plugin_archive::";

pub fn plugin_storage_archive_file_id(plugin_key: &str) -> String {
    format!("{PLUGIN_ARCHIVE_FILE_ID_PREFIX}{plugin_key}")
}

pub fn plugin_storage_archive_path(plugin_key: &str) -> String {
    format!("{PLUGIN_STORAGE_ROOT_DIRECTORY_PATH}{plugin_key}{PLUGIN_ARCHIVE_FILE_EXTENSION}")
}

pub fn plugin_key_from_archive_path(path: &str) -> Option<String> {
    let file_name = path.strip_prefix(PLUGIN_STORAGE_ROOT_DIRECTORY_PATH)?;
    let plugin_key = file_name.strip_suffix(PLUGIN_ARCHIVE_FILE_EXTENSION)?;
    if !is_valid_plugin_key(plugin_key) {
        return None;
    }
    Some(plugin_key.to_string())
}

/// Extracts the plugin key from its canonical deterministic archive file ID.
///
/// This is deliberately stricter than accepting an arbitrary descriptor ID:
/// lifecycle code can use it to distinguish plugin install/update/delete
/// operations without consulting the visible filesystem.
pub fn plugin_key_from_archive_file_id(file_id: &str) -> Option<String> {
    let plugin_key = file_id.strip_prefix(PLUGIN_ARCHIVE_FILE_ID_PREFIX)?;
    if !is_valid_plugin_key(plugin_key) {
        return None;
    }
    Some(plugin_key.to_string())
}

fn is_valid_plugin_key(plugin_key: &str) -> bool {
    if plugin_key.is_empty() || plugin_key.len() > 128 {
        return false;
    }
    let mut bytes = plugin_key.bytes();
    matches!(bytes.next(), Some(b'a'..=b'z'))
        && bytes.all(|byte| matches!(byte, b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-'))
}

pub(crate) fn reject_normal_plugin_storage_mutation(
    path: &str,
    operation: &str,
) -> Result<(), LixError> {
    if !is_plugin_storage_path(path) {
        return Ok(());
    }
    Err(LixError::new(
        LixError::CODE_CONSTRAINT_VIOLATION,
        format!("{operation} cannot modify reserved plugin storage path {path:?}"),
    )
    .with_hint("Write a valid plugin archive file to the plugin storage path to install it."))
}

pub(crate) fn is_plugin_storage_path(path: &str) -> bool {
    path == PLUGIN_STORAGE_ROOT_PATH || path.starts_with(PLUGIN_STORAGE_ROOT_DIRECTORY_PATH)
}

#[cfg(test)]
mod tests {
    use crate::LixError;

    use super::{
        plugin_key_from_archive_file_id, plugin_key_from_archive_path,
        plugin_storage_archive_file_id, plugin_storage_archive_path,
        reject_normal_plugin_storage_mutation,
    };

    #[test]
    fn computes_storage_archive_paths() {
        assert_eq!(
            plugin_storage_archive_path("plugin_json"),
            "/.lix/plugins/plugin_json.lixplugin"
        );
    }

    #[test]
    fn extracts_plugin_key_from_storage_path() {
        assert_eq!(
            plugin_key_from_archive_path("/.lix/plugins/plugin_json.lixplugin"),
            Some("plugin_json".to_string())
        );
        for path in [
            "/.lix/plugins/plugin\\json.lixplugin",
            "/.lix/plugins/nested/plugin.lixplugin",
            "/.lix/plugins/PluginJson.lixplugin",
            "/.lix/plugins/.lixplugin",
        ] {
            assert_eq!(plugin_key_from_archive_path(path), None);
        }
    }

    #[test]
    fn archive_file_id_round_trips_only_canonical_plugin_keys() {
        for key in ["plugin_json", "a", "plugin-2"] {
            let file_id = plugin_storage_archive_file_id(key);
            assert_eq!(
                plugin_key_from_archive_file_id(&file_id).as_deref(),
                Some(key)
            );
        }

        for file_id in [
            "lix_plugin_archive::",
            "lix_plugin_archive::PluginJson",
            "lix_plugin_archive::plugin/nested",
            "lix_plugin_archive::plugin_json::suffix",
            "arbitrary-file-id",
        ] {
            assert_eq!(plugin_key_from_archive_file_id(file_id), None);
        }
    }

    #[test]
    fn rejects_normal_mutations_to_plugin_storage_paths() {
        for path in [
            "/.lix/plugins",
            "/.lix/plugins/plugin_json.lixplugin",
            "/.lix/plugins/nested/file.txt",
        ] {
            let error = reject_normal_plugin_storage_mutation(path, "lix_file write")
                .expect_err("plugin storage paths should be reserved");
            assert_eq!(error.code, LixError::CODE_CONSTRAINT_VIOLATION);
            assert!(error.message.contains("reserved plugin storage path"));
            assert!(
                error
                    .hint
                    .as_deref()
                    .is_some_and(|hint| hint.contains("plugin archive file"))
            );
        }

        reject_normal_plugin_storage_mutation("/.lix/plugins-adjacent/file.txt", "lix_file write")
            .expect("adjacent paths should remain writable");
    }
}
