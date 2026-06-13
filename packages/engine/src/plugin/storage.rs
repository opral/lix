use crate::LixError;

pub const PLUGIN_STORAGE_ROOT_DIRECTORY_PATH: &str = "/.lix_system/plugins/";
const PLUGIN_STORAGE_ROOT_PATH: &str = "/.lix_system/plugins";
pub const PLUGIN_ARCHIVE_FILE_EXTENSION: &str = ".lixplugin";

pub fn plugin_storage_archive_file_id(plugin_key: &str) -> String {
    format!("lix_plugin_archive::{plugin_key}")
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
        plugin_key_from_archive_path, plugin_storage_archive_path,
        reject_normal_plugin_storage_mutation,
    };

    #[test]
    fn computes_storage_archive_paths() {
        assert_eq!(
            plugin_storage_archive_path("plugin_json"),
            "/.lix_system/plugins/plugin_json.lixplugin"
        );
    }

    #[test]
    fn extracts_plugin_key_from_storage_path() {
        assert_eq!(
            plugin_key_from_archive_path("/.lix_system/plugins/plugin_json.lixplugin"),
            Some("plugin_json".to_string())
        );
        for path in [
            "/.lix_system/plugins/plugin\\json.lixplugin",
            "/.lix_system/plugins/nested/plugin.lixplugin",
            "/.lix_system/plugins/PluginJson.lixplugin",
            "/.lix_system/plugins/.lixplugin",
        ] {
            assert_eq!(plugin_key_from_archive_path(path), None);
        }
    }

    #[test]
    fn rejects_normal_mutations_to_plugin_storage_paths() {
        for path in [
            "/.lix_system/plugins",
            "/.lix_system/plugins/plugin_json.lixplugin",
            "/.lix_system/plugins/nested/file.txt",
        ] {
            let error = reject_normal_plugin_storage_mutation(path, "fs.write_file")
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

        reject_normal_plugin_storage_mutation(
            "/.lix_system/plugins-adjacent/file.txt",
            "fs.write_file",
        )
        .expect("adjacent paths should remain writable");
    }
}
