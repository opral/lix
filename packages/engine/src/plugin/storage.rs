use crate::LixError;

pub const PLUGIN_STORAGE_ROOT_DIRECTORY_PATH: &str = "/.lix_system/plugins/";
pub const PLUGIN_ARCHIVE_FILE_EXTENSION: &str = ".lixplugin";

pub fn plugin_storage_archive_file_id(plugin_key: &str) -> String {
    format!("lix_plugin_archive::{plugin_key}")
}

pub fn plugin_storage_archive_path(plugin_key: &str) -> Result<String, LixError> {
    validate_plugin_key_segment(plugin_key)?;
    Ok(format!(
        "{PLUGIN_STORAGE_ROOT_DIRECTORY_PATH}{plugin_key}{PLUGIN_ARCHIVE_FILE_EXTENSION}"
    ))
}

pub fn plugin_key_from_archive_path(path: &str) -> Option<String> {
    let file_name = path.strip_prefix(PLUGIN_STORAGE_ROOT_DIRECTORY_PATH)?;
    let plugin_key = file_name.strip_suffix(PLUGIN_ARCHIVE_FILE_EXTENSION)?;
    if plugin_key.is_empty()
        || plugin_key == "."
        || plugin_key == ".."
        || plugin_key.contains('/')
        || plugin_key.contains('\\')
    {
        return None;
    }
    Some(plugin_key.to_string())
}

fn validate_plugin_key_segment(plugin_key: &str) -> Result<(), LixError> {
    if plugin_key.is_empty()
        || plugin_key == "."
        || plugin_key == ".."
        || plugin_key.contains('/')
        || plugin_key.contains('\\')
    {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            message: format!(
                "plugin key '{}' must be a single relative path segment",
                plugin_key
            ),
            hint: None,
            details: None,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{plugin_key_from_archive_path, plugin_storage_archive_path};

    #[test]
    fn computes_storage_archive_paths() {
        assert_eq!(
            plugin_storage_archive_path("plugin_json").expect("path should build"),
            "/.lix_system/plugins/plugin_json.lixplugin"
        );
    }

    #[test]
    fn extracts_plugin_key_from_storage_path() {
        assert_eq!(
            plugin_key_from_archive_path("/.lix_system/plugins/plugin_json.lixplugin"),
            Some("plugin_json".to_string())
        );
        assert_eq!(
            plugin_key_from_archive_path("/.lix_system/plugins/nested/plugin.lixplugin"),
            None
        );
        assert_eq!(
            plugin_key_from_archive_path("/.lix/plugins/plugin_json.lixplugin"),
            None
        );
    }
}
