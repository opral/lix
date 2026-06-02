use crate::LixError;

pub const PLUGIN_STORAGE_ROOT_DIRECTORY_PATH: &str = "/.lix/plugins/";
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
    .with_hint("Use install_plugin_archive to install plugin archives."))
}

pub(crate) fn is_plugin_storage_path(path: &str) -> bool {
    path == PLUGIN_STORAGE_ROOT_DIRECTORY_PATH.trim_end_matches('/')
        || path.starts_with(PLUGIN_STORAGE_ROOT_DIRECTORY_PATH)
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
            message: format!("plugin key '{plugin_key}' must be a single relative path segment"),
            hint: None,
            details: None,
        });
    }
    Ok(())
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
            plugin_storage_archive_path("plugin_json").expect("path should build"),
            "/.lix/plugins/plugin_json.lixplugin"
        );
    }

    #[test]
    fn extracts_plugin_key_from_storage_path() {
        assert_eq!(
            plugin_key_from_archive_path("/.lix/plugins/plugin_json.lixplugin"),
            Some("plugin_json".to_string())
        );
        assert_eq!(
            plugin_key_from_archive_path("/.lix/plugins/nested/plugin.lixplugin"),
            None
        );
    }

    #[test]
    fn rejects_normal_mutations_to_plugin_storage_paths() {
        for path in [
            "/.lix/plugins",
            "/.lix/plugins/plugin_json.lixplugin",
            "/.lix/plugins/nested/file.txt",
        ] {
            let error = reject_normal_plugin_storage_mutation(path, "fs.write_file")
                .expect_err("plugin storage paths should be reserved");
            assert_eq!(error.code, LixError::CODE_CONSTRAINT_VIOLATION);
            assert!(error.message.contains("reserved plugin storage path"));
            assert!(
                error
                    .hint
                    .as_deref()
                    .is_some_and(|hint| hint.contains("install_plugin_archive"))
            );
        }

        reject_normal_plugin_storage_mutation("/.lix/plugins-adjacent/file.txt", "fs.write_file")
            .expect("adjacent paths should remain writable");
    }
}
