use super::{select_best_glob_match, InstalledPlugin, PluginContentType};

pub(crate) fn select_plugin_for_file<'a>(
    plugins: &'a [InstalledPlugin],
    path: &str,
    content_type: Option<PluginContentType>,
) -> Option<&'a InstalledPlugin> {
    select_best_glob_match(
        path,
        content_type,
        plugins,
        |plugin| plugin.path_glob.as_str(),
        |plugin| plugin.content_type,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::PluginRuntime;

    #[test]
    fn json_plugin_matches_json_path() {
        let plugins = vec![plugin("json", "*.json", None)];

        let selected = select_plugin_for_file(&plugins, "/foo.json", None)
            .expect("json plugin should match json path");

        assert_eq!(selected.key, "json");
    }

    #[test]
    fn non_matching_extension_returns_none() {
        let plugins = vec![plugin("json", "*.json", None)];

        assert!(select_plugin_for_file(&plugins, "/foo.txt", None).is_none());
    }

    #[test]
    fn content_type_mismatch_returns_none() {
        let plugins = vec![plugin("json", "*.json", Some(PluginContentType::Text))];

        assert!(
            select_plugin_for_file(&plugins, "/foo.json", Some(PluginContentType::Binary))
                .is_none()
        );
    }

    #[test]
    fn more_specific_glob_wins() {
        let plugins = vec![
            plugin("generic-json", "*.json", None),
            plugin("package-json", "*/package.json", None),
        ];

        let selected = select_plugin_for_file(&plugins, "/package.json", None)
            .expect("package json plugin should match");

        assert_eq!(selected.key, "package-json");
    }

    fn plugin(
        key: &str,
        path_glob: &str,
        content_type: Option<PluginContentType>,
    ) -> InstalledPlugin {
        InstalledPlugin {
            key: key.to_string(),
            runtime: PluginRuntime::WasmComponentV1,
            api_version: "0.1.0".to_string(),
            path_glob: path_glob.to_string(),
            content_type,
            entry: "plugin.wasm".to_string(),
            manifest_json: "{}".to_string(),
            wasm: b"\0asm\x01\0\0\0".to_vec(),
        }
    }
}
