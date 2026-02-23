pub(crate) mod directory_history_layer;
pub(crate) mod file_history_layer;
pub(crate) mod maintenance;
pub(crate) mod requests;
pub(crate) mod requirements;

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    fn collect_rust_sources(root: &Path, out: &mut Vec<PathBuf>) {
        let entries = fs::read_dir(root).expect("read_dir");
        for entry in entries {
            let entry = entry.expect("dir entry");
            let path = entry.path();
            let file_type = entry.file_type().expect("file_type");
            if file_type.is_dir() {
                collect_rust_sources(&path, out);
            } else if file_type.is_file()
                && path.extension().is_some_and(|extension| extension == "rs")
            {
                out.push(path);
            }
        }
    }

    fn production_source(source: &str) -> &str {
        source.split("#[cfg(test)]").next().unwrap_or(source)
    }

    #[test]
    fn direct_state_history_sql_stays_in_history_layers() {
        let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let src_root = repo_root.join("src");
        let mut rust_sources = Vec::new();
        collect_rust_sources(&src_root, &mut rust_sources);

        let mut offenders = Vec::new();
        for source_path in rust_sources {
            let relative = source_path
                .strip_prefix(repo_root)
                .expect("strip prefix")
                .to_string_lossy()
                .replace('\\', "/");

            if relative.starts_with("src/sql/history/")
                || relative == "src/sql/steps/lix_state_history_view_read.rs"
            {
                continue;
            }

            let source = fs::read_to_string(&source_path).expect("read source");
            let production = production_source(&source).to_ascii_lowercase();
            if production.contains("from lix_state_history")
                || production.contains("join lix_state_history")
            {
                offenders.push(relative);
            }
        }

        assert!(
            offenders.is_empty(),
            "direct lix_state_history SQL leaked outside sql/history/*: {:?}",
            offenders
        );
    }
}
