use std::fs;
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ImportSite {
    path: String,
    line: usize,
    import: String,
}

const READ_RUNTIME_FILE: &str = "src/read_runtime/mod.rs";
const SPECIALIZED_PUBLIC_READ_FILE: &str = "src/sql/executor/public_runtime/read.rs";

const FORBIDDEN_READ_RUNTIME_IMPORTS: &[&str] = &[
    "crate::projections::version::",
    "crate::canonical::read::build_admin_version_source_sql",
    "crate::canonical::read::build_admin_version_source_sql_with_current_heads",
];
const FORBIDDEN_SPECIALIZED_PUBLIC_READ_IMPORTS: &[&str] = &[
    "ReadTimeProjectionRead",
    "ReadTimeProjectionReadQuery",
    "ReadTimeProjectionSurface",
    "try_compile_read_time_projection_read",
    "lower_read_for_execution_with_layouts",
];

#[test]
fn read_runtime_stays_on_live_state_registry_boundary_for_version_reads() {
    let imports = collect_file_imports_for(READ_RUNTIME_FILE);
    let hits = imports
        .into_iter()
        .filter(|site| {
            FORBIDDEN_READ_RUNTIME_IMPORTS
                .iter()
                .any(|needle| site.import.contains(needle))
        })
        .collect::<Vec<_>>();

    assert!(
        hits.is_empty(),
        "read_runtime should execute projection-backed version reads through live_state and must not import concrete projections or canonical admin SQL builders\nhits:\n{}",
        format_import_sites(&hits),
    );
}

#[test]
fn specialized_public_version_read_path_does_not_call_canonical_admin_sql_builders() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let path = manifest_dir.join(SPECIALIZED_PUBLIC_READ_FILE);
    let text = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));

    for forbidden in [
        "build_admin_version_source_sql(",
        "build_admin_version_source_sql_with_current_heads(",
    ] {
        assert!(
            !text.contains(forbidden),
            "specialized public lix_version reads should not call canonical admin SQL builders directly\nfile: {}\nforbidden: {}",
            SPECIALIZED_PUBLIC_READ_FILE,
            forbidden,
        );
    }
}

#[test]
fn specialized_public_version_read_path_does_not_compile_projection_artifacts_locally() {
    let imports = collect_file_imports_for(SPECIALIZED_PUBLIC_READ_FILE);
    let hits = imports
        .into_iter()
        .filter(|site| {
            FORBIDDEN_SPECIALIZED_PUBLIC_READ_IMPORTS
                .iter()
                .any(|needle| site.import.contains(needle))
        })
        .collect::<Vec<_>>();

    assert!(
        hits.is_empty(),
        "specialized public read orchestration should not compile read-time projection artifacts locally\nhits:\n{}",
        format_import_sites(&hits),
    );
}

#[test]
fn specialized_public_version_read_path_does_not_define_compiler_owned_layout_helpers_locally() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let path = manifest_dir.join(SPECIALIZED_PUBLIC_READ_FILE);
    let text = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));

    for forbidden in [
        "fn required_schema_keys_from_dependency_spec(",
        "async fn load_known_live_layouts_for_dependency_spec(",
    ] {
        assert!(
            !text.contains(forbidden),
            "specialized public read orchestration should not define compiler-owned public-read layout helpers locally\nfile: {}\nforbidden: {}",
            SPECIALIZED_PUBLIC_READ_FILE,
            forbidden,
        );
    }
}

fn collect_file_imports_for(relative_path: &str) -> Vec<ImportSite> {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    collect_file_imports(manifest_dir, &manifest_dir.join(relative_path))
}

fn collect_file_imports(manifest_dir: &Path, path: &Path) -> Vec<ImportSite> {
    let text = fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
    let relative = relative_display(manifest_dir, path);

    let mut imports = Vec::new();
    let mut current_import: Option<(usize, String)> = None;
    let mut brace_depth: i32 = 0;
    let mut ignored_depths = Vec::<i32>::new();
    let mut pending_test_attr = false;

    for (index, raw_line) in text.lines().enumerate() {
        let line_no = index + 1;
        let trimmed = raw_line.trim();

        let mut ignore_line = ignored_depths
            .last()
            .is_some_and(|ignored_depth| brace_depth >= *ignored_depth);

        if pending_test_attr {
            ignore_line = true;
            if trimmed.starts_with("#[") {
                // Another attribute attached to the same test-only item.
            } else {
                if raw_line.contains('{') {
                    ignored_depths.push(brace_depth + 1);
                }
                pending_test_attr = false;
            }
        } else if is_test_cfg_attr(trimmed) {
            pending_test_attr = true;
            ignore_line = true;
        }

        if !ignore_line {
            if let Some((start_line, buffer)) = current_import.as_mut() {
                if !trimmed.is_empty() {
                    if !buffer.is_empty() {
                        buffer.push(' ');
                    }
                    buffer.push_str(trimmed);
                }
                if trimmed.ends_with(';') {
                    imports.push(ImportSite {
                        path: relative.clone(),
                        line: *start_line,
                        import: std::mem::take(buffer),
                    });
                    current_import = None;
                }
            } else if is_crate_import_line(trimmed) {
                if trimmed.ends_with(';') {
                    imports.push(ImportSite {
                        path: relative.clone(),
                        line: line_no,
                        import: trimmed.to_string(),
                    });
                } else {
                    current_import = Some((line_no, trimmed.to_string()));
                }
            }
        }

        brace_depth += raw_line.chars().filter(|ch| *ch == '{').count() as i32;
        brace_depth -= raw_line.chars().filter(|ch| *ch == '}').count() as i32;

        while ignored_depths
            .last()
            .is_some_and(|ignored_depth| brace_depth < *ignored_depth)
        {
            ignored_depths.pop();
        }
    }

    imports
}

fn is_crate_import_line(line: &str) -> bool {
    line.starts_with("use crate::")
        || line.starts_with("pub(crate) use crate::")
        || line.starts_with("pub use crate::")
}

fn is_test_cfg_attr(line: &str) -> bool {
    line.starts_with("#[cfg(") && line.contains("test")
}

fn relative_display(manifest_dir: &Path, path: &Path) -> String {
    path.strip_prefix(manifest_dir)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn format_import_sites(hits: &[ImportSite]) -> String {
    let mut out = String::new();
    for hit in hits {
        out.push_str(&hit.path);
        out.push(':');
        out.push_str(&hit.line.to_string());
        out.push_str(": ");
        out.push_str(&hit.import);
        out.push('\n');
    }
    if out.is_empty() {
        out.push_str("(none)\n");
    }
    out
}
