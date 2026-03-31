use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ImportSite {
    path: String,
    line: usize,
    import: String,
}

#[derive(Debug, Clone, Copy)]
struct CrossOwnerCategory {
    key: &'static str,
    prefix: &'static str,
    needle: &'static str,
    exact_path: Option<&'static str>,
}

const CROSS_OWNER_CATEGORIES: &[CrossOwnerCategory] = &[
    CrossOwnerCategory {
        key: "sql -> transaction",
        prefix: "src/sql/",
        needle: "crate::transaction::",
        exact_path: None,
    },
    CrossOwnerCategory {
        key: "sql -> live_state",
        prefix: "src/sql/",
        needle: "crate::live_state::",
        exact_path: None,
    },
    CrossOwnerCategory {
        key: "sql -> session",
        prefix: "src/sql/",
        needle: "crate::session::",
        exact_path: None,
    },
    CrossOwnerCategory {
        key: "sql -> engine",
        prefix: "src/sql/",
        needle: "crate::engine::",
        exact_path: None,
    },
    CrossOwnerCategory {
        key: "sql -> read",
        prefix: "src/sql/",
        needle: "crate::read::",
        exact_path: None,
    },
    CrossOwnerCategory {
        key: "live_state -> sql",
        prefix: "src/live_state/",
        needle: "crate::sql::",
        exact_path: None,
    },
    CrossOwnerCategory {
        key: "live_state -> transaction",
        prefix: "src/live_state/",
        needle: "crate::transaction::",
        exact_path: None,
    },
    CrossOwnerCategory {
        key: "live_state -> session",
        prefix: "src/live_state/",
        needle: "crate::session::",
        exact_path: None,
    },
    CrossOwnerCategory {
        key: "read -> sql",
        prefix: "src/read/",
        needle: "crate::sql::",
        exact_path: None,
    },
    CrossOwnerCategory {
        key: "engine -> sql",
        prefix: "src/",
        needle: "crate::sql::",
        exact_path: Some("src/engine.rs"),
    },
    CrossOwnerCategory {
        key: "transaction -> sql",
        prefix: "src/transaction/",
        needle: "crate::sql::",
        exact_path: None,
    },
    CrossOwnerCategory {
        key: "transaction -> live_state",
        prefix: "src/transaction/",
        needle: "crate::live_state::",
        exact_path: None,
    },
];

const EXPECTED_CROSS_OWNER_COUNTS: &[(&str, usize)] = &[
    ("sql -> transaction", 0),
    ("sql -> live_state", 0),
    ("sql -> session", 0),
    ("sql -> engine", 0),
    ("sql -> read", 0),
    ("live_state -> sql", 0),
    ("live_state -> transaction", 0),
    ("live_state -> session", 0),
    ("read -> sql", 0),
    ("engine -> sql", 0),
    ("transaction -> sql", 14),
    ("transaction -> live_state", 0),
];

const ALLOWED_TRANSACTION_SQL_FILES: &[&str] = &[
    "src/transaction/sql_adapter/compile.rs",
    "src/transaction/sql_adapter/effects.rs",
    "src/transaction/sql_adapter/execute.rs",
    "src/transaction/sql_adapter/planned_write.rs",
    "src/transaction/sql_adapter/runtime.rs",
    "src/transaction/sql_adapter/tracked_apply.rs",
];

const ALLOWED_TRANSACTION_LIVE_STATE_FILES: &[&str] = &[];

const ALLOWED_LOCAL_BARREL_FILES: &[&str] = &[];

#[test]
fn cross_owner_import_counts_match_plan22_phase1_budget() {
    let imports = collect_all_import_sites();
    let counts = cross_owner_counts(&imports);
    let expected = EXPECTED_CROSS_OWNER_COUNTS
        .iter()
        .map(|(key, count)| ((*key).to_string(), *count))
        .collect::<BTreeMap<_, _>>();

    assert_eq!(
        counts,
        expected,
        "cross-owner import count mismatch:\nexpected:\n{}\nactual:\n{}\n\nhits:\n{}",
        format_count_table(&expected),
        format_count_table(&counts),
        format_cross_owner_hits(&imports),
    );
}

#[test]
fn transaction_sql_imports_are_confined_to_phase1_allowlist() {
    let imports = collect_all_import_sites();
    let hits = filter_imports_by_prefix_and_needle(&imports, "src/transaction/", "crate::sql::");
    assert_path_allowlist(
        "transaction -> sql",
        &hits,
        ALLOWED_TRANSACTION_SQL_FILES,
        "transaction -> sql leaks should stay confined to transaction/sql_adapter/* during Phase 1",
    );
}

#[test]
fn transaction_live_state_imports_are_confined_to_phase1_allowlist() {
    let imports = collect_all_import_sites();
    let hits =
        filter_imports_by_prefix_and_needle(&imports, "src/transaction/", "crate::live_state::");
    assert_path_allowlist(
        "transaction -> live_state",
        &hits,
        ALLOWED_TRANSACTION_LIVE_STATE_FILES,
        "transaction -> live_state leaks should remain sealed after Phase 2",
    );
}

#[test]
fn frozen_scopes_do_not_gain_new_crate_barrel_modules() {
    let imports = collect_all_import_sites();
    let hits = imports
        .into_iter()
        .filter(|site| is_crate_barrel_import(site) && is_frozen_scope(site))
        .collect::<Vec<_>>();

    assert_path_allowlist(
        "scoped crate barrels",
        &hits,
        ALLOWED_LOCAL_BARREL_FILES,
        "filesystem/, key_value/, transaction/, and change_view.rs should not gain new crate-level barrel modules",
    );
}

fn cross_owner_counts(imports: &[ImportSite]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for category in CROSS_OWNER_CATEGORIES {
        let hits = filter_imports_for_category(imports, *category);
        counts.insert(category.key.to_string(), hits.len());
    }
    counts
}

fn format_cross_owner_hits(imports: &[ImportSite]) -> String {
    let mut out = String::new();
    for category in CROSS_OWNER_CATEGORIES {
        let hits = filter_imports_for_category(imports, *category);
        if hits.is_empty() {
            continue;
        }
        out.push_str(category.key);
        out.push('\n');
        for hit in hits {
            out.push_str("  ");
            out.push_str(&hit.path);
            out.push(':');
            out.push_str(&hit.line.to_string());
            out.push_str(": ");
            out.push_str(&hit.import);
            out.push('\n');
        }
    }
    if out.is_empty() {
        out.push_str("(no hits)\n");
    }
    out
}

fn format_count_table(counts: &BTreeMap<String, usize>) -> String {
    let mut out = String::new();
    for (key, count) in counts {
        out.push_str(key);
        out.push_str(": ");
        out.push_str(&count.to_string());
        out.push('\n');
    }
    out
}

fn assert_path_allowlist(name: &str, hits: &[ImportSite], allowed: &[&str], message: &str) {
    let actual_paths = hits
        .iter()
        .map(|hit| hit.path.clone())
        .collect::<BTreeSet<_>>();
    let expected_paths = allowed
        .iter()
        .map(|path| (*path).to_string())
        .collect::<BTreeSet<_>>();

    assert_eq!(
        actual_paths,
        expected_paths,
        "{message}\n{name} actual paths:\n{}\n{name} expected paths:\n{}\n{name} hits:\n{}",
        format_path_set(&actual_paths),
        format_path_set(&expected_paths),
        format_import_sites(hits),
    );
}

fn format_path_set(paths: &BTreeSet<String>) -> String {
    let mut out = String::new();
    for path in paths {
        out.push_str(path);
        out.push('\n');
    }
    if out.is_empty() {
        out.push_str("(none)\n");
    }
    out
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

fn filter_imports_for_category(
    imports: &[ImportSite],
    category: CrossOwnerCategory,
) -> Vec<ImportSite> {
    imports
        .iter()
        .filter(|site| {
            if let Some(exact_path) = category.exact_path {
                site.path == exact_path && site.import.contains(category.needle)
            } else {
                site.path.starts_with(category.prefix) && site.import.contains(category.needle)
            }
        })
        .cloned()
        .collect()
}

fn filter_imports_by_prefix_and_needle(
    imports: &[ImportSite],
    prefix: &str,
    needle: &str,
) -> Vec<ImportSite> {
    imports
        .iter()
        .filter(|site| site.path.starts_with(prefix) && site.import.contains(needle))
        .cloned()
        .collect()
}

fn is_crate_barrel_import(site: &ImportSite) -> bool {
    site.import.starts_with("pub(crate) use crate::") || site.import.starts_with("pub use crate::")
}

fn is_frozen_scope(site: &ImportSite) -> bool {
    site.path.starts_with("src/filesystem/")
        || site.path.starts_with("src/key_value/")
        || site.path.starts_with("src/transaction/")
        || site.path == "src/change_view.rs"
}

fn collect_all_import_sites() -> Vec<ImportSite> {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let root = manifest_dir.join("src");
    let mut imports = Vec::new();
    collect_dir_imports(manifest_dir, &root, &mut imports);
    imports.sort();
    imports
}

fn collect_dir_imports(manifest_dir: &Path, dir: &Path, imports: &mut Vec<ImportSite>) {
    let mut entries = fs::read_dir(dir)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", dir.display()))
        .map(|entry| entry.unwrap_or_else(|error| panic!("failed to read dir entry: {error}")))
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.path());

    for entry in entries {
        let path = entry.path();
        if path.is_dir() {
            if is_test_path(&path) {
                continue;
            }
            collect_dir_imports(manifest_dir, &path, imports);
            continue;
        }
        if path.extension().and_then(|ext| ext.to_str()) != Some("rs") || is_test_path(&path) {
            continue;
        }
        imports.extend(collect_file_imports(manifest_dir, &path));
    }
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

fn is_test_path(path: &Path) -> bool {
    path.components()
        .any(|component| component.as_os_str() == "tests")
        || path.file_name().and_then(|name| name.to_str()) == Some("tests.rs")
}

fn relative_display(manifest_dir: &Path, path: &Path) -> String {
    path.strip_prefix(manifest_dir)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}
