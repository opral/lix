use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

const COMPILER_CORE_DIRS: &[&str] = &[
    "src/sql/parser",
    "src/sql/binder",
    "src/sql/semantic_ir",
    "src/sql/logical_plan",
    "src/sql/routing",
    "src/sql/optimizer",
    "src/sql/physical_plan",
    "src/sql/explain",
];

const EXPECTED_COMPILER_CORE_COUNTS: &[(&str, usize)] = &[
    ("canonical_read", 1),
    ("refs", 0),
    ("version", 5),
    ("live_state_root", 0),
    ("workspace_writer_key", 1),
    ("filesystem_debt", 10),
    ("live_state_concrete_contract_debt", 0),
];

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ImportRecord {
    path: String,
    line: usize,
    import: String,
}

#[test]
fn compiler_core_sql_owner_import_surface_matches_phase_a_contract() {
    let imports = collect_top_level_owner_imports(COMPILER_CORE_DIRS);

    let mut counts = BTreeMap::<&'static str, usize>::new();
    let mut forbidden = Vec::new();

    for record in imports {
        classify_compiler_core_import(&record, &mut counts, &mut forbidden);
    }

    assert_expected_counts("compiler-core", &counts, EXPECTED_COMPILER_CORE_COUNTS);
    assert!(
        forbidden.is_empty(),
        "compiler-core SQL imports forbidden owners after Phase A:\n{}",
        format_records(&forbidden)
    );
}

#[test]
fn phase_c_deletes_sql_state_reader_glue_module() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let state_reader = manifest_dir.join("src/sql/services/state_reader.rs");
    assert!(
        !state_reader.exists(),
        "Phase C should delete src/sql/services/state_reader.rs; found {}",
        state_reader.display()
    );
}

#[test]
fn phase_a_does_not_introduce_sql_capabilities_subsystem() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let sql_capabilities = manifest_dir.join("src/sql/capabilities");
    assert!(
        !sql_capabilities.exists(),
        "Phase A should not introduce src/sql/capabilities; found {}",
        sql_capabilities.display()
    );
}

fn classify_compiler_core_import(
    record: &ImportRecord,
    counts: &mut BTreeMap<&'static str, usize>,
    forbidden: &mut Vec<ImportRecord>,
) {
    let line = record.import.as_str();

    if is_commit_import(line)
        || is_canonical_internal_import(line)
        || is_forbidden_workspace_import(line)
    {
        forbidden.push(record.clone());
        return;
    }

    if is_canonical_read_import(line) {
        bump(counts, "canonical_read");
        return;
    }

    if is_refs_import(line) {
        bump(counts, "refs");
        return;
    }

    if is_version_import(line) {
        bump(counts, "version");
        return;
    }

    if is_workspace_writer_key_import(line) {
        bump(counts, "workspace_writer_key");
        return;
    }

    if is_live_state_import(line) {
        bump(counts, "live_state_root");
        if imports_concrete_live_state_contract(line) {
            bump(counts, "live_state_concrete_contract_debt");
        }
        return;
    }

    if is_filesystem_import(line) {
        bump(counts, "filesystem_debt");
        return;
    }

    forbidden.push(record.clone());
}

fn collect_top_level_owner_imports(rel_dirs: &[&str]) -> Vec<ImportRecord> {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut records = Vec::new();
    for rel_dir in rel_dirs {
        let dir = manifest_dir.join(rel_dir);
        collect_dir_imports(manifest_dir, &dir, &mut records);
    }
    records.sort();
    records
}

fn collect_dir_imports(manifest_dir: &Path, dir: &Path, records: &mut Vec<ImportRecord>) {
    let mut entries = fs::read_dir(dir)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", dir.display()))
        .map(|entry| entry.unwrap_or_else(|error| panic!("failed to read dir entry: {error}")))
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.path());

    for entry in entries {
        let path = entry.path();
        if path.is_dir() {
            collect_dir_imports(manifest_dir, &path, records);
            continue;
        }
        if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
            continue;
        }
        records.extend(collect_file_imports(manifest_dir, &path));
    }
}

fn collect_file_imports(manifest_dir: &Path, path: &Path) -> Vec<ImportRecord> {
    let text = fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
    let relative = relative_display(manifest_dir, path);

    let mut imports = Vec::new();
    let mut previous_nonempty = String::new();
    let mut current_import: Option<(usize, String)> = None;
    for (index, raw_line) in text.lines().enumerate() {
        let line_no = index + 1;
        let line = raw_line.trim_end();
        if line.is_empty() {
            continue;
        }
        if let Some((start_line, buffer)) = current_import.as_mut() {
            if !buffer.is_empty() {
                buffer.push(' ');
            }
            buffer.push_str(line.trim());
            if line.trim_end().ends_with(';') {
                let import = std::mem::take(buffer);
                if is_owner_import(&import) {
                    imports.push(ImportRecord {
                        path: relative.clone(),
                        line: *start_line,
                        import,
                    });
                }
                current_import = None;
            }
            previous_nonempty = line.to_string();
            continue;
        }
        if line.starts_with("use crate::") || line.starts_with("pub(crate) use crate::") {
            if previous_nonempty.starts_with("#[cfg(test)]") {
                previous_nonempty = line.to_string();
                continue;
            }
            if line.trim_end().ends_with(';') {
                if is_owner_import(line) {
                    imports.push(ImportRecord {
                        path: relative.clone(),
                        line: line_no,
                        import: line.to_string(),
                    });
                }
            } else {
                current_import = Some((line_no, line.trim().to_string()));
            }
        }
        previous_nonempty = line.to_string();
    }

    imports
}

fn relative_display(manifest_dir: &Path, path: &Path) -> String {
    path.strip_prefix(manifest_dir)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn is_owner_import(line: &str) -> bool {
    line.contains("crate::canonical::")
        || line.contains("crate::commit::")
        || line.contains("crate::refs::")
        || line.contains("crate::version::")
        || line.contains("crate::live_state::")
        || line.contains("crate::workspace::")
        || line.contains("crate::filesystem::")
}

fn is_canonical_read_import(line: &str) -> bool {
    line.contains("crate::canonical::read::")
}

fn is_canonical_internal_import(line: &str) -> bool {
    line.contains("crate::canonical::graph::")
        || line.contains("crate::canonical::journal::")
        || line.contains("crate::canonical::init::")
}

fn is_commit_import(line: &str) -> bool {
    line.contains("crate::commit::")
}

fn is_refs_import(line: &str) -> bool {
    line.contains("crate::refs::")
}

fn is_version_import(line: &str) -> bool {
    line.contains("crate::version::")
}

fn is_live_state_import(line: &str) -> bool {
    line.contains("crate::live_state::")
}

fn is_workspace_writer_key_import(line: &str) -> bool {
    line.contains("crate::workspace::writer_key::")
}

fn is_forbidden_workspace_import(line: &str) -> bool {
    line.contains("crate::workspace::") && !is_workspace_writer_key_import(line)
}

fn is_filesystem_import(line: &str) -> bool {
    line.contains("crate::filesystem::")
}

fn imports_concrete_live_state_contract(line: &str) -> bool {
    line.contains("LiveReadContract")
        || line.contains("TrackedRow")
        || line.contains("UntrackedRow")
        || line.contains("RowIdentity")
        || line.contains("ScanConstraint")
        || line.contains("ScanField")
        || line.contains("ScanOperator")
}

fn bump(counts: &mut BTreeMap<&'static str, usize>, key: &'static str) {
    *counts.entry(key).or_default() += 1;
}

fn assert_expected_counts(
    scope: &str,
    actual: &BTreeMap<&'static str, usize>,
    expected: &[(&str, usize)],
) {
    for (key, expected_count) in expected {
        let actual_count = actual.get(key).copied().unwrap_or_default();
        assert_eq!(
            actual_count, *expected_count,
            "{scope} owner-import count for '{key}' changed: expected {expected_count}, got {actual_count}"
        );
    }

    let expected_keys = expected.iter().map(|(key, _)| *key).collect::<Vec<_>>();
    let unexpected = actual
        .keys()
        .filter(|key| !expected_keys.contains(key))
        .copied()
        .collect::<Vec<_>>();
    assert!(
        unexpected.is_empty(),
        "{scope} recorded unexpected owner-import categories: {unexpected:?}"
    );
}

fn format_records(records: &[ImportRecord]) -> String {
    records
        .iter()
        .map(|record| format!("{}:{}: {}", record.path, record.line, record.import))
        .collect::<Vec<_>>()
        .join("\n")
}
