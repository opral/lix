use std::fs;
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ReferenceHit {
    path: String,
    line: usize,
    text: String,
}

#[test]
fn sql_does_not_reference_transaction() {
    assert_no_reference("src/sql", "crate::transaction::");
}

#[test]
fn sql_does_not_reference_live_state() {
    assert_no_reference("src/sql", "crate::live_state::");
}

#[test]
fn sql_does_not_reference_session() {
    assert_no_reference("src/sql", "crate::session::");
}

#[test]
fn sql_does_not_reference_engine() {
    assert_no_reference("src/sql", "crate::engine::");
}

#[test]
fn sql_does_not_reference_read() {
    assert_no_reference("src/sql", "crate::read::");
}

#[test]
fn live_state_does_not_reference_sql() {
    assert_no_reference("src/live_state", "crate::sql::");
}

#[test]
fn live_state_does_not_reference_transaction() {
    assert_no_reference("src/live_state", "crate::transaction::");
}

#[test]
fn live_state_does_not_reference_session() {
    assert_no_reference("src/live_state", "crate::session::");
}

#[test]
fn read_does_not_reference_sql() {
    assert_no_reference("src/read", "crate::sql::");
}

#[test]
fn canonical_does_not_reference_live_state() {
    assert_no_reference("src/canonical", "crate::live_state::");
}

#[test]
fn runtime_does_not_reference_sql() {
    assert_no_reference("src/runtime", "crate::sql::");
}

#[test]
fn runtime_does_not_reference_live_state() {
    assert_no_reference("src/runtime", "crate::live_state::");
}

#[test]
fn runtime_does_not_reference_session() {
    assert_no_reference("src/runtime", "crate::session::");
}

#[test]
fn runtime_does_not_reference_transaction() {
    assert_no_reference("src/runtime", "crate::transaction::");
}

fn assert_no_reference(relative_dir: &str, needle: &str) {
    let hits = collect_reference_hits(relative_dir, needle);
    assert!(
        hits.is_empty(),
        "found forbidden reference `{needle}` under {relative_dir}:\n{}",
        format_hits(&hits)
    );
}

fn collect_reference_hits(relative_dir: &str, needle: &str) -> Vec<ReferenceHit> {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let root = manifest_dir.join(relative_dir);
    let mut hits = Vec::new();
    collect_dir_hits(manifest_dir, &root, needle, &mut hits);
    hits.sort();
    hits
}

fn collect_dir_hits(manifest_dir: &Path, dir: &Path, needle: &str, hits: &mut Vec<ReferenceHit>) {
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
            collect_dir_hits(manifest_dir, &path, needle, hits);
            continue;
        }
        if path.extension().and_then(|ext| ext.to_str()) != Some("rs") || is_test_path(&path) {
            continue;
        }
        hits.extend(collect_file_hits(manifest_dir, &path, needle));
    }
}

fn collect_file_hits(manifest_dir: &Path, path: &Path, needle: &str) -> Vec<ReferenceHit> {
    let text = fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
    let relative = relative_display(manifest_dir, path);

    let mut hits = Vec::new();
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

        if !ignore_line
            && !trimmed.starts_with("//")
            && !trimmed.starts_with("///")
            && !trimmed.starts_with("//!")
            && raw_line.contains(needle)
        {
            hits.push(ReferenceHit {
                path: relative.clone(),
                line: line_no,
                text: trimmed.to_string(),
            });
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

    hits
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

fn format_hits(hits: &[ReferenceHit]) -> String {
    hits.iter()
        .map(|hit| format!("{}:{}: {}", hit.path, hit.line, hit.text))
        .collect::<Vec<_>>()
        .join("\n")
}
