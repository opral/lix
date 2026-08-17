//! Conformance-suite reader.
//!
//! The bench owns its conformance test data. Every runnable
//! `(markdown input → expected HTML, options, label)` case is stored in our own
//! byte-counted fixture files under
//! `tests/fixtures/conformance/<category>/<source>.cases`. This module
//! parses those fixtures into [`OracleTuple`]s. The suite [`Category`]
//! (commonmark vs gfm) is derived from the subdirectory the file lives in — it
//! is a spec-layer grouping label, not a rendering switch.
//!
//! The fixtures are a faithful snapshot of the cases this bench actually RUNS;
//! cases that the runner would skip are not stored (they were never compared).
//!
//! Fixture format (one file per former oracle source, grouped by category dir):
//!
//! ```text
//! # markdown-syntax AST->HTML conformance suite v1
//! source: <name>
//! count: <N>
//!
//! --- case <i> options <tok,tok|-> label-bytes <L> input-bytes <I> expected-bytes <E>
//! <L bytes of label>
//! --- input
//! <I bytes of markdown input>
//! --- expected
//! <E bytes of expected HTML>
//! --- end
//! ```
//!
//! Each payload is read by its declared byte length, so multi-line inputs or
//! expected HTML containing any delimiter-like text round-trip exactly.
//!
//! The public surface is frozen:
//!
//!   pub fn load_all() -> Vec<crate::types::OracleTuple>
//!
//! `load_all` logs a per-file count summary. Snapshot-integrity anchors live in
//! the test entry so this reader remains a parser only.

use std::fs;
use std::path::{Path, PathBuf};

use super::types::{Category, OracleTuple};

const SUITE_ROOT: &str = "tests/fixtures/conformance";

/// Load every runnable oracle case from our own conformance-suite fixtures.
pub fn load_all() -> Vec<OracleTuple> {
    let mut all = Vec::new();
    let mut commonmark_total = 0usize;
    let mut gfm_total = 0usize;

    for (file, category) in suite_files() {
        let tuples = parse_file(&file, category);
        let n = tuples.len();
        let rel = relative_label(&file);
        match category {
            Category::Gfm => gfm_total += n,
            Category::CommonMark => commonmark_total += n,
        }
        log_count(&rel, n);
        all.extend(tuples);
    }

    eprintln!(
        "[suite] commonmark total = {commonmark_total}; gfm total = {gfm_total}; grand total = {}",
        commonmark_total + gfm_total
    );

    all
}

fn log_count(rel: &str, n: usize) {
    eprintln!("[suite] {rel}: {n} cases");
}

/// All `.cases` files under the suite root, paired with their suite category
/// (the subdirectory they live in), sorted for deterministic order.
fn suite_files() -> Vec<(PathBuf, Category)> {
    let mut files = Vec::new();
    for (subdir, category) in [("commonmark", Category::CommonMark), ("gfm", Category::Gfm)] {
        let dir = Path::new(SUITE_ROOT).join(subdir);
        let entries =
            fs::read_dir(&dir).unwrap_or_else(|e| panic!("read suite dir {}: {e}", dir.display()));
        for entry in entries {
            let path = entry.expect("suite dir entry").path();
            if path.extension().and_then(|e| e.to_str()) == Some("cases") {
                files.push((path, category));
            }
        }
    }
    files.sort_by(|a, b| a.0.cmp(&b.0));
    files
}

fn relative_label(path: &Path) -> String {
    path.strip_prefix(SUITE_ROOT)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn parse_file(path: &Path, category: Category) -> Vec<OracleTuple> {
    let src = fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let rel = relative_label(path);
    // `source_file` is `&'static str`; this is test-only code, so leaking the
    // small per-file label is acceptable and keeps types.rs unchanged.
    let source_file: &'static str = Box::leak(rel.clone().into_boxed_str());

    let mut tuples = Vec::new();
    let mut cursor = 0;
    let declared_count = header_count(&src);

    while let Some(rel_start) = src[cursor..].find("--- case ") {
        let header_start = cursor + rel_start;
        let header_end = src[header_start..]
            .find('\n')
            .map(|o| header_start + o)
            .unwrap_or(src.len());
        let header = &src[header_start..header_end];
        let parsed = parse_case_header(&rel, header);

        // label payload
        let label_start = header_end + 1;
        let label_end = label_start + parsed.label_bytes;
        let label = read_payload(&src, &rel, label_start, label_end, "label");
        expect_marker(&src, &rel, label_end, "\n--- input\n");

        // input payload
        let input_start = label_end + "\n--- input\n".len();
        let input_end = input_start + parsed.input_bytes;
        let input = read_payload(&src, &rel, input_start, input_end, "input");
        expect_marker(&src, &rel, input_end, "\n--- expected\n");

        // expected payload
        let expected_start = input_end + "\n--- expected\n".len();
        let expected_end = expected_start + parsed.expected_bytes;
        let expected = read_payload(&src, &rel, expected_start, expected_end, "expected");
        expect_marker(&src, &rel, expected_end, "\n--- end\n");

        let label = if parsed.has_label {
            Some(label.to_string())
        } else {
            None
        };

        tuples.push(OracleTuple {
            source_file,
            category,
            label,
            input: input.to_string(),
            expected_html: expected.to_string(),
            option_tokens: parsed.option_tokens,
        });

        cursor = expected_end + "\n--- end\n".len();
    }

    if let Some(declared) = declared_count {
        assert_eq!(
            tuples.len(),
            declared,
            "{rel}: declared count {declared} != parsed cases {}",
            tuples.len()
        );
    }

    tuples
}

struct CaseHeader {
    option_tokens: Vec<String>,
    has_label: bool,
    label_bytes: usize,
    input_bytes: usize,
    expected_bytes: usize,
}

fn parse_case_header(rel: &str, header: &str) -> CaseHeader {
    // --- case <i> options <toks|-> label-bytes <L> input-bytes <I> expected-bytes <E>
    let parts: Vec<&str> = header.split_whitespace().collect();
    assert!(
        parts.len() == 11
            && parts[0] == "---"
            && parts[1] == "case"
            && parts[3] == "options"
            && parts[5] == "label-bytes"
            && parts[7] == "input-bytes"
            && parts[9] == "expected-bytes",
        "{rel}: invalid case header: {header}"
    );

    let option_tokens = if parts[4] == "-" {
        Vec::new()
    } else {
        parts[4].split(',').map(|s| s.to_string()).collect()
    };

    let label_bytes = parse_usize(rel, header, parts[6]);
    let input_bytes = parse_usize(rel, header, parts[8]);
    let expected_bytes = parse_usize(rel, header, parts[10]);

    CaseHeader {
        option_tokens,
        // A zero-length label is recorded as "no label" (the source had none);
        // genuine empty-string labels never occur in the snapshot.
        has_label: label_bytes > 0,
        label_bytes,
        input_bytes,
        expected_bytes,
    }
}

fn parse_usize(rel: &str, header: &str, value: &str) -> usize {
    value
        .parse::<usize>()
        .unwrap_or_else(|e| panic!("{rel}: invalid byte count `{value}` in {header}: {e}"))
}

fn read_payload<'a>(src: &'a str, rel: &str, start: usize, end: usize, what: &str) -> &'a str {
    assert!(
        end <= src.len(),
        "{rel}: {what} payload exceeds file length"
    );
    assert!(
        src.is_char_boundary(start) && src.is_char_boundary(end),
        "{rel}: {what} payload is not on a UTF-8 boundary"
    );
    &src[start..end]
}

fn expect_marker(src: &str, rel: &str, at: usize, marker: &str) {
    assert!(
        src[at..].starts_with(marker),
        "{rel}: expected `{}` marker at byte {at}",
        marker.trim_matches('\n')
    );
}

/// Read the `count: N` metadata header line (before the first case).
fn header_count(src: &str) -> Option<usize> {
    for line in src.lines() {
        if line.starts_with("--- case ") {
            break;
        }
        if let Some(value) = line.strip_prefix("count: ") {
            return value.trim().parse::<usize>().ok();
        }
    }
    None
}
