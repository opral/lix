//! AST → HTML conformance bench.
//!
//! This harness uses the crate's opt-in public HTML renderer purely to MEASURE
//! how faithfully the parser's AST reflects CommonMark/GFM semantics, by
//! comparing `parse(input) → to_html_with_options(AST) → HTML` against this
//! bench's own conformance suite under
//! `tests/fixtures/conformance/<category>/<source>.cases`.
//!
//! It exists because the rest of the suite verifies round-trip STABILITY, not
//! CORRECTNESS; this is the only place an actual conformance number is produced.
//!
//! Layout (each declared with an explicit `#[path]` from this crate root so the
//! submodules live under `tests/html_conformance/`):
//!   - `types`      — frozen shared types (OracleTuple, Category, …)
//!   - `normalizer` — faithful port of CommonMark `normalize.py`
//!   - `extractor`  — reads (input, expected_html, options) cases from our suite fixtures
//!   - `runner`     — maps each case's options → parse+public render+compare
//!   - `report`     — pass/fail/skip tallies, headline %, failure dump

#![allow(dead_code)]

#[path = "html_conformance/types.rs"]
mod types;

#[path = "html_conformance/normalizer.rs"]
mod normalizer;

#[path = "html_conformance/extractor.rs"]
mod extractor;

#[path = "html_conformance/runner.rs"]
mod runner;

#[path = "html_conformance/report.rs"]
mod report;

/// Snapshot-integrity check: our CommonMark-spec source fixture must carry
/// exactly 652 cases (the snapshot of the upstream CommonMark spec corpus).
#[test]
fn corpus_counts_match() {
    let tuples = extractor::load_all();
    let commonmark = tuples
        .iter()
        .filter(|t| t.source_file.ends_with("commonmark/commonmark.cases"))
        .count();
    assert_eq!(
        commonmark, 652,
        "commonmark/commonmark.cases must carry exactly 652 cases, got {commonmark}"
    );
}

/// The measurement: parse → render → compare every runnable oracle tuple and
/// print a per-suite / per-file conformance breakdown. Does NOT assert a
/// threshold — it reports a number and dumps failures for triage.
#[test]
fn html_conformance_report() {
    let report = runner::run_all();
    report.print_summary();
    report.write_failures("target/html_conformance_failures.txt");
}
