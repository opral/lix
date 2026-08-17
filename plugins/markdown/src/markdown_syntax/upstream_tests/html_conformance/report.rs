//! Conformance result aggregation + reporting.

use std::collections::BTreeMap;
use std::fs;

use super::types::Category;

/// Per-case outcome.
pub enum Outcome {
    /// Byte-equal after only trimming the document edge (strictest).
    PassRaw,
    /// Equal after full CommonMark normalization, but not byte-raw (cosmetic
    /// difference the normalizer legitimately erases).
    PassNormalized,
    /// Real mismatch after normalization.
    Fail {
        input: String,
        expected: String,
        actual: String,
    },
    /// The parser returned an error for this input (config or strict failure).
    ParseError(String),
}

pub struct CaseResult {
    pub source_file: &'static str,
    pub category: Category,
    pub label: Option<String>,
    pub outcome: Outcome,
}

pub struct Report {
    pub results: Vec<CaseResult>,
}

#[derive(Default, Clone, Copy)]
struct Tally {
    pass_raw: usize,
    pass_norm: usize,
    fail: usize,
    parse_error: usize,
}

impl Tally {
    fn add(&mut self, o: &Outcome) {
        match o {
            Outcome::PassRaw => self.pass_raw += 1,
            Outcome::PassNormalized => self.pass_norm += 1,
            Outcome::Fail { .. } => self.fail += 1,
            Outcome::ParseError(_) => self.parse_error += 1,
        }
    }
    fn ran(&self) -> usize {
        self.pass_raw + self.pass_norm + self.fail + self.parse_error
    }
    fn passed(&self) -> usize {
        self.pass_raw + self.pass_norm
    }
    fn pct(&self) -> f64 {
        let ran = self.ran();
        if ran == 0 {
            0.0
        } else {
            100.0 * self.passed() as f64 / ran as f64
        }
    }
}

impl Report {
    pub fn print_summary(&self) {
        let mut by_suite: BTreeMap<&'static str, Tally> = BTreeMap::new();
        let mut by_file: BTreeMap<&'static str, Tally> = BTreeMap::new();
        let mut total = Tally::default();

        for r in &self.results {
            let sname = category_name(r.category);
            by_suite.entry(sname).or_default().add(&r.outcome);
            by_file.entry(r.source_file).or_default().add(&r.outcome);
            total.add(&r.outcome);
        }

        println!("\n================ AST→HTML CONFORMANCE ================");
        println!(
            "total cases: {}   ran: {}   passed: {}   failed: {}   parse-errors: {}",
            self.results.len(),
            total.ran(),
            total.passed(),
            total.fail,
            total.parse_error,
        );
        println!(
            "HEADLINE conformance: pass {} / ran {} = {:.2}%   (byte-exact: {}, normalized-only: {})",
            total.passed(),
            total.ran(),
            total.pct(),
            total.pass_raw,
            total.pass_norm,
        );

        println!("\n-- by suite --");
        for (suite, t) in &by_suite {
            println!(
                "  {suite:<12} ran {:>5}  pass {:>5} ({:.2}%)  fail {:>5}  perr {:>4}",
                t.ran(),
                t.passed(),
                t.pct(),
                t.fail,
                t.parse_error,
            );
        }

        println!("\n-- files with failures (file: fail/ran, parse-errors) --");
        let mut files: Vec<(&&str, &Tally)> = by_file.iter().collect();
        files.sort_by(|a, b| {
            b.1.fail
                .cmp(&a.1.fail)
                .then(b.1.parse_error.cmp(&a.1.parse_error))
        });
        for (file, t) in files {
            if t.fail == 0 && t.parse_error == 0 {
                continue;
            }
            let short = file.rsplit('/').next().unwrap_or(file);
            println!(
                "  {short:<32} fail {:>4}/{:<5}  perr {:>4}  pass {:>4} ({:.1}%)",
                t.fail,
                t.ran(),
                t.parse_error,
                t.passed(),
                t.pct(),
            );
        }
        println!("=====================================================\n");
    }

    /// Dump every failure (and parse error) as an inspectable block for triage.
    pub fn write_failures(&self, path: &str) {
        let mut out = String::new();
        let mut n = 0;
        for r in &self.results {
            match &r.outcome {
                Outcome::Fail {
                    input,
                    expected,
                    actual,
                } => {
                    n += 1;
                    out.push_str(&format!(
                        "### FAIL #{n} [{}] {}\n--- input ---\n{}\n--- expected ---\n{}\n--- actual ---\n{}\n\n",
                        r.source_file,
                        r.label.as_deref().unwrap_or(""),
                        show(input),
                        show(expected),
                        show(actual),
                    ));
                }
                Outcome::ParseError(e) => {
                    n += 1;
                    out.push_str(&format!(
                        "### PARSE-ERROR #{n} [{}] {}\n{}\n\n",
                        r.source_file,
                        r.label.as_deref().unwrap_or(""),
                        e,
                    ));
                }
                _ => {}
            }
        }
        let header = format!("{n} failures/parse-errors\n\n");
        let _ = fs::write(path, format!("{header}{out}"));
        println!("wrote {n} failure blocks to {path}");
    }
}

fn show(s: &str) -> String {
    // make control chars / trailing space visible
    s.replace('\t', "\\t")
}

fn category_name(c: Category) -> &'static str {
    match c {
        Category::CommonMark => "commonmark",
        Category::Gfm => "gfm",
    }
}
