//! Data-driven Markdown v2 round-trip baselines.
//!
//! Exact bytes are measured because only the bounded format tier promises
//! spelling preservation. Semantic GFM equivalence and stability after the
//! first render are required for every fixture.

use chardetng::{EncodingDetector, Iso2022JpDetection, Utf8Detection};
use encoding_rs::Encoding;
use plugin_md_v2::exports::lix::plugin::api::{EntityState, Guest};
use plugin_md_v2::{File, MarkdownPlugin};
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug)]
struct Fixture {
    name: &'static str,
    input: Vec<u8>,
}

#[derive(Debug)]
struct Outcome {
    exact: bool,
    semantic: bool,
    idempotent: bool,
}

#[derive(Debug, Deserialize)]
struct SpecExample {
    example: usize,
    markdown: String,
}

fn file(data: &[u8]) -> File {
    File {
        filename: Some("regression.md".to_string()),
        data: data.to_vec(),
    }
}

fn project(data: &[u8]) -> Vec<EntityState> {
    MarkdownPlugin::detect_changes(Vec::new(), file(data))
        .expect("fixture should parse")
        .into_iter()
        .filter_map(|change| {
            change.snapshot_content.map(|snapshot_content| EntityState {
                entity_pk: change.entity_pk,
                schema_key: change.schema_key,
                snapshot_content,
                metadata: change.metadata,
            })
        })
        .collect()
}

fn evaluate(input: &[u8]) -> Outcome {
    let state = project(input);
    let output = MarkdownPlugin::render(state.clone()).expect("projected state should render");
    let idempotent = MarkdownPlugin::detect_changes(state, file(&output))
        .expect("canonical output should parse")
        .is_empty();

    Outcome {
        exact: output == input,
        semantic: semantic_html(input) == semantic_html(&output),
        idempotent,
    }
}

fn semantic_html(input: &[u8]) -> String {
    let source = decode_like_plugin(input);
    let options = markdown::Options {
        parse: markdown::ParseOptions::gfm(),
        compile: markdown::CompileOptions::gfm(),
    };
    markdown::to_html_with_options(&source, &options)
        .expect("fixture should compile as GFM")
        .trim_end_matches(['\r', '\n'])
        .to_string()
}

fn decode_like_plugin(input: &[u8]) -> String {
    let (buf, encoding) = if let Some((encoding, skip)) = Encoding::for_bom(input) {
        (&input[skip..], encoding)
    } else {
        let mut detector = EncodingDetector::new(Iso2022JpDetection::Allow);
        detector.feed(input, true);
        (input, detector.guess(None, Utf8Detection::Allow))
    };
    let (decoded, _had_errors) = encoding.decode_without_bom_handling(buf);
    normalize_line_endings(&decoded)
}

fn normalize_line_endings(source: &str) -> String {
    source.replace("\r\n", "\n").replace('\r', "\n")
}

fn utf16le(source: &str) -> Vec<u8> {
    let mut bytes = vec![0xff, 0xfe];
    for code_unit in source.encode_utf16() {
        bytes.extend(code_unit.to_le_bytes());
    }
    bytes
}

fn utf16be(source: &str) -> Vec<u8> {
    let mut bytes = vec![0xfe, 0xff];
    for code_unit in source.encode_utf16() {
        bytes.extend(code_unit.to_be_bytes());
    }
    bytes
}

fn fixtures() -> Vec<Fixture> {
    let case = |name, input: &[u8], _exact_expected, _semantic_expected| Fixture {
        name,
        input: input.to_vec(),
    };

    vec![
        case("empty", b"", false, true),
        case("single_final_lf", b"paragraph\n", true, true),
        case("no_final_lf", b"paragraph", false, true),
        case("multiple_final_lf", b"paragraph\n\n\n", false, true),
        case("leading_blank_lines", b"\n\nparagraph\n", false, true),
        case("extra_interblock_blanks", b"# A\n\n\n\nB\n", false, true),
        case("crlf", b"# A\r\n\r\nB\r\n", false, true),
        case("bare_cr", b"# A\r\rB\r", false, true),
        case("utf8_bom", b"\xef\xbb\xbf# A\n\nB\n", false, true),
        Fixture {
            name: "utf16le_bom",
            input: utf16le("# Café\r\n\r\nText\r\n"),
        },
        Fixture {
            name: "utf16be_bom",
            input: utf16be("# Café\n\nText\n"),
        },
        case("atx_heading_closing", b"## Heading ##\n", true, true),
        case("setext_heading", b"Heading\n=======\n", true, true),
        case(
            "emphasis_strike",
            b"Text with *em*, **strong**, and ~~strike~~.\n",
            true,
            true,
        ),
        case(
            "escapes_entities",
            b"Escaped \\*star\\* &amp; &#35;.\n",
            true,
            true,
        ),
        case("task_list", b"- [x] done\n- [ ] todo\n", true, true),
        case(
            "gfm_table",
            b"| A | B |\n| :- | -: |\n| x | y |\n",
            true,
            true,
        ),
        case(
            "literal_autolink",
            b"Visit www.example.com and a@example.com.\n",
            true,
            true,
        ),
        case(
            "angle_autolink",
            b"<https://example.com/a?b=1>\n",
            true,
            true,
        ),
        case("bracketed_autolink", b"[www.example.com]\n", true, true),
        case("footnote", b"Text[^1].\n\n[^1]: note\n", true, true),
        case(
            "raw_html",
            b"<details open>\n<summary>Title</summary>\nBody\n</details>\n",
            true,
            true,
        ),
        case("html_comment", b"<!-- comment -->\n", true, true),
        case(
            "fenced_code_info",
            b"~~~rust key=value\nfn main() {}\n~~~\n",
            true,
            true,
        ),
        case("indented_code", b"    let x = 1;\n", true, true),
        case(
            "blockquote_nested_list",
            b"> quote\n>\n> 1. one\n> 2. two\n",
            true,
            true,
        ),
        case("thematic_break", b"***\n", true, true),
        case("hard_break_spaces", b"line one  \nline two\n", true, true),
        case(
            "reference_link",
            b"[label][id]\n\n[id]: https://example.com \"title\"\n",
            true,
            true,
        ),
        case(
            "duplicate_reference",
            b"[x][a]\n\n[a]: /first\n[a]: /second\n",
            false,
            true,
        ),
        case(
            "unicode_non_latin",
            "你好 — café — 👩‍💻\n".as_bytes(),
            true,
            true,
        ),
        case("nul_text", b"a\0b\n", true, true),
        case("trailing_spaces_block", b"paragraph   \n", true, true),
        case(
            "list_marker_variants",
            b"* one\n* two\n\n1) first\n2) second\n",
            true,
            true,
        ),
        case("loose_list_extra_blank", b"- one\n\n- two\n", true, true),
        case(
            "yaml_frontmatter",
            b"---\ntitle: Test\n---\n\nBody\n",
            true,
            true,
        ),
        case("mdx_jsx_like", b"<Component foo={1} />\n", true, true),
        case(
            "definition_then_thematic_break",
            b"[a]: /u\n---\n",
            false,
            false,
        ),
        case(
            "definition_then_indented_text",
            b"[a]: /u\n    code\n",
            false,
            false,
        ),
        case(
            "definition_then_custom_html",
            b"[a]: /u\n<x>\n",
            false,
            false,
        ),
        case("unclosed_fence_blank_content", b"~~~\n\n", false, false),
    ]
}

#[test]
fn current_roundtrip_corpus_matches_measured_baseline() {
    let fixtures = fixtures();
    let total = fixtures.len();
    let mut exact = 0usize;
    let mut semantic = 0usize;
    let mut idempotent = 0usize;

    eprintln!("| fixture | exact bytes | semantic GFM | idempotent |");
    eprintln!("| --- | --- | --- | --- |");
    for fixture in &fixtures {
        let outcome = evaluate(&fixture.input);
        eprintln!(
            "| {} | {} | {} | {} |",
            fixture.name, outcome.exact, outcome.semantic, outcome.idempotent
        );
        if !outcome.semantic {
            let state = project(&fixture.input);
            let output = MarkdownPlugin::render(state).unwrap();
            eprintln!(
                "input={:?}\noutput={:?}\ninput_html={}\noutput_html={}",
                String::from_utf8_lossy(&fixture.input),
                String::from_utf8_lossy(&output),
                semantic_html(&fixture.input),
                semantic_html(&output)
            );
        }
        if !outcome.idempotent {
            let state = project(&fixture.input);
            let output = MarkdownPlugin::render(state.clone()).unwrap();
            let delta = MarkdownPlugin::detect_changes(state, file(&output)).unwrap();
            eprintln!(
                "non-idempotent input={:?}\noutput={:?}\ndelta={delta:#?}",
                String::from_utf8_lossy(&fixture.input),
                String::from_utf8_lossy(&output)
            );
        }
        assert!(outcome.semantic, "{} lost GFM semantics", fixture.name);
        assert!(
            outcome.idempotent,
            "{} should be stable after its first render",
            fixture.name
        );
        exact += usize::from(outcome.exact);
        semantic += usize::from(outcome.semantic);
        idempotent += usize::from(outcome.idempotent);
    }

    assert_eq!(semantic, fixtures.len());
    assert_eq!(idempotent, fixtures.len());
    eprintln!(
        "current corpus totals: exact={exact}/{total} semantic={semantic}/{total} idempotent={idempotent}/{total}"
    );
}

#[test]
fn repository_markdown_is_semantic_and_idempotent() {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut paths = Vec::new();
    collect_markdown_files(&repository, &mut paths);
    paths.sort();

    let mut exact = 0usize;
    for path in &paths {
        let input = fs::read(path).expect("repository Markdown should be readable");
        let outcome = evaluate(&input);
        assert!(
            outcome.semantic,
            "repository Markdown lost semantics: {}",
            path.display()
        );
        assert!(
            outcome.idempotent,
            "repository Markdown was not idempotent: {}",
            path.display()
        );
        exact += usize::from(outcome.exact);
    }

    assert!(!paths.is_empty(), "repository corpus should not be empty");
    eprintln!(
        "repository corpus totals: exact={exact}/{} semantic={}/{} idempotent={}/{}",
        paths.len(),
        paths.len(),
        paths.len(),
        paths.len(),
        paths.len()
    );
}

#[test]
#[ignore = "opt-in: requires GFM_SPEC_JSON or /tmp/gfm-spec.json"]
fn official_gfm_examples_are_semantic_and_idempotent() {
    evaluate_official_examples("GFM", "GFM_SPEC_JSON", "/tmp/gfm-spec.json", 670);
}

#[test]
#[ignore = "opt-in: requires COMMONMARK_SPEC_JSON or /tmp/commonmark-spec-0.31.2.json"]
fn official_commonmark_examples_are_semantic_and_idempotent() {
    evaluate_official_examples(
        "CommonMark",
        "COMMONMARK_SPEC_JSON",
        "/tmp/commonmark-spec-0.31.2.json",
        652,
    );
}

fn evaluate_official_examples(
    label: &str,
    environment_variable: &str,
    default_path: &str,
    expected_count: usize,
) {
    let path = std::env::var_os(environment_variable)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(default_path));
    let examples: Vec<SpecExample> =
        serde_json::from_slice(&fs::read(&path).unwrap_or_else(|error| {
            panic!(
                "failed to read {label} corpus '{}': {error}",
                path.display()
            )
        }))
        .unwrap_or_else(|error| {
            panic!("{label} corpus should be a JSON array of examples: {error}")
        });
    assert_eq!(
        examples.len(),
        expected_count,
        "unexpected {label} corpus size"
    );
    let mut exact = 0usize;
    let mut semantic = 0usize;
    let mut idempotent = 0usize;
    let mut failures = Vec::new();
    for example in &examples {
        let outcome = evaluate(example.markdown.as_bytes());
        exact += usize::from(outcome.exact);
        semantic += usize::from(outcome.semantic);
        idempotent += usize::from(outcome.idempotent);
        if !outcome.semantic || !outcome.idempotent {
            failures.push(format!(
                "example {}: semantic={} idempotent={}",
                example.example, outcome.semantic, outcome.idempotent
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {} GFM examples failed:\n{}",
        failures.len(),
        examples.len(),
        failures.join("\n")
    );
    eprintln!(
        "official {label} totals: exact={exact}/{} semantic={semantic}/{} idempotent={idempotent}/{}",
        examples.len(),
        examples.len(),
        examples.len()
    );
}

fn collect_markdown_files(directory: &Path, paths: &mut Vec<PathBuf>) {
    let entries = fs::read_dir(directory).expect("repository directory should be readable");
    for entry in entries {
        let entry = entry.expect("repository entry should be readable");
        let path = entry.path();
        if path.is_dir() {
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("");
            if name.starts_with('.') || matches!(name, "dist" | "node_modules" | "target") {
                continue;
            }
            collect_markdown_files(&path, paths);
        } else if path.extension().is_some_and(|extension| extension == "md") {
            paths.push(path);
        }
    }
}
