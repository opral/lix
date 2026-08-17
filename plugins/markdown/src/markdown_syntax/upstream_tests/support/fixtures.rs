use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

use crate::markdown_syntax::{
    AutolinkKind, Block, Constructs, DiagnosticSeverity, Document, Inline, ParseOptions,
    SerializeOptions, SyntaxOptions,
};

pub(crate) fn profile_options(profile: &str) -> SyntaxOptions {
    match profile {
        "commonmark" => SyntaxOptions::commonmark(),
        "gfm" => SyntaxOptions::gfm(),
        "mdx" => SyntaxOptions::mdx(),
        "math" => {
            let mut constructs = Constructs::commonmark();
            constructs.math_block = true;
            constructs.math_inline = true;
            SyntaxOptions {
                constructs: constructs,
                parse: ParseOptions::default(),
            }
        }
        "frontmatter" => {
            let mut constructs = Constructs::commonmark();
            constructs.frontmatter = true;
            SyntaxOptions {
                constructs: constructs,
                parse: ParseOptions::default(),
            }
        }
        "preserve-escapes" => SyntaxOptions {
            constructs: Constructs::commonmark(),
            parse: ParseOptions {
                preserve_character_escapes: true,
                ..ParseOptions::default()
            },
        },
        "extras" => SyntaxOptions {
            constructs: extra_constructs(),
            parse: extra_parse_options(),
        },
        "wikilink-after" => {
            let mut constructs = extra_constructs();
            constructs.wikilink_title_after_pipe = true;
            SyntaxOptions {
                constructs: constructs,
                parse: extra_parse_options(),
            }
        }
        "wikilink-before" => {
            let mut constructs = extra_constructs();
            constructs.wikilink_title_before_pipe = true;
            SyntaxOptions {
                constructs: constructs,
                parse: extra_parse_options(),
            }
        }
        other => panic!("unknown derived corpus profile: {other}"),
    }
}

fn extra_constructs() -> Constructs {
    let mut constructs = Constructs::gfm();
    constructs.math_block = true;
    constructs.math_inline = true;
    constructs.frontmatter = true;
    constructs.gfm_alert = true;
    constructs.underline = true;
    constructs.insert = true;
    constructs.highlight = true;
    constructs.subscript = true;
    constructs.superscript = true;
    constructs.spoiler = true;
    constructs.shortcode = true;
    constructs.description_list = true;
    constructs.inline_footnote = true;
    constructs.directive_text = true;
    constructs.directive_leaf = true;
    constructs.directive_container = true;
    constructs
}

fn extra_parse_options() -> ParseOptions {
    ParseOptions {
        single_tilde_strikethrough: true,
        preserve_character_escapes: false,
        preserve_character_references: false,
    }
}

pub(crate) fn assert_fixture(stem: &str, options: SyntaxOptions) {
    let input = read_fixture(&format!("{stem}.md"));
    let expected_ast = read_fixture(&format!("{stem}.ast"));
    let expected_markdown =
        normalize_expected_markdown(&read_fixture(&format!("{stem}.canonical.md")));

    let output = options.parse(&input);
    assert_eq!(output.diagnostics, Vec::new());
    assert_eq!(
        snapshot_document(&output.document),
        trim_final_newline(&expected_ast)
    );

    let markdown = output
        .document
        .to_markdown_with(&SerializeOptions::default())
        .expect("document serializes");
    assert_eq!(markdown, expected_markdown);

    let reparsed = options.parse(&markdown);
    assert_eq!(
        snapshot_document(&reparsed.document),
        snapshot_document(&output.document)
    );

    let second = reparsed
        .document
        .to_markdown()
        .expect("reparsed document serializes");
    assert_eq!(second, markdown);
}

pub(crate) fn assert_parse_serialize_stable(path: &str, options: &SyntaxOptions) {
    let input = read_fixture(path);
    let output = options.parse(&input);
    assert!(
        output
            .diagnostics
            .iter()
            .all(|diagnostic| diagnostic.severity != DiagnosticSeverity::Error),
        "{path}: unexpected parse diagnostics: {:?}",
        output.diagnostics
    );

    let markdown = output.document.to_markdown().expect("document serializes");
    let reparsed = options.parse(&markdown);
    assert_eq!(
        snapshot_document(&reparsed.document),
        snapshot_document(&output.document),
        "{path}: AST changed after serialize/reparse"
    );

    let second = reparsed
        .document
        .to_markdown()
        .expect("reparsed document serializes");
    assert_eq!(second, markdown, "{path}: serializer is not idempotent");
}

pub(crate) fn assert_case_file_stable(path: &Path, options: &SyntaxOptions) -> usize {
    let metadata = read_derived_metadata(path);
    let cases = read_derived_cases(path);
    assert_eq!(
        cases.len(),
        metadata.count,
        "{}: header count does not match parsed cases",
        path.display()
    );

    for case in &cases {
        assert_source_stable(&case.input, path, case.index, options);
    }

    cases.len()
}

pub(crate) fn assert_semantic_input_corpus_stable(root: &Path) -> DerivedCorpusStats {
    let mut files = Vec::new();
    collect_files(root, "cases", &mut files);
    files.sort();

    let mut stats = DerivedCorpusStats::default();
    for file in files {
        let metadata = read_derived_metadata(&file);
        assert_eq!(
            metadata.role.as_deref(),
            Some("upstream-input"),
            "{}: executable corpus files must declare role: upstream-input",
            file.display()
        );
        let cases = read_derived_cases(&file);
        assert_eq!(
            cases.len(),
            metadata.count,
            "{}: header count does not match parsed cases",
            file.display()
        );

        match metadata.origin.as_str() {
            "commonmark" => stats.commonmark_cases += cases.len(),
            "gfm" => stats.gfm_cases += cases.len(),
            origin => panic!("{}: unexpected origin: {origin}", file.display()),
        }

        for case in cases {
            stats.profiles.insert(case.profile.clone());
            stats.total_cases += 1;
            let options = profile_options(&case.profile);
            if case.input.contains('\u{0}') {
                // code-lean: literal NUL fuzz cases cover parse totality only; upgrade when
                // NUL serializer/reparse stability becomes a contract.
                let _ = options.parse(&case.input);
            } else {
                assert_source_stable(&case.input, &file, case.index, &options);
            }
        }
    }

    assert_promoted_semantic_sources(root);
    assert_semantic_manifest_matches(root, &stats);
    stats
}

#[derive(Default)]
pub(crate) struct DerivedCorpusStats {
    pub(crate) total_cases: usize,
    pub(crate) commonmark_cases: usize,
    pub(crate) gfm_cases: usize,
    pub(crate) profiles: BTreeSet<String>,
}

pub(crate) fn assert_required_profiles(profiles: &BTreeSet<String>) {
    for profile in [
        "commonmark",
        "gfm",
        "mdx",
        "math",
        "frontmatter",
        "extras",
        "wikilink-after",
        "wikilink-before",
    ] {
        assert!(
            profiles.contains(profile),
            "semantic derived corpus is missing required profile: {profile}"
        );
    }
}

fn assert_promoted_semantic_sources(root: &Path) {
    for relative in [
        "commonmark/attention.cases",
        "commonmark/gfm_strikethrough.cases",
        "commonmark/link_reference.cases",
        "commonmark/list.cases",
        "commonmark/mdx_esm.cases",
    ] {
        let path = root.join(relative);
        assert!(
            path.exists(),
            "{}: promoted semantic input corpus is missing",
            path.display()
        );
    }
}

fn assert_semantic_manifest_matches(root: &Path, stats: &DerivedCorpusStats) {
    let manifest_path = root.join("MANIFEST.md");
    let manifest = fs::read_to_string(&manifest_path)
        .unwrap_or_else(|error| panic!("{}: {error}", manifest_path.display()));
    let total = manifest
        .lines()
        .find_map(|line| line.strip_prefix("Total executable input cases: "))
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or_else(|| {
            panic!(
                "{}: missing Total executable input cases line",
                manifest_path.display()
            )
        });
    assert_eq!(
        total,
        stats.total_cases,
        "{}: manifest total does not match parsed semantic corpus",
        manifest_path.display()
    );

    for profile in &stats.profiles {
        assert!(
            manifest.contains(&format!("`{profile}`")),
            "{}: manifest does not mention executable profile `{profile}`",
            manifest_path.display()
        );
    }
}

pub(crate) struct DerivedMetadata {
    pub(crate) origin: String,
    pub(crate) role: Option<String>,
    pub(crate) count: usize,
}

pub(crate) fn read_derived_metadata(path: &Path) -> DerivedMetadata {
    let source =
        fs::read_to_string(path).unwrap_or_else(|error| panic!("{}: {error}", path.display()));
    let mut origin = None;
    let mut source_seen = false;
    let mut role = None;
    let mut count = None;

    for line in source.lines() {
        if line.starts_with("--- case ") {
            break;
        }
        if let Some(value) = line.strip_prefix("origin: ") {
            origin = Some(value.to_string());
        } else if line.strip_prefix("source: ").is_some() {
            source_seen = true;
        } else if let Some(value) = line.strip_prefix("role: ") {
            role = Some(value.to_string());
        } else if let Some(value) = line.strip_prefix("count: ") {
            count = Some(value.parse::<usize>().unwrap_or_else(|error| {
                panic!(
                    "{}: invalid metadata count `{value}`: {error}",
                    path.display()
                )
            }));
        }
    }

    if !source_seen {
        panic!("{}: missing source metadata", path.display());
    }

    DerivedMetadata {
        origin: origin.unwrap_or_else(|| panic!("{}: missing origin metadata", path.display())),
        role,
        count: count.unwrap_or_else(|| panic!("{}: missing count metadata", path.display())),
    }
}

struct DerivedCase {
    index: usize,
    profile: String,
    input: String,
}

fn read_derived_cases(path: &Path) -> Vec<DerivedCase> {
    let source =
        fs::read_to_string(path).unwrap_or_else(|error| panic!("{}: {error}", path.display()));
    let mut cases = Vec::new();
    let mut cursor = 0;

    while let Some(relative_header_start) = source[cursor..].find("--- case ") {
        let header_start = cursor + relative_header_start;
        let header_end = source[header_start..]
            .find('\n')
            .map(|offset| header_start + offset)
            .unwrap_or(source.len());
        let header = &source[header_start..header_end];
        let (index, profile, byte_len) = parse_case_header(path, header);
        let body_start = header_end.saturating_add(1);
        let body_end = body_start + byte_len;
        assert!(
            source.is_char_boundary(body_start) && source.is_char_boundary(body_end),
            "{}#{index}: case body is not valid UTF-8 boundary",
            path.display()
        );
        assert!(
            body_end <= source.len(),
            "{}#{index}: case body exceeds file length",
            path.display()
        );

        let end_marker = "\n--- end\n";
        assert!(
            source[body_end..].starts_with(end_marker),
            "{}#{index}: missing case end marker",
            path.display()
        );

        cases.push(DerivedCase {
            index,
            profile,
            input: source[body_start..body_end].to_string(),
        });
        cursor = body_end + end_marker.len();
    }

    cases
}

fn parse_case_header(path: &Path, header: &str) -> (usize, String, usize) {
    let parts = header.split_whitespace().collect::<Vec<_>>();
    assert!(
        (parts.len() == 5 && parts[0] == "---" && parts[1] == "case" && parts[3] == "bytes")
            || (parts.len() == 7
                && parts[0] == "---"
                && parts[1] == "case"
                && parts[3] == "profile"
                && parts[5] == "bytes"),
        "{}: invalid case header: {header}",
        path.display()
    );
    let index = parts[2].parse::<usize>().unwrap_or_else(|error| {
        panic!(
            "{}: invalid case index in {header}: {error}",
            path.display()
        )
    });
    let (profile, byte_len_part) = if parts.len() == 7 {
        (parts[4].to_string(), parts[6])
    } else {
        ("commonmark".to_string(), parts[4])
    };
    let byte_len = byte_len_part.parse::<usize>().unwrap_or_else(|error| {
        panic!(
            "{}: invalid case byte length in {header}: {error}",
            path.display()
        )
    });
    (index, profile, byte_len)
}

fn assert_source_stable(source: &str, path: &Path, index: usize, options: &SyntaxOptions) {
    let output = options.parse(source);

    let markdown = output.document.to_markdown().unwrap_or_else(|error| {
        panic!("{}#{index}: serialize failed: {:?}", path.display(), error)
    });
    let reparsed = options.parse(&markdown);
    assert_eq!(
        snapshot_document(&reparsed.document),
        snapshot_document(&output.document),
        "{}#{index}: AST changed after serialize/reparse",
        path.display()
    );
}

fn collect_files(root: &Path, extension: &str, output: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(root).unwrap_or_else(|error| panic!("{}: {error}", root.display())) {
        let path = entry
            .unwrap_or_else(|error| panic!("{}: {error}", root.display()))
            .path();
        if path.is_dir() {
            collect_files(&path, extension, output);
        } else if path.extension().and_then(|value| value.to_str()) == Some(extension) {
            output.push(path);
        }
    }
}

fn read_fixture(path: &str) -> String {
    fs::read_to_string(Path::new(path)).unwrap_or_else(|error| panic!("{path}: {error}"))
}

fn trim_final_newline(input: &str) -> &str {
    input.trim_end_matches('\n')
}

fn normalize_expected_markdown(input: &str) -> String {
    let mut output = input.trim_end_matches('\n').to_string();
    output.push('\n');
    output
}

pub(crate) fn snapshot_document(document: &Document) -> String {
    let mut lines = vec!["Document".to_string()];
    for block in &document.children {
        snapshot_block(block, 1, &mut lines);
    }
    lines.join("\n")
}

fn snapshot_block(block: &Block, indent: usize, lines: &mut Vec<String>) {
    match block {
        Block::Paragraph(node) => {
            push(lines, indent, "Paragraph");
            snapshot_inlines(&node.children, indent + 1, lines);
        }
        Block::Heading(node) => {
            push(
                lines,
                indent,
                format!(
                    "Heading depth={} kind={}",
                    node.depth,
                    match node.kind {
                        crate::markdown_syntax::HeadingKind::Atx => "atx",
                        crate::markdown_syntax::HeadingKind::Setext => "setext",
                    }
                ),
            );
            snapshot_inlines(&node.children, indent + 1, lines);
        }
        Block::ThematicBreak(_) => push(lines, indent, "ThematicBreak"),
        Block::BlockQuote(node) => {
            push(lines, indent, "BlockQuote");
            for child in &node.children {
                snapshot_block(child, indent + 1, lines);
            }
        }
        Block::Alert(node) => {
            push(
                lines,
                indent,
                format!(
                    "Alert kind={} title={}",
                    match node.kind {
                        crate::markdown_syntax::AlertKind::Note => "note",
                        crate::markdown_syntax::AlertKind::Tip => "tip",
                        crate::markdown_syntax::AlertKind::Important => "important",
                        crate::markdown_syntax::AlertKind::Warning => "warning",
                        crate::markdown_syntax::AlertKind::Caution => "caution",
                    },
                    snapshot_title(&node.title)
                ),
            );
            for child in &node.children {
                snapshot_block(child, indent + 1, lines);
            }
        }
        Block::List(node) => {
            push(
                lines,
                indent,
                format!("List ordered={} tight={}", node.ordered, node.tight),
            );
            for item in &node.children {
                push(
                    lines,
                    indent + 1,
                    format!(
                        "ListItem checked={}",
                        item.checked
                            .map(|checked| checked.to_string())
                            .unwrap_or_else(|| "none".into())
                    ),
                );
                for child in &item.children {
                    snapshot_block(child, indent + 2, lines);
                }
            }
        }
        Block::DescriptionList(node) => {
            push(
                lines,
                indent,
                format!("DescriptionList tight={}", node.tight),
            );
            for item in &node.children {
                push(lines, indent + 1, "Item");
                push(lines, indent + 2, "Term");
                snapshot_inlines(&item.term, indent + 3, lines);
                for details in &item.details {
                    push(lines, indent + 2, "Details");
                    for child in &details.children {
                        snapshot_block(child, indent + 3, lines);
                    }
                }
            }
        }
        Block::CodeBlock(node) => {
            push(
                lines,
                indent,
                format!(
                    "CodeBlock kind={} info={}",
                    match node.kind {
                        crate::markdown_syntax::CodeBlockKind::Fenced { .. } => "fenced",
                        crate::markdown_syntax::CodeBlockKind::Indented => "indented",
                    },
                    node.info
                        .as_ref()
                        .map(|info| quote(info))
                        .unwrap_or_else(|| "none".into())
                ),
            );
            push(
                lines,
                indent + 1,
                format!("Value {}", quote_trimmed(&node.value)),
            );
        }
        Block::HtmlBlock(node) => push(
            lines,
            indent,
            format!("HtmlBlock {}", quote_trimmed(&node.value)),
        ),
        Block::HtmlContainer(node) => {
            push(
                lines,
                indent,
                format!(
                    "HtmlContainer tag={} open={} close={}",
                    node.opening.name,
                    quote(&node.opening.raw),
                    quote(&node.closing.raw)
                ),
            );
            match &node.content {
                crate::markdown_syntax::HtmlContainerContent::Blocks(children) => {
                    for child in children {
                        snapshot_block(child, indent + 1, lines);
                    }
                }
                crate::markdown_syntax::HtmlContainerContent::Inlines(children) => {
                    snapshot_inlines(children, indent + 1, lines);
                }
            }
        }
        Block::Definition(node) => push(
            lines,
            indent,
            format!(
                "Definition label={} destination={} title={}",
                node.label,
                node.destination,
                snapshot_title(&node.title)
            ),
        ),
        Block::FootnoteDefinition(node) => {
            push(
                lines,
                indent,
                format!("FootnoteDefinition label={}", node.label),
            );
            for child in &node.children {
                snapshot_block(child, indent + 1, lines);
            }
        }
        Block::Table(node) => {
            push(lines, indent, "Table");
            push(
                lines,
                indent + 1,
                format!(
                    "Alignments {}",
                    node.alignments
                        .iter()
                        .map(|alignment| match alignment {
                            crate::markdown_syntax::TableAlignment::None => "none",
                            crate::markdown_syntax::TableAlignment::Left => "left",
                            crate::markdown_syntax::TableAlignment::Center => "center",
                            crate::markdown_syntax::TableAlignment::Right => "right",
                        })
                        .collect::<Vec<_>>()
                        .join(",")
                ),
            );
            for row in &node.rows {
                push(lines, indent + 1, "Row");
                for cell in &row.cells {
                    push(lines, indent + 2, "Cell");
                    snapshot_inlines(&cell.children, indent + 3, lines);
                }
            }
        }
        Block::MathBlock(node) => push(
            lines,
            indent,
            format!("MathBlock {}", quote_trimmed(&node.value)),
        ),
        Block::Frontmatter(node) => push(
            lines,
            indent,
            format!("Frontmatter {}", quote_trimmed(&node.value)),
        ),
        Block::MdxEsm(node) => push(
            lines,
            indent,
            format!("MdxEsm {}", quote_trimmed(&node.value)),
        ),
        Block::MdxExpression(node) => push(
            lines,
            indent,
            format!("MdxExpression {}", quote_trimmed(&node.value)),
        ),
        Block::MdxJsx(node) => push(
            lines,
            indent,
            format!("MdxJsx {}", quote_trimmed(&node.value)),
        ),
        Block::LeafDirective(node) => {
            push(
                lines,
                indent,
                format!(
                    "LeafDirective name={} attrs={}",
                    node.name,
                    snapshot_attrs(&node.attributes)
                ),
            );
            if !node.label.is_empty() {
                push(lines, indent + 1, "Label");
                snapshot_inlines(&node.label, indent + 2, lines);
            }
        }
        Block::ContainerDirective(node) => {
            push(
                lines,
                indent,
                format!(
                    "ContainerDirective name={} attrs={}",
                    node.name,
                    snapshot_attrs(&node.attributes)
                ),
            );
            if !node.label.is_empty() {
                push(lines, indent + 1, "Label");
                snapshot_inlines(&node.label, indent + 2, lines);
            }
            for child in &node.children {
                snapshot_block(child, indent + 1, lines);
            }
        }
    }
}

fn snapshot_inlines(inlines: &[Inline], indent: usize, lines: &mut Vec<String>) {
    for inline in inlines {
        match inline {
            Inline::Text(node) => push(lines, indent, format!("Text {}", quote(&node.value))),
            Inline::Escape(node) => {
                push(lines, indent, format!("Escape {}", quote_char(node.value)))
            }
            Inline::CharacterReference(node) => push(
                lines,
                indent,
                format!(
                    "CharacterReference reference={} value={}",
                    quote(&node.reference),
                    quote(&node.value)
                ),
            ),
            Inline::Emphasis(node) => {
                push(lines, indent, "Emphasis");
                snapshot_inlines(&node.children, indent + 1, lines);
            }
            Inline::Strong(node) => {
                push(lines, indent, "Strong");
                snapshot_inlines(&node.children, indent + 1, lines);
            }
            Inline::Underline(node) => {
                push(lines, indent, "Underline");
                snapshot_inlines(&node.children, indent + 1, lines);
            }
            Inline::Delete(node) => {
                push(lines, indent, "Delete");
                snapshot_inlines(&node.children, indent + 1, lines);
            }
            Inline::Insert(node) => {
                push(lines, indent, "Insert");
                snapshot_inlines(&node.children, indent + 1, lines);
            }
            Inline::Mark(node) => {
                push(lines, indent, "Mark");
                snapshot_inlines(&node.children, indent + 1, lines);
            }
            Inline::Subscript(node) => {
                push(lines, indent, "Subscript");
                snapshot_inlines(&node.children, indent + 1, lines);
            }
            Inline::Superscript(node) => {
                push(lines, indent, "Superscript");
                snapshot_inlines(&node.children, indent + 1, lines);
            }
            Inline::Spoiler(node) => {
                push(lines, indent, "Spoiler");
                snapshot_inlines(&node.children, indent + 1, lines);
            }
            Inline::Shortcode(node) => {
                push(lines, indent, format!("Shortcode {}", quote(&node.name)));
            }
            Inline::Code(node) => push(
                lines,
                indent,
                format!(
                    "Code value={} raw={} fence={}",
                    quote(&node.value),
                    quote(&node.raw),
                    node.fence_length
                ),
            ),
            Inline::Link(node) => {
                push(
                    lines,
                    indent,
                    format!(
                        "Link destination={} title={}",
                        node.destination,
                        snapshot_title(&node.title)
                    ),
                );
                snapshot_inlines(&node.children, indent + 1, lines);
            }
            Inline::Image(node) => {
                push(
                    lines,
                    indent,
                    format!(
                        "Image destination={} title={}",
                        node.destination,
                        snapshot_title(&node.title)
                    ),
                );
                snapshot_inlines(&node.alt, indent + 1, lines);
            }
            Inline::LinkReference(node) => {
                push(
                    lines,
                    indent,
                    format!("LinkReference identifier={}", node.identifier),
                );
                snapshot_inlines(&node.children, indent + 1, lines);
            }
            Inline::ImageReference(node) => {
                push(
                    lines,
                    indent,
                    format!("ImageReference identifier={}", node.identifier),
                );
                snapshot_inlines(&node.alt, indent + 1, lines);
            }
            Inline::Autolink(node) => {
                let kind = match &node.kind {
                    AutolinkKind::Angle => String::from("angle"),
                    AutolinkKind::GfmLiteral { original } => {
                        format!("gfm-literal original={}", quote(original))
                    }
                };
                push(
                    lines,
                    indent,
                    format!("Autolink {} kind={kind}", quote(&node.destination)),
                );
            }
            Inline::Html(node) => push(lines, indent, format!("HtmlInline {}", quote(&node.value))),
            Inline::SoftBreak(_) => push(lines, indent, "SoftBreak"),
            Inline::LineBreak(node) => push(
                lines,
                indent,
                format!(
                    "LineBreak kind={}",
                    match node.kind {
                        crate::markdown_syntax::LineBreakKind::Backslash => "backslash",
                        crate::markdown_syntax::LineBreakKind::Spaces => "spaces",
                    }
                ),
            ),
            Inline::Math(node) => push(
                lines,
                indent,
                match node.kind {
                    crate::markdown_syntax::MathInlineKind::Dollar { dollars } => {
                        format!("Math {} dollars={}", quote(&node.value), dollars)
                    }
                    crate::markdown_syntax::MathInlineKind::Code => {
                        format!("Math {} code", quote(&node.value))
                    }
                },
            ),
            Inline::FootnoteReference(node) => {
                push(lines, indent, format!("FootnoteReference {}", node.label))
            }
            Inline::InlineFootnote(node) => {
                push(lines, indent, "InlineFootnote");
                snapshot_inlines(&node.children, indent + 1, lines);
            }
            Inline::WikiLink(node) => push(
                lines,
                indent,
                format!(
                    "WikiLink target={} label={} order={}",
                    quote(&node.target),
                    quote(&node.label),
                    match node.label_order {
                        crate::markdown_syntax::WikiLinkLabelOrder::AfterPipe => "after",
                        crate::markdown_syntax::WikiLinkLabelOrder::BeforePipe => "before",
                    }
                ),
            ),
            Inline::MdxExpression(node) => push(
                lines,
                indent,
                format!("MdxExpression {}", quote(&node.value)),
            ),
            Inline::MdxJsx(node) => push(lines, indent, format!("MdxJsx {}", quote(&node.value))),
            Inline::TextDirective(node) => {
                push(
                    lines,
                    indent,
                    format!(
                        "TextDirective name={} attrs={}",
                        node.name,
                        snapshot_attrs(&node.attributes)
                    ),
                );
                if !node.label.is_empty() {
                    push(lines, indent + 1, "Label");
                    snapshot_inlines(&node.label, indent + 2, lines);
                }
            }
        }
    }
}

fn snapshot_attrs(attributes: &[crate::markdown_syntax::DirectiveAttribute]) -> String {
    if attributes.is_empty() {
        return "none".into();
    }
    attributes
        .iter()
        .map(|attribute| match &attribute.value {
            Some(value) => format!("{}:{}", attribute.name, value),
            None => attribute.name.clone(),
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn snapshot_title(title: &Option<String>) -> String {
    title
        .as_ref()
        .map(|title| quote(title))
        .unwrap_or_else(|| "none".into())
}

fn push(lines: &mut Vec<String>, indent: usize, text: impl Into<String>) {
    lines.push(format!("{}{}", "  ".repeat(indent), text.into()));
}

fn quote(input: &str) -> String {
    format!(
        "\"{}\"",
        input
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('\n', "\\n")
    )
}

fn quote_char(input: char) -> String {
    let mut value = String::new();
    value.push(input);
    quote(&value)
}

fn quote_trimmed(input: &str) -> String {
    quote(input.trim_end_matches('\n'))
}
