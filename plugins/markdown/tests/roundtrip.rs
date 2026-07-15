mod support;

use plugin_md_v2::MarkdownPlugin;
use plugin_md_v2::exports::lix::plugin::api::Guest;
use support::{file, render, rows_of_kind, semantic_html, snapshot, state_from_source};

#[test]
fn promised_format_tier_round_trips_exactly() {
    let fixtures = [
        ("empty", ""),
        (
            "yaml-frontmatter",
            "---\ntitle: Tokyo Packing List\ntags:\n  - travel\n  - checklist\n---\n\n# Tokyo Packing List\n",
        ),
        (
            "toml-frontmatter",
            "+++\ntitle = \"Tokyo Packing List\"\n+++\n\n# Tokyo Packing List\n",
        ),
        (
            "crlf-yaml-frontmatter",
            "---\r\ntitle: Tokyo Packing List\r\n---\r\n\r\n# Tokyo Packing List\r\n",
        ),
        (
            "empty-yaml-frontmatter",
            "---\n\n---\n\n# Tokyo Packing List\n",
        ),
        (
            "yaml-frontmatter-with-block-scalar",
            "---\nsummary: |\n  first\n  ---\n  last\n---\n\n# Tokyo Packing List\n",
        ),
        ("no-final-newline", "paragraph"),
        ("final-newline", "paragraph\n"),
        ("crlf", "# A\r\n\r\nB\r\n"),
        ("crlf-no-final-newline", "# A\r\n\r\nB"),
        ("crlf-html", "<!-- comment\r\ncontinues -->\r\n"),
        ("crlf-fence", "~~~ rust\r\ncode\r\n~~~\r\n"),
        ("setext", "Heading\n=======\n"),
        ("setext-level-two", "Heading\n-------\n"),
        ("emphasis", "Text _em_ and __strong__.\n"),
        ("nested-emphasis", "**bold _italic_ bold**\n"),
        ("single-tilde-delete", "Text ~deleted~.\n"),
        ("double-tilde-delete", "Text ~~deleted~~.\n"),
        ("list-markers", "* one\n* two\n\n1) first\n2) second\n"),
        ("thematic-marker", "***\n"),
        ("fence", "~~~~\ncode\n~~~~\n"),
        ("hard-break", "line one\\\nline two\n"),
        ("hard-break-spaces", "line one  \nline two\n"),
        ("crlf-hard-break", "line one  \r\nline two\r\n"),
        ("character-reference", "A &#38; B\n"),
        ("named-character-reference", "A &amp; B\n"),
        ("inline-code", "Use `` a ` b `` here.\n"),
        (
            "resource-angle-destination",
            "[text](<https://example.com/a b>)\n",
        ),
        ("resource-single-title", "[text](/target 'title')\n"),
        ("resource-paren-title", "[text](/target (title))\n"),
        ("image-resource", "![alt](<image name.png> 'title')\n"),
        (
            "reference",
            "[text][Label]\n\n[Label]: <https://example.com> 'title'\n",
        ),
        (
            "crlf-reference",
            "[text][Label]\r\n\r\n[Label]: <https://example.com> 'title'\r\n",
        ),
        ("collapsed-reference", "[Label][]\n\n[Label]: /target\n"),
        ("shortcut-reference", "[Label]\n\n[Label]: /target\n"),
        ("autolink", "Visit www.example.com.\n"),
        ("angle-autolink", "<https://example.com/a?b=1>\n"),
        ("email-autolink", "Mail user.name+tag@example.com.\n"),
        ("inline-html", "before <kbd>Ctrl</kbd> after\n"),
    ];
    let mut failures = Vec::new();
    for (name, source) in fixtures {
        let output = render(state_from_source(source));
        if output != source {
            failures.push(format!(
                "{name}: exact mismatch\ninput={source:?}\noutput={output:?}"
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n\n"));
}

#[test]
fn yaml_frontmatter_is_structured_and_idempotent() {
    let source = concat!(
        "---\n",
        "title: Tokyo Packing List\n",
        "date: 2026-07-15\n",
        "tags:\n",
        "  - travel\n",
        "  - checklist\n",
        "---\n",
        "\n",
        "# Tokyo Packing List\n",
    );

    let state = state_from_source(source);
    let frontmatter = rows_of_kind(&state, "frontmatter");
    assert_eq!(frontmatter.len(), 1);
    assert_eq!(snapshot(frontmatter[0])["payload"]["kind"], "yaml");
    assert_eq!(
        snapshot(frontmatter[0])["payload"]["value"],
        "title: Tokyo Packing List\ndate: 2026-07-15\ntags:\n  - travel\n  - checklist"
    );
    assert_eq!(rows_of_kind(&state, "thematic_break").len(), 0);
    assert_eq!(render(state.clone()), source);
    assert!(
        MarkdownPlugin::detect_changes(state, file(source))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn frontmatter_before_a_heading_canonicalizes_to_one_blank_separator() {
    let source = "---\ntitle: Tokyo Packing List\n---\n# Tokyo Packing List\n";
    let canonical = "---\ntitle: Tokyo Packing List\n---\n\n# Tokyo Packing List\n";
    let state = state_from_source(source);

    assert_eq!(rows_of_kind(&state, "frontmatter").len(), 1);
    assert_eq!(rows_of_kind(&state, "heading").len(), 1);
    assert_eq!(render(state.clone()), canonical);
    assert!(
        MarkdownPlugin::detect_changes(state, file(canonical))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn a_leading_thematic_break_is_not_frontmatter() {
    let source = "---\n\nBody\n";
    let state = state_from_source(source);

    assert_eq!(rows_of_kind(&state, "frontmatter").len(), 0);
    assert_eq!(rows_of_kind(&state, "thematic_break").len(), 1);
    assert_eq!(render(state), "- - -\n\nBody\n");
}

#[test]
fn incomplete_frontmatter_opener_remains_ordinary_markdown() {
    let source = "---\ntitle: Tokyo Packing List\n";
    let canonical = "- - -\n\ntitle: Tokyo Packing List\n";
    let state = state_from_source(source);

    assert_eq!(rows_of_kind(&state, "frontmatter").len(), 0);
    assert_eq!(rows_of_kind(&state, "thematic_break").len(), 1);
    assert_eq!(render(state.clone()), canonical);
    assert_eq!(semantic_html(source), semantic_html(canonical));
    assert!(
        MarkdownPlugin::detect_changes(state, file(canonical))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn canonicalized_features_are_semantic_and_idempotent() {
    let fixtures = [
        "# Heading #\n",
        "# A\n\n\n\nB\n",
        "| A | B |\n| - | -: |\n| x | y |\n",
        "~~~js\ncode();\n~~~\n",
    ];
    for source in fixtures {
        let state = state_from_source(source);
        let output = render(state.clone());
        assert_eq!(semantic_html(source), semantic_html(&output), "{source:?}");
        assert!(
            MarkdownPlugin::detect_changes(state, file(&output))
                .unwrap()
                .is_empty(),
            "canonical output was not idempotent: {output:?}"
        );
    }
}

#[test]
fn adjacency_regressions_keep_gfm_semantics() {
    let fixtures = [
        "[a]: /u\n---\n",
        "[a]: /u\n    code\n",
        "[a]: /u\n<x>\n",
        "~~~\n\n",
    ];
    for source in fixtures {
        let output = render(state_from_source(source));
        assert_eq!(
            semantic_html(source),
            semantic_html(&output),
            "output: {output:?}"
        );
    }
}

#[test]
fn mixed_gfm_document_round_trips_semantically() {
    let source = concat!(
        "# Release\n\n",
        "> Quoted **context**.\n\n",
        "- [x] shipped\n",
        "- [ ] follow up\n\n",
        "| Name | Status |\n",
        "| :--- | ---: |\n",
        "| parser | ready |\n\n",
        "A [link](https://example.com \"title\") and ~~old text~~.\n\n",
        "```rust\nfn main() {}\n```\n\n",
        "[^1]: Footnote body.\n\n",
        "See note[^1].\n",
    );
    let state = state_from_source(source);
    let output = render(state.clone());
    assert_eq!(semantic_html(source), semantic_html(&output));
    assert!(
        MarkdownPlugin::detect_changes(state, file(&output))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn live_edit_states_are_semantic_and_idempotent() {
    let fixtures = [
        ("single-space", " "),
        ("single-newline", "\n"),
        ("crlf-only", "\r\n"),
        ("indent-only", "    "),
        ("heading-marker", "#"),
        ("heading-marker-space", "## "),
        ("list-marker", "-"),
        ("list-marker-space", "- "),
        ("ordered-list-marker", "1."),
        ("task-prefix", "- ["),
        ("blockquote-marker", ">"),
        ("blockquote-marker-space", "> "),
        ("open-bracket", "["),
        ("unfinished-link-label", "[label"),
        ("unfinished-link-destination", "[label]("),
        ("empty-link-destination", "[label]()"),
        ("open-code-span", "`code"),
        ("open-emphasis", "*text"),
        ("open-strong", "**text"),
        ("open-delete", "~~text"),
        ("open-fence", "```"),
        ("open-fence-info", "```rust"),
        ("open-angle", "<"),
        ("open-entity", "&amp"),
        ("open-footnote", "[^"),
        ("empty-footnote-definition", "[^note]:"),
        ("empty-link-definition", "[label]:"),
        ("unfinished-angle-definition", "[label]: <"),
        ("unfinished-title-definition", "[label]: /target \""),
        ("table-fragment", "| a |"),
        ("table-delimiter-fragment", "| a |\n| -"),
    ];
    assert_semantic_and_idempotent(fixtures);
}

#[test]
fn nested_inline_constructs_are_semantic_and_idempotent() {
    let fixtures = [
        ("nested-strong-emphasis", "**bold _italic_ bold**\n"),
        ("nested-emphasis-strong", "_italic **bold** italic_\n"),
        ("combined-asterisk", "***bold italic***\n"),
        ("combined-underscore", "___bold italic___\n"),
        ("delete-containing-strong", "~~gone **strongly**~~\n"),
        ("strong-containing-delete", "**keep ~~except this~~**\n"),
        (
            "link-containing-emphasis",
            "[an *important* link](/target)\n",
        ),
        (
            "image-alt-formatting",
            "![an *important* image](/asset.png)\n",
        ),
        ("code-containing-markers", "`**literal** and _literal_`\n"),
        ("escaped-markers", "\\*literal\\* and \\_literal\\_\n"),
        ("adjacent-asterisk-emphasis", "*one**two*\n"),
        ("adjacent-underscore-emphasis", "__three___four_\n"),
        ("literal-underscores-inside-strong", "__foo__bar__baz__\n"),
        ("adjacent-emphasis", "*one**two* __three___four_\n"),
        ("unicode-delimiters", "**你好 _café_ 👩‍💻**\n"),
    ];
    assert_semantic_and_idempotent(fixtures);
}

#[test]
fn links_references_and_autolinks_are_semantic_and_idempotent() {
    let fixtures = [
        ("bare-resource", "[label](https://example.com/a?b=1#c)\n"),
        ("angle-resource", "[label](<https://example.com/a b>)\n"),
        ("empty-resource", "[label]()\n"),
        ("double-title", "[label](/target \"double\")\n"),
        ("single-title", "[label](/target 'single')\n"),
        ("paren-title", "[label](/target (paren))\n"),
        (
            "full-reference",
            "[text][Mixed Label]\n\n[Mixed Label]: /target\n",
        ),
        (
            "collapsed-reference",
            "[Mixed Label][]\n\n[Mixed Label]: /target\n",
        ),
        (
            "shortcut-reference",
            "[Mixed Label]\n\n[Mixed Label]: /target\n",
        ),
        (
            "image-reference",
            "![alt][image]\n\n[image]: /asset.png 'title'\n",
        ),
        (
            "escaped-reference-label",
            "[a \\] b][id]\n\n[id]: /target\n",
        ),
        ("angle-uri-autolink", "<https://example.com/a?b=1&c=2>\n"),
        ("angle-email-autolink", "<user.name+tag@example.com>\n"),
        ("literal-www-autolink", "Visit www.example.com/path?q=1.\n"),
        (
            "literal-http-autolink",
            "Visit https://example.com/path_(x).\n",
        ),
        (
            "literal-email-autolink",
            "Mail user.name+tag@example.com.\n",
        ),
        ("bracketed-autolink", "[www.example.com]\n"),
        (
            "autolink-next-to-brackets",
            "[www.example.com] and [https://example.com]\n",
        ),
        (
            "autolink-next-to-entity-like-text",
            "www.google.com/search?q=commonmark&hl;\n",
        ),
        ("autolink-next-to-emphasis", "www.example.com*emphasis*\n"),
        ("autolink-next-to-less-than", "www.example.com<literal\n"),
    ];
    assert_semantic_and_idempotent(fixtures);
}

#[test]
fn block_edge_cases_are_semantic_and_idempotent() {
    let fixtures = [
        ("crlf-without-final-newline", "# A\r\n\r\nB"),
        ("crlf-hard-break", "one  \r\ntwo\r\n"),
        ("crlf-fenced-code", "~~~ rust\r\ncode\r\n~~~\r\n"),
        ("empty-fenced-code", "```\n```\n"),
        ("empty-fence-in-blockquote", "> ```\n> ```\n"),
        ("nonempty-fence-in-blockquote", "> ```\n> code\n> ```\n"),
        ("empty-fence-in-bullet", "- ```\n  ```\n"),
        ("empty-fence-in-ordered-item", "1. ```\n   ```\n"),
        ("empty-fence-in-nested-blockquote", "> > ```\n> > ```\n"),
        ("fence-containing-shorter-fence", "````\n```\n````\n"),
        (
            "empty-fence-interrupted-by-blockquote-exit",
            "> ```\nfoo\n```\n",
        ),
        ("unclosed-fence-with-content", "~~~js\ncode\n"),
        ("tilde-fence-info", "~~~~ rust key=value\ncode\n~~~~\n"),
        ("indented-blank-code", "    one\n    \n    two\n"),
        ("html-comment", "<!-- a > b -->\n"),
        ("html-declaration", "<!DOCTYPE html>\n"),
        ("html-processing-instruction", "<?target data?>\n"),
        ("html-cdata", "<![CDATA[a < b]]>\n"),
        (
            "html-container",
            "<details open>\n<summary>Title</summary>\n\nBody\n</details>\n",
        ),
        (
            "inline-html",
            "before <span data-x=\"1\">inside</span> after\n",
        ),
        (
            "nested-task-list",
            "- [x] parent\n  - [ ] child\n  - ordinary\n",
        ),
        ("loose-task-list", "- [x] one\n\n- [ ] two\n"),
        ("task-like-text", "- [X] upper\n- [~] not-a-task\n"),
        (
            "multiblock-footnote",
            "Text[^note].\n\n[^note]: first\n\n    second *paragraph*\n",
        ),
        (
            "footnote-with-list",
            "Text[^note].\n\n[^note]:\n    - one\n    - two\n",
        ),
        (
            "table-escaped-pipe",
            "| A | B |\n| - | - |\n| a \\| b | `x|y` |\n",
        ),
        (
            "table-inline-formatting",
            "| A | B |\n| - | - |\n| **x** | [y](/z) |\n",
        ),
    ];
    assert_semantic_and_idempotent(fixtures);
}

#[test]
fn empty_fence_sentinel_cannot_remove_authored_code_content() {
    let mut state = state_from_source("```\n```\n\n```\ncontent\n```\n");
    let code_rows = rows_of_kind(&state, "code_block");
    let empty_id = code_rows
        .iter()
        .find(|row| snapshot(row)["payload"]["value"] == "")
        .expect("fixture should contain an empty code block")
        .entity_pk[0]
        .clone();
    let nonempty_id = code_rows
        .iter()
        .find(|row| snapshot(row)["payload"]["value"] != "")
        .expect("fixture should contain a nonempty code block")
        .entity_pk[0]
        .clone();
    let authored_sentinel = format!("\u{e000}lix-empty-code:{empty_id}\u{e001}");
    let row = state
        .iter_mut()
        .find(|row| row.entity_pk.first() == Some(&nonempty_id))
        .unwrap();
    let mut value = snapshot(row);
    value["payload"]["value"] = format!("before\n{authored_sentinel}\nafter\n").into();
    row.snapshot_content = value.to_string();

    let output = render(state);
    assert!(output.contains(&authored_sentinel), "output={output:?}");
    assert!(output.contains("before\n"), "output={output:?}");
    assert!(output.contains("after\n"), "output={output:?}");
}

fn assert_semantic_and_idempotent<const N: usize>(fixtures: [(&str, &str); N]) {
    let mut failures = Vec::new();
    for (name, source) in fixtures {
        let state = state_from_source(source);
        let output = render(state.clone());
        let input_html = semantic_html(source);
        let output_html = semantic_html(&output);
        if input_html != output_html {
            failures.push(format!(
                "{name}: semantic mismatch\ninput={source:?}\noutput={output:?}\ninput_html={input_html:?}\noutput_html={output_html:?}"
            ));
            continue;
        }
        let delta = MarkdownPlugin::detect_changes(state, file(&output)).unwrap();
        if !delta.is_empty() {
            failures.push(format!(
                "{name}: output was not idempotent\ninput={source:?}\noutput={output:?}\ndelta={delta:#?}"
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n\n"));
}
