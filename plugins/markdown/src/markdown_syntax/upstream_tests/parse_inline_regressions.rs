//! Inline-parsing regression coverage: emphasis/strong delimiter resolution,
//! the inline delimiter stack (asterisk/underscore/tilde/underline), and the
//! Unicode-awareness fixes for flanking and reference-label folding.
//!
//! Each former regression file is preserved verbatim inside its own `mod` so
//! that helper functions and test names cannot collide across the merged
//! sources.

mod emphasis {
    //! Regression coverage for CommonMark `*`/`_` emphasis resolution.
    //!
    //! These cases exercise the delimiter-stack matcher in `parse_inlines`,
    //! particularly partial matches where an opener run is longer than the closer
    //! consumes (or vice versa): the leftover delimiters must stay outside the
    //! emphasis, and closers must bind to the nearest preceding compatible opener.

    use crate::markdown_syntax::{Block, Inline, SyntaxOptions};

    /// Parses `input` as CommonMark and returns the inlines of the first paragraph.
    fn paragraph_inlines(input: &str) -> Vec<Inline> {
        let output = SyntaxOptions::commonmark().parse(input);
        match output.document.children.into_iter().next() {
            Some(Block::Paragraph(paragraph)) => paragraph.children,
            other => panic!("expected a paragraph, got {other:?}"),
        }
    }

    fn text(node: &Inline) -> &str {
        match node {
            Inline::Text(value) => &value.value,
            other => panic!("expected text, got {other:?}"),
        }
    }

    fn emphasis(node: &Inline) -> &[Inline] {
        match node {
            Inline::Emphasis(value) => &value.children,
            other => panic!("expected emphasis, got {other:?}"),
        }
    }

    fn strong(node: &Inline) -> &[Inline] {
        match node {
            Inline::Strong(value) => &value.children,
            other => panic!("expected strong, got {other:?}"),
        }
    }

    /// `*foo*` -> emphasis "foo".
    #[test]
    fn single_star_is_emphasis() {
        let nodes = paragraph_inlines("*foo*");
        assert_eq!(nodes.len(), 1);
        assert_eq!(text(&emphasis(&nodes[0])[0]), "foo");
    }

    /// `**foo**` -> strong "foo".
    #[test]
    fn double_star_is_strong() {
        let nodes = paragraph_inlines("**foo**");
        assert_eq!(nodes.len(), 1);
        assert_eq!(text(&strong(&nodes[0])[0]), "foo");
    }

    /// `***foo***` -> emphasis wrapping strong "foo".
    #[test]
    fn triple_star_is_emphasis_around_strong() {
        let nodes = paragraph_inlines("***foo***");
        assert_eq!(nodes.len(), 1);
        let inner = emphasis(&nodes[0]);
        assert_eq!(inner.len(), 1);
        assert_eq!(text(&strong(&inner[0])[0]), "foo");
    }

    /// `**foo*` -> leftover `*` stays to the LEFT of the emphasis it could not strengthen.
    #[test]
    fn double_open_single_close_leaves_star_left() {
        let nodes = paragraph_inlines("**foo*");
        assert_eq!(nodes.len(), 2);
        assert_eq!(text(&nodes[0]), "*");
        assert_eq!(text(&emphasis(&nodes[1])[0]), "foo");
    }

    /// `*foo**` -> leftover `*` stays to the RIGHT of the emphasis.
    #[test]
    fn single_open_double_close_leaves_star_right() {
        let nodes = paragraph_inlines("*foo**");
        assert_eq!(nodes.len(), 2);
        assert_eq!(text(&emphasis(&nodes[0])[0]), "foo");
        assert_eq!(text(&nodes[1]), "*");
    }

    /// `***foo*` -> leftover `**` stays to the LEFT.
    #[test]
    fn triple_open_single_close_leaves_double_star_left() {
        let nodes = paragraph_inlines("***foo*");
        assert_eq!(nodes.len(), 2);
        assert_eq!(text(&nodes[0]), "**");
        assert_eq!(text(&emphasis(&nodes[1])[0]), "foo");
    }

    /// `*foo***` -> leftover `**` stays to the RIGHT.
    #[test]
    fn single_open_triple_close_leaves_double_star_right() {
        let nodes = paragraph_inlines("*foo***");
        assert_eq!(nodes.len(), 2);
        assert_eq!(text(&emphasis(&nodes[0])[0]), "foo");
        assert_eq!(text(&nodes[1]), "**");
    }

    /// `**foo*bar*` -> the unmatched `**` merges with `foo`; `*bar*` is emphasis.
    #[test]
    fn unmatched_double_open_merges_with_following_text() {
        let nodes = paragraph_inlines("**foo*bar*");
        assert_eq!(nodes.len(), 2);
        assert_eq!(text(&nodes[0]), "**foo");
        assert_eq!(text(&emphasis(&nodes[1])[0]), "bar");
    }

    /// `*foo**bar*` -> the inner `**` cannot strengthen, so it stays literal inside.
    #[test]
    fn interior_double_star_stays_literal_inside_emphasis() {
        let nodes = paragraph_inlines("*foo**bar*");
        assert_eq!(nodes.len(), 1);
        let inner = emphasis(&nodes[0]);
        assert_eq!(inner.len(), 1);
        assert_eq!(text(&inner[0]), "foo**bar");
    }

    /// `**bold*****bold+italic***` -> strong "bold" then emphasis-around-strong.
    #[test]
    fn adjacent_runs_split_into_strong_then_emphasis_strong() {
        let nodes = paragraph_inlines("**bold*****bold+italic***");
        assert_eq!(nodes.len(), 2);
        assert_eq!(text(&strong(&nodes[0])[0]), "bold");

        let outer = emphasis(&nodes[1]);
        assert_eq!(outer.len(), 1);
        assert_eq!(text(&strong(&outer[0])[0]), "bold+italic");
    }

    /// `****foo****` -> nested strong (strong "foo").
    #[test]
    fn quadruple_star_is_nested_strong() {
        let nodes = paragraph_inlines("****foo****");
        assert_eq!(nodes.len(), 1);
        let outer = strong(&nodes[0]);
        assert_eq!(outer.len(), 1);
        assert_eq!(text(&strong(&outer[0])[0]), "foo");
    }

    /// `*foo* **bar**` -> emphasis, space, strong.
    #[test]
    fn separate_runs_resolve_independently() {
        let nodes = paragraph_inlines("*foo* **bar**");
        assert_eq!(nodes.len(), 3);
        assert_eq!(text(&emphasis(&nodes[0])[0]), "foo");
        assert_eq!(text(&nodes[1]), " ");
        assert_eq!(text(&strong(&nodes[2])[0]), "bar");
    }

    /// `a*b*c` -> intraword `*` still opens/closes emphasis.
    #[test]
    fn intraword_star_emphasis_resolves() {
        let nodes = paragraph_inlines("a*b*c");
        assert_eq!(nodes.len(), 3);
        assert_eq!(text(&nodes[0]), "a");
        assert_eq!(text(&emphasis(&nodes[1])[0]), "b");
        assert_eq!(text(&nodes[2]), "c");
    }
}

mod inline_delimiter {
    use crate::markdown_syntax::{
        Block, Constructs, DeleteMarker, Inline, ParseOptions, SyntaxOptions,
    };

    #[test]
    fn asterisk_mixed_runs_nest_emphasis_and_strong() {
        let inlines = paragraph("**foo *bar***\n", &SyntaxOptions::commonmark());
        let [Inline::Strong(strong)] = inlines.as_slice() else {
            panic!("expected outer strong");
        };
        let [Inline::Text(prefix), Inline::Emphasis(emphasis)] = strong.children.as_slice() else {
            panic!("expected text followed by inner emphasis");
        };
        assert_eq!(prefix.value, "foo ");
        assert_text(emphasis.children.as_slice(), "bar");

        let inlines = paragraph("*foo **bar***\n", &SyntaxOptions::commonmark());
        let [Inline::Emphasis(emphasis)] = inlines.as_slice() else {
            panic!("expected outer emphasis");
        };
        let [Inline::Text(prefix), Inline::Strong(strong)] = emphasis.children.as_slice() else {
            panic!("expected text followed by inner strong");
        };
        assert_eq!(prefix.value, "foo ");
        assert_text(strong.children.as_slice(), "bar");
    }

    #[test]
    fn underscore_triple_and_mixed_runs_nest_emphasis_and_strong() {
        let inlines = paragraph("___foo___\n", &SyntaxOptions::commonmark());
        let [Inline::Emphasis(emphasis)] = inlines.as_slice() else {
            panic!("expected outer emphasis");
        };
        let [Inline::Strong(strong)] = emphasis.children.as_slice() else {
            panic!("expected inner strong");
        };
        assert_text(strong.children.as_slice(), "foo");

        let inlines = paragraph("__foo _bar___\n", &SyntaxOptions::commonmark());
        let [Inline::Strong(strong)] = inlines.as_slice() else {
            panic!("expected outer strong");
        };
        let [Inline::Text(prefix), Inline::Emphasis(emphasis)] = strong.children.as_slice() else {
            panic!("expected text followed by inner emphasis");
        };
        assert_eq!(prefix.value, "foo ");
        assert_text(emphasis.children.as_slice(), "bar");
    }

    #[test]
    fn intraword_underscore_stays_text() {
        let inlines = paragraph("foo_bar_baz\n", &SyntaxOptions::commonmark());
        assert_text(inlines.as_slice(), "foo_bar_baz");
    }

    #[test]
    fn strikethrough_coexists_with_attention_when_gfm_is_enabled() {
        let inlines = paragraph("~~two *emphasis* two~~\n", &SyntaxOptions::gfm());
        let [Inline::Delete(delete)] = inlines.as_slice() else {
            panic!("expected delete");
        };
        assert_eq!(delete.marker, DeleteMarker::DoubleTilde);
        let [
            Inline::Text(prefix),
            Inline::Emphasis(emphasis),
            Inline::Text(suffix),
        ] = delete.children.as_slice()
        else {
            panic!("expected delete containing emphasis");
        };
        assert_eq!(prefix.value, "two ");
        assert_text(emphasis.children.as_slice(), "emphasis");
        assert_eq!(suffix.value, " two");

        let inlines = paragraph("***~~xxx~~***\n", &SyntaxOptions::gfm());
        let [Inline::Emphasis(emphasis)] = inlines.as_slice() else {
            panic!("expected outer emphasis");
        };
        let [Inline::Strong(strong)] = emphasis.children.as_slice() else {
            panic!("expected inner strong");
        };
        let [Inline::Delete(delete)] = strong.children.as_slice() else {
            panic!("expected delete inside strong");
        };
        assert_eq!(delete.marker, DeleteMarker::DoubleTilde);
        assert_text(delete.children.as_slice(), "xxx");
    }

    #[test]
    fn single_tilde_strikethrough_respects_parse_option_and_subscript_shape() {
        let inlines = paragraph("a ~one~ b and ~~two~~ c\n", &SyntaxOptions::gfm());
        let [
            Inline::Text(prefix),
            Inline::Delete(one),
            Inline::Text(middle),
            Inline::Delete(two),
            Inline::Text(suffix),
        ] = inlines.as_slice()
        else {
            panic!("expected single and double tilde delete nodes");
        };
        assert_eq!(prefix.value, "a ");
        assert_eq!(one.marker, DeleteMarker::SingleTilde);
        assert_text(one.children.as_slice(), "one");
        assert_eq!(middle.value, " b and ");
        assert_eq!(two.marker, DeleteMarker::DoubleTilde);
        assert_text(two.children.as_slice(), "two");
        assert_eq!(suffix.value, " c");

        let mut constructs = Constructs::gfm();
        let disabled = SyntaxOptions {
            constructs: constructs.clone(),
            parse: ParseOptions {
                single_tilde_strikethrough: false,
                ..ParseOptions::default()
            },
        };
        let inlines = paragraph("a ~one~ b\n", &disabled);
        assert_text(inlines.as_slice(), "a ~one~ b");

        constructs.subscript = true;
        let with_subscript = SyntaxOptions {
            constructs: constructs,
            parse: ParseOptions {
                single_tilde_strikethrough: true,
                ..ParseOptions::default()
            },
        };
        let inlines = paragraph("H~2~O and ~gone~\n", &with_subscript);
        assert!(matches!(
            &inlines[..],
            [
                Inline::Text(prefix),
                Inline::Subscript(_),
                Inline::Text(middle),
                Inline::Delete(delete)
            ] if prefix.value == "H" && middle.value == "O and " && delete.marker == DeleteMarker::SingleTilde
        ));
    }

    #[test]
    fn underline_extension_keeps_double_underscore_precedence() {
        let mut constructs = Constructs::commonmark();
        constructs.underline = true;
        let options = SyntaxOptions {
            constructs: constructs,
            parse: ParseOptions::default(),
        };

        let inlines = paragraph("___foo___\n", &options);
        let [Inline::Emphasis(emphasis)] = inlines.as_slice() else {
            panic!("expected outer emphasis");
        };
        let [Inline::Underline(underline)] = emphasis.children.as_slice() else {
            panic!("expected underline to keep extension precedence");
        };
        assert_text(underline.children.as_slice(), "foo");
    }

    fn paragraph(source: &str, options: &SyntaxOptions) -> Vec<Inline> {
        let output = options.parse(source);
        assert!(
            output.diagnostics.is_empty(),
            "expected no parse diagnostics: {:?}",
            output.diagnostics
        );
        let [Block::Paragraph(paragraph)] = output.document.children.as_slice() else {
            panic!("expected a single paragraph");
        };
        paragraph.children.clone()
    }

    fn assert_text(inlines: &[Inline], expected: &str) {
        let [Inline::Text(text)] = inlines else {
            panic!("expected a single text inline");
        };
        assert_eq!(text.value, expected);
    }
}

mod review_inline {
    use crate::markdown_syntax::{
        Block, Inline, LineBreakKind, LinkDestinationKind, SyntaxOptions,
    };

    fn parse_blocks(input: &str, gfm: bool) -> Vec<Block> {
        let options = if gfm {
            SyntaxOptions::gfm()
        } else {
            SyntaxOptions::commonmark()
        };
        options.parse(input).document.children
    }

    fn only_paragraph(input: &str, gfm: bool) -> Vec<Inline> {
        let blocks = parse_blocks(input, gfm);
        let [Block::Paragraph(paragraph)] = blocks.as_slice() else {
            panic!("expected a single paragraph, got {blocks:?}");
        };
        paragraph.children.clone()
    }

    #[test]
    fn h1_bang_declaration_is_an_html_block() {
        let blocks = parse_blocks("<!a>\nbar\n", false);
        let Some(Block::HtmlBlock(html)) = blocks.first() else {
            panic!("expected an HTML declaration block, got {blocks:?}");
        };
        assert_eq!(html.value, "<!a>");
        assert!(matches!(blocks.get(1), Some(Block::Paragraph(_))));
    }

    #[test]
    fn h1_bang_declaration_is_inline_html() {
        let inlines = only_paragraph("a <!b\nc>\n", false);
        assert!(matches!(
            inlines.as_slice(),
            [Inline::Text(text), Inline::Html(html)]
                if text.value == "a " && html.value == "<!b\nc>"
        ));
    }

    #[test]
    fn i2_tab_after_trailing_spaces_is_a_soft_break() {
        let inlines = only_paragraph("aaa  \t\nbb\n", false);
        assert!(matches!(
            inlines.as_slice(),
            [Inline::Text(a), Inline::SoftBreak(_), Inline::Text(b)]
                if a.value == "aaa" && b.value == "bb"
        ));
    }

    #[test]
    fn i2_pure_double_space_remains_a_hard_break() {
        let inlines = only_paragraph("aaa  \nbb\n", false);
        assert!(matches!(
            inlines.as_slice(),
            [Inline::Text(a), Inline::LineBreak(br), Inline::Text(b)]
                if a.value == "aaa" && b.value == "bb" && br.kind == LineBreakKind::Spaces
        ));
    }

    #[test]
    fn l1_bracketed_reference_label_is_literal() {
        let blocks = parse_blocks("[ref[bar]]: /uri\n\n[foo][ref[bar]]\n", false);
        assert_eq!(blocks.len(), 2);
        assert!(
            blocks
                .iter()
                .all(|block| matches!(block, Block::Paragraph(_))),
            "expected two literal paragraphs, got {blocks:?}"
        );
    }

    #[test]
    fn l1_inline_link_text_still_nests_brackets() {
        let inlines = only_paragraph("[a[b]](u)\n", false);
        assert!(matches!(
            inlines.as_slice(),
            [Inline::Link(link)] if link.destination == "u"
        ));
    }

    #[test]
    fn l5_blank_definition_label_is_literal() {
        let blocks = parse_blocks("[ ]: /uri\n", false);
        assert!(matches!(blocks.as_slice(), [Block::Paragraph(_)]));
    }

    #[test]
    fn l4_unicode_space_is_part_of_bare_destination() {
        let inlines = only_paragraph("[a](/url\u{00A0}\"title\")\n", false);
        let [Inline::Link(link)] = inlines.as_slice() else {
            panic!("expected a single link, got {inlines:?}");
        };
        assert_eq!(link.destination, "/url\u{00A0}\"title\"");
        assert_eq!(link.destination_kind, LinkDestinationKind::Bare);
        assert!(link.title.is_none());
    }

    #[test]
    fn l2_dotless_email_autolink_is_valid() {
        let inlines = only_paragraph(
            "<asd@012345678901234567890123456789012345678901234567890123456789012>\n",
            false,
        );
        assert!(matches!(
            inlines.as_slice(),
            [Inline::Autolink(autolink)]
                if autolink.destination
                    == "asd@012345678901234567890123456789012345678901234567890123456789012"
        ));
    }

    #[test]
    fn g1_delimiter_cells_reject_interior_colons_and_spaces() {
        for source in ["|a|\n|-:-|\n", "|a|\n|- -|\n", "|a|\n|-::|\n"] {
            let blocks = parse_blocks(source, true);
            assert!(
                blocks
                    .iter()
                    .all(|block| matches!(block, Block::Paragraph(_))),
                "{source:?} should not form a table, got {blocks:?}"
            );
        }
    }

    #[test]
    fn g1_valid_delimiter_cells_still_form_a_table() {
        for source in [
            "|a|\n|:-:|\n",
            "|a|\n|---|\n",
            "|a|\n|:--|\n",
            "|a|\n|--:|\n",
        ] {
            let blocks = parse_blocks(source, true);
            assert!(
                matches!(blocks.as_slice(), [Block::Table(_)]),
                "{source:?} should still form a table, got {blocks:?}"
            );
        }
    }

    #[test]
    fn g2_www_autolink_rejects_underscore_in_last_two_segments() {
        let inlines = only_paragraph("www.aaa.bbb.ccc_ccc\n", true);
        assert!(matches!(
            inlines.as_slice(),
            [Inline::Text(text)] if text.value == "www.aaa.bbb.ccc_ccc"
        ));
    }

    #[test]
    fn g2_www_autolink_allows_underscore_before_last_two_segments() {
        let inlines = only_paragraph("www.aaa.bbb_bbb.ccc.ddd\n", true);
        assert!(matches!(
            inlines.as_slice(),
            [Inline::Autolink(autolink)] if autolink.destination == "http://www.aaa.bbb_bbb.ccc.ddd"
        ));
    }

    #[test]
    fn g3_literal_email_allows_underscore_in_domain() {
        let inlines = only_paragraph("a@a_b.c\n", true);
        assert!(matches!(
            inlines.as_slice(),
            [Inline::Autolink(autolink)] if autolink.destination == "mailto:a@a_b.c"
        ));
    }

    #[test]
    fn g3_literal_email_rejects_trailing_underscore_period() {
        let inlines = only_paragraph("aaa@a.b_.\n", true);
        assert!(matches!(
            inlines.as_slice(),
            [Inline::Text(text)] if text.value == "aaa@a.b_."
        ));
    }

    #[test]
    fn hg2_literal_link_excludes_trailing_entity_run() {
        let inlines = only_paragraph("www.example.com&xxx;.\n", true);
        let [Inline::Autolink(autolink), Inline::Text(rest)] = inlines.as_slice() else {
            panic!("expected an autolink followed by literal text, got {inlines:?}");
        };
        assert_eq!(autolink.destination, "http://www.example.com");
        assert_eq!(rest.value, "&xxx;.");
    }

    #[test]
    fn hg2_literal_link_keeps_entity_run_without_semicolon() {
        let inlines = only_paragraph("www.example.com&xxx\n", true);
        assert!(matches!(
            inlines.as_slice(),
            [Inline::Autolink(autolink)] if autolink.destination == "http://www.example.com&xxx"
        ));
    }

    #[test]
    fn g4_table_body_stops_at_a_block_start() {
        let blocks = parse_blocks("| a |\n| - |\n> b | c\n", true);
        assert!(
            matches!(blocks.first(), Some(Block::Table(_))),
            "expected a table first, got {blocks:?}"
        );
        assert!(
            matches!(blocks.get(1), Some(Block::BlockQuote(_))),
            "expected the blockquote line to start its own block, got {blocks:?}"
        );
    }
}

mod review_unicode {
    //! Regression coverage for the Unicode-awareness fixes (matching the
    //! CommonMark reference): emphasis flanking treats the Unicode `P*`/`S*`
    //! categories as punctuation (not only ASCII), and reference-label matching
    //! uses a Unicode case fold (not ASCII lowercasing).

    use crate::markdown_syntax::{Block, Inline, SyntaxOptions};

    fn paragraph_inlines(input: &str) -> Vec<Inline> {
        let output = SyntaxOptions::commonmark().parse(input);
        match output.document.children.into_iter().next() {
            Some(Block::Paragraph(paragraph)) => paragraph.children,
            other => panic!("expected a paragraph, got {other:?}"),
        }
    }

    /// `foo*…*bar` — the `*` is preceded by a letter and followed by U+2026
    /// (Unicode punctuation), so it is NOT left-flanking and cannot open emphasis.
    /// With ASCII-only punctuation classification this wrongly produced
    /// `foo<em>…</em>bar`.
    #[test]
    fn emphasis_does_not_open_before_unicode_punctuation() {
        let nodes = paragraph_inlines("foo*\u{2026}*bar\n");
        assert_eq!(
            nodes.len(),
            1,
            "expected a single literal text node: {nodes:?}"
        );
        match &nodes[0] {
            Inline::Text(text) => assert_eq!(text.value, "foo*\u{2026}*bar"),
            other => panic!("expected literal text, got {other:?}"),
        }
    }

    /// `a*。*b` — CJK full stop U+3002 is Unicode punctuation; same rule, no emphasis.
    #[test]
    fn emphasis_does_not_open_before_cjk_punctuation() {
        let nodes = paragraph_inlines("a*\u{3002}*b\n");
        assert!(
            !nodes.iter().any(|node| matches!(node, Inline::Emphasis(_))),
            "no emphasis should form around CJK punctuation: {nodes:?}"
        );
    }

    /// Control: a `*` between ASCII letters still opens/closes emphasis — the
    /// Unicode-punctuation change must not over-restrict intraword emphasis.
    #[test]
    fn emphasis_still_opens_between_ascii_letters() {
        let nodes = paragraph_inlines("foo*x*bar\n");
        assert!(
            nodes.iter().any(|node| matches!(node, Inline::Emphasis(_))),
            "intraword emphasis must still form: {nodes:?}"
        );
    }

    /// Reference labels match case-insensitively via a Unicode case fold: `[Ä]`
    /// resolves against `[ä]: /url`. ASCII lowercasing would leave `Ä` unchanged and
    /// fail to match.
    #[test]
    fn reference_label_matches_with_unicode_case_fold() {
        let nodes = paragraph_inlines("[\u{00C4}]\n\n[\u{00E4}]: /url\n");
        assert_eq!(nodes.len(), 1);
        match &nodes[0] {
            Inline::LinkReference(reference) => assert_eq!(reference.identifier, "\u{00E4}"),
            other => panic!("expected a resolved link reference, got {other:?}"),
        }
    }

    /// ASCII reference-label folding is unchanged by the switch to Unicode folding.
    #[test]
    fn ascii_reference_label_folding_is_unchanged() {
        let nodes = paragraph_inlines("[Ref]\n\n[ref]: /url\n");
        assert_eq!(nodes.len(), 1);
        match &nodes[0] {
            Inline::LinkReference(reference) => assert_eq!(reference.identifier, "ref"),
            other => panic!("expected a resolved link reference, got {other:?}"),
        }
    }
}
