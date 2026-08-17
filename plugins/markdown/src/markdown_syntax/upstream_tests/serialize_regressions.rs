//! Serializer round-trip regression coverage: math/directive/table fences,
//! delete-marker handling, escape rules for labels and pipes, and the
//! serializer defects from the review pass.
//!
//! Each former regression file is preserved verbatim inside its own `mod` so
//! that helper functions and test names cannot collide across the merged
//! sources.

mod serializer {
    use crate::markdown_syntax::*;

    fn text(value: &str) -> Inline {
        Inline::Text(Text {
            meta: NodeMeta::default(),
            value: value.into(),
        })
    }

    fn paragraph(children: Vec<Inline>) -> Block {
        Block::Paragraph(Paragraph {
            meta: NodeMeta::default(),
            children,
        })
    }

    fn math_options() -> SyntaxOptions {
        let mut constructs = Constructs::commonmark();
        constructs.math_block = true;
        constructs.math_inline = true;
        SyntaxOptions {
            constructs: constructs,
            parse: ParseOptions::default(),
        }
    }

    fn directive_options() -> SyntaxOptions {
        let mut constructs = Constructs::commonmark();
        constructs.directive_container = true;
        SyntaxOptions {
            constructs: constructs,
            parse: ParseOptions::default(),
        }
    }

    fn underline_options() -> SyntaxOptions {
        let mut constructs = Constructs::commonmark();
        constructs.underline = true;
        SyntaxOptions {
            constructs: constructs,
            parse: ParseOptions::default(),
        }
    }

    fn parse_document(markdown: &str, options: &SyntaxOptions) -> Document {
        let output = options.parse(markdown);
        assert_eq!(output.diagnostics, Vec::new());
        output.document
    }

    fn assert_single_tilde_delete_with_internal_runs_shape(document: &Document) {
        assert!(
            matches!(
            &document.children[..],
            [Block::Paragraph(Paragraph {
                children,
                ..
            })] if matches!(
                &children[..],
                [
                    Inline::Text(Text { value: before, .. }),
                    Inline::Delete(Delete {
                        children: delete_children,
                        marker: DeleteMarker::SingleTilde,
                        ..
                    }),
                    Inline::Text(Text { value: after, .. }),
                ] if before == "This "
                    && matches!(&delete_children[..], [Inline::Text(Text { value, .. })] if value == "text~~~~ is ~~~~curious")
                    && after == "."
            )
            ),
            "unexpected document shape: {document:#?}"
        );
    }

    #[test]
    fn list_markers_preserve_by_default_avoid_merging_and_can_be_overridden() {
        let input = "- a\n\n+ b\n\n* c\n";
        let document = parse_document(input, &SyntaxOptions::commonmark());

        let markdown = document.to_markdown().expect("document serializes");
        assert_eq!(markdown, input);

        let reparsed = parse_document(&markdown, &SyntaxOptions::commonmark());
        assert_eq!(reparsed.children.len(), 3);
        assert!(
            reparsed
                .children
                .iter()
                .all(|block| matches!(block, Block::List(_)))
        );

        let mut options = SerializeOptions::default();
        options.bullet = ListDelimiter::Plus;
        let overridden = document
            .to_markdown_with(&options)
            .expect("document serializes with options");
        assert_eq!(overridden, "+ a\n\n+ b\n\n+ c\n");
    }

    #[test]
    fn math_serialization_uses_parseable_fences() {
        let document = Document {
            meta: NodeMeta::default(),
            children: vec![
                Block::MathBlock(MathBlock {
                    meta: NodeMeta::default(),
                    value: "$$\n$$$\na $$ b".into(),
                }),
                paragraph(vec![Inline::Math(MathInline {
                    meta: NodeMeta::default(),
                    value: "a $$ b".into(),
                    kind: MathInlineKind::Code,
                })]),
            ],
        };

        let markdown = document.to_markdown().expect("document serializes");
        assert!(markdown.contains("$$$$\n$$\n$$$\na $$ b\n$$$$"));
        assert!(markdown.contains("$`a $$ b`$"));

        let reparsed = parse_document(&markdown, &math_options());
        match &reparsed.children[..] {
            [Block::MathBlock(block), Block::Paragraph(paragraph)] => {
                assert_eq!(block.value, "$$\n$$$\na $$ b\n");
                assert!(matches!(
                    &paragraph.children[..],
                    [Inline::Math(MathInline { value, .. })] if value == "a $$ b"
                ));
            }
            other => panic!("unexpected document shape: {other:?}"),
        }
    }

    #[test]
    fn inline_math_that_code_math_cannot_represent_fails() {
        let document = Document {
            meta: NodeMeta::default(),
            children: vec![paragraph(vec![Inline::Math(MathInline {
                meta: NodeMeta::default(),
                value: "a $$ b `$ c".into(),
                kind: MathInlineKind::Code,
            })])],
        };

        assert!(matches!(
            document.to_markdown(),
            Err(SerializeError::UnsupportedNode(message))
                if message.contains("inline math")
        ));
    }

    #[test]
    fn container_directive_fence_exceeds_serialized_code_colons() {
        let document = Document {
            meta: NodeMeta::default(),
            children: vec![Block::ContainerDirective(ContainerDirective {
                meta: NodeMeta::default(),
                name: "note".into(),
                label: Vec::new(),
                attributes: Vec::new(),
                children: vec![Block::CodeBlock(CodeBlock {
                    meta: NodeMeta::default(),
                    kind: CodeBlockKind::Fenced {
                        marker: FenceMarker::Backtick,
                        length: 3,
                    },
                    info: None,
                    value: "before\n:::\n::::\nafter".into(),
                })],
            })],
        };

        let markdown = document.to_markdown().expect("document serializes");
        assert!(markdown.starts_with(":::::note\n"));

        let reparsed = parse_document(&markdown, &directive_options());
        match &reparsed.children[..] {
            [Block::ContainerDirective(container)] => match &container.children[..] {
                [Block::CodeBlock(code)] => {
                    assert_eq!(code.value, "before\n:::\n::::\nafter\n");
                }
                other => panic!("unexpected directive children: {other:?}"),
            },
            other => panic!("unexpected document shape: {other:?}"),
        }
    }

    #[test]
    fn table_cells_escape_resource_pipes() {
        let document = Document {
            meta: NodeMeta::default(),
            children: vec![Block::Table(Table {
                meta: NodeMeta::default(),
                alignments: vec![TableAlignment::None, TableAlignment::None],
                rows: vec![
                    TableRow {
                        meta: NodeMeta::default(),
                        cells: vec![
                            TableCell {
                                meta: NodeMeta::default(),
                                children: vec![text("Link")],
                            },
                            TableCell {
                                meta: NodeMeta::default(),
                                children: vec![text("Image")],
                            },
                        ],
                    },
                    TableRow {
                        meta: NodeMeta::default(),
                        cells: vec![
                            TableCell {
                                meta: NodeMeta::default(),
                                children: vec![Inline::Link(Link {
                                    meta: NodeMeta::default(),
                                    destination: "b|c".into(),
                                    destination_kind: LinkDestinationKind::Bare,
                                    title: Some("t|u".into()),
                                    title_kind: Some(LinkTitleKind::DoubleQuote),
                                    children: vec![text("a")],
                                })],
                            },
                            TableCell {
                                meta: NodeMeta::default(),
                                children: vec![Inline::Image(Image {
                                    meta: NodeMeta::default(),
                                    destination: "y|z".into(),
                                    destination_kind: LinkDestinationKind::Bare,
                                    title: Some("i|j".into()),
                                    title_kind: Some(LinkTitleKind::DoubleQuote),
                                    alt: vec![text("x")],
                                })],
                            },
                        ],
                    },
                ],
            })],
        };

        let markdown = document.to_markdown().expect("document serializes");
        assert!(markdown.contains(r#"b\|c "t\|u""#));
        assert!(markdown.contains(r#"y\|z "i\|j""#));

        let reparsed = parse_document(&markdown, &SyntaxOptions::gfm());
        match &reparsed.children[..] {
            [Block::Table(table)] => {
                assert_eq!(table.rows[1].cells.len(), 2);
                assert!(matches!(
                    &table.rows[1].cells[0].children[..],
                    [Inline::Link(Link {
                        destination,
                        title: Some(title),
                        ..
                    })] if destination == "b|c" && title == "t|u"
                ));
                assert!(matches!(
                    &table.rows[1].cells[1].children[..],
                    [Inline::Image(Image {
                        destination,
                        title: Some(title),
                        ..
                    })] if destination == "y|z" && title == "i|j"
                ));
            }
            other => panic!("unexpected document shape: {other:?}"),
        }
    }

    #[test]
    fn strong_uses_star_delimiters_when_underline_enabled() {
        let document = Document {
            meta: NodeMeta::default(),
            children: vec![paragraph(vec![Inline::Strong(Strong {
                meta: NodeMeta::default(),
                children: vec![Inline::Emphasis(Emphasis {
                    meta: NodeMeta::default(),
                    children: vec![text("em")],
                })],
            })])],
        };

        let markdown = document.to_markdown().expect("document serializes");
        assert_eq!(markdown, "**_em_**\n");

        let reparsed = parse_document(&markdown, &underline_options());
        assert!(matches!(
            &reparsed.children[..],
            [Block::Paragraph(Paragraph {
                children,
                ..
            })] if matches!(
                &children[..],
                [Inline::Strong(Strong {
                    children: strong_children,
                    ..
                })] if matches!(&strong_children[..], [Inline::Emphasis(_)])
            )
        ));
    }

    #[test]
    fn single_tilde_origin_delete_adjacent_to_tilde_runs_roundtrips() {
        let input = "This ~text~~~~ is ~~~~curious~.\n";
        let document = parse_document(input, &SyntaxOptions::gfm());
        assert_single_tilde_delete_with_internal_runs_shape(&document);

        let markdown = document.to_markdown().expect("document serializes");
        assert_eq!(
            markdown,
            "This ~text\\~\\~\\~\\~ is \\~\\~\\~\\~curious~.\n"
        );

        let reparsed = parse_document(&markdown, &SyntaxOptions::gfm());
        assert_single_tilde_delete_with_internal_runs_shape(&reparsed);
    }

    #[test]
    fn text_double_tilde_run_with_single_tilde_close_stays_text() {
        let input = "a ~~two/one~ b\n";
        let document = parse_document(input, &SyntaxOptions::gfm());
        assert!(matches!(
            &document.children[..],
            [Block::Paragraph(Paragraph {
                children,
                ..
            })] if matches!(&children[..], [Inline::Text(Text { value, .. })] if value == "a ~~two/one~ b")
        ));

        let markdown = document.to_markdown().expect("document serializes");
        assert_eq!(markdown, "a \\~\\~two/one~ b\n");

        let reparsed = parse_document(&markdown, &SyntaxOptions::gfm());
        assert!(matches!(
            &reparsed.children[..],
            [Block::Paragraph(Paragraph {
                children,
                ..
            })] if matches!(&children[..], [Inline::Text(Text { value, .. })] if value == "a ~~two/one~ b")
        ));
    }

    #[test]
    fn delete_without_single_tilde_origin_keeps_double_tilde_marker() {
        let document = Document {
            meta: NodeMeta::default(),
            children: vec![paragraph(vec![Inline::Delete(Delete {
                meta: NodeMeta::default(),
                marker: DeleteMarker::DoubleTilde,
                children: vec![text("text~~~")],
            })])],
        };

        let markdown = document.to_markdown().expect("document serializes");
        assert!(markdown.starts_with("~~"));
        assert!(markdown.trim_end().ends_with("~~"));
    }

    #[test]
    fn single_tilde_delete_marker_is_ast_owned_without_source_span() {
        let document = Document {
            meta: NodeMeta::default(),
            children: vec![paragraph(vec![Inline::Delete(Delete {
                meta: NodeMeta::default(),
                marker: DeleteMarker::SingleTilde,
                children: vec![text("text~~~~ is ~~~~curious")],
            })])],
        };

        let markdown = document.to_markdown().expect("document serializes");
        assert_eq!(markdown, "~text\\~\\~\\~\\~ is \\~\\~\\~\\~curious~\n");

        let reparsed = parse_document(&markdown, &SyntaxOptions::gfm());
        assert!(matches!(
            &reparsed.children[..],
            [Block::Paragraph(Paragraph {
                children,
                ..
            })] if matches!(
                &children[..],
                [Inline::Delete(Delete {
                    marker: DeleteMarker::SingleTilde,
                    children: delete_children,
                    ..
                })] if matches!(&delete_children[..], [Inline::Text(Text { value, .. })] if value == "text~~~~ is ~~~~curious")
            )
        ));
    }

    #[test]
    fn serialize_options_apply_to_lists_and_code_fences_without_overflow() {
        let document = Document {
            meta: NodeMeta::default(),
            children: vec![
                Block::List(List {
                    meta: NodeMeta::default(),
                    ordered: false,
                    start: None,
                    delimiter: ListDelimiter::Dash,
                    tight: true,
                    children: vec![ListItem {
                        meta: NodeMeta::default(),
                        checked: None,
                        children: vec![paragraph(vec![text("bullet")])],
                    }],
                }),
                Block::List(List {
                    meta: NodeMeta::default(),
                    ordered: true,
                    start: Some(999_999_998),
                    delimiter: ListDelimiter::Period,
                    tight: true,
                    children: vec![
                        ListItem {
                            meta: NodeMeta::default(),
                            checked: None,
                            children: vec![paragraph(vec![text("one")])],
                        },
                        ListItem {
                            meta: NodeMeta::default(),
                            checked: None,
                            children: vec![paragraph(vec![text("two")])],
                        },
                    ],
                }),
                Block::CodeBlock(CodeBlock {
                    meta: NodeMeta::default(),
                    kind: CodeBlockKind::Fenced {
                        marker: FenceMarker::Backtick,
                        length: 3,
                    },
                    info: None,
                    value: "code".into(),
                }),
            ],
        };
        let mut options = SerializeOptions::default();
        options.bullet = ListDelimiter::Plus;
        options.ordered_delimiter = ListDelimiter::Paren;
        options.fence_marker = FenceMarker::Tilde;

        let markdown = document
            .to_markdown_with(&options)
            .expect("document serializes");
        assert_eq!(
            markdown,
            concat!(
                "+ bullet\n\n",
                "999999998) one\n",
                "999999999) two\n\n",
                "~~~\n",
                "code\n",
                "~~~\n"
            )
        );
    }

    #[test]
    fn resource_destination_and_title_kinds_are_ast_owned() {
        let document = Document {
            meta: NodeMeta::default(),
            children: vec![
                Block::Definition(Definition {
                    meta: NodeMeta::default(),
                    label: "foo".into(),
                    identifier: "foo".into(),
                    destination: "my url".into(),
                    destination_kind: LinkDestinationKind::Angle,
                    title: Some("single title".into()),
                    title_kind: Some(LinkTitleKind::SingleQuote),
                }),
                paragraph(vec![
                    Inline::Link(Link {
                        meta: NodeMeta::default(),
                        destination: "foo bar".into(),
                        destination_kind: LinkDestinationKind::Angle,
                        title: Some("paren title".into()),
                        title_kind: Some(LinkTitleKind::Paren),
                        children: vec![text("angle")],
                    }),
                    text(" "),
                    Inline::Image(Image {
                        meta: NodeMeta::default(),
                        destination: String::new(),
                        destination_kind: LinkDestinationKind::Omitted,
                        title: Some("empty title".into()),
                        title_kind: Some(LinkTitleKind::DoubleQuote),
                        alt: vec![text("empty")],
                    }),
                ]),
            ],
        };

        let markdown = document.to_markdown().expect("document serializes");
        assert_eq!(
            markdown,
            "[foo]: <my url> 'single title'\n\n[angle](<foo bar> (paren title)) ![empty]( \"empty title\")\n"
        );

        let reparsed = parse_document(&markdown, &SyntaxOptions::commonmark());
        assert!(matches!(
            &reparsed.children[..],
            [
                Block::Definition(Definition {
                    destination,
                    destination_kind: LinkDestinationKind::Angle,
                    title,
                    title_kind: Some(LinkTitleKind::SingleQuote),
                    ..
                }),
                Block::Paragraph(Paragraph {
                    children,
                    ..
                })
            ] if destination == "my url"
                && title.as_deref() == Some("single title")
                && matches!(
                    &children[..],
                    [
                        Inline::Link(Link {
                            destination: link_destination,
                            destination_kind: LinkDestinationKind::Angle,
                            title: link_title,
                            title_kind: Some(LinkTitleKind::Paren),
                            ..
                        }),
                        Inline::Text(_),
                        Inline::Image(Image {
                            destination: image_destination,
                            destination_kind: LinkDestinationKind::Omitted,
                            title: image_title,
                            title_kind: Some(LinkTitleKind::DoubleQuote),
                            ..
                        })
                    ] if link_destination == "foo bar"
                        && link_title.as_deref() == Some("paren title")
                        && image_destination.is_empty()
                        && image_title.as_deref() == Some("empty title")
                )
        ));
    }

    #[test]
    fn empty_resource_titles_are_preserved_with_their_kind() {
        let input = concat!(
            "[a](/u \"\")\n\n",
            "[b](/u '')\n\n",
            "[c](/u ())\n\n",
            "[](<> \"\")\n\n",
            "[d]: /u \"\"\n",
        );
        let document = parse_document(input, &SyntaxOptions::commonmark());

        let link_titles = document
            .children
            .iter()
            .filter_map(|block| match block {
                Block::Paragraph(Paragraph { children, .. }) => match &children[..] {
                    [Inline::Link(link)] => Some((link.title.clone(), link.title_kind)),
                    _ => None,
                },
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            link_titles,
            vec![
                (Some(String::new()), Some(LinkTitleKind::DoubleQuote)),
                (Some(String::new()), Some(LinkTitleKind::SingleQuote)),
                (Some(String::new()), Some(LinkTitleKind::Paren)),
                (Some(String::new()), Some(LinkTitleKind::DoubleQuote)),
            ],
            "empty inline titles must survive as Some(\"\") with their original kind"
        );

        let definition = document
            .children
            .iter()
            .find_map(|block| match block {
                Block::Definition(definition) => Some(definition),
                _ => None,
            })
            .expect("definition is present");
        assert_eq!(definition.title.as_deref(), Some(""));
        assert_eq!(definition.title_kind, Some(LinkTitleKind::DoubleQuote));

        let markdown = document.to_markdown().expect("document serializes");
        assert_eq!(markdown, input);

        let reparsed = parse_document(&markdown, &SyntaxOptions::commonmark());
        let second = reparsed
            .to_markdown()
            .expect("reparsed document serializes");
        assert_eq!(second, markdown);
    }

    #[test]
    fn ordinary_at_text_does_not_become_escape_when_preserving_escapes() {
        let document = Document {
            meta: NodeMeta::default(),
            children: vec![paragraph(vec![text("This@that.")])],
        };

        let markdown = document.to_markdown().expect("document serializes");
        assert_eq!(markdown, "This@that.\n");

        let options = SyntaxOptions {
            constructs: Constructs::commonmark(),
            parse: ParseOptions {
                preserve_character_escapes: true,
                ..ParseOptions::default()
            },
        };
        let reparsed = parse_document(&markdown, &options);
        assert!(matches!(
            &reparsed.children[..],
            [Block::Paragraph(Paragraph {
                children,
                ..
            })] if matches!(&children[..], [Inline::Text(Text { value, .. })] if value == "This@that.")
        ));
    }

    #[test]
    fn definition_labels_escape_brackets_backslashes_and_newlines() {
        let document = Document {
            meta: NodeMeta::default(),
            children: vec![
                Block::Definition(Definition {
                    meta: NodeMeta::default(),
                    label: "a]b\\c[d".into(),
                    identifier: "a]b\\c[d".into(),
                    destination: "/bracket".into(),
                    destination_kind: LinkDestinationKind::Bare,
                    title: None,
                    title_kind: None,
                }),
                Block::Definition(Definition {
                    meta: NodeMeta::default(),
                    label: "line\nbreak".into(),
                    identifier: "line break".into(),
                    destination: "/newline".into(),
                    destination_kind: LinkDestinationKind::Bare,
                    title: None,
                    title_kind: None,
                }),
            ],
        };

        let markdown = document.to_markdown().expect("document serializes");
        assert!(markdown.contains("[a\\]b\\\\c\\[d]: /bracket"));
        assert!(markdown.contains("[line&#xA;break]: /newline"));

        // CommonMark matches reference labels on their RAW text (no backslash
        // unescape, no entity decode), so a label that must escape `]`/`[`/`\` to
        // serialize re-parses to the escaped raw identifier, and the parsed
        // reference would match it because it folds identically.
        let reparsed = parse_document(&markdown, &SyntaxOptions::commonmark());
        match &reparsed.children[..] {
            [Block::Definition(bracket), Block::Definition(newline)] => {
                assert_eq!(bracket.identifier, "a\\]b\\\\c\\[d");
                assert_eq!(bracket.destination, "/bracket");
                assert_eq!(newline.identifier, "line&#xa;break");
                assert_eq!(newline.destination, "/newline");
            }
            other => panic!("unexpected document shape: {other:?}"),
        }
    }

    #[test]
    fn footnote_labels_escape_brackets_backslashes_and_whitespace() {
        let document = Document {
            meta: NodeMeta::default(),
            children: vec![
                paragraph(vec![
                    text("See "),
                    Inline::FootnoteReference(FootnoteReference {
                        meta: NodeMeta::default(),
                        label: "a]b\\c[d".into(),
                        identifier: "a]b\\c[d".into(),
                    }),
                    text(" and "),
                    Inline::FootnoteReference(FootnoteReference {
                        meta: NodeMeta::default(),
                        label: "white space".into(),
                        identifier: "white space".into(),
                    }),
                ]),
                Block::FootnoteDefinition(FootnoteDefinition {
                    meta: NodeMeta::default(),
                    label: "a]b\\c[d".into(),
                    identifier: "a]b\\c[d".into(),
                    children: vec![paragraph(vec![text("bracket")])],
                }),
                Block::FootnoteDefinition(FootnoteDefinition {
                    meta: NodeMeta::default(),
                    label: "white space".into(),
                    identifier: "white space".into(),
                    children: vec![paragraph(vec![text("space")])],
                }),
            ],
        };

        let markdown = document.to_markdown().expect("document serializes");
        assert!(markdown.contains("[^a\\]b\\\\c\\[d]"));
        assert!(markdown.contains("[^white&#x20;space]"));

        let reparsed = parse_document(&markdown, &SyntaxOptions::gfm());
        assert_eq!(reparsed.children.len(), 3);
        let children = match &reparsed.children[0] {
            Block::Paragraph(Paragraph { children, .. }) => children,
            other => panic!("unexpected first block: {other:?}"),
        };
        let bracket = match &reparsed.children[1] {
            Block::FootnoteDefinition(definition) => definition,
            other => panic!("unexpected second block: {other:?}"),
        };
        let space = match &reparsed.children[2] {
            Block::FootnoteDefinition(definition) => definition,
            other => panic!("unexpected third block: {other:?}"),
        };

        assert!(matches!(
            &children[..],
            [
                Inline::Text(Text { value: before, .. }),
                Inline::FootnoteReference(FootnoteReference {
                    identifier: first,
                    ..
                }),
                Inline::Text(Text { value: between, .. }),
                Inline::FootnoteReference(FootnoteReference {
                    identifier: second,
                    ..
                }),
            ] if before == "See "
                && first == "a\\]b\\\\c\\[d"
                && between == " and "
                && second == "white&#x20;space"
        ));
        // Raw-label matching keeps the escaped/entity-encoded spelling: a footnote
        // ref and its definition fold identically (so they still link), but the
        // identifier is the RAW source rather than the unescaped/decoded form.
        assert_eq!(bracket.identifier, "a\\]b\\\\c\\[d");
        assert_eq!(space.identifier, "white&#x20;space");
    }
}

mod serializer_escape {
    use crate::markdown_syntax::*;

    fn text(value: &str) -> Inline {
        Inline::Text(Text {
            meta: NodeMeta::default(),
            value: value.into(),
        })
    }

    fn paragraph(children: Vec<Inline>) -> Block {
        Block::Paragraph(Paragraph {
            meta: NodeMeta::default(),
            children,
        })
    }

    fn parse_document(markdown: &str, options: &SyntaxOptions) -> Document {
        let output = options.parse(markdown);
        assert_eq!(output.diagnostics, Vec::new());
        output.document
    }

    fn preserve_escape_options(constructs: Constructs) -> SyntaxOptions {
        SyntaxOptions {
            constructs: constructs,
            parse: ParseOptions {
                preserve_character_escapes: true,
                ..ParseOptions::default()
            },
        }
    }

    fn table_extension_options() -> SyntaxOptions {
        let mut constructs = Constructs::gfm();
        constructs.directive_text = true;
        constructs.math_inline = true;
        constructs.spoiler = true;
        SyntaxOptions {
            constructs: constructs,
            parse: ParseOptions::default(),
        }
    }

    fn assert_single_text(document: &Document, expected: &str) {
        assert!(matches!(
            &document.children[..],
            [Block::Paragraph(Paragraph {
                children,
                ..
            })] if matches!(&children[..], [Inline::Text(Text { value, .. })] if value == expected)
        ));
    }

    #[test]
    fn ordinary_punctuation_text_does_not_reparse_as_character_escapes() {
        let value = "a+b = c, #tag, wow!, a | b, a < b, C++ and x^2 ~ y & z, one ` tick";
        let document = Document {
            meta: NodeMeta::default(),
            children: vec![paragraph(vec![text(value)])],
        };

        let markdown = document.to_markdown().expect("document serializes");
        assert_eq!(markdown, format!("{value}\n"));

        let reparsed = parse_document(
            &markdown,
            &preserve_escape_options(Constructs::commonmark()),
        );
        assert_single_text(&reparsed, value);
    }

    #[test]
    fn invalid_character_reference_like_text_stays_text() {
        let value = "Invalid &unknown; &copy and &#x; stay text.";
        let document = Document {
            meta: NodeMeta::default(),
            children: vec![paragraph(vec![text(value)])],
        };

        let markdown = document.to_markdown().expect("document serializes");
        assert_eq!(markdown, "Invalid \\&unknown; &copy and &#x; stay text.\n");

        let reparsed = parse_document(&markdown, &SyntaxOptions::commonmark());
        assert_single_text(&reparsed, value);
    }

    #[test]
    fn table_cells_do_not_leak_inline_pipes_that_split_cells() {
        let document = Document {
            meta: NodeMeta::default(),
            children: vec![
                Block::Table(Table {
                    meta: NodeMeta::default(),
                    alignments: vec![
                        TableAlignment::None,
                        TableAlignment::None,
                        TableAlignment::None,
                        TableAlignment::None,
                        TableAlignment::None,
                        TableAlignment::None,
                        TableAlignment::None,
                    ],
                    rows: vec![
                        TableRow {
                            meta: NodeMeta::default(),
                            cells: vec![
                                TableCell {
                                    meta: NodeMeta::default(),
                                    children: vec![text("Text")],
                                },
                                TableCell {
                                    meta: NodeMeta::default(),
                                    children: vec![text("Code")],
                                },
                                TableCell {
                                    meta: NodeMeta::default(),
                                    children: vec![text("Math")],
                                },
                                TableCell {
                                    meta: NodeMeta::default(),
                                    children: vec![text("Link")],
                                },
                                TableCell {
                                    meta: NodeMeta::default(),
                                    children: vec![text("Image")],
                                },
                                TableCell {
                                    meta: NodeMeta::default(),
                                    children: vec![text("Reference")],
                                },
                                TableCell {
                                    meta: NodeMeta::default(),
                                    children: vec![text("Directive")],
                                },
                            ],
                        },
                        TableRow {
                            meta: NodeMeta::default(),
                            cells: vec![
                                TableCell {
                                    meta: NodeMeta::default(),
                                    children: vec![text("a|b")],
                                },
                                TableCell {
                                    meta: NodeMeta::default(),
                                    children: vec![Inline::Code(CodeInline {
                                        meta: NodeMeta::default(),
                                        value: "c|d".into(),
                                        raw: String::new(),
                                        fence_length: 0,
                                    })],
                                },
                                TableCell {
                                    meta: NodeMeta::default(),
                                    children: vec![Inline::Math(MathInline {
                                        meta: NodeMeta::default(),
                                        value: "x|y".into(),
                                        kind: MathInlineKind::Dollar { dollars: 1 },
                                    })],
                                },
                                TableCell {
                                    meta: NodeMeta::default(),
                                    children: vec![Inline::Link(Link {
                                        meta: NodeMeta::default(),
                                        destination: "/link".into(),
                                        destination_kind: LinkDestinationKind::Bare,
                                        title: None,
                                        title_kind: None,
                                        children: vec![text("link|label")],
                                    })],
                                },
                                TableCell {
                                    meta: NodeMeta::default(),
                                    children: vec![Inline::Image(Image {
                                        meta: NodeMeta::default(),
                                        destination: "/img".into(),
                                        destination_kind: LinkDestinationKind::Bare,
                                        title: None,
                                        title_kind: None,
                                        alt: vec![text("img|alt")],
                                    })],
                                },
                                TableCell {
                                    meta: NodeMeta::default(),
                                    children: vec![Inline::LinkReference(LinkReference {
                                        meta: NodeMeta::default(),
                                        identifier: "pipe|id".into(),
                                        label: "pipe|id".into(),
                                        kind: ReferenceKind::Full,
                                        children: vec![text("ref|text")],
                                    })],
                                },
                                TableCell {
                                    meta: NodeMeta::default(),
                                    children: vec![Inline::TextDirective(TextDirective {
                                        meta: NodeMeta::default(),
                                        name: "note".into(),
                                        label: vec![text("label|text")],
                                        attributes: vec![DirectiveAttribute {
                                            name: "data".into(),
                                            value: Some("value|pipe".into()),
                                        }],
                                    })],
                                },
                            ],
                        },
                    ],
                }),
                Block::Definition(Definition {
                    meta: NodeMeta::default(),
                    label: "pipe|id".into(),
                    identifier: "pipe|id".into(),
                    destination: "/dest".into(),
                    destination_kind: LinkDestinationKind::Bare,
                    title: None,
                    title_kind: None,
                }),
            ],
        };

        let markdown = document.to_markdown().expect("document serializes");
        assert!(markdown.contains("a&#x7C;b"));
        assert!(markdown.contains(r"`c\|d`"));
        assert!(markdown.contains(r"$`x\|y`$"));
        assert!(markdown.contains("[link&#x7C;label](/link)"));
        assert!(markdown.contains("![img&#x7C;alt](/img)"));
        assert!(markdown.contains("[ref&#x7C;text][pipe\\|id]"));
        assert!(markdown.contains(":note[label&#x7C;text]{data=\"value\\|pipe\"}"));

        let reparsed = parse_document(&markdown, &table_extension_options());
        match &reparsed.children[..] {
            [Block::Table(table), Block::Definition(_)] => {
                assert_eq!(table.rows[1].cells.len(), 7);
                assert!(matches!(
                    &table.rows[1].cells[0].children[..],
                    [Inline::Text(Text { value, .. })] if value == "a|b"
                ));
                assert!(matches!(
                    &table.rows[1].cells[1].children[..],
                    [Inline::Code(CodeInline { value, .. })] if value == "c|d"
                ));
                assert!(matches!(
                    &table.rows[1].cells[2].children[..],
                    [Inline::Math(MathInline { value, .. })] if value == "x|y"
                ));
                assert!(matches!(
                    &table.rows[1].cells[3].children[..],
                    [Inline::Link(Link { children, .. })]
                        if matches!(&children[..], [Inline::Text(Text { value, .. })] if value == "link|label")
                ));
                assert!(matches!(
                    &table.rows[1].cells[4].children[..],
                    [Inline::Image(Image { alt, .. })]
                        if matches!(&alt[..], [Inline::Text(Text { value, .. })] if value == "img|alt")
                ));
                assert!(matches!(
                    &table.rows[1].cells[5].children[..],
                    [Inline::LinkReference(LinkReference {
                        identifier,
                        children,
                        ..
                    })] if identifier == "pipe|id"
                        && matches!(&children[..], [Inline::Text(Text { value, .. })] if value == "ref|text")
                ));
                assert!(matches!(
                    &table.rows[1].cells[6].children[..],
                    [Inline::TextDirective(TextDirective {
                        label,
                        attributes,
                        ..
                    })] if matches!(&label[..], [Inline::Text(Text { value, .. })] if value == "label|text")
                        && matches!(&attributes[..], [DirectiveAttribute { name, value: Some(value) }]
                            if name == "data" && value == "value|pipe")
                ));
            }
            other => panic!("unexpected document shape: {other:?}"),
        }
    }

    #[test]
    fn spoiler_in_table_cell_roundtrips_without_splitting_columns() {
        let document = Document {
            meta: NodeMeta::default(),
            children: vec![Block::Table(Table {
                meta: NodeMeta::default(),
                alignments: vec![TableAlignment::None],
                rows: vec![
                    TableRow {
                        meta: NodeMeta::default(),
                        cells: vec![TableCell {
                            meta: NodeMeta::default(),
                            children: vec![text("Result")],
                        }],
                    },
                    TableRow {
                        meta: NodeMeta::default(),
                        cells: vec![TableCell {
                            meta: NodeMeta::default(),
                            children: vec![Inline::Spoiler(Spoiler {
                                meta: NodeMeta::default(),
                                children: vec![text("visible")],
                            })],
                        }],
                    },
                ],
            })],
        };

        let markdown = document.to_markdown().expect("document serializes");
        assert_eq!(markdown, "| Result |\n| --- |\n| ||visible|| |\n");

        let reparsed = parse_document(&markdown, &table_extension_options());
        assert!(matches!(
            &reparsed.children[..],
            [Block::Table(Table { rows, .. })]
                if rows.len() == 2
                    && rows[1].cells.len() == 1
                    && matches!(
                        &rows[1].cells[0].children[..],
                        [Inline::Spoiler(Spoiler { children, .. })]
                            if matches!(&children[..], [Inline::Text(Text { value, .. })] if value == "visible")
                    )
        ));
    }
}

mod review_serialize {
    //! Regression coverage for serializer round-trip defects.
    //! Each test builds (or parses) an AST, serializes it, asserts the
    //! exact serialized string, and asserts that re-parsing yields the same AST.

    use crate::markdown_syntax::*;

    fn text(value: &str) -> Inline {
        Inline::Text(Text {
            meta: NodeMeta::default(),
            value: value.into(),
        })
    }

    fn soft_break() -> Inline {
        Inline::SoftBreak(SoftBreak {
            meta: NodeMeta::default(),
        })
    }

    fn emphasis(children: Vec<Inline>) -> Inline {
        Inline::Emphasis(Emphasis {
            meta: NodeMeta::default(),
            children,
        })
    }

    fn paragraph(children: Vec<Inline>) -> Block {
        Block::Paragraph(Paragraph {
            meta: NodeMeta::default(),
            children,
        })
    }

    fn document(children: Vec<Block>) -> Document {
        Document {
            meta: NodeMeta::default(),
            children,
        }
    }

    fn parse(markdown: &str, options: &SyntaxOptions) -> Document {
        let output = options.parse(markdown);
        assert_eq!(output.diagnostics, Vec::new());
        output.document
    }

    /// Assert that re-parsing the serialized markdown lands on the same document.
    /// Spans differ between a hand-built AST and a freshly parsed one, so equality
    /// is checked through the serializer (which is span-agnostic and idempotent):
    /// the reparsed document must serialize back to exactly the same markdown.
    fn assert_round_trip_fixpoint(original_markdown: &str, reparsed: &Document) {
        let reserialized = reparsed
            .to_markdown()
            .expect("reparsed document serializes");
        assert_eq!(reserialized, original_markdown);
    }

    fn preserve_references_options() -> SyntaxOptions {
        let constructs = Constructs::commonmark();
        let parse = ParseOptions {
            preserve_character_references: true,
            ..ParseOptions::default()
        };
        SyntaxOptions {
            constructs: constructs,
            parse: parse,
        }
    }

    // --- SR1: entity-encoded shortcut/collapsed references stay implicit --------

    #[test]
    fn sr1_entity_reference_shortcut_is_not_promoted_to_full() {
        let options = preserve_references_options();
        // CommonMark matches reference labels on their RAW text (Unicode case fold +
        // whitespace collapse only — no entity decode, no backslash unescape). So an
        // entity-spelled shortcut matches a definition with the SAME entity spelling;
        // the label oracle agrees and leaves the reference a Shortcut instead of
        // expanding it to `[f&#246;o][f&#246;o]`.
        let document = parse("[f&#246;o]\n\n[f&#246;o]: /url\n", &options);

        let markdown = document.to_markdown().expect("document serializes");
        assert_eq!(markdown, "[f&#246;o]\n\n[f&#246;o]: /url\n");

        let reparsed = parse(&markdown, &options);
        assert_round_trip_fixpoint(&markdown, &reparsed);
    }

    // --- L3: explicit reference labels keep their original case/spelling --------

    #[test]
    fn l3_full_reference_preserves_label_case() {
        let document = parse("[text][Ref]\n\n[ref]: /url\n", &SyntaxOptions::commonmark());

        let markdown = document.to_markdown().expect("document serializes");
        assert_eq!(markdown, "[text][Ref]\n\n[ref]: /url\n");

        let reparsed = parse(&markdown, &SyntaxOptions::commonmark());
        assert_round_trip_fixpoint(&markdown, &reparsed);
    }

    #[test]
    fn l3_explicit_label_is_not_double_escaped() {
        let document = parse(
            "Use [text][Foo\\]] and [t][A &amp; B].\n\n[Foo\\]]: /a\n\n[A &amp; B]: /b\n",
            &SyntaxOptions::commonmark(),
        );

        let markdown = document.to_markdown().expect("document serializes");
        // The parsed (source) label is emitted verbatim — no re-escaping of the
        // `\]` or re-encoding of `&amp;`.
        assert!(markdown.contains("[text][Foo\\]]"));
        assert!(markdown.contains("[t][A &amp; B]"));

        let reparsed = parse(&markdown, &SyntaxOptions::commonmark());
        assert_round_trip_fixpoint(&markdown, &reparsed);
    }

    // --- S1: dash thematic break re-parses as a dash thematic break -------------

    #[test]
    fn s1_dash_thematic_break_round_trips_after_a_block() {
        let document = document(vec![
            paragraph(vec![text("intro")]),
            Block::ThematicBreak(ThematicBreak {
                meta: NodeMeta::default(),
                marker: ThematicBreakMarker::Dash,
            }),
        ]);

        let markdown = document.to_markdown().expect("document serializes");
        assert_eq!(markdown, "intro\n\n---\n");

        let reparsed = parse(&markdown, &SyntaxOptions::commonmark());
        assert!(matches!(
            reparsed.children.as_slice(),
            [
                Block::Paragraph(_),
                Block::ThematicBreak(ThematicBreak {
                    marker: ThematicBreakMarker::Dash,
                    ..
                })
            ]
        ));
        assert_round_trip_fixpoint(&markdown, &reparsed);
    }

    #[test]
    fn s1_leading_dash_thematic_break_uses_spaced_form() {
        let document = document(vec![Block::ThematicBreak(ThematicBreak {
            meta: NodeMeta::default(),
            marker: ThematicBreakMarker::Dash,
        })]);

        let markdown = document.to_markdown().expect("document serializes");
        // A contiguous `---` at the document start would open frontmatter, so the
        // spaced form is used; it still re-parses as a dash thematic break.
        assert_eq!(markdown, "- - -\n");

        let mut constructs = Constructs::commonmark();
        constructs.frontmatter = true;
        let frontmatter = SyntaxOptions {
            constructs: constructs,
            parse: ParseOptions::default(),
        };
        let reparsed = parse(&markdown, &frontmatter);
        assert!(matches!(
            reparsed.children.as_slice(),
            [Block::ThematicBreak(ThematicBreak {
                marker: ThematicBreakMarker::Dash,
                ..
            })]
        ));
    }

    // --- SR11: fenced code honors the stored fence marker -----------------------

    #[test]
    fn sr11_fenced_code_honors_tilde_marker() {
        let document = document(vec![Block::CodeBlock(CodeBlock {
            meta: NodeMeta::default(),
            kind: CodeBlockKind::Fenced {
                marker: FenceMarker::Tilde,
                length: 3,
            },
            info: None,
            value: "code".into(),
        })]);

        let markdown = document.to_markdown().expect("document serializes");
        assert_eq!(markdown, "~~~\ncode\n~~~\n");

        let reparsed = parse(&markdown, &SyntaxOptions::commonmark());
        assert!(matches!(
            reparsed.children.as_slice(),
            [Block::CodeBlock(CodeBlock {
                kind: CodeBlockKind::Fenced {
                    marker: FenceMarker::Tilde,
                    ..
                },
                ..
            })]
        ));
        assert_round_trip_fixpoint(&markdown, &reparsed);
    }

    // --- S4 / SR3: adjacent same-delimiter emphasis stay two nodes --------------

    #[test]
    fn s4_adjacent_emphasis_does_not_merge() {
        let document = document(vec![paragraph(vec![
            emphasis(vec![text("a")]),
            emphasis(vec![text("b")]),
        ])]);

        let markdown = document.to_markdown().expect("document serializes");
        // The second run switches to `_` so the two runs do not fuse into `*a**b*`.
        assert_eq!(markdown, "*a*_b_\n");

        let reparsed = parse(&markdown, &SyntaxOptions::commonmark());
        assert!(matches!(
            reparsed.children.as_slice(),
            [Block::Paragraph(Paragraph { children, .. })]
                if matches!(children.as_slice(), [Inline::Emphasis(_), Inline::Emphasis(_)])
        ));
        assert_round_trip_fixpoint(&markdown, &reparsed);
    }

    #[test]
    fn s4_emphasis_then_text_starting_with_delimiter_does_not_merge() {
        let document = document(vec![paragraph(vec![emphasis(vec![text("a")]), text("*b")])]);

        let markdown = document.to_markdown().expect("document serializes");
        assert_eq!(markdown, "*a*\\*b\n");

        let reparsed = parse(&markdown, &SyntaxOptions::commonmark());
        assert!(matches!(
            reparsed.children.as_slice(),
            [Block::Paragraph(Paragraph { children, .. })]
                if matches!(
                    children.as_slice(),
                    [Inline::Emphasis(_), Inline::Text(Text { value, .. })] if value == "*b"
                )
        ));
        assert_round_trip_fixpoint(&markdown, &reparsed);
    }

    // --- S3: trailing `#` in ATX heading content is escaped ---------------------

    #[test]
    fn s3_atx_heading_escapes_trailing_hash() {
        let document = document(vec![Block::Heading(Heading {
            meta: NodeMeta::default(),
            depth: 1,
            kind: HeadingKind::Atx,
            children: vec![text("foo #")],
        })]);

        let markdown = document.to_markdown().expect("document serializes");
        assert_eq!(markdown, "# foo \\#\n");

        let reparsed = parse(&markdown, &SyntaxOptions::commonmark());
        assert!(matches!(
            reparsed.children.as_slice(),
            [Block::Heading(Heading { depth: 1, children, .. })]
                if matches!(children.as_slice(), [Inline::Text(Text { value, .. })] if value == "foo #")
        ));
        assert_round_trip_fixpoint(&markdown, &reparsed);
    }

    // --- S2: setext fallback to ATX for unrepresentable depth -------------------

    #[test]
    fn s2_setext_depth_three_falls_back_to_atx() {
        let document = document(vec![Block::Heading(Heading {
            meta: NodeMeta::default(),
            depth: 3,
            kind: HeadingKind::Setext,
            children: vec![text("foo")],
        })]);

        let markdown = document.to_markdown().expect("document serializes");
        assert_eq!(markdown, "### foo\n");

        let reparsed = parse(&markdown, &SyntaxOptions::commonmark());
        assert!(matches!(
            reparsed.children.as_slice(),
            [Block::Heading(Heading {
                depth: 3,
                kind: HeadingKind::Atx,
                ..
            })]
        ));
    }

    #[test]
    fn s2_multi_line_setext_stays_setext() {
        let document = document(vec![Block::Heading(Heading {
            meta: NodeMeta::default(),
            depth: 1,
            kind: HeadingKind::Setext,
            children: vec![text("foo"), soft_break(), text("bar")],
        })]);

        let markdown = document.to_markdown().expect("document serializes");
        assert_eq!(markdown, "foo\nbar\n=======\n");

        let reparsed = parse(&markdown, &SyntaxOptions::commonmark());
        assert!(matches!(
            reparsed.children.as_slice(),
            [Block::Heading(Heading { depth: 1, kind: HeadingKind::Setext, children, .. })]
                if children.iter().filter(|i| matches!(i, Inline::SoftBreak(_))).count() == 1
        ));
        assert_round_trip_fixpoint(&markdown, &reparsed);
    }

    // --- S5: asterisk-bullet thematic break stays inside its list ---------------

    #[test]
    fn s5_asterisk_bullet_thematic_break_item_is_disambiguated() {
        let list = Block::List(List {
            meta: NodeMeta::default(),
            ordered: false,
            start: None,
            delimiter: ListDelimiter::Asterisk,
            tight: true,
            children: vec![ListItem {
                meta: NodeMeta::default(),
                checked: None,
                children: vec![Block::ThematicBreak(ThematicBreak {
                    meta: NodeMeta::default(),
                    marker: ThematicBreakMarker::Asterisk,
                })],
            }],
        });
        let document = document(vec![list]);

        let markdown = document.to_markdown().expect("document serializes");
        // `* ***` would escape the list as a top-level thematic break, so the item
        // body is rewritten to a dash break that stays inside the list.
        assert_eq!(markdown, "* ---\n");

        let reparsed = parse(&markdown, &SyntaxOptions::commonmark());
        assert!(matches!(
            reparsed.children.as_slice(),
            [Block::List(List { children, .. })]
                if matches!(
                    children.as_slice(),
                    [ListItem { children, .. }]
                        if matches!(children.as_slice(), [Block::ThematicBreak(_)])
                )
        ));
    }

    #[test]
    fn s5_asterisk_bullet_nested_list_is_left_intact() {
        let inner = Block::List(List {
            meta: NodeMeta::default(),
            ordered: false,
            start: None,
            delimiter: ListDelimiter::Asterisk,
            tight: true,
            children: vec![ListItem {
                meta: NodeMeta::default(),
                checked: None,
                children: Vec::new(),
            }],
        });
        let outer = Block::List(List {
            meta: NodeMeta::default(),
            ordered: false,
            start: None,
            delimiter: ListDelimiter::Asterisk,
            tight: true,
            children: vec![ListItem {
                meta: NodeMeta::default(),
                checked: None,
                children: vec![inner],
            }],
        });
        let document = document(vec![outer]);

        let markdown = document.to_markdown().expect("document serializes");
        // A genuine nested bullet (`* *`) has interior whitespace and must NOT be
        // rewritten into a thematic break.
        assert!(!markdown.contains("---"));
        assert!(markdown.contains('*'));
    }
}
