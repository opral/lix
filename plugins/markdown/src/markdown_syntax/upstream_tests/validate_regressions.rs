//! `validate_document` regression coverage: the standalone zero-width-table
//! check plus the validator-hardening rejects from the review pass.
//!
//! Each former regression file is preserved verbatim inside its own `mod` so
//! that helper functions and test names cannot collide across the merged
//! sources.

mod validation {
    use crate::markdown_syntax::*;

    #[test]
    fn empty_table_is_invalid() {
        let document = Document {
            meta: NodeMeta::default(),
            children: vec![Block::Table(Table {
                meta: NodeMeta::default(),
                alignments: vec![],
                rows: vec![],
            })],
        };

        let diagnostics = document.validate();
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(
            diagnostics[0].message,
            "table must contain at least a header row"
        );

        assert!(matches!(
            document.to_markdown(),
            Err(SerializeError::InvalidDocument(_))
        ));
    }

    #[test]
    fn zero_width_table_is_invalid() {
        let document = Document {
            meta: NodeMeta::default(),
            children: vec![Block::Table(Table {
                meta: NodeMeta::default(),
                alignments: vec![],
                rows: vec![TableRow {
                    meta: NodeMeta::default(),
                    cells: vec![],
                }],
            })],
        };

        let diagnostics = document.validate();
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(
            diagnostics[0].message,
            "table header row must contain at least one cell"
        );

        assert!(matches!(
            document.to_markdown(),
            Err(SerializeError::InvalidDocument(_))
        ));
    }
}

mod review_validate {
    //! Regressions for `validate_document` hardening.
    //!
    //! Each item rejects a hand-buildable AST shape that serializes to Markdown the
    //! parser cannot reconstruct. Every case pairs a rejected (bad) shape with a
    //! nearby valid (good) shape that must still pass, so the rejects stay narrow.

    use crate::markdown_syntax::*;

    fn paragraph(children: Vec<Inline>) -> Document {
        Document {
            meta: NodeMeta::default(),
            children: vec![Block::Paragraph(Paragraph {
                meta: NodeMeta::default(),
                children,
            })],
        }
    }

    fn text(value: &str) -> Inline {
        Inline::Text(Text {
            meta: NodeMeta::default(),
            value: value.into(),
        })
    }

    #[test]
    fn zero_dollar_inline_math_is_invalid() {
        let bad = paragraph(vec![Inline::Math(MathInline {
            meta: NodeMeta::default(),
            value: "x".into(),
            kind: MathInlineKind::Dollar { dollars: 0 },
        })]);
        assert!(!bad.validate().is_empty());
        assert!(matches!(
            bad.to_markdown(),
            Err(SerializeError::InvalidDocument(_))
        ));

        let good = paragraph(vec![Inline::Math(MathInline {
            meta: NodeMeta::default(),
            value: "x".into(),
            kind: MathInlineKind::Dollar { dollars: 1 },
        })]);
        assert!(good.validate().is_empty());
    }

    #[test]
    fn sr6_rejects_each_emphasis_like_container_when_empty() {
        let empty_containers = [
            Inline::Emphasis(Emphasis {
                meta: NodeMeta::default(),
                children: vec![],
            }),
            Inline::Strong(Strong {
                meta: NodeMeta::default(),
                children: vec![],
            }),
            Inline::Underline(Underline {
                meta: NodeMeta::default(),
                children: vec![],
            }),
            Inline::Delete(Delete {
                meta: NodeMeta::default(),
                marker: DeleteMarker::DoubleTilde,
                children: vec![],
            }),
            Inline::Insert(Insert {
                meta: NodeMeta::default(),
                children: vec![],
            }),
            Inline::Mark(Mark {
                meta: NodeMeta::default(),
                children: vec![],
            }),
            Inline::Subscript(Subscript {
                meta: NodeMeta::default(),
                children: vec![],
            }),
            Inline::Superscript(Superscript {
                meta: NodeMeta::default(),
                children: vec![],
            }),
            Inline::Spoiler(Spoiler {
                meta: NodeMeta::default(),
                children: vec![],
            }),
        ];

        for container in empty_containers {
            let document = paragraph(vec![container]);
            assert!(
                !document.validate().is_empty(),
                "empty container should be rejected"
            );
        }

        let bad = paragraph(vec![Inline::Emphasis(Emphasis {
            meta: NodeMeta::default(),
            children: vec![],
        })]);
        assert!(matches!(
            bad.to_markdown(),
            Err(SerializeError::InvalidDocument(_))
        ));

        let good = paragraph(vec![Inline::Emphasis(Emphasis {
            meta: NodeMeta::default(),
            children: vec![text("a")],
        })]);
        assert!(good.validate().is_empty());
    }

    // SR7 — an escape of a non-ASCII-punctuation char serializes `\x` which the
    // parser keeps literal.
    #[test]
    fn sr7_non_punctuation_escape_is_invalid() {
        let bad = paragraph(vec![Inline::Escape(Escape {
            meta: NodeMeta::default(),
            value: 'a',
        })]);
        assert!(!bad.validate().is_empty());

        // Escaping an ASCII punctuation char is valid.
        let good = paragraph(vec![Inline::Escape(Escape {
            meta: NodeMeta::default(),
            value: '*',
        })]);
        assert!(good.validate().is_empty());
    }

    // SR8 — an autolink destination containing whitespace or angle brackets cannot
    // serialize as `<dest>` and round-trip.
    #[test]
    fn sr8_autolink_with_whitespace_or_angles_is_invalid() {
        for dest in ["has space", "has<angle", "has>angle"] {
            let bad = paragraph(vec![Inline::Autolink(Autolink {
                meta: NodeMeta::default(),
                destination: dest.into(),
                kind: AutolinkKind::Angle,
            })]);
            assert!(
                !bad.validate().is_empty(),
                "autolink `{dest}` should be rejected"
            );
        }

        let good = paragraph(vec![Inline::Autolink(Autolink {
            meta: NodeMeta::default(),
            destination: "https://example.com/path?q=1".into(),
            kind: AutolinkKind::Angle,
        })]);
        assert!(good.validate().is_empty());
    }

    // SR9 — inline code stored as a raw passthrough whose backtick run is at least
    // as long as its fence would close the span early.
    #[test]
    fn sr9_inline_code_raw_backtick_run_is_invalid() {
        let bad = paragraph(vec![Inline::Code(CodeInline {
            meta: NodeMeta::default(),
            value: "a`b".into(),
            raw: "a`b".into(),
            fence_length: 1,
        })]);
        assert!(!bad.validate().is_empty());

        // A raw run shorter than the fence is safe.
        let shorter = paragraph(vec![Inline::Code(CodeInline {
            meta: NodeMeta::default(),
            value: "a`b".into(),
            raw: "a`b".into(),
            fence_length: 2,
        })]);
        assert!(shorter.validate().is_empty());

        // A raw run LONGER than the fence is also inert (a fence of length N closes
        // only on a run of exactly N) — this is the `` ` `` ` `` code-span shape.
        let longer = paragraph(vec![Inline::Code(CodeInline {
            meta: NodeMeta::default(),
            value: "``".into(),
            raw: " `` ".into(),
            fence_length: 1,
        })]);
        assert!(longer.validate().is_empty());

        // The value path (no raw passthrough) is always safe.
        let value_only = paragraph(vec![Inline::Code(CodeInline {
            meta: NodeMeta::default(),
            value: "a`b".into(),
            raw: String::new(),
            fence_length: 0,
        })]);
        assert!(value_only.validate().is_empty());
    }

    // SR4 — an ordered list start beyond the parser's 9-digit marker cap round-trips
    // to a paragraph.
    #[test]
    fn sr4_ordered_list_start_overflow_is_invalid() {
        let bad = Document {
            meta: NodeMeta::default(),
            children: vec![Block::List(List {
                meta: NodeMeta::default(),
                ordered: true,
                start: Some(1_000_000_000),
                delimiter: ListDelimiter::Period,
                tight: true,
                children: vec![ListItem {
                    meta: NodeMeta::default(),
                    checked: None,
                    children: vec![Block::Paragraph(Paragraph {
                        meta: NodeMeta::default(),
                        children: vec![text("foo")],
                    })],
                }],
            })],
        };
        assert!(!bad.validate().is_empty());
        assert!(matches!(
            bad.to_markdown(),
            Err(SerializeError::InvalidDocument(_))
        ));

        // The largest 9-digit start is still representable.
        let good = Document {
            meta: NodeMeta::default(),
            children: vec![Block::List(List {
                meta: NodeMeta::default(),
                ordered: true,
                start: Some(999_999_999),
                delimiter: ListDelimiter::Period,
                tight: true,
                children: vec![ListItem {
                    meta: NodeMeta::default(),
                    checked: None,
                    children: vec![Block::Paragraph(Paragraph {
                        meta: NodeMeta::default(),
                        children: vec![text("foo")],
                    })],
                }],
            })],
        };
        assert!(good.validate().is_empty());
    }

    // SR5 — a hard line break as the final inline of a container serializes to a
    // dangling `\` / trailing spaces the parser cannot reconstruct as a break.
    #[test]
    fn sr5_trailing_hard_line_break_is_invalid() {
        let bad = paragraph(vec![
            text("foo"),
            Inline::LineBreak(LineBreak {
                meta: NodeMeta::default(),
                kind: LineBreakKind::Backslash,
            }),
        ]);
        assert!(!bad.validate().is_empty());

        // A mid-paragraph hard break (followed by content) is fine.
        let good = paragraph(vec![
            text("foo"),
            Inline::LineBreak(LineBreak {
                meta: NodeMeta::default(),
                kind: LineBreakKind::Backslash,
            }),
            text("bar"),
        ]);
        assert!(good.validate().is_empty());
    }

    // L5 (AST side) — a definition with an empty/blank identifier, parity with the
    // existing footnote-definition empty-identifier check.
    #[test]
    fn l5_definition_empty_identifier_is_invalid() {
        let bad = Document {
            meta: NodeMeta::default(),
            children: vec![Block::Definition(Definition {
                meta: NodeMeta::default(),
                label: String::new(),
                identifier: "   ".into(),
                destination: "/uri".into(),
                destination_kind: LinkDestinationKind::Bare,
                title: None,
                title_kind: None,
            })],
        };
        assert!(!bad.validate().is_empty());

        let good = Document {
            meta: NodeMeta::default(),
            children: vec![Block::Definition(Definition {
                meta: NodeMeta::default(),
                label: "ref".into(),
                identifier: "ref".into(),
                destination: "/uri".into(),
                destination_kind: LinkDestinationKind::Bare,
                title: None,
                title_kind: None,
            })],
        };
        assert!(good.validate().is_empty());
    }
}
