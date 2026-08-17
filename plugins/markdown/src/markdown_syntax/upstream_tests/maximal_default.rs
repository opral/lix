//! Locks the maximal-default dialect and its delimiter-collision resolutions
//! (decisions/005). `parse` enables every non-MDX construct except `underline`;
//! these cases pin the decided picks so a future change cannot silently move them.

use crate::markdown_syntax::prelude::*;

fn first_para(md: &str) -> Vec<Inline> {
    match parse(md).document.children.into_iter().next() {
        Some(Block::Paragraph(p)) => p.children,
        other => panic!("expected a paragraph, got {other:?}"),
    }
}

#[test]
fn underscore_strong_stays_strong() {
    // `underline` is excluded from the default, so `__x__` is CommonMark strong.
    assert!(matches!(
        first_para("a __b__ c").get(1),
        Some(Inline::Strong(_))
    ));
    // `**x**` strong is unaffected either way.
    assert!(matches!(
        first_para("a **b** c").get(1),
        Some(Inline::Strong(_))
    ));
}

#[test]
fn underline_is_opt_in_via_builder() {
    let out = SyntaxOptions::default()
        .enable(Construct::Underline)
        .parse("a __b__ c");
    let Block::Paragraph(p) = &out.document.children[0] else {
        panic!("expected paragraph");
    };
    assert!(matches!(p.children.get(1), Some(Inline::Underline(_))));
}

#[test]
fn delimiter_collisions_resolve_per_decision() {
    assert!(matches!(
        first_para("~~s~~").first(),
        Some(Inline::Delete(_))
    ));
    assert!(matches!(
        first_para("H~2~O").get(1),
        Some(Inline::Subscript(_))
    ));
    assert!(matches!(
        first_para("x^2^").get(1),
        Some(Inline::Superscript(_))
    ));
    assert!(matches!(
        first_para("note^[x] tail").get(1),
        Some(Inline::InlineFootnote(_))
    ));
    assert!(matches!(
        first_para("a :tada: b").get(1),
        Some(Inline::Shortcode(_))
    ));
}

#[test]
fn dollar_amounts_stay_text() {
    // The math parser needs tight delimiters, so `$5 to $10` is not inline math.
    let inlines = first_para("price $5 to $10 today");
    assert!(
        inlines.iter().all(|i| !matches!(i, Inline::Math(_))),
        "unexpected math node: {inlines:?}"
    );
}

#[test]
fn wikilink_default_is_after_pipe() {
    let inlines = first_para("see [[target|label]] here");
    let Some(Inline::WikiLink(link)) = inlines.get(1) else {
        panic!("expected a wikilink: {inlines:?}");
    };
    assert_eq!(link.label_order, WikiLinkLabelOrder::AfterPipe);
    assert_eq!(link.target, "target");
    assert_eq!(link.label, "label");
}

#[test]
fn html_containers_are_in_the_maximal_default() {
    let out = parse("<details>\n<summary>Open</summary>\n\nbody\n\n</details>\n");

    assert!(matches!(
        out.document.children.as_slice(),
        [Block::HtmlContainer(_)]
    ));
}

#[test]
fn build_layer_round_trips() {
    let document = Document {
        meta: NodeMeta::default(),
        children: vec![
            Heading::new(1, [Text::from("Title")]).into(),
            Paragraph::new([Text::from("hello")]).into(),
        ],
    };
    // Hand-built nodes carry no span.
    assert_eq!(document.children[0].span(), None);
    assert_eq!(document.to_markdown().unwrap(), "# Title\n\nhello\n");
}
