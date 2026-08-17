use crate::markdown_syntax::parse;

#[test]
fn top_level_block_spans_slice_the_original_source() {
    for (name, source) in [
        (
            "ordinary_markdown",
            "# Title\n\nparagraph with *emphasis*\n\n- one\n- two\n",
        ),
        ("crlf_line_endings", "# Title\r\n\r\nparagraph\r\n"),
        ("leading_bom", "\u{feff}# title\n\nparagraph\n"),
        ("embedded_nul", "alpha \u{0} beta\n\nparagraph\n"),
        ("unterminated_fence", "```rust\nlet value = 1;\n"),
    ] {
        assert_original_source_tiling(name, source);
    }
}

fn assert_original_source_tiling(name: &str, source: &str) {
    let output = parse(source);
    let mut cursor = 0;

    for (index, block) in output.document.children.iter().enumerate() {
        let span = block
            .span()
            .unwrap_or_else(|| panic!("{name}: top-level block {index} has no span"));

        assert!(
            span.start >= cursor,
            "{name}: top-level block {index} overlaps the previous span: {span:?}"
        );
        assert!(
            span.end <= source.len(),
            "{name}: top-level block {index} span exceeds source length: {span:?}, len={}",
            source.len()
        );
        assert!(
            source.is_char_boundary(span.start) && source.is_char_boundary(span.end),
            "{name}: top-level block {index} span is not on UTF-8 boundaries: {span:?}"
        );

        assert!(
            source[cursor..span.start].chars().all(char::is_whitespace),
            "{name}: non-trivia bytes before top-level block {index}: {:?}",
            &source[cursor..span.start]
        );

        let _slice = &source[span.start..span.end];
        cursor = span.end;
    }

    assert!(
        source[cursor..].chars().all(char::is_whitespace),
        "{name}: non-trivia bytes after final top-level block: {:?}",
        &source[cursor..]
    );
}
