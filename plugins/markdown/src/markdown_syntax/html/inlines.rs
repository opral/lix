//! All 30 `Inline` arms.

use alloc::format;
use alloc::string::String;

use crate::ast::{AutolinkKind, DirectiveAttribute, Inline, MathInlineKind};

use super::escape::{
    attr_escape, attr_escape_gfm, encode_href, escape_text, filter_img_protocol, filter_protocol,
};
use super::footnotes;
use super::refs::{escaped_alt, flatten_alt, visible_text};
use super::{Ctx, SafeRawHtmlForm};

/// Render an inline slice by concatenating each node's HTML.
pub fn render_inlines(children: &[Inline], ctx: &Ctx) -> String {
    let mut out = String::new();
    for child in children {
        out.push_str(&render_inline(child, ctx));
    }
    out
}

/// Render a single inline node. Every one of the 30 `Inline` arms is handled
/// explicitly — there is no catch-all.
pub fn render_inline(inline: &Inline, ctx: &Ctx) -> String {
    match inline {
        // 1. Text — escape the parsed text value.
        Inline::Text(t) => escape_text(&t.value),

        // 2. Escape — the single escaped char, text-escaped.
        Inline::Escape(e) => {
            let mut buf = [0u8; 4];
            escape_text(e.value.encode_utf8(&mut buf))
        }

        // 3. CharacterReference — `value` is the decoded scalar(s); re-escape.
        Inline::CharacterReference(c) => escape_text(&c.value),

        // 4. Emphasis.
        Inline::Emphasis(n) => format!("<em>{}</em>", render_inlines(&n.children, ctx)),

        // 5. Strong.
        Inline::Strong(n) => format!("<strong>{}</strong>", render_inlines(&n.children, ctx)),

        // 6. Underline (GFM `__x__`).
        Inline::Underline(n) => format!("<u>{}</u>", render_inlines(&n.children, ctx)),

        // 7. Delete — both markers render identically.
        Inline::Delete(n) => format!("<del>{}</del>", render_inlines(&n.children, ctx)),

        // 8. Insert (GFM `++x++`).
        Inline::Insert(n) => format!("<ins>{}</ins>", render_inlines(&n.children, ctx)),

        // 9. Mark (GFM `==x==`).
        Inline::Mark(n) => format!("<mark>{}</mark>", render_inlines(&n.children, ctx)),

        // 10. Subscript (GFM `~x~`).
        Inline::Subscript(n) => format!("<sub>{}</sub>", render_inlines(&n.children, ctx)),

        // 11. Superscript (GFM `^x^`, bare, no class).
        Inline::Superscript(n) => format!("<sup>{}</sup>", render_inlines(&n.children, ctx)),

        // 12. Spoiler (GFM `||x||`).
        Inline::Spoiler(n) => {
            format!(
                "<span class=\"spoiler\">{}</span>",
                render_inlines(&n.children, ctx)
            )
        }

        // 13. Shortcode — emoji glyph (gemoji), text-escaped, no wrapper.
        Inline::Shortcode(s) => escape_text(&emoji_glyph(&s.name)),

        // 14. Code — `value` already code-span-normalized; text-escape only.
        Inline::Code(c) => format!("<code>{}</code>", escape_text(&c.value)),

        // 15. Link.
        Inline::Link(n) => {
            let href = encode_href(&filter_protocol(
                &n.destination,
                ctx.allow_dangerous_protocol,
                ctx.gfm_url_denylist(),
            ));
            let title = title_attr(n.title.as_deref());
            format!(
                "<a href=\"{href}\"{title}>{}</a>",
                render_inlines(&n.children, ctx)
            )
        }

        // 16. Image.
        Inline::Image(n) => {
            let src = encode_href(&filter_img_protocol(
                &n.destination,
                ctx.allow_dangerous_protocol,
                ctx.allow_any_img_src,
            ));
            let alt = escaped_alt(&n.alt);
            let title = title_attr(n.title.as_deref());
            format!("<img src=\"{src}\" alt=\"{alt}\"{title} />")
        }

        // 17. LinkReference — resolve against the definition map.
        Inline::LinkReference(n) => match ctx.defs.resolve(&n.identifier) {
            Some(def) => {
                let href = encode_href(&filter_protocol(
                    &def.destination,
                    ctx.allow_dangerous_protocol,
                    ctx.gfm_url_denylist(),
                ));
                let title = title_attr(def.title.as_deref());
                format!(
                    "<a href=\"{href}\"{title}>{}</a>",
                    render_inlines(&n.children, ctx)
                )
            }
            None => link_reference_fallback(n, ctx),
        },

        // 18. ImageReference — resolve against the definition map.
        Inline::ImageReference(n) => match ctx.defs.resolve(&n.identifier) {
            Some(def) => {
                let src = encode_href(&filter_img_protocol(
                    &def.destination,
                    ctx.allow_dangerous_protocol,
                    ctx.allow_any_img_src,
                ));
                let alt = escaped_alt(&n.alt);
                let title = title_attr(def.title.as_deref());
                format!("<img src=\"{src}\" alt=\"{alt}\"{title} />")
            }
            None => image_reference_fallback(n),
        },

        // 19. Autolink.
        //   - Angle `<dest>`: an email-shaped destination (`@`, no `scheme:`)
        //     takes a synthesized `mailto:` href per CommonMark §6.5; the
        //     visible text is the destination (sans the synthesized prefix).
        //   - GFM literal: the destination is the already-synthesized href
        //     (e.g. `http://www…`, `mailto:…`) and may legally contain chars
        //     like `> [ ] { } | \ ^` and backtick, which `encode_href`
        //     percent-encodes; the visible text is the raw `original` source.
        Inline::Autolink(a) => match &a.kind {
            AutolinkKind::Angle => {
                let dest = autolink_href_dest(&a.destination);
                let href = encode_href(&filter_protocol(
                    &dest,
                    ctx.allow_dangerous_protocol,
                    ctx.gfm_url_denylist(),
                ));
                let text = escape_text(&visible_text(&a.destination));
                format!("<a href=\"{href}\">{text}</a>")
            }
            AutolinkKind::GfmLiteral { original } => {
                let href = encode_href(&filter_protocol(
                    &a.destination,
                    ctx.allow_dangerous_protocol,
                    ctx.gfm_url_denylist(),
                ));
                let text = escape_text(original);
                format!("<a href=\"{href}\">{text}</a>")
            }
        },

        // 20. Html — verbatim under danger (with tagfilter), else text-escape.
        Inline::Html(h) => render_raw_html(&h.value, ctx),

        // 21. SoftBreak.
        Inline::SoftBreak(_) => String::from("\n"),

        // 22. LineBreak — both kinds identical.
        Inline::LineBreak(_) => String::from("<br />\n"),

        // 23. Math (GFM form). A 2+-dollar fence is display, a 1-dollar fence is
        //     inline, and `$`…`$` code-math is an inline `<code>`.
        Inline::Math(m) => match m.kind {
            MathInlineKind::Code => format!(
                "<code data-math-style=\"inline\">{}</code>",
                escape_text(&m.value)
            ),
            MathInlineKind::Dollar { dollars } if dollars >= 2 => format!(
                "<span data-math-style=\"display\">{}</span>",
                escape_text(&m.value)
            ),
            MathInlineKind::Dollar { .. } => format!(
                "<span data-math-style=\"inline\">{}</span>",
                escape_text(&m.value)
            ),
        },

        // 24. FootnoteReference (GFM shape). An undefined reference renders
        //     as its literal `[^label]` source text.
        Inline::FootnoteReference(fr) => {
            if ctx.footnotes.is_defined(&fr.identifier) {
                footnote_marker(&fr.identifier, ctx)
            } else {
                format!("[^{}]", escape_text(&fr.label))
            }
        }

        // 25. InlineFootnote — renders like a footnote reference; its body was
        //     harvested into the doc-end section during the pre-pass.
        Inline::InlineFootnote(_) => {
            let id = footnotes::next_inline_id(ctx.footnotes);
            footnote_marker(&id, ctx)
        }

        // 26. WikiLink — GFM shape; both label orders identical output.
        Inline::WikiLink(w) => {
            let href = attr_escape_gfm(&encode_href(&w.target));
            format!(
                "<a href=\"{href}\" data-wikilink=\"true\">{}</a>",
                escape_text(&w.label)
            )
        }

        // 27. MDX expression (inline) — no HTML.
        Inline::MdxExpression(_) => String::new(),

        // 28. MDX JSX (inline) — no HTML (node carries no children).
        Inline::MdxJsx(_) => String::new(),

        // 29. TextDirective [CONV] — classed span carrying name + attrs.
        Inline::TextDirective(d) => {
            let attrs = directive_attrs(&d.attributes);
            format!(
                "<span class=\"directive directive-text\" data-directive-name=\"{}\"{attrs}>{}</span>",
                attr_escape(&d.name),
                render_inlines(&d.label, ctx)
            )
        }
    }
}

/// ` title="…"` when the title is `Some` (including empty `""` → drop, since
/// the parser only produces `Some("")` for an explicit empty title which the
/// oracles still drop). CommonMark/GFM both drop an empty title.
fn title_attr(title: Option<&str>) -> String {
    match title {
        Some(t) if !t.is_empty() => format!(" title=\"{}\"", attr_escape(t)),
        _ => String::new(),
    }
}

/// Synthesize the autolink href destination: an email-shaped destination
/// (contains `@` and has no `scheme:` prefix) gets a `mailto:` prefix; every
/// other destination is returned unchanged. The visible text is never altered.
fn autolink_href_dest(dest: &str) -> String {
    if dest.contains('@') && !has_uri_scheme(dest) {
        return format!("mailto:{dest}");
    }
    String::from(dest)
}

/// True when `dest` begins with a URI scheme (`scheme:` where scheme starts
/// with an ASCII letter followed by letters/digits/`+`/`.`/`-`).
fn has_uri_scheme(dest: &str) -> bool {
    let mut chars = dest.char_indices();
    match chars.next() {
        Some((_, c)) if c.is_ascii_alphabetic() => {}
        _ => return false,
    }
    for (_, c) in chars {
        if c == ':' {
            return true;
        }
        if !(c.is_ascii_alphanumeric() || c == '+' || c == '.' || c == '-') {
            return false;
        }
    }
    false
}

/// The GFM footnote reference marker `<sup class="footnote-ref">…`. The
/// `#fn-`/`fnref-` ids use the first definition's preserved-case label.
fn footnote_marker(id: &str, ctx: &Ctx) -> String {
    let (number, fnref) = footnotes::reference_marker(ctx.footnotes, id);
    let enc = footnotes::reference_fn_target(ctx.footnotes, id);
    format!(
        "<sup class=\"footnote-ref\"><a href=\"#fn-{enc}\" id=\"{fnref}\" data-footnote-ref>{number}</a></sup>",
    )
}

pub(super) fn render_raw_html(value: &str, ctx: &Ctx) -> String {
    if ctx.allow_dangerous_html {
        if ctx.gfm_tagfilter {
            return apply_tagfilter(value);
        }
        return String::from(value);
    }
    safe_raw_html(value, ctx)
}

/// Shared directive data-* attribute serializer.
pub(super) fn directive_attrs(attributes: &[DirectiveAttribute]) -> String {
    let mut out = String::new();
    for attr in attributes {
        let value = attr.value.as_deref().unwrap_or("");
        out.push_str(&format!(
            " data-{}=\"{}\"",
            attr_escape(&attr.name),
            attr_escape(value)
        ));
    }
    out
}

/// Safe-mode raw HTML: the gfm suite replaces it with a fixed placeholder; the
/// commonmark suite text-escapes it (oracle `html_flow` case 1 →
/// `&lt;!-- asd --&gt;`). Shared by the inline and block raw-HTML renderers.
pub(super) fn safe_raw_html(value: &str, ctx: &Ctx) -> String {
    match ctx.safe_raw_html_form {
        SafeRawHtmlForm::OmitPlaceholder => String::from(RAW_HTML_OMITTED),
        SafeRawHtmlForm::EscapeText => escape_text(value),
    }
}

/// The GFM safe-mode placeholder emitted in place of raw HTML.
pub(super) const RAW_HTML_OMITTED: &str = "<!-- raw HTML omitted -->";

/// GFM tagfilter: rewrite the leading `<` of a blocklisted open/close tag to
/// `&lt;`. Applied only when danger is on.
pub fn apply_tagfilter(value: &str) -> String {
    const BLOCKED: [&str; 9] = [
        "title",
        "textarea",
        "style",
        "xmp",
        "iframe",
        "noembed",
        "noframes",
        "script",
        "plaintext",
    ];
    let bytes = value.as_bytes();
    let mut out = String::with_capacity(value.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'<' {
            let after = &value[i + 1..];
            let tag_body = after.strip_prefix('/').unwrap_or(after);
            let matched = BLOCKED.iter().find(|tag| {
                tag_body.len() >= tag.len()
                    && tag_body[..tag.len()].eq_ignore_ascii_case(tag)
                    && tag_terminates(&tag_body[tag.len()..])
            });
            if matched.is_some() {
                out.push_str("&lt;");
                i += 1;
                continue;
            }
        }
        // Push one full UTF-8 char starting at i.
        let ch = value[i..].chars().next().unwrap();
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

/// True when the char after a blocklisted tag name terminates the tag name
/// (so `<scriptx>` is not filtered but `<script>`/`<script `/`<script/` are).
fn tag_terminates(rest: &str) -> bool {
    match rest.chars().next() {
        None => true,
        Some(c) => c == '>' || c == '/' || c.is_whitespace(),
    }
}

/// Literal-source fallback for an unresolved link reference: re-emit the
/// bracketed source text so the surrounding output is not silently dropped.
fn link_reference_fallback(n: &crate::ast::LinkReference, ctx: &Ctx) -> String {
    use crate::ast::ReferenceKind;
    let inner = render_inlines(&n.children, ctx);
    match n.kind {
        ReferenceKind::Shortcut => format!("[{inner}]"),
        ReferenceKind::Collapsed => format!("[{inner}][]"),
        ReferenceKind::Full => format!("[{inner}][{}]", escape_text(&n.label)),
    }
}

/// Literal-source fallback for an unresolved image reference.
fn image_reference_fallback(n: &crate::ast::ImageReference) -> String {
    use crate::ast::ReferenceKind;
    let inner = escape_text(&flatten_alt(&n.alt));
    match n.kind {
        ReferenceKind::Shortcut => format!("![{inner}]"),
        ReferenceKind::Collapsed => format!("![{inner}][]"),
        ReferenceKind::Full => format!("![{inner}][{}]", escape_text(&n.label)),
    }
}

/// Resolve a gemoji shortcode alias to its glyph. The table covers the aliases
/// exercised by the GFM `shortcodes` oracle; an unknown alias round-trips
/// to its `:name:` source form (deterministic and lossless).
fn emoji_glyph(name: &str) -> String {
    let glyph = match name {
        "smile" => "\u{1F604}",
        "+1" | "thumbsup" => "\u{1F44D}",
        "-1" | "thumbsdown" => "\u{1F44E}",
        "clock12" => "\u{1F55B}",
        "heart" => "\u{2764}\u{FE0F}",
        "tada" => "\u{1F389}",
        "rocket" => "\u{1F680}",
        "100" => "\u{1F4AF}",
        "x" => "\u{274C}",
        "1234" => "\u{1F522}",
        "1st_place_medal" => "\u{1F947}",
        "e-mail" => "\u{1F4E7}",
        "non-potable_water" => "\u{1F6B1}",
        _ => return format!(":{name}:"),
    };
    String::from(glyph)
}
