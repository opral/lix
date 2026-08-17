//! Faithful Rust port of the CommonMark spec test harness `normalize.py`
//! (commonmark/commonmark-spec `test/normalize.py`).
//!
//! Both the oracle's expected HTML and our renderer's output are passed through
//! the SAME normalization before comparison, so the comparison ignores only
//! the differences the CommonMark project itself deems insignificant:
//!
//! * runs of whitespace collapse to one space (outside `<pre>`);
//! * whitespace adjacent to BLOCK-level tags is stripped (inline-adjacent
//!   whitespace is preserved — `a <em>b</em>` keeps its space);
//! * self-closing tags become open tags (`<br />` → `<br>`);
//! * attributes are lowercased + sorted; `href`/`src` values are URL-canonicalized
//!   (`quote(unquote(v), safe='/')`); other values are `html.escape`d;
//! * character/entity references decode to their character, except `< > & "`
//!   which re-emit as entities; unknown refs pass through verbatim;
//! * `<pre>` content is preserved byte-for-byte.
//!
//! Structure (tag names, nesting, attribute presence/value, intra-`<pre>` bytes,
//! entity output) is preserved, so real defects still surface. This is the
//! comparison the official CommonMark suite uses.

#[derive(Clone, Copy, PartialEq, Eq)]
enum Last {
    StartTag,
    EndTag,
    Data,
    Other,
}

struct Normalizer {
    last: Last,
    last_tag: String,
    in_pre: bool,
    output: String,
}

const BLOCK_TAGS: &[&str] = &[
    "article",
    "header",
    "aside",
    "hgroup",
    "blockquote",
    "hr",
    "iframe",
    "body",
    "li",
    "map",
    "button",
    "object",
    "canvas",
    "ol",
    "caption",
    "output",
    "col",
    "p",
    "colgroup",
    "pre",
    "dd",
    "progress",
    "div",
    "section",
    "dl",
    "table",
    "td",
    "dt",
    "tbody",
    "embed",
    "textarea",
    "fieldset",
    "tfoot",
    "figcaption",
    "th",
    "figure",
    "thead",
    "footer",
    "tr",
    "form",
    "ul",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "video",
    "script",
    "style",
];

fn is_block_tag(tag: &str) -> bool {
    BLOCK_TAGS.contains(&tag)
}

/// `html.escape(v, quote=True)`: & < > " '  → entities.
fn html_escape_attr(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#x27;"),
            _ => out.push(c),
        }
    }
    out
}

/// CPython's always-safe set for `urllib.parse.quote`: ALPHA DIGIT `_.-~`.
fn is_url_always_safe(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-' | b'~')
}

/// `urllib.parse.unquote(v)`: decode `%XX` byte sequences, interpret as UTF-8
/// (invalid sequences are replaced, matching Python's `errors='replace'`).
fn url_unquote(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut decoded: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(h), Some(l)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2])) {
                decoded.push((h << 4) | l);
                i += 3;
                continue;
            }
        }
        decoded.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&decoded).into_owned()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// `urllib.parse.quote(s, safe='/')` over UTF-8 bytes.
fn url_quote_safe_slash(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for &b in value.as_bytes() {
        if is_url_always_safe(b) || b == b'/' {
            out.push(b as char);
        } else {
            out.push('%');
            out.push(hex_upper(b >> 4));
            out.push(hex_upper(b & 0xF));
        }
    }
    out
}

fn hex_upper(nibble: u8) -> char {
    match nibble {
        0..=9 => (b'0' + nibble) as char,
        _ => (b'A' + nibble - 10) as char,
    }
}

/// Decode the 5 standard named entities + numeric refs that appear in rendered
/// HTML attribute values / data. Unknown named entities yield `None` (the
/// caller keeps them verbatim, matching `normalize.py`'s KeyError fallback).
fn decode_entity(name: &str) -> Option<char> {
    if let Some(rest) = name.strip_prefix('#') {
        let code = if let Some(hex) = rest.strip_prefix(['x', 'X']) {
            u32::from_str_radix(hex, 16).ok()?
        } else {
            rest.parse::<u32>().ok()?
        };
        return char::from_u32(code);
    }
    match name {
        "amp" => Some('&'),
        "lt" => Some('<'),
        "gt" => Some('>'),
        "quot" => Some('"'),
        "apos" => Some('\''),
        _ => None,
    }
}

/// Unescape entities inside an HTML attribute value (HTMLParser behavior).
fn unescape_attr_value(value: &str) -> String {
    let chars: Vec<char> = value.chars().collect();
    let mut out = String::with_capacity(value.len());
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '&' {
            if let Some((decoded, consumed)) = try_match_entity(&chars[i..]) {
                match decoded {
                    Some(c) => out.push(c),
                    None => out.extend(&chars[i..i + consumed]),
                }
                i += consumed;
                continue;
            }
        }
        out.push(chars[i]);
        i += 1;
    }
    out
}

/// At `chars[0] == '&'`, try to match `&name;`. Returns `(decoded, consumed)`
/// where `consumed` includes the leading `&` and trailing `;`.
fn try_match_entity(chars: &[char]) -> Option<(Option<char>, usize)> {
    debug_assert_eq!(chars[0], '&');
    let mut j = 1;
    // body of the reference name
    if j < chars.len() && chars[j] == '#' {
        j += 1;
        if j < chars.len() && (chars[j] == 'x' || chars[j] == 'X') {
            j += 1;
            let start = j;
            while j < chars.len() && chars[j].is_ascii_hexdigit() {
                j += 1;
            }
            if j == start {
                return None;
            }
        } else {
            let start = j;
            while j < chars.len() && chars[j].is_ascii_digit() {
                j += 1;
            }
            if j == start {
                return None;
            }
        }
    } else {
        let start = j;
        while j < chars.len() && chars[j].is_ascii_alphanumeric() {
            j += 1;
        }
        if j == start {
            return None;
        }
    }
    if j < chars.len() && chars[j] == ';' {
        let name: String = chars[1..j].iter().collect();
        return Some((decode_entity(&name), j + 1));
    }
    None
}

impl Normalizer {
    fn new() -> Self {
        Self {
            last: Last::StartTag,
            last_tag: String::new(),
            in_pre: false,
            output: String::new(),
        }
    }

    /// `output_char`: decoded char, with `< > & "` re-emitted as entities and
    /// unknown refs passed through via `fallback`.
    fn output_char(&mut self, decoded: Option<char>, fallback: &str) {
        match decoded {
            Some('<') => self.output.push_str("&lt;"),
            Some('>') => self.output.push_str("&gt;"),
            Some('&') => self.output.push_str("&amp;"),
            Some('"') => self.output.push_str("&quot;"),
            Some(c) => self.output.push(c),
            None => self.output.push_str(fallback),
        }
        self.last = Last::Other;
    }

    /// `handle_data` for a pure-text segment (no entities).
    fn handle_data(&mut self, data: &str) {
        let after_tag = matches!(self.last, Last::StartTag | Last::EndTag);
        let after_block_tag = after_tag && is_block_tag(&self.last_tag);

        let mut text = data.to_string();
        if after_tag && self.last_tag == "br" {
            text = text.trim_start_matches('\n').to_string();
        }
        if !self.in_pre {
            text = collapse_whitespace(&text);
        }
        if after_block_tag && !self.in_pre {
            text = match self.last {
                Last::StartTag => text.trim_start().to_string(),
                Last::EndTag => text.trim().to_string(),
                _ => text,
            };
        }
        self.output.push_str(&text);
        self.last = Last::Data;
    }

    /// Process a `[^<]+` text chunk, splitting entities out like HTMLParser.
    fn handle_text_chunk(&mut self, chunk: &str) {
        let chars: Vec<char> = chunk.chars().collect();
        let mut buf = String::new();
        let mut i = 0;
        while i < chars.len() {
            if chars[i] == '&' {
                if let Some((decoded, consumed)) = try_match_entity(&chars[i..]) {
                    if !buf.is_empty() {
                        self.handle_data(&buf);
                        buf.clear();
                    }
                    let fallback: String = chars[i..i + consumed].iter().collect();
                    self.output_char(decoded, &fallback);
                    i += consumed;
                    continue;
                }
            }
            buf.push(chars[i]);
            i += 1;
        }
        if !buf.is_empty() {
            self.handle_data(&buf);
        }
    }

    fn handle_starttag(
        &mut self,
        tag: &str,
        attrs: &[(String, Option<String>)],
        self_closing: bool,
    ) {
        if tag == "pre" {
            self.in_pre = true;
        }
        if is_block_tag(tag) {
            let trimmed = self.output.trim_end().to_string();
            self.output = trimmed;
        }
        self.output.push('<');
        self.output.push_str(tag);
        if !attrs.is_empty() {
            let mut sorted = attrs.to_vec();
            sorted.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
            for (k, v) in &sorted {
                self.output.push(' ');
                self.output.push_str(k);
                if let Some(v) = v {
                    if k == "href" || k == "src" {
                        self.output.push_str("=\"");
                        self.output.push_str(&url_quote_safe_slash(&url_unquote(v)));
                        self.output.push('"');
                    } else {
                        self.output.push_str("=\"");
                        self.output.push_str(&html_escape_attr(v));
                        self.output.push('"');
                    }
                }
            }
        }
        self.output.push('>');
        self.last_tag = tag.to_string();
        self.last = Last::StartTag;
        if self_closing {
            // handle_startendtag: emit as start tag, then mark as endtag.
            self.last = Last::EndTag;
        }
    }

    fn handle_endtag(&mut self, tag: &str) {
        if tag == "pre" {
            self.in_pre = false;
        } else if is_block_tag(tag) {
            let trimmed = self.output.trim_end().to_string();
            self.output = trimmed;
        }
        self.output.push_str("</");
        self.output.push_str(tag);
        self.output.push('>');
        self.last_tag = tag.to_string();
        self.last = Last::EndTag;
    }

    fn handle_verbatim(&mut self, raw: &str) {
        // comments, declarations, processing instructions, CDATA
        self.output.push_str(raw);
        self.last = Last::Other;
    }
}

fn collapse_whitespace(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut in_ws = false;
    for c in s.chars() {
        if c.is_ascii_whitespace() {
            if !in_ws {
                out.push(' ');
                in_ws = true;
            }
        } else {
            out.push(c);
            in_ws = false;
        }
    }
    out
}

/// Public entry: normalize an HTML string for conformance comparison.
pub fn normalize_html(input: &str) -> String {
    let mut p = Normalizer::new();
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '<' {
            if let Some(consumed) = scan_construct(&chars[i..], &mut p) {
                i += consumed;
                continue;
            }
            // stray '<' — treat as data
            p.handle_text_chunk("<");
            i += 1;
            continue;
        }
        // text run until next '<'
        let start = i;
        while i < chars.len() && chars[i] != '<' {
            i += 1;
        }
        let chunk: String = chars[start..i].iter().collect();
        p.handle_text_chunk(&chunk);
    }
    p.output
}

/// At `chars[0] == '<'`, try to consume one HTML construct. Returns the number
/// of chars consumed, or `None` if `<` does not start a recognized construct.
fn scan_construct(chars: &[char], p: &mut Normalizer) -> Option<usize> {
    let starts_with = |pat: &str| {
        chars
            .iter()
            .take(pat.chars().count())
            .copied()
            .eq(pat.chars())
    };

    if starts_with("<!--") {
        // comment: up to "-->"
        let end = find_subseq(chars, &['-', '-', '>'], 4);
        let stop = end.map(|e| e + 3).unwrap_or(chars.len());
        let raw: String = chars[..stop].iter().collect();
        p.handle_verbatim(&raw);
        return Some(stop);
    }
    if starts_with("<![CDATA[") {
        let end = find_subseq(chars, &[']', ']', '>'], 9);
        let stop = end.map(|e| e + 3).unwrap_or(chars.len());
        let raw: String = chars[..stop].iter().collect();
        p.handle_verbatim(&raw); // CDATA passed through verbatim
        return Some(stop);
    }
    if starts_with("<!") || starts_with("<?") {
        // declaration / PI: up to '>'
        let stop = chars.iter().position(|&c| c == '>').map(|e| e + 1)?;
        let raw: String = chars[..stop].iter().collect();
        p.handle_verbatim(&raw);
        return Some(stop);
    }

    // start or end tag: <[/]name ...>
    let is_end = chars.len() > 1 && chars[1] == '/';
    let name_start = if is_end { 2 } else { 1 };
    if name_start >= chars.len() || !chars[name_start].is_ascii_alphabetic() {
        return None;
    }
    // find the closing '>' respecting quotes
    let mut j = name_start;
    let mut quote: Option<char> = None;
    while j < chars.len() {
        let c = chars[j];
        match quote {
            Some(q) => {
                if c == q {
                    quote = None;
                }
            }
            None => {
                if c == '"' || c == '\'' {
                    quote = Some(c);
                } else if c == '>' {
                    break;
                }
            }
        }
        j += 1;
    }
    if j >= chars.len() {
        return None; // unterminated tag
    }
    let inner: String = chars[name_start..j].iter().collect();
    let consumed = j + 1;

    if is_end {
        let tag = parse_tag_name(&inner);
        p.handle_endtag(&tag);
        return Some(consumed);
    }
    let (tag, attrs, self_closing) = parse_start_tag(&inner);
    p.handle_starttag(&tag, &attrs, self_closing);
    Some(consumed)
}

fn find_subseq(chars: &[char], needle: &[char], from: usize) -> Option<usize> {
    if needle.is_empty() || chars.len() < needle.len() {
        return None;
    }
    let mut i = from;
    while i + needle.len() <= chars.len() {
        if chars[i..i + needle.len()] == *needle {
            return Some(i);
        }
        i += 1;
    }
    None
}

fn parse_tag_name(inner: &str) -> String {
    inner
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '.' | ':' | '_'))
        .collect::<String>()
        .to_ascii_lowercase()
}

fn parse_start_tag(inner: &str) -> (String, Vec<(String, Option<String>)>, bool) {
    let chars: Vec<char> = inner.chars().collect();
    let mut i = 0;
    // tag name
    let name_start = i;
    while i < chars.len()
        && (chars[i].is_ascii_alphanumeric() || matches!(chars[i], '-' | '.' | ':' | '_'))
    {
        i += 1;
    }
    let tag: String = chars[name_start..i]
        .iter()
        .collect::<String>()
        .to_ascii_lowercase();

    let mut attrs: Vec<(String, Option<String>)> = Vec::new();
    let mut self_closing = false;
    while i < chars.len() {
        // skip whitespace
        while i < chars.len() && chars[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= chars.len() {
            break;
        }
        if chars[i] == '/' {
            self_closing = true;
            i += 1;
            continue;
        }
        // attribute name
        let astart = i;
        while i < chars.len()
            && !chars[i].is_ascii_whitespace()
            && chars[i] != '='
            && chars[i] != '/'
            && chars[i] != '>'
        {
            i += 1;
        }
        if i == astart {
            // stray char (e.g. lone '='); skip to avoid infinite loop
            i += 1;
            continue;
        }
        let aname: String = chars[astart..i]
            .iter()
            .collect::<String>()
            .to_ascii_lowercase();
        // optional '= value'
        while i < chars.len() && chars[i].is_ascii_whitespace() {
            i += 1;
        }
        if i < chars.len() && chars[i] == '=' {
            i += 1;
            while i < chars.len() && chars[i].is_ascii_whitespace() {
                i += 1;
            }
            let value = if i < chars.len() && (chars[i] == '"' || chars[i] == '\'') {
                let q = chars[i];
                i += 1;
                let vstart = i;
                while i < chars.len() && chars[i] != q {
                    i += 1;
                }
                let v: String = chars[vstart..i].iter().collect();
                if i < chars.len() {
                    i += 1; // closing quote
                }
                v
            } else {
                let vstart = i;
                while i < chars.len() && !chars[i].is_ascii_whitespace() && chars[i] != '>' {
                    i += 1;
                }
                chars[vstart..i].iter().collect()
            };
            attrs.push((aname, Some(unescape_attr_value(&value))));
        } else {
            attrs.push((aname, None));
        }
    }
    (tag, attrs, self_closing)
}

/// Trim the document edge so an oracle's trailing-newline convention does not
/// count as a structural difference. Document-level trailing whitespace is
/// never significant; interior whitespace is untouched.
pub fn trim_doc_edge(s: &str) -> &str {
    s.trim_end_matches(['\n', '\r', ' ', '\t'])
}

/// The two equality verdicts the runner records per case.
#[derive(Clone, Copy, Debug)]
pub struct Comparison {
    /// Byte-equal after only trimming the document edge (strictest).
    pub raw_match: bool,
    /// Equal after full CommonMark normalization (the headline metric).
    pub normalized_match: bool,
}

pub fn compare(rendered: &str, expected: &str) -> Comparison {
    let raw_match = trim_doc_edge(rendered) == trim_doc_edge(expected);
    let normalized_match =
        normalize_html(trim_doc_edge(rendered)) == normalize_html(trim_doc_edge(expected));
    Comparison {
        raw_match,
        normalized_match,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collapses_whitespace_outside_pre() {
        assert_eq!(normalize_html("<p>a  \t b</p>"), "<p>a b</p>");
        assert_eq!(normalize_html("<p>a  \t\nb</p>"), "<p>a b</p>");
    }

    #[test]
    fn strips_whitespace_around_block_tags_only() {
        assert_eq!(normalize_html(" <p>a  b</p>"), "<p>a b</p>");
        assert_eq!(normalize_html("<p>a  b</p> "), "<p>a b</p>");
        assert_eq!(
            normalize_html("\n\t<p>\n\t\ta  b\t\t</p>\n\t"),
            "<p>a b</p>"
        );
    }

    #[test]
    fn preserves_inline_adjacent_whitespace() {
        // The classic masking trap: the space in `a <em>b</em>` is significant.
        assert_eq!(normalize_html("<i>a  b</i> "), "<i>a b</i> ");
        assert_eq!(normalize_html("<p>a <em>b</em></p>"), "<p>a <em>b</em></p>");
    }

    #[test]
    fn self_closing_to_open() {
        assert_eq!(normalize_html("<br />"), "<br>");
    }

    #[test]
    fn sorts_and_lowercases_attributes() {
        assert_eq!(
            normalize_html("<a title=\"bar\" HREF=\"foo\">x</a>"),
            "<a href=\"foo\" title=\"bar\">x</a>"
        );
    }

    #[test]
    fn entities_decoded_except_markup_chars() {
        // Both upstream oracles DECODE entities to literal characters at render
        // time, so rendered/expected HTML only ever contains the four
        // markup-significant escapes plus numeric refs — never arbitrary named
        // entities like `&forall;`. So we decode numeric refs + re-emit the four
        // markup chars as entities; a full named-entity table is unnecessary.
        assert_eq!(normalize_html("&amp;&gt;&lt;&quot;"), "&amp;&gt;&lt;&quot;");
        assert_eq!(normalize_html("&#x2200;"), "\u{2200}");
        assert_eq!(normalize_html("&#35;"), "#");
        assert_eq!(normalize_html("&#38;&#60;"), "&amp;&lt;"); // numeric markup re-entified
    }

    #[test]
    fn pre_content_is_byte_exact() {
        assert_eq!(
            normalize_html("<pre><code>a  b\n  c\n</code></pre>"),
            "<pre><code>a  b\n  c\n</code></pre>"
        );
    }

    #[test]
    fn idempotent() {
        for s in [
            "<p>a  b</p>\n",
            "<ul>\n<li>x</li>\n</ul>\n",
            "<pre><code>a  b\n</code></pre>\n",
            "<a href=\"x y\">z</a>",
            "&amp;&copy;&#35;",
        ] {
            let once = normalize_html(s);
            assert_eq!(normalize_html(&once), once, "not idempotent: {s:?}");
        }
    }

    #[test]
    fn negative_fixtures_still_fail_after_normalization() {
        // Anti-masking guard: real structural defects must NOT be erased.
        assert!(!compare("<p><em>x</em></p>", "<p><strong>x</strong></p>").normalized_match);
        assert!(!compare("<p>ab</p>", "<p>a b</p>").normalized_match); // word-merge
        assert!(!compare("<a>x</a>", "<a href=\"u\">x</a>").normalized_match); // dropped attr
        assert!(
            !compare(
                "<pre><code>a b</code></pre>",
                "<pre><code>a  b</code></pre>"
            )
            .normalized_match
        ); // pre ws
        assert!(!compare("<p>&amp;</p>", "<p>&</p>").normalized_match); // entity vs raw... both decode? check
    }

    #[test]
    fn trailing_newline_reconciled() {
        assert!(compare("<p>x</p>", "<p>x</p>\n").raw_match);
        assert!(compare("<p>x</p>\n", "<p>x</p>").normalized_match);
    }
}
