//! Escaping + URL-encoding helpers shared by the inline/block renderers.
//!
//! Inputs are already-decoded Rust strings; the renderer never re-decodes or
//! re-trims them.

use alloc::string::String;

/// CommonMark text escaper: replace, IN ORDER, `&`→`&amp;`, `<`→`&lt;`,
/// `>`→`&gt;`, `"`→`&quot;`. The single quote is left literal.
pub fn escape_text(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            _ => out.push(ch),
        }
    }
    out
}

/// Attribute escaper for the CommonMark / link-title context: identical to
/// [`escape_text`] (`&` `<` `>` `"`), with `'` left literal because the
/// attribute is always emitted double-quoted.
pub fn attr_escape(s: &str) -> String {
    escape_text(s)
}

/// Attribute escaper for the GFM directive / wikilink context: same as
/// [`attr_escape`] but additionally maps `'`→`&#x27;`.
pub fn attr_escape_gfm(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#x27;"),
            _ => out.push(ch),
        }
    }
    out
}

/// True for the cmark "href-safe" byte set kept literal by the houdini
/// encoder: `A-Z a-z 0-9 ! # $ ' ( ) * + , - . / : ; = ? @ _ ~`.
fn is_href_safe(b: u8) -> bool {
    matches!(b,
        b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9'
        | b'!' | b'#' | b'$' | b'\'' | b'(' | b')' | b'*' | b'+'
        | b',' | b'-' | b'.' | b'/' | b':' | b';' | b'=' | b'?'
        | b'@' | b'_' | b'~')
}

fn is_ascii_hex(b: u8) -> bool {
    b.is_ascii_hexdigit()
}

/// cmark houdini href encoder over the UTF-8 bytes of the (already-decoded)
/// destination. `&`→`&amp;`; `%`+2hex preserved else `%25`; href-safe bytes
/// literal; everything else `%XX` uppercase. (See `misc_url.rs:141`.)
pub fn encode_href(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'&' {
            out.push_str("&amp;");
            i += 1;
        } else if b == b'%' {
            if i + 2 < bytes.len() && is_ascii_hex(bytes[i + 1]) && is_ascii_hex(bytes[i + 2]) {
                out.push('%');
            } else {
                out.push_str("%25");
            }
            i += 1;
        } else if is_href_safe(b) {
            out.push(b as char);
            i += 1;
        } else {
            push_percent(&mut out, b);
            i += 1;
        }
    }
    out
}

fn push_percent(out: &mut String, b: u8) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    out.push('%');
    out.push(HEX[(b >> 4) as usize] as char);
    out.push(HEX[(b & 0x0f) as usize] as char);
}

/// Lowercase ASCII scheme of `dest`: the chars before the first `:` that
/// occurs before any of `/ ? #`. `None` when there is no such scheme.
fn url_scheme(dest: &str) -> Option<String> {
    let mut scheme = String::new();
    for ch in dest.chars() {
        match ch {
            ':' => return Some(scheme.to_ascii_lowercase()),
            '/' | '?' | '#' => return None,
            _ => scheme.push(ch),
        }
    }
    None
}

/// Link href scheme DENYLIST — cmark-gfm's `scan_dangerous_url` policy (used for
/// the GFM suite, which is cmark-gfm-derived): a URL is dropped only when its
/// scheme is one cmark-gfm treats as dangerous (`javascript`, `vbscript`,
/// `file`, `data`); the four safe `data:image/*` prefixes are carved out
/// separately by `is_allowed_data_image`. Every other scheme (`http`, `ftp`,
/// `smb`, `irc`, `rdar`, …, and the empty scheme from a leading `://`) is kept.
fn is_dangerous_scheme(scheme: &str) -> bool {
    matches!(scheme, "javascript" | "vbscript" | "file" | "data")
}

/// Link href scheme ALLOWLIST — micromark's policy (used for the CommonMark
/// suite): a URL is kept only when its scheme is one of the safe set (or it is a
/// safe `data:image/*` URI). Unknown schemes (`made-up-scheme:`, `a+b+c:`) are
/// blanked unless `allow_dangerous_protocol` is set. The CommonMark and GFM
/// suites genuinely disagree here (cmark-gfm keeps `smb:`; micromark blanks
/// `made-up-scheme:`), so the policy is selected per suite category.
fn is_allowed_link_scheme(scheme: &str) -> bool {
    matches!(
        scheme,
        "http" | "https" | "irc" | "ircs" | "mailto" | "xmpp"
    )
}

/// Image src scheme allowlist (only http/https; irc/mailto are NOT allowed for
/// images).
fn is_allowed_img_scheme(scheme: &str) -> bool {
    matches!(scheme, "http" | "https")
}

/// The four `data:image/*` URI prefixes the GFM sanitizer allows.
fn is_allowed_data_image(dest: &str) -> bool {
    let lower = dest.to_ascii_lowercase();
    lower.starts_with("data:image/png")
        || lower.starts_with("data:image/gif")
        || lower.starts_with("data:image/jpeg")
        || lower.starts_with("data:image/webp")
}

/// Link href protocol filter. `gfm_denylist` selects the suite's policy: the
/// GFM (cmark-gfm) suite blanks only dangerous schemes; the CommonMark
/// (micromark) suite blanks any non-allowlisted scheme. `allow_dangerous_protocol`
/// bypasses both (keeps everything). The two oracles genuinely differ (cmark-gfm
/// keeps `smb:`; micromark blanks `made-up-scheme:`), so this is category-keyed
/// alongside the other category-divergent oracle conventions.
pub fn filter_protocol(dest: &str, allow_dangerous_protocol: bool, gfm_denylist: bool) -> String {
    if allow_dangerous_protocol {
        return String::from(dest);
    }
    match url_scheme(dest) {
        None => String::from(dest),
        Some(scheme) if gfm_denylist => {
            if is_dangerous_scheme(&scheme) && !is_allowed_data_image(dest) {
                String::new()
            } else {
                String::from(dest)
            }
        }
        Some(scheme) => {
            if is_allowed_link_scheme(&scheme) || is_allowed_data_image(dest) {
                String::from(dest)
            } else {
                String::new()
            }
        }
    }
}

/// Image src protocol filter (ALLOWLIST). `allow_dangerous_protocol` and
/// `allow_any_img_src` both bypass the filter (mirroring the link href path and
/// upstream micromark `allowDangerousProtocol` semantics, which apply to image
/// src as well); otherwise a scheme-less destination passes, a schemed
/// destination passes only for http/https, and the four `data:image/*` URIs are
/// allowed.
pub fn filter_img_protocol(
    dest: &str,
    allow_dangerous_protocol: bool,
    allow_any_img_src: bool,
) -> String {
    if allow_dangerous_protocol || allow_any_img_src {
        return String::from(dest);
    }
    match url_scheme(dest) {
        None => String::from(dest),
        Some(scheme) if is_allowed_img_scheme(&scheme) => String::from(dest),
        Some(_) if is_allowed_data_image(dest) => String::from(dest),
        Some(_) => String::new(),
    }
}
