//! Canonical Lix filesystem paths live in this module.
//!
//! Contract:
//!
//! - Canonical internal form is an RFC 3987 `ipath-absolute` IRI path.
//! - The engine stores an internal IRI, not a WHATWG URL.
//! - ASCII-only URI spelling is a boundary serialization, not the internal form.
//! - Unicode is normalized with UAX #15 NFC.
//! - Canonicalization uses RFC 3986 rules for dot-segment removal and
//!   percent-encoding normalization.
//!
//! Canonicalization order:
//!
//! 1. Normalize raw input to NFC.
//! 2. Validate the normalized form and percent-encoding structure.
//! 3. Apply percent-encoding normalization.
//! 4. Remove dot segments.
//!
//! Fixed RFC-derived rules:
//!
//! - Dot segments are removed before storage per RFC 3986 §5.2.4.
//! - Percent triplets use uppercase hex digits per RFC 3986 §6.2.2.1.
//! - Percent-encoded unreserved characters are decoded to raw form per
//!   RFC 3986 §6.2.2.2.
//! - Comparison is exact-string and case-sensitive after canonicalization.
//!
//! Lix profile rules:
//!
//! - File paths never end with `/`.
//! - Directory paths always end with `/`.
//! - `NUL` is rejected in all segments.
//! - Root is represented as the normalized directory path `/`.
//! - Git/CLI import and ASCII-only URI serialization are boundary adapters,
//!   not part of the core `fs_path` contract.
//!
//! Length policy:
//!
//! - Paths are unbounded at the `fs_path` spec layer.
//!
//! Runtime strategy:
//!
//! - This module keeps a small engine-local validator/normalizer at runtime.
//! - `iref` is the RFC 3987 / RFC 3986 oracle in tests, not the runtime parser.
//!
//! Glossary:
//!
//! - Raw input path: caller-provided path before normalization.
//! - Normalized path: path after NFC normalization.
//! - Canonical path: stored path after full normalization/canonicalization.
//! - File path: canonical path naming a file, without a trailing slash.
//! - Directory path: canonical path naming a directory, with a trailing slash.
//! - Internal IRI form: the canonical Unicode-bearing representation used by
//!   the engine.
//! - Boundary URI form: an ASCII-only serialization used when interoperating
//!   with URI-only systems.
//!
//! This module is being aligned to this contract in Plan 119.

use unicode_normalization::UnicodeNormalization;

use crate::LixError;
use std::fmt;
use std::ops::Deref;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NormalizedDirectoryPath(String);

impl NormalizedDirectoryPath {
    pub(crate) fn try_from_path(path: &str) -> Result<Self, LixError> {
        normalize_directory_path(path).map(Self)
    }

    pub(crate) fn root() -> Self {
        Self("/".to_string())
    }

    pub(crate) fn from_normalized(path: String) -> Self {
        Self(path)
    }

    pub(crate) fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub(crate) fn is_root(&self) -> bool {
        self.as_str() == "/"
    }
}

impl Deref for NormalizedDirectoryPath {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl fmt::Display for NormalizedDirectoryPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NormalizedFilePath(String);

impl NormalizedFilePath {
    pub(crate) fn from_normalized(path: String) -> Self {
        Self(path)
    }

    pub(crate) fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl Deref for NormalizedFilePath {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl fmt::Display for NormalizedFilePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParsedFilePath {
    pub(crate) normalized_path: NormalizedFilePath,
    pub(crate) directory_path: Option<NormalizedDirectoryPath>,
    pub(crate) name: String,
    pub(crate) extension: Option<String>,
}

impl ParsedFilePath {
    pub(crate) fn try_from_path(path: &str) -> Result<Self, LixError> {
        parse_file_path(path)
    }

    pub(crate) fn from_normalized_path(path: String) -> Result<Self, LixError> {
        parse_file_path(&path)
    }
}

pub(crate) fn normalize_path_segment(raw: &str) -> Result<String, LixError> {
    let normalized = raw.nfc().collect::<String>();
    let canonical = normalize_validated_path_segment(&normalized)?;
    if canonical == "." || canonical == ".." {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "path segment cannot be '.' or '..'".to_string(),
        });
    }
    Ok(canonical)
}

fn validate_path_segment_chars(normalized: &str) -> Result<(), LixError> {
    if normalized.is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "path segment must not be empty".to_string(),
        });
    }
    if normalized.contains('/') || normalized.contains('\\') {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "path segment must not contain slashes".to_string(),
        });
    }
    if !segment_has_valid_percent_encoding(&normalized) {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "path segment contains invalid percent encoding".to_string(),
        });
    }
    if normalized.chars().any(is_disallowed_bidi_formatting_char) {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "path segment contains disallowed bidi formatting characters".to_string(),
        });
    }
    if !normalized.chars().all(is_allowed_segment_char) {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "path segment contains unsupported characters".to_string(),
        });
    }
    Ok(())
}

fn normalize_validated_path_segment(normalized: &str) -> Result<String, LixError> {
    validate_path_segment_chars(normalized)?;
    Ok(canonicalize_percent_encoding(normalized))
}

fn is_allowed_segment_char(ch: char) -> bool {
    is_pchar_ascii(ch) || is_iunreserved_ucschar(ch)
}

fn is_unreserved(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '-' | '.' | '_' | '~')
}

fn is_sub_delim(ch: char) -> bool {
    matches!(
        ch,
        '!' | '$' | '&' | '\'' | '(' | ')' | '*' | '+' | ',' | ';' | '='
    )
}

fn is_pchar_ascii(ch: char) -> bool {
    is_unreserved(ch) || is_sub_delim(ch) || matches!(ch, ':' | '@' | '%')
}

fn is_iunreserved_ucschar(ch: char) -> bool {
    let cp = ch as u32;
    matches!(
        cp,
        0x00A0..=0xD7FF
            | 0xF900..=0xFDCF
            | 0xFDF0..=0xFFEF
            | 0x10000..=0x1FFFD
            | 0x20000..=0x2FFFD
            | 0x30000..=0x3FFFD
            | 0x40000..=0x4FFFD
            | 0x50000..=0x5FFFD
            | 0x60000..=0x6FFFD
            | 0x70000..=0x7FFFD
            | 0x80000..=0x8FFFD
            | 0x90000..=0x9FFFD
            | 0xA0000..=0xAFFFD
            | 0xB0000..=0xBFFFD
            | 0xC0000..=0xCFFFD
            | 0xD0000..=0xDFFFD
            | 0xE1000..=0xEFFFD
    )
}

fn is_disallowed_bidi_formatting_char(ch: char) -> bool {
    matches!(
        ch,
        '\u{200E}'
            | '\u{200F}'
            | '\u{202A}'
            | '\u{202B}'
            | '\u{202C}'
            | '\u{202D}'
            | '\u{202E}'
    )
}

fn canonicalize_percent_encoding(segment: &str) -> String {
    let bytes = segment.as_bytes();
    let mut normalized = String::with_capacity(segment.len());
    let mut index = 0usize;

    while index < bytes.len() {
        if bytes[index] == b'%' {
            let hi = bytes[index + 1];
            let lo = bytes[index + 2];
            let decoded = (hex_value(hi) << 4) | hex_value(lo);
            let decoded_char = decoded as char;

            if is_unreserved(decoded_char) {
                normalized.push(decoded_char);
            } else {
                normalized.push('%');
                normalized.push(upper_hex_digit(hi));
                normalized.push(upper_hex_digit(lo));
            }

            index += 3;
            continue;
        }

        let ch = segment[index..]
            .chars()
            .next()
            .expect("slice at char boundary should yield a char");
        normalized.push(ch);
        index += ch.len_utf8();
    }

    normalized
}

fn hex_value(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => 10 + (byte - b'a'),
        b'A'..=b'F' => 10 + (byte - b'A'),
        _ => unreachable!("hex_value only called after percent validation"),
    }
}

fn upper_hex_digit(byte: u8) -> char {
    (byte as char).to_ascii_uppercase()
}

fn segment_has_valid_percent_encoding(segment: &str) -> bool {
    let bytes = segment.as_bytes();
    let mut index = 0usize;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len() {
                return false;
            }
            let hi = bytes[index + 1];
            let lo = bytes[index + 2];
            if !hi.is_ascii_hexdigit() || !lo.is_ascii_hexdigit() {
                return false;
            }
            index += 3;
            continue;
        }
        index += 1;
    }
    true
}

pub(crate) fn normalize_file_path(path: &str) -> Result<String, LixError> {
    let normalized = path.nfc().collect::<String>();
    if !normalized.starts_with('/') {
        return Err(LixError {
            code: "LIX_ERROR_INVALID_FILE_PATH".to_string(),
            description: format!(
                "file paths must start with '/'. Got '{path}', use '/{path}' instead"
            ),
        });
    }
    if normalized.ends_with('/') || normalized == "/" {
        return Err(LixError {
            code: "LIX_ERROR_INVALID_FILE_PATH".to_string(),
            description: format!("lix_file_descriptor: Invalid file path {path}"),
        });
    }
    if normalized.contains('\\') || normalized.contains("//") {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("lix_file_descriptor: Invalid file path {path}"),
        });
    }
    let segments = normalized
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    if segments.is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("lix_file_descriptor: Invalid file path {path}"),
        });
    }
    let canonical_segments = canonicalize_path_segments(&segments)?;
    if canonical_segments.is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_INVALID_FILE_PATH".to_string(),
            description: format!("lix_file_descriptor: Invalid file path {path}"),
        });
    }
    Ok(format!("/{}", canonical_segments.join("/")))
}

pub(crate) fn normalize_directory_path(path: &str) -> Result<String, LixError> {
    let normalized = path.nfc().collect::<String>();
    if !normalized.starts_with('/') {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("lix_directory_descriptor: Invalid directory path {path}"),
        });
    }
    if normalized.contains('\\') || normalized.contains("//") {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("lix_directory_descriptor: Invalid directory path {path}"),
        });
    }
    if normalized == "/" {
        return Ok("/".to_string());
    }
    if !normalized.ends_with('/') {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("lix_directory_descriptor: Invalid directory path {path}"),
        });
    }
    let segments = normalized
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    let normalized_segments = canonicalize_path_segments(&segments)?;
    if normalized_segments.is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("lix_directory_descriptor: Invalid directory path {path}"),
        });
    }
    Ok(format!("/{}/", normalized_segments.join("/")))
}

fn canonicalize_path_segments(segments: &[&str]) -> Result<Vec<String>, LixError> {
    let mut canonical_segments = Vec::with_capacity(segments.len());

    for segment in segments {
        let normalized_segment = normalize_validated_path_segment(segment)?;
        match normalized_segment.as_str() {
            "." => {}
            ".." => {
                canonical_segments.pop();
            }
            _ => canonical_segments.push(normalized_segment),
        }
    }

    Ok(canonical_segments)
}

pub(crate) fn parse_file_path(path: &str) -> Result<ParsedFilePath, LixError> {
    let normalized_path = normalize_file_path(path)?;
    let segments = normalized_path
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    let file_name = segments.last().ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("Invalid file path {path}"),
    })?;
    let directory_path = if segments.len() > 1 {
        Some(NormalizedDirectoryPath::from_normalized(format!(
            "/{}/",
            segments[..segments.len() - 1].join("/")
        )))
    } else {
        None
    };

    let last_dot = file_name.rfind('.');
    let (name, extension) = match last_dot {
        Some(index) if index > 0 => {
            let name = file_name[..index].to_string();
            let extension = file_name[index + 1..].to_string();
            let extension = if extension.is_empty() {
                None
            } else {
                Some(extension)
            };
            (name, extension)
        }
        _ => (file_name.to_string(), None),
    };

    Ok(ParsedFilePath {
        normalized_path: NormalizedFilePath::from_normalized(normalized_path),
        directory_path,
        name,
        extension,
    })
}

pub(crate) fn directory_ancestor_paths(path: &str) -> Vec<String> {
    ancestor_directory_paths(path)
}

fn ancestor_directory_paths(path: &str) -> Vec<String> {
    let segments = path
        .trim_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    if segments.len() <= 1 {
        return Vec::new();
    }

    let mut ancestors = Vec::with_capacity(segments.len() - 1);
    let mut prefix_segments: Vec<&str> = Vec::with_capacity(segments.len() - 1);
    for segment in segments.iter().take(segments.len() - 1) {
        prefix_segments.push(segment);
        ancestors.push(format!("/{}/", prefix_segments.join("/")));
    }
    ancestors
}

pub(crate) fn parent_directory_path(path: &str) -> Option<String> {
    let segments = path
        .trim_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    if segments.len() <= 1 {
        return None;
    }
    Some(format!("/{}/", segments[..segments.len() - 1].join("/")))
}

pub(crate) fn directory_name_from_path(path: &str) -> Option<String> {
    path.trim_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .next_back()
        .map(|segment| segment.to_string())
}

pub(crate) fn compose_directory_path(parent_path: &str, name: &str) -> Result<String, LixError> {
    let normalized_name = normalize_path_segment(name)?;
    if parent_path == "/" {
        Ok(format!("/{normalized_name}/"))
    } else if parent_path.starts_with('/') && parent_path.ends_with('/') {
        Ok(format!("{parent_path}{normalized_name}/"))
    } else {
        Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("Invalid directory parent path {parent_path}"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_normalized_file_paths_with_unicode_and_percent_encoding() {
        for path in [
            "/docs/readme.md",
            "/a/b/c.txt",
            "/dash--path",
            "/unicodé/段落.md",
            "/docs/%20notes.md",
            "/docs/hello:world@x!$&'()*+,;=.md",
        ] {
            assert!(
                normalize_file_path(path).is_ok(),
                "expected valid path {path}"
            );
        }
    }

    #[test]
    fn rejects_structural_file_path_anomalies() {
        for path in ["/", "/trailing/", "no-leading", "/bad//double"] {
            assert!(
                normalize_file_path(path).is_err(),
                "expected invalid path {path}"
            );
        }
    }

    #[test]
    fn rejects_file_paths_with_dot_segments() {
        for (path, expected) in [("/docs/./file", "/docs/file"), ("/docs/../file", "/file")] {
            assert!(
                normalize_file_path(path).as_deref() == Ok(expected),
                "expected canonicalized dot-segment path {path}"
            );
        }
    }

    #[test]
    fn rejects_file_paths_with_invalid_characters() {
        for path in ["/docs/file?.md", "/docs/#hash", "/docs/file name.md"] {
            assert!(
                normalize_file_path(path).is_err(),
                "expected invalid character path {path}"
            );
        }
    }

    #[test]
    fn rejects_file_paths_with_private_use_and_noncharacter_code_points() {
        for path in ["/docs/\u{E000}.md", "/docs/\u{FDD0}.md"] {
            assert!(
                normalize_file_path(path).is_err(),
                "expected invalid RFC3987 code point path {path}"
            );
        }
    }

    #[test]
    fn rejects_file_paths_with_bidi_formatting_characters() {
        for path in ["/docs/\u{200E}.md", "/docs/\u{202E}.md"] {
            assert!(
                normalize_file_path(path).is_err(),
                "expected invalid bidi-formatting path {path}"
            );
        }
    }

    #[test]
    fn validates_percent_encoding_in_file_paths() {
        assert!(normalize_file_path("/docs/%20notes.md").is_ok());
        assert!(normalize_file_path("/docs/%zz.md").is_err());
        assert!(normalize_file_path("/docs/abc%.md").is_err());
        assert!(normalize_file_path("/docs/abc%2.md").is_err());
    }

    #[test]
    fn canonicalizes_percent_encoding_in_file_paths() {
        assert_eq!(
            normalize_file_path("/docs/%7e%41%2e%2E.md").as_deref(),
            Ok("/docs/~A...md")
        );
        assert_eq!(
            normalize_file_path("/docs/%2fkept%3aencoded").as_deref(),
            Ok("/docs/%2Fkept%3Aencoded")
        );
        assert_eq!(
            normalize_file_path("/docs/%2e/file").as_deref(),
            Ok("/docs/file")
        );
    }

    #[test]
    fn normalization_is_stable_on_renormalization() {
        let once = normalize_file_path("/docs/%7e%2E/../%41.md").expect("first normalization");
        let twice = normalize_file_path(&once).expect("second normalization");
        assert_eq!(once, twice);
    }

    #[test]
    fn accepts_and_rejects_directory_paths_like_legacy_rules() {
        for path in ["/", "/docs/", "/docs/guides/", "/unicodé/章节/", "/docs/%20/"] {
            assert!(
                normalize_directory_path(path).is_ok(),
                "expected valid directory path {path}"
            );
        }
        for path in ["/file.md", "/docs", "/docs/ ", "no-leading", "/docs/%zz/"] {
            assert!(
                normalize_directory_path(path).is_err(),
                "expected invalid directory path {path}"
            );
        }
    }

    #[test]
    fn canonicalizes_directory_paths() {
        assert_eq!(
            normalize_directory_path("/docs/%2e/guide/").as_deref(),
            Ok("/docs/guide/")
        );
        assert_eq!(
            normalize_directory_path("/docs/%7e%2fkept/").as_deref(),
            Ok("/docs/~%2Fkept/")
        );
    }

    #[test]
    fn represents_root_as_a_normalized_directory_path() {
        let root = NormalizedDirectoryPath::try_from_path("/").expect("root path");
        assert_eq!(root.as_str(), "/");
        assert!(root.is_root());
        assert_eq!(NormalizedDirectoryPath::root(), root);
    }

    #[test]
    fn root_parent_and_top_level_parent_are_absent() {
        assert_eq!(parent_directory_path("/"), None);
        assert_eq!(parent_directory_path("/top-level.txt"), None);
    }

    #[test]
    fn compose_directory_path_under_root() {
        assert_eq!(
            compose_directory_path("/", "docs").as_deref(),
            Ok("/docs/")
        );
    }
}
