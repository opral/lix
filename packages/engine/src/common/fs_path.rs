//! Canonical Lix filesystem paths live in this module.
//!
//! Contract:
//!
//! - Canonical internal form is an RFC 3987 `ipath-absolute` IRI path.
//! - The engine stores an internal IRI, not a WHATWG URL.
//! - ASCII-only URI spelling is a boundary serialization, not the internal form.
//! - Unicode is normalized with UAX #15 NFC.
//! - Canonicalization uses RFC 3986-compatible percent-encoding normalization.
//! - Dot segments are rejected rather than rewritten because Lix paths are
//!   stable logical identities, not URI references being resolved against a
//!   base path.
//!
//! Canonicalization order:
//!
//! 1. Normalize raw input to NFC.
//! 2. Validate the normalized form and percent-encoding structure.
//! 3. Apply percent-encoding normalization.
//! 4. Reject dot segments.
//!
//! Fixed RFC-derived rules:
//!
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
    #[cfg(test)]
    pub(crate) fn try_from_path(path: &str) -> Result<Self, LixError> {
        normalize_directory_path(path).map(Self)
    }
    pub(crate) fn from_normalized(path: String) -> Self {
        Self(path)
    }

    pub(crate) fn as_str(&self) -> &str {
        self.0.as_str()
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
}

impl ParsedFilePath {
    pub(crate) fn try_from_path(path: &str) -> Result<Self, LixError> {
        parse_file_path(path)
    }
}

type PathResult<T> = Result<T, PathError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PathError {
    MissingLeadingSlash,
    UnexpectedTrailingSlashOnFilePath,
    MissingTrailingSlashOnDirectoryPath,
    EmptySegment,
    DotSegment,
    SlashInSegment,
    Backslash,
    InvalidPercentEncoding,
    InvalidIriCodePoint,
    NulByte,
    InvalidRootUsage,
    #[cfg(test)]
    InvalidDirectoryParentPath,
}

impl PathError {
    fn into_lix_error(self) -> LixError {
        let (code, message, hint) = match self {
            Self::MissingLeadingSlash => (
                "LIX_ERROR_PATH_MISSING_LEADING_SLASH",
                "path must start with '/'",
                Some("prefix the path with '/'"),
            ),
            Self::UnexpectedTrailingSlashOnFilePath => (
                "LIX_ERROR_PATH_UNEXPECTED_TRAILING_SLASH_ON_FILE",
                "file path must not end with '/'",
                Some("remove the trailing slash or use a directory path instead"),
            ),
            Self::MissingTrailingSlashOnDirectoryPath => (
                "LIX_ERROR_PATH_MISSING_TRAILING_SLASH_ON_DIRECTORY",
                "directory path must end with '/'",
                Some("append a trailing slash or use a file path instead"),
            ),
            Self::EmptySegment => (
                "LIX_ERROR_PATH_EMPTY_SEGMENT",
                "path must not contain empty segments",
                Some("remove duplicate slashes like '//'"),
            ),
            Self::DotSegment => (
                "LIX_ERROR_PATH_DOT_SEGMENT",
                "path segment cannot be '.' or '..'",
                Some("use a real segment name instead of '.' or '..'"),
            ),
            Self::SlashInSegment => (
                "LIX_ERROR_PATH_SLASH_IN_SEGMENT",
                "path segment must not contain '/'",
                Some("pass a single segment name, not a full path"),
            ),
            Self::Backslash => (
                "LIX_ERROR_PATH_BACKSLASH",
                "path must not contain '\\'",
                Some("use '/' separators instead of '\\'"),
            ),
            Self::InvalidPercentEncoding => (
                "LIX_ERROR_PATH_INVALID_PERCENT_ENCODING",
                "path contains invalid percent encoding",
                Some("use percent triplets like %20 and escape '%' as %25"),
            ),
            Self::InvalidIriCodePoint => (
                "LIX_ERROR_PATH_INVALID_IRI_CODE_POINT",
                "path contains a raw character that is not allowed in canonical Lix paths",
                Some("canonical paths allow Unicode, but raw spaces, '?' and '#' must be percent-encoded at boundaries"),
            ),
            Self::NulByte => (
                "LIX_ERROR_PATH_NUL_BYTE",
                "path must not contain a NUL byte",
                Some("remove the NUL byte from the path"),
            ),
            Self::InvalidRootUsage => (
                "LIX_ERROR_PATH_INVALID_ROOT_USAGE",
                "root '/' is only valid as a directory path",
                Some("use '/' as a directory path, never as a file path"),
            ),
            #[cfg(test)]
            Self::InvalidDirectoryParentPath => (
                "LIX_ERROR_PATH_INVALID_DIRECTORY_PARENT",
                "directory parent path must be a normalized directory path",
                Some("pass '/' or a path ending with '/' as the parent directory"),
            ),
        };

        let err = LixError::new(code, message);
        match hint {
            Some(hint) => err.with_hint(hint),
            None => err,
        }
    }
}

pub(crate) fn normalize_path_segment(raw: &str) -> Result<String, LixError> {
    normalize_path_segment_impl(raw).map_err(PathError::into_lix_error)
}

fn normalize_path_segment_impl(raw: &str) -> PathResult<String> {
    let normalized = raw.nfc().collect::<String>();
    let canonical = normalize_validated_path_segment(&normalized)?;
    if canonical == "." || canonical == ".." {
        return Err(PathError::DotSegment);
    }
    Ok(canonical)
}

fn validate_path_segment_chars(normalized: &str) -> PathResult<()> {
    if normalized.is_empty() {
        return Err(PathError::EmptySegment);
    }
    if normalized.contains('\0') {
        return Err(PathError::NulByte);
    }
    if normalized.contains('/') {
        return Err(PathError::SlashInSegment);
    }
    if normalized.contains('\\') {
        return Err(PathError::Backslash);
    }
    if !segment_has_valid_percent_encoding(&normalized) {
        return Err(PathError::InvalidPercentEncoding);
    }
    if normalized
        .chars()
        .any(|ch| is_disallowed_bidi_formatting_char(ch) || is_disallowed_zero_width_char(ch))
    {
        return Err(PathError::InvalidIriCodePoint);
    }
    if !normalized.chars().all(is_allowed_segment_char) {
        return Err(PathError::InvalidIriCodePoint);
    }
    Ok(())
}

fn normalize_validated_path_segment(normalized: &str) -> PathResult<String> {
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
        '\u{061C}'
            | '\u{200E}'
            | '\u{200F}'
            | '\u{202A}'
            | '\u{202B}'
            | '\u{202C}'
            | '\u{202D}'
            | '\u{202E}'
            | '\u{2066}'
            | '\u{2067}'
            | '\u{2068}'
            | '\u{2069}'
    )
}

fn is_disallowed_zero_width_char(ch: char) -> bool {
    matches!(ch, '\u{200B}' | '\u{200C}' | '\u{200D}' | '\u{FEFF}')
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

fn normalize_file_path_impl(path: &str) -> PathResult<String> {
    let normalized = path.nfc().collect::<String>();
    if !normalized.starts_with('/') {
        return Err(PathError::MissingLeadingSlash);
    }
    if normalized == "/" {
        return Err(PathError::InvalidRootUsage);
    }
    if normalized.ends_with('/') {
        return Err(PathError::UnexpectedTrailingSlashOnFilePath);
    }
    if normalized.contains('\\') {
        return Err(PathError::Backslash);
    }
    if normalized.contains("//") {
        return Err(PathError::EmptySegment);
    }
    let segments = normalized
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    if segments.is_empty() {
        return Err(PathError::EmptySegment);
    }
    let canonical_segments = canonicalize_path_segments(&segments)?;
    if canonical_segments.is_empty() {
        return Err(PathError::InvalidRootUsage);
    }
    Ok(format!("/{}", canonical_segments.join("/")))
}

pub(crate) fn normalize_directory_path(path: &str) -> Result<String, LixError> {
    normalize_directory_path_impl(path).map_err(PathError::into_lix_error)
}

fn normalize_directory_path_impl(path: &str) -> PathResult<String> {
    let normalized = path.nfc().collect::<String>();
    if !normalized.starts_with('/') {
        return Err(PathError::MissingLeadingSlash);
    }
    if normalized.contains('\\') {
        return Err(PathError::Backslash);
    }
    if normalized.contains("//") {
        return Err(PathError::EmptySegment);
    }
    if normalized == "/" {
        return Ok("/".to_string());
    }
    if !normalized.ends_with('/') {
        return Err(PathError::MissingTrailingSlashOnDirectoryPath);
    }
    let segments = normalized
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    let normalized_segments = canonicalize_path_segments(&segments)?;
    if normalized_segments.is_empty() {
        return Ok("/".to_string());
    }
    Ok(format!("/{}/", normalized_segments.join("/")))
}

fn canonicalize_path_segments(segments: &[&str]) -> PathResult<Vec<String>> {
    let mut canonical_segments = Vec::with_capacity(segments.len());

    for segment in segments {
        let normalized_segment = normalize_validated_path_segment(segment)?;
        match normalized_segment.as_str() {
            "." | ".." => return Err(PathError::DotSegment),
            _ => canonical_segments.push(normalized_segment),
        }
    }

    Ok(canonical_segments)
}

pub(crate) fn parse_file_path(path: &str) -> Result<ParsedFilePath, LixError> {
    parse_file_path_impl(path).map_err(PathError::into_lix_error)
}

fn parse_file_path_impl(path: &str) -> PathResult<ParsedFilePath> {
    let normalized_path = normalize_file_path_impl(path)?;
    let segments = normalized_path
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    let file_name = segments
        .last()
        .ok_or(PathError::InvalidRootUsage)?
        .to_string();
    let directory_path = if segments.len() > 1 {
        Some(NormalizedDirectoryPath::from_normalized(format!(
            "/{}/",
            segments[..segments.len() - 1].join("/")
        )))
    } else {
        None
    };

    Ok(ParsedFilePath {
        normalized_path: NormalizedFilePath::from_normalized(normalized_path),
        directory_path,
        name: file_name,
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

#[cfg(test)]
pub(crate) fn compose_directory_path(parent_path: &str, name: &str) -> Result<String, LixError> {
    let normalized_name = normalize_path_segment_impl(name).map_err(PathError::into_lix_error)?;
    if parent_path == "/" {
        Ok(format!("/{normalized_name}/"))
    } else if parent_path.starts_with('/') && parent_path.ends_with('/') {
        Ok(format!("{parent_path}{normalized_name}/"))
    } else {
        Err(PathError::InvalidDirectoryParentPath.into_lix_error())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iref::iri::Path as IriPath;

    #[derive(Clone, Copy, Debug)]
    enum NormalizationKind {
        File,
        Directory,
        Segment,
    }

    #[derive(Clone, Copy, Debug)]
    enum LixFixtureKind {
        File,
        Directory,
    }

    #[derive(Clone, Copy, Debug)]
    struct RfcFixture {
        label: &'static str,
        input: &'static str,
    }

    #[derive(Clone, Copy, Debug)]
    struct LixProfileFixture {
        label: &'static str,
        kind: LixFixtureKind,
        input: &'static str,
        oracle_accepts: bool,
        expected: Result<&'static str, PathError>,
    }

    #[derive(Clone, Copy, Debug)]
    struct NormalizationFixture {
        label: &'static str,
        kind: NormalizationKind,
        input: &'static str,
        expected: &'static str,
    }

    fn assert_path_error<T: fmt::Debug>(result: PathResult<T>, expected: PathError) {
        assert_eq!(result.unwrap_err(), expected);
    }

    fn iri_oracle_accepts(path: &str) -> bool {
        IriPath::new(path).is_ok()
    }

    fn normalize_with_kind(kind: NormalizationKind, input: &str) -> Result<String, LixError> {
        match kind {
            NormalizationKind::File => {
                normalize_file_path_impl(input).map_err(PathError::into_lix_error)
            }
            NormalizationKind::Directory => normalize_directory_path(input),
            NormalizationKind::Segment => normalize_path_segment(input),
        }
    }

    fn normalize_file_path(path: &str) -> Result<String, LixError> {
        normalize_file_path_impl(path).map_err(PathError::into_lix_error)
    }

    fn assert_lix_profile_fixture(fixture: LixProfileFixture) {
        assert_eq!(
            iri_oracle_accepts(fixture.input),
            fixture.oracle_accepts,
            "iref oracle mismatch for {} ({})",
            fixture.label,
            fixture.input
        );

        match fixture.kind {
            LixFixtureKind::File => match fixture.expected {
                Ok(expected) => assert_eq!(
                    normalize_file_path(fixture.input).as_deref(),
                    Ok(expected),
                    "unexpected file result for {} ({})",
                    fixture.label,
                    fixture.input
                ),
                Err(expected) => {
                    assert_path_error(normalize_file_path_impl(fixture.input), expected)
                }
            },
            LixFixtureKind::Directory => match fixture.expected {
                Ok(expected) => assert_eq!(
                    normalize_directory_path(fixture.input).as_deref(),
                    Ok(expected),
                    "unexpected directory result for {} ({})",
                    fixture.label,
                    fixture.input
                ),
                Err(expected) => {
                    assert_path_error(normalize_directory_path_impl(fixture.input), expected)
                }
            },
        }
    }

    const RFC_POSITIVE_FIXTURES: &[RfcFixture] = &[
        RfcFixture {
            label: "absolute unicode file path",
            input: "/unicodé/段落.md",
        },
        RfcFixture {
            label: "absolute path with pct-encoded space",
            input: "/docs/%20notes.md",
        },
        RfcFixture {
            label: "path with pchar punctuation",
            input: "/docs/hello:world@x!$&'()*+,;=.md",
        },
    ];

    const RFC_NEGATIVE_FIXTURES: &[RfcFixture] = &[
        RfcFixture {
            label: "invalid percent triplet",
            input: "/docs/%zz.md",
        },
        RfcFixture {
            label: "truncated percent triplet",
            input: "/docs/%2",
        },
        RfcFixture {
            label: "raw space is not allowed in an ipath",
            input: "/docs/file name.md",
        },
        RfcFixture {
            label: "raw fragment delimiter is not part of the path grammar",
            input: "/docs/#hash",
        },
        RfcFixture {
            label: "private use code point is excluded from ucschar",
            input: "/docs/\u{E000}.md",
        },
    ];

    const LIX_PROFILE_POSITIVE_FIXTURES: &[LixProfileFixture] = &[
        LixProfileFixture {
            label: "root directory is representable",
            kind: LixFixtureKind::Directory,
            input: "/",
            oracle_accepts: true,
            expected: Ok("/"),
        },
        LixProfileFixture {
            label: "directory paths require trailing slash",
            kind: LixFixtureKind::Directory,
            input: "/docs/",
            oracle_accepts: true,
            expected: Ok("/docs/"),
        },
        LixProfileFixture {
            label: "file paths stay slashless at the end",
            kind: LixFixtureKind::File,
            input: "/docs/readme.md",
            oracle_accepts: true,
            expected: Ok("/docs/readme.md"),
        },
    ];

    const LIX_PROFILE_NEGATIVE_FIXTURES: &[LixProfileFixture] = &[
        LixProfileFixture {
            label: "relative-looking path is valid RFC syntax but not a Lix path",
            kind: LixFixtureKind::File,
            input: "docs/readme.md",
            oracle_accepts: true,
            expected: Err(PathError::MissingLeadingSlash),
        },
        LixProfileFixture {
            label: "file paths reject trailing slash even though RFC syntax allows it",
            kind: LixFixtureKind::File,
            input: "/docs/",
            oracle_accepts: true,
            expected: Err(PathError::UnexpectedTrailingSlashOnFilePath),
        },
        LixProfileFixture {
            label: "directory paths reject missing trailing slash even though RFC syntax allows it",
            kind: LixFixtureKind::Directory,
            input: "/docs",
            oracle_accepts: true,
            expected: Err(PathError::MissingTrailingSlashOnDirectoryPath),
        },
        LixProfileFixture {
            label: "empty segments are valid RFC paths but banned by the Lix profile",
            kind: LixFixtureKind::File,
            input: "/docs//guide.md",
            oracle_accepts: true,
            expected: Err(PathError::EmptySegment),
        },
        LixProfileFixture {
            label: "root is not a valid file path",
            kind: LixFixtureKind::File,
            input: "/",
            oracle_accepts: true,
            expected: Err(PathError::InvalidRootUsage),
        },
        LixProfileFixture {
            label: "bidi formatting is rejected by the Lix validator even though iref accepts it",
            kind: LixFixtureKind::File,
            input: "/docs/\u{202E}.md",
            oracle_accepts: true,
            expected: Err(PathError::InvalidIriCodePoint),
        },
        LixProfileFixture {
            label: "dot segments are valid RFC syntax but banned by the Lix profile",
            kind: LixFixtureKind::File,
            input: "/docs/../guide.md",
            oracle_accepts: true,
            expected: Err(PathError::DotSegment),
        },
    ];

    const NORMALIZATION_FIXTURES: &[NormalizationFixture] = &[
        NormalizationFixture {
            label: "nfc composition happens before validation",
            kind: NormalizationKind::File,
            input: "/Cafe\u{0301}.md",
            expected: "/Café.md",
        },
        NormalizationFixture {
            label: "percent triplets are uppercased when preserved",
            kind: NormalizationKind::Directory,
            input: "/docs/%2fkept/",
            expected: "/docs/%2Fkept/",
        },
        NormalizationFixture {
            label: "unreserved percent encoding is decoded",
            kind: NormalizationKind::File,
            input: "/docs/%7e%41.md",
            expected: "/docs/~A.md",
        },
        NormalizationFixture {
            label: "root survives directory normalization",
            kind: NormalizationKind::Directory,
            input: "/",
            expected: "/",
        },
        NormalizationFixture {
            label: "segment normalization decodes unreserved percent triplets",
            kind: NormalizationKind::Segment,
            input: "%7ehello",
            expected: "~hello",
        },
    ];

    #[test]
    fn rfc_positive_path_fixtures_agree_with_iref() {
        for fixture in RFC_POSITIVE_FIXTURES {
            assert!(
                iri_oracle_accepts(fixture.input),
                "iref should accept {} ({})",
                fixture.label,
                fixture.input
            );
            assert!(
                normalize_file_path_impl(fixture.input).is_ok(),
                "lix should accept {} ({})",
                fixture.label,
                fixture.input
            );
        }
    }

    #[test]
    fn rfc_negative_path_fixtures_agree_with_iref() {
        for fixture in RFC_NEGATIVE_FIXTURES {
            assert!(
                !iri_oracle_accepts(fixture.input),
                "iref should reject {} ({})",
                fixture.label,
                fixture.input
            );
            assert!(
                normalize_file_path_impl(fixture.input).is_err(),
                "lix should reject {} ({})",
                fixture.label,
                fixture.input
            );
        }
    }

    #[test]
    fn lix_profile_positive_fixtures_are_pinned() {
        for fixture in LIX_PROFILE_POSITIVE_FIXTURES {
            assert_lix_profile_fixture(*fixture);
        }
    }

    #[test]
    fn lix_profile_negative_fixtures_document_divergence_from_the_oracle() {
        for fixture in LIX_PROFILE_NEGATIVE_FIXTURES {
            assert_lix_profile_fixture(*fixture);
        }
    }

    #[test]
    fn normalization_fixture_table_covers_canonicalization_rules() {
        for fixture in NORMALIZATION_FIXTURES {
            assert_eq!(
                normalize_with_kind(fixture.kind, fixture.input).as_deref(),
                Ok(fixture.expected),
                "unexpected normalized value for {} ({})",
                fixture.label,
                fixture.input
            );
        }
    }

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
        assert_path_error(normalize_file_path_impl("/"), PathError::InvalidRootUsage);
        assert_path_error(
            normalize_file_path_impl("/trailing/"),
            PathError::UnexpectedTrailingSlashOnFilePath,
        );
        assert_path_error(
            normalize_file_path_impl("no-leading"),
            PathError::MissingLeadingSlash,
        );
        assert_path_error(
            normalize_file_path_impl("/bad//double"),
            PathError::EmptySegment,
        );
    }

    #[test]
    fn rejects_file_paths_with_dot_segments() {
        for path in [
            "/docs/./file",
            "/docs/../file",
            "/docs/%2e/file",
            "/docs/%2E%2E/file",
        ] {
            assert_path_error(normalize_file_path_impl(path), PathError::DotSegment);
        }
    }

    #[test]
    fn rejects_file_paths_with_invalid_characters() {
        for path in ["/docs/file?.md", "/docs/#hash", "/docs/file name.md"] {
            assert_path_error(
                normalize_file_path_impl(path),
                PathError::InvalidIriCodePoint,
            );
        }
    }

    #[test]
    fn rejects_file_paths_with_private_use_and_noncharacter_code_points() {
        for path in ["/docs/\u{E000}.md", "/docs/\u{FDD0}.md"] {
            assert_path_error(
                normalize_file_path_impl(path),
                PathError::InvalidIriCodePoint,
            );
        }
    }

    #[test]
    fn rejects_file_paths_with_bidi_formatting_characters() {
        for path in ["/docs/\u{200E}.md", "/docs/\u{202E}.md"] {
            assert_path_error(
                normalize_file_path_impl(path),
                PathError::InvalidIriCodePoint,
            );
        }
    }

    #[test]
    fn validates_percent_encoding_in_file_paths() {
        assert!(normalize_file_path("/docs/%20notes.md").is_ok());
        assert_path_error(
            normalize_file_path_impl("/docs/%zz.md"),
            PathError::InvalidPercentEncoding,
        );
        assert_path_error(
            normalize_file_path_impl("/docs/abc%.md"),
            PathError::InvalidPercentEncoding,
        );
        assert_path_error(
            normalize_file_path_impl("/docs/abc%2.md"),
            PathError::InvalidPercentEncoding,
        );
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
    }

    #[test]
    fn normalization_is_stable_on_renormalization() {
        let once = normalize_file_path("/docs/%7e/%41.md").expect("first normalization");
        let twice = normalize_file_path(&once).expect("second normalization");
        assert_eq!(once, twice);
    }

    #[test]
    fn accepts_and_rejects_directory_paths_like_legacy_rules() {
        for path in [
            "/",
            "/docs/",
            "/docs/guides/",
            "/unicodé/章节/",
            "/docs/%20/",
        ] {
            assert!(
                normalize_directory_path(path).is_ok(),
                "expected valid directory path {path}"
            );
        }
        assert_path_error(
            normalize_directory_path_impl("/file.md"),
            PathError::MissingTrailingSlashOnDirectoryPath,
        );
        assert_path_error(
            normalize_directory_path_impl("/docs"),
            PathError::MissingTrailingSlashOnDirectoryPath,
        );
        assert_path_error(
            normalize_directory_path_impl("/docs/ "),
            PathError::MissingTrailingSlashOnDirectoryPath,
        );
        assert_path_error(
            normalize_directory_path_impl("/docs/ /"),
            PathError::InvalidIriCodePoint,
        );
        assert_path_error(
            normalize_directory_path_impl("no-leading"),
            PathError::MissingLeadingSlash,
        );
        assert_path_error(
            normalize_directory_path_impl("/docs/%zz/"),
            PathError::InvalidPercentEncoding,
        );
    }

    #[test]
    fn canonicalizes_directory_paths() {
        assert_eq!(
            normalize_directory_path("/docs/%7e%2fkept/").as_deref(),
            Ok("/docs/~%2Fkept/")
        );
    }

    #[test]
    fn rejects_directory_paths_with_dot_segments() {
        for path in ["/docs/./", "/docs/../", "/docs/%2e/", "/docs/%2E%2E/"] {
            assert_path_error(normalize_directory_path_impl(path), PathError::DotSegment);
        }
    }

    #[test]
    fn represents_root_as_a_normalized_directory_path() {
        let root = NormalizedDirectoryPath::try_from_path("/").expect("root path");
        assert_eq!(root.as_str(), "/");
        assert_eq!(
            root,
            NormalizedDirectoryPath::from_normalized("/".to_string())
        );
    }

    #[test]
    fn root_parent_and_top_level_parent_are_absent() {
        assert_eq!(parent_directory_path("/"), None);
        assert_eq!(parent_directory_path("/top-level.txt"), None);
    }

    #[test]
    fn compose_directory_path_under_root() {
        assert_eq!(compose_directory_path("/", "docs").as_deref(), Ok("/docs/"));
    }

    #[test]
    fn exposes_stable_lix_errors_with_hints() {
        let missing_leading = normalize_file_path("docs/readme.md").expect_err("leading slash");
        assert_eq!(missing_leading.code, "LIX_ERROR_PATH_MISSING_LEADING_SLASH");
        assert_eq!(missing_leading.hint(), Some("prefix the path with '/'"));

        let bad_percent = normalize_file_path("/docs/%zz.md").expect_err("bad percent");
        assert_eq!(bad_percent.code, "LIX_ERROR_PATH_INVALID_PERCENT_ENCODING");
        assert_eq!(
            bad_percent.hint(),
            Some("use percent triplets like %20 and escape '%' as %25")
        );

        let root_file = normalize_file_path("/").expect_err("root as file");
        assert_eq!(root_file.code, "LIX_ERROR_PATH_INVALID_ROOT_USAGE");
        assert_eq!(
            root_file.hint(),
            Some("use '/' as a directory path, never as a file path")
        );
    }
}
