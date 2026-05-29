//! Canonical Lix filesystem paths live in this module.
//!
//! Contract:
//!
//! - Canonical internal form is an absolute slash-separated Lix filesystem
//!   path, structurally aligned with RFC 3986 `path-absolute` / RFC 8089 file
//!   URI paths.
//! - RFC 3986/8089 URI spelling is a boundary serialization, not the internal
//!   identity form.
//! - Each non-empty segment is enforced with an RFC 8264 PRECIS
//!   `IdentifierClass` profile, case-preserved and NFC-normalized.
//! - Percent encoding is accepted only as boundary input. Canonical internal
//!   paths store decoded Unicode segments, never percent triplets.
//! - Dot segments are rejected rather than rewritten because Lix paths are
//!   stable logical identities, not URI references being resolved against a
//!   base path.
//!
//! Canonicalization order:
//!
//! 1. Validate and decode RFC 3986 percent triplets in each segment.
//! 2. Normalize decoded segment text to NFC.
//! 3. Apply PRECIS IdentifierClass enforcement.
//! 4. Reject Lix structural sentinels and separators.
//!
//! Fixed standard-derived rules:
//!
//! - Path shape follows the absolute-path grammar used by RFC 3986/RFC 8089.
//! - Segment text follows RFC 8264 PRECIS IdentifierClass semantics.
//! - Comparison is exact-string and case-sensitive after canonicalization.
//!
//! Lix profile rules:
//!
//! - File paths never end with `/`.
//! - Directory paths always end with `/`.
//! - `NUL` is rejected in all segments.
//! - `/`, `\`, empty segments, `.`, and `..` are rejected in all non-root
//!   segments.
//! - `%`, `?`, and `#` are reserved for URI boundary syntax and are rejected
//!   in canonical internal segments.
//! - Segments cannot begin with a combining mark.
//! - Root is represented as the normalized directory path `/`.
//! - Git/CLI import and ASCII-only URI serialization are boundary adapters,
//!   not part of the core `fs_path` contract.
//!
//! Length policy:
//!
//! - Each canonical segment is capped at 255 bytes, matching common
//!   filesystem component limits.
//! - Each full canonical path is capped at 4096 bytes.
//! - Raw boundary input is separately capped before normalization so oversized
//!   URI spellings cannot reach Unicode processing.
//!
//! Runtime strategy:
//!
//! - This module keeps Lix structural checks local and delegates Unicode
//!   segment validity to the PRECIS implementation.
//! - `iref` is an RFC 3987 / RFC 3986 shape oracle in tests, not the runtime
//!   segment authority.
//!
//! Glossary:
//!
//! - Raw input path: caller-provided path before normalization.
//! - Normalized path: path after NFC normalization.
//! - Canonical path: stored path after full normalization/canonicalization.
//! - File path: canonical path naming a file, without a trailing slash.
//! - Directory path: canonical path naming a directory, with a trailing slash.
//! - Internal path form: the canonical Unicode-bearing representation used by
//!   the engine.
//! - Boundary URI form: an ASCII-only serialization used when interoperating
//!   with URI-only systems.

#![allow(
    clippy::redundant_closure_for_method_calls,
    clippy::semicolon_if_nothing_returned
)]

use precis_profiles::UsernameCasePreserved;
use precis_profiles::precis_core::profile::Profile;
use unicode_normalization::{UnicodeNormalization, char::is_combining_mark};

use crate::LixError;
use std::fmt;
use std::ops::Deref;

const MAX_CANONICAL_PATH_BYTES: usize = 4096;
const MAX_CANONICAL_PATH_SEGMENT_BYTES: usize = 255;
const MAX_RAW_PATH_INPUT_BYTES: usize = 16 * 1024;

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
    InvalidPathSegmentCodePoint,
    PathTooLong,
    RawPathInputTooLong,
    SegmentTooLong,
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
                Some(
                    "use valid percent triplets only for URI boundary input; '%' is not allowed in canonical path segments",
                ),
            ),
            Self::InvalidPathSegmentCodePoint => (
                "LIX_ERROR_PATH_INVALID_SEGMENT_CODE_POINT",
                "path segment contains a character that is not allowed in canonical Lix paths",
                Some(
                    "canonical paths use RFC 8264 PRECIS IdentifierClass segments; use URI percent encoding only at boundaries",
                ),
            ),
            Self::PathTooLong => (
                "LIX_ERROR_PATH_TOO_LONG",
                "path is too long",
                Some("keep canonical paths at or below 4096 bytes"),
            ),
            Self::RawPathInputTooLong => (
                "LIX_ERROR_PATH_INPUT_TOO_LONG",
                "path input is too long",
                Some("keep raw path input at or below 16384 bytes"),
            ),
            Self::SegmentTooLong => (
                "LIX_ERROR_PATH_SEGMENT_TOO_LONG",
                "path segment is too long",
                Some("keep each canonical path segment at or below 255 bytes"),
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
    ensure_raw_path_input_len(raw)?;
    let normalized = raw.nfc().collect::<String>();
    let canonical = normalize_validated_path_segment(&normalized)?;
    if canonical == "." || canonical == ".." {
        return Err(PathError::DotSegment);
    }
    Ok(canonical)
}

fn validate_path_segment_chars(normalized: &str) -> PathResult<String> {
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
    if !segment_has_valid_percent_encoding(normalized) {
        return Err(PathError::InvalidPercentEncoding);
    }
    let decoded = decode_percent_encoded_segment(normalized)?;
    validate_decoded_path_segment_structure(&decoded)?;
    Ok(decoded)
}

fn normalize_validated_path_segment(normalized: &str) -> PathResult<String> {
    let decoded = validate_path_segment_chars(normalized)?;
    ensure_canonical_segment_len(&decoded)?;
    let canonical = enforce_precis_segment(&decoded)?;
    ensure_canonical_segment_len(&canonical)?;
    Ok(canonical)
}

fn decode_percent_encoded_segment(segment: &str) -> PathResult<String> {
    let bytes = segment.as_bytes();
    let mut decoded = Vec::with_capacity(segment.len());
    let mut index = 0usize;

    while index < bytes.len() {
        if bytes[index] == b'%' {
            decoded.push((hex_value(bytes[index + 1]) << 4) | hex_value(bytes[index + 2]));
            index += 3;
            continue;
        }

        let ch = segment[index..]
            .chars()
            .next()
            .expect("slice at char boundary should yield a char");
        let mut utf8 = [0u8; 4];
        decoded.extend_from_slice(ch.encode_utf8(&mut utf8).as_bytes());
        index += ch.len_utf8();
    }

    String::from_utf8(decoded).map_err(|_| PathError::InvalidPathSegmentCodePoint)
}

fn hex_value(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => 10 + (byte - b'a'),
        b'A'..=b'F' => 10 + (byte - b'A'),
        _ => unreachable!("hex_value only called after percent validation"),
    }
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

fn validate_decoded_path_segment_structure(segment: &str) -> PathResult<()> {
    if segment.contains('\0') {
        return Err(PathError::NulByte);
    }
    if segment.contains('/') {
        return Err(PathError::SlashInSegment);
    }
    if segment.contains('\\') {
        return Err(PathError::Backslash);
    }
    if segment.contains('%') || segment.contains('?') || segment.contains('#') {
        return Err(PathError::InvalidPathSegmentCodePoint);
    }
    if segment.chars().next().is_some_and(is_combining_mark) {
        return Err(PathError::InvalidPathSegmentCodePoint);
    }
    Ok(())
}

fn enforce_precis_segment(segment: &str) -> PathResult<String> {
    UsernameCasePreserved::new()
        .enforce(segment)
        .map(|segment| segment.into_owned())
        .map_err(|_| PathError::InvalidPathSegmentCodePoint)
}

fn normalize_file_path_impl(path: &str) -> PathResult<String> {
    ensure_raw_path_input_len(path)?;
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
    let canonical = format!("/{}", canonical_segments.join("/"));
    ensure_canonical_path_len(&canonical)?;
    Ok(canonical)
}

pub(crate) fn normalize_directory_path(path: &str) -> Result<String, LixError> {
    normalize_directory_path_impl(path).map_err(PathError::into_lix_error)
}

fn normalize_directory_path_impl(path: &str) -> PathResult<String> {
    ensure_raw_path_input_len(path)?;
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
    let canonical = format!("/{}/", normalized_segments.join("/"));
    ensure_canonical_path_len(&canonical)?;
    Ok(canonical)
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

fn ensure_canonical_path_len(path: &str) -> PathResult<()> {
    if path.len() > MAX_CANONICAL_PATH_BYTES {
        Err(PathError::PathTooLong)
    } else {
        Ok(())
    }
}

fn ensure_raw_path_input_len(path: &str) -> PathResult<()> {
    if path.len() > MAX_RAW_PATH_INPUT_BYTES {
        Err(PathError::RawPathInputTooLong)
    } else {
        Ok(())
    }
}

fn ensure_canonical_segment_len(segment: &str) -> PathResult<()> {
    if segment.len() > MAX_CANONICAL_PATH_SEGMENT_BYTES {
        Err(PathError::SegmentTooLong)
    } else {
        Ok(())
    }
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
        .rfind(|segment| !segment.is_empty())
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
            label: "percent-encoded spaces are valid URI syntax but not Lix segment identity",
            kind: LixFixtureKind::File,
            input: "/docs/%20notes.md",
            oracle_accepts: true,
            expected: Err(PathError::InvalidPathSegmentCodePoint),
        },
        LixProfileFixture {
            label: "bidi formatting is rejected by the Lix validator even though iref accepts it",
            kind: LixFixtureKind::File,
            input: "/docs/\u{202E}.md",
            oracle_accepts: true,
            expected: Err(PathError::InvalidPathSegmentCodePoint),
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
            label: "percent-encoded segment text is decoded before storage",
            kind: NormalizationKind::Directory,
            input: "/docs/%43afe%CC%81/",
            expected: "/docs/Café/",
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
                PathError::InvalidPathSegmentCodePoint,
            );
        }
    }

    #[test]
    fn rejects_file_paths_and_segments_over_length_limits() {
        let segment_at_limit = "a".repeat(MAX_CANONICAL_PATH_SEGMENT_BYTES);
        let path_at_limit = format!("/{segment_at_limit}");
        assert_eq!(
            normalize_file_path(&path_at_limit).as_deref(),
            Ok(path_at_limit.as_str())
        );

        let segment_over_limit = "a".repeat(MAX_CANONICAL_PATH_SEGMENT_BYTES + 1);
        assert_path_error(
            normalize_file_path_impl(&format!("/{segment_over_limit}")),
            PathError::SegmentTooLong,
        );
        assert_path_error(
            normalize_path_segment_impl(&segment_over_limit),
            PathError::SegmentTooLong,
        );

        let mut segments = Vec::new();
        let mut raw_len = 1usize;
        while raw_len <= MAX_CANONICAL_PATH_BYTES {
            segments.push("abcd");
            raw_len = 1 + segments.join("/").len();
        }
        assert_path_error(
            normalize_file_path_impl(&format!("/{}", segments.join("/"))),
            PathError::PathTooLong,
        );
    }

    #[test]
    fn rejects_file_paths_with_private_use_and_noncharacter_code_points() {
        for path in ["/docs/\u{E000}.md", "/docs/\u{FDD0}.md"] {
            assert_path_error(
                normalize_file_path_impl(path),
                PathError::InvalidPathSegmentCodePoint,
            );
        }
    }

    #[test]
    fn rejects_file_paths_with_bidi_formatting_characters() {
        for path in ["/docs/\u{200E}.md", "/docs/\u{202E}.md"] {
            assert_path_error(
                normalize_file_path_impl(path),
                PathError::InvalidPathSegmentCodePoint,
            );
        }
    }

    #[test]
    fn rejects_default_ignorable_and_invisible_segment_characters() {
        for path in [
            "/docs/a\u{200B}b.md", // ZERO WIDTH SPACE
            "/docs/a\u{200C}b.md", // ZERO WIDTH NON-JOINER
            "/docs/a\u{200D}b.md", // ZERO WIDTH JOINER
            "/docs/a\u{2060}b.md", // WORD JOINER
            "/docs/a\u{00AD}b.md", // SOFT HYPHEN
            "/docs/a\u{034F}b.md", // COMBINING GRAPHEME JOINER
            "/docs/a\u{180E}b.md", // MONGOLIAN VOWEL SEPARATOR
            "/docs/a\u{FEFF}b.md", // ZERO WIDTH NO-BREAK SPACE
        ] {
            assert_path_error(
                normalize_file_path_impl(path),
                PathError::InvalidPathSegmentCodePoint,
            );
        }
    }

    #[test]
    fn rejects_unicode_separators_and_leading_combining_marks() {
        for path in [
            "/docs/a\u{00A0}b.md", // NO-BREAK SPACE
            "/docs/a\u{2028}b.md", // LINE SEPARATOR
            "/docs/a\u{2029}b.md", // PARAGRAPH SEPARATOR
            "/docs/\u{0301}.md",   // COMBINING ACUTE ACCENT
        ] {
            assert_path_error(
                normalize_file_path_impl(path),
                PathError::InvalidPathSegmentCodePoint,
            );
        }
    }

    #[test]
    fn validates_percent_encoding_in_file_paths() {
        assert_eq!(
            normalize_file_path("/docs/%43afe%CC%81.md").as_deref(),
            Ok("/docs/Café.md")
        );
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
    fn applies_segment_length_limit_to_canonical_text_not_percent_encoded_boundary_spelling() {
        let encoded_segment_at_limit = "%61".repeat(MAX_CANONICAL_PATH_SEGMENT_BYTES);
        let canonical_segment_at_limit = "a".repeat(MAX_CANONICAL_PATH_SEGMENT_BYTES);
        assert_eq!(
            normalize_file_path(&format!("/{encoded_segment_at_limit}")).as_deref(),
            Ok(format!("/{canonical_segment_at_limit}").as_str())
        );
        assert_eq!(
            normalize_directory_path(&format!("/{encoded_segment_at_limit}/")).as_deref(),
            Ok(format!("/{canonical_segment_at_limit}/").as_str())
        );

        let encoded_segment_over_limit = "%61".repeat(MAX_CANONICAL_PATH_SEGMENT_BYTES + 1);
        assert_path_error(
            normalize_file_path_impl(&format!("/{encoded_segment_over_limit}")),
            PathError::SegmentTooLong,
        );
        assert_path_error(
            normalize_directory_path_impl(&format!("/{encoded_segment_over_limit}/")),
            PathError::SegmentTooLong,
        );
    }

    #[test]
    fn rejects_raw_path_input_over_length_budget_before_unicode_processing() {
        let huge_file_path = format!("/{}", "a".repeat(1024 * 1024));
        assert_path_error(
            normalize_file_path_impl(&huge_file_path),
            PathError::RawPathInputTooLong,
        );

        let huge_directory_path = format!("/{}/", "a".repeat(1024 * 1024));
        assert_path_error(
            normalize_directory_path_impl(&huge_directory_path),
            PathError::RawPathInputTooLong,
        );
    }

    #[test]
    fn rejects_percent_encoded_forbidden_code_points_in_file_paths() {
        for (path, expected) in [
            ("/docs/%00evil.md", PathError::NulByte),
            ("/docs/%2Fevil.md", PathError::SlashInSegment),
            ("/docs/%5Cevil.md", PathError::Backslash),
            ("/docs/%25evil.md", PathError::InvalidPathSegmentCodePoint),
            ("/docs/%3Fevil.md", PathError::InvalidPathSegmentCodePoint),
            ("/docs/%23evil.md", PathError::InvalidPathSegmentCodePoint),
            (
                "/docs/%E2%80%AEevil.md",
                PathError::InvalidPathSegmentCodePoint,
            ),
            (
                "/docs/%E2%80%8Eevil.md",
                PathError::InvalidPathSegmentCodePoint,
            ),
            (
                "/docs/%E2%81%A0evil.md",
                PathError::InvalidPathSegmentCodePoint,
            ),
            (
                "/docs/%C2%ADevil.md",
                PathError::InvalidPathSegmentCodePoint,
            ),
            (
                "/docs/%CD%8Fevil.md",
                PathError::InvalidPathSegmentCodePoint,
            ),
            (
                "/docs/%E1%A0%8Eevil.md",
                PathError::InvalidPathSegmentCodePoint,
            ),
            (
                "/docs/%EF%BB%BFevil.md",
                PathError::InvalidPathSegmentCodePoint,
            ),
            (
                "/docs/%EF%B7%90evil.md",
                PathError::InvalidPathSegmentCodePoint,
            ),
            (
                "/docs/%EE%80%80evil.md",
                PathError::InvalidPathSegmentCodePoint,
            ),
            ("/docs/%FFevil.md", PathError::InvalidPathSegmentCodePoint),
        ] {
            assert_path_error(normalize_file_path_impl(path), expected);
        }
    }

    #[test]
    fn rejects_percent_encoded_forbidden_code_points_in_directory_paths() {
        for (path, expected) in [
            ("/docs/%00evil/", PathError::NulByte),
            ("/docs/%2Fevil/", PathError::SlashInSegment),
            ("/docs/%5Cevil/", PathError::Backslash),
            (
                "/docs/%E2%80%AEevil/",
                PathError::InvalidPathSegmentCodePoint,
            ),
            (
                "/docs/%E2%80%8Eevil/",
                PathError::InvalidPathSegmentCodePoint,
            ),
            (
                "/docs/%E2%81%A0evil/",
                PathError::InvalidPathSegmentCodePoint,
            ),
            (
                "/docs/%EF%BB%BFevil/",
                PathError::InvalidPathSegmentCodePoint,
            ),
            (
                "/docs/%EF%B7%90evil/",
                PathError::InvalidPathSegmentCodePoint,
            ),
            (
                "/docs/%EE%80%80evil/",
                PathError::InvalidPathSegmentCodePoint,
            ),
            ("/docs/%FFevil/", PathError::InvalidPathSegmentCodePoint),
        ] {
            assert_path_error(normalize_directory_path_impl(path), expected);
        }
    }

    #[test]
    fn canonicalizes_percent_encoding_in_file_paths() {
        assert_eq!(
            normalize_file_path("/docs/%7e%41%2e%2E.md").as_deref(),
            Ok("/docs/~A...md")
        );
        assert_path_error(
            normalize_file_path_impl("/docs/%2fkept%3aencoded"),
            PathError::SlashInSegment,
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
        for path in ["/", "/docs/", "/docs/guides/", "/unicodé/章节/"] {
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
            PathError::InvalidPathSegmentCodePoint,
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
            normalize_directory_path("/docs/%43afe%CC%81/").as_deref(),
            Ok("/docs/Café/")
        );
    }

    #[test]
    fn rejects_directory_paths_and_segments_over_length_limits() {
        let segment_at_limit = "a".repeat(MAX_CANONICAL_PATH_SEGMENT_BYTES);
        let path_at_limit = format!("/{segment_at_limit}/");
        assert_eq!(
            normalize_directory_path(&path_at_limit).as_deref(),
            Ok(path_at_limit.as_str())
        );

        let segment_over_limit = "a".repeat(MAX_CANONICAL_PATH_SEGMENT_BYTES + 1);
        assert_path_error(
            normalize_directory_path_impl(&format!("/{segment_over_limit}/")),
            PathError::SegmentTooLong,
        );

        let mut segments = Vec::new();
        let mut raw_len = 1usize;
        while raw_len <= MAX_CANONICAL_PATH_BYTES {
            segments.push("abcd");
            raw_len = 2 + segments.join("/").len();
        }
        assert_path_error(
            normalize_directory_path_impl(&format!("/{}/", segments.join("/"))),
            PathError::PathTooLong,
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
            Some(
                "use valid percent triplets only for URI boundary input; '%' is not allowed in canonical path segments"
            )
        );

        let root_file = normalize_file_path("/").expect_err("root as file");
        assert_eq!(root_file.code, "LIX_ERROR_PATH_INVALID_ROOT_USAGE");
        assert_eq!(
            root_file.hint(),
            Some("use '/' as a directory path, never as a file path")
        );

        let long_segment = normalize_file_path(&format!(
            "/{}",
            "a".repeat(MAX_CANONICAL_PATH_SEGMENT_BYTES + 1)
        ))
        .expect_err("long segment");
        assert_eq!(long_segment.code, "LIX_ERROR_PATH_SEGMENT_TOO_LONG");
        assert_eq!(
            long_segment.hint(),
            Some("keep each canonical path segment at or below 255 bytes")
        );

        let long_input =
            normalize_file_path(&format!("/{}", "a".repeat(MAX_RAW_PATH_INPUT_BYTES + 1)))
                .expect_err("long raw input");
        assert_eq!(long_input.code, "LIX_ERROR_PATH_INPUT_TOO_LONG");
        assert_eq!(
            long_input.hint(),
            Some("keep raw path input at or below 16384 bytes")
        );
    }
}
