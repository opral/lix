//! Lix filesystem path parsing and rendering lives in this module.
//!
//! Contract:
//!
//! - Internal path text is an absolute slash-separated Lix logical filesystem
//!   path.
//! - Segments are opaque text except for reserved structural sentinels.
//!
//! Slash path shape:
//!
//! - Non-root paths never end with `/`; row kind is carried by the typed
//!   file or directory surface rather than encoded in path text.
//! - Empty, `.`, and `..` segments are rejected because they do not name stable
//!   Lix filesystem entries.
//! - `/` is only a separator, so a standalone segment cannot contain `/`.
//! - NUL is rejected because it cannot be represented by host filesystem APIs.
//! - Root is represented as the directory path `/`.
//! - Segment text is otherwise preserved literally: paths are not URL-decoded,
//!   case-folded, or Unicode-normalized.
//!
//! Runtime strategy:
//!
//! - This module keeps only the structural checks needed to parse and render
//!   slash paths. FilesystemSync is responsible for mapping logical names to
//!   host-native paths.
//!
//! Glossary:
//!
//! - Raw input path: caller-provided path before structural parsing.
//! - Path text: path after structural parsing; segment text is unchanged.
//! - File path: slash-rendered path naming a file.
//! - Directory path: slash-rendered path naming a directory.
//! - Internal path form: the Unicode-bearing representation used by the engine.

#![allow(
    clippy::redundant_closure_for_method_calls,
    clippy::semicolon_if_nothing_returned
)]

use std::fmt;

use crate::LixError;

type PathResult<T> = Result<T, PathError>;

/// A validated absolute Lix logical filesystem path.
///
/// Construct paths with [`LixPath::try_from_file_path`] or
/// [`LixPath::try_from_directory_path`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LixPath {
    path: String,
}

impl LixPath {
    /// Parses an absolute logical file path.
    ///
    /// The root path `/` is rejected because it names a directory.
    pub fn try_from_file_path(path: &str) -> Result<Self, LixError> {
        let Some(path) = path.strip_prefix('/') else {
            return Err(PathError::MissingLeadingSlash.into_lix_error());
        };
        if path.is_empty() {
            return Err(PathError::InvalidRootUsage.into_lix_error());
        }
        if path.ends_with('/') {
            return Err(PathError::UnexpectedTrailingSlash.into_lix_error());
        }
        path.split('/')
            .try_for_each(validate_segment)
            .map_err(PathError::into_lix_error)?;
        Ok(Self {
            path: format!("/{path}"),
        })
    }

    /// Parses an absolute logical directory path, including root `/`.
    pub fn try_from_directory_path(path: &str) -> Result<Self, LixError> {
        let Some(path) = path.strip_prefix('/') else {
            return Err(PathError::MissingLeadingSlash.into_lix_error());
        };
        if !path.is_empty() {
            if path.ends_with('/') {
                return Err(PathError::UnexpectedTrailingSlash.into_lix_error());
            }
            path.split('/')
                .try_for_each(validate_segment)
                .map_err(PathError::into_lix_error)?;
        }
        Ok(Self {
            path: format!("/{path}"),
        })
    }

    /// Returns the canonical absolute logical path text.
    pub fn as_str(&self) -> &str {
        &self.path
    }

    /// Returns the literal path segments without the structural `/` separators.
    pub fn segments(&self) -> impl Iterator<Item = &str> {
        let segments = self.path.strip_prefix('/').unwrap_or(&self.path);
        (!segments.is_empty())
            .then_some(segments)
            .into_iter()
            .flat_map(|segments| segments.split('/'))
    }
}

impl AsRef<str> for LixPath {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for LixPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

fn validate_segment(segment: &str) -> PathResult<()> {
    if segment.is_empty() {
        return Err(PathError::EmptySegment);
    }
    if segment == "." || segment == ".." {
        return Err(PathError::DotSegment);
    }
    if segment.contains('\0') {
        return Err(PathError::Nul);
    }
    if segment.contains('/') {
        return Err(PathError::SlashInSegment);
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PathError {
    MissingLeadingSlash,
    UnexpectedTrailingSlash,
    EmptySegment,
    DotSegment,
    Nul,
    SlashInSegment,
    InvalidRootUsage,
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
            Self::UnexpectedTrailingSlash => (
                "LIX_ERROR_PATH_UNEXPECTED_TRAILING_SLASH",
                "non-root path must not end with '/'",
                Some("remove the trailing slash"),
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
            Self::Nul => (
                "LIX_ERROR_PATH_NUL",
                "path segment cannot contain NUL",
                Some("remove the NUL character from the path segment"),
            ),
            Self::SlashInSegment => (
                "LIX_ERROR_PATH_SLASH_IN_SEGMENT",
                "path segment must not contain '/'",
                Some("pass a single segment name, not a full path"),
            ),
            Self::InvalidRootUsage => (
                "LIX_ERROR_PATH_INVALID_ROOT_USAGE",
                "root '/' is only valid as a directory path",
                Some("use '/' as a directory path, never as a file path"),
            ),
            Self::InvalidDirectoryParentPath => (
                "LIX_ERROR_PATH_INVALID_DIRECTORY_PARENT",
                "directory parent path must be absolute and must not end with '/'",
                Some("pass '/' or a canonical directory path without a trailing slash"),
            ),
        };

        let err = LixError::new(code, message);
        match hint {
            Some(hint) => err.with_hint(hint),
            None => err,
        }
    }
}

fn renderable_segment_text(segment: &str) -> PathResult<&str> {
    validate_segment(segment)?;
    Ok(segment)
}

/// Validates one literal segment of a Lix logical filesystem path.
///
/// Valid segments are non-empty UTF-8 strings other than `.` and `..` and do
/// not contain `/` or NUL. No URL decoding, case folding, or Unicode
/// normalization is performed.
pub fn validate_lix_path_segment(segment: &str) -> Result<(), LixError> {
    renderable_segment_text(segment)
        .map(|_| ())
        .map_err(PathError::into_lix_error)
}

pub(crate) fn compose_file_path(
    directory_path: Option<&str>,
    name: &str,
) -> Result<String, LixError> {
    let name_text = renderable_segment_text(name).map_err(PathError::into_lix_error)?;
    let parent_path = directory_path.unwrap_or("/");
    if parent_path == "/" {
        Ok(format!("/{name_text}"))
    } else if parent_path.starts_with('/') && !parent_path.ends_with('/') {
        Ok(format!("{parent_path}/{name_text}"))
    } else {
        Err(PathError::InvalidDirectoryParentPath.into_lix_error())
    }
}

pub(crate) fn compose_directory_path(
    parent_path: Option<&str>,
    name: &str,
) -> Result<String, LixError> {
    let name_text = renderable_segment_text(name).map_err(PathError::into_lix_error)?;
    let parent_path = parent_path.unwrap_or("/");
    if parent_path == "/" {
        Ok(format!("/{name_text}"))
    } else if parent_path.starts_with('/') && !parent_path.ends_with('/') {
        Ok(format!("{parent_path}/{name_text}"))
    } else {
        Err(PathError::InvalidDirectoryParentPath.into_lix_error())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logical_paths_preserve_literal_utf8_segments() {
        let path = LixPath::try_from_file_path("/docs/100% @2x/日本語.txt").unwrap();

        assert_eq!(
            path.segments().collect::<Vec<_>>(),
            vec!["docs", "100% @2x", "日本語.txt"]
        );
        assert_eq!(path.as_str(), "/docs/100% @2x/日本語.txt");
    }

    #[test]
    fn logical_paths_do_not_normalize_unicode() {
        let composed = LixPath::try_from_file_path("/caf\u{e9}.txt").unwrap();
        let decomposed = LixPath::try_from_file_path("/cafe\u{301}.txt").unwrap();

        assert_ne!(composed, decomposed);
    }

    #[test]
    fn logical_paths_reject_structural_segments_and_nul() {
        for path in ["relative", "/", "/a/", "/a//b", "/a/./b", "/a/../b"] {
            assert!(LixPath::try_from_file_path(path).is_err(), "{path:?}");
        }
        let error = LixPath::try_from_file_path("/nul\0name").unwrap_err();
        assert_eq!(error.code, "LIX_ERROR_PATH_NUL");
    }
}
