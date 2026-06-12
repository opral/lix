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
//! - File paths never end with `/`.
//! - Directory paths always end with `/`.
//! - Empty, `.`, and `..` segments are rejected because they do not name stable
//!   Lix filesystem entries.
//! - `/` is only a separator, so a standalone segment cannot contain `/`.
//! - Root is represented as the directory path `/`.
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
//! - File path: slash-rendered path naming a file, without a trailing slash.
//! - Directory path: slash-rendered path naming a directory, with a trailing
//!   slash.
//! - Internal path form: the Unicode-bearing representation used by the engine.

#![allow(
    clippy::redundant_closure_for_method_calls,
    clippy::semicolon_if_nothing_returned
)]

use crate::LixError;

type PathResult<T> = Result<T, PathError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LixPath {
    segments: String,
}

impl LixPath {
    pub(crate) fn try_from_file_path(path: &str) -> Result<Self, LixError> {
        let Some(path) = path.strip_prefix('/') else {
            return Err(PathError::MissingLeadingSlash.into_lix_error());
        };
        if path.is_empty() {
            return Err(PathError::InvalidRootUsage.into_lix_error());
        }
        if path.ends_with('/') {
            return Err(PathError::UnexpectedTrailingSlashOnFilePath.into_lix_error());
        }
        path.split('/')
            .try_for_each(validate_segment)
            .map_err(PathError::into_lix_error)?;
        Ok(Self {
            segments: path.to_owned(),
        })
    }

    pub(crate) fn try_from_directory_path(path: &str) -> Result<Self, LixError> {
        let Some(path) = path.strip_prefix('/') else {
            return Err(PathError::MissingLeadingSlash.into_lix_error());
        };
        if !path.is_empty() {
            let Some(path_) = path.strip_suffix('/') else {
                return Err(PathError::MissingTrailingSlashOnDirectoryPath.into_lix_error());
            };
            path_
                .split('/')
                .try_for_each(validate_segment)
                .map_err(PathError::into_lix_error)?;
        }
        Ok(Self {
            segments: path.to_owned(),
        })
    }

    pub(crate) fn segments(&self) -> impl Iterator<Item = &str> {
        (!self.segments.is_empty())
            .then_some(&self.segments)
            .into_iter()
            .flat_map(|segments| segments.strip_suffix('/').unwrap_or(segments).split('/'))
    }

    pub(crate) fn to_file_path(&self) -> String {
        assert!(!self.segments.is_empty() && !self.segments.ends_with('/'));
        format!("/{}", self.segments)
    }

    pub(crate) fn to_directory_path(&self) -> String {
        assert!(self.segments.is_empty() || self.segments.ends_with('/'));
        format!("/{}", self.segments)
    }
}

fn validate_segment(segment: &str) -> PathResult<()> {
    if segment.is_empty() {
        return Err(PathError::EmptySegment);
    }
    if segment == "." || segment == ".." {
        return Err(PathError::DotSegment);
    }
    debug_assert!(!segment.contains('/'));
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PathError {
    MissingLeadingSlash,
    UnexpectedTrailingSlashOnFilePath,
    MissingTrailingSlashOnDirectoryPath,
    EmptySegment,
    DotSegment,
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
            Self::InvalidRootUsage => (
                "LIX_ERROR_PATH_INVALID_ROOT_USAGE",
                "root '/' is only valid as a directory path",
                Some("use '/' as a directory path, never as a file path"),
            ),
            Self::InvalidDirectoryParentPath => (
                "LIX_ERROR_PATH_INVALID_DIRECTORY_PARENT",
                "directory parent path must be '/' or end with '/'",
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

fn renderable_segment_text(segment: &str) -> PathResult<&str> {
    if segment.is_empty() {
        return Err(PathError::EmptySegment);
    }
    if segment == "." || segment == ".." {
        return Err(PathError::DotSegment);
    }
    if segment.contains('/') {
        return Err(PathError::SlashInSegment);
    }
    Ok(segment)
}

pub(crate) fn compose_file_path(
    directory_path: Option<&str>,
    name: &str,
) -> Result<String, LixError> {
    let name_text = renderable_segment_text(name).map_err(PathError::into_lix_error)?;
    let parent_path = directory_path.unwrap_or("/");
    if parent_path == "/" {
        Ok(format!("/{name_text}"))
    } else if parent_path.starts_with('/') && parent_path.ends_with('/') {
        Ok(format!("{parent_path}{name_text}"))
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
        Ok(format!("/{name_text}/"))
    } else if parent_path.starts_with('/') && parent_path.ends_with('/') {
        Ok(format!("{parent_path}{name_text}/"))
    } else {
        Err(PathError::InvalidDirectoryParentPath.into_lix_error())
    }
}
