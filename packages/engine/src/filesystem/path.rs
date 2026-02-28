use unicode_normalization::UnicodeNormalization;

use crate::LixError;

#[derive(Debug, Clone)]
pub(crate) struct ParsedFilePath {
    pub(crate) normalized_path: String,
    pub(crate) directory_path: Option<String>,
    pub(crate) name: String,
    pub(crate) extension: Option<String>,
}

pub(crate) fn path_depth(path: &str) -> usize {
    path.trim_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .count()
}

pub(crate) fn normalize_path_segment(raw: &str) -> Result<String, LixError> {
    let normalized = raw.nfc().collect::<String>();
    if normalized.is_empty() {
        return Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: "path segment must not be empty".to_string(),
        });
    }
    if normalized == "." || normalized == ".." {
        return Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: "path segment cannot be '.' or '..'".to_string(),
        });
    }
    if normalized.contains('/') || normalized.contains('\\') {
        return Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: "path segment must not contain slashes".to_string(),
        });
    }
    if !segment_has_valid_percent_encoding(&normalized) {
        return Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: "path segment contains invalid percent encoding".to_string(),
        });
    }
    if !normalized.chars().all(is_allowed_segment_char) {
        return Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: "path segment contains unsupported characters".to_string(),
        });
    }
    Ok(normalized)
}

fn is_allowed_segment_char(ch: char) -> bool {
    ch.is_alphanumeric() || matches!(ch, '.' | '_' | '~' | '%' | '-')
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
    if !normalized.starts_with('/') || normalized.ends_with('/') || normalized == "/" {
        return Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: format!("lix_file_descriptor: Invalid file path {path}"),
        });
    }
    if normalized.contains('\\') || normalized.contains("//") {
        return Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: format!("lix_file_descriptor: Invalid file path {path}"),
        });
    }
    let segments = normalized
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    if segments.is_empty() {
        return Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: format!("lix_file_descriptor: Invalid file path {path}"),
        });
    }
    for segment in &segments {
        let _ = normalize_path_segment(segment)?;
    }
    Ok(format!("/{}", segments.join("/").nfc().collect::<String>()))
}

pub(crate) fn normalize_directory_path(path: &str) -> Result<String, LixError> {
    let normalized = path.nfc().collect::<String>();
    if !normalized.starts_with('/') || !normalized.ends_with('/') || normalized == "/" {
        return Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: format!("lix_directory_descriptor: Invalid directory path {path}"),
        });
    }
    if normalized.contains('\\') || normalized.contains("//") {
        return Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: format!("lix_directory_descriptor: Invalid directory path {path}"),
        });
    }
    let mut normalized_segments: Vec<String> = Vec::new();
    for segment in normalized.split('/').filter(|segment| !segment.is_empty()) {
        normalized_segments.push(normalize_path_segment(segment)?);
    }
    if normalized_segments.is_empty() {
        return Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: format!("lix_directory_descriptor: Invalid directory path {path}"),
        });
    }
    Ok(format!("/{}/", normalized_segments.join("/")))
}

pub(crate) fn parse_file_path(path: &str) -> Result<ParsedFilePath, LixError> {
    let normalized_path = normalize_file_path(path)?;
    let segments = normalized_path
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    let file_name = segments.last().ok_or_else(|| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: format!("Invalid file path {path}"),
    })?;
    let directory_path = if segments.len() > 1 {
        Some(format!("/{}/", segments[..segments.len() - 1].join("/")))
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
        normalized_path,
        directory_path,
        name,
        extension,
    })
}

pub(crate) fn file_ancestor_directory_paths(path: &str) -> Vec<String> {
    ancestor_directory_paths(path)
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
        Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: format!("Invalid directory parent path {parent_path}"),
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
        for path in ["/docs/./file", "/docs/../file"] {
            assert!(
                normalize_file_path(path).is_err(),
                "expected invalid dot-segment path {path}"
            );
        }
    }

    #[test]
    fn rejects_file_paths_with_reserved_characters() {
        for path in ["/docs/file?.md", "/docs/#hash", "/docs/foo:bar"] {
            assert!(
                normalize_file_path(path).is_err(),
                "expected invalid reserved-char path {path}"
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
    fn accepts_and_rejects_directory_paths_like_legacy_rules() {
        for path in ["/docs/", "/docs/guides/", "/unicodé/章节/", "/docs/%20/"] {
            assert!(
                normalize_directory_path(path).is_ok(),
                "expected valid directory path {path}"
            );
        }
        for path in [
            "/",
            "/file.md",
            "/docs",
            "/docs/ ",
            "no-leading",
            "/docs/%zz/",
        ] {
            assert!(
                normalize_directory_path(path).is_err(),
                "expected invalid directory path {path}"
            );
        }
    }
}
