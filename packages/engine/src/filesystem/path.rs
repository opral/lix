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
        return Err(LixError {
            message: "path segment must not be empty".to_string(),
        });
    }
    if normalized == "." || normalized == ".." {
        return Err(LixError {
            message: "path segment cannot be '.' or '..'".to_string(),
        });
    }
    if normalized.contains('/') || normalized.contains('\\') {
        return Err(LixError {
            message: "path segment must not contain slashes".to_string(),
        });
    }
    Ok(normalized)
}

pub(crate) fn normalize_file_path(path: &str) -> Result<String, LixError> {
    let normalized = path.nfc().collect::<String>();
    if !normalized.starts_with('/') || normalized.ends_with('/') || normalized == "/" {
        return Err(LixError {
            message: format!("lix_file_descriptor: Invalid file path {path}"),
        });
    }
    if normalized.contains('\\') || normalized.contains("//") {
        return Err(LixError {
            message: format!("lix_file_descriptor: Invalid file path {path}"),
        });
    }
    let segments = normalized
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    if segments.is_empty() {
        return Err(LixError {
            message: format!("lix_file_descriptor: Invalid file path {path}"),
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
        return Err(LixError {
            message: format!("lix_directory_descriptor: Invalid directory path {path}"),
        });
    }
    if normalized.contains('\\') || normalized.contains("//") {
        return Err(LixError {
            message: format!("lix_directory_descriptor: Invalid directory path {path}"),
        });
    }
    let mut normalized_segments: Vec<String> = Vec::new();
    for segment in normalized.split('/').filter(|segment| !segment.is_empty()) {
        normalized_segments.push(normalize_path_segment(segment)?);
    }
    if normalized_segments.is_empty() {
        return Err(LixError {
            message: format!("lix_directory_descriptor: Invalid directory path {path}"),
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
    let file_name = segments.last().ok_or_else(|| LixError {
        message: format!("Invalid file path {path}"),
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
        Err(LixError {
            message: format!("Invalid directory parent path {parent_path}"),
        })
    }
}
