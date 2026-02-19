use globset::GlobBuilder;

pub(crate) fn select_best_glob_match<'a, T, C: Copy + PartialEq>(
    path: &str,
    file_content_type: Option<C>,
    candidates: &'a [T],
    glob: impl Fn(&T) -> &str,
    required_content_type: impl Fn(&T) -> Option<C>,
) -> Option<&'a T> {
    let mut selected: Option<&T> = None;
    let mut selected_rank: Option<(u8, i32)> = None;

    for candidate in candidates {
        let pattern = glob(candidate);
        if !glob_matches_path(pattern, path) {
            continue;
        }
        if let (Some(actual_type), Some(required_type)) =
            (file_content_type, required_content_type(candidate))
        {
            if actual_type != required_type {
                continue;
            }
        }

        let rank = glob_specificity_rank(pattern);
        match selected_rank {
            None => {
                selected = Some(candidate);
                selected_rank = Some(rank);
            }
            Some(existing_rank) if rank > existing_rank => {
                selected = Some(candidate);
                selected_rank = Some(rank);
            }
            _ => {
                // Keep the existing winner on equal rank to preserve candidate-order tie-break.
            }
        }
    }

    selected
}

pub(crate) fn glob_matches_path(glob: &str, path: &str) -> bool {
    let normalized_glob = glob.trim();
    let normalized_path = path.trim();
    if normalized_glob.is_empty() || normalized_path.is_empty() {
        return false;
    }
    if is_catch_all_glob(normalized_glob) {
        return true;
    }

    GlobBuilder::new(normalized_glob)
        .literal_separator(false)
        .case_insensitive(true)
        .build()
        .map(|compiled| compiled.compile_matcher().is_match(normalized_path))
        .unwrap_or(false)
}

fn glob_specificity_rank(glob: &str) -> (u8, i32) {
    let normalized = glob.trim();
    if is_catch_all_glob(normalized) {
        return (0, i32::MIN);
    }
    (1, glob_specificity_score(normalized))
}

fn glob_specificity_score(glob: &str) -> i32 {
    let mut literal_chars = 0i32;
    let mut wildcard_chars = 0i32;
    for ch in glob.chars() {
        match ch {
            '*' | '?' | '[' | ']' | '{' | '}' => wildcard_chars += 1,
            _ => literal_chars += 1,
        }
    }
    literal_chars - wildcard_chars
}

fn is_catch_all_glob(glob: &str) -> bool {
    glob == "*" || glob == "**/*" || glob == "**"
}

#[cfg(test)]
mod tests {
    use super::{glob_matches_path, select_best_glob_match};
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum ContentType {
        Text,
        Binary,
    }

    #[derive(Debug)]
    struct Candidate {
        id: &'static str,
        glob: &'static str,
        content_type: Option<ContentType>,
    }

    #[test]
    fn detect_changes_glob_matches_paths() {
        assert!(glob_matches_path("*.{md,mdx}", "/notes.md"));
        assert!(glob_matches_path("*.{md,mdx}", "/notes.MDX"));
        assert!(glob_matches_path("docs/**/*.md", "docs/nested/readme.md"));
        assert!(glob_matches_path("**/*.mdx", "/docs/nested/readme.mdx"));
        assert!(!glob_matches_path("*.{md,mdx}", "/notes.json"));
        assert!(!glob_matches_path("docs/**/*.md", "notes/readme.md"));
    }

    #[test]
    fn detect_changes_glob_invalid_pattern_does_not_match() {
        assert!(!glob_matches_path("*.{md,mdx", "/notes.md"));
    }

    #[test]
    fn prefers_specific_glob_over_catch_all() {
        let candidates = vec![
            Candidate {
                id: "text",
                glob: "*",
                content_type: None,
            },
            Candidate {
                id: "markdown",
                glob: "*.{md,mdx}",
                content_type: None,
            },
        ];

        let selected = select_best_glob_match(
            "/docs/readme.md",
            None,
            &candidates,
            |c| c.glob,
            |c| c.content_type,
        )
        .expect("should select markdown");
        assert_eq!(selected.id, "markdown");
    }

    #[test]
    fn prefers_more_specific_glob_by_score() {
        let candidates = vec![
            Candidate {
                id: "generic-md",
                glob: "*.md",
                content_type: None,
            },
            Candidate {
                id: "docs-md",
                glob: "docs/**/*.md",
                content_type: None,
            },
        ];

        let selected = select_best_glob_match(
            "docs/nested/readme.md",
            None,
            &candidates,
            |c| c.glob,
            |c| c.content_type,
        )
        .expect("should select a match");
        assert_eq!(selected.id, "docs-md");
    }

    #[test]
    fn equal_specificity_uses_candidate_order_as_tie_break() {
        let candidates = vec![
            Candidate {
                id: "first",
                glob: "*.md",
                content_type: None,
            },
            Candidate {
                id: "second",
                glob: "*.md",
                content_type: None,
            },
        ];

        let selected = select_best_glob_match(
            "readme.md",
            None,
            &candidates,
            |c| c.glob,
            |c| c.content_type,
        )
        .expect("should match");
        assert_eq!(selected.id, "first");
    }

    #[test]
    fn applies_content_type_filter_when_available() {
        let candidates = vec![
            Candidate {
                id: "text",
                glob: "*",
                content_type: Some(ContentType::Text),
            },
            Candidate {
                id: "binary",
                glob: "*",
                content_type: Some(ContentType::Binary),
            },
        ];

        let selected = select_best_glob_match(
            "images/logo.png",
            Some(ContentType::Binary),
            &candidates,
            |c| c.glob,
            |c| c.content_type,
        )
        .expect("should match binary handler");
        assert_eq!(selected.id, "binary");
    }

    #[test]
    fn does_not_filter_by_content_type_when_unknown() {
        let candidates = vec![Candidate {
            id: "text",
            glob: "*",
            content_type: Some(ContentType::Text),
        }];

        let selected = select_best_glob_match(
            "notes.txt",
            None,
            &candidates,
            |c| c.glob,
            |c| c.content_type,
        );
        assert!(selected.is_some());
    }
}
