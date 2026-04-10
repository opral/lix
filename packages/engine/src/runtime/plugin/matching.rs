#[allow(unused_imports)]
pub(crate) use crate::contracts::{glob_matches_path, select_best_glob_match};

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
    fn match_path_glob_matches_paths() {
        assert!(glob_matches_path("*.{md,mdx}", "/notes.md"));
        assert!(glob_matches_path("*.{md,mdx}", "/notes.MDX"));
        assert!(glob_matches_path("docs/**/*.md", "docs/nested/readme.md"));
        assert!(glob_matches_path("**/*.mdx", "/docs/nested/readme.mdx"));
        assert!(!glob_matches_path("*.{md,mdx}", "/notes.json"));
        assert!(!glob_matches_path("docs/**/*.md", "notes/readme.md"));
    }

    #[test]
    fn match_path_glob_invalid_pattern_does_not_match() {
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
