#[cfg(test)]
pub(crate) use crate::statement_support::PlaceholderRef;
pub(crate) use crate::statement_support::{
    parse_placeholder_ref, resolve_placeholder_index, resolve_placeholder_ref, PlaceholderState,
};

#[cfg(test)]
mod tests {
    use super::{
        parse_placeholder_ref, resolve_placeholder_index, resolve_placeholder_ref, PlaceholderRef,
        PlaceholderState,
    };

    #[test]
    fn parses_placeholder_kinds() {
        assert_eq!(parse_placeholder_ref("?").unwrap(), PlaceholderRef::Next);
        assert_eq!(
            parse_placeholder_ref(" ?3 ").unwrap(),
            PlaceholderRef::Explicit(3)
        );
        assert_eq!(
            parse_placeholder_ref("$2").unwrap(),
            PlaceholderRef::Explicit(2)
        );
    }

    #[test]
    fn advances_ordinal_placeholders() {
        let mut state = PlaceholderState::new();

        assert_eq!(
            resolve_placeholder_ref(PlaceholderRef::Next, 3, &mut state).unwrap(),
            0
        );
        assert_eq!(
            resolve_placeholder_ref(PlaceholderRef::Next, 3, &mut state).unwrap(),
            1
        );
        assert_eq!(
            resolve_placeholder_ref(PlaceholderRef::Explicit(3), 3, &mut state).unwrap(),
            2
        );
    }

    #[test]
    fn preserves_legacy_token_entrypoint() {
        let mut state = PlaceholderState::new();

        assert_eq!(resolve_placeholder_index("?", 3, &mut state).unwrap(), 0);
        assert_eq!(resolve_placeholder_index("?", 3, &mut state).unwrap(), 1);
        assert_eq!(resolve_placeholder_index("?3", 3, &mut state).unwrap(), 2);
    }

    #[test]
    fn rejects_out_of_range_placeholders() {
        let mut state = PlaceholderState::new();
        let error = resolve_placeholder_index("$2", 1, &mut state).unwrap_err();

        assert!(error.description.contains("parameter 2"));
    }
}
