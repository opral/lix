use std::fs;
use std::path::Path;

const PUBLIC_READ_SQL_FILE: &str = "src/public_read_sql.rs";

#[test]
fn public_read_sql_is_explicitly_transitional() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let path = manifest_dir.join(PUBLIC_READ_SQL_FILE);
    let text = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));

    assert!(
        text.contains("Transitional surface-family SQL builders."),
        "public_read_sql.rs should document itself as transitional\nfile: {}",
        PUBLIC_READ_SQL_FILE,
    );
}

#[test]
fn public_read_sql_no_longer_defines_compiler_owned_helper_cluster() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let path = manifest_dir.join(PUBLIC_READ_SQL_FILE);
    let text = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));

    for forbidden in [
        "fn split_effective_state_pushdown_predicates(",
        "fn render_where_clause_sql(",
        "fn render_qualified_where_clause_sql(",
        "fn render_qualified_predicate_sql(",
        "fn json_array_text_join_sql(",
        "fn expr_references_identifier(",
        "fn expr_contains_string_literal(",
        "fn render_identifier(",
        "fn escape_sql_string(",
        "fn quote_ident(",
    ] {
        assert!(
            !text.contains(forbidden),
            "public_read_sql.rs should not define the generic compiler-owned helper cluster locally\nfile: {}\nforbidden: {}",
            PUBLIC_READ_SQL_FILE,
            forbidden,
        );
    }
}
