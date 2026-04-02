use std::fs;
use std::path::Path;

const ROWSET_RUNTIME_FILE: &str = "src/read_runtime/rowset.rs";

#[test]
fn rowset_runtime_does_not_import_sql_ast_or_compiler_ir() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let path = manifest_dir.join(ROWSET_RUNTIME_FILE);
    let text = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));

    for forbidden in [
        "use sqlparser::ast::",
        "use crate::sql::ast::",
        "use crate::sql::binder::",
        "use crate::sql::logical_plan::",
        "use crate::sql::optimizer::",
        "use crate::sql::parser::",
        "use crate::sql::semantic_ir::",
    ] {
        assert!(
            !text.contains(forbidden),
            "bounded rowset runtime should not import SQL AST or compiler IR\nfile: {}\nforbidden: {}",
            ROWSET_RUNTIME_FILE,
            forbidden,
        );
    }
}
