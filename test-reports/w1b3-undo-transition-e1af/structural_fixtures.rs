//! Compiled parser fixtures for the W1b-3 candidate source contract.
//!
//! These are source-shaped fixtures, not Lix or adapter code. They make the
//! negative cases executable: comments and strings cannot satisfy a source
//! gate, omitted helper/commit calls fail, and aliases/second authorities
//! fail closed.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Function<'a> {
    body: &'a str,
}

fn mask_non_code(source: &str) -> String {
    let mut output = String::with_capacity(source.len());
    let bytes = source.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index..].starts_with(b"//") {
            output.push_str("  ");
            index += 2;
            while index < bytes.len() && bytes[index] != b'\n' {
                output.push(' ');
                index += 1;
            }
            continue;
        }
        if bytes[index..].starts_with(b"/*") {
            output.push_str("  ");
            index += 2;
            while index + 1 < bytes.len() && !bytes[index..].starts_with(b"*/") {
                output.push(if bytes[index] == b'\n' { '\n' } else { ' ' });
                index += 1;
            }
            if index + 1 < bytes.len() {
                output.push_str("  ");
                index += 2;
            }
            continue;
        }
        if bytes[index] == b'"' {
            output.push(' ');
            index += 1;
            let mut escaped = false;
            while index < bytes.len() {
                let byte = bytes[index];
                output.push(if byte == b'\n' { '\n' } else { ' ' });
                index += 1;
                if escaped {
                    escaped = false;
                } else if byte == b'\\' {
                    escaped = true;
                } else if byte == b'"' {
                    break;
                }
            }
            continue;
        }
        output.push(bytes[index] as char);
        index += 1;
    }
    output
}

fn body<'a>(source: &'a str, name: &str) -> Option<Function<'a>> {
    let masked = mask_non_code(source);
    let marker = format!("fn {name}");
    let start = masked.find(&marker)?;
    let open = masked[start..].find('{')? + start;
    let mut depth = 0;
    for (offset, byte) in masked.as_bytes()[open..].iter().enumerate() {
        match byte {
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(Function {
                        body: &source[open + 1..open + offset],
                    });
                }
            }
            _ => {}
        }
    }
    None
}

fn call_count(source: &str, name: &str) -> usize {
    let masked = mask_non_code(source);
    masked.matches(&format!("{name}(")).count()
}

fn accepts(source: &str) -> bool {
    let masked = mask_non_code(source);
    let Some(operation) = body(source, "undo_in_transaction") else {
        return false;
    };
    let operation_code = mask_non_code(operation.body);
    if call_count(operation.body, "ForkTreeReadFacade::from_opening_read") != 1
        || !operation_code.contains("forktree_read")
        || operation_code.contains("begin_read(")
        || operation_code.contains("commit_graph_reader(")
        || operation_code.contains("raw_store")
        || operation_code.contains("fallback")
        || operation_code.contains("cache")
        || operation_code.contains("compat")
        || operation_code.contains("let alias = forktree_read")
        || call_count(operation.body, "prepare_typed_transition") != 1
        || call_count(operation.body, "commit_atomic") != 1
    {
        return false;
    }
    for helper in [
        "semantic_state_at",
        "operation_marker_at",
        "apply_state_diff",
    ] {
        let call = format!("{helper}(forktree_read");
        if !operation_code.contains(&call) {
            return false;
        }
    }
    masked.contains("fn semantic_state_at(forktree_read: &ForkTreeReadFacade)")
        && masked.contains("fn operation_marker_at(forktree_read: &ForkTreeReadFacade)")
        && masked.contains("fn apply_state_diff(forktree_read: &ForkTreeReadFacade)")
}

fn green_fixture() -> &'static str {
    r#"
        struct ForkTreeReadFacade;
        async fn semantic_state_at(forktree_read: &ForkTreeReadFacade) {}
        async fn operation_marker_at(forktree_read: &ForkTreeReadFacade) {}
        async fn apply_state_diff(forktree_read: &ForkTreeReadFacade) {}
        async fn undo_in_transaction(transaction: &Transaction) {
            let forktree_read = ForkTreeReadFacade::from_opening_read(
                transaction.opening_read()
            );
            semantic_state_at(forktree_read).await;
            operation_marker_at(forktree_read).await;
            apply_state_diff(forktree_read).await;
            prepare_typed_transition(forktree_read).await;
            commit_atomic(forktree_read).await;
        }
    "#
}

#[test]
fn positive_fixture_requires_real_calls_and_arguments() {
    assert!(accepts(green_fixture()));
}

#[test]
fn comments_and_strings_cannot_fake_the_contract() {
    let source = r#"
        // fn undo_in_transaction() { ForkTreeReadFacade::from_opening_read(transaction.opening_read()); }
        fn undo_in_transaction(transaction: &Transaction) {
            let text = "prepare_typed_transition(forktree_read); commit_atomic(forktree_read)";
        }
    "#;
    assert!(!accepts(source));
}

#[test]
fn omitted_helper_or_commit_calls_fail() {
    let missing = green_fixture().replace("apply_state_diff(forktree_read).await;", "");
    assert!(!accepts(&missing));
    let missing_commit = green_fixture().replace("commit_atomic(forktree_read).await;", "");
    assert!(!accepts(&missing_commit));
}

#[test]
fn second_reader_alias_and_legacy_paths_fail() {
    let alias = green_fixture().replace(
        "apply_state_diff(forktree_read).await;",
        "let alias = forktree_read; apply_state_diff(alias).await;",
    );
    assert!(!accepts(&alias));
    let second = green_fixture().replace(
        "prepare_typed_transition(forktree_read).await;",
        "transaction.begin_read(); prepare_typed_transition(forktree_read).await;",
    );
    assert!(!accepts(&second));
    let fallback = green_fixture().replace(
        "prepare_typed_transition(forktree_read).await;",
        "fallback_cache(forktree_read).await; prepare_typed_transition(forktree_read).await;",
    );
    assert!(!accepts(&fallback));
}
