use crate::error::CliError;
use lix_rs_sdk::{ExecuteResult, Value};

#[derive(Debug)]
pub struct CommandOutput {
    pub hints: Vec<String>,
}

impl CommandOutput {
    pub fn empty() -> Self {
        Self { hints: Vec::new() }
    }

    pub fn with_hints(hints: Vec<String>) -> Self {
        Self { hints }
    }
}

// ── Hint generators (all hint text and conditions live here) ─────────

pub fn hint_after_init() -> Vec<String> {
    vec![
        "Try inserting data with: lix sql execute \"INSERT INTO lix_key_value (key, value) VALUES ('hello', '\"world\"')\"".into(),
        "Store files with: lix sql execute \"INSERT INTO lix_file (path, data) VALUES ('/readme.txt', lix_text_encode('hello'))\"".into(),
    ]
}

pub fn hint_blob_in_result(result: &ExecuteResult) -> Vec<String> {
    let has_blob = result
        .rows()
        .iter()
        .any(|row| row.values().iter().any(|v| matches!(v, Value::Blob(_))));
    if has_blob {
        vec!["Tip: use lix_text_decode(data) to view text content".into()]
    } else {
        Vec::new()
    }
}

/// Extract an engine-produced hint from a `CliError`, if any.
///
/// Returns an empty `Vec` for error variants that do not carry a `LixError`
/// (e.g. `InvalidArgs`, `Message`, `Io`) or when the underlying `LixError`
/// has no hint attached.
pub fn hint_from_error(err: &CliError) -> Vec<String> {
    err.hint().map(|h| vec![h.to_string()]).unwrap_or_default()
}

// ── Infrastructure ───────────────────────────────────────────────────

/// Query lix_key_value for 'lix_cli_hints'. Returns true unless value is explicitly "false".
pub fn are_hints_enabled(lix: &crate::db::FileLix) -> bool {
    let result = crate::db::block_on(lix.execute(
        "SELECT value FROM lix_key_value WHERE key = 'lix_cli_hints'",
        &[],
    ));
    match result {
        Ok(result) => {
            if let Some(row) = result.rows().first() {
                if let Ok(value) = row.get::<String>("value") {
                    return value != "false";
                }
            }
            true // key absent = hints ON
        }
        Err(_) => true, // on error, default to hints ON
    }
}

/// Print hints to stderr as "hint: {message}".
pub fn render_hints(hints: &[String]) {
    for hint in hints {
        eprintln!("hint: {hint}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lix_rs_sdk::LixError;

    #[test]
    fn hint_from_error_returns_empty_for_non_lix_variants() {
        assert!(hint_from_error(&CliError::msg("oops")).is_empty());
        assert!(hint_from_error(&CliError::InvalidArgs("bad")).is_empty());
    }

    #[test]
    fn hint_from_error_returns_empty_when_lix_error_has_no_hint() {
        let cli_err = CliError::from_lix("ctx", LixError::new("CODE", "desc"));
        assert!(hint_from_error(&cli_err).is_empty());
    }

    #[test]
    fn hint_from_error_returns_lix_hint() {
        let cli_err = CliError::from_lix(
            "sql execution failed",
            LixError::new("CODE", "desc").with_hint("use lix_json(...)"),
        );
        assert_eq!(
            hint_from_error(&cli_err),
            vec!["use lix_json(...)".to_string()]
        );
    }
}
