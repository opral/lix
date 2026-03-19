use lix_rs_sdk::{ExecuteResult, Lix, Value};

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

pub fn hint_sqlite_master_query(sql: &str) -> Vec<String> {
    let sql_lower = sql.to_lowercase();
    if sql_lower.contains("sqlite_master") || sql_lower.contains("sqlite_schema") {
        vec![
            "lix uses virtual tables not visible in sqlite_master. Query lix_registered_schema, lix_file, lix_state, or lix_key_value instead.".into(),
        ]
    } else {
        Vec::new()
    }
}

pub fn hint_blob_in_result(result: &ExecuteResult) -> Vec<String> {
    let has_blob = result.statements.iter().any(|stmt| {
        stmt.rows
            .iter()
            .any(|row| row.iter().any(|v| matches!(v, Value::Blob(_))))
    });
    if has_blob {
        vec!["Tip: use lix_text_decode(data) to view text content".into()]
    } else {
        Vec::new()
    }
}

// ── Infrastructure ───────────────────────────────────────────────────

/// Query lix_key_value for 'lix_cli_hints'. Returns true unless value is explicitly "false".
pub fn are_hints_enabled(lix: &Lix) -> bool {
    let result = pollster::block_on(lix.execute(
        "SELECT value FROM lix_key_value WHERE key = 'lix_cli_hints'",
        &[],
    ));
    match result {
        Ok(result) => {
            if let Some(stmt) = result.statements.first() {
                if let Some(row) = stmt.rows.first() {
                    if let Some(lix_rs_sdk::Value::Text(value)) = row.first() {
                        return value != "false";
                    }
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
