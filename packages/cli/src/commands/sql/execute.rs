use crate::app::AppContext;
use crate::cli::sql::{SqlExecuteArgs, SqlOutputFormat};
use crate::db;
use crate::error::CliError;
use crate::output;
use lix_rs_sdk::Value;
use std::io::Read;

pub fn run(context: &AppContext, args: SqlExecuteArgs) -> Result<(), CliError> {
    let sql = resolve_sql(&args)?;
    let params = resolve_params(&args);
    let lix_path = db::resolve_db_path(context)?;
    let lix = db::open_lix_at(&lix_path)?;
    let result = pollster::block_on(lix.execute(&sql, &params))
        .map_err(|err| CliError::msg(format!("sql execution failed: {err}")))?;

    match args.format {
        SqlOutputFormat::Json => output::print_query_result_json(&result),
        SqlOutputFormat::Table => output::print_query_result_table(&result),
    }

    Ok(())
}

fn resolve_sql(args: &SqlExecuteArgs) -> Result<String, CliError> {
    if args.sql == "-" {
        let mut input = String::new();
        std::io::stdin()
            .read_to_string(&mut input)
            .map_err(|source| CliError::io("failed to read SQL from stdin", source))?;
        if input.trim().is_empty() {
            return Err(CliError::InvalidArgs("stdin SQL input is empty"));
        }
        return Ok(input);
    }

    Ok(args.sql.clone())
}

fn resolve_params(args: &SqlExecuteArgs) -> Vec<Value> {
    args.params
        .iter()
        .cloned()
        .map(Value::Text)
        .collect::<Vec<_>>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn resolve_params_maps_param_flags_to_text_values() {
        let args = SqlExecuteArgs {
            format: SqlOutputFormat::Table,
            params: vec!["alpha".to_string(), "beta".to_string()],
            sql: "SELECT 1".to_string(),
        };

        let resolved = resolve_params(&args);
        assert_eq!(
            resolved,
            vec![
                Value::Text("alpha".to_string()),
                Value::Text("beta".to_string())
            ]
        );
    }

    #[test]
    fn execute_accepts_numbered_placeholders_with_param_flags() {
        let handle = std::thread::Builder::new()
            .name("sql-execute-param-binding".to_string())
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                let path = test_lix_path("param-binding");
                std::fs::File::create(&path).expect("create test lix file");
                let context = AppContext {
                    lix_path: Some(path.clone()),
                };
                let args = SqlExecuteArgs {
                    format: SqlOutputFormat::Json,
                    params: vec!["left".to_string(), "right".to_string()],
                    sql: "SELECT ?1 AS first_value, ?2 AS second_value".to_string(),
                };

                let result = run(&context, args);
                let _ = std::fs::remove_file(&path);
                assert!(result.is_ok(), "expected sql execute to succeed: {result:?}");
            })
            .expect("spawn test thread");

        handle.join().expect("test thread joins");
    }

    fn test_lix_path(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock after unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("lix-cli-{label}-{nonce}.lix"))
    }
}
