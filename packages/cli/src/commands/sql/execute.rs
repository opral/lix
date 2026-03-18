use crate::app::AppContext;
use crate::cli::sql::{SqlExecuteArgs, SqlOutputFormat};
use crate::db;
use crate::error::CliError;
use crate::hints::{self, CommandOutput};
use crate::output;
use base64::Engine as _;
use lix_rs_sdk::Value;
use serde_json::Value as JsonValue;
use std::io::Read;

pub fn run(context: &AppContext, args: SqlExecuteArgs) -> Result<CommandOutput, CliError> {
    let (sql, params) = resolve_sql_and_params(&args)?;
    let lix_path = db::resolve_db_path(context)?;
    let lix = db::open_lix_at(&lix_path)?;
    let result = pollster::block_on(lix.execute(&sql, &params))
        .map_err(|err| CliError::msg(format!("sql execution failed: {err}")))?;

    match args.format {
        SqlOutputFormat::Json => output::print_execute_result_json(&result),
        SqlOutputFormat::Table => output::print_execute_result_table(&result),
    }

    let output_hints = if context.no_hints || !hints::are_hints_enabled(&lix) {
        Vec::new()
    } else {
        let mut h = hints::hint_sqlite_master_query(&sql);
        h.extend(hints::hint_blob_in_result(&result));
        h
    };

    Ok(CommandOutput::with_hints(output_hints))
}

fn resolve_sql_and_params(args: &SqlExecuteArgs) -> Result<(String, Vec<Value>), CliError> {
    let sql_from_stdin = args.sql == "-";
    let params_from_stdin = args.params.as_deref() == Some("-");
    if sql_from_stdin && params_from_stdin {
        return Err(CliError::InvalidArgs(
            "sql and params cannot both be read from stdin",
        ));
    }

    let stdin_payload = if sql_from_stdin {
        Some(read_stdin("failed to read SQL from stdin")?)
    } else if params_from_stdin {
        Some(read_stdin("failed to read params JSON from stdin")?)
    } else {
        None
    };

    let sql = if sql_from_stdin {
        let input = stdin_payload
            .as_deref()
            .ok_or(CliError::InvalidArgs("stdin SQL input is empty"))?;
        if input.trim().is_empty() {
            return Err(CliError::InvalidArgs("stdin SQL input is empty"));
        }
        input.to_string()
    } else {
        args.sql.clone()
    };

    let params = resolve_params(args.params.as_deref(), stdin_payload.as_deref())?;
    Ok((sql, params))
}

fn read_stdin(context: &'static str) -> Result<String, CliError> {
    let mut input = String::new();
    std::io::stdin()
        .read_to_string(&mut input)
        .map_err(|source| CliError::io(context, source))?;
    Ok(input)
}

fn resolve_params(
    params_input: Option<&str>,
    stdin_payload: Option<&str>,
) -> Result<Vec<Value>, CliError> {
    let Some(raw_params) = params_input else {
        return Ok(Vec::new());
    };

    let json_text = if raw_params == "-" {
        let input =
            stdin_payload.ok_or(CliError::InvalidArgs("stdin params JSON input is empty"))?;
        if input.trim().is_empty() {
            return Err(CliError::InvalidArgs("stdin params JSON input is empty"));
        }
        input
    } else {
        raw_params
    };

    parse_params_json(json_text)
}

fn parse_params_json(raw: &str) -> Result<Vec<Value>, CliError> {
    let parsed: JsonValue = serde_json::from_str(raw).map_err(|error| {
        CliError::msg(format!(
            "invalid --params JSON: expected a JSON array, parse error: {error}"
        ))
    })?;

    let values = parsed.as_array().ok_or_else(|| {
        CliError::msg("invalid --params JSON: expected a JSON array of positional parameters")
    })?;

    values
        .iter()
        .enumerate()
        .map(|(index, value)| parse_param_value(value, index))
        .collect::<Result<Vec<_>, _>>()
}

fn parse_param_value(value: &JsonValue, index: usize) -> Result<Value, CliError> {
    match value {
        JsonValue::Null => Ok(Value::Null),
        JsonValue::Bool(v) => Ok(Value::Boolean(*v)),
        JsonValue::Number(v) => {
            if let Some(as_i64) = v.as_i64() {
                return Ok(Value::Integer(as_i64));
            }
            if let Some(as_f64) = v.as_f64() {
                return Ok(Value::Real(as_f64));
            }
            Err(CliError::msg(format!(
                "invalid --params value at index {index}: unsupported number representation"
            )))
        }
        JsonValue::String(v) => Ok(Value::Text(v.clone())),
        JsonValue::Object(map) => parse_object_param(map, index),
        JsonValue::Array(_) => Err(CliError::msg(format!(
            "invalid --params value at index {index}: nested arrays are not supported"
        ))),
    }
}

fn parse_object_param(
    map: &serde_json::Map<String, JsonValue>,
    index: usize,
) -> Result<Value, CliError> {
    if map.len() == 1 && map.contains_key("$blob") {
        let encoded = map
            .get("$blob")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| {
                CliError::msg(format!(
                    "invalid --params value at index {index}: $blob must be a base64 string"
                ))
            })?;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|error| {
                CliError::msg(format!(
                    "invalid --params value at index {index}: $blob is not valid base64: {error}"
                ))
            })?;
        return Ok(Value::Blob(bytes));
    }

    Err(CliError::msg(format!(
        "invalid --params value at index {index}: objects must use only {{\"$blob\":\"<base64>\"}}"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn resolve_params_defaults_to_empty_when_unset() {
        let resolved = resolve_params(None, None).expect("params should resolve");
        assert!(resolved.is_empty());
    }

    #[test]
    fn resolve_params_maps_json_array_values_to_typed_sql_values() {
        let resolved = resolve_params(
            Some("[null, true, 7, 2.5, \"hello\", {\"$blob\":\"aGk=\"}]"),
            None,
        )
        .expect("typed params should resolve");
        assert_eq!(
            resolved,
            vec![
                Value::Null,
                Value::Boolean(true),
                Value::Integer(7),
                Value::Real(2.5),
                Value::Text("hello".to_string()),
                Value::Blob(vec![0x68, 0x69]),
            ]
        );
    }

    #[test]
    fn resolve_params_rejects_non_array_json() {
        let error = resolve_params(Some("{\"a\":1}"), None).expect_err("non-array should fail");
        assert_eq!(
            error.to_string(),
            "invalid --params JSON: expected a JSON array of positional parameters"
        );
    }

    #[test]
    fn resolve_params_rejects_invalid_object_shape() {
        let error =
            resolve_params(Some("[{\"k\":\"v\"}]"), None).expect_err("invalid object should fail");
        assert_eq!(
            error.to_string(),
            "invalid --params value at index 0: objects must use only {\"$blob\":\"<base64>\"}"
        );
    }

    #[test]
    fn resolve_sql_and_params_rejects_double_stdin_usage() {
        let args = SqlExecuteArgs {
            format: SqlOutputFormat::Table,
            params: Some("-".to_string()),
            sql: "-".to_string(),
        };
        let error =
            resolve_sql_and_params(&args).expect_err("double stdin read should be rejected");
        assert_eq!(
            error.to_string(),
            "invalid arguments: sql and params cannot both be read from stdin"
        );
    }

    #[test]
    fn execute_accepts_numbered_placeholders_with_json_params() {
        let handle = std::thread::Builder::new()
            .name("sql-execute-param-binding".to_string())
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                let path = test_lix_path("param-binding");
                db::init_lix_at(&path).expect("init test lix file");
                let context = AppContext {
                    lix_path: Some(path.clone()),
                    no_hints: true,
                };
                let args = SqlExecuteArgs {
                    format: SqlOutputFormat::Json,
                    params: Some("[\"left\", \"right\"]".to_string()),
                    sql: "SELECT ?1 AS first_value, ?2 AS second_value".to_string(),
                };

                let result = run(&context, args);
                let _ = std::fs::remove_file(&path);
                assert!(
                    result.is_ok(),
                    "expected sql execute to succeed: {result:?}"
                );
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
