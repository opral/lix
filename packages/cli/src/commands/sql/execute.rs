use crate::app::AppContext;
use crate::cli::sql::{SqlExecuteArgs, SqlOutputFormat};
use crate::db;
use crate::error::CliError;
use crate::output;
use lix_rs_sdk::Value;
use std::io::Read;

pub fn run(context: &AppContext, args: SqlExecuteArgs) -> Result<(), CliError> {
    let sql = resolve_sql(&args)?;
    let lix_path = db::resolve_db_path(context)?;
    let lix = db::open_lix_at(&lix_path)?;
    let result = pollster::block_on(lix.execute(&sql, &[] as &[Value]))
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
