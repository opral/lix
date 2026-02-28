use clap::{Args, Subcommand, ValueEnum};

#[derive(Debug, Args)]
pub struct SqlCommand {
    #[command(subcommand)]
    pub command: SqlSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum SqlSubcommand {
    /// Execute SQL text. Use '-' to read SQL from stdin.
    Execute(SqlExecuteArgs),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum SqlOutputFormat {
    Table,
    Json,
}

#[derive(Debug, Args)]
pub struct SqlExecuteArgs {
    /// Output format for query results.
    #[arg(long, value_enum, default_value_t = SqlOutputFormat::Table)]
    pub format: SqlOutputFormat,

    /// SQL query text to execute. Use '-' to read from stdin.
    pub sql: String,
}
