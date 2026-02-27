use clap::{Args, Subcommand};

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

#[derive(Debug, Args)]
pub struct SqlExecuteArgs {
    /// SQL query text to execute. Use '-' to read from stdin.
    pub sql: String,
}
