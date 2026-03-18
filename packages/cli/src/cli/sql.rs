use clap::{Args, Subcommand, ValueEnum};

#[derive(Debug, Args)]
pub struct SqlCommand {
    #[command(subcommand)]
    pub command: SqlSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum SqlSubcommand {
    /// Execute SQL text. Use '-' to read SQL from stdin.
    #[command(after_long_help = "\
Examples:
  lix sql execute \"INSERT INTO lix_file (path, data) VALUES ('/hello.md', lix_text_encode('# Hello'))\"
  lix sql execute \"SELECT path, lix_text_decode(data) FROM lix_file\"
  lix sql execute \"SELECT path, lixcol_depth FROM lix_file_history\"")]
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

    /// Bind positional SQL parameters from a JSON array.
    ///
    /// Use inline JSON (`--params '[1,true,null,\"text\"]'`) or `-` to read JSON from stdin.
    /// Supported values: null, booleans, numbers, strings, and blobs via {"$blob":"<base64>"}.
    #[arg(long = "params")]
    pub params: Option<String>,

    /// SQL query text to execute. Use '-' to read from stdin.
    pub sql: String,
}
