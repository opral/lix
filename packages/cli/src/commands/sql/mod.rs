mod execute;

use crate::app::AppContext;
use crate::cli::sql::{SqlCommand, SqlSubcommand};
use crate::error::CliError;

pub fn run(context: &AppContext, command: SqlCommand) -> Result<(), CliError> {
    match command.command {
        SqlSubcommand::Execute(args) => execute::run(context, args),
    }
}
