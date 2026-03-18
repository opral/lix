use crate::cli::init::InitCommand;
use crate::db;
use crate::error::CliError;
use crate::hints::{self, CommandOutput};

pub fn run(command: InitCommand) -> Result<CommandOutput, CliError> {
    let initialized = db::init_lix_at(&command.path)?;
    if initialized {
        println!("initialized {}", command.path.display());
    } else {
        println!("already initialized {}", command.path.display());
    }
    Ok(CommandOutput::with_hints(hints::hint_after_init()))
}
