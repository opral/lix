use crate::cli::init::InitCommand;
use crate::db;
use crate::error::CliError;

pub fn run(command: InitCommand) -> Result<(), CliError> {
    let initialized = db::init_lix_at(&command.path)?;
    if initialized {
        println!("initialized {}", command.path.display());
    } else {
        println!("already initialized {}", command.path.display());
    }
    Ok(())
}
