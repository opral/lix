use crate::cli::init::InitCommand;
use crate::db;
use crate::error::CliError;

pub fn run(command: InitCommand) -> Result<(), CliError> {
    let created = db::init_lix_at(&command.path)?;
    if created {
        println!("initialized {}", command.path.display());
    } else {
        println!("already initialized {}", command.path.display());
    }
    Ok(())
}
