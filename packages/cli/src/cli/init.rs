use clap::{Args, ValueHint};
use std::path::PathBuf;

#[derive(Debug, Args)]
pub struct InitCommand {
    /// Path to the .lix file to initialize.
    #[arg(value_hint = ValueHint::FilePath)]
    pub path: PathBuf,
}
