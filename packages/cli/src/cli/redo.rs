use clap::Args;

#[derive(Debug, Args)]
pub struct RedoCommand {
    /// Target branch id. Defaults to the active branch.
    #[arg(long)]
    pub branch: Option<String>,
}
