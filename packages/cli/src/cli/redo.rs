use clap::Args;

#[derive(Debug, Args)]
pub struct RedoCommand {
    /// Override the target version by `lix_version.id` / active `version_id`,
    /// not the `lix_active_version.id` row key.
    #[arg(long)]
    pub version: Option<String>,
}
