use clap::{Args, Subcommand};

#[derive(Debug, Args)]
pub struct VersionCommand {
    #[command(subcommand)]
    pub command: VersionSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum VersionSubcommand {
    /// Create a new version from the current active version head.
    Create(CreateVersionCommand),
    /// Merge one version into another.
    Merge(MergeVersionCommand),
    /// Switch the active version.
    Switch(SwitchVersionCommand),
}

#[derive(Debug, Args)]
pub struct CreateVersionCommand {
    /// Explicit version id. If omitted, Lix generates one.
    #[arg(long)]
    pub id: Option<String>,

    /// Human-readable version name. Defaults to the id.
    #[arg(long)]
    pub name: Option<String>,

    /// Hide the version from default listings.
    #[arg(long, default_value_t = false)]
    pub hidden: bool,
}

#[derive(Debug, Args)]
pub struct MergeVersionCommand {
    /// Source version to merge from.
    #[arg(long)]
    pub source: String,

    /// Target version to merge into.
    #[arg(long)]
    pub target: String,
}

#[derive(Debug, Args)]
pub struct SwitchVersionCommand {
    /// Version id to make active.
    pub version_id: String,
}
