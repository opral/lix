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

    /// Source version id to branch from. Defaults to the active version.
    #[arg(long, conflicts_with = "from_name")]
    pub from_id: Option<String>,

    /// Source version name to branch from. Defaults to the active version.
    #[arg(long, conflicts_with = "from_id")]
    pub from_name: Option<String>,

    /// Hide the version from default listings.
    #[arg(long, default_value_t = false)]
    pub hidden: bool,
}

#[derive(Debug, Args)]
pub struct MergeVersionCommand {
    /// Source version id to merge from.
    #[arg(long, conflicts_with = "source_name", required_unless_present = "source_name")]
    pub source_id: Option<String>,

    /// Source version name to merge from.
    #[arg(long, conflicts_with = "source_id", required_unless_present = "source_id")]
    pub source_name: Option<String>,

    /// Target version id to merge into.
    #[arg(long, conflicts_with = "target_name", required_unless_present = "target_name")]
    pub target_id: Option<String>,

    /// Target version name to merge into.
    #[arg(long, conflicts_with = "target_id", required_unless_present = "target_id")]
    pub target_name: Option<String>,
}

#[derive(Debug, Args)]
pub struct SwitchVersionCommand {
    /// Version id to make active.
    #[arg(long, conflicts_with = "name", required_unless_present = "name")]
    pub id: Option<String>,

    /// Version name to make active.
    #[arg(long, conflicts_with = "id", required_unless_present = "id")]
    pub name: Option<String>,
}
