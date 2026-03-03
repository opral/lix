use clap::{value_parser, Args, Subcommand, ValueHint};
use std::path::PathBuf;

#[derive(Debug, Args)]
pub struct ExpCommand {
    #[command(subcommand)]
    pub command: ExpSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum ExpSubcommand {
    /// Replay git history into a Lix artifact.
    GitReplay(ExpGitReplayArgs),
}

#[derive(Debug, Args)]
pub struct ExpGitReplayArgs {
    /// Path to the git repository to replay.
    #[arg(long, value_hint = ValueHint::DirPath)]
    pub repo_path: PathBuf,

    /// Output .lix path.
    #[arg(long, value_hint = ValueHint::FilePath)]
    pub output_lix_path: PathBuf,

    /// Branch/ref to replay from (use '*' to replay commits reachable from all refs).
    #[arg(long, default_value = "main")]
    pub branch: String,

    /// Start replay from this commit (inclusive).
    #[arg(long)]
    pub from_commit: Option<String>,

    /// Maximum number of commits to replay (after applying --from-commit, if set).
    #[arg(long, value_parser = value_parser!(u32).range(1..))]
    pub num_commits: Option<u32>,

    /// Verify file paths and payload hashes after each replayed commit.
    #[arg(long, default_value_t = false)]
    pub verify_state: bool,
}
