use clap::{Args, Subcommand, ValueHint, value_parser};
use std::path::PathBuf;

#[derive(Debug, Args)]
pub struct ExpCommand {
    #[command(subcommand)]
    pub command: ExpSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum ExpSubcommand {
    /// Replay Git history into a RocksDB-backed Lix database.
    GitReplay(ExpGitReplayArgs),
}

#[derive(Debug, Args)]
pub struct ExpGitReplayArgs {
    /// Path to the git repository to replay.
    #[arg(long, value_hint = ValueHint::DirPath)]
    pub repo_path: PathBuf,

    /// Empty output directory for the replayed RocksDB database.
    #[arg(long, value_hint = ValueHint::DirPath)]
    pub output_rocksdb_path: PathBuf,

    /// One branch or ref whose first-parent history will be replayed.
    #[arg(long, default_value = "main")]
    pub branch: String,

    /// Start replay from this commit (inclusive). Its parent tree is seeded before timed replay.
    #[arg(long)]
    pub from_commit: Option<String>,

    /// Maximum number of commits to replay (after applying --from-commit, if set).
    #[arg(long, value_parser = value_parser!(u32).range(1..))]
    pub num_commits: Option<u32>,

    /// Verify file paths and payload hashes after each replayed commit.
    #[arg(long, default_value_t = false)]
    pub verify_state: bool,

    /// Replace an existing RocksDB output directory and output files.
    #[arg(long, default_value_t = false)]
    pub force: bool,

    /// Write per-commit replay profiling data as JSON.
    #[arg(long, value_hint = ValueHint::FilePath)]
    pub profile_json: Option<PathBuf>,
}
