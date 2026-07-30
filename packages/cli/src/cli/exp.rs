use clap::{Args, Subcommand, ValueEnum, ValueHint, value_parser};
use std::path::PathBuf;

#[derive(Debug, Args)]
pub struct ExpCommand {
    #[command(subcommand)]
    pub command: ExpSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum ExpSubcommand {
    /// Replay Git history into a storage-backed Lix database.
    GitReplay(ExpGitReplayArgs),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum GitReplayStorage {
    Rocksdb,
    Slatedb,
}

impl GitReplayStorage {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Rocksdb => "rocksdb",
            Self::Slatedb => "slatedb",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum GitReplayPlugins {
    All,
    None,
}

impl GitReplayPlugins {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::None => "none",
        }
    }
}

#[derive(Debug, Args)]
pub struct ExpGitReplayArgs {
    /// Path to the git repository to replay.
    #[arg(long, value_hint = ValueHint::DirPath)]
    pub repo_path: PathBuf,

    /// Empty output directory for the replayed database.
    #[arg(long, value_hint = ValueHint::DirPath)]
    pub output_path: PathBuf,

    /// Storage adapter used for the replay.
    #[arg(long, value_enum, default_value_t = GitReplayStorage::Rocksdb)]
    pub storage: GitReplayStorage,

    /// Install all bundled semantic plugins or run the no-plugin control.
    #[arg(long, value_enum, default_value_t = GitReplayPlugins::All)]
    pub plugins: GitReplayPlugins,

    /// One branch or ref whose first-parent history will be replayed.
    #[arg(long, default_value = "main")]
    pub branch: String,

    /// Start replay from this commit (inclusive). Its parent tree is seeded before timed replay.
    #[arg(long)]
    pub from_commit: Option<String>,

    /// Maximum number of commits to replay (after applying --from-commit, if set).
    #[arg(long, value_parser = value_parser!(u32).range(1..))]
    pub num_commits: Option<u32>,

    /// Create a Lix checkpoint after every N replayed Git commits.
    #[arg(long, value_parser = value_parser!(u32).range(1..))]
    pub checkpoint_every: Option<u32>,

    /// Verify changed files after each commit and the complete final tree.
    #[arg(long, default_value_t = false)]
    pub verify_state: bool,

    /// Replace an existing RocksDB output directory and output files.
    #[arg(long, default_value_t = false)]
    pub force: bool,

    /// Write per-commit replay profiling data as JSON.
    #[arg(long, value_hint = ValueHint::FilePath)]
    pub profile_json: Option<PathBuf>,
}
