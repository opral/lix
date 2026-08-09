//! Minimal real-repository Git/LFS replay driver.
//!
//! This benchmark-only binary includes the exact `lix_cli` replay module and
//! supplies only its argument, error, and blocking adapters. It does not link
//! or dispatch the full CLI command surface, and it never reimplements replay,
//! plugin, prepared-CAS, or final-tree semantics.

mod cli {
    pub mod exp {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../cli/src/cli/exp.rs"
        ));
    }
}

mod error {
    include!(concat!(env!("CARGO_MANIFEST_DIR"), "/../cli/src/error.rs"));
}

mod db {
    pub use lix_storage_rocksdb::RocksDB;
    pub use lix_storage_slatedb::SlateDB;
    use std::future::{Future, IntoFuture};

    pub fn block_on<F>(future: F) -> F::Output
    where
        F: IntoFuture,
        F::IntoFuture: Future<Output = F::Output>,
    {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("real Git replay runtime should initialize")
            .block_on(future.into_future())
    }
}

mod git_replay {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../cli/src/commands/exp/git_replay.rs"
    ));
}

use cli::exp::{ExpGitReplayArgs, GitReplayParentTree, GitReplayPlugins, GitReplayStorage};
use lix::storage::Storage;
use lix::{Value, open_lix};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::ffi::CString;
use std::path::{Path, PathBuf};
use std::process::Command;

#[derive(Debug)]
struct ReplayDriverArgs {
    repo_path: PathBuf,
    output_path: PathBuf,
    storage: String,
    from_commit: String,
    to_commit: String,
    num_commits: u32,
    parent_tree: String,
    checkpoint_every: Option<u32>,
    profile_json: PathBuf,
    force: bool,
}

impl ReplayDriverArgs {
    fn parse() -> Self {
        let mut args = std::env::args().skip(1);
        let mut repo_path = None;
        let mut output_path = None;
        let mut storage = "rocksdb".to_owned();
        let mut from_commit = None;
        let mut to_commit = None;
        let mut num_commits = 32;
        let mut parent_tree = "full".to_owned();
        let mut checkpoint_every = None;
        let mut profile_json = None;
        let mut force = false;

        while let Some(flag) = args.next() {
            match flag.as_str() {
                "--repo-path" => repo_path = Some(PathBuf::from(required(&mut args, &flag))),
                "--output-path" => output_path = Some(PathBuf::from(required(&mut args, &flag))),
                "--storage" => storage = required(&mut args, &flag),
                "--from-commit" => from_commit = Some(required(&mut args, &flag)),
                "--to-commit" => to_commit = Some(required(&mut args, &flag)),
                "--num-commits" => {
                    num_commits = required(&mut args, &flag)
                        .parse()
                        .unwrap_or_else(|_| usage("--num-commits must be a positive integer"));
                    if num_commits == 0 {
                        usage("--num-commits must be positive");
                    }
                }
                "--parent-tree" => parent_tree = required(&mut args, &flag),
                "--checkpoint-every" => {
                    checkpoint_every =
                        Some(required(&mut args, &flag).parse().unwrap_or_else(|_| {
                            usage("--checkpoint-every must be a positive integer")
                        }));
                }
                "--profile-json" => profile_json = Some(PathBuf::from(required(&mut args, &flag))),
                "--force" => force = true,
                "--plugins" => {
                    if required(&mut args, &flag) != "all" {
                        usage("this driver is fixed to --plugins all");
                    }
                }
                "--help" | "-h" => usage(""),
                other => usage(&format!("unknown argument {other}")),
            }
        }

        let Some(repo_path) = repo_path else {
            usage("--repo-path is required");
        };
        let Some(output_path) = output_path else {
            usage("--output-path is required");
        };
        let Some(from_commit) = from_commit else {
            usage("--from-commit is required");
        };
        let Some(to_commit) = to_commit else {
            usage("--to-commit is required");
        };
        if !matches!(storage.as_str(), "rocksdb" | "slatedb") {
            usage("--storage must be rocksdb or slatedb");
        }
        if !matches!(parent_tree.as_str(), "full" | "window") {
            usage("--parent-tree must be full or window");
        }
        let profile_json = profile_json.unwrap_or_else(|| output_path.join("replay-profile.json"));
        Self {
            repo_path,
            output_path,
            storage,
            from_commit,
            to_commit,
            num_commits,
            parent_tree,
            checkpoint_every,
            profile_json,
            force,
        }
    }
}

fn required(args: &mut impl Iterator<Item = String>, flag: &str) -> String {
    args.next()
        .unwrap_or_else(|| usage(&format!("missing value for {flag}")))
}

fn usage(message: &str) -> ! {
    if !message.is_empty() {
        eprintln!("{message}");
    }
    eprintln!(
        "usage: git_replay_real_repo --repo-path PATH --from-commit COMMIT \
         --to-commit COMMIT --output-path PATH [--num-commits N] \
         [--storage rocksdb|slatedb] \
         [--plugins all] [--parent-tree full|window] [--checkpoint-every N] \
         [--profile-json PATH] [--force]"
    );
    std::process::exit(2)
}

#[derive(Clone, Copy, Debug)]
struct ResourceSample {
    user_us: u64,
    system_us: u64,
    max_rss_kib: u64,
    read_bytes: u64,
    write_bytes: u64,
    disk_free_bytes: u64,
}

fn main() {
    let driver = ReplayDriverArgs::parse();
    let before = sample_resources(&driver.output_path);
    eprintln!(
        "[git-replay-real] plugins=all repo={} from={} to={} storage={} profile={} resources_before={before:?}",
        driver.repo_path.display(),
        driver.from_commit,
        driver.to_commit,
        driver.storage,
        driver.profile_json.display(),
    );
    git_replay::run(to_cli_args(&driver)).unwrap_or_else(|error| {
        eprintln!("[git-replay-real] failed: {error}");
        std::process::exit(1);
    });

    let digest = match driver.storage.as_str() {
        "rocksdb" => db::block_on(public_digest_rocks(&driver.output_path)),
        "slatedb" => db::block_on(public_digest_slate(&driver.output_path)),
        _ => unreachable!(),
    }
    .unwrap_or_else(|error| {
        eprintln!("[git-replay-real] public digest failed: {error}");
        std::process::exit(1);
    });
    let after = sample_resources(&driver.output_path);
    let tree_oid = git_tree_oid(&driver.repo_path, &driver.to_commit).unwrap_or_else(|error| {
        eprintln!("[git-replay-real] Git tree digest failed: {error}");
        std::process::exit(1);
    });
    println!(
        "[git-replay-real] verified=true git_tree_oid={tree_oid} public_semantic_sha256={} public_rows={} resources_before={before:?} resources_after={after:?}",
        digest.sha256, digest.rows
    );
}

fn to_cli_args(driver: &ReplayDriverArgs) -> ExpGitReplayArgs {
    ExpGitReplayArgs {
        repo_path: driver.repo_path.clone(),
        output_path: driver.output_path.clone(),
        storage: match driver.storage.as_str() {
            "rocksdb" => GitReplayStorage::Rocksdb,
            "slatedb" => GitReplayStorage::Slatedb,
            _ => unreachable!(),
        },
        plugins: GitReplayPlugins::All,
        branch: driver.to_commit.clone(),
        from_commit: Some(driver.from_commit.clone()),
        parent_tree: match driver.parent_tree.as_str() {
            "full" => GitReplayParentTree::Full,
            "window" => GitReplayParentTree::Window,
            _ => unreachable!(),
        },
        num_commits: Some(driver.num_commits),
        checkpoint_every: driver.checkpoint_every,
        force: driver.force,
        profile_json: Some(driver.profile_json.clone()),
    }
}

#[derive(Debug)]
struct PublicDigest {
    sha256: String,
    rows: usize,
}

async fn public_digest_rocks(output_path: &Path) -> Result<PublicDigest, String> {
    let storage = db::RocksDB::open(output_path)
        .map_err(|error| format!("open RocksDB for public digest: {error}"))?;
    digest_lix(storage).await
}

async fn public_digest_slate(output_path: &Path) -> Result<PublicDigest, String> {
    let storage = db::SlateDB::open(output_path)
        .map_err(|error| format!("open SlateDB for public digest: {error}"))?;
    digest_lix(storage).await
}

async fn digest_lix<S>(storage: S) -> Result<PublicDigest, String>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let lix = open_lix()
        .with_storage(storage)
        .await
        .map_err(|error| format!("open public digest Lix: {error}"))?;
    let paths = [
        "/plugin_csv.wasm",
        "/plugin_excalidraw.wasm",
        "/plugin_markdown.wasm",
        "/plugin_text.wasm",
    ];
    let params = paths
        .iter()
        .map(|path| Value::Text((*path).to_owned()))
        .collect::<Vec<_>>();
    let result = lix
        .execute(
            "SELECT path, content, lixcol_metadata FROM lix_file \
             WHERE path NOT LIKE '/.lix/plugins/%' AND path NOT IN (?, ?, ?, ?) \
             ORDER BY path",
            &params,
        )
        .await
        .map_err(|error| format!("query public digest rows: {error}"))?;
    let mut hash = Sha256::new();
    for row in result.rows() {
        let path = match row.get_index(0) {
            Some(Value::Text(value)) => value,
            _ => return Err("public digest path is not text".to_owned()),
        };
        hash.update((path.len() as u64).to_le_bytes());
        hash.update(path.as_bytes());
        match row.get_index(1) {
            Some(Value::Null) => hash.update([0]),
            Some(Value::Blob(bytes)) => {
                hash.update([1]);
                hash.update((bytes.len() as u64).to_le_bytes());
                hash.update(bytes);
            }
            _ => return Err(format!("public digest content is invalid for {path}")),
        }
        let metadata = match row.get_index(2) {
            Some(Value::Json(value)) => canonical_json(value),
            _ => return Err(format!("public digest metadata is invalid for {path}")),
        };
        let metadata = serde_json::to_vec(&metadata).map_err(|error| error.to_string())?;
        hash.update((metadata.len() as u64).to_le_bytes());
        hash.update(metadata);
    }
    let digest = hash.finalize();
    Ok(PublicDigest {
        sha256: hex(&digest),
        rows: result.rows().len(),
    })
}

fn canonical_json(value: &JsonValue) -> JsonValue {
    match value {
        JsonValue::Object(object) => {
            let mut sorted = BTreeMap::new();
            for (key, value) in object {
                sorted.insert(key.clone(), canonical_json(value));
            }
            JsonValue::Object(sorted.into_iter().collect())
        }
        JsonValue::Array(values) => JsonValue::Array(values.iter().map(canonical_json).collect()),
        value => value.clone(),
    }
}

fn git_tree_oid(repo_path: &Path, commit: &str) -> Result<String, String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo_path)
        .args(["rev-parse", "--verify"])
        .arg(format!("{commit}^{{tree}}"))
        .output()
        .map_err(|error| format!("spawn git tree digest: {error}"))?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).trim().to_owned());
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn sample_resources(disk_path: &Path) -> ResourceSample {
    let (user_us, system_us, max_rss_kib) = process_cpu_rss();
    let disk_path = if disk_path.exists() {
        disk_path
    } else {
        disk_path.parent().unwrap_or_else(|| Path::new("."))
    };
    ResourceSample {
        user_us,
        system_us,
        max_rss_kib,
        read_bytes: proc_counter("/proc/self/io", "read_bytes:"),
        write_bytes: proc_counter("/proc/self/io", "write_bytes:"),
        disk_free_bytes: disk_free_bytes(disk_path),
    }
}

fn proc_counter(path: &str, key: &str) -> u64 {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|contents| {
            contents.lines().find_map(|line| {
                let mut fields = line.split_ascii_whitespace();
                (fields.next() == Some(key))
                    .then(|| fields.next())
                    .flatten()
                    .and_then(|value| value.parse().ok())
            })
        })
        .unwrap_or_default()
}

fn process_cpu_rss() -> (u64, u64, u64) {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if result != 0 {
        return (0, 0, proc_counter("/proc/self/status", "VmHWM:"));
    }
    let usage = unsafe { usage.assume_init() };
    let micros = |value: libc::timeval| {
        (value.tv_sec.max(0) as u64)
            .saturating_mul(1_000_000)
            .saturating_add(value.tv_usec.max(0) as u64)
    };
    (
        micros(usage.ru_utime),
        micros(usage.ru_stime),
        usage.ru_maxrss as u64,
    )
}

fn disk_free_bytes(path: &Path) -> u64 {
    let Ok(path) = CString::new(path.to_string_lossy().as_bytes()) else {
        return 0;
    };
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::zeroed();
    if unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) } != 0 {
        return 0;
    }
    let stat = unsafe { stat.assume_init() };
    stat.f_bavail.saturating_mul(stat.f_frsize)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
