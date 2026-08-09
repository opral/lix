//! Test/benchmark-only real-repository comparator for the existing
//! `lix exp git-replay` implementation.
//!
//! This wrapper deliberately delegates Git history, blob, plugin, storage,
//! and final-tree work to the CLI's `git_replay` command with
//! `--plugins all`. It does not reimplement replay or treat Git LFS pointers
//! as file contents. `verify-only` performs one correctness run without the
//! external resource sampler; `timed` enables `/usr/bin/time -v` for a later
//! controlled sample. No timed sample is run by this source target.
//!
//! The semantic digest enumerates every public table advertised by
//! `information_schema.tables`, canonicalizes every row, and hashes the
//! complete ordered table/column/row surface. The Git tree digest is SHA-256
//! over one frozen NUL-delimited `git ls-tree -r -z --full-tree` encoding.
//! Adapter-level calls/bytes are not exposed by the existing CLI profile; the
//! report records that boundary explicitly while retaining the profile's
//! physical execution groups, plugin counters, filesystem counters, RSS, and
//! settled output size.

use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::Instant;

const FROZEN_FROM: &str = "7bffc534f48c2dd1110aff0b7bf618c1d1c030b1";
const FROZEN_TO: &str = "74f6c45c91823e59b72d0a60787fccf482900023";
const FROZEN_COMMIT_COUNT: usize = 32;
const FROZEN_TREE: &str = "15729cd85f5434cc7e056db8cbbf6f7ae6e6cd63";
const FROZEN_TREE_SHA256: &str = "8102fc51c0358b6e916733d99d6949c29f24ffe6ef5650809ee156c7931985fd";
const PUBLIC_TABLE_QUERY: &str = "SELECT table_schema, table_name FROM information_schema.tables WHERE table_catalog = 'datafusion' AND table_schema = 'public' ORDER BY table_schema, table_name";
const KNOWN_PLUGIN_TABLES: &[&str] = &[
    "csv_row",
    "csv_table",
    "excalidraw_element",
    "excalidraw_file",
    "excalidraw_scene",
    "markdown_node",
    "text_line",
];
const LFS_POINTER_VERSION: &str = "version https://git-lfs.github.com/spec/v1";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mode {
    VerifyOnly,
    Timed,
}

impl Mode {
    fn parse(raw: &str) -> Result<Self, String> {
        match raw {
            "verify-only" => Ok(Self::VerifyOnly),
            "timed" => Ok(Self::Timed),
            other => Err(format!(
                "invalid --mode {other}; expected verify-only or timed"
            )),
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::VerifyOnly => "verify-only",
            Self::Timed => "timed",
        }
    }
}

#[derive(Debug)]
struct Config {
    mode: Mode,
    lix_bin: PathBuf,
    repo: PathBuf,
    branch: String,
    from: String,
    to: String,
    output: PathBuf,
    report: PathBuf,
    storage: String,
    parent_tree: String,
    checkpoint_every: Option<u32>,
    lix_source_repo: PathBuf,
    lix_source_commit: String,
    lix_source_tree: String,
    expected_lix_bin_sha256: String,
    harness_source_repo: PathBuf,
    harness_source_commit: String,
    harness_source_tree: String,
    expected_harness_bin_sha256: String,
    force: bool,
}

#[derive(Debug, serde::Serialize)]
struct VerifiedProvenance {
    lix_source_commit: String,
    lix_source_tree: String,
    lix_binary_sha256: String,
    harness_source_commit: String,
    harness_source_tree: String,
    harness_binary_sha256: String,
}

#[derive(Debug)]
struct CommitWindow {
    branch: String,
    from: String,
    to: String,
    count: usize,
    commits: Vec<String>,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("git_replay_real_repo: {error}");
        std::process::exit(2);
    }
}

fn run() -> Result<(), String> {
    let config = Config::parse(std::env::args_os().skip(1))?;
    validate_config(&config)?;
    let provenance = verify_provenance(&config)?;

    let window = resolve_window(&config.repo, &config.branch, &config.from, &config.to)?;
    validate_frozen_window(&window)?;
    let lfs = inspect_lfs(&config.repo, &window)?;

    let tree_oid = git_text(
        &config.repo,
        &["rev-parse", &format!("{}^{{tree}}", window.to)],
    )?;
    if tree_oid != FROZEN_TREE {
        return Err(format!(
            "frozen endpoint tree mismatch: expected {FROZEN_TREE}, got {tree_oid}"
        ));
    }
    let tree_listing = git_capture(&config.repo, &canonical_tree_args(&window.to))?;
    let tree_digest = sha256_hex(&tree_listing.stdout);
    validate_tree_digest(FROZEN_TREE_SHA256, &tree_digest)?;
    let profile_path = profile_path(&config.report);
    ensure_parent(&config.output)?;
    ensure_parent(&config.report)?;
    ensure_parent(&profile_path)?;

    let replay_started = Instant::now();
    let replay = run_replay(&config, &window, &profile_path)?;
    let wall_ms = duration_ms(replay_started.elapsed());
    if !replay.status.success() {
        return Err(format_child_failure("git-replay failed", &replay));
    }

    let profile_bytes = fs::read(&profile_path)
        .map_err(|error| format!("read replay profile {}: {error}", profile_path.display()))?;
    let profile: Value = serde_json::from_slice(&profile_bytes)
        .map_err(|error| format!("parse replay profile {}: {error}", profile_path.display()))?;
    let semantic = read_public_semantic_digest(&config)?;
    let disk_bytes = directory_bytes(&config.output).map_err(|error| {
        format!(
            "measure settled output {}: {error}",
            config.output.display()
        )
    })?;
    let backend = backend_counters(&profile);
    let resource = if config.mode == Mode::Timed {
        parse_time_metrics(&replay.stderr)
    } else {
        json!({
            "mode": "verify-only",
            "external_sampler": "not run",
            "wall_ms": wall_ms,
        })
    };

    let report = json!({
        "schema": "lix.real_git_replay_comparator.v2",
        "mode": config.mode.as_str(),
        "provenance": {
            "verified": provenance,
            "git_replay_plugins": "GitReplayPlugins::All (--plugins all)",
        },
        "fixture": {
            "repo": config.repo,
            "branch": window.branch,
            "from": window.from,
            "to": window.to,
            "commit_count": window.count,
            "selected_first_parent_commits": window.commits,
            "inclusive_window": true,
            "frozen_commit_count": FROZEN_COMMIT_COUNT,
            "pinned_vscode_docs_head": FROZEN_TO,
            "pinned_vscode_docs_tree": FROZEN_TREE,
            "lfs": lfs,
        },
        "git": {
            "tree_listing": "git ls-tree -r -z --full-tree <to>",
            "tree_digest_sha256": tree_digest,
            "expected_tree_digest_sha256": FROZEN_TREE_SHA256,
        },
        "semantic": {
            "canonical_json_digest_sha256": semantic.digest,
            "canonicalization": "public tables ordered by (schema, table); columns retain advertised order; rows ordered by canonical typed JSON bytes",
            "row_bytes": semantic.bytes,
            "table_count": semantic.table_count,
            "row_count": semantic.row_count,
            "tables": semantic.tables,
        },
        "replay": {
            "storage": config.storage,
            "parent_tree": config.parent_tree,
            "checkpoint_every": config.checkpoint_every,
            "profile_path": profile_path,
            "profile_sha256": sha256_hex(&profile_bytes),
            "profile": profile,
        },
        "backend_counters": backend,
        "operation_coverage": {
            "replay": "supported by git_replay with plugins all",
            "commit": "represented by selected first-parent replay commits",
            "branch": "unsupported by this harness; not relabeled",
            "diff": "unsupported by this harness; not relabeled",
            "merge": "unsupported by this harness; not relabeled",
            "adapter_calls_bytes": "unsupported by the current git_replay profile; not inferred",
        },
        "resource_counters": resource,
        "settled_output_bytes": disk_bytes,
        "child_exit_code": replay.status.code(),
    });
    write_json(&config.report, &report)?;
    println!("git_replay_real_repo report={}", config.report.display());
    println!("git_replay_real_repo tree_sha256={tree_digest}");
    println!("git_replay_real_repo semantic_sha256={}", semantic.digest);
    Ok(())
}

impl Config {
    fn parse<I>(args: I) -> Result<Self, String>
    where
        I: IntoIterator<Item = OsString>,
    {
        let mut mode = None;
        let mut lix_bin = None;
        let mut repo = None;
        let mut branch = String::from("main");
        let mut from = None;
        let mut to = None;
        let mut output = None;
        let mut report = None;
        let mut storage = String::from("rocksdb");
        let mut parent_tree = String::from("window");
        let mut checkpoint_every = None;
        let mut lix_source_repo = None;
        let mut lix_source_commit = None;
        let mut lix_source_tree = None;
        let mut expected_lix_bin_sha256 = None;
        let mut harness_source_repo = None;
        let mut harness_source_commit = None;
        let mut harness_source_tree = None;
        let mut expected_harness_bin_sha256 = None;
        let mut force = false;
        let mut args = args.into_iter();

        while let Some(raw) = args.next() {
            let flag = raw
                .to_str()
                .ok_or_else(|| "arguments must be valid UTF-8".to_string())?;
            if flag == "--help" || flag == "-h" {
                print_help();
                std::process::exit(0);
            }
            if flag == "--force" {
                force = true;
                continue;
            }
            let mut value = || {
                args.next()
                    .ok_or_else(|| format!("missing value after {flag}"))
            };
            match flag {
                "--mode" => mode = Some(Mode::parse(value()?.to_str().ok_or("invalid mode")?)?),
                "--lix-bin" => lix_bin = Some(PathBuf::from(value()?)),
                "--repo" => repo = Some(PathBuf::from(value()?)),
                "--branch" => branch = value()?.to_str().ok_or("invalid branch")?.to_owned(),
                "--from" => from = Some(value()?.to_str().ok_or("invalid from")?.to_owned()),
                "--to" => to = Some(value()?.to_str().ok_or("invalid to")?.to_owned()),
                "--output" => output = Some(PathBuf::from(value()?)),
                "--report" => report = Some(PathBuf::from(value()?)),
                "--storage" => storage = value()?.to_str().ok_or("invalid storage")?.to_owned(),
                "--parent-tree" => {
                    parent_tree = value()?.to_str().ok_or("invalid parent-tree")?.to_owned()
                }
                "--checkpoint-every" => {
                    checkpoint_every = Some(
                        value()?
                            .to_str()
                            .ok_or("invalid checkpoint interval")?
                            .parse::<u32>()
                            .map_err(|error| format!("invalid --checkpoint-every: {error}"))?,
                    )
                }
                "--lix-source-repo" => lix_source_repo = Some(PathBuf::from(value()?)),
                "--lix-source-commit" => {
                    lix_source_commit = Some(
                        value()?
                            .to_str()
                            .ok_or("invalid Lix source commit")?
                            .to_owned(),
                    )
                }
                "--lix-source-tree" => {
                    lix_source_tree = Some(
                        value()?
                            .to_str()
                            .ok_or("invalid Lix source tree")?
                            .to_owned(),
                    )
                }
                "--expected-lix-bin-sha256" => {
                    expected_lix_bin_sha256 = Some(
                        value()?
                            .to_str()
                            .ok_or("invalid Lix binary SHA-256")?
                            .to_owned(),
                    )
                }
                "--harness-source-repo" => harness_source_repo = Some(PathBuf::from(value()?)),
                "--harness-source-commit" => {
                    harness_source_commit = Some(
                        value()?
                            .to_str()
                            .ok_or("invalid harness source commit")?
                            .to_owned(),
                    )
                }
                "--harness-source-tree" => {
                    harness_source_tree = Some(
                        value()?
                            .to_str()
                            .ok_or("invalid harness source tree")?
                            .to_owned(),
                    )
                }
                "--expected-harness-bin-sha256" => {
                    expected_harness_bin_sha256 = Some(
                        value()?
                            .to_str()
                            .ok_or("invalid harness binary SHA-256")?
                            .to_owned(),
                    )
                }
                other => return Err(format!("unknown argument {other}; use --help")),
            }
        }

        Ok(Self {
            mode: mode.ok_or("--mode is required")?,
            lix_bin: lix_bin.ok_or("--lix-bin is required")?,
            repo: repo.ok_or("--repo is required")?,
            branch,
            from: from.ok_or("--from is required")?,
            to: to.ok_or("--to is required")?,
            output: output.ok_or("--output is required")?,
            report: report.ok_or("--report is required")?,
            storage,
            parent_tree,
            checkpoint_every,
            lix_source_repo: lix_source_repo.ok_or("--lix-source-repo is required")?,
            lix_source_commit: lix_source_commit.ok_or("--lix-source-commit is required")?,
            lix_source_tree: lix_source_tree.ok_or("--lix-source-tree is required")?,
            expected_lix_bin_sha256: expected_lix_bin_sha256
                .ok_or("--expected-lix-bin-sha256 is required")?,
            harness_source_repo: harness_source_repo.ok_or("--harness-source-repo is required")?,
            harness_source_commit: harness_source_commit
                .ok_or("--harness-source-commit is required")?,
            harness_source_tree: harness_source_tree.ok_or("--harness-source-tree is required")?,
            expected_harness_bin_sha256: expected_harness_bin_sha256
                .ok_or("--expected-harness-bin-sha256 is required")?,
            force,
        })
    }
}

fn print_help() {
    println!(
        "Usage: git_replay_real_repo --mode <verify-only|timed> --lix-bin <path> --repo <path> \\\n  --from <commit> --to <commit> --output <path.lix> --report <path.json> [options]\n\n\
Options:\n  --branch <ref>                 first-parent ref (default: main)\n  --storage <rocksdb|slatedb>    replay adapter (default: rocksdb)\n  --parent-tree <window|full>    parent bootstrap scope (default: window)\n  --checkpoint-every <N>         pass through to git-replay\n  --force                        replace output/profile paths\n\n\
Required provenance:\n  --lix-source-repo <path> --lix-source-commit <oid> --lix-source-tree <oid>\n  --expected-lix-bin-sha256 <sha256>\n  --harness-source-repo <path> --harness-source-commit <oid> --harness-source-tree <oid>\n  --expected-harness-bin-sha256 <sha256>\n\n\
The timed mode is a later one-sample gate; this source target does not run it.\n\
LFS pointers are authenticated by the delegated git-replay reader; missing\n\
objects fail closed and are never treated as pointer bytes."
    );
}

fn validate_config(config: &Config) -> Result<(), String> {
    if !config.lix_bin.is_file() {
        return Err(format!(
            "--lix-bin is not a file: {}",
            config.lix_bin.display()
        ));
    }
    if !config.repo.is_dir() {
        return Err(format!(
            "--repo is not a directory: {}",
            config.repo.display()
        ));
    }
    if !config.lix_source_repo.is_dir() {
        return Err(format!(
            "--lix-source-repo is not a directory: {}",
            config.lix_source_repo.display()
        ));
    }
    if !config.harness_source_repo.is_dir() {
        return Err(format!(
            "--harness-source-repo is not a directory: {}",
            config.harness_source_repo.display()
        ));
    }
    for (label, value, length) in [
        ("Lix source commit", config.lix_source_commit.as_str(), 40),
        ("Lix source tree", config.lix_source_tree.as_str(), 40),
        (
            "harness source commit",
            config.harness_source_commit.as_str(),
            40,
        ),
        (
            "harness source tree",
            config.harness_source_tree.as_str(),
            40,
        ),
        (
            "Lix binary SHA-256",
            config.expected_lix_bin_sha256.as_str(),
            64,
        ),
        (
            "harness binary SHA-256",
            config.expected_harness_bin_sha256.as_str(),
            64,
        ),
    ] {
        validate_lower_hex(label, value, length)?;
    }
    if config.from != FROZEN_FROM || config.to != FROZEN_TO {
        return Err(format!(
            "frozen workload requires inclusive --from {FROZEN_FROM} --to {FROZEN_TO}"
        ));
    }
    if config.output.extension().and_then(|value| value.to_str()) != Some("lix") {
        return Err(format!(
            "--output must be a .lix directory path: {}",
            config.output.display()
        ));
    }
    if config.storage != "rocksdb" && config.storage != "slatedb" {
        return Err(format!(
            "invalid --storage {}; expected rocksdb or slatedb",
            config.storage
        ));
    }
    if config.parent_tree != "window" && config.parent_tree != "full" {
        return Err(format!(
            "invalid --parent-tree {}; expected window or full",
            config.parent_tree
        ));
    }
    if !config.force && config.output.exists() {
        return Err(format!(
            "output already exists: {} (pass --force to replace it)",
            config.output.display()
        ));
    }
    Ok(())
}

fn validate_lower_hex(label: &str, value: &str, length: usize) -> Result<(), String> {
    if value.len() != length
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(format!(
            "{label} must be exactly {length} lowercase hexadecimal characters"
        ));
    }
    Ok(())
}

fn verify_provenance(config: &Config) -> Result<VerifiedProvenance, String> {
    verify_commit_tree(
        &config.lix_source_repo,
        &config.lix_source_commit,
        &config.lix_source_tree,
        "Lix source",
    )?;
    verify_commit_tree(
        &config.harness_source_repo,
        &config.harness_source_commit,
        &config.harness_source_tree,
        "harness source",
    )?;
    let lix_binary_sha256 = sha256_file(&config.lix_bin)
        .map_err(|error| format!("hash Lix binary {}: {error}", config.lix_bin.display()))?;
    let harness_binary = std::env::current_exe()
        .map_err(|error| format!("resolve current harness executable: {error}"))?;
    let harness_binary_sha256 = sha256_file(&harness_binary)
        .map_err(|error| format!("hash harness binary {}: {error}", harness_binary.display()))?;
    validate_provenance_hash(
        "Lix binary",
        &config.expected_lix_bin_sha256,
        &lix_binary_sha256,
    )?;
    validate_provenance_hash(
        "harness binary",
        &config.expected_harness_bin_sha256,
        &harness_binary_sha256,
    )?;
    Ok(VerifiedProvenance {
        lix_source_commit: config.lix_source_commit.clone(),
        lix_source_tree: config.lix_source_tree.clone(),
        lix_binary_sha256,
        harness_source_commit: config.harness_source_commit.clone(),
        harness_source_tree: config.harness_source_tree.clone(),
        harness_binary_sha256,
    })
}

fn verify_commit_tree(repo: &Path, commit: &str, tree: &str, label: &str) -> Result<(), String> {
    let actual_commit = git_text(
        repo,
        &["rev-parse", "--verify", &format!("{commit}^{{commit}}")],
    )?;
    if actual_commit != commit {
        return Err(format!(
            "{label} commit substitution: expected {commit}, got {actual_commit}"
        ));
    }
    let actual_tree = git_text(repo, &["rev-parse", &format!("{commit}^{{tree}}")])?;
    validate_provenance_hash(&format!("{label} tree"), tree, &actual_tree)
}

fn validate_provenance_hash(label: &str, expected: &str, actual: &str) -> Result<(), String> {
    if expected != actual {
        return Err(format!(
            "{label} substitution: expected {expected}, got {actual}"
        ));
    }
    Ok(())
}

fn validate_frozen_window(window: &CommitWindow) -> Result<(), String> {
    if window.from != FROZEN_FROM || window.to != FROZEN_TO || window.count != FROZEN_COMMIT_COUNT {
        return Err(format!(
            "frozen inclusive window mismatch: expected {FROZEN_FROM}..={FROZEN_TO} ({FROZEN_COMMIT_COUNT} commits), got {}..={} ({} commits)",
            window.from, window.to, window.count
        ));
    }
    Ok(())
}

fn canonical_tree_args(to: &str) -> [&str; 5] {
    ["ls-tree", "-r", "-z", "--full-tree", to]
}

fn validate_tree_digest(expected: &str, actual: &str) -> Result<(), String> {
    if expected != actual {
        return Err(format!(
            "canonical Git tree digest mismatch: expected {expected}, got {actual}"
        ));
    }
    Ok(())
}

fn resolve_window(repo: &Path, branch: &str, from: &str, to: &str) -> Result<CommitWindow, String> {
    let branch_oid = git_text(
        repo,
        &["rev-parse", "--verify", &format!("{branch}^{{commit}}")],
    )?;
    let from_oid = git_text(
        repo,
        &["rev-parse", "--verify", &format!("{from}^{{commit}}")],
    )?;
    let to_oid = git_text(
        repo,
        &["rev-parse", "--verify", &format!("{to}^{{commit}}")],
    )?;
    let listing = git_text(
        repo,
        &[
            "rev-list",
            "--reverse",
            "--first-parent",
            "--parents",
            &branch_oid,
        ],
    )?;
    let commits = listing
        .lines()
        .filter_map(|line| line.split_ascii_whitespace().next().map(str::to_owned))
        .collect::<Vec<_>>();
    let selected = inclusive_first_parent_window(&commits, &from_oid, &to_oid, branch)?;
    Ok(CommitWindow {
        branch: branch.to_owned(),
        from: from_oid,
        to: to_oid,
        count: selected.len(),
        commits: selected,
    })
}

fn inclusive_first_parent_window(
    commits: &[String],
    from: &str,
    to: &str,
    branch: &str,
) -> Result<Vec<String>, String> {
    let from_index = commits
        .iter()
        .position(|commit| commit == from)
        .ok_or_else(|| format!("--from {from} is not on first-parent history of {branch}"))?;
    let to_index = commits
        .iter()
        .position(|commit| commit == to)
        .ok_or_else(|| format!("--to {to} is not on first-parent history of {branch}"))?;
    if from_index > to_index {
        return Err(format!(
            "--from {from} occurs after --to {to} on first-parent history"
        ));
    }
    Ok(commits[from_index..=to_index].to_vec())
}

#[derive(Debug, serde::Serialize)]
struct LfsStats {
    selected_commit_count: usize,
    pointer_files: usize,
    unique_oids: usize,
    declared_pointer_bytes: u64,
    unique_declared_bytes: u64,
    local_object_files: usize,
    validated_objects: usize,
    validated_unique_bytes: u64,
    objects_path: PathBuf,
}

fn inspect_lfs(repo: &Path, window: &CommitWindow) -> Result<LfsStats, String> {
    let mut requirements = BTreeMap::<String, u64>::new();
    let mut pointer_files = 0usize;
    let mut declared_pointer_bytes = 0u64;
    for commit in &window.commits {
        for path in lfs_pointer_paths(repo, commit)? {
            let commit_path = format!("{commit}:{path}");
            let pointer = git_output(repo, &["show", &commit_path])?;
            if !pointer.status.success() {
                return Err(format_child_failure(
                    &format!("read Git LFS pointer {commit_path}"),
                    &pointer,
                ));
            }
            let (oid, size) = parse_lfs_pointer(&pointer.stdout, &commit_path)?;
            pointer_files = pointer_files
                .checked_add(1)
                .ok_or_else(|| "Git LFS pointer count overflow".to_string())?;
            declared_pointer_bytes = declared_pointer_bytes
                .checked_add(size)
                .ok_or_else(|| "Git LFS declared byte count overflow".to_string())?;
            if let Some(previous_size) = requirements.insert(oid.clone(), size)
                && previous_size != size
            {
                return Err(format!(
                    "Git LFS OID {oid} has conflicting declared sizes {previous_size} and {size} in selected first-parent commits"
                ));
            }
        }
    }
    let common_dir = git_text(repo, &["rev-parse", "--git-common-dir"])?;
    let common_dir = PathBuf::from(common_dir);
    let common_dir = if common_dir.is_absolute() {
        common_dir
    } else {
        repo.join(common_dir)
    };
    let objects_path = common_dir.join("lfs").join("objects");
    let local_objects = collect_local_lfs_objects(&objects_path)
        .map_err(|error| format!("scan {}: {error}", objects_path.display()))?;
    let (validated_objects, validated_unique_bytes) =
        validate_lfs_objects(&objects_path, &requirements, &local_objects)?;
    let unique_declared_bytes = requirements.values().try_fold(0u64, |total, size| {
        total
            .checked_add(*size)
            .ok_or_else(|| "Git LFS unique declared byte count overflow".to_string())
    })?;
    Ok(LfsStats {
        selected_commit_count: window.commits.len(),
        pointer_files,
        unique_oids: requirements.len(),
        declared_pointer_bytes,
        unique_declared_bytes,
        local_object_files: local_objects.len(),
        validated_objects,
        validated_unique_bytes,
        objects_path,
    })
}

fn lfs_pointer_paths(repo: &Path, commit: &str) -> Result<Vec<String>, String> {
    let listing = git_output(
        repo,
        &[
            "grep",
            "-z",
            "-I",
            "-l",
            "-e",
            LFS_POINTER_VERSION,
            commit,
            "--",
        ],
    )?;
    if listing.status.code() == Some(1) {
        return Ok(Vec::new());
    }
    if !listing.status.success() {
        return Err(format_child_failure(
            &format!("inspect Git LFS pointers in {commit}"),
            &listing,
        ));
    }
    let prefix = format!("{commit}:");
    listing
        .stdout
        .split(|byte| *byte == 0)
        .filter(|path| !path.is_empty())
        .map(|path| {
            let path = String::from_utf8(path.to_vec())
                .map_err(|_| format!("Git LFS pointer path in {commit} is not UTF-8"))?;
            Ok(path.strip_prefix(&prefix).unwrap_or(&path).to_owned())
        })
        .collect()
}

fn parse_lfs_pointer(bytes: &[u8], location: &str) -> Result<(String, u64), String> {
    let contents = std::str::from_utf8(bytes)
        .map_err(|_| format!("Git LFS pointer {location} is not UTF-8"))?;
    let lines = contents.lines().collect::<Vec<_>>();
    if lines.len() != 3 || lines[0] != LFS_POINTER_VERSION {
        return Err(format!(
            "Git LFS pointer {location} is malformed: expected exactly version/oid/size"
        ));
    }
    let oid = lines[1]
        .strip_prefix("oid sha256:")
        .filter(|oid| {
            oid.len() == 64
                && oid
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
        .ok_or_else(|| format!("Git LFS pointer {location} has a noncanonical SHA-256 OID"))?;
    let size = lines[2]
        .strip_prefix("size ")
        .filter(|size| !size.is_empty() && size.bytes().all(|byte| byte.is_ascii_digit()))
        .ok_or_else(|| format!("Git LFS pointer {location} has an invalid declared size"))?
        .parse::<u64>()
        .map_err(|error| {
            format!("Git LFS pointer {location} has an invalid declared size: {error}")
        })?;
    Ok((oid.to_owned(), size))
}

fn collect_local_lfs_objects(objects_path: &Path) -> io::Result<BTreeMap<String, PathBuf>> {
    let mut objects = BTreeMap::new();
    if objects_path.is_dir() {
        collect_local_lfs_objects_recursive(objects_path, objects_path, &mut objects)?;
    }
    Ok(objects)
}

fn collect_local_lfs_objects_recursive(
    root: &Path,
    path: &Path,
    objects: &mut BTreeMap<String, PathBuf>,
) -> io::Result<()> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let entry_path = entry.path();
        if entry_path.is_dir() {
            collect_local_lfs_objects_recursive(root, &entry_path, objects)?;
            continue;
        }
        let relative = entry_path.strip_prefix(root).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("LFS object path {}: {error}", entry_path.display()),
            )
        })?;
        let components = relative
            .components()
            .map(|component| component.as_os_str().to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        if components.len() != 3
            || components[0].len() != 2
            || components[1].len() != 2
            || components[2].len() != 64
            || !components[0]
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || !components[1]
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || !components[2]
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || components[0] != components[2][..2]
            || components[1] != components[2][2..4]
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unexpected local LFS object path {}", entry_path.display()),
            ));
        }
        let oid = components[2].clone();
        if objects.insert(oid.clone(), entry_path).is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("duplicate local LFS object path for OID {oid}"),
            ));
        }
    }
    Ok(())
}

fn validate_lfs_objects(
    objects_path: &Path,
    requirements: &BTreeMap<String, u64>,
    local_objects: &BTreeMap<String, PathBuf>,
) -> Result<(usize, u64), String> {
    for (oid, expected_size) in requirements {
        let path = local_objects.get(oid).ok_or_else(|| {
            format!(
                "required Git LFS object {oid} is missing from {} ({} exact OID/size requirements; refusing partial or unrelated cache)",
                objects_path.display(),
                requirements.len()
            )
        })?;
        let actual_size = fs::metadata(path)
            .map_err(|error| {
                format!(
                    "stat required Git LFS object {oid} at {}: {error}",
                    path.display()
                )
            })?
            .len();
        if actual_size != *expected_size {
            return Err(format!(
                "required Git LFS object {oid} at {} has size {actual_size}, expected {expected_size}",
                path.display()
            ));
        }
        let actual_oid = sha256_file(path).map_err(|error| {
            format!(
                "hash required Git LFS object {oid} at {}: {error}",
                path.display()
            )
        })?;
        if actual_oid != *oid {
            return Err(format!(
                "required Git LFS object {oid} at {} hashes as {actual_oid}",
                path.display()
            ));
        }
    }
    if let Some((oid, path)) = local_objects
        .iter()
        .find(|(oid, _)| !requirements.contains_key(*oid))
    {
        return Err(format!(
            "unrelated local Git LFS object {oid} at {} is outside the selected first-parent requirements",
            path.display()
        ));
    }
    let validated_unique_bytes = requirements.values().try_fold(0u64, |total, size| {
        total
            .checked_add(*size)
            .ok_or_else(|| "Git LFS validated byte count overflow".to_string())
    })?;
    Ok((requirements.len(), validated_unique_bytes))
}

fn sha256_file(path: &Path) -> io::Result<String> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_only_canonical_lfs_pointer() {
        let oid = "a".repeat(64);
        let pointer = format!("{LFS_POINTER_VERSION}\noid sha256:{oid}\nsize 7\n");
        assert_eq!(parse_lfs_pointer(pointer.as_bytes(), "valid"), Ok((oid, 7)));
        assert!(
            parse_lfs_pointer(
                format!(
                    "{LFS_POINTER_VERSION}\noid sha256:{}\nsize 7\n",
                    "A".repeat(64)
                )
                .as_bytes(),
                "uppercase"
            )
            .is_err()
        );
        assert!(parse_lfs_pointer(b"not an LFS pointer", "malformed").is_err());
    }

    #[test]
    fn validates_exact_required_lfs_objects_and_rejects_partial_or_unrelated_cache() {
        let temp = tempfile::tempdir().expect("temporary LFS object directory");
        let payload = b"payload";
        let oid = sha256_hex(payload);
        let object = temp.path().join(&oid[..2]).join(&oid[2..4]).join(&oid);
        fs::create_dir_all(object.parent().expect("object parent")).expect("object directory");
        fs::write(&object, payload).expect("object bytes");
        let required = BTreeMap::from([(oid.clone(), payload.len() as u64)]);
        let local = collect_local_lfs_objects(temp.path()).expect("local object scan");
        assert_eq!(
            validate_lfs_objects(temp.path(), &required, &local),
            Ok((1, payload.len() as u64))
        );

        let unrelated_oid = "b".repeat(64);
        let unrelated = temp
            .path()
            .join(&unrelated_oid[..2])
            .join(&unrelated_oid[2..4])
            .join(&unrelated_oid);
        fs::create_dir_all(unrelated.parent().expect("unrelated parent"))
            .expect("unrelated directory");
        fs::write(&unrelated, b"unrelated").expect("unrelated bytes");
        let local_with_unrelated =
            collect_local_lfs_objects(temp.path()).expect("local object scan with unrelated");
        assert!(
            validate_lfs_objects(temp.path(), &required, &local_with_unrelated)
                .expect_err("unrelated object must fail")
                .contains("unrelated local Git LFS object")
        );

        let missing = BTreeMap::from([("c".repeat(64), payload.len() as u64)]);
        assert!(
            validate_lfs_objects(temp.path(), &missing, &local_with_unrelated)
                .expect_err("partial cache must fail")
                .contains("required Git LFS object")
        );

        fs::remove_file(unrelated).expect("remove unrelated object");
        fs::write(&object, b"PAYLOAD").expect("wrong object bytes with preserved size");
        let local_with_wrong_hash =
            collect_local_lfs_objects(temp.path()).expect("wrong-hash object scan");
        assert!(
            validate_lfs_objects(temp.path(), &required, &local_with_wrong_hash)
                .expect_err("wrong hash must fail")
                .contains("hashes as")
        );
    }

    #[test]
    fn inclusive_window_rejects_the_prior_off_by_one_start() {
        let preceding = "05f14b".to_string();
        let mut commits = vec![preceding.clone(), FROZEN_FROM.to_string()];
        commits.extend((0..30).map(|index| format!("middle-{index:02}")));
        commits.push(FROZEN_TO.to_string());

        let selected =
            inclusive_first_parent_window(&commits, FROZEN_FROM, FROZEN_TO, "fixture-main")
                .expect("frozen inclusive window");
        assert_eq!(selected.len(), FROZEN_COMMIT_COUNT);
        assert_eq!(selected.first().map(String::as_str), Some(FROZEN_FROM));
        assert_eq!(selected.last().map(String::as_str), Some(FROZEN_TO));

        let off_by_one =
            inclusive_first_parent_window(&commits, &preceding, FROZEN_TO, "fixture-main")
                .expect("prior mixed inclusive window");
        assert_eq!(off_by_one.len(), FROZEN_COMMIT_COUNT + 1);
    }

    #[test]
    fn canonical_tree_encoding_mismatch_fails_closed() {
        assert_eq!(
            canonical_tree_args(FROZEN_TO),
            ["ls-tree", "-r", "-z", "--full-tree", FROZEN_TO]
        );
        assert!(
            validate_tree_digest(FROZEN_TREE_SHA256, &"0".repeat(64))
                .expect_err("tree encoding substitution must fail")
                .contains("canonical Git tree digest mismatch")
        );
    }

    #[test]
    fn omitted_plugin_rows_change_digest_and_missing_plugin_surface_fails() {
        let lix_file = CanonicalSemanticTable {
            schema: "public".to_string(),
            table: "lix_file".to_string(),
            columns: vec!["path".to_string()],
            rows: vec![json!(["README.md"])],
        };
        let markdown = CanonicalSemanticTable {
            schema: "public".to_string(),
            table: "markdown_node".to_string(),
            columns: vec!["file_id".to_string(), "type".to_string()],
            rows: vec![json!(["readme", "heading"])],
        };
        let full = digest_public_tables(&[lix_file.clone(), markdown.clone()])
            .expect("full semantic digest");
        let omitted = digest_public_tables(&[lix_file]).expect("omitted semantic digest");
        assert_ne!(full.digest, omitted.digest);
        assert_eq!(full.table_count, 2);
        assert_eq!(full.row_count, 2);

        let full_names = BTreeSet::from([
            ("public".to_string(), "lix_file".to_string()),
            ("public".to_string(), "markdown_node".to_string()),
        ]);
        require_plugin_surface(&full_names).expect("known plugin table present");
        let omitted_names = BTreeSet::from([("public".to_string(), "lix_file".to_string())]);
        assert!(
            require_plugin_surface(&omitted_names)
                .expect_err("omitted plugin surface must fail")
                .contains("omitted every known plugin table")
        );
    }

    #[test]
    fn provenance_substitution_fails_closed() {
        let expected = "1".repeat(64);
        let substituted = "2".repeat(64);
        assert!(
            validate_provenance_hash("harness binary", &expected, &substituted)
                .expect_err("substituted binary provenance must fail")
                .contains("harness binary substitution")
        );
    }
}

fn run_replay(config: &Config, window: &CommitWindow, profile: &Path) -> Result<Output, String> {
    let mut replay_args = vec![
        OsString::from("--no-hints"),
        OsString::from("exp"),
        OsString::from("git-replay"),
        OsString::from("--repo-path"),
        config.repo.clone().into_os_string(),
        OsString::from("--output-path"),
        config.output.clone().into_os_string(),
        OsString::from("--storage"),
        OsString::from(&config.storage),
        OsString::from("--plugins"),
        OsString::from("all"),
        OsString::from("--branch"),
        OsString::from(&config.branch),
        OsString::from("--from-commit"),
        OsString::from(&window.from),
        OsString::from("--num-commits"),
        OsString::from(window.count.to_string()),
        OsString::from("--parent-tree"),
        OsString::from(&config.parent_tree),
        OsString::from("--profile-json"),
        profile.to_path_buf().into_os_string(),
    ];
    if let Some(interval) = config.checkpoint_every {
        replay_args.push(OsString::from("--checkpoint-every"));
        replay_args.push(OsString::from(interval.to_string()));
    }
    if config.force {
        replay_args.push(OsString::from("--force"));
    }
    if config.mode == Mode::Timed {
        let mut command = Command::new("/usr/bin/time");
        command
            .arg("-v")
            .arg("--")
            .arg(&config.lix_bin)
            .args(replay_args);
        command.stdout(Stdio::piped()).stderr(Stdio::piped());
        return command
            .output()
            .map_err(|error| format!("timed mode requires /usr/bin/time -v: {error}"));
    }
    Command::new(&config.lix_bin)
        .args(replay_args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|error| format!("spawn lix git-replay: {error}"))
}

#[derive(Debug)]
struct SemanticDigest {
    digest: String,
    bytes: usize,
    table_count: usize,
    row_count: usize,
    tables: Vec<SemanticTableSummary>,
}

#[derive(Clone, Debug, serde::Serialize)]
struct CanonicalSemanticTable {
    schema: String,
    table: String,
    columns: Vec<String>,
    rows: Vec<Value>,
}

#[derive(Debug, serde::Serialize)]
struct SemanticTableSummary {
    schema: String,
    table: String,
    columns: Vec<String>,
    row_count: usize,
    canonical_sha256: String,
}

fn read_public_semantic_digest(config: &Config) -> Result<SemanticDigest, String> {
    let inventory = run_sql_json(config, PUBLIC_TABLE_QUERY, "public table inventory")?;
    let (inventory_columns, inventory_rows) =
        parse_query_envelope(inventory, "public table inventory")?;
    column_index(&inventory_columns, "table_schema", "public table inventory")?;
    column_index(&inventory_columns, "table_name", "public table inventory")?;
    let mut names = BTreeSet::new();
    for row in inventory_rows {
        let schema = row_string(&row, "table_schema", "public table inventory")?;
        let table = row_string(&row, "table_name", "public table inventory")?;
        names.insert((schema, table));
    }
    if names.is_empty() {
        return Err("public table inventory is empty after plugins-all replay".to_string());
    }
    require_plugin_surface(&names)?;

    let mut tables = Vec::with_capacity(names.len());
    for (schema, table) in names {
        let sql = format!("SELECT * FROM {}", quote_identifier(&table));
        let value = run_sql_json(config, &sql, &format!("public table {schema}.{table}"))?;
        let (columns, rows) =
            parse_query_envelope(value, &format!("public table {schema}.{table}"))?;
        let mut canonical_rows = rows
            .into_iter()
            .map(|row| canonical_row(&row, &columns, &schema, &table))
            .collect::<Result<Vec<_>, _>>()?;
        canonical_rows.sort_by(|left, right| {
            serde_json::to_vec(left)
                .expect("canonical row serialization")
                .cmp(&serde_json::to_vec(right).expect("canonical row serialization"))
        });
        tables.push(CanonicalSemanticTable {
            schema,
            table,
            columns,
            rows: canonical_rows,
        });
    }
    digest_public_tables(&tables)
}

fn run_sql_json(config: &Config, sql: &str, label: &str) -> Result<Value, String> {
    let output = Command::new(&config.lix_bin)
        .args([
            OsString::from("--no-hints"),
            OsString::from("--path"),
            config.output.clone().into_os_string(),
            OsString::from("sql"),
            OsString::from("execute"),
            OsString::from("--format"),
            OsString::from("json"),
            OsString::from(sql),
        ])
        .output()
        .map_err(|error| format!("spawn {label} SQL query: {error}"))?;
    if !output.status.success() {
        return Err(format_child_failure(
            &format!("{label} SQL query failed"),
            &output,
        ));
    }
    serde_json::from_slice(&output.stdout)
        .map_err(|error| format!("{label} SQL output was not JSON: {error}"))
}

fn parse_query_envelope(
    value: Value,
    label: &str,
) -> Result<(Vec<String>, Vec<Map<String, Value>>), String> {
    let object = value
        .as_object()
        .ok_or_else(|| format!("{label} result must be a JSON object"))?;
    let columns = object
        .get("columns")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("{label} result has no columns array"))?
        .iter()
        .map(|column| {
            column
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| format!("{label} contains a non-string column name"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if columns.iter().collect::<BTreeSet<_>>().len() != columns.len() {
        return Err(format!("{label} contains duplicate column names"));
    }
    let rows = object
        .get("rows")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("{label} result has no rows array"))?
        .iter()
        .map(|row| {
            row.as_object()
                .cloned()
                .ok_or_else(|| format!("{label} contains a non-object row"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((columns, rows))
}

fn column_index(columns: &[String], name: &str, label: &str) -> Result<usize, String> {
    columns
        .iter()
        .position(|column| column == name)
        .ok_or_else(|| format!("{label} is missing required column {name}"))
}

fn row_string(row: &Map<String, Value>, name: &str, label: &str) -> Result<String, String> {
    row.get(name)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| format!("{label} row has non-string or missing {name}"))
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn canonical_row(
    row: &Map<String, Value>,
    columns: &[String],
    schema: &str,
    table: &str,
) -> Result<Value, String> {
    if row.len() != columns.len() || row.keys().any(|key| !columns.contains(key)) {
        return Err(format!(
            "public table {schema}.{table} row keys do not exactly match advertised columns"
        ));
    }
    columns
        .iter()
        .map(|column| {
            row.get(column).cloned().map(canonical_json).ok_or_else(|| {
                format!("public table {schema}.{table} row is missing column {column}")
            })
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Value::Array)
}

fn require_plugin_surface(names: &BTreeSet<(String, String)>) -> Result<(), String> {
    if names
        .iter()
        .any(|(schema, table)| schema == "public" && KNOWN_PLUGIN_TABLES.contains(&table.as_str()))
    {
        return Ok(());
    }
    Err(format!(
        "plugins-all public surface omitted every known plugin table ({})",
        KNOWN_PLUGIN_TABLES.join(", ")
    ))
}

fn digest_public_tables(tables: &[CanonicalSemanticTable]) -> Result<SemanticDigest, String> {
    let bytes = serde_json::to_vec(tables)
        .map_err(|error| format!("serialize canonical public semantic surface: {error}"))?;
    let row_count = tables.iter().map(|table| table.rows.len()).sum();
    let summaries = tables
        .iter()
        .map(|table| {
            let table_bytes = serde_json::to_vec(table)
                .expect("canonical semantic table serialization cannot fail");
            SemanticTableSummary {
                schema: table.schema.clone(),
                table: table.table.clone(),
                columns: table.columns.clone(),
                row_count: table.rows.len(),
                canonical_sha256: sha256_hex(&table_bytes),
            }
        })
        .collect();
    Ok(SemanticDigest {
        digest: sha256_hex(&bytes),
        bytes: bytes.len(),
        table_count: tables.len(),
        row_count,
        tables: summaries,
    })
}

fn canonical_json(value: Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.into_iter().map(canonical_json).collect()),
        Value::Object(values) => {
            let mut sorted = BTreeMap::new();
            for (key, value) in values {
                sorted.insert(key, canonical_json(value));
            }
            Value::Object(sorted.into_iter().collect::<Map<String, Value>>())
        }
        other => other,
    }
}

fn backend_counters(profile: &Value) -> Value {
    let commits = profile
        .get("commits")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let sum = |key: &str| {
        commits
            .iter()
            .filter_map(|commit| commit.get(key).and_then(Value::as_u64))
            .sum::<u64>()
    };
    json!({
        "changed_paths": profile.get("changed_paths_total").cloned().unwrap_or(Value::Null),
        "physical_execution_groups": sum("physical_execution_groups"),
        "logical_statements": sum("logical_statement_count"),
        "sql_chars": sum("sql_chars"),
        "blob_bytes": sum("blob_bytes"),
        "git_lfs_objects_materialized": profile.get("git_lfs_objects_materialized").cloned().unwrap_or(Value::Null),
        "git_lfs_bytes_materialized": profile.get("git_lfs_bytes_materialized").cloned().unwrap_or(Value::Null),
        "storage_flush_ms": profile.get("storage_flush_ms").cloned().unwrap_or(Value::Null),
        "adapter_calls_bytes": "not exported by existing git_replay profile",
    })
}

fn parse_time_metrics(stderr: &[u8]) -> Value {
    let text = text(stderr);
    let mut metrics = Map::new();
    for line in text.lines() {
        let Some((key, raw)) = line.split_once(':') else {
            continue;
        };
        let key = key.trim();
        let raw = raw.trim();
        let field = match key {
            "User time (seconds)" => Some("user_seconds"),
            "System time (seconds)" => Some("system_seconds"),
            "Maximum resident set size (kbytes)" => Some("max_rss_kib"),
            "File system inputs" => Some("filesystem_inputs"),
            "File system outputs" => Some("filesystem_outputs"),
            "Voluntary context switches" => Some("voluntary_context_switches"),
            "Involuntary context switches" => Some("involuntary_context_switches"),
            "Elapsed (wall clock) time (h:mm:ss or m:ss)" => Some("elapsed_wall"),
            _ => None,
        };
        if let Some(field) = field {
            metrics.insert(field.to_owned(), Value::String(raw.to_owned()));
        }
    }
    Value::Object(metrics)
}

fn profile_path(report: &Path) -> PathBuf {
    let stem = report
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("git-replay");
    report.with_file_name(format!("{stem}.replay-profile.json"))
}

fn write_json(path: &Path, value: &Value) -> Result<(), String> {
    let bytes = serde_json::to_vec_pretty(value)
        .map_err(|error| format!("serialize report {}: {error}", path.display()))?;
    fs::write(path, bytes).map_err(|error| format!("write report {}: {error}", path.display()))
}

fn git_text(repo: &Path, args: &[&str]) -> Result<String, String> {
    let output = git_output(repo, args)?;
    if !output.status.success() {
        return Err(format_child_failure("git command failed", &output));
    }
    Ok(text(&output.stdout).trim().to_owned())
}

fn git_capture(repo: &Path, args: &[&str]) -> Result<Output, String> {
    git_output(repo, args)
}

fn git_output(repo: &Path, args: &[&str]) -> Result<Output, String> {
    Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .output()
        .map_err(|error| format!("spawn git: {error}"))
}

fn format_child_failure(label: &str, output: &Output) -> String {
    let stdout = text(&output.stdout);
    let stderr = text(&output.stderr);
    let mut detail = String::new();
    if !stdout.trim().is_empty() {
        detail.push_str(&format!(" stdout={:?}", truncate(&stdout)));
    }
    if !stderr.trim().is_empty() {
        detail.push_str(&format!(" stderr={:?}", truncate(&stderr)));
    }
    format!("{label}: status={}{}", output.status, detail)
}

fn truncate(value: &str) -> String {
    const MAX: usize = 4096;
    if value.len() <= MAX {
        return value.to_owned();
    }
    format!("{}…", &value[..MAX])
}

fn ensure_parent(path: &Path) -> Result<(), String> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(|error| format!("create {}: {error}", parent.display()))
}

fn directory_bytes(path: &Path) -> io::Result<u64> {
    if path.is_file() {
        return Ok(fs::metadata(path)?.len());
    }
    let mut total = 0;
    for entry in fs::read_dir(path)? {
        total += directory_bytes(&entry?.path())?;
    }
    Ok(total)
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn duration_ms(duration: std::time::Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}
