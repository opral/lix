use crate::cli::exp::ExpGitReplayArgs;
use crate::db;
use crate::error::CliError;
use lix_rocksdb_storage::RocksDB;
use lix_sdk::{ExecuteBatchStatement, Lix, Value, open_lix_with_storage};
use serde::Serialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs;
use std::io::{BufRead, BufReader, BufWriter, Cursor, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use zip::write::SimpleFileOptions;

const PROGRESS_EVERY: usize = 10;
const DEFAULT_INSERT_BATCH_ROWS: usize = 100;
const GIT_TEXT_PLUGIN_KEY: &str = "plugin_git_text_v2";
const GIT_REPLAY_MARKER_KEY: &str = "git_replay_marker_v1";

type RocksLix = Lix<RocksDB>;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Change {
    status: char,
    old_mode: String,
    new_mode: String,
    new_oid: String,
    old_path: Option<GitPath>,
    new_path: Option<GitPath>,
}

impl Change {
    fn new_is_blob(&self) -> bool {
        mode_is_blob(&self.new_mode)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReplayCommit {
    sha: String,
    first_parent: Option<String>,
}

/// A Git pathname is bytes, not Unicode. Keep that identity byte-exact and
/// encode it only at the Lix filesystem boundary.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct GitPath(Vec<u8>);

impl GitPath {
    fn from_diff_token(token: &[u8]) -> Result<Self, CliError> {
        if token.is_empty() {
            return Err(CliError::msg("malformed git diff-tree output: empty path"));
        }
        if token.starts_with(b"/") {
            return Err(CliError::msg(
                "malformed git diff-tree output: Git path must be relative",
            ));
        }
        Ok(Self(token.to_vec()))
    }

    fn relative_lix_path(&self) -> String {
        encode_git_path_bytes(&self.0)
    }

    fn lix_path(&self) -> String {
        format!("/{}", self.relative_lix_path())
    }
}

#[derive(Default)]
struct ReplayState {
    path_to_file_id: HashMap<GitPath, String>,
    known_file_ids: HashSet<String>,
}

#[derive(Debug, Clone)]
struct WriteRow {
    id: String,
    path: String,
    data: Option<Vec<u8>>,
    git_mode: String,
    git_oid: String,
}

#[derive(Debug)]
struct PreparedBatch {
    deletes: Vec<String>,
    inserts: Vec<WriteRow>,
    updates: Vec<WriteRow>,
}

#[derive(Debug)]
struct SqlStatement {
    sql: String,
    params: Vec<Value>,
}

#[derive(Debug, Clone)]
struct ExpectedFile {
    path: String,
    sha256: Option<String>,
    git_mode: String,
    git_oid: String,
}

#[derive(Debug, Default, Serialize)]
struct ReplayProfilePhaseTotals {
    read_diff_ms: f64,
    read_blobs_ms: f64,
    prepare_ms: f64,
    build_sql_ms: f64,
    execute_ms: f64,
    verify_ms: f64,
    total_ms: f64,
}

#[derive(Debug, Serialize)]
struct ReplayCommitProfile {
    commit_sha: String,
    changed_paths: usize,
    inserts: usize,
    updates: usize,
    deletes: usize,
    statement_count: usize,
    sql_chars: usize,
    blob_bytes: usize,
    marker_only: bool,
    read_diff_ms: f64,
    read_blobs_ms: f64,
    prepare_ms: f64,
    build_sql_ms: f64,
    execute_ms: f64,
    verify_ms: Option<f64>,
    total_ms: f64,
}

#[derive(Debug, Serialize)]
struct ReplayProfileReport {
    repo_path: String,
    output_rocksdb_path: String,
    branch: String,
    from_commit: Option<String>,
    num_commits_requested: Option<u32>,
    verify_state: bool,
    plugin_install_ms: f64,
    baseline_seed_parent: Option<String>,
    baseline_seed_ms: f64,
    baseline_seed_files: usize,
    final_tree_verify_ms: Option<f64>,
    replay_elapsed_ms: f64,
    rocksdb_flush_ms: f64,
    commits_replayed: usize,
    commits_applied: usize,
    commits_marker_only: usize,
    changed_paths_total: usize,
    phase_totals: ReplayProfilePhaseTotals,
    commits: Vec<ReplayCommitProfile>,
}

pub fn run(args: ExpGitReplayArgs) -> Result<(), CliError> {
    let repo_path = absolutize_from_cwd(&args.repo_path)?;
    validate_repo_dir(&repo_path)?;
    validate_git_repo(&repo_path)?;
    let output_rocksdb_path = absolutize_from_cwd(&args.output_rocksdb_path)?;
    validate_safe_rocksdb_output_path(&repo_path, &output_rocksdb_path)?;
    let profile_json_path = args
        .profile_json
        .as_ref()
        .map(|path| absolutize_from_cwd(path))
        .transpose()?;

    let replay_ref = resolve_replay_ref_oid(&repo_path, &args.branch)?;
    let from_commit = args
        .from_commit
        .as_deref()
        .map(|raw| resolve_commit_oid(&repo_path, raw))
        .transpose()?;
    let commits = list_linear_commits(
        &repo_path,
        &replay_ref,
        from_commit.as_deref(),
        args.num_commits,
    )?;
    if commits.is_empty() {
        return Err(CliError::msg(format!(
            "no commits found in {} for ref '{}'",
            repo_path.display(),
            args.branch
        )));
    }

    // Do not let `--force` delete a valid previous replay until every
    // non-destructive input check has succeeded.
    prepare_rocksdb_output_path(&output_rocksdb_path, args.force)?;
    if let Some(path) = &profile_json_path {
        prepare_regular_output_path(path, args.force)?;
    }

    let storage = RocksDB::open(&output_rocksdb_path).map_err(|error| {
        CliError::msg(format!(
            "failed to open RocksDB at {}: {error}",
            output_rocksdb_path.display()
        ))
    })?;
    let lix = db::block_on(open_lix_with_storage(storage.clone()))
        .map_err(|error| CliError::msg(format!("failed to open RocksDB Lix: {error}")))?;
    db::block_on(lix.execute(
        "INSERT INTO lix_key_value (key, value, lixcol_global, lixcol_untracked) \
         VALUES ('lix_deterministic_mode', lix_json('{\"enabled\":true}'), true, true)",
        &[],
    ))
    .map_err(|error| CliError::msg(format!("failed to enable deterministic mode: {error}")))?;

    let plugin_install_started = Instant::now();
    install_embedded_git_text_plugin(&lix)?;
    let plugin_install_ms = duration_to_ms(plugin_install_started.elapsed());

    let mut state = ReplayState::default();
    let mut expected_state_by_id = HashMap::<String, ExpectedFile>::new();
    let baseline_seed_parent = commits
        .first()
        .and_then(|commit| commit.first_parent.clone());
    let mut baseline_seed_ms = 0.0;
    let mut baseline_seed_files = 0usize;
    let seeded_blob_reader = if let Some(parent) = baseline_seed_parent.as_deref() {
        let seed_started = Instant::now();
        let mut blob_reader = GitBlobReader::spawn(&repo_path)?;
        let seeded = seed_parent_tree(
            &repo_path,
            parent,
            &mut blob_reader,
            &mut state,
            &mut expected_state_by_id,
            args.verify_state,
            &lix,
        )?;
        baseline_seed_files = seeded;
        baseline_seed_ms = duration_to_ms(seed_started.elapsed());
        Some(blob_reader)
    } else {
        None
    };

    // Plugin installation and optional parent-tree bootstrap are reported
    // separately. Opening the streaming Git readers is part of timed replay.
    let replay_started = Instant::now();
    let mut diff_reader = GitDiffTreeReader::spawn(&repo_path, &commits)?;
    let mut blob_reader = match seeded_blob_reader {
        Some(reader) => reader,
        None => GitBlobReader::spawn(&repo_path)?,
    };
    let mut applied = 0usize;
    let mut marker_only = 0usize;
    let mut changed_paths = 0usize;
    let mut verified = 0usize;
    let mut phase_totals = ReplayProfilePhaseTotals::default();
    let mut commit_profiles = Vec::<ReplayCommitProfile>::with_capacity(commits.len());

    println!(
        "[git-replay] replaying {} commits from {} into RocksDB",
        commits.len(),
        repo_path.display()
    );

    for (index, commit) in commits.iter().enumerate() {
        let commit_sha = &commit.sha;
        let commit_started = Instant::now();

        let read_diff_started = Instant::now();
        let changes = diff_reader.read_commit(commit_sha)?;
        let read_diff_ms = duration_to_ms(read_diff_started.elapsed());
        phase_totals.read_diff_ms += read_diff_ms;
        changed_paths += changes.len();

        let read_blobs_started = Instant::now();
        let wanted_blob_ids = collect_wanted_blob_ids(&changes);
        let blob_by_oid = blob_reader.read_blobs(&wanted_blob_ids)?;
        let read_blobs_ms = duration_to_ms(read_blobs_started.elapsed());
        phase_totals.read_blobs_ms += read_blobs_ms;

        let prepare_started = Instant::now();
        let prepared = prepare_commit_changes(&mut state, &changes, &blob_by_oid)?;
        let prepare_ms = duration_to_ms(prepare_started.elapsed());
        phase_totals.prepare_ms += prepare_ms;

        let build_sql_started = Instant::now();
        let mut statements = build_replay_commit_statements(&prepared, DEFAULT_INSERT_BATCH_ROWS);
        statements.push(git_replay_marker_statement(commit));
        let build_sql_ms = duration_to_ms(build_sql_started.elapsed());
        phase_totals.build_sql_ms += build_sql_ms;

        let statement_count = statements.len();
        let sql_chars = total_statement_sql_chars(&statements);
        let blob_bytes = prepared_blob_bytes(&prepared);
        let inserts = prepared.inserts.len();
        let updates = prepared.updates.len();
        let deletes = prepared.deletes.len();
        let mut verify_ms = None;

        if prepared.deletes.is_empty() && prepared.inserts.is_empty() && prepared.updates.is_empty()
        {
            marker_only += 1;
        }
        let execute_started = Instant::now();
        execute_statements_as_transaction(&lix, &statements, commit_sha)?;
        let execute_ms = duration_to_ms(execute_started.elapsed());
        phase_totals.execute_ms += execute_ms;
        applied += 1;

        if args.verify_state {
            let verify_started = Instant::now();
            apply_prepared_to_expected_state(&mut expected_state_by_id, &prepared);
            verify_commit_state_hashes(&lix, &expected_state_by_id, commit_sha)?;
            let verify_elapsed_ms = duration_to_ms(verify_started.elapsed());
            phase_totals.verify_ms += verify_elapsed_ms;
            verify_ms = Some(verify_elapsed_ms);
            verified += 1;
        }

        let total_ms = duration_to_ms(commit_started.elapsed());
        phase_totals.total_ms += total_ms;
        commit_profiles.push(ReplayCommitProfile {
            commit_sha: commit_sha.clone(),
            changed_paths: changes.len(),
            inserts,
            updates,
            deletes,
            statement_count,
            sql_chars,
            blob_bytes,
            marker_only: prepared.deletes.is_empty()
                && prepared.inserts.is_empty()
                && prepared.updates.is_empty(),
            read_diff_ms,
            read_blobs_ms,
            prepare_ms,
            build_sql_ms,
            execute_ms,
            verify_ms,
            total_ms,
        });

        if index == 0 || (index + 1) % PROGRESS_EVERY == 0 || index + 1 == commits.len() {
            println!(
                "[git-replay] {}/{} commits (applied={}, markerOnly={}, changedPaths={})",
                index + 1,
                commits.len(),
                applied,
                marker_only,
                changed_paths
            );
        }
    }

    diff_reader.finish()?;
    let replay_before_final_verification = replay_started.elapsed();
    let final_tree_verify_ms = if args.verify_state {
        let verify_started = Instant::now();
        verify_final_git_tree(
            &repo_path,
            &commits
                .last()
                .expect("non-empty replay commits were validated above")
                .sha,
            &mut blob_reader,
            &lix,
        )?;
        Some(duration_to_ms(verify_started.elapsed()))
    } else {
        None
    };
    let replay_cleanup_started = Instant::now();
    blob_reader.finish()?;
    let replay_elapsed_ms =
        duration_to_ms(replay_before_final_verification + replay_cleanup_started.elapsed());
    db::block_on(lix.close())
        .map_err(|error| CliError::msg(format!("failed to close replay Lix: {error}")))?;
    let flush_started = Instant::now();
    storage.flush().map_err(|error| {
        CliError::msg(format!(
            "failed to flush replay RocksDB at {}: {error}",
            output_rocksdb_path.display()
        ))
    })?;
    let rocksdb_flush_ms = duration_to_ms(flush_started.elapsed());

    println!("[git-replay] done");
    println!("[git-replay] ref: {}", args.branch);
    println!(
        "[git-replay] output RocksDB: {}",
        output_rocksdb_path.display()
    );
    println!("[git-replay] commits replayed: {}", commits.len());
    println!("[git-replay] commits applied: {applied}");
    println!("[git-replay] commits with marker only: {marker_only}");
    println!("[git-replay] changed paths total: {changed_paths}");
    println!("[git-replay] Git-text setup excluded from replay timing: {plugin_install_ms:.3}ms");
    if let Some(parent) = &baseline_seed_parent {
        println!(
            "[git-replay] parent-tree bootstrap excluded from replay timing: {baseline_seed_ms:.3}ms ({baseline_seed_files} files from {parent})"
        );
    }
    println!("[git-replay] replay elapsed: {replay_elapsed_ms:.3}ms");
    if args.verify_state {
        println!(
            "[git-replay] verified commits: {verified}/{}",
            commits.len()
        );
        println!(
            "[git-replay] final Git tree manifest verified in {:.3}ms",
            final_tree_verify_ms.expect("verification timing should exist")
        );
    }
    if let Some(profile_path) = &profile_json_path {
        write_profile_report(
            profile_path,
            ReplayProfileReport {
                repo_path: repo_path.display().to_string(),
                output_rocksdb_path: output_rocksdb_path.display().to_string(),
                branch: args.branch.clone(),
                from_commit: args.from_commit.clone(),
                num_commits_requested: args.num_commits,
                verify_state: args.verify_state,
                plugin_install_ms,
                baseline_seed_parent,
                baseline_seed_ms,
                baseline_seed_files,
                final_tree_verify_ms,
                replay_elapsed_ms,
                rocksdb_flush_ms,
                commits_replayed: commits.len(),
                commits_applied: applied,
                commits_marker_only: marker_only,
                changed_paths_total: changed_paths,
                phase_totals,
                commits: commit_profiles,
            },
        )?;
        println!("[git-replay] profile json: {}", profile_path.display());
    }

    Ok(())
}

fn execute_statements_as_transaction(
    lix: &RocksLix,
    statements: &[SqlStatement],
    commit_sha: &str,
) -> Result<(), CliError> {
    let batch = statements
        .iter()
        .map(|statement| ExecuteBatchStatement {
            sql: statement.sql.clone(),
            params: statement.params.clone(),
        })
        .collect::<Vec<_>>();

    db::block_on(lix.execute_batch(&batch)).map_err(|error| {
        let sql_preview = batch
            .first()
            .map(|statement| statement.sql.chars().take(160).collect::<String>())
            .unwrap_or_default();
        CliError::msg(format!(
            "failed at commit {commit_sha} while executing atomic replay batch starting '{sql_preview}': {error}"
        ))
    })?;

    Ok(())
}

fn git_replay_marker_statement(commit: &ReplayCommit) -> SqlStatement {
    SqlStatement {
        sql: "INSERT INTO lix_key_value (key, value) VALUES (?, ?) \
              ON CONFLICT(key) DO UPDATE SET value = excluded.value"
            .to_string(),
        params: vec![
            Value::Text(GIT_REPLAY_MARKER_KEY.to_string()),
            Value::Json(json!({
                "sha": commit.sha,
                "first_parent": commit.first_parent,
            })),
        ],
    }
}

fn prepared_blob_bytes(prepared: &PreparedBatch) -> usize {
    prepared
        .inserts
        .iter()
        .chain(prepared.updates.iter())
        .filter_map(|row| row.data.as_ref())
        .map(Vec::len)
        .sum()
}

fn total_statement_sql_chars(statements: &[SqlStatement]) -> usize {
    statements.iter().map(|statement| statement.sql.len()).sum()
}

fn duration_to_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

fn write_profile_report(path: &Path, report: ReplayProfileReport) -> Result<(), CliError> {
    let mut bytes = serde_json::to_vec_pretty(&report).map_err(|error| {
        CliError::msg(format!(
            "failed to serialize replay profile report: {error}"
        ))
    })?;
    bytes.push(b'\n');
    fs::write(path, bytes).map_err(|source| CliError::io("failed to write profile json", source))
}

fn list_linear_commits(
    repo_path: &Path,
    replay_ref: &str,
    from_commit: Option<&str>,
    limit: Option<u32>,
) -> Result<Vec<ReplayCommit>, CliError> {
    let args = vec![
        "rev-list".to_string(),
        "--reverse".to_string(),
        "--first-parent".to_string(),
        "--parents".to_string(),
        replay_ref.to_string(),
    ];

    let output = run_git_text(repo_path, &args, None)?;
    let commits = output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| {
            let mut fields = line.split_ascii_whitespace();
            let sha = fields.next().ok_or_else(|| {
                CliError::msg("malformed git rev-list output: commit line has no object id")
            })?;
            if !is_full_git_oid(sha.as_bytes()) {
                return Err(CliError::msg(format!(
                    "malformed git rev-list output: invalid commit object id {sha}"
                )));
            }
            let first_parent = fields.next().map(ToOwned::to_owned);
            if let Some(parent) = &first_parent
                && !is_full_git_oid(parent.as_bytes())
            {
                return Err(CliError::msg(format!(
                    "malformed git rev-list output: invalid parent object id {parent}"
                )));
            }
            Ok(ReplayCommit {
                sha: sha.to_string(),
                first_parent,
            })
        })
        .collect::<Result<Vec<_>, CliError>>()?;
    select_replay_commits(commits, from_commit, limit)
}

fn select_replay_commits(
    mut commits: Vec<ReplayCommit>,
    from_commit: Option<&str>,
    limit: Option<u32>,
) -> Result<Vec<ReplayCommit>, CliError> {
    if let Some(from_commit) = from_commit {
        let from_index = commits
            .iter()
            .position(|commit| commit.sha == from_commit)
            .ok_or_else(|| {
                CliError::msg(format!(
                    "--from-commit {from_commit} is not reachable from selected ref"
                ))
            })?;
        commits = commits.split_off(from_index);
    }

    if let Some(limit) = limit {
        commits.truncate(limit as usize);
    }

    Ok(commits)
}

fn resolve_commit_oid(repo_path: &Path, raw: &str) -> Result<String, CliError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(CliError::InvalidArgs("from_commit must not be empty"));
    }

    let args = vec![
        "rev-parse".to_string(),
        "--verify".to_string(),
        "--end-of-options".to_string(),
        format!("{trimmed}^{{commit}}"),
    ];
    let output = run_git_text(repo_path, &args, None).map_err(|error| {
        CliError::msg(format!("failed to resolve --from-commit {raw}: {error}"))
    })?;
    let oid = output.trim();
    if oid.is_empty() {
        return Err(CliError::msg(format!(
            "failed to resolve --from-commit {raw}: empty rev-parse output"
        )));
    }
    Ok(oid.to_string())
}

fn resolve_replay_ref_oid(repo_path: &Path, raw: &str) -> Result<String, CliError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(CliError::InvalidArgs("branch must not be empty"));
    }
    if trimmed == "*" || trimmed.starts_with('-') || trimmed.contains("..") {
        return Err(CliError::InvalidArgs(
            "branch must name one commit or ref, not an option, range, or all-refs selector",
        ));
    }
    let args = vec![
        "rev-parse".to_string(),
        "--verify".to_string(),
        "--end-of-options".to_string(),
        format!("{trimmed}^{{commit}}"),
    ];
    let output = run_git_text(repo_path, &args, None)
        .map_err(|error| CliError::msg(format!("failed to resolve --branch {raw}: {error}")))?;
    let oid = output.trim();
    if !is_full_git_oid(oid.as_bytes()) {
        return Err(CliError::msg(format!(
            "failed to resolve --branch {raw}: expected one full commit object id"
        )));
    }
    Ok(oid.to_string())
}

fn seed_parent_tree(
    repo_path: &Path,
    parent_commit: &str,
    blob_reader: &mut GitBlobReader,
    state: &mut ReplayState,
    expected_state_by_id: &mut HashMap<String, ExpectedFile>,
    verify_state: bool,
    lix: &RocksLix,
) -> Result<usize, CliError> {
    let changes = read_tree_snapshot_changes(repo_path, parent_commit)?;
    let wanted_blob_ids = collect_wanted_blob_ids(&changes);
    let blob_by_oid = blob_reader.read_blobs(&wanted_blob_ids)?;
    let prepared = prepare_commit_changes(state, &changes, &blob_by_oid)?;
    let statements = build_replay_commit_statements(&prepared, DEFAULT_INSERT_BATCH_ROWS);
    if !statements.is_empty() {
        execute_statements_as_transaction(lix, &statements, parent_commit)?;
    }
    if verify_state {
        apply_prepared_to_expected_state(expected_state_by_id, &prepared);
        verify_commit_state_hashes(lix, expected_state_by_id, parent_commit)?;
    }
    Ok(prepared.inserts.len())
}

fn read_tree_snapshot_changes(repo_path: &Path, commit_sha: &str) -> Result<Vec<Change>, CliError> {
    let args = vec![
        "ls-tree".to_string(),
        "-r".to_string(),
        "-z".to_string(),
        "--full-tree".to_string(),
        commit_sha.to_string(),
    ];
    let raw = run_git_bytes(repo_path, &args, None)?;
    let mut changes = Vec::new();
    for token in raw
        .split(|byte| *byte == 0)
        .filter(|token| !token.is_empty())
    {
        let tab = token
            .iter()
            .position(|byte| *byte == b'\t')
            .ok_or_else(|| {
                CliError::msg("malformed git ls-tree output: missing tab before path")
            })?;
        let (header, path_with_tab) = token.split_at(tab);
        let path = &path_with_tab[1..];
        let header = std::str::from_utf8(header)
            .map_err(|_| CliError::msg("malformed git ls-tree output: header is not ASCII"))?;
        let fields = header.split_ascii_whitespace().collect::<Vec<_>>();
        if fields.len() != 3 || !is_full_git_oid(fields[2].as_bytes()) {
            return Err(CliError::msg(format!(
                "malformed git ls-tree record: {header}"
            )));
        }
        let is_blob = fields[1] == "blob" && mode_is_blob(fields[0]);
        let is_gitlink = fields[1] == "commit" && mode_is_gitlink(fields[0]);
        if !is_blob && !is_gitlink {
            return Err(CliError::msg(format!(
                "unsupported git ls-tree entry {header}; expected blob or gitlink"
            )));
        }
        changes.push(Change {
            status: 'A',
            old_mode: "000000".to_string(),
            new_mode: fields[0].to_string(),
            new_oid: fields[2].to_string(),
            old_path: None,
            new_path: Some(GitPath::from_diff_token(path)?),
        });
    }
    Ok(changes)
}

struct GitDiffTreeReader {
    repo_path: PathBuf,
    child: Option<Child>,
    stdout: Option<BufReader<ChildStdout>>,
    stdin_writer: Option<JoinHandle<std::io::Result<()>>>,
    stderr_reader: Option<JoinHandle<std::io::Result<Vec<u8>>>>,
    root_commit: Option<(String, Vec<Change>)>,
    remaining_stream_commits: usize,
}

impl GitDiffTreeReader {
    fn spawn(repo_path: &Path, commits: &[ReplayCommit]) -> Result<Self, CliError> {
        let mut root_commit = None;
        let mut stream_inputs = Vec::<(String, String)>::new();

        for (index, commit) in commits.iter().enumerate() {
            match &commit.first_parent {
                Some(parent) => stream_inputs.push((commit.sha.clone(), parent.clone())),
                None if index == 0 => {
                    let args = vec![
                        "diff-tree".to_string(),
                        "--root".to_string(),
                        "--always".to_string(),
                        "--raw".to_string(),
                        "-r".to_string(),
                        "-z".to_string(),
                        "--no-abbrev".to_string(),
                        "--no-ext-diff".to_string(),
                        "--find-renames".to_string(),
                        "--pretty=format:".to_string(),
                        commit.sha.clone(),
                    ];
                    let raw = run_git_bytes(repo_path, &args, None)?;
                    root_commit = Some((commit.sha.clone(), parse_raw_diff_tree(&raw)?));
                }
                None => {
                    return Err(CliError::msg(
                        "malformed first-parent history: only the first replay commit may be root",
                    ));
                }
            }
        }

        if stream_inputs.is_empty() {
            return Ok(Self {
                repo_path: repo_path.to_path_buf(),
                child: None,
                stdout: None,
                stdin_writer: None,
                stderr_reader: None,
                root_commit,
                remaining_stream_commits: 0,
            });
        }

        let remaining_stream_commits = commits.len() - usize::from(root_commit.is_some());
        let mut command = Command::new("git");
        command
            .arg("-C")
            .arg(repo_path)
            .args([
                "diff-tree",
                "--stdin",
                "--always",
                "--raw",
                "-r",
                "-z",
                "--no-abbrev",
                "--no-ext-diff",
                "--find-renames",
                // With --stdin, `<target> <parent>` already emits the
                // parent-to-target delta. `-R` would reverse it back into a
                // target-to-parent rollback.
                "--pretty=format:%H%x00",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command
            .spawn()
            .map_err(|source| CliError::io("failed to spawn persistent git diff-tree", source))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| CliError::msg("failed to open stdin for persistent git diff-tree"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| CliError::msg("failed to open stdout for persistent git diff-tree"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| CliError::msg("failed to open stderr for persistent git diff-tree"))?;
        let stdin_writer = thread::spawn(move || {
            let mut stdin = BufWriter::new(stdin);
            for (commit, parent) in stream_inputs {
                stdin.write_all(commit.as_bytes())?;
                stdin.write_all(b" ")?;
                stdin.write_all(parent.as_bytes())?;
                stdin.write_all(b"\n")?;
            }
            stdin.flush()
        });
        let stderr_reader = thread::spawn(move || {
            let mut stderr = stderr;
            let mut bytes = Vec::new();
            stderr.read_to_end(&mut bytes)?;
            Ok(bytes)
        });

        Ok(Self {
            repo_path: repo_path.to_path_buf(),
            child: Some(child),
            stdout: Some(BufReader::new(stdout)),
            stdin_writer: Some(stdin_writer),
            stderr_reader: Some(stderr_reader),
            root_commit,
            remaining_stream_commits,
        })
    }

    fn read_commit(&mut self, expected_sha: &str) -> Result<Vec<Change>, CliError> {
        if let Some((root_sha, changes)) = self.root_commit.take() {
            if root_sha != expected_sha {
                return Err(CliError::msg(format!(
                    "git diff-tree root order mismatch: expected {expected_sha}, got {root_sha}"
                )));
            }
            return Ok(changes);
        }

        let stdout = self.stdout.as_mut().ok_or_else(|| {
            CliError::msg(format!(
                "git diff-tree ended before replay commit {expected_sha} was available"
            ))
        })?;
        let marker = read_nul_token(stdout)?.ok_or_else(|| {
            CliError::msg(format!(
                "git diff-tree ended before replay commit marker {expected_sha}"
            ))
        })?;
        if marker.as_slice() != expected_sha.as_bytes() {
            return Err(CliError::msg(format!(
                "git diff-tree marker order mismatch: expected {expected_sha}, got {}",
                String::from_utf8_lossy(&marker)
            )));
        }

        let mut changes = Vec::new();
        let mut first_record = true;
        loop {
            let Some(token) = read_nul_token(stdout)? else {
                if self.remaining_stream_commits == 1 {
                    break;
                }
                return Err(CliError::msg(format!(
                    "git diff-tree ended inside replay commit {expected_sha}"
                )));
            };
            if token.is_empty() {
                break;
            }
            let header = if first_record {
                token.strip_prefix(b"\n").ok_or_else(|| {
                    CliError::msg(format!(
                        "malformed git diff-tree output for {expected_sha}: missing marker separator"
                    ))
                })?
            } else {
                token.as_slice()
            };
            let parsed = parse_raw_diff_header(header)?;
            let first_path = read_nul_token(stdout)?.ok_or_else(|| {
                CliError::msg("malformed git diff-tree output: missing path token")
            })?;
            let second_path = if parsed.requires_second_path() {
                Some(read_nul_token(stdout)?.ok_or_else(|| {
                    CliError::msg("malformed git diff-tree output: missing rename destination")
                })?)
            } else {
                None
            };
            changes.push(change_from_raw_parts(
                parsed,
                &first_path,
                second_path.as_deref(),
            )?);
            first_record = false;
        }
        self.remaining_stream_commits =
            self.remaining_stream_commits
                .checked_sub(1)
                .ok_or_else(|| {
                    CliError::msg("git diff-tree returned more commit groups than requested")
                })?;
        Ok(changes)
    }

    fn finish(&mut self) -> Result<(), CliError> {
        let writer_result: Result<(), CliError> =
            self.stdin_writer
                .take()
                .map_or(Ok(()), |handle| match handle.join() {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(source)) => Err(CliError::io(
                        "failed to write persistent git diff-tree input",
                        source,
                    )),
                    Err(_) => Err(CliError::msg("git diff-tree stdin writer panicked")),
                });
        self.stdout.take();
        let status = match self.child.take() {
            Some(mut child) => child.wait().map_err(|source| {
                CliError::io("failed to wait for persistent git diff-tree", source)
            })?,
            None => return Ok(()),
        };
        let stderr = self
            .stderr_reader
            .take()
            .ok_or_else(|| CliError::msg("missing persistent git diff-tree stderr reader"))?
            .join()
            .map_err(|_| CliError::msg("git diff-tree stderr reader panicked"))?
            .map_err(|source| {
                CliError::io("failed to read persistent git diff-tree stderr", source)
            })?;
        writer_result?;
        if status.success() {
            return Ok(());
        }
        Err(git_process_error(
            &self.repo_path,
            "diff-tree --stdin",
            status,
            &stderr,
        ))
    }
}

impl Drop for GitDiffTreeReader {
    fn drop(&mut self) {
        self.stdout.take();
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        if let Some(writer) = self.stdin_writer.take() {
            let _ = writer.join();
        }
        if let Some(stderr) = self.stderr_reader.take() {
            let _ = stderr.join();
        }
    }
}

struct GitBlobReader {
    repo_path: PathBuf,
    child: Option<Child>,
    stdin: Option<BufWriter<ChildStdin>>,
    stdout: BufReader<ChildStdout>,
    stderr_reader: Option<JoinHandle<std::io::Result<Vec<u8>>>>,
}

impl GitBlobReader {
    fn spawn(repo_path: &Path) -> Result<Self, CliError> {
        let mut command = Command::new("git");
        command
            .arg("-C")
            .arg(repo_path)
            .args(["cat-file", "--batch"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command
            .spawn()
            .map_err(|source| CliError::io("failed to spawn persistent git cat-file", source))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| CliError::msg("failed to open stdin for persistent git cat-file"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| CliError::msg("failed to open stdout for persistent git cat-file"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| CliError::msg("failed to open stderr for persistent git cat-file"))?;
        let stderr_reader = thread::spawn(move || {
            let mut stderr = stderr;
            let mut bytes = Vec::new();
            stderr.read_to_end(&mut bytes)?;
            Ok(bytes)
        });
        Ok(Self {
            repo_path: repo_path.to_path_buf(),
            child: Some(child),
            stdin: Some(BufWriter::new(stdin)),
            stdout: BufReader::new(stdout),
            stderr_reader: Some(stderr_reader),
        })
    }

    fn read_blobs(&mut self, blob_ids: &[String]) -> Result<HashMap<String, Vec<u8>>, CliError> {
        let mut blobs = HashMap::with_capacity(blob_ids.len());
        for requested_oid in blob_ids {
            if !is_full_git_oid(requested_oid.as_bytes()) {
                return Err(CliError::msg(format!(
                    "refusing malformed git blob object id {requested_oid}"
                )));
            }
            let stdin = self
                .stdin
                .as_mut()
                .ok_or_else(|| CliError::msg("persistent git cat-file is already closed"))?;
            stdin
                .write_all(requested_oid.as_bytes())
                .and_then(|()| stdin.write_all(b"\n"))
                .and_then(|()| stdin.flush())
                .map_err(|source| {
                    CliError::io("failed to request blob from git cat-file", source)
                })?;

            let mut header = Vec::new();
            self.stdout
                .read_until(b'\n', &mut header)
                .map_err(|source| CliError::io("failed to read git cat-file header", source))?;
            let Some(header_without_newline) = header.strip_suffix(b"\n") else {
                return Err(CliError::msg(format!(
                    "git cat-file output truncated while reading header for {requested_oid}"
                )));
            };
            let header = std::str::from_utf8(header_without_newline).map_err(|_| {
                CliError::msg(format!(
                    "malformed non-UTF-8 git cat-file header for {requested_oid}"
                ))
            })?;
            let fields = header.split_ascii_whitespace().collect::<Vec<_>>();
            if fields.len() == 2 && fields[1] == "missing" {
                return Err(CliError::msg(format!(
                    "missing blob object in git repository: {}",
                    fields[0]
                )));
            }
            if fields.len() != 3 {
                return Err(CliError::msg(format!(
                    "malformed git cat-file header for {requested_oid}: {header}"
                )));
            }
            if fields[0] != requested_oid {
                return Err(CliError::msg(format!(
                    "git cat-file object order mismatch: requested {requested_oid}, got {}",
                    fields[0]
                )));
            }
            if fields[1] != "blob" {
                return Err(CliError::msg(format!(
                    "git object {requested_oid} is {}, not a blob",
                    fields[1]
                )));
            }
            let size = fields[2].parse::<usize>().map_err(|_| {
                CliError::msg(format!(
                    "invalid blob size '{}' in git cat-file output for {requested_oid}",
                    fields[2]
                ))
            })?;
            let mut data = vec![0u8; size];
            self.stdout.read_exact(&mut data).map_err(|source| {
                CliError::io("git cat-file output truncated while reading blob", source)
            })?;
            let mut separator = [0u8; 1];
            self.stdout.read_exact(&mut separator).map_err(|source| {
                CliError::io("git cat-file output truncated after blob", source)
            })?;
            if separator != *b"\n" {
                return Err(CliError::msg(format!(
                    "malformed git cat-file output: blob {requested_oid} lacks trailing newline"
                )));
            }
            blobs.insert(requested_oid.clone(), data);
        }
        Ok(blobs)
    }

    fn finish(&mut self) -> Result<(), CliError> {
        self.stdin.take();
        let status = self
            .child
            .take()
            .ok_or_else(|| CliError::msg("persistent git cat-file is already closed"))?
            .wait()
            .map_err(|source| CliError::io("failed to wait for persistent git cat-file", source))?;
        let stderr = self
            .stderr_reader
            .take()
            .ok_or_else(|| CliError::msg("missing persistent git cat-file stderr reader"))?
            .join()
            .map_err(|_| CliError::msg("git cat-file stderr reader panicked"))?
            .map_err(|source| {
                CliError::io("failed to read persistent git cat-file stderr", source)
            })?;
        if status.success() {
            return Ok(());
        }
        Err(git_process_error(
            &self.repo_path,
            "cat-file --batch",
            status,
            &stderr,
        ))
    }
}

impl Drop for GitBlobReader {
    fn drop(&mut self) {
        self.stdin.take();
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        if let Some(stderr) = self.stderr_reader.take() {
            let _ = stderr.join();
        }
    }
}

#[derive(Debug)]
struct RawDiffHeader {
    status: char,
    old_mode: String,
    new_mode: String,
    new_oid: String,
}

impl RawDiffHeader {
    fn requires_second_path(&self) -> bool {
        matches!(self.status, 'R' | 'C')
    }
}

fn parse_raw_diff_tree(raw: &[u8]) -> Result<Vec<Change>, CliError> {
    let mut tokens = raw.split(|byte| *byte == 0).collect::<Vec<_>>();
    if tokens.last().is_some_and(|token| token.is_empty()) {
        tokens.pop();
    }
    let mut changes = Vec::new();
    let mut index = 0usize;
    while index < tokens.len() {
        let header = parse_raw_diff_header(tokens[index])?;
        index += 1;
        let first_path = *tokens
            .get(index)
            .ok_or_else(|| CliError::msg("malformed git diff-tree output: missing path token"))?;
        index += 1;
        let second_path = if header.requires_second_path() {
            let path = *tokens.get(index).ok_or_else(|| {
                CliError::msg("malformed git diff-tree output: missing rename destination")
            })?;
            index += 1;
            Some(path)
        } else {
            None
        };
        changes.push(change_from_raw_parts(header, first_path, second_path)?);
    }
    Ok(changes)
}

fn parse_raw_diff_header(header: &[u8]) -> Result<RawDiffHeader, CliError> {
    if !header.starts_with(b":") {
        return Err(CliError::msg(
            "malformed git diff-tree output: expected raw header",
        ));
    }
    let header = std::str::from_utf8(header)
        .map_err(|_| CliError::msg("malformed git diff-tree output: raw header is not ASCII"))?;
    let fields = header[1..].split_ascii_whitespace().collect::<Vec<_>>();
    if fields.len() != 5 {
        return Err(CliError::msg(format!(
            "malformed git diff-tree raw header: {header}"
        )));
    }
    if !is_full_git_oid(fields[2].as_bytes()) || !is_full_git_oid(fields[3].as_bytes()) {
        return Err(CliError::msg(format!(
            "malformed git diff-tree raw header object ids: {header}"
        )));
    }
    let status = fields[4]
        .chars()
        .next()
        .filter(char::is_ascii_alphabetic)
        .ok_or_else(|| CliError::msg(format!("malformed git diff-tree status: {}", fields[4])))?;
    Ok(RawDiffHeader {
        status,
        old_mode: fields[0].to_string(),
        new_mode: fields[1].to_string(),
        new_oid: fields[3].to_string(),
    })
}

fn change_from_raw_parts(
    header: RawDiffHeader,
    first_path: &[u8],
    second_path: Option<&[u8]>,
) -> Result<Change, CliError> {
    let first_path = GitPath::from_diff_token(first_path)?;
    if header.requires_second_path() {
        let second_path = second_path.ok_or_else(|| {
            CliError::msg("malformed git diff-tree output: missing rename destination")
        })?;
        return Ok(Change {
            status: header.status,
            old_mode: header.old_mode,
            new_mode: header.new_mode,
            new_oid: header.new_oid,
            old_path: Some(first_path),
            new_path: Some(GitPath::from_diff_token(second_path)?),
        });
    }
    Ok(Change {
        status: header.status,
        old_mode: header.old_mode,
        new_mode: header.new_mode,
        new_oid: header.new_oid,
        old_path: if header.status == 'A' {
            None
        } else {
            Some(first_path.clone())
        },
        new_path: if header.status == 'D' {
            None
        } else {
            Some(first_path)
        },
    })
}

fn read_nul_token(reader: &mut BufReader<ChildStdout>) -> Result<Option<Vec<u8>>, CliError> {
    let mut token = Vec::new();
    let read = reader
        .read_until(0, &mut token)
        .map_err(|source| CliError::io("failed to read git diff-tree output", source))?;
    if read == 0 {
        return Ok(None);
    }
    if token.last() != Some(&0) {
        return Err(CliError::msg(
            "git diff-tree output ended before a NUL-terminated token completed",
        ));
    }
    token.pop();
    Ok(Some(token))
}

fn is_full_git_oid(value: &[u8]) -> bool {
    matches!(value.len(), 40 | 64) && value.iter().all(u8::is_ascii_hexdigit)
}

fn git_process_error(
    repo_path: &Path,
    operation: &str,
    status: std::process::ExitStatus,
    stderr: &[u8],
) -> CliError {
    let status = status
        .code()
        .map(|code| format!("exit code {code}"))
        .unwrap_or_else(|| "terminated by signal".to_string());
    CliError::msg(format!(
        "git -C {} {} failed with {}: {}",
        repo_path.display(),
        operation,
        status,
        String::from_utf8_lossy(stderr).trim()
    ))
}

fn collect_wanted_blob_ids(changes: &[Change]) -> Vec<String> {
    let mut wanted_blob_ids = BTreeSet::<String>::new();
    for change in changes {
        if change.new_path.is_none() || !change.new_is_blob() {
            continue;
        }
        if !change.new_oid.is_empty() && !is_null_git_oid(&change.new_oid) {
            wanted_blob_ids.insert(change.new_oid.clone());
        }
    }
    wanted_blob_ids.into_iter().collect()
}

fn prepare_commit_changes(
    state: &mut ReplayState,
    changes: &[Change],
    blob_by_oid: &HashMap<String, Vec<u8>>,
) -> Result<PreparedBatch, CliError> {
    let mut delete_ids = BTreeSet::<String>::new();
    let mut inserts_by_id = BTreeMap::<String, WriteRow>::new();
    let mut updates_by_id = BTreeMap::<String, WriteRow>::new();

    for change in changes {
        let status = normalize_status(change.status);

        if should_delete_old_entry(change, status) {
            if let Some(deleted_id) = resolve_delete_path(state, change) {
                delete_ids.insert(deleted_id.clone());
                inserts_by_id.remove(&deleted_id);
                updates_by_id.remove(&deleted_id);
            }
        }

        if status == 'D' {
            continue;
        }

        if !mode_is_replay_file(&change.new_mode) {
            return Err(CliError::msg(format!(
                "unsupported Git file mode {} for replayed path {}",
                change.new_mode,
                change
                    .new_path
                    .as_ref()
                    .map(GitPath::lix_path)
                    .unwrap_or_else(|| "<missing>".to_string())
            )));
        }

        let Some(new_path) = &change.new_path else {
            continue;
        };

        let target = resolve_write_target(state, change)?;
        let data = if change.new_is_blob() {
            Some(
                blob_by_oid
                    .get(&change.new_oid)
                    .ok_or_else(|| {
                        CliError::msg(format!(
                            "missing blob {} while applying {} {}",
                            change.new_oid,
                            status,
                            new_path.lix_path()
                        ))
                    })?
                    .clone(),
            )
        } else {
            None
        };

        let row = WriteRow {
            id: target.id.clone(),
            path: new_path.lix_path(),
            data,
            git_mode: change.new_mode.clone(),
            git_oid: change.new_oid.clone(),
        };

        if delete_ids.contains(&row.id) {
            delete_ids.remove(&row.id);
        }

        if target.is_insert {
            inserts_by_id.insert(row.id.clone(), row);
            updates_by_id.remove(&target.id);
            state.known_file_ids.insert(target.id);
            continue;
        }

        if inserts_by_id.contains_key(&row.id) {
            inserts_by_id.insert(row.id.clone(), row);
            continue;
        }

        updates_by_id.insert(row.id.clone(), row);
    }

    Ok(PreparedBatch {
        deletes: delete_ids.into_iter().collect(),
        inserts: inserts_by_id.into_values().collect(),
        updates: updates_by_id.into_values().collect(),
    })
}

fn should_delete_old_entry(change: &Change, status: char) -> bool {
    if change.old_path.is_none() || !mode_is_replay_file(&change.old_mode) {
        return false;
    }

    match status {
        // A V2 derived materialization proof is path-bound. Git's path rename
        // must therefore retire the old descriptor and create a new one, so
        // the installed Git-text plugin can derive a proof for the new path
        // in the same atomic replay revision.
        'D' | 'R' => true,
        'A' | 'C' => false,
        _ => !mode_is_replay_file(&change.new_mode),
    }
}

struct WriteTarget {
    id: String,
    is_insert: bool,
}

fn resolve_delete_path(state: &mut ReplayState, change: &Change) -> Option<String> {
    let old_path = change.old_path.as_ref()?;
    let id = state.path_to_file_id.remove(old_path)?;
    state.known_file_ids.remove(&id);
    Some(id)
}

fn resolve_write_target(state: &mut ReplayState, change: &Change) -> Result<WriteTarget, CliError> {
    let new_path = change
        .new_path
        .as_ref()
        .ok_or(CliError::InvalidArgs("write target requires new path"))?;

    if let Some(existing_id) = state.path_to_file_id.get(new_path).cloned() {
        return Ok(WriteTarget {
            id: existing_id,
            is_insert: false,
        });
    }

    let generated = stable_file_id(new_path);
    let is_insert = !state.known_file_ids.contains(&generated);
    state
        .path_to_file_id
        .insert(new_path.clone(), generated.clone());
    Ok(WriteTarget {
        id: generated,
        is_insert,
    })
}

fn build_replay_commit_statements(
    batch: &PreparedBatch,
    max_insert_rows: usize,
) -> Vec<SqlStatement> {
    if batch.deletes.is_empty() && batch.inserts.is_empty() && batch.updates.is_empty() {
        return Vec::new();
    }

    let mut statements = Vec::<SqlStatement>::new();

    for delete_chunk in batch.deletes.chunks(500) {
        if delete_chunk.is_empty() {
            continue;
        }

        let placeholders = vec!["?"; delete_chunk.len()].join(", ");
        let sql = format!("DELETE FROM lix_file WHERE id IN ({placeholders})");
        let params = delete_chunk
            .iter()
            .cloned()
            .map(Value::Text)
            .collect::<Vec<_>>();
        statements.push(SqlStatement { sql, params });
    }

    let insert_batch_size = max_insert_rows.max(1);
    for insert_chunk in batch.inserts.chunks(insert_batch_size) {
        if insert_chunk.is_empty() {
            continue;
        }

        let mut params = Vec::<Value>::with_capacity(insert_chunk.len() * 4);
        let values_sql = insert_chunk
            .iter()
            .map(|row| {
                params.push(Value::Text(row.id.clone()));
                params.push(Value::Text(row.path.clone()));
                params.push(value_from_optional_blob(row.data.as_ref()));
                params.push(git_file_metadata_value(row));
                "(?, ?, ?, ?)"
            })
            .collect::<Vec<_>>()
            .join(", ");
        let sql =
            format!("INSERT INTO lix_file (id, path, data, lixcol_metadata) VALUES {values_sql}");
        statements.push(SqlStatement { sql, params });
    }

    for row in &batch.updates {
        statements.push(SqlStatement {
            sql: "UPDATE lix_file SET data = ?, lixcol_metadata = ? WHERE id = ?".to_string(),
            params: vec![
                value_from_optional_blob(row.data.as_ref()),
                git_file_metadata_value(row),
                Value::Text(row.id.clone()),
            ],
        });
    }

    statements
}

fn value_from_optional_blob(data: Option<&Vec<u8>>) -> Value {
    // A Gitlink is a commit reference rather than a blob. `lix_file` requires
    // binary data, so its payload is empty and the real Git type/object id is
    // carried in metadata. Readers must use `git_mode` to distinguish it from
    // an ordinary empty blob.
    Value::Blob(data.cloned().unwrap_or_default().into())
}

fn git_file_metadata_value(row: &WriteRow) -> Value {
    Value::Json(json!({
        "git_mode": row.git_mode,
        "git_oid": row.git_oid,
    }))
}

fn apply_prepared_to_expected_state(
    expected_state_by_id: &mut HashMap<String, ExpectedFile>,
    prepared: &PreparedBatch,
) {
    for id in &prepared.deletes {
        expected_state_by_id.remove(id);
    }

    for row in prepared.inserts.iter().chain(prepared.updates.iter()) {
        expected_state_by_id.insert(
            row.id.clone(),
            ExpectedFile {
                path: row.path.clone(),
                sha256: row.data.as_deref().map(sha256_hex),
                git_mode: row.git_mode.clone(),
                git_oid: row.git_oid.clone(),
            },
        );
    }
}

fn verify_commit_state_hashes(
    lix: &RocksLix,
    expected_state_by_id: &HashMap<String, ExpectedFile>,
    commit_sha: &str,
) -> Result<(), CliError> {
    let params: &[Value] = &[];
    let result = db::block_on(lix.execute(
        "SELECT id, path, data, lixcol_metadata FROM lix_file \
         WHERE path NOT LIKE '/.lix/plugins/%'",
        params,
    ))
    .map_err(|err| {
        CliError::msg(format!(
            "failed to query replay state for verification: {err}"
        ))
    })?;
    let rows = result.rows();
    if rows.len() != expected_state_by_id.len() {
        return Err(CliError::msg(format!(
            "state mismatch at {commit_sha}: row count differs (lix={}, expected={})",
            rows.len(),
            expected_state_by_id.len()
        )));
    }

    let mut seen = HashSet::<String>::new();
    for (index, row) in rows.iter().enumerate() {
        if row.values().len() < 4 {
            return Err(CliError::msg(format!(
                "state mismatch at {commit_sha}: row {index} has fewer than 4 columns"
            )));
        }

        let id = value_to_string(
            row.get_index(0)
                .ok_or_else(|| CliError::msg(format!("missing verify.id[{index}]")))?,
            &format!("verify.id[{index}]"),
        )?;
        let path = value_to_string(
            row.get_index(1)
                .ok_or_else(|| CliError::msg(format!("missing verify.path[{index}]")))?,
            &format!("verify.path[{index}]"),
        )?;
        let data = value_to_optional_blob(
            row.get_index(2)
                .ok_or_else(|| CliError::msg(format!("missing verify.data[{index}]")))?,
            &format!("verify.data[{index}]"),
        )?;
        let metadata = value_to_json(
            row.get_index(3)
                .ok_or_else(|| CliError::msg(format!("missing verify.metadata[{index}]")))?,
            &format!("verify.metadata[{index}]"),
        )?;

        let expected = expected_state_by_id.get(&id).ok_or_else(|| {
            CliError::msg(format!(
                "state mismatch at {commit_sha}: unexpected file id in lix state: {id}"
            ))
        })?;
        if expected.path != path {
            return Err(CliError::msg(format!(
                "state mismatch at {commit_sha}: path differs for id {id} (lix={path}, expected={})",
                expected.path
            )));
        }
        if mode_is_gitlink(&expected.git_mode) {
            if data.is_some_and(|bytes| !bytes.is_empty()) {
                return Err(CliError::msg(format!(
                    "state mismatch at {commit_sha}: gitlink {id} has non-empty synthetic payload"
                )));
            }
        } else {
            let hash = data.map(sha256_hex);
            if expected.sha256 != hash {
                return Err(CliError::msg(format!(
                    "state mismatch at {commit_sha}: hash differs for id {id}"
                )));
            }
        }
        if metadata.get("git_mode").and_then(serde_json::Value::as_str) != Some(&expected.git_mode)
            || metadata.get("git_oid").and_then(serde_json::Value::as_str)
                != Some(&expected.git_oid)
        {
            return Err(CliError::msg(format!(
                "state mismatch at {commit_sha}: Git metadata differs for id {id}"
            )));
        }

        seen.insert(id);
    }

    if seen.len() != expected_state_by_id.len() {
        return Err(CliError::msg(format!(
            "state mismatch at {commit_sha}: missing rows (lix={}, expected={})",
            seen.len(),
            expected_state_by_id.len()
        )));
    }

    Ok(())
}

fn verify_final_git_tree(
    repo_path: &Path,
    commit_sha: &str,
    blob_reader: &mut GitBlobReader,
    lix: &RocksLix,
) -> Result<(), CliError> {
    let tree_changes = read_tree_snapshot_changes(repo_path, commit_sha)?;
    let blob_by_oid = blob_reader.read_blobs(&collect_wanted_blob_ids(&tree_changes))?;
    let mut expected_by_path = HashMap::<String, ExpectedFile>::with_capacity(tree_changes.len());
    for change in tree_changes {
        let path = change
            .new_path
            .as_ref()
            .ok_or_else(|| CliError::msg("Git tree snapshot entry has no path"))?
            .lix_path();
        let sha256 = if change.new_is_blob() {
            let bytes = blob_by_oid.get(&change.new_oid).ok_or_else(|| {
                CliError::msg(format!(
                    "Git tree blob {} was not returned for {path}",
                    change.new_oid
                ))
            })?;
            Some(sha256_hex(bytes))
        } else {
            None
        };
        if expected_by_path.contains_key(&path) {
            return Err(CliError::msg(format!(
                "Git tree snapshot contains duplicate path {path}"
            )));
        }
        expected_by_path.insert(
            path.clone(),
            ExpectedFile {
                path,
                sha256,
                git_mode: change.new_mode,
                git_oid: change.new_oid,
            },
        );
    }

    let result = db::block_on(lix.execute(
        "SELECT path, data, lixcol_metadata FROM lix_file \
         WHERE path NOT LIKE '/.lix/plugins/%'",
        &[],
    ))
    .map_err(|error| CliError::msg(format!("failed to query Lix final tree: {error}")))?;
    let rows = result.rows();
    if rows.len() != expected_by_path.len() {
        return Err(CliError::msg(format!(
            "final tree mismatch at {commit_sha}: row count differs (lix={}, git={})",
            rows.len(),
            expected_by_path.len()
        )));
    }

    let mut seen = HashSet::with_capacity(rows.len());
    for (index, row) in rows.iter().enumerate() {
        let path = value_to_string(
            row.get_index(0)
                .ok_or_else(|| CliError::msg(format!("missing final.path[{index}]")))?,
            &format!("final.path[{index}]"),
        )?;
        let data = value_to_optional_blob(
            row.get_index(1)
                .ok_or_else(|| CliError::msg(format!("missing final.data[{index}]")))?,
            &format!("final.data[{index}]"),
        )?;
        let metadata = value_to_json(
            row.get_index(2)
                .ok_or_else(|| CliError::msg(format!("missing final.metadata[{index}]")))?,
            &format!("final.metadata[{index}]"),
        )?;
        let expected = expected_by_path.get(&path).ok_or_else(|| {
            CliError::msg(format!(
                "final tree mismatch at {commit_sha}: unexpected Lix path {path}"
            ))
        })?;
        verify_file_manifest_entry(&path, data, metadata, expected, commit_sha)?;
        seen.insert(path);
    }
    if seen.len() != expected_by_path.len() {
        return Err(CliError::msg(format!(
            "final tree mismatch at {commit_sha}: Lix is missing Git paths"
        )));
    }
    Ok(())
}

fn verify_file_manifest_entry(
    path: &str,
    data: Option<&[u8]>,
    metadata: &serde_json::Value,
    expected: &ExpectedFile,
    commit_sha: &str,
) -> Result<(), CliError> {
    if mode_is_gitlink(&expected.git_mode) {
        if data.is_some_and(|bytes| !bytes.is_empty()) {
            return Err(CliError::msg(format!(
                "state mismatch at {commit_sha}: gitlink {path} has non-empty synthetic payload"
            )));
        }
    } else {
        let hash = data.map(sha256_hex);
        if expected.sha256 != hash {
            return Err(CliError::msg(format!(
                "state mismatch at {commit_sha}: hash differs for path {path}"
            )));
        }
    }
    if metadata.get("git_mode").and_then(serde_json::Value::as_str) != Some(&expected.git_mode)
        || metadata.get("git_oid").and_then(serde_json::Value::as_str) != Some(&expected.git_oid)
    {
        return Err(CliError::msg(format!(
            "state mismatch at {commit_sha}: Git metadata differs for path {path}"
        )));
    }
    Ok(())
}

fn value_to_string(value: &Value, context: &str) -> Result<String, CliError> {
    match value {
        Value::Text(text) => Ok(text.clone()),
        Value::Integer(number) => Ok(number.to_string()),
        Value::Real(number) => Ok(number.to_string()),
        Value::Boolean(flag) => Ok(flag.to_string()),
        _ => Err(CliError::msg(format!(
            "unexpected scalar type for {context}"
        ))),
    }
}

fn value_to_optional_blob<'a>(
    value: &'a Value,
    context: &str,
) -> Result<Option<&'a [u8]>, CliError> {
    match value {
        Value::Null => Ok(None),
        Value::Blob(bytes) => Ok(Some(bytes)),
        _ => Err(CliError::msg(format!("unexpected blob type for {context}"))),
    }
}

fn value_to_json<'a>(value: &'a Value, context: &str) -> Result<&'a serde_json::Value, CliError> {
    match value {
        Value::Json(value) => Ok(value),
        _ => Err(CliError::msg(format!("unexpected JSON type for {context}"))),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        out.push(hex_digit_lower(byte >> 4));
        out.push(hex_digit_lower(byte & 0x0f));
    }
    out
}

fn hex_digit_lower(value: u8) -> char {
    match value {
        0..=9 => (b'0' + value) as char,
        10..=15 => (b'a' + (value - 10)) as char,
        _ => '0',
    }
}

fn hex_digit_upper(value: u8) -> char {
    match value {
        0..=9 => (b'0' + value) as char,
        10..=15 => (b'A' + (value - 10)) as char,
        _ => '0',
    }
}

fn normalize_status(value: char) -> char {
    value.to_ascii_uppercase()
}

fn stable_file_id(path: &GitPath) -> String {
    path.lix_path()
}

fn encode_git_path_bytes(path: &[u8]) -> String {
    let mut encoded = String::new();
    for byte in path {
        if *byte == b'/' {
            encoded.push('/');
            continue;
        }
        let is_alpha_num = byte.is_ascii_alphanumeric();
        let is_safe = matches!(*byte, b'.' | b'_' | b'~' | b'-');
        if is_alpha_num || is_safe {
            encoded.push(*byte as char);
        } else {
            encoded.push('%');
            encoded.push(hex_digit_upper(byte >> 4));
            encoded.push(hex_digit_upper(byte & 0x0f));
        }
    }
    encoded
}

fn mode_is_blob(mode: &str) -> bool {
    matches!(mode, "100644" | "100755" | "120000")
}

fn mode_is_gitlink(mode: &str) -> bool {
    mode == "160000"
}

fn mode_is_replay_file(mode: &str) -> bool {
    mode_is_blob(mode) || mode_is_gitlink(mode)
}

fn is_null_git_oid(oid: &str) -> bool {
    !oid.is_empty() && oid.as_bytes().iter().all(|byte| *byte == b'0')
}

fn run_git_text(
    repo_path: &Path,
    args: &[String],
    stdin: Option<&[u8]>,
) -> Result<String, CliError> {
    let output = run_git_bytes(repo_path, args, stdin)?;
    Ok(String::from_utf8_lossy(&output).to_string())
}

fn run_git_bytes(
    repo_path: &Path,
    args: &[String],
    stdin: Option<&[u8]>,
) -> Result<Vec<u8>, CliError> {
    let mut command = Command::new("git");
    command.arg("-C").arg(repo_path);
    for arg in args {
        command.arg(arg);
    }
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());
    if stdin.is_some() {
        command.stdin(Stdio::piped());
    } else {
        command.stdin(Stdio::null());
    }

    let mut child = command
        .spawn()
        .map_err(|source| CliError::io("failed to spawn git command", source))?;

    if let Some(input) = stdin {
        let mut child_stdin = child
            .stdin
            .take()
            .ok_or_else(|| CliError::msg("failed to open stdin for git command"))?;
        child_stdin
            .write_all(input)
            .map_err(|source| CliError::io("failed to write stdin for git command", source))?;
    }

    let output = child
        .wait_with_output()
        .map_err(|source| CliError::io("failed to wait for git command", source))?;

    if output.status.success() {
        return Ok(output.stdout);
    }

    let args_preview = args.join(" ");
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let status = output
        .status
        .code()
        .map(|code| format!("exit code {code}"))
        .unwrap_or_else(|| "terminated by signal".to_string());
    Err(CliError::msg(format!(
        "git -C {} {} failed with {}: {}",
        repo_path.display(),
        args_preview,
        status,
        stderr
    )))
}

fn install_embedded_git_text_plugin(lix: &RocksLix) -> Result<(), CliError> {
    let archive = build_embedded_git_text_plugin_archive()?;
    db::block_on(lix.execute(
        "INSERT INTO lix_file (path, data) VALUES (?, ?)",
        &[
            Value::Text(format!("/.lix/plugins/{GIT_TEXT_PLUGIN_KEY}.lixplugin")),
            Value::Blob(archive.into()),
        ],
    ))
    .map_err(|error| {
        CliError::msg(format!(
            "failed to install embedded Git-text plugin: {error}"
        ))
    })?;
    Ok(())
}

fn build_embedded_git_text_plugin_archive() -> Result<Vec<u8>, CliError> {
    let wasm_path = Path::new(env!(
        "CARGO_CDYLIB_FILE_PLUGIN_GIT_TEXT_V2_plugin_git_text_v2"
    ));
    let wasm = fs::read(wasm_path).map_err(|source| {
        CliError::msg(format!(
            "failed to read bindep-built Git-text plugin at {}: {source}",
            wasm_path.display()
        ))
    })?;
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options = SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (path, bytes) in [
        (
            "manifest.json",
            include_str!("../../../../../plugins/text-v2/manifest.json").as_bytes(),
        ),
        (
            "schema/git_text_line_v2.json",
            include_str!("../../../../../plugins/text-v2/schema/git_text_line_v2.json").as_bytes(),
        ),
        ("plugin.wasm", wasm.as_slice()),
    ] {
        writer.start_file(path, options).map_err(|error| {
            CliError::msg(format!(
                "failed to create Git-text plugin archive entry {path}: {error}"
            ))
        })?;
        writer.write_all(bytes).map_err(|source| {
            CliError::msg(format!(
                "failed to write Git-text plugin archive entry {path}: {source}"
            ))
        })?;
    }
    writer
        .finish()
        .map_err(|error| {
            CliError::msg(format!("failed to finish Git-text plugin archive: {error}"))
        })
        .map(Cursor::into_inner)
}

fn prepare_rocksdb_output_path(path: &Path, force: bool) -> Result<(), CliError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|source| CliError::io("failed to create RocksDB output parent", source))?;
    }
    if !path.exists() {
        return Ok(());
    }
    if !path.is_dir() {
        return Err(CliError::msg(format!(
            "RocksDB output path points to a file, expected a directory: {}",
            path.display()
        )));
    }
    if !force {
        return Err(CliError::msg(format!(
            "RocksDB output directory already exists: {} (pass --force to replace it)",
            path.display()
        )));
    }
    fs::remove_dir_all(path).map_err(|source| {
        CliError::io("failed to remove existing RocksDB output directory", source)
    })
}

fn validate_safe_rocksdb_output_path(repo_path: &Path, output_path: &Path) -> Result<(), CliError> {
    if output_path
        .components()
        .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        return Err(CliError::msg(format!(
            "RocksDB output path must not contain '.' or '..' components: {}",
            output_path.display()
        )));
    }
    let output = canonicalize_candidate_path(output_path)?;
    let repo = repo_path
        .canonicalize()
        .map_err(|source| CliError::io("failed to canonicalize git repository path", source))?;
    let cwd = std::env::current_dir()
        .map_err(|source| CliError::io("failed to read current directory", source))?
        .canonicalize()
        .map_err(|source| CliError::io("failed to canonicalize current directory", source))?;

    let protected = ["/", "/tmp", "/dev", "/dev/shm"];
    if output.parent().is_none()
        || output.parent() == Some(Path::new("/"))
        || protected
            .iter()
            .any(|protected_path| output == Path::new(protected_path))
    {
        return Err(CliError::msg(format!(
            "refusing broad RocksDB output directory: {}",
            output.display()
        )));
    }
    if paths_overlap(&output, &repo) || paths_overlap(&output, &cwd) {
        return Err(CliError::msg(format!(
            "RocksDB output directory must not overlap the repository or current directory: {}",
            output.display()
        )));
    }
    Ok(())
}

fn canonicalize_candidate_path(path: &Path) -> Result<PathBuf, CliError> {
    let mut probe = path;
    let mut missing_suffix = Vec::<PathBuf>::new();
    while !probe.exists() {
        let name = probe.file_name().ok_or_else(|| {
            CliError::msg(format!(
                "cannot resolve an existing parent for output path {}",
                path.display()
            ))
        })?;
        missing_suffix.push(PathBuf::from(name));
        probe = probe.parent().ok_or_else(|| {
            CliError::msg(format!(
                "cannot resolve an existing parent for output path {}",
                path.display()
            ))
        })?;
    }
    let mut canonical = probe
        .canonicalize()
        .map_err(|source| CliError::io("failed to canonicalize RocksDB output path", source))?;
    for segment in missing_suffix.iter().rev() {
        canonical.push(segment);
    }
    Ok(canonical)
}

fn paths_overlap(left: &Path, right: &Path) -> bool {
    left.starts_with(right) || right.starts_with(left)
}

fn prepare_regular_output_path(path: &Path, force: bool) -> Result<(), CliError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|source| CliError::io("failed to create output directory", source))?;
    }

    if path.exists() {
        if path.is_dir() {
            return Err(CliError::msg(format!(
                "output path points to a directory, expected a file: {}",
                path.display()
            )));
        }
        if force {
            fs::remove_file(path)
                .map_err(|source| CliError::io("failed to remove existing output file", source))?;
            return Ok(());
        }
        return Err(CliError::msg(format!(
            "output path already exists: {}",
            path.display()
        )));
    }

    Ok(())
}

fn validate_repo_dir(path: &Path) -> Result<(), CliError> {
    if path.is_dir() {
        return Ok(());
    }

    Err(CliError::msg(format!(
        "repo path does not exist or is not a directory: {}",
        path.display()
    )))
}

fn validate_git_repo(path: &Path) -> Result<(), CliError> {
    let args = vec!["rev-parse".to_string(), "--is-inside-work-tree".to_string()];
    let output = run_git_text(path, &args, None)?;
    if output.trim() == "true" {
        return Ok(());
    }
    Err(CliError::msg(format!(
        "repo path is not a git work tree: {}",
        path.display()
    )))
}

fn absolutize_from_cwd(path: &Path) -> Result<PathBuf, CliError> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }

    let cwd = std::env::current_dir()
        .map_err(|source| CliError::io("failed to read current directory", source))?;
    Ok(cwd.join(path))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn collect_wanted_blob_ids_skips_gitlink_oids() {
        let changes = vec![
            Change {
                status: 'A',
                old_mode: "000000".to_string(),
                new_mode: "100644".to_string(),
                new_oid: "1111111111111111111111111111111111111111".to_string(),
                old_path: None,
                new_path: Some(git_path(b"regular.txt")),
            },
            Change {
                status: 'A',
                old_mode: "000000".to_string(),
                new_mode: "160000".to_string(),
                new_oid: "4c9431adbd4a24aed1d9afdecbfe4eaac3a6bba9".to_string(),
                old_path: None,
                new_path: Some(git_path(b"submodule")),
            },
        ];

        let wanted = collect_wanted_blob_ids(&changes);
        assert_eq!(
            wanted,
            vec!["1111111111111111111111111111111111111111".to_string()]
        );
    }

    #[test]
    fn select_replay_commits_starts_from_specific_commit_inclusive() {
        let commits = replay_commits(&["a", "b", "c", "d"]);
        let selected = select_replay_commits(commits, Some("c"), None)
            .expect("select_replay_commits should succeed");
        assert_eq!(commit_shas(&selected), vec!["c", "d"]);
    }

    #[test]
    fn select_replay_commits_applies_limit_after_from_commit() {
        let commits = replay_commits(&["a", "b", "c", "d"]);
        let selected = select_replay_commits(commits, Some("b"), Some(2))
            .expect("select_replay_commits should succeed");
        assert_eq!(commit_shas(&selected), vec!["b", "c"]);
    }

    #[test]
    fn select_replay_commits_errors_when_from_commit_missing() {
        let commits = replay_commits(&["a", "b"]);
        let result = select_replay_commits(commits, Some("missing"), None);
        assert!(result.is_err(), "expected error for missing from-commit");
        let message = format!(
            "{}",
            result.expect_err("expected missing from-commit error")
        );
        assert!(
            message.contains("not reachable from selected ref"),
            "unexpected error message: {message}"
        );
    }

    #[test]
    fn prepare_commit_changes_typechange_blob_to_gitlink_preserves_file_identity() {
        let mut state = ReplayState::default();
        state.path_to_file_id.insert(
            git_path(b"artifact/spa-prerender-repro"),
            "/artifact/spa-prerender-repro".to_string(),
        );
        state
            .known_file_ids
            .insert("/artifact/spa-prerender-repro".to_string());

        let changes = vec![Change {
            status: 'T',
            old_mode: "100644".to_string(),
            new_mode: "160000".to_string(),
            new_oid: "4c9431adbd4a24aed1d9afdecbfe4eaac3a6bba9".to_string(),
            old_path: Some(git_path(b"artifact/spa-prerender-repro")),
            new_path: Some(git_path(b"artifact/spa-prerender-repro")),
        }];

        let prepared = prepare_commit_changes(&mut state, &changes, &HashMap::new())
            .expect("gitlink typechange should not error");

        assert!(prepared.deletes.is_empty());
        assert!(prepared.inserts.is_empty());
        assert_eq!(prepared.updates.len(), 1);
        assert_eq!(prepared.updates[0].data, None);
        assert_eq!(prepared.updates[0].git_mode, "160000");
        assert!(
            state
                .path_to_file_id
                .contains_key(&git_path(b"artifact/spa-prerender-repro"))
        );
    }

    #[test]
    fn prepare_commit_changes_rename_rebinds_path_bound_semantic_proof() {
        let old_path = git_path(b"src/old.ts");
        let new_path = git_path(b"src/new.ts");
        let mut state = ReplayState::default();
        state
            .path_to_file_id
            .insert(old_path.clone(), "/src/old.ts".to_string());
        state.known_file_ids.insert("/src/old.ts".to_string());
        let oid = "a".repeat(40);
        let changes = vec![Change {
            status: 'R',
            old_mode: "100644".to_string(),
            new_mode: "100644".to_string(),
            new_oid: oid.clone(),
            old_path: Some(old_path),
            new_path: Some(new_path.clone()),
        }];
        let blobs = HashMap::from([(oid, b"const rebind = true;\n".to_vec())]);

        let prepared =
            prepare_commit_changes(&mut state, &changes, &blobs).expect("rename should prepare");

        assert_eq!(prepared.deletes, vec!["/src/old.ts"]);
        assert_eq!(prepared.inserts.len(), 1);
        assert!(prepared.updates.is_empty());
        assert_eq!(prepared.inserts[0].id, "/src/new.ts");
        assert_eq!(prepared.inserts[0].path, "/src/new.ts");
        assert_eq!(
            state.path_to_file_id.get(&new_path),
            Some(&"/src/new.ts".to_string())
        );
    }

    #[test]
    fn prepare_rocksdb_output_path_rejects_existing_file() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("temp dir should be created");
        let output_path = temp_dir.join("existing.rocksdb");
        fs::write(&output_path, b"existing").expect("seed file should be written");

        let result = prepare_rocksdb_output_path(&output_path, false);
        assert!(result.is_err(), "expected error when output file exists");
        let message = format!("{}", result.expect_err("expected output path error"));
        assert!(
            message.contains("expected a directory"),
            "unexpected error message: {message}"
        );

        fs::remove_file(&output_path).expect("seed file should be removable");
        fs::remove_dir_all(&temp_dir).expect("temp dir should be removable");
    }

    #[test]
    fn prepare_rocksdb_output_path_allows_absent_directory_and_creates_parent() {
        let temp_dir = unique_temp_dir();
        let nested_parent = temp_dir.join("nested").join("output");
        let output_path = nested_parent.join("new.rocksdb");

        let result = prepare_rocksdb_output_path(&output_path, false);
        assert!(result.is_ok(), "expected success for absent output file");
        assert!(
            nested_parent.is_dir(),
            "expected parent directories to be created"
        );

        fs::remove_dir_all(&temp_dir).expect("temp dir should be removable");
    }

    #[test]
    fn prepare_rocksdb_output_path_force_removes_existing_directory() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("temp dir should be created");
        let output_path = temp_dir.join("existing.rocksdb");
        fs::create_dir_all(&output_path).expect("seed directory should be created");
        fs::write(output_path.join("CURRENT"), b"rocksdb").expect("seed file should be written");

        let result = prepare_rocksdb_output_path(&output_path, true);
        assert!(result.is_ok(), "expected success when force is enabled");
        assert!(
            !output_path.exists(),
            "expected existing output directory to be removed"
        );

        fs::remove_dir_all(&temp_dir).expect("temp dir should be removable");
    }

    #[test]
    fn rocksdb_output_rejects_repository_overlap_before_force_can_remove_it() {
        let repo = unique_temp_dir();
        fs::create_dir_all(&repo).expect("fixture repository should be created");
        let result = validate_safe_rocksdb_output_path(&repo, &repo.join("replay.rocksdb"));
        assert!(result.is_err(), "repository child must be rejected");
        fs::remove_dir_all(&repo).expect("fixture repository should be removable");
    }

    #[test]
    fn rocksdb_output_rejects_unresolved_parent_traversal() {
        let temp_dir = unique_temp_dir();
        let repo = temp_dir.join("repo");
        fs::create_dir_all(&repo).expect("fixture repository should be created");
        let output = temp_dir
            .join("missing-parent")
            .join("..")
            .join("repo")
            .join("replay.rocksdb");

        let result = validate_safe_rocksdb_output_path(&repo, &output);

        assert!(result.is_err(), "lexical parent traversal must be rejected");
        fs::remove_dir_all(&temp_dir).expect("fixture directory should be removable");
    }

    #[test]
    fn invalid_ref_does_not_delete_forced_rocksdb_output() {
        let temp_dir = unique_temp_dir();
        let repo = temp_dir.join("repo");
        let output = temp_dir.join("replay.rocksdb");
        fs::create_dir_all(&repo).expect("fixture repository should be created");
        git_ok(&repo, &["init", "-q", "-b", "main"]);
        fs::create_dir_all(&output).expect("existing output should be created");
        let sentinel = output.join("CURRENT");
        fs::write(&sentinel, b"previous replay").expect("sentinel should be written");

        let result = run(ExpGitReplayArgs {
            repo_path: repo,
            output_rocksdb_path: output,
            branch: "missing-ref".to_string(),
            from_commit: None,
            num_commits: None,
            verify_state: false,
            force: true,
            profile_json: None,
        });

        assert!(result.is_err(), "invalid ref must be rejected");
        assert!(
            sentinel.exists(),
            "invalid ref must not delete an existing forced output"
        );
        fs::remove_dir_all(&temp_dir).expect("fixture directory should be removable");
    }

    #[test]
    fn replay_marker_is_one_tracked_upsert_per_git_commit() {
        let statement = git_replay_marker_statement(&ReplayCommit {
            sha: "a".repeat(40),
            first_parent: Some("b".repeat(40)),
        });

        assert_eq!(
            statement.sql,
            "INSERT INTO lix_key_value (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value"
        );
        assert_eq!(
            statement.params[0],
            Value::Text(GIT_REPLAY_MARKER_KEY.to_string())
        );
        assert_eq!(
            statement.params[1],
            Value::Json(json!({"sha": "a".repeat(40), "first_parent": "b".repeat(40)}))
        );
    }

    #[test]
    fn build_replay_commit_statements_omits_path_for_stable_updates() {
        let batch = PreparedBatch {
            deletes: Vec::new(),
            inserts: Vec::new(),
            updates: vec![WriteRow {
                id: "/src/main.ts".to_string(),
                path: "/src/main.ts".to_string(),
                data: Some(b"hello".to_vec()),
                git_mode: "100755".to_string(),
                git_oid: "a".repeat(40),
            }],
        };

        let statements = build_replay_commit_statements(&batch, DEFAULT_INSERT_BATCH_ROWS);

        assert_eq!(statements.len(), 1);
        assert_eq!(
            statements[0].sql,
            "UPDATE lix_file SET data = ?, lixcol_metadata = ? WHERE id = ?"
        );
        assert_eq!(
            statements[0].params,
            vec![
                Value::Blob(b"hello".to_vec().into()),
                Value::Json(json!({"git_mode": "100755", "git_oid": "a".repeat(40)})),
                Value::Text("/src/main.ts".to_string())
            ]
        );
    }

    #[test]
    fn build_replay_commit_statements_rebinds_renames_via_delete_then_insert() {
        let batch = PreparedBatch {
            deletes: vec!["/src/old.ts".to_string()],
            inserts: vec![WriteRow {
                id: "/src/new.ts".to_string(),
                path: "/src/new.ts".to_string(),
                data: Some(b"hello".to_vec()),
                git_mode: "120000".to_string(),
                git_oid: "b".repeat(40),
            }],
            updates: Vec::new(),
        };

        let statements = build_replay_commit_statements(&batch, DEFAULT_INSERT_BATCH_ROWS);

        assert_eq!(statements.len(), 2);
        assert_eq!(statements[0].sql, "DELETE FROM lix_file WHERE id IN (?)");
        assert_eq!(
            statements[0].params,
            vec![Value::Text("/src/old.ts".to_string())]
        );
        assert_eq!(
            statements[1].sql,
            "INSERT INTO lix_file (id, path, data, lixcol_metadata) VALUES (?, ?, ?, ?)"
        );
        assert_eq!(
            statements[1].params,
            vec![
                Value::Text("/src/new.ts".to_string()),
                Value::Text("/src/new.ts".to_string()),
                Value::Blob(b"hello".to_vec().into()),
                Value::Json(json!({"git_mode": "120000", "git_oid": "b".repeat(40)})),
            ]
        );
    }

    #[test]
    fn gitlink_rows_use_empty_binary_payload_and_preserve_reference_metadata() {
        let batch = PreparedBatch {
            deletes: Vec::new(),
            inserts: vec![WriteRow {
                id: "/vendor/submodule".to_string(),
                path: "/vendor/submodule".to_string(),
                data: None,
                git_mode: "160000".to_string(),
                git_oid: "c".repeat(40),
            }],
            updates: Vec::new(),
        };

        let statements = build_replay_commit_statements(&batch, DEFAULT_INSERT_BATCH_ROWS);

        assert_eq!(statements.len(), 1);
        assert_eq!(statements[0].params[2], Value::Blob(Vec::new().into()));
        assert_eq!(
            statements[0].params[3],
            Value::Json(json!({"git_mode": "160000", "git_oid": "c".repeat(40)}))
        );
    }

    #[test]
    fn raw_diff_parser_preserves_non_utf8_git_path_bytes() {
        let raw = b":100644 100755 1111111111111111111111111111111111111111 2222222222222222222222222222222222222222 M\0dir/\xff name\0";
        let changes = parse_raw_diff_tree(raw).expect("raw diff should parse");

        assert_eq!(changes.len(), 1);
        let path = changes[0]
            .new_path
            .as_ref()
            .expect("modified entry should have new path");
        assert_eq!(path.0, b"dir/\xff name");
        assert_eq!(path.lix_path(), "/dir/%FF%20name");
    }

    #[test]
    fn null_sha256_oid_is_not_requested_as_a_blob() {
        assert!(is_null_git_oid(&"0".repeat(64)));
        assert!(!is_null_git_oid(&"a".repeat(64)));
    }

    #[test]
    fn persistent_diff_reader_replays_only_first_parent_merge_delta_and_empty_commits() {
        let repo = unique_temp_dir();
        fs::create_dir_all(&repo).expect("fixture repository should be created");
        git_ok(&repo, &["init", "-q", "-b", "main"]);
        git_ok(&repo, &["config", "user.email", "replay@example.test"]);
        git_ok(&repo, &["config", "user.name", "Replay Test"]);

        fs::write(repo.join("root.txt"), b"root\n").expect("root file should write");
        git_ok(&repo, &["add", "root.txt"]);
        git_ok(&repo, &["commit", "-qm", "root"]);
        git_ok(&repo, &["commit", "--allow-empty", "-qm", "empty"]);

        git_ok(&repo, &["checkout", "-qb", "side"]);
        fs::write(repo.join("side.txt"), b"side\n").expect("side file should write");
        git_ok(&repo, &["add", "side.txt"]);
        git_ok(&repo, &["commit", "-qm", "side"]);

        git_ok(&repo, &["checkout", "-q", "main"]);
        fs::write(repo.join("main.txt"), b"main\n").expect("main file should write");
        git_ok(&repo, &["add", "main.txt"]);
        git_ok(&repo, &["commit", "-qm", "main"]);
        git_ok(&repo, &["merge", "--no-ff", "-qm", "merge side", "side"]);

        let commits = list_linear_commits(&repo, "HEAD", None, None)
            .expect("first-parent history should list");
        assert_eq!(commits.len(), 4);
        let mut reader =
            GitDiffTreeReader::spawn(&repo, &commits).expect("persistent diff reader should start");
        let changes = commits
            .iter()
            .map(|commit| reader.read_commit(&commit.sha).expect("commit should read"))
            .collect::<Vec<_>>();
        reader
            .finish()
            .expect("persistent diff reader should finish");

        assert_eq!(changes[0].len(), 1, "root must add root.txt");
        assert!(
            changes[1].is_empty(),
            "empty Git commit must remain visible"
        );
        assert_eq!(changes[2].len(), 1, "main must add main.txt");
        assert_eq!(changes[3].len(), 1, "merge must include only side delta");
        assert_eq!(
            changes[3][0]
                .new_path
                .as_ref()
                .expect("merge add should have a path")
                .lix_path(),
            "/side.txt"
        );

        fs::remove_dir_all(&repo).expect("fixture repository should be removable");
    }

    #[test]
    fn rocksdb_replay_installs_git_text_and_keeps_nul_files_binary() {
        let fixture = unique_temp_dir();
        let repo = fixture.join("repo");
        let output = fixture.join("replay.rocksdb");
        let profile = fixture.join("profile.json");
        fs::create_dir_all(&repo).expect("fixture repository should be created");
        git_ok(&repo, &["init", "-q", "-b", "main"]);
        git_ok(&repo, &["config", "user.email", "replay@example.test"]);
        git_ok(&repo, &["config", "user.name", "Replay Test"]);

        fs::write(repo.join("notes.txt"), b"first line\nsecond line\n")
            .expect("text fixture should write");
        fs::write(repo.join("binary.bin"), b"raw\0bytes\n").expect("binary fixture should write");
        git_ok(&repo, &["add", "notes.txt", "binary.bin"]);
        git_ok(&repo, &["commit", "-qm", "root"]);
        git_ok(&repo, &["commit", "--allow-empty", "-qm", "empty"]);

        git_ok(&repo, &["checkout", "-qb", "side"]);
        fs::write(repo.join("side.txt"), b"side branch\n").expect("side fixture should write");
        git_ok(&repo, &["add", "side.txt"]);
        git_ok(&repo, &["commit", "-qm", "side"]);

        git_ok(&repo, &["checkout", "-q", "main"]);
        fs::create_dir_all(repo.join("docs")).expect("rename directory should be created");
        fs::rename(repo.join("notes.txt"), repo.join("docs/renamed.txt"))
            .expect("text fixture should rename");
        fs::write(repo.join("main.txt"), b"main branch\n").expect("main fixture should write");
        git_ok(&repo, &["add", "-A"]);
        git_ok(&repo, &["commit", "-qm", "rename on main"]);
        git_ok(
            &repo,
            &["merge", "--no-ff", "-q", "-m", "merge side", "side"],
        );

        run(ExpGitReplayArgs {
            repo_path: repo,
            output_rocksdb_path: output.clone(),
            branch: "main".to_string(),
            from_commit: None,
            num_commits: None,
            verify_state: true,
            force: false,
            profile_json: Some(profile.clone()),
        })
        .expect("RocksDB Git replay should complete");

        let profile_json: serde_json::Value =
            serde_json::from_slice(&fs::read(&profile).expect("replay profile should be written"))
                .expect("replay profile should be valid JSON");
        assert_eq!(
            profile_json
                .get("commits_replayed")
                .and_then(serde_json::Value::as_u64),
            Some(4),
            "first-parent replay must exclude the side-only commit"
        );
        assert_eq!(
            profile_json
                .get("commits_marker_only")
                .and_then(serde_json::Value::as_u64),
            Some(1),
            "the empty Git commit must remain a Lix revision"
        );

        let storage = RocksDB::open(&output).expect("replay RocksDB should reopen");
        let lix = db::block_on(open_lix_with_storage(storage))
            .expect("replay Lix should reopen with installed plugin");
        let text_rows = db::block_on(lix.execute(
            "SELECT lixcol_entity_pk FROM git_text_line_v2 WHERE lixcol_file_id = ?",
            &[Value::Text("/docs/renamed.txt".to_string())],
        ))
        .expect("Git text rows should be queryable after replay");
        assert!(
            !text_rows.rows().is_empty(),
            "renamed text file must derive Git-text semantic rows at its new path"
        );
        let binary_rows = db::block_on(lix.execute(
            "SELECT lixcol_entity_pk FROM git_text_line_v2 WHERE lixcol_file_id = ?",
            &[Value::Text("/binary.bin".to_string())],
        ))
        .expect("Git text rows should query for binary fixture");
        assert!(
            binary_rows.rows().is_empty(),
            "NUL-bearing file must remain a raw binary blob"
        );
        db::block_on(lix.close()).expect("reopened Lix should close");

        fs::remove_dir_all(&fixture).expect("fixture directory should be removable");
    }

    fn git_path(bytes: &[u8]) -> GitPath {
        GitPath::from_diff_token(bytes).expect("test Git path should be valid")
    }

    fn replay_commits(shas: &[&str]) -> Vec<ReplayCommit> {
        shas.iter()
            .enumerate()
            .map(|(index, sha)| ReplayCommit {
                sha: (*sha).to_string(),
                first_parent: index
                    .checked_sub(1)
                    .map(|previous| shas[previous].to_string()),
            })
            .collect()
    }

    fn commit_shas(commits: &[ReplayCommit]) -> Vec<&str> {
        commits.iter().map(|commit| commit.sha.as_str()).collect()
    }

    fn git_ok(repo: &Path, args: &[&str]) {
        let output = Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(args)
            .output()
            .expect("fixture git command should start");
        assert!(
            output.status.success(),
            "fixture git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn unique_temp_dir() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "lix-cli-git-replay-test-{}-{nanos}",
            std::process::id()
        ))
    }
}
