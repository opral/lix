use crate::cli::exp::{ExpGitReplayArgs, GitReplayParentTree, GitReplayPlugins, GitReplayStorage};
use crate::db;
use crate::error::CliError;
use lix::storage::Storage;
use lix::{Lix, Value, open_lix};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;
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

#[cfg(test)]
use lix::Memory;

const PROGRESS_EVERY: usize = 10;
const DEFAULT_INSERT_BATCH_ROWS: usize = 100;
const TREE_BLOB_READ_BATCH_ROWS: usize = 8;
const TREE_TRANSACTION_TARGET_BYTES: usize = 128 * 1024 * 1024;
const FINAL_VERIFY_MAX_ROWS: usize = 512;
const FINAL_VERIFY_TARGET_BYTES: usize = 128 * 1024 * 1024;
// Four SHA-256 `<oid>\n` requests are 260 bytes, below POSIX `PIPE_BUF`.
// Keeping each flush below that floor lets the caller enqueue a small request
// window before draining responses without depending on a platform's larger
// pipe capacity or deadlocking behind a large blob response.
const CAT_FILE_REQUESTS_PER_BATCH: usize = 4;
const TEXT_PLUGIN_KEY: &str = "plugin_text";
const CSV_PLUGIN_KEY: &str = "plugin_csv";
const MARKDOWN_PLUGIN_KEY: &str = "plugin_markdown";
const EXCALIDRAW_PLUGIN_KEY: &str = "plugin_excalidraw";
const GIT_REPLAY_MARKER_KEY: &str = "git_replay_marker_v1";

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
    fn new_is_regular_file(&self) -> bool {
        mode_is_regular_file(&self.new_mode)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReplayCommit {
    sha: String,
    first_parent: Option<String>,
}

/// `lix_file.path` is literal UTF-8. Reject Git's non-UTF-8 pathnames instead
/// of inventing an encoded path with different identity.
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
        std::str::from_utf8(token).map_err(|_| {
            CliError::msg(
                "unsupported Git path: lix_file paths must be valid UTF-8 and are stored literally",
            )
        })?;
        Ok(Self(token.to_vec()))
    }

    fn relative_lix_path(&self) -> String {
        std::str::from_utf8(&self.0)
            .expect("GitPath construction validates UTF-8")
            .to_string()
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

#[derive(Debug, Default)]
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
    size_bytes: Option<usize>,
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
    checkpoint_ms: f64,
    total_ms: f64,
}

#[derive(Debug, Default, Serialize)]
struct ReplayPluginCounters {
    source_read_calls: u64,
    source_bytes_read: u64,
    packet_pages: u64,
    packet_records: u64,
    attachment_reads: u64,
    attachment_bytes_read: u64,
    component_import_calls: u64,
    component_boundary_bytes: u64,
    guest_linear_memory_high_water_bytes: u64,
    host_full_diff_bytes_compared: u64,
    host_content_classification_bytes: u64,
    full_state_semantic_rows_materialized: u64,
    change_payload_requests: u64,
    returned_change_payloads: u64,
    durable_semantic_changes: u64,
    private_document_cache_hits: u64,
    shared_renderer_cache_hits: u64,
    full_document_reparses: u64,
    full_renderer_invocations: u64,
    filesystem_sync_full_renders: u64,
    conflict_resolution_calls: u64,
    conflict_resolution_records: u64,
    conflict_resolution_takes: u64,
}

#[derive(Debug, Serialize)]
struct ReplayCommitProfile {
    commit_sha: String,
    changed_paths: usize,
    inserts: usize,
    updates: usize,
    deletes: usize,
    logical_statement_count: usize,
    physical_execution_groups: usize,
    sql_chars: usize,
    blob_bytes: usize,
    marker_only: bool,
    read_diff_ms: f64,
    read_blobs_ms: f64,
    prepare_ms: f64,
    build_sql_ms: f64,
    execute_ms: f64,
    checkpoint_ms: Option<f64>,
    plugin_counters: ReplayPluginCounters,
    total_ms: f64,
}

#[derive(Debug, Serialize)]
struct ReplayProfileReport {
    repo_path: String,
    output_path: String,
    storage: String,
    plugins: String,
    branch: String,
    from_commit: Option<String>,
    num_commits_requested: Option<u32>,
    checkpoint_every: Option<u32>,
    history_scope: String,
    scoped_paths: usize,
    git_lfs_objects_materialized: u64,
    git_lfs_bytes_materialized: u64,
    plugin_install_ms: f64,
    baseline_seed_parent: Option<String>,
    baseline_seed_ms: f64,
    baseline_seed_files: usize,
    baseline_seed_batches: usize,
    final_tree_verify_ms: f64,
    replay_elapsed_ms: f64,
    storage_flush_ms: f64,
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
    let output_path = absolutize_from_cwd(&args.output_path)?;
    validate_safe_storage_output_path(&repo_path, &output_path)?;
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
    prepare_storage_output_path(&output_path, args.force)?;
    if let Some(path) = &profile_json_path {
        prepare_regular_output_path(path, args.force)?;
    }

    let storage_backend = args.storage;
    match storage_backend {
        GitReplayStorage::Rocksdb => {
            let storage = RocksDB::open(&output_path).map_err(|error| {
                CliError::msg(format!(
                    "failed to open RocksDB at {}: {error}",
                    output_path.display()
                ))
            })?;
            run_with_storage(
                args,
                repo_path,
                output_path,
                profile_json_path,
                commits,
                storage,
                |storage| {
                    storage
                        .flush()
                        .map_err(|error| CliError::msg(format!("failed to flush RocksDB: {error}")))
                },
            )
        }
        GitReplayStorage::Slatedb => {
            let storage = SlateDB::open(&output_path).map_err(|error| {
                CliError::msg(format!(
                    "failed to open SlateDB at {}: {error}",
                    output_path.display()
                ))
            })?;
            run_with_storage(
                args,
                repo_path,
                output_path,
                profile_json_path,
                commits,
                storage,
                |storage| {
                    db::block_on(storage.flush())
                        .map_err(|error| CliError::msg(format!("failed to flush SlateDB: {error}")))
                },
            )
        }
    }
}

fn run_with_storage<StorageImpl>(
    args: ExpGitReplayArgs,
    repo_path: PathBuf,
    output_path: PathBuf,
    profile_json_path: Option<PathBuf>,
    commits: Vec<ReplayCommit>,
    storage: StorageImpl,
    flush_storage: impl FnOnce(&StorageImpl) -> Result<(), CliError>,
) -> Result<(), CliError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let baseline_seed_parent = commits
        .first()
        .and_then(|commit| commit.first_parent.clone());
    let mut replay_scope = collect_replay_scope(&repo_path, &commits)?;
    if args.parent_tree == GitReplayParentTree::Full
        && let Some(parent) = baseline_seed_parent.as_deref()
    {
        extend_replay_scope_with_tree(&repo_path, parent, &mut replay_scope)?;
    }
    let history_scope = if commits
        .first()
        .is_some_and(|commit| commit.first_parent.is_none())
    {
        "complete"
    } else if args.parent_tree == GitReplayParentTree::Full {
        "full-parent-window"
    } else {
        "window"
    };
    let lix = db::block_on(open_lix().with_storage(storage.clone()))
        .map_err(|error| CliError::msg(format!("failed to open replay Lix: {error}")))?;
    db::block_on(lix.execute(
        "INSERT INTO lix_key_value (key, value, lixcol_global, lixcol_untracked) \
         VALUES ('lix_deterministic_mode', '{\"enabled\":true}'::jsonb, true, true)",
        &[],
    ))
    .map_err(|error| CliError::msg(format!("failed to enable deterministic mode: {error}")))?;

    let plugin_install_started = Instant::now();
    if args.plugins == GitReplayPlugins::All {
        install_embedded_replay_plugins(&lix)?;
    }
    let plugin_install_ms = duration_to_ms(plugin_install_started.elapsed());

    let mut state = ReplayState::default();
    let mut baseline_seed_ms = 0.0;
    let mut baseline_seed_files = 0usize;
    let mut baseline_seed_batches = 0usize;
    let seeded_blob_reader = if let Some(parent) = baseline_seed_parent.as_deref() {
        let seed_started = Instant::now();
        let mut blob_reader = GitBlobReader::spawn(&repo_path)?;
        let seeded = seed_parent_tree(
            &repo_path,
            parent,
            &replay_scope,
            &mut blob_reader,
            &mut state,
            &lix,
        )?;
        baseline_seed_files = seeded.files;
        baseline_seed_batches = seeded.batches;
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
    let mut phase_totals = ReplayProfilePhaseTotals::default();
    let mut commit_profiles = Vec::<ReplayCommitProfile>::with_capacity(commits.len());
    let checkpoint_every = args.checkpoint_every.map(|interval| interval as usize);

    println!(
        "[git-replay] replaying {} commits over {} scoped paths from {} into {}",
        commits.len(),
        replay_scope.len(),
        repo_path.display(),
        args.storage.as_str()
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

        let logical_statement_count = statements.len();
        let sql_chars = total_statement_sql_chars(&statements);
        let blob_bytes = prepared_blob_bytes(&prepared);
        let inserts = prepared.inserts.len();
        let updates = prepared.updates.len();
        let deletes = prepared.deletes.len();
        if prepared.deletes.is_empty() && prepared.inserts.is_empty() && prepared.updates.is_empty()
        {
            marker_only += 1;
        }
        let execute_started = Instant::now();
        let physical_execution_groups =
            execute_statements_as_transaction(&lix, &statements, commit_sha)?;
        let execute_ms = duration_to_ms(execute_started.elapsed());
        let plugin_counters = ReplayPluginCounters::default();
        phase_totals.execute_ms += execute_ms;
        applied += 1;
        let checkpoint_ms = if checkpoint_every.is_some_and(|interval| (index + 1) % interval == 0)
        {
            let checkpoint_started = Instant::now();
            db::block_on(lix.create_checkpoint()).map_err(|error| {
                CliError::msg(format!(
                    "failed to checkpoint after replay commit {commit_sha}: {error}"
                ))
            })?;
            let elapsed_ms = duration_to_ms(checkpoint_started.elapsed());
            phase_totals.checkpoint_ms += elapsed_ms;
            Some(elapsed_ms)
        } else {
            None
        };

        let total_ms = duration_to_ms(commit_started.elapsed());
        phase_totals.total_ms += total_ms;
        commit_profiles.push(ReplayCommitProfile {
            commit_sha: commit_sha.clone(),
            changed_paths: changes.len(),
            inserts,
            updates,
            deletes,
            logical_statement_count,
            physical_execution_groups,
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
            checkpoint_ms,
            plugin_counters,
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
    let verify_started = Instant::now();
    verify_final_git_tree(
        &repo_path,
        &commits
            .last()
            .expect("non-empty replay commits were validated above")
            .sha,
        &replay_scope,
        &mut blob_reader,
        &lix,
    )?;
    let final_tree_verify_ms = duration_to_ms(verify_started.elapsed());
    let replay_cleanup_started = Instant::now();
    let git_lfs_objects_materialized = blob_reader.git_lfs_objects_materialized;
    let git_lfs_bytes_materialized = blob_reader.git_lfs_bytes_materialized;
    blob_reader.finish()?;
    let replay_elapsed_ms =
        duration_to_ms(replay_before_final_verification + replay_cleanup_started.elapsed());
    db::block_on(lix.close())
        .map_err(|error| CliError::msg(format!("failed to close replay Lix: {error}")))?;
    let flush_started = Instant::now();
    flush_storage(&storage)?;
    let storage_flush_ms = duration_to_ms(flush_started.elapsed());

    println!("[git-replay] done");
    println!("[git-replay] ref: {}", args.branch);
    println!(
        "[git-replay] output {}: {}",
        args.storage.as_str(),
        output_path.display()
    );
    println!("[git-replay] commits replayed: {}", commits.len());
    println!("[git-replay] commits applied: {applied}");
    println!("[git-replay] commits with marker only: {marker_only}");
    println!("[git-replay] changed paths total: {changed_paths}");
    println!(
        "[git-replay] history scope: {history_scope} ({} paths)",
        replay_scope.len()
    );
    println!("[git-replay] plugins: {}", args.plugins.as_str());
    println!(
        "[git-replay] materialized {git_lfs_objects_materialized} unique Git LFS objects ({git_lfs_bytes_materialized} bytes)"
    );
    if args.plugins == GitReplayPlugins::All {
        println!(
            "[git-replay] text/CSV/Markdown/Excalidraw plugin setup excluded from replay timing: {plugin_install_ms:.3}ms"
        );
    }
    if let Some(parent) = &baseline_seed_parent {
        println!(
            "[git-replay] scoped parent bootstrap excluded from replay timing: {baseline_seed_ms:.3}ms ({baseline_seed_files} files in {baseline_seed_batches} transactions from {parent})"
        );
    }
    println!("[git-replay] replay elapsed: {replay_elapsed_ms:.3}ms");
    println!("[git-replay] scoped final Git tree manifest verified in {final_tree_verify_ms:.3}ms");
    if let Some(profile_path) = &profile_json_path {
        write_profile_report(
            profile_path,
            ReplayProfileReport {
                repo_path: repo_path.display().to_string(),
                output_path: output_path.display().to_string(),
                storage: args.storage.as_str().to_string(),
                plugins: args.plugins.as_str().to_string(),
                branch: args.branch.clone(),
                from_commit: args.from_commit.clone(),
                num_commits_requested: args.num_commits,
                checkpoint_every: args.checkpoint_every,
                history_scope: history_scope.to_string(),
                scoped_paths: replay_scope.len(),
                git_lfs_objects_materialized,
                git_lfs_bytes_materialized,
                plugin_install_ms,
                baseline_seed_parent,
                baseline_seed_ms,
                baseline_seed_files,
                baseline_seed_batches,
                final_tree_verify_ms,
                replay_elapsed_ms,
                storage_flush_ms,
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

fn execute_statements_as_transaction<StorageImpl>(
    lix: &Lix<StorageImpl>,
    statements: &[SqlStatement],
    commit_sha: &str,
) -> Result<usize, CliError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    // Keep every statement in one explicit transaction. Prepared bulk writes
    // are an engine implementation detail, so the CLI uses the same public SQL
    // transaction API as other applications.
    let mut transaction = db::block_on(lix.begin_transaction()).map_err(|error| {
        CliError::msg(format!(
            "failed to begin replay transaction {commit_sha}: {error}"
        ))
    })?;
    let mut index = 0;
    let mut physical_execution_groups = 0usize;
    while index < statements.len() {
        let statement = &statements[index];
        let mut end = index + 1;
        while end < statements.len()
            && statements[end].sql == statement.sql
            && statements[end].params.len() == statement.params.len()
        {
            end += 1;
        }
        for statement in &statements[index..end] {
            db::block_on(transaction.execute(&statement.sql, &statement.params)).map_err(
                |error| {
                    CliError::msg(format!(
                        "failed at commit {commit_sha} while executing replay statement: {error}"
                    ))
                },
            )?;
            physical_execution_groups += 1;
        }
        index = end;
    }
    db::block_on(transaction.commit()).map_err(|error| {
        CliError::msg(format!(
            "failed to commit replay transaction {commit_sha}: {error}"
        ))
    })?;

    Ok(physical_execution_groups)
}

fn git_replay_marker_statement(commit: &ReplayCommit) -> SqlStatement {
    SqlStatement {
        sql: "INSERT INTO lix_key_value (key, value) VALUES ($1, $2) \
              ON CONFLICT(key) DO UPDATE SET value = excluded.value"
            .to_string(),
        params: vec![
            Value::Text(GIT_REPLAY_MARKER_KEY.to_string()),
            Value::Jsonb(
                json!({
                    "sha": commit.sha,
                    "first_parent": commit.first_parent,
                })
                .into(),
            ),
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

#[derive(Debug, Default, PartialEq, Eq)]
struct SeedResult {
    files: usize,
    batches: usize,
}

fn collect_replay_scope(
    repo_path: &Path,
    commits: &[ReplayCommit],
) -> Result<HashSet<GitPath>, CliError> {
    let mut reader = GitDiffTreeReader::spawn(repo_path, commits)?;
    let mut scope = HashSet::new();
    for commit in commits {
        for change in reader.read_commit(&commit.sha)? {
            if let Some(path) = change.old_path {
                scope.insert(path);
            }
            if let Some(path) = change.new_path {
                scope.insert(path);
            }
        }
    }
    reader.finish()?;
    Ok(scope)
}

fn extend_replay_scope_with_tree(
    repo_path: &Path,
    commit: &str,
    scope: &mut HashSet<GitPath>,
) -> Result<(), CliError> {
    for change in read_tree_snapshot_changes(repo_path, commit)? {
        if let Some(path) = change.new_path {
            scope.insert(path);
        }
    }
    Ok(())
}

fn seed_parent_tree<StorageImpl>(
    repo_path: &Path,
    parent_commit: &str,
    replay_scope: &HashSet<GitPath>,
    blob_reader: &mut GitBlobReader,
    state: &mut ReplayState,
    lix: &Lix<StorageImpl>,
) -> Result<SeedResult, CliError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let changes = read_scoped_tree_snapshot_changes(repo_path, parent_commit, replay_scope)?;
    let mut pending = PreparedBatch::default();
    let mut result = SeedResult::default();
    for change_batch in changes.chunks(TREE_BLOB_READ_BATCH_ROWS) {
        let wanted_blob_ids = collect_wanted_blob_ids(change_batch);
        let blob_by_oid = blob_reader.read_blobs(&wanted_blob_ids)?;
        let prepared = prepare_commit_changes(state, change_batch, &blob_by_oid)?;
        let pending_bytes = prepared_blob_bytes(&pending);
        let prepared_bytes = prepared_blob_bytes(&prepared);
        if pending_bytes > 0
            && pending_bytes.saturating_add(prepared_bytes) > TREE_TRANSACTION_TARGET_BYTES
        {
            let inserted = flush_seed_batch(&mut pending, lix, parent_commit)?;
            result.files += inserted;
            result.batches += usize::from(inserted > 0);
        }
        append_prepared_batch(&mut pending, prepared);
        if prepared_blob_bytes(&pending) >= TREE_TRANSACTION_TARGET_BYTES {
            let inserted = flush_seed_batch(&mut pending, lix, parent_commit)?;
            result.files += inserted;
            result.batches += usize::from(inserted > 0);
        }
    }
    let inserted = flush_seed_batch(&mut pending, lix, parent_commit)?;
    result.files += inserted;
    result.batches += usize::from(inserted > 0);
    Ok(result)
}

fn append_prepared_batch(target: &mut PreparedBatch, mut source: PreparedBatch) {
    target.deletes.append(&mut source.deletes);
    target.inserts.append(&mut source.inserts);
    target.updates.append(&mut source.updates);
}

fn flush_seed_batch<StorageImpl>(
    pending: &mut PreparedBatch,
    lix: &Lix<StorageImpl>,
    parent_commit: &str,
) -> Result<usize, CliError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    if pending.deletes.is_empty() && pending.inserts.is_empty() && pending.updates.is_empty() {
        return Ok(0);
    }
    let prepared = std::mem::take(pending);
    let statements = build_replay_commit_statements(&prepared, DEFAULT_INSERT_BATCH_ROWS);
    execute_statements_as_transaction(lix, &statements, parent_commit)?;
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

fn read_scoped_tree_snapshot_changes(
    repo_path: &Path,
    commit_sha: &str,
    replay_scope: &HashSet<GitPath>,
) -> Result<Vec<Change>, CliError> {
    let changes = read_tree_snapshot_changes(repo_path, commit_sha)?
        .into_iter()
        .filter(|change| {
            change
                .new_path
                .as_ref()
                .is_some_and(|path| replay_scope.contains(path))
        })
        .collect::<Vec<_>>();

    for change in &changes {
        reject_unsupported_git_mode(&change.new_mode, change.new_path.as_ref())?;
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
    git_lfs_objects_path: Option<PathBuf>,
    git_lfs_oids_materialized: HashSet<String>,
    git_lfs_objects_materialized: u64,
    git_lfs_bytes_materialized: u64,
    child: Option<Child>,
    stdin: Option<ChildStdin>,
    stdout: BufReader<ChildStdout>,
    stderr_reader: Option<JoinHandle<std::io::Result<Vec<u8>>>>,
}

#[derive(Debug, PartialEq, Eq)]
struct GitLfsPointer {
    oid: String,
    size: u64,
}

fn git_lfs_objects_path(repo_path: &Path) -> Result<PathBuf, CliError> {
    let common_dir = run_git_text(
        repo_path,
        &["rev-parse".to_string(), "--git-common-dir".to_string()],
        None,
    )?;
    let common_dir = PathBuf::from(common_dir.trim());
    let common_dir = if common_dir.is_absolute() {
        common_dir
    } else {
        repo_path.join(common_dir)
    };
    Ok(common_dir.join("lfs").join("objects"))
}

fn parse_git_lfs_pointer(bytes: &[u8]) -> Result<Option<GitLfsPointer>, CliError> {
    const VERSION: &str = "version https://git-lfs.github.com/spec/v1";
    let Ok(text) = std::str::from_utf8(bytes) else {
        return Ok(None);
    };
    let mut lines = text.lines();
    if lines.next() != Some(VERSION) {
        return Ok(None);
    }
    let mut oid = None;
    let mut size = None;
    for line in lines {
        if let Some(value) = line.strip_prefix("oid sha256:") {
            if oid.is_some()
                || value.len() != 64
                || !value.bytes().all(|byte| byte.is_ascii_hexdigit())
            {
                return Err(CliError::msg("malformed Git LFS SHA-256 pointer"));
            }
            oid = Some(value.to_ascii_lowercase());
        } else if let Some(value) = line.strip_prefix("size ") {
            if size.is_some() {
                return Err(CliError::msg("duplicate Git LFS pointer size"));
            }
            size = Some(
                value
                    .parse::<u64>()
                    .map_err(|_| CliError::msg("malformed Git LFS pointer size"))?,
            );
        }
    }
    Ok(Some(GitLfsPointer {
        oid: oid.ok_or_else(|| CliError::msg("Git LFS pointer is missing its SHA-256 oid"))?,
        size: size.ok_or_else(|| CliError::msg("Git LFS pointer is missing its size"))?,
    }))
}

impl GitBlobReader {
    fn spawn(repo_path: &Path) -> Result<Self, CliError> {
        let git_lfs_objects_path = Some(git_lfs_objects_path(repo_path)?);
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
            git_lfs_objects_path,
            git_lfs_oids_materialized: HashSet::new(),
            git_lfs_objects_materialized: 0,
            git_lfs_bytes_materialized: 0,
            child: Some(child),
            stdin: Some(stdin),
            stdout: BufReader::new(stdout),
            stderr_reader: Some(stderr_reader),
        })
    }

    fn read_blobs(&mut self, blob_ids: &[String]) -> Result<HashMap<String, Vec<u8>>, CliError> {
        for requested_oid in blob_ids {
            if !is_full_git_oid(requested_oid.as_bytes()) {
                return Err(CliError::msg(format!(
                    "refusing malformed git blob object id {requested_oid}"
                )));
            }
        }

        let mut blobs = HashMap::with_capacity(blob_ids.len());
        for request_batch in blob_ids.chunks(CAT_FILE_REQUESTS_PER_BATCH) {
            self.request_blob_batch(request_batch)?;
            for requested_oid in request_batch {
                let data = self.read_blob_response(requested_oid)?;
                blobs.insert(requested_oid.clone(), data);
            }
        }
        Ok(blobs)
    }

    fn request_blob_batch(&mut self, blob_ids: &[String]) -> Result<(), CliError> {
        let stdin = self
            .stdin
            .as_mut()
            .ok_or_else(|| CliError::msg("persistent git cat-file is already closed"))?;
        let mut request = Vec::with_capacity(blob_ids.len() * 65);
        for requested_oid in blob_ids {
            request.extend_from_slice(requested_oid.as_bytes());
            request.push(b'\n');
        }
        stdin
            .write_all(&request)
            .and_then(|()| stdin.flush())
            .map_err(|source| CliError::io("failed to flush git cat-file requests", source))
    }

    fn read_blob_response(&mut self, requested_oid: &str) -> Result<Vec<u8>, CliError> {
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
        self.stdout
            .read_exact(&mut separator)
            .map_err(|source| CliError::io("git cat-file output truncated after blob", source))?;
        if separator != *b"\n" {
            return Err(CliError::msg(format!(
                "malformed git cat-file output: blob {requested_oid} lacks trailing newline"
            )));
        }
        let Some(objects_path) = &self.git_lfs_objects_path else {
            return Ok(data);
        };
        let Some(pointer) = parse_git_lfs_pointer(&data)? else {
            return Ok(data);
        };
        let object_path = objects_path
            .join(&pointer.oid[..2])
            .join(&pointer.oid[2..4])
            .join(&pointer.oid);
        let materialized = fs::read(&object_path).map_err(|source| {
            CliError::msg(format!(
                "Git LFS object {} is unavailable ({source}); fetch every historical object before replay with `git -C {} lfs fetch --all`",
                pointer.oid,
                self.repo_path.display()
            ))
        })?;
        if materialized.len() as u64 != pointer.size {
            return Err(CliError::msg(format!(
                "Git LFS object {} has {} bytes, pointer declares {}",
                pointer.oid,
                materialized.len(),
                pointer.size
            )));
        }
        let actual_oid = sha256_hex(&materialized);
        if actual_oid != pointer.oid {
            return Err(CliError::msg(format!(
                "Git LFS object {} failed SHA-256 verification (read {actual_oid})",
                pointer.oid
            )));
        }
        if self.git_lfs_oids_materialized.insert(pointer.oid) {
            self.git_lfs_objects_materialized = self.git_lfs_objects_materialized.saturating_add(1);
            self.git_lfs_bytes_materialized =
                self.git_lfs_bytes_materialized.saturating_add(pointer.size);
        }
        Ok(materialized)
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
        if change.new_path.is_none() || !change.new_is_regular_file() {
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

        reject_unsupported_git_mode(&change.old_mode, change.old_path.as_ref())?;
        reject_unsupported_git_mode(&change.new_mode, change.new_path.as_ref())?;

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

        if !mode_is_regular_file(&change.new_mode) {
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
        let data = Some(
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
        );

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
    if change.old_path.is_none() || !mode_is_regular_file(&change.old_mode) {
        return false;
    }

    match status {
        // Plugin-owned state is path-bound. Retire the old descriptor and
        // create a new one so the selected plugin initializes the new path in
        // the same atomic replay revision.
        'D' | 'R' => true,
        'A' | 'C' => false,
        _ => !mode_is_regular_file(&change.new_mode),
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
    _max_insert_rows: usize,
) -> Vec<SqlStatement> {
    if batch.deletes.is_empty() && batch.inserts.is_empty() && batch.updates.is_empty() {
        return Vec::new();
    }

    let mut statements = Vec::<SqlStatement>::new();

    for delete_chunk in batch.deletes.chunks(500) {
        if delete_chunk.is_empty() {
            continue;
        }

        let placeholders = (1..=delete_chunk.len())
            .map(|index| format!("${index}"))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("DELETE FROM lix_file WHERE id IN ({placeholders})");
        let params = delete_chunk
            .iter()
            .cloned()
            .map(Value::Text)
            .collect::<Vec<_>>();
        statements.push(SqlStatement { sql, params });
    }

    for row in &batch.inserts {
        statements.push(SqlStatement {
            sql:
                "INSERT INTO lix_file (id, path, content, lixcol_metadata) VALUES ($1, $2, $3, $4)"
                    .to_string(),
            params: vec![
                Value::Text(row.id.clone()),
                Value::Text(row.path.clone()),
                value_from_optional_blob(row.data.as_ref()),
                git_file_metadata_value(row),
            ],
        });
    }

    for row in &batch.updates {
        statements.push(SqlStatement {
            sql:
                "INSERT INTO lix_file (id, path, content, lixcol_metadata) VALUES ($1, $2, $3, $4) \
                 ON CONFLICT(id) DO UPDATE SET content = excluded.content, \
                 lixcol_metadata = excluded.lixcol_metadata"
                    .to_string(),
            params: vec![
                Value::Text(row.id.clone()),
                Value::Text(row.path.clone()),
                value_from_optional_blob(row.data.as_ref()),
                git_file_metadata_value(row),
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
    Value::Jsonb(
        json!({
            "git_mode": row.git_mode,
            "git_oid": row.git_oid,
        })
        .into(),
    )
}

fn verify_final_git_tree<StorageImpl>(
    repo_path: &Path,
    commit_sha: &str,
    replay_scope: &HashSet<GitPath>,
    blob_reader: &mut GitBlobReader,
    lix: &Lix<StorageImpl>,
) -> Result<(), CliError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let tree_changes = read_scoped_tree_snapshot_changes(repo_path, commit_sha, replay_scope)?;
    let mut expected_by_path = HashMap::<String, ExpectedFile>::with_capacity(tree_changes.len());
    for change_batch in tree_changes.chunks(TREE_BLOB_READ_BATCH_ROWS) {
        let blob_by_oid = blob_reader.read_blobs(&collect_wanted_blob_ids(change_batch))?;
        for change in change_batch {
            let path = change
                .new_path
                .as_ref()
                .ok_or_else(|| CliError::msg("Git tree snapshot entry has no path"))?
                .lix_path();
            let data = Some(blob_by_oid.get(&change.new_oid).ok_or_else(|| {
                CliError::msg(format!(
                    "Git tree blob {} was not returned for {path}",
                    change.new_oid
                ))
            })?);
            if expected_by_path.contains_key(&path) {
                return Err(CliError::msg(format!(
                    "Git tree snapshot contains duplicate path {path}"
                )));
            }
            expected_by_path.insert(
                path.clone(),
                ExpectedFile {
                    path,
                    sha256: data.map(|bytes| sha256_hex(bytes)),
                    size_bytes: data.map(|bytes| bytes.len()),
                    git_mode: change.new_mode.clone(),
                    git_oid: change.new_oid.clone(),
                },
            );
        }
    }

    let count = db::block_on(lix.execute(
        "SELECT COUNT(*) FROM lix_file \
         WHERE path NOT LIKE '/.lix/plugins/%'",
        &[],
    ))
    .map_err(|error| CliError::msg(format!("failed to count Lix final tree: {error}")))?;
    let lix_count = match count.rows().first().and_then(|row| row.get_index(0)) {
        Some(Value::Integer(value)) => usize::try_from(*value)
            .map_err(|_| CliError::msg("Lix final tree count does not fit usize"))?,
        _ => return Err(CliError::msg("Lix final tree count is not an integer")),
    };
    if lix_count != expected_by_path.len() {
        return Err(CliError::msg(format!(
            "final tree mismatch at {commit_sha}: row count differs (lix={}, git={})",
            lix_count,
            expected_by_path.len()
        )));
    }

    let mut expected = expected_by_path.values().collect::<Vec<_>>();
    expected.sort_unstable_by(|left, right| left.path.cmp(&right.path));
    let mut offset = 0usize;
    while offset < expected.len() {
        let mut end = offset;
        let mut expected_bytes = 0usize;
        while end < expected.len() && end - offset < FINAL_VERIFY_MAX_ROWS {
            let row_bytes = expected[end].size_bytes.unwrap_or_default();
            if end > offset && expected_bytes.saturating_add(row_bytes) > FINAL_VERIFY_TARGET_BYTES
            {
                break;
            }
            expected_bytes = expected_bytes.saturating_add(row_bytes);
            end += 1;
        }
        let batch = &expected[offset..end];
        let placeholders = (1..=batch.len())
            .map(|index| format!("${index}"))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT path, content, lixcol_metadata FROM lix_file WHERE path IN ({placeholders})"
        );
        let params = batch
            .iter()
            .map(|expected| Value::Text(expected.path.clone()))
            .collect::<Vec<_>>();
        let result = db::block_on(lix.execute(&sql, &params))
            .map_err(|error| CliError::msg(format!("failed to query Lix final tree: {error}")))?;
        if result.rows().len() != batch.len() {
            return Err(CliError::msg(format!(
                "final tree mismatch at {commit_sha}: expected {} bounded rows, got {}",
                batch.len(),
                result.rows().len()
            )));
        }
        let mut seen = HashSet::with_capacity(batch.len());
        for (index, row) in result.rows().iter().enumerate() {
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
            verify_file_manifest_entry(&path, data, &metadata, expected, commit_sha)?;
            seen.insert(path);
        }
        if batch.iter().any(|expected| !seen.contains(&expected.path)) {
            return Err(CliError::msg(format!(
                "final tree mismatch at {commit_sha}: Lix is missing a bounded Git path"
            )));
        }
        offset = end;
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
    if expected.size_bytes != data.map(<[u8]>::len) {
        return Err(CliError::msg(format!(
            "state mismatch at {commit_sha}: byte length differs for path {path}"
        )));
    }
    let hash = data.map(sha256_hex);
    if expected.sha256 != hash {
        return Err(CliError::msg(format!(
            "state mismatch at {commit_sha}: hash differs for path {path}"
        )));
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

fn value_to_json(value: &Value, context: &str) -> Result<serde_json::Value, CliError> {
    match value {
        Value::Jsonb(value) => Ok(value.to_value()),
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

fn normalize_status(value: char) -> char {
    value.to_ascii_uppercase()
}

fn stable_file_id(path: &GitPath) -> String {
    let lix_path = path.lix_path();
    uuid::Uuid::new_v5(
        &uuid::Uuid::NAMESPACE_URL,
        format!("https://lix.dev/git-replay/file/v1{lix_path}").as_bytes(),
    )
    .to_string()
}

fn reject_unsupported_git_mode(mode: &str, path: Option<&GitPath>) -> Result<(), CliError> {
    if mode == "000000" || mode_is_regular_file(mode) {
        return Ok(());
    }
    let kind = match mode {
        "120000" => "symbolic link",
        "160000" => "gitlink/submodule",
        _ => "non-regular entry",
    };
    Err(CliError::msg(format!(
        "unsupported Git {kind} mode {mode} at {}; lix_file stores regular file contents only",
        path.map(GitPath::lix_path)
            .unwrap_or_else(|| "<missing path>".to_string())
    )))
}

fn mode_is_regular_file(mode: &str) -> bool {
    matches!(mode, "100644" | "100755")
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

fn install_embedded_replay_plugins<StorageImpl>(lix: &Lix<StorageImpl>) -> Result<(), CliError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let plugins = [
        (
            TEXT_PLUGIN_KEY,
            build_embedded_plugin_archive(
                include_str!("../../../../../plugins/text/manifest.json"),
                &[(
                    "schema/text_line.json",
                    include_bytes!("../../../../../plugins/text/schema/text_line.json"),
                )],
                Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_TEXT_plugin_text")),
            )?,
        ),
        (
            CSV_PLUGIN_KEY,
            build_embedded_plugin_archive(
                include_str!("../../../../../plugins/csv/manifest.json"),
                &[
                    (
                        "schema/csv_table.json",
                        include_bytes!("../../../../../plugins/csv/schema/csv_table.json"),
                    ),
                    (
                        "schema/csv_row.json",
                        include_bytes!("../../../../../plugins/csv/schema/csv_row.json"),
                    ),
                ],
                Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_CSV_plugin_csv")),
            )?,
        ),
        (
            MARKDOWN_PLUGIN_KEY,
            build_embedded_plugin_archive(
                include_str!("../../../../../plugins/markdown/manifest.json"),
                &[(
                    "schema/markdown_node.json",
                    include_bytes!("../../../../../plugins/markdown/schema/markdown_node.json"),
                )],
                Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_MARKDOWN_plugin_markdown")),
            )?,
        ),
        (
            EXCALIDRAW_PLUGIN_KEY,
            build_embedded_plugin_archive(
                include_str!("../../../../../plugins/excalidraw/manifest.json"),
                &[
                    (
                        "schema/excalidraw_scene.json",
                        include_bytes!(
                            "../../../../../plugins/excalidraw/schema/excalidraw_scene.json"
                        ),
                    ),
                    (
                        "schema/excalidraw_element.json",
                        include_bytes!(
                            "../../../../../plugins/excalidraw/schema/excalidraw_element.json"
                        ),
                    ),
                    (
                        "schema/excalidraw_file.json",
                        include_bytes!(
                            "../../../../../plugins/excalidraw/schema/excalidraw_file.json"
                        ),
                    ),
                ],
                Path::new(env!(
                    "CARGO_CDYLIB_FILE_PLUGIN_EXCALIDRAW_plugin_excalidraw"
                )),
            )?,
        ),
    ];
    for (key, archive) in plugins {
        db::block_on(lix.execute(
            "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
            &[
                Value::Text(format!("/.lix/plugins/{key}.lixplugin")),
                Value::Blob(archive.into()),
            ],
        ))
        .map_err(|error| {
            CliError::msg(format!("failed to install embedded {key} plugin: {error}"))
        })?;
    }
    Ok(())
}

fn build_embedded_plugin_archive(
    manifest: &str,
    schemas: &[(&str, &[u8])],
    wasm_path: &Path,
) -> Result<Vec<u8>, CliError> {
    let wasm = fs::read(wasm_path).map_err(|source| {
        CliError::msg(format!(
            "failed to read bindep-built replay plugin at {}: {source}",
            wasm_path.display()
        ))
    })?;
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options = SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (path, bytes) in std::iter::once(("manifest.json", manifest.as_bytes()))
        .chain(schemas.iter().copied())
        .chain(std::iter::once(("plugin.wasm", wasm.as_slice())))
    {
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

fn prepare_storage_output_path(path: &Path, force: bool) -> Result<(), CliError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|source| CliError::io("failed to create storage output parent", source))?;
    }
    if !path.exists() {
        return Ok(());
    }
    if !path.is_dir() {
        return Err(CliError::msg(format!(
            "storage output path points to a file, expected a directory: {}",
            path.display()
        )));
    }
    if !force {
        return Err(CliError::msg(format!(
            "storage output directory already exists: {} (pass --force to replace it)",
            path.display()
        )));
    }
    fs::remove_dir_all(path).map_err(|source| {
        CliError::io("failed to remove existing storage output directory", source)
    })
}

fn validate_safe_storage_output_path(repo_path: &Path, output_path: &Path) -> Result<(), CliError> {
    if output_path
        .components()
        .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        return Err(CliError::msg(format!(
            "storage output path must not contain '.' or '..' components: {}",
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
            "refusing broad storage output directory: {}",
            output.display()
        )));
    }
    if paths_overlap(&output, &repo) || paths_overlap(&output, &cwd) {
        return Err(CliError::msg(format!(
            "storage output directory must not overlap the repository or current directory: {}",
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
        .map_err(|source| CliError::io("failed to canonicalize storage output path", source))?;
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
    fn git_lfs_pointer_parser_rejects_missing_and_malformed_fields() {
        assert_eq!(
            parse_git_lfs_pointer(b"ordinary bytes").expect("ordinary blob should parse"),
            None
        );
        assert!(
            parse_git_lfs_pointer(
                b"version https://git-lfs.github.com/spec/v1\noid sha256:not-a-hash\nsize 5\n"
            )
            .is_err()
        );
        assert!(
            parse_git_lfs_pointer(
                b"version https://git-lfs.github.com/spec/v1\noid sha256:\
                  1111111111111111111111111111111111111111111111111111111111111111\n"
            )
            .is_err()
        );
    }

    #[test]
    fn ordinary_multi_file_replay_keeps_marker_atomic() {
        let lix =
            db::block_on(open_lix().with_storage(Memory::new())).expect("memory Lix should open");
        let statements =
            vec![
            SqlStatement {
                sql: "INSERT INTO lix_file (id, path, content, lixcol_metadata) VALUES ($1, $2, $3, $4)"
                    .to_string(),
                params: vec![
                    Value::Text(stable_file_id(&git_path(b"one"))),
                    Value::Text("/one".to_string()),
                    Value::Blob(vec![1_u8, 2].into()),
                    Value::Jsonb(json!({"git_mode": "100644", "git_oid": "oid-1"}).into()),
                ],
            },
            SqlStatement {
                sql: "INSERT INTO lix_file (id, path, content, lixcol_metadata) VALUES ($1, $2, $3, $4)"
                    .to_string(),
                params: vec![
                    Value::Text(stable_file_id(&git_path(b"two"))),
                    Value::Text("/two".to_string()),
                    Value::Blob(vec![3_u8, 4].into()),
                    Value::Jsonb(json!({"git_mode": "100644", "git_oid": "oid-2"}).into()),
                ],
            },
            git_replay_marker_statement(&ReplayCommit {
                sha: "commit-1".to_string(),
                first_parent: None,
            }),
        ];
        let physical_execution_groups =
            execute_statements_as_transaction(&lix, &statements, "commit-1")
                .expect("ordinary replay transaction should commit");
        assert_eq!(physical_execution_groups, 3);
        let marker = db::block_on(lix.execute(
            "SELECT value FROM lix_key_value WHERE key = $1",
            &[Value::Text(GIT_REPLAY_MARKER_KEY.to_string())],
        ))
        .expect("marker should be visible after atomic replay");
        assert_eq!(marker.rows().len(), 1);
    }

    #[test]
    fn persistent_blob_reader_materializes_and_verifies_local_git_lfs_objects() {
        let repo = unique_temp_dir();
        fs::create_dir_all(&repo).expect("fixture repository should be created");
        git_ok(&repo, &["init", "-q", "-b", "main"]);
        let content = b"materialized LFS bytes";
        let lfs_oid = sha256_hex(content);
        let pointer = format!(
            "version https://git-lfs.github.com/spec/v1\noid sha256:{lfs_oid}\nsize {}\n",
            content.len()
        );
        fs::write(repo.join("asset.bin"), pointer).expect("pointer fixture should write");
        git_ok(&repo, &["add", "asset.bin"]);
        let index = run_git_text(&repo, &["ls-files".to_string(), "-s".to_string()], None)
            .expect("index should list pointer blob");
        let git_oid = index
            .split_ascii_whitespace()
            .nth(1)
            .expect("index should contain pointer object id");
        let object = repo
            .join(".git/lfs/objects")
            .join(&lfs_oid[..2])
            .join(&lfs_oid[2..4])
            .join(&lfs_oid);
        fs::create_dir_all(object.parent().expect("LFS object has parent"))
            .expect("LFS object directory should be created");
        fs::write(&object, content).expect("LFS object should write");

        let mut reader = GitBlobReader::spawn(&repo).expect("persistent blob reader should start");
        let blobs = reader
            .read_blobs(&[git_oid.to_string()])
            .expect("LFS pointer should materialize");
        assert_eq!(
            blobs.get(git_oid).map(Vec::as_slice),
            Some(content.as_slice())
        );
        assert_eq!(reader.git_lfs_objects_materialized, 1);
        assert_eq!(reader.git_lfs_bytes_materialized, content.len() as u64);
        reader
            .finish()
            .expect("persistent blob reader should finish");

        fs::write(&object, vec![b'x'; content.len()]).expect("corrupt LFS object should write");
        let mut reader = GitBlobReader::spawn(&repo).expect("corrupt-object reader should start");
        let error = reader
            .read_blobs(&[git_oid.to_string()])
            .expect_err("same-size corrupt LFS content must fail its SHA-256 check");
        assert!(error.to_string().contains("SHA-256 verification"));
        drop(reader);
        fs::remove_dir_all(&repo).expect("fixture repository should be removable");
    }

    #[test]
    fn replayed_lfs_history_survives_without_git_or_lfs_storage() {
        let fixture = unique_temp_dir();
        let repo = fixture.join("repo");
        let output = fixture.join("replay.rocksdb");
        fs::create_dir_all(&repo).expect("fixture repository should be created");
        git_ok(&repo, &["init", "-q", "-b", "main"]);
        git_ok(&repo, &["config", "user.email", "replay@example.test"]);
        git_ok(&repo, &["config", "user.name", "Replay Test"]);

        let versions = [
            b"materialized LFS version A".as_slice(),
            b"materialized LFS version B".as_slice(),
        ];
        let mut git_commits = Vec::new();
        for (index, content) in versions.iter().enumerate() {
            let lfs_oid = sha256_hex(content);
            let pointer = format!(
                "version https://git-lfs.github.com/spec/v1\noid sha256:{lfs_oid}\nsize {}\n",
                content.len()
            );
            fs::write(repo.join("asset.bin"), pointer).expect("pointer fixture should write");
            let object = repo
                .join(".git/lfs/objects")
                .join(&lfs_oid[..2])
                .join(&lfs_oid[2..4])
                .join(&lfs_oid);
            fs::create_dir_all(object.parent().expect("LFS object has parent"))
                .expect("LFS object directory should be created");
            fs::write(object, content).expect("LFS object should write");
            git_ok(&repo, &["add", "asset.bin"]);
            git_ok(&repo, &["commit", "-qm", &format!("LFS version {index}")]);
            git_commits.push(
                run_git_text(&repo, &["rev-parse".to_string(), "HEAD".to_string()], None)
                    .expect("commit id should resolve")
                    .trim()
                    .to_string(),
            );
        }

        run(ExpGitReplayArgs {
            repo_path: repo.clone(),
            output_path: output.clone(),
            storage: GitReplayStorage::Rocksdb,
            plugins: GitReplayPlugins::None,
            branch: "main".to_string(),
            from_commit: None,
            parent_tree: GitReplayParentTree::Window,
            num_commits: None,
            checkpoint_every: None,
            force: false,
            profile_json: None,
        })
        .expect("LFS history replay should complete");

        fs::remove_dir_all(&repo).expect("Git and LFS source should be removable");
        let storage = RocksDB::open(&output).expect("replay RocksDB should reopen without Git");
        let lix = db::block_on(open_lix().with_storage(storage))
            .expect("replay Lix should reopen without Git");
        let marker_rows = db::block_on(lix.execute(
            "SELECT value, lixcol_observed_commit_id \
             FROM lix_key_value_history() \
             WHERE key = $1 AND NOT lixcol_is_deleted",
            &[Value::Text(GIT_REPLAY_MARKER_KEY.to_string())],
        ))
        .expect("replay markers should be queryable without Git");
        let mut lix_commit_by_git_sha = HashMap::new();
        for (index, row) in marker_rows.rows().iter().enumerate() {
            let marker = value_to_json(
                row.get_index(0)
                    .unwrap_or_else(|| panic!("missing marker value at row {index}")),
                "historical replay marker",
            )
            .expect("historical replay marker should be JSON");
            let git_sha = marker
                .get("sha")
                .and_then(serde_json::Value::as_str)
                .expect("historical replay marker should contain Git SHA");
            let lix_commit = value_to_string(
                row.get_index(1)
                    .unwrap_or_else(|| panic!("missing observed commit at row {index}")),
                "historical replay commit",
            )
            .expect("historical replay commit should be text");
            lix_commit_by_git_sha.insert(git_sha.to_string(), lix_commit);
        }

        for ((git_sha, expected), version_index) in
            git_commits.iter().zip(versions.iter()).zip(0usize..)
        {
            let lix_commit = lix_commit_by_git_sha
                .get(git_sha)
                .unwrap_or_else(|| panic!("missing Lix commit for Git version {version_index}"));
            let historical = db::block_on(lix.execute(
                "SELECT content FROM lix_file_history($1) \
                 WHERE path = '/asset.bin' AND lixcol_depth = 0 AND NOT lixcol_is_deleted",
                &[Value::Text(lix_commit.clone())],
            ))
            .expect("historical LFS bytes should query without Git");
            assert_eq!(historical.rows().len(), 1);
            let actual = value_to_optional_blob(
                historical.rows()[0]
                    .get_index(0)
                    .expect("historical LFS row should contain data"),
                "historical LFS bytes",
            )
            .expect("historical LFS data should be a blob");
            assert_eq!(actual, Some(*expected));
        }
        db::block_on(lix.close()).expect("reopened Lix should close");
        fs::remove_dir_all(&fixture).expect("fixture directory should be removable");
    }

    #[test]
    fn collect_wanted_blob_ids_only_includes_regular_files() {
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
    fn full_parent_tree_extends_window_scope_with_untouched_paths() {
        let fixture = unique_temp_dir();
        let repo = fixture.join("repo");
        fs::create_dir_all(&repo).expect("fixture repository should be created");
        git_ok(&repo, &["init", "-q", "-b", "main"]);
        git_ok(&repo, &["config", "user.email", "replay@example.test"]);
        git_ok(&repo, &["config", "user.name", "Replay Test"]);
        fs::write(repo.join("renamed-before.txt"), b"before\n").expect("fixture should write");
        fs::write(repo.join("untouched.txt"), b"untouched\n").expect("fixture should write");
        git_ok(&repo, &["add", "-A"]);
        git_ok(&repo, &["commit", "-qm", "root"]);

        fs::rename(
            repo.join("renamed-before.txt"),
            repo.join("renamed-after.txt"),
        )
        .expect("fixture should rename");
        git_ok(&repo, &["add", "-A"]);
        git_ok(&repo, &["commit", "-qm", "rename"]);
        let rename_commit =
            run_git_text(&repo, &["rev-parse".to_string(), "HEAD".to_string()], None)
                .expect("rename commit should resolve");
        let commits = list_linear_commits(&repo, "main", Some(rename_commit.trim()), None)
            .expect("rename window should select");
        let mut scope = collect_replay_scope(&repo, &commits).expect("scope should collect");

        assert_eq!(
            scope,
            HashSet::from([
                git_path(b"renamed-before.txt"),
                git_path(b"renamed-after.txt"),
            ])
        );
        assert!(!scope.contains(&git_path(b"untouched.txt")));
        let parent = commits[0]
            .first_parent
            .as_deref()
            .expect("rename window should have a parent");
        extend_replay_scope_with_tree(&repo, parent, &mut scope)
            .expect("full parent tree should extend scope");
        assert!(scope.contains(&git_path(b"untouched.txt")));
        fs::remove_dir_all(&fixture).expect("fixture directory should be removable");
    }

    #[cfg(unix)]
    #[test]
    fn scoped_tree_snapshot_ignores_unrelated_symbolic_links() {
        use std::os::unix::fs::symlink;

        let fixture = unique_temp_dir();
        let repo = fixture.join("repo");
        fs::create_dir_all(&repo).expect("fixture repository should be created");
        git_ok(&repo, &["init", "-q", "-b", "main"]);
        git_ok(&repo, &["config", "user.email", "replay@example.test"]);
        git_ok(&repo, &["config", "user.name", "Replay Test"]);
        fs::write(repo.join("selected.txt"), b"selected\n").expect("fixture should write");
        symlink("selected.txt", repo.join("unrelated-link")).expect("fixture should link");
        git_ok(&repo, &["add", "-A"]);
        git_ok(&repo, &["commit", "-qm", "root"]);
        let commit = run_git_text(&repo, &["rev-parse".to_string(), "HEAD".to_string()], None)
            .expect("fixture commit should resolve");

        let window_scope = HashSet::from([git_path(b"selected.txt")]);
        let changes = read_scoped_tree_snapshot_changes(&repo, commit.trim(), &window_scope)
            .expect("an unrelated symbolic link should not fail window scope");
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].new_path, Some(git_path(b"selected.txt")));

        let full_scope = HashSet::from([git_path(b"selected.txt"), git_path(b"unrelated-link")]);
        let error = read_scoped_tree_snapshot_changes(&repo, commit.trim(), &full_scope)
            .expect_err("a symbolic link inside replay scope should be rejected");
        assert!(error.to_string().contains("symbolic link"));
        assert!(error.to_string().contains("/unrelated-link"));

        fs::remove_dir_all(&fixture).expect("fixture directory should be removable");
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
    fn prepare_commit_changes_rejects_gitlinks() {
        let path = git_path(b"artifact/spa-prerender-repro");
        let file_id = stable_file_id(&path);
        let mut state = ReplayState::default();
        state.path_to_file_id.insert(path, file_id.clone());
        state.known_file_ids.insert(file_id);

        let changes = vec![Change {
            status: 'T',
            old_mode: "100644".to_string(),
            new_mode: "160000".to_string(),
            new_oid: "4c9431adbd4a24aed1d9afdecbfe4eaac3a6bba9".to_string(),
            old_path: Some(git_path(b"artifact/spa-prerender-repro")),
            new_path: Some(git_path(b"artifact/spa-prerender-repro")),
        }];

        let error = prepare_commit_changes(&mut state, &changes, &HashMap::new())
            .expect_err("gitlink typechange should be rejected");

        assert!(error.to_string().contains("gitlink/submodule"));
        assert!(error.to_string().contains("regular file contents only"));
    }

    #[test]
    fn prepare_commit_changes_rename_rebinds_path_bound_semantic_proof() {
        let old_path = git_path(b"src/old.ts");
        let new_path = git_path(b"src/new.ts");
        let old_file_id = stable_file_id(&old_path);
        let new_file_id = stable_file_id(&new_path);
        let mut state = ReplayState::default();
        state
            .path_to_file_id
            .insert(old_path.clone(), old_file_id.clone());
        state.known_file_ids.insert(old_file_id.clone());
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

        assert_eq!(prepared.deletes, vec![old_file_id]);
        assert_eq!(prepared.inserts.len(), 1);
        assert!(prepared.updates.is_empty());
        assert_eq!(prepared.inserts[0].id, new_file_id);
        assert_eq!(prepared.inserts[0].path, "/src/new.ts");
        assert_eq!(
            state.path_to_file_id.get(&new_path),
            Some(&stable_file_id(&new_path))
        );
    }

    #[test]
    fn prepare_storage_output_path_rejects_existing_file() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("temp dir should be created");
        let output_path = temp_dir.join("existing.rocksdb");
        fs::write(&output_path, b"existing").expect("seed file should be written");

        let result = prepare_storage_output_path(&output_path, false);
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
    fn prepare_storage_output_path_allows_absent_directory_and_creates_parent() {
        let temp_dir = unique_temp_dir();
        let nested_parent = temp_dir.join("nested").join("output");
        let output_path = nested_parent.join("new.rocksdb");

        let result = prepare_storage_output_path(&output_path, false);
        assert!(result.is_ok(), "expected success for absent output file");
        assert!(
            nested_parent.is_dir(),
            "expected parent directories to be created"
        );

        fs::remove_dir_all(&temp_dir).expect("temp dir should be removable");
    }

    #[test]
    fn prepare_storage_output_path_force_removes_existing_directory() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("temp dir should be created");
        let output_path = temp_dir.join("existing.rocksdb");
        fs::create_dir_all(&output_path).expect("seed directory should be created");
        fs::write(output_path.join("CURRENT"), b"rocksdb").expect("seed file should be written");

        let result = prepare_storage_output_path(&output_path, true);
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
        let result = validate_safe_storage_output_path(&repo, &repo.join("replay.rocksdb"));
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

        let result = validate_safe_storage_output_path(&repo, &output);

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
            output_path: output,
            storage: GitReplayStorage::Rocksdb,
            plugins: GitReplayPlugins::All,
            branch: "missing-ref".to_string(),
            from_commit: None,
            parent_tree: GitReplayParentTree::Window,
            num_commits: None,
            checkpoint_every: None,
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
            "INSERT INTO lix_key_value (key, value) VALUES ($1, $2) ON CONFLICT(key) DO UPDATE SET value = excluded.value"
        );
        assert_eq!(
            statement.params[0],
            Value::Text(GIT_REPLAY_MARKER_KEY.to_string())
        );
        assert_eq!(
            statement.params[1],
            Value::Jsonb(json!({"sha": "a".repeat(40), "first_parent": "b".repeat(40)}).into())
        );
    }

    #[test]
    fn build_replay_commit_statements_batches_stable_id_upserts() {
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
            "INSERT INTO lix_file (id, path, content, lixcol_metadata) VALUES ($1, $2, $3, $4) ON CONFLICT(id) DO UPDATE SET content = excluded.content, lixcol_metadata = excluded.lixcol_metadata"
        );
        assert_eq!(
            statements[0].params,
            vec![
                Value::Text("/src/main.ts".to_string()),
                Value::Text("/src/main.ts".to_string()),
                Value::Blob(b"hello".to_vec().into()),
                Value::Jsonb(json!({"git_mode": "100755", "git_oid": "a".repeat(40)}).into()),
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
                git_mode: "100644".to_string(),
                git_oid: "b".repeat(40),
            }],
            updates: Vec::new(),
        };

        let statements = build_replay_commit_statements(&batch, DEFAULT_INSERT_BATCH_ROWS);

        assert_eq!(statements.len(), 2);
        assert_eq!(statements[0].sql, "DELETE FROM lix_file WHERE id IN ($1)");
        assert_eq!(
            statements[0].params,
            vec![Value::Text("/src/old.ts".to_string())]
        );
        assert_eq!(
            statements[1].sql,
            "INSERT INTO lix_file (id, path, content, lixcol_metadata) VALUES ($1, $2, $3, $4)"
        );
        assert_eq!(
            statements[1].params,
            vec![
                Value::Text("/src/new.ts".to_string()),
                Value::Text("/src/new.ts".to_string()),
                Value::Blob(b"hello".to_vec().into()),
                Value::Jsonb(json!({"git_mode": "100644", "git_oid": "b".repeat(40)}).into()),
            ]
        );
    }

    #[test]
    fn prepare_commit_changes_rejects_symbolic_links() {
        let changes = vec![Change {
            status: 'A',
            old_mode: "000000".to_string(),
            new_mode: "120000".to_string(),
            new_oid: "c".repeat(40),
            old_path: None,
            new_path: Some(git_path(b"link.txt")),
        }];

        let error = prepare_commit_changes(&mut ReplayState::default(), &changes, &HashMap::new())
            .expect_err("symbolic links should be rejected");

        assert!(error.to_string().contains("symbolic link"));
        assert!(error.to_string().contains("/link.txt"));
    }

    #[test]
    fn raw_diff_parser_rejects_non_utf8_git_paths() {
        let raw = b":100644 100755 1111111111111111111111111111111111111111 2222222222222222222222222222222222222222 M\0dir/\xff name\0";
        let error = parse_raw_diff_tree(raw).expect_err("non-UTF-8 path should fail");

        assert!(error.to_string().contains("valid UTF-8"));
    }

    #[test]
    fn git_paths_are_literal_utf8() {
        let path = GitPath::from_diff_token("docs/100% @2x/日本語.txt".as_bytes())
            .expect("UTF-8 Git path should be accepted");

        assert_eq!(path.lix_path(), "/docs/100% @2x/日本語.txt");
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
    fn persistent_blob_reader_batches_requests_before_draining_large_responses() {
        let repo = unique_temp_dir();
        fs::create_dir_all(&repo).expect("fixture repository should be created");
        git_ok(&repo, &["init", "-q", "-b", "main"]);

        let mut large_blob = vec![b'x'; 128 * 1024];
        large_blob.push(b'\n');
        fs::write(repo.join("00-large.bin"), large_blob).expect("large blob should write");
        for index in 0..CAT_FILE_REQUESTS_PER_BATCH {
            fs::write(
                repo.join(format!("item-{index:02}.txt")),
                format!("blob {index}\n"),
            )
            .expect("small blob should write");
        }
        git_ok(&repo, &["add", "-A"]);

        let index = run_git_text(&repo, &["ls-files".to_string(), "-s".to_string()], None)
            .expect("index should list staged blobs");
        let mut blob_ids = Vec::new();
        let mut expected_by_oid = HashMap::new();
        for line in index.lines() {
            let (header, path) = line
                .split_once('\t')
                .expect("git ls-files record should contain a path separator");
            let oid = header
                .split_ascii_whitespace()
                .nth(1)
                .expect("git ls-files record should contain an object id");
            blob_ids.push(oid.to_string());
            expected_by_oid.insert(
                oid.to_string(),
                fs::read(repo.join(path)).expect("staged fixture data should read"),
            );
        }
        assert_eq!(
            blob_ids.len(),
            CAT_FILE_REQUESTS_PER_BATCH + 1,
            "fixture must exercise a second request batch"
        );

        let mut reader = GitBlobReader::spawn(&repo).expect("persistent blob reader should start");
        let blobs = reader
            .read_blobs(&blob_ids)
            .expect("batched blob requests should preserve every response");
        reader
            .finish()
            .expect("persistent blob reader should finish");

        assert_eq!(blobs, expected_by_oid);
        fs::remove_dir_all(&repo).expect("fixture repository should be removable");
    }

    #[test]
    fn rocksdb_replay_installs_format_plugins_and_keeps_nul_files_binary() {
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
        fs::write(repo.join("table.csv"), b"name,value\nalpha,1\n")
            .expect("CSV fixture should write");
        fs::write(repo.join("binary.bin"), b"raw\0bytes\n").expect("binary fixture should write");
        git_ok(&repo, &["add", "notes.txt", "table.csv", "binary.bin"]);
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
            output_path: output.clone(),
            storage: GitReplayStorage::Rocksdb,
            plugins: GitReplayPlugins::All,
            branch: "main".to_string(),
            from_commit: None,
            parent_tree: GitReplayParentTree::Window,
            num_commits: None,
            checkpoint_every: Some(1),
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
        assert_eq!(profile_json["checkpoint_every"], 1);
        assert!(
            profile_json["commits"]
                .as_array()
                .expect("profile commits should be an array")
                .iter()
                .all(|commit| commit["checkpoint_ms"].is_number()),
            "checkpoint-every replay must profile every checkpoint"
        );

        let storage = RocksDB::open(&output).expect("replay RocksDB should reopen");
        let lix = db::block_on(open_lix().with_storage(storage))
            .expect("replay Lix should reopen with installed plugin");
        let checkpoint_rows =
            db::block_on(lix.execute("SELECT count(*) AS count FROM lix_checkpoint", &[]))
                .expect("checkpoint history should be queryable");
        assert_eq!(
            checkpoint_rows.rows()[0]
                .get::<i64>("count")
                .expect("checkpoint count should decode"),
            5,
            "initialization plus four replayed commits should publish five checkpoints"
        );
        let text_rows = db::block_on(lix.execute(
            "SELECT lixcol_row_pk FROM text_line WHERE lixcol_file_id = $1",
            &[Value::Text(stable_file_id(&git_path(b"docs/renamed.txt")))],
        ))
        .expect("Git text rows should be queryable after replay");
        assert!(
            !text_rows.rows().is_empty(),
            "renamed text file must derive Git-text semantic rows at its new path"
        );
        let csv_rows = db::block_on(lix.execute(
            "SELECT lixcol_row_pk FROM csv_row WHERE lixcol_file_id = $1",
            &[Value::Text(stable_file_id(&git_path(b"table.csv")))],
        ))
        .expect("CSV rows should be queryable after replay");
        assert_eq!(
            csv_rows.rows().len(),
            2,
            "CSV replay must eagerly materialize both records"
        );
        let binary_rows = db::block_on(lix.execute(
            "SELECT lixcol_row_pk FROM text_line WHERE lixcol_file_id = $1",
            &[Value::Text(stable_file_id(&git_path(b"binary.bin")))],
        ))
        .expect("Git text rows should query for binary fixture");
        assert!(
            binary_rows.rows().is_empty(),
            "NUL-bearing file must remain a raw binary blob"
        );
        db::block_on(lix.close()).expect("reopened Lix should close");

        fs::remove_dir_all(&fixture).expect("fixture directory should be removable");
    }

    #[test]
    fn replay_profiles_plugin_controls_on_both_storage_adapters() {
        thread::Builder::new()
            .name("git-replay-storage-plugin-matrix".to_string())
            .stack_size(16 * 1024 * 1024)
            .spawn(replay_profiles_plugin_controls_on_both_storage_adapters_inner)
            .expect("replay matrix thread should spawn")
            .join()
            .expect("replay matrix thread should complete");
    }

    fn replay_profiles_plugin_controls_on_both_storage_adapters_inner() {
        let fixture = unique_temp_dir();
        let repo = fixture.join("repo");
        fs::create_dir_all(&repo).expect("fixture repository should be created");
        git_ok(&repo, &["init", "-q", "-b", "main"]);
        git_ok(&repo, &["config", "user.email", "replay@example.test"]);
        git_ok(&repo, &["config", "user.name", "Replay Test"]);
        fs::write(repo.join("table.csv"), b"name,value\nalpha,1\n")
            .expect("CSV fixture should write");
        git_ok(&repo, &["add", "table.csv"]);
        git_ok(&repo, &["commit", "-qm", "root"]);

        for storage in [GitReplayStorage::Rocksdb, GitReplayStorage::Slatedb] {
            for plugins in [GitReplayPlugins::None, GitReplayPlugins::All] {
                let label = format!("{}-{}", storage.as_str(), plugins.as_str());
                let output = fixture.join(format!("{label}.storage"));
                let profile = fixture.join(format!("{label}.json"));
                run(ExpGitReplayArgs {
                    repo_path: repo.clone(),
                    output_path: output.clone(),
                    storage,
                    plugins,
                    branch: "main".to_string(),
                    from_commit: None,
                    parent_tree: GitReplayParentTree::Window,
                    num_commits: Some(1),
                    checkpoint_every: None,
                    force: false,
                    profile_json: Some(profile.clone()),
                })
                .expect("storage/plugin replay matrix should complete");

                let profile_json: serde_json::Value = serde_json::from_slice(
                    &fs::read(&profile).expect("replay profile should be written"),
                )
                .expect("replay profile should be valid JSON");
                assert_eq!(profile_json["storage"], storage.as_str());
                assert_eq!(profile_json["plugins"], plugins.as_str());
                assert_eq!(profile_json["commits_replayed"], 1);
                assert_eq!(profile_json["history_scope"], "complete");
                assert_eq!(profile_json["scoped_paths"], 1);

                if plugins == GitReplayPlugins::All {
                    match storage {
                        GitReplayStorage::Rocksdb => {
                            assert_csv_semantics(RocksDB::open(&output).expect("reopen RocksDB"));
                        }
                        GitReplayStorage::Slatedb => {
                            assert_csv_semantics(SlateDB::open(&output).expect("reopen SlateDB"));
                        }
                    }
                }
            }
        }

        fs::remove_dir_all(&fixture).expect("fixture directory should be removable");
    }

    fn assert_csv_semantics<StorageImpl>(storage: StorageImpl)
    where
        StorageImpl: Storage + Clone + Send + Sync + 'static,
    {
        let lix = db::block_on(open_lix().with_storage(storage))
            .expect("replay Lix should reopen cleanly");
        let rows = db::block_on(lix.execute(
            "SELECT lixcol_row_pk FROM csv_row WHERE lixcol_file_id = $1",
            &[Value::Text(stable_file_id(&git_path(b"table.csv")))],
        ))
        .expect("CSV rows should be queryable after replay");
        assert_eq!(rows.rows().len(), 2);
        db::block_on(lix.close()).expect("reopened Lix should close");
    }

    #[test]
    fn rocksdb_replay_reports_physical_groups_for_text_actor_lifecycle_across_hundred_commits() {
        const TEXT_FILES: usize = 17;

        let fixture = unique_temp_dir();
        let repo = fixture.join("repo");
        let output = fixture.join("replay.rocksdb");
        let profile = fixture.join("profile.json");
        fs::create_dir_all(&repo).expect("fixture repository should be created");
        git_ok(&repo, &["init", "-q", "-b", "main"]);
        git_ok(&repo, &["config", "user.email", "replay@example.test"]);
        git_ok(&repo, &["config", "user.name", "Replay Test"]);

        let mut expected_final = Vec::with_capacity(TEXT_FILES);
        for index in 0..TEXT_FILES {
            let bytes = format!("root-{index}\n").into_bytes();
            fs::write(repo.join(format!("bulk-{index:02}.txt")), &bytes)
                .expect("root text fixture should write");
            expected_final.push(bytes);
        }
        git_ok(&repo, &["add", "-A"]);
        git_ok(&repo, &["commit", "-qm", "root text working set"]);

        // This one atomic replay batch updates every file that was just
        // imported. It exercises the cold-open Existing path as well as the
        // New actors in the root commit, and deliberately exceeds the default
        // 16-Store working set.
        for (index, bytes) in expected_final.iter_mut().enumerate() {
            *bytes = format!("second-{index}\n").into_bytes();
            fs::write(repo.join(format!("bulk-{index:02}.txt")), bytes)
                .expect("second text fixture should write");
        }
        git_ok(&repo, &["add", "-A"]);
        git_ok(&repo, &["commit", "-qm", "update broad text working set"]);

        for revision in 2..100 {
            let index = revision % TEXT_FILES;
            let path = format!("bulk-{index:02}.txt");
            let bytes = format!("revision-{revision}-{index}\n").into_bytes();
            fs::write(repo.join(&path), &bytes).expect("history text fixture should write");
            expected_final[index] = bytes;
            let message = format!("revision {revision}");
            git_ok(&repo, &["add", &path]);
            git_ok(&repo, &["commit", "-qm", &message]);
        }

        run(ExpGitReplayArgs {
            repo_path: repo,
            output_path: output.clone(),
            storage: GitReplayStorage::Rocksdb,
            plugins: GitReplayPlugins::All,
            branch: "main".to_string(),
            from_commit: None,
            parent_tree: GitReplayParentTree::Window,
            num_commits: Some(100),
            checkpoint_every: None,
            force: false,
            profile_json: Some(profile.clone()),
        })
        .expect("100-commit replay beyond the Store working set should complete");

        let profile_json: serde_json::Value =
            serde_json::from_slice(&fs::read(&profile).expect("replay profile should be written"))
                .expect("replay profile should be valid JSON");
        assert_eq!(
            profile_json
                .get("num_commits_requested")
                .and_then(serde_json::Value::as_u64),
            Some(100)
        );
        assert_eq!(
            profile_json
                .get("commits_replayed")
                .and_then(serde_json::Value::as_u64),
            Some(100)
        );
        assert_eq!(
            profile_json
                .get("commits_applied")
                .and_then(serde_json::Value::as_u64),
            Some(100)
        );
        assert_eq!(
            profile_json
                .get("commits_marker_only")
                .and_then(serde_json::Value::as_u64),
            Some(0)
        );
        assert_eq!(
            profile_json
                .get("changed_paths_total")
                .and_then(serde_json::Value::as_u64),
            Some((TEXT_FILES * 2 + 98) as u64)
        );
        let commits = profile_json
            .get("commits")
            .and_then(serde_json::Value::as_array)
            .expect("profile commits should be an array");
        assert_eq!(commits.len(), 100);
        assert_eq!(
            commits[0]
                .get("inserts")
                .and_then(serde_json::Value::as_u64),
            Some(TEXT_FILES as u64)
        );
        assert_eq!(
            commits[0]
                .get("updates")
                .and_then(serde_json::Value::as_u64),
            Some(0)
        );
        assert_eq!(
            commits[0]
                .get("logical_statement_count")
                .and_then(serde_json::Value::as_u64),
            Some((TEXT_FILES + 1) as u64),
            "logical descriptors include each file row and the replay marker"
        );
        assert_eq!(
            commits[0]
                .get("physical_execution_groups")
                .and_then(serde_json::Value::as_u64),
            Some((TEXT_FILES + 1) as u64),
            "each insert and the replay marker must use the public SQL transaction API"
        );
        assert_eq!(
            commits[1]
                .get("updates")
                .and_then(serde_json::Value::as_u64),
            Some(TEXT_FILES as u64)
        );
        assert_eq!(
            commits[1]
                .get("logical_statement_count")
                .and_then(serde_json::Value::as_u64),
            Some((TEXT_FILES + 1) as u64),
            "logical descriptors include each file row and the replay marker"
        );
        assert_eq!(
            commits[1]
                .get("physical_execution_groups")
                .and_then(serde_json::Value::as_u64),
            Some((TEXT_FILES + 1) as u64),
            "each update and the replay marker must use the public SQL transaction API"
        );

        let storage = RocksDB::open(&output).expect("replay RocksDB should reopen");
        let lix = db::block_on(open_lix().with_storage(storage))
            .expect("replay Lix should reopen with installed plugin");
        for (index, expected) in expected_final.iter().enumerate() {
            let path = format!("/bulk-{index:02}.txt");
            let file_rows = db::block_on(lix.execute(
                "SELECT content FROM lix_file WHERE path = $1",
                &[Value::Text(path.clone())],
            ))
            .expect("reopened rendered text file should query");
            assert_eq!(file_rows.rows().len(), 1, "expected one row for {path}");
            let rendered = value_to_optional_blob(
                file_rows.rows()[0]
                    .get_index(0)
                    .expect("rendered file row should contain data"),
                "reopened rendered text data",
            )
            .expect("rendered text data should be a blob");
            assert_eq!(
                rendered,
                Some(expected.as_slice()),
                "wrong bytes for {path}"
            );

            let semantic_rows = db::block_on(lix.execute(
                "SELECT lixcol_row_pk FROM text_line WHERE lixcol_file_id = $1",
                &[Value::Text(stable_file_id(&git_path(
                    path.trim_start_matches('/').as_bytes(),
                )))],
            ))
            .expect("reopened Git-text rows should query");
            assert_eq!(
                semantic_rows.rows().len(),
                1,
                "exactly one text semantic row should persist for {path}"
            );
        }
        db::block_on(lix.close()).expect("reopened Lix should close");

        fs::remove_dir_all(&fixture).expect("fixture directory should be removable");
    }

    #[test]
    fn rocksdb_replay_reconciles_duplicate_text_lines_across_commits() {
        let fixture = unique_temp_dir();
        let repo = fixture.join("repo");
        let output = fixture.join("replay.rocksdb");
        let profile = fixture.join("profile.json");
        fs::create_dir_all(repo.join("src")).expect("fixture repository should be created");
        git_ok(&repo, &["init", "-q", "-b", "main"]);
        git_ok(&repo, &["config", "user.email", "replay@example.test"]);
        git_ok(&repo, &["config", "user.name", "Replay Test"]);

        fs::write(repo.join("src/index.ts"), b"a\n").expect("root text fixture should write");
        git_ok(&repo, &["add", "src/index.ts"]);
        git_ok(&repo, &["commit", "-qm", "root text file"]);

        fs::write(repo.join("src/index.ts"), b"a\na\n")
            .expect("duplicate-line text fixture should write");
        git_ok(&repo, &["add", "src/index.ts"]);
        git_ok(&repo, &["commit", "-qm", "add duplicate line"]);

        run(ExpGitReplayArgs {
            repo_path: repo,
            output_path: output.clone(),
            storage: GitReplayStorage::Rocksdb,
            plugins: GitReplayPlugins::All,
            branch: "main".to_string(),
            from_commit: None,
            parent_tree: GitReplayParentTree::Window,
            num_commits: Some(2),
            checkpoint_every: None,
            force: false,
            profile_json: Some(profile.clone()),
        })
        .expect("duplicate-line text replay should complete");

        let profile_json: serde_json::Value =
            serde_json::from_slice(&fs::read(&profile).expect("replay profile should be written"))
                .expect("replay profile should be valid JSON");
        assert_eq!(
            profile_json
                .get("commits_replayed")
                .and_then(serde_json::Value::as_u64),
            Some(2)
        );
        assert_eq!(
            profile_json
                .get("commits_applied")
                .and_then(serde_json::Value::as_u64),
            Some(2)
        );
        let commits = profile_json
            .get("commits")
            .and_then(serde_json::Value::as_array)
            .expect("profile commits should be an array");
        assert_eq!(
            commits[1]
                .get("updates")
                .and_then(serde_json::Value::as_u64),
            Some(1)
        );

        let storage = RocksDB::open(&output).expect("replay RocksDB should reopen");
        let lix = db::block_on(open_lix().with_storage(storage))
            .expect("replay Lix should reopen with installed plugin");
        let file_rows = db::block_on(lix.execute(
            "SELECT content FROM lix_file WHERE path = $1",
            &[Value::Text("/src/index.ts".to_string())],
        ))
        .expect("replayed text file should query");
        assert_eq!(file_rows.rows().len(), 1);
        let rendered = value_to_optional_blob(
            file_rows.rows()[0]
                .get_index(0)
                .expect("replayed text row should contain data"),
            "replayed duplicate-line text data",
        )
        .expect("replayed text data should be a blob");
        assert_eq!(rendered, Some(b"a\na\n".as_slice()));

        let semantic_rows = db::block_on(lix.execute(
            "SELECT lixcol_row_pk FROM text_line WHERE lixcol_file_id = $1",
            &[Value::Text(stable_file_id(&git_path(b"src/index.ts")))],
        ))
        .expect("replayed Git-text rows should query");
        assert_eq!(semantic_rows.rows().len(), 2);
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
