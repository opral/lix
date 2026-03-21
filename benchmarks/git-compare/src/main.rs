use clap::Parser;
use lix_engine::{
    boot as boot_engine, BootArgs as EngineConfig, Engine, EngineTransaction, ExecuteOptions, Value,
};
use lix_rs_sdk::{SqliteBackend, WasmRuntime, WasmtimeRuntime};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Instant;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

const NULL_OID: &str = "0000000000000000000000000000000000000000";

type DynError = Box<dyn std::error::Error + Send + Sync>;
type DynResult<T> = Result<T, DynError>;

#[derive(Parser, Debug, Clone)]
#[command(about = "Benchmark write+commit latency for Git vs Lix on real repo workloads")]
struct Args {
    #[arg(long)]
    repo_path: PathBuf,
    #[arg(long, default_value = "HEAD")]
    head_ref: String,
    #[arg(long = "commit-sha")]
    commit_shas: Vec<String>,
    #[arg(long, default_value = "artifact/benchmarks/git-compare")]
    output_dir: PathBuf,
    #[arg(long, default_value_t = 5)]
    max_workloads: usize,
    #[arg(long, default_value_t = 200)]
    scan_commits: usize,
    #[arg(long, default_value_t = 5)]
    runs: usize,
    #[arg(long, default_value_t = 1)]
    warmups: usize,
    #[arg(long, default_value_t = 1)]
    min_changed_paths: usize,
    #[arg(long, default_value_t = 25)]
    max_changed_paths: usize,
    #[arg(long)]
    skip_verify: bool,
    #[arg(long)]
    keep_temp: bool,
    #[arg(long)]
    force: bool,
}

#[derive(Clone)]
struct CommitInfo {
    sha: String,
    parents: Vec<String>,
    subject: String,
}

#[derive(Clone)]
struct PatchSet {
    changes: Vec<RawChange>,
    blobs: HashMap<String, Vec<u8>>,
}

#[derive(Clone)]
struct RawChange {
    status: char,
    old_mode: String,
    new_mode: String,
    old_oid: String,
    new_oid: String,
    old_path: Option<String>,
    new_path: Option<String>,
}

#[derive(Clone)]
enum OperationKind {
    Add,
    Modify,
    Delete,
    Rename,
    Copy,
}

#[derive(Clone)]
struct FileOperation {
    kind: OperationKind,
    old_path: Option<String>,
    new_path: Option<String>,
    new_bytes: Option<Vec<u8>>,
    new_executable: bool,
}

#[derive(Clone)]
struct Workload {
    commit_sha: String,
    parent_sha: String,
    subject: String,
    changed_paths: usize,
    child_tree_sha: String,
    operations: Vec<FileOperation>,
    expected_files: BTreeMap<String, Vec<u8>>,
}

#[derive(Clone)]
struct LixTemplate {
    seed_rows: Vec<LixSeedRow>,
    path_to_id: BTreeMap<String, String>,
}

#[derive(Clone)]
struct LixSeedRow {
    id: String,
    path: String,
    data: Vec<u8>,
}

#[derive(Clone)]
struct PreparedWorkload {
    workload: Workload,
    git_template_dir: PathBuf,
    lix_template: LixTemplate,
}

#[derive(Serialize)]
struct Report {
    repo_path: String,
    head_ref: String,
    head_commit: String,
    config: ConfigReport,
    workload_selection: WorkloadSelectionReport,
    template_seed: TemplateSeedReport,
    workloads: Vec<WorkloadReport>,
    overall: OverallReport,
}

#[derive(Serialize)]
struct ConfigReport {
    runs: usize,
    warmups: usize,
    verify_state: bool,
    min_changed_paths: usize,
    max_changed_paths: usize,
    max_workloads: usize,
    scan_commits: usize,
}

#[derive(Serialize)]
struct WorkloadSelectionReport {
    selected_count: usize,
    skipped: Vec<SkippedCandidate>,
}

#[derive(Serialize)]
struct SkippedCandidate {
    commit_sha: String,
    subject: String,
    reason: String,
}

#[derive(Serialize)]
struct TemplateSeedReport {
    mode: &'static str,
}

#[derive(Serialize)]
struct WorkloadReport {
    commit_sha: String,
    parent_sha: String,
    subject: String,
    changed_paths: usize,
    child_tree_sha: String,
    git: MetricReport,
    lix: MetricReport,
    total_ratio_lix_over_git: f64,
    total_pct_less_time_for_lix: f64,
    trials: Vec<TrialResult>,
}

#[derive(Serialize)]
struct OverallReport {
    git: MetricReport,
    lix: MetricReport,
    total_ratio_lix_over_git: f64,
    total_pct_less_time_for_lix: f64,
}

#[derive(Serialize, Clone)]
struct MetricReport {
    write_ms: SummaryStats,
    commit_ms: SummaryStats,
    total_ms: SummaryStats,
}

#[derive(Serialize, Clone, Default)]
struct SummaryStats {
    samples: usize,
    min_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    mean_ms: f64,
    max_ms: f64,
}

#[derive(Serialize, Clone)]
struct TrialResult {
    workload_commit_sha: String,
    system: &'static str,
    iteration: usize,
    warmup: bool,
    write_ms: f64,
    commit_ms: f64,
    total_ms: f64,
    verified: bool,
}

fn main() {
    if let Err(error) = run_with_large_stack(real_main) {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn run_with_large_stack<F>(f: F) -> DynResult<()>
where
    F: FnOnce() -> DynResult<()> + Send + 'static,
{
    let handle = std::thread::Builder::new()
        .name("git-compare-benchmark".to_string())
        .stack_size(32 * 1024 * 1024)
        .spawn(f)?;
    match handle.join() {
        Ok(result) => result,
        Err(_) => Err("benchmark thread panicked".into()),
    }
}

fn real_main() -> DynResult<()> {
    let args = Args::parse();
    validate_args(&args)?;

    let repo_path = fs::canonicalize(&args.repo_path)?;
    ensure_git_repo(&repo_path)?;
    prepare_output_dir(&args.output_dir, args.force)?;

    let tmp_root = args.output_dir.join("tmp");
    fs::create_dir_all(&tmp_root)?;

    let head_commit = rev_parse_commit(&repo_path, &args.head_ref)?;
    let (workloads, skipped) = select_workloads(&repo_path, &args, &head_commit)?;
    let prepared = prepare_workloads(&repo_path, &args, &tmp_root, &workloads)?;

    let mut workload_reports = Vec::with_capacity(prepared.workloads.len());
    let mut all_trials = Vec::new();

    println!(
        "[git-compare] selected {} workloads from {}",
        prepared.workloads.len(),
        repo_path.display()
    );

    for prepared_workload in &prepared.workloads {
        println!(
            "[git-compare] workload {} {} ({} changed paths)",
            &prepared_workload.workload.commit_sha[..12],
            prepared_workload.workload.subject,
            prepared_workload.workload.changed_paths
        );
        let trials = run_workload_trials(
            &repo_path,
            &args,
            &tmp_root,
            prepared_workload,
            Arc::clone(&prepared.wasm_runtime),
        )?;
        let git_trials = filtered_trials(&trials, "git");
        let lix_trials = filtered_trials(&trials, "lix");
        let git_report = build_metric_report(&git_trials);
        let lix_report = build_metric_report(&lix_trials);
        let ratio = safe_ratio(lix_report.total_ms.p50_ms, git_report.total_ms.p50_ms);
        let pct_less = pct_less_time(lix_report.total_ms.p50_ms, git_report.total_ms.p50_ms);
        workload_reports.push(WorkloadReport {
            commit_sha: prepared_workload.workload.commit_sha.clone(),
            parent_sha: prepared_workload.workload.parent_sha.clone(),
            subject: prepared_workload.workload.subject.clone(),
            changed_paths: prepared_workload.workload.changed_paths,
            child_tree_sha: prepared_workload.workload.child_tree_sha.clone(),
            git: git_report,
            lix: lix_report,
            total_ratio_lix_over_git: ratio,
            total_pct_less_time_for_lix: pct_less,
            trials: trials.clone(),
        });
        all_trials.extend(trials);
    }

    let overall_git = build_metric_report(&filtered_trials(&all_trials, "git"));
    let overall_lix = build_metric_report(&filtered_trials(&all_trials, "lix"));
    let report = Report {
        repo_path: repo_path.display().to_string(),
        head_ref: args.head_ref.clone(),
        head_commit,
        config: ConfigReport {
            runs: args.runs,
            warmups: args.warmups,
            verify_state: !args.skip_verify,
            min_changed_paths: args.min_changed_paths,
            max_changed_paths: args.max_changed_paths,
            max_workloads: args.max_workloads,
            scan_commits: args.scan_commits,
        },
        workload_selection: WorkloadSelectionReport {
            selected_count: workload_reports.len(),
            skipped,
        },
        template_seed: TemplateSeedReport {
            mode: "git-parent-checkout + lix-parent-snapshot",
        },
        workloads: workload_reports,
        overall: OverallReport {
            git: overall_git.clone(),
            lix: overall_lix.clone(),
            total_ratio_lix_over_git: safe_ratio(
                overall_lix.total_ms.p50_ms,
                overall_git.total_ms.p50_ms,
            ),
            total_pct_less_time_for_lix: pct_less_time(
                overall_lix.total_ms.p50_ms,
                overall_git.total_ms.p50_ms,
            ),
        },
    };

    let json_path = args.output_dir.join("report.json");
    let markdown_path = args.output_dir.join("report.md");
    fs::write(
        &json_path,
        format!("{}\n", serde_json::to_string_pretty(&report)?),
    )?;
    fs::write(&markdown_path, render_markdown_report(&report))?;

    println!(
        "[git-compare] overall median total: git {:.2}ms, lix {:.2}ms, lix {:.2}% less time",
        report.overall.git.total_ms.p50_ms,
        report.overall.lix.total_ms.p50_ms,
        report.overall.total_pct_less_time_for_lix
    );
    println!("[git-compare] json: {}", json_path.display());
    println!("[git-compare] markdown: {}", markdown_path.display());

    if !args.keep_temp {
        let _ = fs::remove_dir_all(&tmp_root);
    }

    Ok(())
}

struct PreparedBenchmark {
    workloads: Vec<PreparedWorkload>,
    wasm_runtime: Arc<dyn WasmRuntime>,
}

fn validate_args(args: &Args) -> DynResult<()> {
    if args.max_workloads == 0 {
        return Err("--max-workloads must be >= 1".into());
    }
    if args.runs == 0 {
        return Err("--runs must be >= 1".into());
    }
    if args.min_changed_paths == 0 {
        return Err("--min-changed-paths must be >= 1".into());
    }
    if args.min_changed_paths > args.max_changed_paths {
        return Err("--min-changed-paths must be <= --max-changed-paths".into());
    }
    Ok(())
}

fn ensure_git_repo(repo_path: &Path) -> DynResult<()> {
    run_git_text(repo_path, ["rev-parse", "--git-dir"])?;
    Ok(())
}

fn prepare_output_dir(path: &Path, force: bool) -> DynResult<()> {
    if path.exists() {
        if !force {
            return Err(format!(
                "output dir already exists: {} (pass --force to overwrite)",
                path.display()
            )
            .into());
        }
        fs::remove_dir_all(path)?;
    }
    fs::create_dir_all(path)?;
    Ok(())
}

fn select_workloads(
    repo_path: &Path,
    args: &Args,
    head_commit: &str,
) -> DynResult<(Vec<Workload>, Vec<SkippedCandidate>)> {
    let commit_infos = if args.commit_shas.is_empty() {
        list_first_parent_commit_info(repo_path, &args.head_ref, Some(args.scan_commits))?
    } else {
        let mut commits = Vec::with_capacity(args.commit_shas.len());
        for commit_sha in &args.commit_shas {
            commits.push(read_commit_info(repo_path, commit_sha)?);
        }
        commits
    };
    let mut selected = Vec::new();
    let mut skipped = Vec::new();

    for commit in commit_infos {
        if selected.len() >= args.max_workloads {
            break;
        }
        if commit.sha == head_commit && commit.parents.is_empty() {
            skipped.push(SkippedCandidate {
                commit_sha: commit.sha,
                subject: commit.subject,
                reason: "root commit is not a useful user write+commit workload".to_string(),
            });
            continue;
        }
        if commit.parents.len() != 1 {
            skipped.push(SkippedCandidate {
                commit_sha: commit.sha,
                subject: commit.subject,
                reason: "merge commit skipped as a timed workload".to_string(),
            });
            continue;
        }

        let patch_set = read_commit_patch_set(repo_path, &commit.sha)?;
        if patch_set.changes.len() < args.min_changed_paths {
            skipped.push(SkippedCandidate {
                commit_sha: commit.sha,
                subject: commit.subject,
                reason: format!(
                    "changed path count {} below minimum {}",
                    patch_set.changes.len(),
                    args.min_changed_paths
                ),
            });
            continue;
        }
        if patch_set.changes.len() > args.max_changed_paths {
            skipped.push(SkippedCandidate {
                commit_sha: commit.sha,
                subject: commit.subject,
                reason: format!(
                    "changed path count {} above maximum {}",
                    patch_set.changes.len(),
                    args.max_changed_paths
                ),
            });
            continue;
        }
        if let Some(reason) = first_unsupported_change_reason(&patch_set.changes) {
            skipped.push(SkippedCandidate {
                commit_sha: commit.sha,
                subject: commit.subject,
                reason,
            });
            continue;
        }

        let operations = compile_operations(&patch_set)?;
        let expected_files =
            normalize_snapshot_for_lix(&read_tree_snapshot(repo_path, &commit.sha)?);
        let child_tree_sha = rev_parse_tree(repo_path, &commit.sha)?;

        selected.push(Workload {
            commit_sha: commit.sha,
            parent_sha: commit.parents[0].clone(),
            subject: commit.subject,
            changed_paths: operations.len(),
            child_tree_sha,
            operations,
            expected_files,
        });
    }

    if selected.is_empty() {
        return Err("no benchmark workloads selected; widen scan or changed-path filters".into());
    }

    Ok((selected, skipped))
}

fn prepare_workloads(
    repo_path: &Path,
    args: &Args,
    tmp_root: &Path,
    workloads: &[Workload],
) -> DynResult<PreparedBenchmark> {
    let wasm_runtime: Arc<dyn WasmRuntime> = Arc::new(WasmtimeRuntime::new()?);
    let git_templates_dir = tmp_root.join("git-templates");
    fs::create_dir_all(&git_templates_dir)?;
    let mut prepared_workloads = Vec::with_capacity(workloads.len());

    for workload in workloads {
        let parent_files = read_tree_snapshot(repo_path, &workload.parent_sha)?;
        let git_template_dir = git_templates_dir.join(&workload.commit_sha);
        create_git_checkout_template(repo_path, &git_template_dir, &workload.parent_sha)?;
        let lix_template = create_lix_snapshot_template(&parent_files)?;
        prepared_workloads.push(PreparedWorkload {
            workload: workload.clone(),
            git_template_dir,
            lix_template,
        });
    }

    Ok(PreparedBenchmark {
        workloads: prepared_workloads,
        wasm_runtime,
    })
}

fn run_workload_trials(
    repo_path: &Path,
    args: &Args,
    tmp_root: &Path,
    workload: &PreparedWorkload,
    wasm_runtime: Arc<dyn WasmRuntime>,
) -> DynResult<Vec<TrialResult>> {
    let git_trial_root = tmp_root
        .join("git-runs")
        .join(&workload.workload.commit_sha);
    let lix_trial_root = tmp_root
        .join("lix-runs")
        .join(&workload.workload.commit_sha);
    fs::create_dir_all(&git_trial_root)?;
    fs::create_dir_all(&lix_trial_root)?;

    let total_iterations = args.warmups + args.runs;
    let mut trials = Vec::with_capacity(total_iterations * 2);

    for iteration in 0..total_iterations {
        let warmup = iteration < args.warmups;
        let order = if iteration % 2 == 0 {
            ["git", "lix"]
        } else {
            ["lix", "git"]
        };

        for system in order {
            let trial = match system {
                "git" => run_git_trial(
                    &git_trial_root,
                    iteration,
                    warmup,
                    workload,
                    !args.skip_verify,
                )?,
                "lix" => run_lix_trial(
                    repo_path,
                    &lix_trial_root,
                    iteration,
                    warmup,
                    workload,
                    Arc::clone(&wasm_runtime),
                    !args.skip_verify,
                )?,
                _ => unreachable!(),
            };
            trials.push(trial);
        }
    }

    Ok(trials)
}

fn run_git_trial(
    trial_root: &Path,
    iteration: usize,
    warmup: bool,
    workload: &PreparedWorkload,
    verify_state: bool,
) -> DynResult<TrialResult> {
    let repo_dir = trial_root.join(format!("trial-{iteration}"));
    if repo_dir.exists() {
        fs::remove_dir_all(&repo_dir)?;
    }
    copy_directory(&workload.git_template_dir, &repo_dir)?;

    let write_started = Instant::now();
    apply_operations_to_git(&repo_dir, &workload.workload.operations)?;
    let write_ms = elapsed_ms(write_started);

    let commit_started = Instant::now();
    let commit_message = format!("bench {}", &workload.workload.commit_sha[..12]);
    run_git_text(&repo_dir, ["add", "-A"])?;
    run_git_text(
        &repo_dir,
        [
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "commit.gpgSign=false",
            "commit",
            "-q",
            "--allow-empty",
            "-m",
            &commit_message,
        ],
    )?;
    let commit_ms = elapsed_ms(commit_started);

    let verified = if verify_state {
        let actual_tree = run_git_text(&repo_dir, ["rev-parse", "HEAD^{tree}"])?;
        let actual_tree = actual_tree.trim();
        if actual_tree != workload.workload.child_tree_sha {
            return Err(format!(
                "git trial tree mismatch for {}: expected {}, got {}",
                workload.workload.commit_sha, workload.workload.child_tree_sha, actual_tree
            )
            .into());
        }
        true
    } else {
        false
    };

    fs::remove_dir_all(&repo_dir)?;
    Ok(TrialResult {
        workload_commit_sha: workload.workload.commit_sha.clone(),
        system: "git",
        iteration,
        warmup,
        write_ms,
        commit_ms,
        total_ms: write_ms + commit_ms,
        verified,
    })
}

fn run_lix_trial(
    _repo_path: &Path,
    trial_root: &Path,
    iteration: usize,
    warmup: bool,
    workload: &PreparedWorkload,
    wasm_runtime: Arc<dyn WasmRuntime>,
    verify_state: bool,
) -> DynResult<TrialResult> {
    let db_path = trial_root.join(format!("trial-{iteration}.lix"));
    if db_path.exists() {
        fs::remove_file(&db_path)?;
    }
    let engine = create_initialized_engine(&db_path, wasm_runtime)?;
    if !workload.lix_template.seed_rows.is_empty() {
        let seed_rows = workload.lix_template.seed_rows.clone();
        pollster::block_on(engine.transaction(ExecuteOptions::default(), |tx| {
            Box::pin(async move {
                for row in seed_rows {
                    tx.execute(
                        "INSERT INTO lix_file (id, path, data) VALUES (?1, ?2, ?3)",
                        &[
                            Value::Text(row.id),
                            Value::Text(row.path),
                            Value::Blob(row.data),
                        ],
                    )
                    .await?;
                }
                Ok(())
            })
        }))?;
    }
    let mut path_to_id = workload.lix_template.path_to_id.clone();
    let mut next_file_id = next_file_id_from_map(&path_to_id);
    let mut transaction =
        pollster::block_on(engine.begin_transaction_with_options(ExecuteOptions::default()))?;

    let write_started = Instant::now();
    for operation in &workload.workload.operations {
        execute_engine_operation(
            &mut transaction,
            operation,
            &mut path_to_id,
            &mut next_file_id,
        )?;
    }
    let write_ms = elapsed_ms(write_started);

    let commit_started = Instant::now();
    pollster::block_on(transaction.commit())?;
    let commit_ms = elapsed_ms(commit_started);

    let verified = if verify_state {
        verify_engine_state(&engine, &workload.workload.expected_files)?;
        true
    } else {
        false
    };

    drop(engine);
    let _ = fs::remove_file(&db_path);
    let _ = fs::remove_file(format!("{}-journal", db_path.display()));
    let _ = fs::remove_file(format!("{}-wal", db_path.display()));
    let _ = fs::remove_file(format!("{}-shm", db_path.display()));

    Ok(TrialResult {
        workload_commit_sha: workload.workload.commit_sha.clone(),
        system: "lix",
        iteration,
        warmup,
        write_ms,
        commit_ms,
        total_ms: write_ms + commit_ms,
        verified,
    })
}

fn create_git_checkout_template(
    repo_path: &Path,
    template_dir: &Path,
    parent_sha: &str,
) -> DynResult<()> {
    if template_dir.exists() {
        fs::remove_dir_all(template_dir)?;
    }
    run_command(
        "git",
        [
            "clone",
            "--local",
            "--quiet",
            repo_path.to_str().ok_or("invalid repo path")?,
            template_dir.to_str().ok_or("invalid template path")?,
        ],
        None,
        None,
    )?;
    run_git_text(template_dir, ["checkout", "--quiet", parent_sha])?;
    run_git_text(template_dir, ["config", "user.email", "bench@example.com"])?;
    run_git_text(template_dir, ["config", "user.name", "git-compare-bench"])?;
    run_git_text(template_dir, ["config", "core.hooksPath", "/dev/null"])?;
    run_git_text(template_dir, ["config", "commit.gpgSign", "false"])?;
    run_git_text(template_dir, ["config", "gc.auto", "0"])?;
    run_git_text(template_dir, ["config", "maintenance.auto", "false"])?;
    run_git_text(template_dir, ["config", "gc.autoDetach", "false"])?;
    Ok(())
}

fn create_lix_snapshot_template(
    parent_files: &BTreeMap<String, Vec<u8>>,
) -> DynResult<LixTemplate> {
    let mut path_to_id = BTreeMap::new();
    let mut next_file_id = 1_u64;
    let mut seed_rows = Vec::with_capacity(parent_files.len());
    for (path, bytes) in parent_files {
        let file_id = allocate_file_id(&mut next_file_id);
        let lix_path = to_lix_path(path);
        path_to_id.insert(lix_path.clone(), file_id.clone());
        seed_rows.push(LixSeedRow {
            id: file_id,
            path: lix_path,
            data: bytes.clone(),
        });
    }
    Ok(LixTemplate {
        seed_rows,
        path_to_id,
    })
}

fn apply_operations_to_git(repo_dir: &Path, operations: &[FileOperation]) -> DynResult<()> {
    for operation in operations {
        match operation.kind {
            OperationKind::Add | OperationKind::Copy | OperationKind::Modify => {
                let path = repo_dir.join(
                    operation
                        .new_path
                        .as_ref()
                        .ok_or("missing new path for git write")?,
                );
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::write(
                    &path,
                    operation
                        .new_bytes
                        .as_ref()
                        .ok_or("missing bytes for git write")?,
                )?;
                set_executable_if_needed(&path, operation.new_executable)?;
            }
            OperationKind::Rename => {
                if let Some(old_path) = &operation.old_path {
                    let old_full = repo_dir.join(old_path);
                    if old_full.exists() {
                        fs::remove_file(&old_full)?;
                    }
                }
                let new_full = repo_dir.join(
                    operation
                        .new_path
                        .as_ref()
                        .ok_or("missing new path for rename")?,
                );
                if let Some(parent) = new_full.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::write(
                    &new_full,
                    operation
                        .new_bytes
                        .as_ref()
                        .ok_or("missing bytes for rename")?,
                )?;
                set_executable_if_needed(&new_full, operation.new_executable)?;
            }
            OperationKind::Delete => {
                let path = repo_dir.join(
                    operation
                        .old_path
                        .as_ref()
                        .ok_or("missing old path for delete")?,
                );
                if path.exists() {
                    fs::remove_file(path)?;
                }
            }
        }
    }
    Ok(())
}

fn set_executable_if_needed(path: &Path, executable: bool) -> DynResult<()> {
    #[cfg(unix)]
    {
        let mode = if executable { 0o755 } else { 0o644 };
        let mut permissions = fs::metadata(path)?.permissions();
        permissions.set_mode(mode);
        fs::set_permissions(path, permissions)?;
    }
    #[cfg(not(unix))]
    let _ = (path, executable);
    Ok(())
}

fn execute_engine_operation(
    transaction: &mut EngineTransaction<'_>,
    operation: &FileOperation,
    path_to_id: &mut BTreeMap<String, String>,
    next_file_id: &mut u64,
) -> DynResult<()> {
    match operation.kind {
        OperationKind::Add | OperationKind::Copy => {
            let path = to_lix_path(
                operation
                    .new_path
                    .as_ref()
                    .ok_or("missing new path for Lix insert")?,
            );
            let file_id = allocate_file_id(next_file_id);
            pollster::block_on(
                transaction.execute(
                    "INSERT INTO lix_file (id, path, data) VALUES (?1, ?2, ?3)",
                    &[
                        Value::Text(file_id.clone()),
                        Value::Text(path.clone()),
                        Value::Blob(
                            operation
                                .new_bytes
                                .as_ref()
                                .ok_or("missing bytes for Lix insert")?
                                .clone(),
                        ),
                    ],
                ),
            )?;
            path_to_id.insert(path.clone(), file_id);
        }
        OperationKind::Modify => {
            let path = to_lix_path(
                operation
                    .new_path
                    .as_ref()
                    .ok_or("missing path for Lix update")?,
            );
            let file_id = path_to_id
                .get(&path)
                .cloned()
                .ok_or_else(|| format!("missing file id for modified path {path}"))?;
            pollster::block_on(
                transaction.execute(
                    "UPDATE lix_file SET data = ?1 WHERE id = ?2",
                    &[
                        Value::Blob(
                            operation
                                .new_bytes
                                .as_ref()
                                .ok_or("missing bytes for Lix update")?
                                .clone(),
                        ),
                        Value::Text(file_id),
                    ],
                ),
            )?;
        }
        OperationKind::Rename => {
            let old_path = to_lix_path(
                operation
                    .old_path
                    .as_ref()
                    .ok_or("missing old path for Lix rename")?,
            );
            let new_path = to_lix_path(
                operation
                    .new_path
                    .as_ref()
                    .ok_or("missing new path for Lix rename")?,
            );
            let file_id = path_to_id
                .remove(&old_path)
                .ok_or_else(|| format!("missing file id for renamed path {old_path}"))?;
            pollster::block_on(
                transaction.execute(
                    "UPDATE lix_file SET path = ?1, data = ?2 WHERE id = ?3",
                    &[
                        Value::Text(new_path.clone()),
                        Value::Blob(
                            operation
                                .new_bytes
                                .as_ref()
                                .ok_or("missing bytes for Lix rename")?
                                .clone(),
                        ),
                        Value::Text(file_id.clone()),
                    ],
                ),
            )?;
            path_to_id.insert(new_path.clone(), file_id);
        }
        OperationKind::Delete => {
            let old_path = to_lix_path(
                operation
                    .old_path
                    .as_ref()
                    .ok_or("missing old path for Lix delete")?,
            );
            let file_id = path_to_id
                .remove(&old_path)
                .ok_or_else(|| format!("missing file id for deleted path {old_path}"))?;
            pollster::block_on(transaction.execute(
                "DELETE FROM lix_file WHERE id = ?1",
                &[Value::Text(file_id)],
            ))?;
        }
    }
    Ok(())
}

fn verify_engine_state(
    engine: &Engine,
    expected_files: &BTreeMap<String, Vec<u8>>,
) -> DynResult<()> {
    let result =
        pollster::block_on(engine.execute("SELECT path, data FROM lix_file ORDER BY path", &[]))?;
    let mut actual = BTreeMap::new();
    for row in &result.statements[0].rows {
        let path = expect_text(&row[0])?;
        let bytes = value_as_bytes(&row[1])?;
        actual.insert(path, bytes);
    }
    if &actual != expected_files {
        return Err(format!(
            "Lix state verification failed: expected {} files, got {} files",
            expected_files.len(),
            actual.len()
        )
        .into());
    }

    Ok(())
}

fn create_initialized_engine(path: &Path, wasm_runtime: Arc<dyn WasmRuntime>) -> DynResult<Engine> {
    if path.exists() {
        fs::remove_file(path)?;
    }
    let init_backend = SqliteBackend::from_path(path)?;
    let engine = boot_engine(EngineConfig::new(
        Box::new(init_backend),
        Arc::clone(&wasm_runtime),
    ));
    let _ = pollster::block_on(engine.initialize_if_needed())?;
    pollster::block_on(engine.open_existing())?;
    Ok(engine)
}

fn expect_text(value: &Value) -> DynResult<String> {
    match value {
        Value::Text(text) => Ok(text.clone()),
        other => Err(format!("expected text value, got {other:?}").into()),
    }
}

fn value_as_bytes(value: &Value) -> DynResult<Vec<u8>> {
    match value {
        Value::Blob(bytes) => Ok(bytes.clone()),
        Value::Text(text) => Ok(text.as_bytes().to_vec()),
        other => Err(format!("expected blob/text value, got {other:?}").into()),
    }
}

fn next_file_id_from_map(path_to_id: &BTreeMap<String, String>) -> u64 {
    path_to_id
        .values()
        .filter_map(|id| id.strip_prefix("bench-file-"))
        .filter_map(|tail| tail.parse::<u64>().ok())
        .max()
        .unwrap_or(0)
        + 1
}

fn allocate_file_id(next_file_id: &mut u64) -> String {
    let file_id = format!("bench-file-{next_file_id}");
    *next_file_id += 1;
    file_id
}

fn filtered_trials(trials: &[TrialResult], system: &str) -> Vec<TrialResult> {
    trials
        .iter()
        .filter(|trial| trial.system == system && !trial.warmup)
        .cloned()
        .collect()
}

fn build_metric_report(trials: &[TrialResult]) -> MetricReport {
    MetricReport {
        write_ms: summarize(trials.iter().map(|trial| trial.write_ms).collect()),
        commit_ms: summarize(trials.iter().map(|trial| trial.commit_ms).collect()),
        total_ms: summarize(trials.iter().map(|trial| trial.total_ms).collect()),
    }
}

fn summarize(mut values: Vec<f64>) -> SummaryStats {
    if values.is_empty() {
        return SummaryStats::default();
    }
    values.sort_by(|left, right| left.partial_cmp(right).unwrap());
    let samples = values.len();
    let sum: f64 = values.iter().sum();
    SummaryStats {
        samples,
        min_ms: values[0],
        p50_ms: percentile(&values, 0.50),
        p95_ms: percentile(&values, 0.95),
        mean_ms: sum / samples as f64,
        max_ms: values[samples - 1],
    }
}

fn percentile(sorted_values: &[f64], percentile: f64) -> f64 {
    if sorted_values.is_empty() {
        return 0.0;
    }
    let rank = percentile * (sorted_values.len().saturating_sub(1)) as f64;
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;
    if lower == upper {
        return sorted_values[lower];
    }
    let weight = rank - lower as f64;
    sorted_values[lower] * (1.0 - weight) + sorted_values[upper] * weight
}

fn safe_ratio(numerator: f64, denominator: f64) -> f64 {
    if denominator == 0.0 {
        0.0
    } else {
        numerator / denominator
    }
}

fn pct_less_time(lix_ms: f64, git_ms: f64) -> f64 {
    if git_ms == 0.0 {
        0.0
    } else {
        (1.0 - (lix_ms / git_ms)) * 100.0
    }
}

fn render_markdown_report(report: &Report) -> String {
    let mut output = String::new();
    output.push_str("# Git Compare Benchmark\n\n");
    output.push_str(&format!(
        "Repo: `{}`  \nHead: `{}` (`{}`)\n\n",
        report.repo_path, report.head_ref, report.head_commit
    ));
    output.push_str("## Setup\n\n");
    output.push_str(&format!(
        "- workloads: `{}`\n- runs per system: `{}`\n- warmups: `{}`\n- verification: `{}`\n\n",
        report.workload_selection.selected_count,
        report.config.runs,
        report.config.warmups,
        report.config.verify_state,
    ));
    output.push_str("## Overall Median\n\n");
    output.push_str("| system | write ms | commit ms | total ms | p95 total ms |\n");
    output.push_str("| --- | ---: | ---: | ---: | ---: |\n");
    output.push_str(&format!(
        "| git | {:.2} | {:.2} | {:.2} | {:.2} |\n",
        report.overall.git.write_ms.p50_ms,
        report.overall.git.commit_ms.p50_ms,
        report.overall.git.total_ms.p50_ms,
        report.overall.git.total_ms.p95_ms
    ));
    output.push_str(&format!(
        "| lix | {:.2} | {:.2} | {:.2} | {:.2} |\n\n",
        report.overall.lix.write_ms.p50_ms,
        report.overall.lix.commit_ms.p50_ms,
        report.overall.lix.total_ms.p50_ms,
        report.overall.lix.total_ms.p95_ms
    ));
    output.push_str(&format!(
        "Lix median total time was `{:.2}%` less than Git on this benchmark (`{:.2}x` Lix/Git).\n\n",
        report.overall.total_pct_less_time_for_lix,
        report.overall.total_ratio_lix_over_git
    ));
    output.push_str("## Workloads\n\n");
    output.push_str("| commit | changed paths | git total ms | lix total ms | lix less time |\n");
    output.push_str("| --- | ---: | ---: | ---: | ---: |\n");
    for workload in &report.workloads {
        output.push_str(&format!(
            "| `{}` | {} | {:.2} | {:.2} | {:.2}% |\n",
            &workload.commit_sha[..12],
            workload.changed_paths,
            workload.git.total_ms.p50_ms,
            workload.lix.total_ms.p50_ms,
            workload.total_pct_less_time_for_lix
        ));
    }
    output.push_str("\n## Notes\n\n");
    output.push_str(&format!(
        "- template seed mode: `{}`\n- skipped candidate commits during workload selection: `{}`\n",
        report.template_seed.mode,
        report.workload_selection.skipped.len()
    ));
    output
}

fn list_first_parent_commit_info(
    repo_path: &Path,
    reference: &str,
    limit: Option<usize>,
) -> DynResult<Vec<CommitInfo>> {
    let mut args = vec![
        "log".to_string(),
        "--first-parent".to_string(),
        "--format=%H%x1f%P%x1f%s%x1e".to_string(),
    ];
    if let Some(limit) = limit {
        args.push("-n".to_string());
        args.push(limit.to_string());
    }
    args.push(reference.to_string());
    let output = run_git_text(repo_path, args.iter().map(String::as_str))?;
    let mut commits = Vec::new();
    for record in output.split('\x1e') {
        let trimmed = record.trim();
        if trimmed.is_empty() {
            continue;
        }
        let mut parts = trimmed.split('\x1f');
        let sha = parts.next().unwrap_or_default().trim().to_string();
        let parent_part = parts.next().unwrap_or_default().trim();
        let subject = parts.next().unwrap_or_default().trim().to_string();
        commits.push(CommitInfo {
            sha,
            parents: if parent_part.is_empty() {
                Vec::new()
            } else {
                parent_part
                    .split_whitespace()
                    .map(ToString::to_string)
                    .collect()
            },
            subject,
        });
    }
    Ok(commits)
}

fn read_commit_info(repo_path: &Path, reference: &str) -> DynResult<CommitInfo> {
    let sha = rev_parse_commit(repo_path, reference)?;
    let output = run_git_text(repo_path, ["log", "-1", "--format=%P%x1f%s", &sha])?;
    let trimmed = output.trim();
    let mut parts = trimmed.split('\x1f');
    let parent_part = parts.next().unwrap_or_default().trim();
    let subject = parts.next().unwrap_or_default().trim().to_string();
    Ok(CommitInfo {
        sha,
        parents: if parent_part.is_empty() {
            Vec::new()
        } else {
            parent_part
                .split_whitespace()
                .map(ToString::to_string)
                .collect()
        },
        subject,
    })
}

fn rev_parse_commit(repo_path: &Path, reference: &str) -> DynResult<String> {
    Ok(run_git_text(
        repo_path,
        ["rev-parse", "--verify", &format!("{reference}^{{commit}}")],
    )?
    .trim()
    .to_string())
}

fn rev_parse_tree(repo_path: &Path, commit_sha: &str) -> DynResult<String> {
    Ok(
        run_git_text(repo_path, ["rev-parse", &format!("{commit_sha}^{{tree}}")])?
            .trim()
            .to_string(),
    )
}

fn read_commit_patch_set(repo_path: &Path, commit_sha: &str) -> DynResult<PatchSet> {
    let raw = run_git_bytes(
        repo_path,
        [
            "diff-tree",
            "--root",
            "--raw",
            "-r",
            "-z",
            "-m",
            "--first-parent",
            "--find-renames",
            "--no-commit-id",
            commit_sha,
        ],
        None,
    )?;
    let changes = parse_raw_diff_tree(&raw)?;
    let wanted_blob_ids = collect_wanted_blob_ids(&changes);
    let blobs = read_blobs(repo_path, &wanted_blob_ids)?;
    Ok(PatchSet { changes, blobs })
}

fn parse_raw_diff_tree(raw: &[u8]) -> DynResult<Vec<RawChange>> {
    if raw.is_empty() {
        return Ok(Vec::new());
    }
    let tokens = raw
        .split(|byte| *byte == 0)
        .filter(|token| !token.is_empty())
        .collect::<Vec<_>>();
    let mut changes = Vec::new();
    let mut index = 0;
    while index < tokens.len() {
        let header = std::str::from_utf8(tokens[index])?;
        index += 1;
        if !header.starts_with(':') {
            continue;
        }
        let fields = header[1..].split(' ').collect::<Vec<_>>();
        if fields.len() < 5 {
            continue;
        }
        let status_token = fields[4];
        let status = status_token.chars().next().unwrap_or('M');
        let first_path =
            std::str::from_utf8(tokens.get(index).ok_or("missing diff-tree path")?)?.to_string();
        index += 1;
        if status == 'R' || status == 'C' {
            let second_path =
                std::str::from_utf8(tokens.get(index).ok_or("missing rename target path")?)?
                    .to_string();
            index += 1;
            changes.push(RawChange {
                status,
                old_mode: fields[0].to_string(),
                new_mode: fields[1].to_string(),
                old_oid: fields[2].to_string(),
                new_oid: fields[3].to_string(),
                old_path: Some(first_path),
                new_path: Some(second_path),
            });
            continue;
        }
        changes.push(RawChange {
            status,
            old_mode: fields[0].to_string(),
            new_mode: fields[1].to_string(),
            old_oid: fields[2].to_string(),
            new_oid: fields[3].to_string(),
            old_path: if status == 'A' {
                None
            } else {
                Some(first_path.clone())
            },
            new_path: if status == 'D' {
                None
            } else {
                Some(first_path)
            },
        });
    }
    Ok(changes)
}

fn collect_wanted_blob_ids(changes: &[RawChange]) -> Vec<String> {
    let mut ids = BTreeSet::new();
    for change in changes {
        if change.new_path.is_some()
            && is_regular_blob_mode(&change.new_mode)
            && change.new_oid != NULL_OID
        {
            ids.insert(change.new_oid.clone());
        }
    }
    ids.into_iter().collect()
}

fn read_tree_snapshot(repo_path: &Path, commit_sha: &str) -> DynResult<BTreeMap<String, Vec<u8>>> {
    let raw = run_git_bytes(
        repo_path,
        ["ls-tree", "-r", "-z", "--full-tree", commit_sha],
        None,
    )?;
    let mut path_by_oid = BTreeMap::new();
    for token in raw
        .split(|byte| *byte == 0)
        .filter(|token| !token.is_empty())
    {
        let entry = std::str::from_utf8(token)?;
        let (header, path) = entry.split_once('\t').ok_or("invalid ls-tree entry")?;
        let fields = header.split_whitespace().collect::<Vec<_>>();
        if fields.len() != 3 {
            continue;
        }
        let mode = fields[0];
        let object_type = fields[1];
        let oid = fields[2];
        if object_type != "blob" || !is_regular_blob_mode(mode) {
            continue;
        }
        path_by_oid.insert(path.to_string(), oid.to_string());
    }
    let blob_ids = path_by_oid.values().cloned().collect::<Vec<_>>();
    let blobs = read_blobs(repo_path, &blob_ids)?;
    let mut files = BTreeMap::new();
    for (path, oid) in path_by_oid {
        let bytes = blobs
            .get(&oid)
            .cloned()
            .ok_or_else(|| format!("missing blob {oid} for path {path}"))?;
        files.insert(path, bytes);
    }
    Ok(files)
}

fn compile_operations(patch_set: &PatchSet) -> DynResult<Vec<FileOperation>> {
    let mut operations = Vec::with_capacity(patch_set.changes.len());
    for change in &patch_set.changes {
        let new_bytes = if change.new_path.is_some() && is_regular_blob_mode(&change.new_mode) {
            Some(
                patch_set
                    .blobs
                    .get(&change.new_oid)
                    .cloned()
                    .ok_or_else(|| format!("missing blob bytes for {}", change.new_oid))?,
            )
        } else {
            None
        };

        let kind = match change.status {
            'A' => OperationKind::Add,
            'M' => OperationKind::Modify,
            'D' => OperationKind::Delete,
            'R' => OperationKind::Rename,
            'C' => OperationKind::Copy,
            other => {
                return Err(format!("unsupported diff status '{other}'").into());
            }
        };
        operations.push(FileOperation {
            kind,
            old_path: change.old_path.clone(),
            new_path: change.new_path.clone(),
            new_bytes,
            new_executable: change.new_mode == "100755",
        });
    }
    Ok(operations)
}

fn normalize_snapshot_for_lix(files: &BTreeMap<String, Vec<u8>>) -> BTreeMap<String, Vec<u8>> {
    files
        .iter()
        .map(|(path, bytes)| (to_lix_path(path), bytes.clone()))
        .collect()
}

fn to_lix_path(path: &str) -> String {
    let trimmed = path.trim_start_matches('/');
    let segments = trimmed
        .split('/')
        .filter(|segment| !segment.is_empty())
        .map(encode_lix_path_segment)
        .collect::<Vec<_>>();
    format!("/{}", segments.join("/"))
}

fn encode_lix_path_segment(segment: &str) -> String {
    let mut encoded = String::new();
    for byte in segment.as_bytes() {
        let ch = *byte as char;
        let allowed = ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '~' | '-');
        if allowed {
            encoded.push(ch);
        } else {
            encoded.push_str(&format!("%{:02X}", byte));
        }
    }
    encoded
}

fn first_unsupported_change_reason(changes: &[RawChange]) -> Option<String> {
    changes.iter().find_map(unsupported_change_reason)
}

fn unsupported_change_reason(change: &RawChange) -> Option<String> {
    match change.status {
        'A' => {
            if !is_regular_blob_mode(&change.new_mode) {
                Some(format!(
                    "added path {:?} uses unsupported mode {}",
                    change.new_path, change.new_mode
                ))
            } else {
                None
            }
        }
        'M' => {
            if !is_regular_blob_mode(&change.old_mode) || !is_regular_blob_mode(&change.new_mode) {
                return Some(format!(
                    "modified path {:?} uses unsupported mode {} -> {}",
                    change.new_path, change.old_mode, change.new_mode
                ));
            }
            if change.old_path == change.new_path
                && change.old_oid == change.new_oid
                && change.old_mode != change.new_mode
            {
                return Some(format!(
                    "mode-only change on {:?} is not represented by lix_file",
                    change.new_path
                ));
            }
            None
        }
        'D' => {
            if !is_regular_blob_mode(&change.old_mode) {
                Some(format!(
                    "deleted path {:?} uses unsupported mode {}",
                    change.old_path, change.old_mode
                ))
            } else {
                None
            }
        }
        'R' | 'C' => {
            if !is_regular_blob_mode(&change.old_mode) || !is_regular_blob_mode(&change.new_mode) {
                Some(format!(
                    "rename/copy {:?} -> {:?} uses unsupported mode {} -> {}",
                    change.old_path, change.new_path, change.old_mode, change.new_mode
                ))
            } else {
                None
            }
        }
        other => Some(format!("unsupported diff status '{other}'")),
    }
}

fn is_regular_blob_mode(mode: &str) -> bool {
    mode == "100644" || mode == "100755"
}

fn read_blobs(repo_path: &Path, blob_ids: &[String]) -> DynResult<HashMap<String, Vec<u8>>> {
    if blob_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let input = format!("{}\n", blob_ids.join("\n")).into_bytes();
    let output = run_git_bytes(repo_path, ["cat-file", "--batch"], Some(input))?;
    let mut blobs = HashMap::with_capacity(blob_ids.len());
    let mut offset = 0usize;
    while offset < output.len() {
        let line_end = output[offset..]
            .iter()
            .position(|byte| *byte == b'\n')
            .map(|index| offset + index)
            .ok_or("invalid cat-file batch output")?;
        let header = std::str::from_utf8(&output[offset..line_end])?;
        offset = line_end + 1;

        let header_fields = header.split_whitespace().collect::<Vec<_>>();
        if header_fields.len() != 3 {
            return Err(format!("invalid cat-file header: {header}").into());
        }
        let oid = header_fields[0].to_string();
        let object_type = header_fields[1];
        let size: usize = header_fields[2].parse()?;
        if object_type != "blob" {
            return Err(format!("expected blob for {oid}, got {object_type}").into());
        }
        let body_end = offset + size;
        if body_end > output.len() {
            return Err(format!("truncated blob body for {oid}").into());
        }
        blobs.insert(oid, output[offset..body_end].to_vec());
        offset = body_end + 1;
    }
    Ok(blobs)
}

fn run_git_text<I, S>(repo_path: &Path, args: I) -> DynResult<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let args_vec = args
        .into_iter()
        .map(|arg| arg.as_ref().to_string())
        .collect::<Vec<_>>();
    let output = run_command(
        "git",
        args_vec.iter().map(String::as_str),
        Some(repo_path),
        None,
    )?;
    Ok(String::from_utf8(output)?)
}

fn run_git_bytes<I, S>(repo_path: &Path, args: I, stdin: Option<Vec<u8>>) -> DynResult<Vec<u8>>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let args_vec = args
        .into_iter()
        .map(|arg| arg.as_ref().to_string())
        .collect::<Vec<_>>();
    run_command(
        "git",
        args_vec.iter().map(String::as_str),
        Some(repo_path),
        stdin,
    )
}

fn run_command<I, S>(
    program: &str,
    args: I,
    cwd: Option<&Path>,
    stdin: Option<Vec<u8>>,
) -> DynResult<Vec<u8>>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let args_vec = args
        .into_iter()
        .map(|arg| arg.as_ref().to_string())
        .collect::<Vec<_>>();
    let mut command = Command::new(program);
    command.args(&args_vec);
    if let Some(cwd) = cwd {
        command.current_dir(cwd);
    }
    if stdin.is_some() {
        command.stdin(Stdio::piped());
    }
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());
    let mut child = command.spawn()?;
    if let Some(stdin_bytes) = stdin {
        use std::io::Write;
        let mut child_stdin = child.stdin.take().ok_or("missing child stdin")?;
        child_stdin.write_all(&stdin_bytes)?;
    }
    let output = child.wait_with_output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "command failed: {} {}\n{}",
            program,
            args_vec.join(" "),
            stderr.trim()
        )
        .into());
    }
    Ok(output.stdout)
}

fn copy_directory(source: &Path, destination: &Path) -> DynResult<()> {
    if destination.exists() {
        fs::remove_dir_all(destination)?;
    }
    run_command(
        "cp",
        [
            "-R",
            source.to_str().ok_or("invalid source path")?,
            destination.to_str().ok_or("invalid destination path")?,
        ],
        None,
        None,
    )?;
    Ok(())
}

fn elapsed_ms(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1000.0
}
