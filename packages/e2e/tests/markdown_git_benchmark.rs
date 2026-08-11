//! End-to-end Markdown comparison using the same deterministic corpus, edit
//! offsets, commit count, and unrelated-branch topology for Lix and Git.
//! Git is reported both before and after explicit GC; Lix is measured after a
//! clean close with its normal automatic storage maintenance.

use lix::storage::Storage;
use lix::{CreateBranchOptions, Lix, MergeBranchOptions, SwitchBranchOptions, Value, open_lix};
use lix_storage_rocksdb::RocksDB;
use std::fs;
use std::io::{Cursor, Write};
use std::path::Path;
use std::process::{Command, Output};
use std::time::{Duration, Instant};

const DEFAULT_TARGET_BYTES: usize = 7 * 1024 * 1024 / 2;
const DEFAULT_EDIT_SAMPLES: usize = 20;
const DEFAULT_MERGE_SAMPLES: usize = 7;
const DEFAULT_COLD_SAMPLES: usize = 5;
const DEFAULT_PROFILE_SAMPLES: usize = 200;
const DEFAULT_SYNTAX_RICH_SAMPLES: usize = 3;
const PARAGRAPH_BODY_BYTES: usize = 496;

#[derive(Debug)]
struct Corpus {
    bytes: Vec<u8>,
    edit_offsets: Vec<usize>,
    texts: Vec<String>,
}

#[derive(Debug)]
struct MarkdownNode {
    id: String,
    payload_json: String,
}

#[derive(Debug)]
struct RepoSizes {
    total_bytes: u64,
    metadata_bytes: u64,
}

#[derive(Debug)]
struct ColdLixDurations {
    total: Vec<Duration>,
    storage_open: Vec<Duration>,
    engine_open: Vec<Duration>,
    materialized_read: Vec<Duration>,
}

#[derive(Debug)]
struct ColdLixEditDurations {
    total: Vec<Duration>,
    storage_open: Vec<Duration>,
    engine_open: Vec<Duration>,
    write: Vec<Duration>,
}

#[derive(Debug)]
struct GitFixture {
    root: tempfile::TempDir,
}

/// CPU-profile target for the ordinary public SQL byte-write path.
///
/// This deliberately excludes Git, semantic writes, cold opens, and merges so
/// a sampled profile is dominated by repeated steady-state Lix edits rather
/// than fixture construction. The comparison benchmark below remains the
/// source of user-visible Git versus Lix numbers.
#[tokio::test]
#[ignore = "manual steady-state Markdown byte-write profile target"]
async fn markdown_lix_byte_hotpath_profile() {
    init_perf_tracing();

    let target_bytes = env_usize("LIX_MARKDOWN_GIT_BENCH_BYTES", DEFAULT_TARGET_BYTES);
    let samples = env_usize("LIX_MARKDOWN_GIT_PROFILE_SAMPLES", DEFAULT_PROFILE_SAMPLES);
    assert!(samples > 0);

    let corpus = markdown_corpus(target_bytes);
    let archive = build_markdown_plugin_archive();
    let root = tempfile::tempdir().expect("create Markdown profile directory");
    let lix = open_rocksdb_lix(root.path()).await;
    install_plugin(&lix, "plugin_markdown", &archive).await;
    write_file(&lix, "/profile.md", corpus.bytes.clone()).await;

    let mut bytes = corpus.bytes;
    let started = Instant::now();
    for sample in 0..samples {
        let index = spread_index(sample, samples, corpus.edit_offsets.len() / 2);
        let offset = corpus.edit_offsets[index];
        bytes[offset] = edit_replacement(bytes[offset], sample);
        write_file(&lix, "/profile.md", bytes.clone()).await;
    }
    let elapsed = started.elapsed();
    eprintln!(
        "markdown_lix_hot_profile bytes={} samples={} total_ms={:.3} mean_ms={:.3}",
        bytes.len(),
        samples,
        elapsed.as_secs_f64() * 1_000.0,
        elapsed.as_secs_f64() * 1_000.0 / samples as f64,
    );

    assert_same_bytes(
        "profile target must preserve exact Markdown bytes",
        &read_file(&lix, "/profile.md").await,
        &bytes,
    );
    lix.close().await.expect("close Markdown profile Lix");
}

/// Guards the fallback side of the canonical literal-prose optimization with
/// a realistic GFM document. The timer deliberately follows the ordinary
/// public SQL write path and excludes workspace/plugin installation.
#[tokio::test]
#[ignore = "manual syntax-rich Markdown initial-import control"]
async fn markdown_syntax_rich_initial_import_control() {
    init_perf_tracing();

    let target_bytes = env_usize("LIX_MARKDOWN_SYNTAX_RICH_BENCH_BYTES", DEFAULT_TARGET_BYTES);
    let samples = env_usize(
        "LIX_MARKDOWN_SYNTAX_RICH_BENCH_SAMPLES",
        DEFAULT_SYNTAX_RICH_SAMPLES,
    );
    assert!(samples > 0);
    let source = syntax_rich_markdown_corpus(target_bytes);
    assert!(source.len() >= target_bytes.saturating_sub(1_024));
    let archive = build_markdown_plugin_archive();
    let mut imports = Vec::with_capacity(samples);

    for _sample in 0..samples {
        let root = tempfile::tempdir().expect("create syntax-rich Markdown benchmark directory");
        let lix = open_rocksdb_lix(root.path()).await;
        install_plugin(&lix, "plugin_markdown", &archive).await;

        let started = Instant::now();
        write_file(&lix, "/syntax-rich.md", source.clone()).await;
        imports.push(started.elapsed());

        assert_same_bytes(
            "syntax-rich Markdown import must preserve exact source bytes",
            &read_file(&lix, "/syntax-rich.md").await,
            &source,
        );
        let file_id = file_id_at_path(&lix, "/syntax-rich.md").await;
        assert!(
            !markdown_nodes_by_kind(&lix, &file_id, "table_cell")
                .await
                .is_empty(),
            "syntax-rich import must materialize semantic table-cell rows"
        );
        lix.close()
            .await
            .expect("close syntax-rich Markdown benchmark Lix");
    }

    print_duration_metric("syntax_rich_initial_import", "lix-semantic", &imports);
    eprintln!(
        "markdown_syntax_rich_control corpus_bytes={} samples={}",
        source.len(),
        samples,
    );
}

#[tokio::test]
#[ignore = "manual Git versus Lix Markdown benchmark"]
async fn markdown_git_semantic_rows_benchmark() {
    init_perf_tracing();

    let target_bytes = env_usize("LIX_MARKDOWN_GIT_BENCH_BYTES", DEFAULT_TARGET_BYTES);
    let edit_samples = env_usize("LIX_MARKDOWN_GIT_BENCH_EDIT_SAMPLES", DEFAULT_EDIT_SAMPLES);
    let semantic_edit_samples =
        env_usize("LIX_MARKDOWN_GIT_BENCH_SEMANTIC_EDIT_SAMPLES", edit_samples);
    let merge_samples = env_usize(
        "LIX_MARKDOWN_GIT_BENCH_MERGE_SAMPLES",
        DEFAULT_MERGE_SAMPLES,
    );
    let cold_samples = env_usize("LIX_MARKDOWN_GIT_BENCH_COLD_SAMPLES", DEFAULT_COLD_SAMPLES);
    assert!(edit_samples > 0 && semantic_edit_samples > 0 && merge_samples > 0 && cold_samples > 0);

    let corpus = markdown_corpus(target_bytes);
    assert!(
        corpus.texts.len() > edit_samples.max(semantic_edit_samples) + merge_samples * 4,
        "benchmark corpus must contain enough unrelated paragraphs"
    );
    let archive = build_markdown_plugin_archive();

    let lix_root = tempfile::tempdir().expect("create semantic Lix benchmark directory");
    let baseline_lix = open_rocksdb_lix(lix_root.path()).await;
    install_plugin(&baseline_lix, "plugin_markdown", &archive).await;
    baseline_lix.close().await.expect("close baseline Lix");
    let lix_fixed = lix_repo_sizes(lix_root.path());

    let lix = open_rocksdb_lix(lix_root.path()).await;
    let lix_import_started = Instant::now();
    write_file(&lix, "/benchmark.md", corpus.bytes.clone()).await;
    let lix_import = lix_import_started.elapsed();
    let file_id = file_id_at_path(&lix, "/benchmark.md").await;
    let initial_nodes = markdown_nodes_by_kind(&lix, &file_id, "paragraph").await;
    assert_eq!(initial_nodes.len(), corpus.texts.len());

    let mut lix_semantic_edits = Vec::with_capacity(semantic_edit_samples);
    let mut lix_main_state = corpus.bytes.clone();
    for sample in 0..semantic_edit_samples {
        let index = spread_index(sample, semantic_edit_samples, corpus.texts.len() / 2);
        let replacement = edit_replacement(corpus.bytes[corpus.edit_offsets[index]], sample);
        let payload = payload_with_replacement(
            &initial_nodes[index].payload_json,
            &corpus.texts[index],
            replacement,
        );
        let started = Instant::now();
        update_markdown_node(&lix, &file_id, &initial_nodes[index].id, payload).await;
        lix_semantic_edits.push(started.elapsed());
        lix_main_state[corpus.edit_offsets[index]] = replacement;
    }
    assert_same_bytes(
        "Lix semantic edit trace",
        &read_file(&lix, "/benchmark.md").await,
        &lix_main_state,
    );
    let premerge_state = lix_main_state.clone();
    lix.close().await.expect("close Lix history fixture");
    let lix_history = lix_repo_sizes(lix_root.path());

    let lix = open_rocksdb_lix(lix_root.path()).await;
    let mut lix_merges = Vec::with_capacity(merge_samples);
    let main_branch_id = lix.active_branch_id().await.expect("resolve main branch");
    for sample in 0..merge_samples {
        let source = lix
            .create_branch(CreateBranchOptions {
                id: Some(format!("01920000-0000-7000-8000-{:012x}", 0x600 + sample)),
                name: format!("Markdown merge source {sample}"),
                from_commit_id: None,
            })
            .await
            .expect("create Lix merge source branch");
        let target_index = corpus.texts.len() / 2 + sample * 2;
        let source_index = corpus.texts.len() - 1 - sample * 2;
        let target_payload = payload_with_replacement(
            &initial_nodes[target_index].payload_json,
            &corpus.texts[target_index],
            edit_replacement(
                corpus.bytes[corpus.edit_offsets[target_index]],
                100 + sample,
            ),
        );
        update_markdown_node(
            &lix,
            &file_id,
            &initial_nodes[target_index].id,
            target_payload,
        )
        .await;
        lix_main_state[corpus.edit_offsets[target_index]] = edit_replacement(
            corpus.bytes[corpus.edit_offsets[target_index]],
            100 + sample,
        );

        lix.switch_branch(SwitchBranchOptions {
            branch_id: source.id.clone(),
        })
        .await
        .expect("switch to Lix merge source");
        let source_payload = payload_with_replacement(
            &initial_nodes[source_index].payload_json,
            &corpus.texts[source_index],
            edit_replacement(
                corpus.bytes[corpus.edit_offsets[source_index]],
                200 + sample,
            ),
        );
        update_markdown_node(
            &lix,
            &file_id,
            &initial_nodes[source_index].id,
            source_payload,
        )
        .await;
        lix.switch_branch(SwitchBranchOptions {
            branch_id: main_branch_id.clone(),
        })
        .await
        .expect("switch to Lix merge target");

        let started = Instant::now();
        lix.merge_branch(MergeBranchOptions {
            source_branch_id: source.id,
        })
        .await
        .expect("unrelated Markdown rows should merge");
        lix_merges.push(started.elapsed());
        lix_main_state[corpus.edit_offsets[source_index]] = edit_replacement(
            corpus.bytes[corpus.edit_offsets[source_index]],
            200 + sample,
        );
    }
    let final_nodes = markdown_nodes_by_kind(&lix, &file_id, "paragraph")
        .await
        .into_iter()
        .map(|node| (node.id, node.payload_json))
        .collect::<std::collections::BTreeMap<_, _>>();
    for sample in 0..merge_samples {
        for (side, index, replacement_sample) in [
            ("target", corpus.texts.len() / 2 + sample * 2, 100 + sample),
            ("source", corpus.texts.len() - 1 - sample * 2, 200 + sample),
        ] {
            let replacement =
                edit_replacement(corpus.bytes[corpus.edit_offsets[index]], replacement_sample);
            let expected = payload_with_replacement(
                &initial_nodes[index].payload_json,
                &corpus.texts[index],
                replacement,
            );
            let actual = &final_nodes[&initial_nodes[index].id];
            assert_eq!(
                actual, &expected,
                "semantic {side} row from merge sample {sample} must remain visible"
            );
        }
    }
    let lix_final = read_file(&lix, "/benchmark.md").await;
    assert_same_bytes(
        "semantic merge must materialize both unrelated edits",
        &lix_final,
        &lix_main_state,
    );
    lix.close().await.expect("close semantic Lix benchmark");
    let lix_live = lix_repo_sizes(lix_root.path());

    let byte_root = tempfile::tempdir().expect("create byte-path Lix benchmark directory");
    let byte_lix = open_rocksdb_lix(byte_root.path()).await;
    install_plugin(&byte_lix, "plugin_markdown", &archive).await;
    write_file(&byte_lix, "/benchmark.md", corpus.bytes.clone()).await;
    let mut byte_state = corpus.bytes.clone();
    let mut lix_byte_edits = Vec::with_capacity(edit_samples);
    for sample in 0..edit_samples {
        let index = spread_index(sample, edit_samples, corpus.texts.len() / 2);
        byte_state[corpus.edit_offsets[index]] =
            edit_replacement(corpus.bytes[corpus.edit_offsets[index]], sample);
        let started = Instant::now();
        write_file(&byte_lix, "/benchmark.md", byte_state.clone()).await;
        lix_byte_edits.push(started.elapsed());
    }
    assert_same_bytes(
        "Lix byte path must retain the exact edited Markdown",
        &read_file(&byte_lix, "/benchmark.md").await,
        &byte_state,
    );
    byte_lix.close().await.expect("close byte-path Lix");
    let lix_cold_edits = cold_lix_byte_edits(
        byte_root.path(),
        &mut byte_state,
        &corpus.edit_offsets,
        cold_samples,
    )
    .await;

    // Keep the raw Git timing cohort independent from the semantic-history
    // cohort. This lets hot-byte measurements use a statistically useful
    // sample count even when semantic edit timings are intentionally bounded.
    let mut git_timing = GitFixture::new();
    git_timing.write_worktree(&corpus.bytes);
    git_timing.commit_all("initial Markdown corpus");
    let mut git_timing_state = corpus.bytes.clone();
    let mut git_edits = Vec::with_capacity(edit_samples);
    for sample in 0..edit_samples {
        let index = spread_index(sample, edit_samples, corpus.texts.len() / 2);
        git_timing_state[corpus.edit_offsets[index]] =
            edit_replacement(corpus.bytes[corpus.edit_offsets[index]], sample);
        let started = Instant::now();
        git_timing.write_worktree(&git_timing_state);
        git_timing.commit_all(&format!("timed edit paragraph {index}"));
        git_edits.push(started.elapsed());
    }

    let mut git = GitFixture::new();
    let git_fixed = git.repo_sizes();
    let git_import_started = Instant::now();
    git.write_worktree(&corpus.bytes);
    git.commit_all("initial Markdown corpus");
    let git_import = git_import_started.elapsed();

    let mut git_state = corpus.bytes.clone();
    for sample in 0..semantic_edit_samples {
        let index = spread_index(sample, semantic_edit_samples, corpus.texts.len() / 2);
        git_state[corpus.edit_offsets[index]] =
            edit_replacement(corpus.bytes[corpus.edit_offsets[index]], sample);
        git.write_worktree(&git_state);
        git.commit_all(&format!("edit paragraph {index}"));
    }
    assert_same_bytes(
        "Git and Lix must match after the identical pre-merge edit trace",
        &git_state,
        &premerge_state,
    );
    let git_history_live = git.repo_sizes();
    let git_gc_started = Instant::now();
    git.run(["gc", "--prune=now"]);
    let git_gc = git_gc_started.elapsed();
    let git_history_packed = git.repo_sizes();

    let mut git_merges = Vec::with_capacity(merge_samples);
    for sample in 0..merge_samples {
        let branch = format!("markdown-merge-source-{sample}");
        git.run(["branch", branch.as_str()]);
        let target_index = corpus.texts.len() / 2 + sample * 2;
        let source_index = corpus.texts.len() - 1 - sample * 2;

        git_state[corpus.edit_offsets[target_index]] = edit_replacement(
            corpus.bytes[corpus.edit_offsets[target_index]],
            100 + sample,
        );
        git.write_worktree(&git_state);
        git.commit_all(&format!("target edit {sample}"));

        git.run(["switch", "--quiet", branch.as_str()]);
        let mut source_state = git.read_worktree();
        source_state[corpus.edit_offsets[source_index]] = edit_replacement(
            corpus.bytes[corpus.edit_offsets[source_index]],
            200 + sample,
        );
        git.write_worktree(&source_state);
        git.commit_all(&format!("source edit {sample}"));
        git.run(["switch", "--quiet", "main"]);

        let started = Instant::now();
        git.run([
            "merge",
            "--quiet",
            "--no-ff",
            branch.as_str(),
            "-m",
            &format!("merge unrelated paragraph {sample}"),
        ]);
        git_merges.push(started.elapsed());
        git_state[corpus.edit_offsets[source_index]] = edit_replacement(
            corpus.bytes[corpus.edit_offsets[source_index]],
            200 + sample,
        );
        assert_same_bytes(
            "Git merge must contain both unrelated paragraph edits",
            &git.read_worktree(),
            &git_state,
        );
    }

    let git_live = git.repo_sizes();

    let lix_cold = cold_lix_reads(lix_root.path(), &lix_main_state, cold_samples).await;
    let git_cold = git.cold_object_reads(&git_state, cold_samples);
    semantic_table_merge_quality(&archive).await;

    print_duration_metric("initial_import", "lix-semantic", &[lix_import]);
    print_duration_metric("initial_import", "git", &[git_import]);
    print_duration_metric("sparse_edit_commit", "lix-byte", &lix_byte_edits);
    print_duration_metric("sparse_edit_commit", "lix-semantic", &lix_semantic_edits);
    print_duration_metric("sparse_edit_commit", "git", &git_edits);
    print_duration_metric("cold_sparse_edit_total", "lix-byte", &lix_cold_edits.total);
    print_duration_metric(
        "cold_sparse_edit_storage_open",
        "lix-byte",
        &lix_cold_edits.storage_open,
    );
    print_duration_metric(
        "cold_sparse_edit_engine_open",
        "lix-byte",
        &lix_cold_edits.engine_open,
    );
    print_duration_metric("cold_sparse_edit_write", "lix-byte", &lix_cold_edits.write);
    print_duration_metric("unrelated_row_merge", "lix-semantic", &lix_merges);
    print_duration_metric("unrelated_row_merge", "git", &git_merges);
    print_duration_metric("cold_open_read", "lix-semantic", &lix_cold.total);
    print_duration_metric("cold_storage_open", "lix-semantic", &lix_cold.storage_open);
    print_duration_metric("cold_engine_open", "lix-semantic", &lix_cold.engine_open);
    print_duration_metric(
        "cold_materialized_read",
        "lix-semantic",
        &lix_cold.materialized_read,
    );
    print_duration_metric("cold_object_read", "git", &git_cold);
    print_duration_metric("maintenance_gc", "git", &[git_gc]);
    print_storage_metric("fixed", "lix", &lix_fixed);
    print_storage_metric("history-live", "lix", &lix_history);
    print_storage_metric("post-successful-merges", "lix", &lix_live);
    print_storage_metric("fixed", "git", &git_fixed);
    print_storage_metric("history-live", "git", &git_history_live);
    print_storage_metric("history-packed", "git", &git_history_packed);
    print_storage_metric("post-successful-merges", "git", &git_live);
    eprintln!(
        "markdown_git_bench merge_quality scenario=distinct_table_cells_same_line \
         system=lix clean_merges=1 conflicts=0 preserved_edits=2"
    );
    eprintln!(
        "markdown_git_bench merge_quality scenario=distinct_table_cells_same_line \
         system=git clean_merges=0 conflicts=1 preserved_edits=0"
    );
    eprintln!(
        "markdown_git_bench corpus_bytes={} paragraphs={} edit_samples={} semantic_edit_samples={} merge_samples={} \
         lix_incremental_total_bytes={} lix_incremental_metadata_bytes={} \
         git_live_incremental_total_bytes={} git_live_incremental_metadata_bytes={} \
         git_packed_incremental_total_bytes={} git_packed_incremental_metadata_bytes={}",
        corpus.bytes.len(),
        corpus.texts.len(),
        edit_samples,
        semantic_edit_samples,
        merge_samples,
        lix_history
            .total_bytes
            .saturating_sub(lix_fixed.total_bytes),
        lix_history
            .metadata_bytes
            .saturating_sub(lix_fixed.metadata_bytes),
        git_history_live
            .total_bytes
            .saturating_sub(git_fixed.total_bytes),
        git_history_live
            .metadata_bytes
            .saturating_sub(git_fixed.metadata_bytes),
        git_history_packed
            .total_bytes
            .saturating_sub(git_fixed.total_bytes),
        git_history_packed
            .metadata_bytes
            .saturating_sub(git_fixed.metadata_bytes),
    );
}

impl GitFixture {
    fn new() -> Self {
        let root = tempfile::tempdir().expect("create Git benchmark directory");
        let fixture = Self { root };
        fixture.run(["init", "--quiet", "--initial-branch=main"]);
        for (key, value) in [
            ("user.name", "Lix benchmark"),
            ("user.email", "benchmark@lix.dev"),
            ("commit.gpgSign", "false"),
            ("gc.auto", "0"),
            ("core.autocrlf", "false"),
            ("core.fsync", "none"),
        ] {
            fixture.run(["config", key, value]);
        }
        fixture
    }

    fn run<const N: usize>(&self, args: [&str; N]) -> Output {
        let output = self.try_run(args);
        assert!(
            output.status.success(),
            "Git command failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        output
    }

    fn try_run<const N: usize>(&self, args: [&str; N]) -> Output {
        Command::new("git")
            .args(args)
            .current_dir(self.root.path())
            .env("GIT_AUTHOR_DATE", "2000-01-01T00:00:00Z")
            .env("GIT_COMMITTER_DATE", "2000-01-01T00:00:00Z")
            .output()
            .expect("run Git command")
    }

    fn write_worktree(&self, bytes: &[u8]) {
        fs::write(self.root.path().join("benchmark.md"), bytes).expect("write Git worktree file");
    }

    fn read_worktree(&self) -> Vec<u8> {
        fs::read(self.root.path().join("benchmark.md")).expect("read Git worktree file")
    }

    fn commit_all(&mut self, message: &str) {
        self.run(["add", "--", "benchmark.md"]);
        self.run(["commit", "--quiet", "-m", message]);
    }

    fn repo_sizes(&self) -> RepoSizes {
        RepoSizes {
            total_bytes: directory_bytes(self.root.path()),
            metadata_bytes: directory_bytes(&self.root.path().join(".git")),
        }
    }

    fn cold_object_reads(&self, expected: &[u8], samples: usize) -> Vec<Duration> {
        let mut durations = Vec::with_capacity(samples);
        for _ in 0..samples {
            let started = Instant::now();
            let output = self.run(["show", "HEAD:benchmark.md"]);
            durations.push(started.elapsed());
            assert_same_bytes("Git cold object read", &output.stdout, expected);
        }
        durations
    }
}

async fn cold_lix_byte_edits(
    root: &Path,
    bytes: &mut [u8],
    edit_offsets: &[usize],
    samples: usize,
) -> ColdLixEditDurations {
    let mut total = Vec::with_capacity(samples);
    let mut storage_open = Vec::with_capacity(samples);
    let mut engine_open = Vec::with_capacity(samples);
    let mut write = Vec::with_capacity(samples);
    for sample in 0..samples {
        let index = spread_index(sample, samples, edit_offsets.len() / 2);
        bytes[edit_offsets[index]] = edit_replacement(bytes[edit_offsets[index]], 500 + sample);
        let started = Instant::now();
        let storage_started = Instant::now();
        let storage =
            RocksDB::open(root.join(".lix")).expect("open cold byte-edit Lix RocksDB storage");
        storage_open.push(storage_started.elapsed());
        let engine_started = Instant::now();
        let lix = open_lix()
            .with_storage(storage)
            .await
            .expect("open cold byte-edit Lix workspace");
        engine_open.push(engine_started.elapsed());
        let write_started = Instant::now();
        write_file(&lix, "/benchmark.md", bytes.to_vec()).await;
        write.push(write_started.elapsed());
        total.push(started.elapsed());
        lix.close().await.expect("close cold byte-edit sample");
    }
    ColdLixEditDurations {
        total,
        storage_open,
        engine_open,
        write,
    }
}

async fn cold_lix_reads(root: &Path, expected: &[u8], samples: usize) -> ColdLixDurations {
    let mut total = Vec::with_capacity(samples);
    let mut storage_open = Vec::with_capacity(samples);
    let mut engine_open = Vec::with_capacity(samples);
    let mut materialized_read = Vec::with_capacity(samples);
    for _ in 0..samples {
        let started = Instant::now();
        let storage_started = Instant::now();
        let storage = RocksDB::open(root.join(".lix")).expect("open cold Lix RocksDB storage");
        storage_open.push(storage_started.elapsed());
        let engine_started = Instant::now();
        let lix = open_lix()
            .with_storage(storage)
            .await
            .expect("open cold Lix workspace");
        engine_open.push(engine_started.elapsed());
        let read_started = Instant::now();
        let actual = read_file(&lix, "/benchmark.md").await;
        materialized_read.push(read_started.elapsed());
        total.push(started.elapsed());
        assert_same_bytes("Lix cold read", &actual, expected);
        lix.close().await.expect("close cold Lix sample");
    }
    ColdLixDurations {
        total,
        storage_open,
        engine_open,
        materialized_read,
    }
}

async fn semantic_table_merge_quality(archive: &[u8]) {
    const TABLE: &[u8] = b"| left | right |\n| --- | --- |\n| alpha | beta |\n";

    let root = tempfile::tempdir().expect("create Lix semantic quality directory");
    let lix = open_rocksdb_lix(root.path()).await;
    install_plugin(&lix, "plugin_markdown", archive).await;
    write_file(&lix, "/quality.md", TABLE.to_vec()).await;
    let file_id = file_id_at_path(&lix, "/quality.md").await;
    let cells = markdown_nodes_by_kind(&lix, &file_id, "table_cell").await;
    let alpha = cells
        .iter()
        .find(|node| payload_contains_text(&node.payload_json, "alpha"))
        .expect("Markdown table should expose alpha as a cell row");
    let beta = cells
        .iter()
        .find(|node| payload_contains_text(&node.payload_json, "beta"))
        .expect("Markdown table should expose beta as a cell row");
    assert_ne!(alpha.id, beta.id, "table cells must be distinct rows");

    let main_branch_id = lix.active_branch_id().await.expect("resolve main branch");
    let source = lix
        .create_branch(CreateBranchOptions {
            id: Some("01920000-0000-7000-8000-000000000504".to_string()),
            name: "Markdown table source".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("create Lix table-cell source branch");
    update_markdown_node(
        &lix,
        &file_id,
        &alpha.id,
        payload_with_text(&alpha.payload_json, "ALPHA"),
    )
    .await;
    lix.switch_branch(SwitchBranchOptions {
        branch_id: source.id.clone(),
    })
    .await
    .expect("switch to Lix table-cell source");
    update_markdown_node(
        &lix,
        &file_id,
        &beta.id,
        payload_with_text(&beta.payload_json, "BETA"),
    )
    .await;
    lix.switch_branch(SwitchBranchOptions {
        branch_id: main_branch_id,
    })
    .await
    .expect("switch to Lix table-cell target");
    lix.merge_branch(MergeBranchOptions {
        source_branch_id: source.id,
    })
    .await
    .expect("distinct table-cell rows should merge in Lix");
    let rendered = String::from_utf8(read_file(&lix, "/quality.md").await)
        .expect("rendered Markdown table should be UTF-8");
    assert!(
        rendered.contains("ALPHA") && rendered.contains("BETA"),
        "Lix semantic table merge must preserve both cell edits: {rendered:?}"
    );
    lix.close().await.expect("close Lix table quality fixture");

    let mut git = GitFixture::new();
    git.write_worktree(TABLE);
    git.commit_all("initial Markdown table");
    git.run(["branch", "markdown-table-source"]);
    git.write_worktree(
        &String::from_utf8_lossy(TABLE)
            .replace("alpha", "ALPHA")
            .into_bytes(),
    );
    git.commit_all("edit left table cell");
    git.run(["switch", "--quiet", "markdown-table-source"]);
    git.write_worktree(
        &String::from_utf8_lossy(TABLE)
            .replace("beta", "BETA")
            .into_bytes(),
    );
    git.commit_all("edit right table cell");
    git.run(["switch", "--quiet", "main"]);
    let merge = git.try_run([
        "merge",
        "--quiet",
        "--no-ff",
        "markdown-table-source",
        "-m",
        "merge distinct table cells",
    ]);
    assert!(
        !merge.status.success(),
        "Git should report a same-line conflict for distinct table-cell edits"
    );
}

fn markdown_corpus(target_bytes: usize) -> Corpus {
    let mut bytes = Vec::with_capacity(target_bytes + 512);
    let mut edit_offsets = Vec::new();
    let mut texts = Vec::new();
    let paragraph_bytes = 8 + PARAGRAPH_BODY_BYTES + 2;
    let paragraph_count = target_bytes.div_ceil(paragraph_bytes);
    for index in 0..paragraph_count {
        let mut text = format!("P{index:06} ");
        text.push_str(&paragraph_body(index));
        let offset = bytes.len() + 8;
        edit_offsets.push(offset);
        texts.push(text.clone());
        bytes.extend_from_slice(text.as_bytes());
        if index + 1 == paragraph_count {
            bytes.push(b'\n');
        } else {
            bytes.extend_from_slice(b"\n\n");
        }
    }
    Corpus {
        bytes,
        edit_offsets,
        texts,
    }
}

fn syntax_rich_markdown_corpus(target_bytes: usize) -> Vec<u8> {
    const RICH_BLOCK_INTERVAL: usize = 64;

    let mut bytes = Vec::with_capacity(target_bytes + 512);
    for index in 0usize.. {
        let prose = format!("P{index:06} {}\n\n", paragraph_body(index));
        if bytes.len() + prose.len() > target_bytes {
            break;
        }
        bytes.extend_from_slice(prose.as_bytes());

        // A single non-literal block makes the entire document take the
        // canonical-rendering fallback. Keeping these periodic mirrors a
        // typical prose document and avoids treating an all-table stress case
        // as the 90%-path control.
        if index % RICH_BLOCK_INTERVAL == RICH_BLOCK_INTERVAL - 1 {
            let block = format!(
                "## Section {index}\n\nParagraph {index} has *emphasis*, **strong**, ~~delete~~, [a link](https://example.com/{index}), and `code`.\n\n- alpha {index}\n- beta {index}\n\n| key | value |\n| --- | :--- |\n| left {index} | right {index} |\n\n```rust\nlet value_{index} = {index};\n```\n\n> quoted paragraph {index}\n\n"
            );
            if bytes.len() + block.len() <= target_bytes {
                bytes.extend_from_slice(block.as_bytes());
            }
        }
    }
    bytes
}

fn spread_index(sample: usize, samples: usize, upper: usize) -> usize {
    ((sample + 1) * upper / (samples + 1)).max(1)
}

fn paragraph_body(index: usize) -> String {
    const WORDS: &[&str] = &[
        "amber", "branch", "canvas", "delta", "ember", "forest", "gentle", "harbor", "island",
        "jungle", "kernel", "lantern", "meadow", "native", "orbit", "paper", "quiet", "river",
        "silver", "timber", "update", "violet", "window", "yellow",
    ];
    let mut body = String::with_capacity(PARAGRAPH_BODY_BYTES);
    let mut cursor = index.wrapping_mul(17);
    while body.len() < PARAGRAPH_BODY_BYTES {
        if !body.is_empty() {
            body.push(' ');
        }
        body.push_str(WORDS[cursor % WORDS.len()]);
        cursor = cursor.wrapping_mul(1_103_515_245).wrapping_add(12_345);
    }
    body.truncate(PARAGRAPH_BODY_BYTES);
    if body.ends_with(' ') {
        body.pop();
        body.push('x');
    }
    body
}

fn edit_replacement(original: u8, sample: usize) -> u8 {
    assert!(original.is_ascii_lowercase());
    b'a' + ((original - b'a' + 1 + u8::try_from(sample % 25).expect("bounded replacement")) % 26)
}

fn payload_with_replacement(payload: &str, original_text: &str, replacement: u8) -> String {
    let mut expected = original_text.as_bytes().to_vec();
    expected[8] = replacement;
    let replacement_text = String::from_utf8(expected).expect("replacement remains UTF-8");
    let mut value: serde_json::Value =
        serde_json::from_str(payload).expect("Markdown payload should be JSON");
    assert!(
        replace_first_text_value(&mut value, &replacement_text),
        "Markdown paragraph payload must contain a text inline"
    );
    serde_json::to_string(&value).expect("serialize updated Markdown payload")
}

fn payload_with_text(payload: &str, replacement: &str) -> String {
    let mut value: serde_json::Value =
        serde_json::from_str(payload).expect("Markdown payload should be JSON");
    assert!(
        replace_first_text_value(&mut value, replacement),
        "Markdown payload must contain a text inline"
    );
    serde_json::to_string(&value).expect("serialize updated Markdown payload")
}

fn payload_contains_text(payload: &str, expected: &str) -> bool {
    fn contains(value: &serde_json::Value, expected: &str) -> bool {
        match value {
            serde_json::Value::String(value) => value == expected,
            serde_json::Value::Array(values) => {
                values.iter().any(|value| contains(value, expected))
            }
            serde_json::Value::Object(object) => {
                object.values().any(|value| contains(value, expected))
            }
            _ => false,
        }
    }

    serde_json::from_str(payload).is_ok_and(|value| contains(&value, expected))
}

fn replace_first_text_value(value: &mut serde_json::Value, replacement: &str) -> bool {
    match value {
        serde_json::Value::Object(object) => {
            if object.get("type").and_then(serde_json::Value::as_str) == Some("text")
                && object
                    .get("value")
                    .is_some_and(serde_json::Value::is_string)
            {
                object.insert(
                    "value".to_owned(),
                    serde_json::Value::String(replacement.to_owned()),
                );
                return true;
            }
            object
                .values_mut()
                .any(|child| replace_first_text_value(child, replacement))
        }
        serde_json::Value::Array(array) => array
            .iter_mut()
            .any(|child| replace_first_text_value(child, replacement)),
        _ => false,
    }
}

async fn markdown_nodes_by_kind<StorageImpl>(
    lix: &Lix<StorageImpl>,
    file_id: &str,
    kind: &str,
) -> Vec<MarkdownNode>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "SELECT id, payload_json FROM markdown_node \
         WHERE kind = $1 AND lixcol_file_id = $2 ORDER BY order_key, id",
        &[
            Value::Text(kind.to_owned()),
            Value::Text(file_id.to_owned()),
        ],
    )
    .await
    .expect("query Markdown rows by kind")
    .rows()
    .iter()
    .map(|row| MarkdownNode {
        id: row.get::<String>("id").expect("Markdown ID should be text"),
        payload_json: row
            .get::<String>("payload_json")
            .expect("Markdown payload should be text"),
    })
    .collect()
}

async fn update_markdown_node<StorageImpl>(
    lix: &Lix<StorageImpl>,
    file_id: &str,
    id: &str,
    payload_json: String,
) where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    for attempt in 0..5 {
        match lix
            .execute(
                "UPDATE markdown_node SET payload_json = $1 \
                 WHERE id = $2 AND lixcol_file_id = $3",
                &[
                    Value::Text(payload_json.clone()),
                    Value::Text(id.to_owned()),
                    Value::Text(file_id.to_owned()),
                ],
            )
            .await
        {
            Ok(result) => {
                assert_eq!(result.rows_affected(), 1);
                return;
            }
            Err(error) if error.code == "LIX_TRANSACTION_CONFLICT" && attempt < 4 => {
                tokio::task::yield_now().await;
            }
            Err(error) => panic!("update Markdown semantic row: {error:?}"),
        }
    }
    unreachable!("bounded Markdown row update retry loop returns or panics");
}

fn init_perf_tracing() {
    if std::env::var_os("LIX_MARKDOWN_GIT_TRACE").is_some() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("lix_perf=debug")
            .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
            .with_test_writer()
            .try_init();
    }
}

async fn open_rocksdb_lix(path: &Path) -> Lix<RocksDB> {
    let storage = RocksDB::open(path.join(".lix")).expect("open Lix RocksDB storage");
    open_lix()
        .with_storage(storage)
        .await
        .expect("open Lix workspace")
}

async fn install_plugin<StorageImpl>(lix: &Lix<StorageImpl>, key: &str, archive: &[u8])
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    write_file(
        lix,
        &format!("/.lix/plugins/{key}.lixplugin"),
        archive.to_vec(),
    )
    .await;
}

async fn write_file<StorageImpl>(lix: &Lix<StorageImpl>, path: &str, data: Vec<u8>)
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "INSERT INTO lix_file (path, content) VALUES ($1, $2) \
         ON CONFLICT (path) DO UPDATE SET content = excluded.content",
        &[Value::Text(path.to_owned()), Value::Blob(data.into())],
    )
    .await
    .expect("write benchmark file");
}

async fn read_file<StorageImpl>(lix: &Lix<StorageImpl>, path: &str) -> Vec<u8>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "SELECT content FROM lix_file WHERE path = $1",
        &[Value::Text(path.to_owned())],
    )
    .await
    .expect("read benchmark file")
    .rows()[0]
        .get::<Vec<u8>>("content")
        .expect("benchmark file should be bytes")
}

async fn file_id_at_path<StorageImpl>(lix: &Lix<StorageImpl>, path: &str) -> String
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "SELECT id FROM lix_file WHERE path = $1",
        &[Value::Text(path.to_owned())],
    )
    .await
    .expect("query benchmark file ID")
    .rows()[0]
        .get::<String>("id")
        .expect("benchmark file ID should be text")
}

fn build_markdown_plugin_archive() -> Vec<u8> {
    let wasm_path = Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_MARKDOWN_plugin_markdown"));
    let wasm = fs::read(wasm_path).unwrap_or_else(|error| {
        panic!(
            "failed to read Markdown v2 component at {}: {error}",
            wasm_path.display()
        )
    });
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (path, bytes) in [
        (
            "manifest.json",
            include_str!("../../../plugins/markdown/manifest.json").as_bytes(),
        ),
        (
            "schema/markdown_node.json",
            include_str!("../../../plugins/markdown/schema/markdown_node.json").as_bytes(),
        ),
        ("plugin.wasm", wasm.as_slice()),
    ] {
        writer
            .start_file(path, options)
            .expect("start plugin entry");
        writer.write_all(bytes).expect("write plugin entry");
    }
    writer
        .finish()
        .expect("finish Markdown plugin archive")
        .into_inner()
}

fn lix_repo_sizes(root: &Path) -> RepoSizes {
    RepoSizes {
        total_bytes: directory_bytes(root),
        metadata_bytes: directory_bytes(&root.join(".lix")),
    }
}

fn directory_bytes(path: &Path) -> u64 {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return 0;
    };
    if metadata.is_file() {
        return metadata.len();
    }
    if !metadata.is_dir() {
        return 0;
    }
    fs::read_dir(path)
        .expect("read benchmark directory")
        .map(|entry| directory_bytes(&entry.expect("read benchmark entry").path()))
        .sum()
}

fn print_duration_metric(operation: &str, system: &str, samples: &[Duration]) {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    eprintln!(
        "markdown_git_bench duration operation={operation} system={system} samples={} \
         p50_ms={:.3} p95_ms={:.3}",
        sorted.len(),
        percentile(&sorted, 50).as_secs_f64() * 1_000.0,
        percentile(&sorted, 95).as_secs_f64() * 1_000.0,
    );
}

fn print_storage_metric(state: &str, system: &str, sizes: &RepoSizes) {
    eprintln!(
        "markdown_git_bench storage state={state} system={system} total_bytes={} metadata_bytes={}",
        sizes.total_bytes, sizes.metadata_bytes,
    );
}

fn percentile(sorted: &[Duration], percentile: usize) -> Duration {
    let index = ((sorted.len() * percentile).div_ceil(100)).saturating_sub(1);
    sorted[index]
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn assert_same_bytes(label: &str, actual: &[u8], expected: &[u8]) {
    let mismatch = actual
        .iter()
        .zip(expected)
        .position(|(actual, expected)| actual != expected);
    assert!(
        mismatch.is_none() && actual.len() == expected.len(),
        "{label}: first_mismatch={mismatch:?} actual_len={} expected_len={} \
         actual_byte={:?} expected_byte={:?}",
        actual.len(),
        expected.len(),
        mismatch.and_then(|index| actual.get(index)),
        mismatch.and_then(|index| expected.get(index)),
    );
}
