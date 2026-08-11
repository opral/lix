use std::time::Instant;

use lix::changelog::bench::{append_ordered_linear_commits, stage_append_once};
use lix::storage_adapter::{Memory, StorageAdapter};
use lix::storage_bench::{
    CommitGraphBenchMode, read_commit_graph_for_bench, seed_commit_graph_members_for_bench,
};

fn main() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create commit-graph benchmark runtime")
        .block_on(run());
}

async fn run() {
    let args = std::env::args().collect::<Vec<_>>();
    let commits = parse_positive(args.get(1), "commits", 10_000);
    let samples = parse_positive(args.get(2), "samples", 11);
    let warmups = parse_nonnegative(args.get(3), "warmups", 3);
    let mode_filter = args.get(4).map(String::as_str);
    let append = append_ordered_linear_commits(commits).expect("build linear commit history");
    let commit_ids = append.commit_ids();
    let head_commit_id = commit_ids
        .last()
        .cloned()
        .expect("linear history has a head");
    let memory = Memory::new();
    stage_append_once(memory.clone(), &append)
        .await
        .expect("seed linear commit history");
    let storage = StorageAdapter::new(memory);
    seed_commit_graph_members_for_bench(&storage, &commit_ids)
        .await
        .expect("seed representative member payloads");

    for (name, mode) in [
        ("all_nodes", CommitGraphBenchMode::AllNodes),
        ("legacy_all_nodes", CommitGraphBenchMode::LegacyAllNodes),
        ("reachable_nodes", CommitGraphBenchMode::ReachableNodes),
        (
            "legacy_reachable_nodes",
            CommitGraphBenchMode::LegacyReachableNodes,
        ),
        ("history_full", CommitGraphBenchMode::HistoryFull),
        ("history_depth0", CommitGraphBenchMode::HistoryDepth0),
        ("history_limit10", CommitGraphBenchMode::HistoryLimit10),
    ] {
        if mode_filter.is_some_and(|filter| filter != name) {
            continue;
        }
        for _ in 0..warmups {
            std::hint::black_box(
                read_commit_graph_for_bench(&storage, &head_commit_id, mode)
                    .await
                    .expect("warm commit graph read"),
            );
        }
        let mut elapsed_us = Vec::with_capacity(samples);
        let mut result = None;
        for _ in 0..samples {
            let started = Instant::now();
            result = Some(
                read_commit_graph_for_bench(&storage, &head_commit_id, mode)
                    .await
                    .expect("measured commit graph read"),
            );
            elapsed_us.push(started.elapsed().as_secs_f64() * 1_000_000.0);
        }
        elapsed_us.sort_by(f64::total_cmp);
        let result = result.expect("positive sample count");
        let median_us = elapsed_us[elapsed_us.len() / 2];
        let p95_index = ((elapsed_us.len() - 1) * 95).div_ceil(100);
        println!(
            "commit_graph_scale,mode={name},commits={commits},samples={samples},median_us={median_us:.3},p95_us={:.3},nodes={},edges={},member_changes={}",
            elapsed_us[p95_index], result.nodes, result.edges, result.member_changes,
        );
    }
}

fn parse_positive(value: Option<&String>, name: &str, default: usize) -> usize {
    let parsed = parse_nonnegative(value, name, default);
    assert!(parsed > 0, "{name} must be positive");
    parsed
}

fn parse_nonnegative(value: Option<&String>, name: &str, default: usize) -> usize {
    value.map_or(default, |value| {
        value
            .parse::<usize>()
            .unwrap_or_else(|error| panic!("invalid {name} '{value}': {error}"))
    })
}
