use std::time::Instant;

use lix::Memory;

use super::model::{DiffAccounting, ForkTree, Mutation, RelationalValue};
use super::{
    CountingStorage, begin_allocation_profile, end_allocation_profile, process_cpu_nanos,
    process_resident_bytes, take_stats,
};

const ROWS: usize = 10_000;
const BRANCHES: usize = 100;
const EDITED_BRANCHES: usize = 10;
const ROWS_PER_EDIT: usize = 100;

pub(super) async fn run() {
    let memory = Memory::new();
    let heads = {
        let (storage, _) = CountingStorage::new(memory.clone());
        let tree = ForkTree::new(storage);
        let base = tree
            .initialize(&initial_rows())
            .await
            .expect("initialize memory cold-diff tree");
        let mut heads = Vec::new();
        for branch in 0..BRANCHES {
            let name = branch_name(branch);
            tree.create_branch(&name, Some(base))
                .await
                .expect("create memory cold-diff branch");
            if branch < EDITED_BRANCHES {
                let (head, _) = tree
                    .apply_sorted_mutations_on(&name, &branch_mutations(branch))
                    .await
                    .expect("edit memory cold-diff branch");
                heads.push(head);
            }
        }
        (base, heads)
    };
    let snapshot = memory
        .export_snapshot()
        .expect("export memory cold-diff snapshot");
    let reopened = Memory::from_snapshot(&snapshot).expect("reopen memory cold-diff snapshot");
    let (storage, stats) = CountingStorage::new(reopened);
    let tree = ForkTree::new(storage);
    let _ = take_stats(&stats);
    let before_cpu = process_cpu_nanos();
    let before_rss = process_resident_bytes();
    begin_allocation_profile();
    let started = Instant::now();
    let mut changes = 0usize;
    let mut accounting = DiffAccounting::default();
    for head in heads.1 {
        let (diff, one) = tree
            .diff_commits_profiled(heads.0, head)
            .await
            .expect("memory cold diff");
        changes += diff.len();
        add_accounting(&mut accounting, one);
    }
    let wall_nanos = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
    let (allocated_bytes, allocation_calls) = end_allocation_profile();
    let cpu_nanos = process_cpu_nanos().saturating_sub(before_cpu);
    let after_rss = process_resident_bytes();
    let io = take_stats(&stats);
    assert_eq!(changes, EDITED_BRANCHES * ROWS_PER_EDIT);
    println!(
        "branch_diff_memory,rows={ROWS},branches={BRANCHES},edited_branches={EDITED_BRANCHES},changes={changes},wall_nanos={wall_nanos},cpu_nanos={cpu_nanos},allocated_bytes={allocated_bytes},allocation_calls={allocation_calls},rss_before_bytes={before_rss},rss_after_bytes={after_rss},begin_reads={},get_calls={},get_keys={},get_value_bytes={},hash_pruned_nodes={},decoded_nodes={},node_batches={},node_objects={},value_batches={},unique_value_packs={},authenticated_bytes={},commit_read_nanos={},node_read_nanos={},node_decode_nanos={},value_read_nanos={},value_decode_nanos={}",
        io.begin_reads,
        io.get_calls,
        io.get_keys,
        io.get_value_bytes,
        accounting.hash_pruned_nodes,
        accounting.decoded_nodes,
        accounting.node_batches,
        accounting.node_objects,
        accounting.value_batches,
        accounting.unique_value_packs,
        accounting.authenticated_bytes,
        accounting.commit_read_nanos,
        accounting.node_read_nanos,
        accounting.node_decode_nanos,
        accounting.value_read_nanos,
        accounting.value_decode_nanos,
    );
}

fn initial_rows() -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..ROWS)
        .map(|index| {
            (
                row_key(index),
                format!("base-{index:08}-{}", "x".repeat(48)).into_bytes(),
            )
        })
        .collect()
}

fn branch_mutations(branch: usize) -> Vec<Mutation> {
    (0..ROWS_PER_EDIT)
        .map(|ordinal| Mutation::Update {
            key: row_key((ordinal + 1) * ROWS / (ROWS_PER_EDIT + 1)),
            value: RelationalValue::Bytes(
                format!("branch-{branch:08}-{}", "y".repeat(48)).into_bytes(),
            ),
        })
        .collect()
}

fn row_key(index: usize) -> Vec<u8> {
    format!("row-{index:08}").into_bytes()
}

fn branch_name(index: usize) -> String {
    format!("memory-diff-{index:04}")
}

fn add_accounting(total: &mut DiffAccounting, one: DiffAccounting) {
    total.changes += one.changes;
    total.hash_pruned_nodes += one.hash_pruned_nodes;
    total.decoded_nodes += one.decoded_nodes;
    total.commit_batches += one.commit_batches;
    total.commit_objects += one.commit_objects;
    total.node_batches += one.node_batches;
    total.node_objects += one.node_objects;
    total.value_batches += one.value_batches;
    total.value_references += one.value_references;
    total.unique_value_packs += one.unique_value_packs;
    total.authenticated_bytes += one.authenticated_bytes;
    total.commit_read_nanos += one.commit_read_nanos;
    total.node_read_nanos += one.node_read_nanos;
    total.node_decode_nanos += one.node_decode_nanos;
    total.value_read_nanos += one.value_read_nanos;
    total.value_decode_nanos += one.value_decode_nanos;
}
