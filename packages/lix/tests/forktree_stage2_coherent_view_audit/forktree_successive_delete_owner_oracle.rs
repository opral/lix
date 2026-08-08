//! Test-only acceptance contract for the ForkTree 65-row successive-delete collapse.
//!
//! This file deliberately depends on a sealed test adapter instead of ForkTree internals.
//! The production writer owner maps that adapter to the first accepted writer milestone.
//! No raw space, ObjectId, selector, or mutable working-set handle may cross the adapter.

#![allow(async_fn_in_trait)]

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Adapter {
    RocksDb,
    SlateDb,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CollapseSiblingFault {
    Missing,
    Malformed,
    WrongKind,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OwnerCounters {
    /// Counters cover mutation preparation/publication only. Verification and
    /// cold-reopen reads are recorded by the harness separately.
    /// One caller-owned read per publication attempt. Tree helpers add zero.
    pub publication_begin_reads: u64,
    /// Full-value reads in the authoritative immutable ordered-tree object space.
    pub tree_get_calls: u64,
    pub tree_get_keys: u64,
    /// Reachable newly encoded ordered-tree nodes, after operation-local pruning.
    pub tree_node_puts: u64,
    pub publication_begin_writes: u64,
    pub publication_commits: u64,
    pub selector_writes: u64,
}

/// Sealed test facade supplied by the production writer owner.
///
/// `arm_collapse_sibling_fault` is an owner-internal read interceptor/fixture hook. It
/// identifies the authenticated sole sibling by role; callers never receive its ObjectId.
pub trait SuccessiveDeleteOwner {
    async fn select_adapter_and_seed(&mut self, adapter: Adapter, rows: usize);
    async fn delete_batch(&mut self, first_index: usize, count: usize) -> Result<(), String>;
    async fn row_indices(&mut self) -> Result<Vec<usize>, String>;
    async fn cold_reopen(&mut self) -> Result<(), String>;
    async fn arm_collapse_sibling_fault(&mut self, fault: CollapseSiblingFault);
    fn reset_counters(&mut self);
    fn counters(&self) -> OwnerCounters;
}

pub async fn run_successive_delete_owner_oracle(owner: &mut impl SuccessiveDeleteOwner) {
    for adapter in [Adapter::RocksDb, Adapter::SlateDb] {
        run_64_sequential(owner, adapter).await;
        run_65_sequential(owner, adapter).await;
        run_65_batch_control(owner, adapter).await;
        for fault in [
            CollapseSiblingFault::Missing,
            CollapseSiblingFault::Malformed,
            CollapseSiblingFault::WrongKind,
        ] {
            run_fail_closed_control(owner, adapter, fault).await;
        }
    }
}

async fn run_64_sequential(owner: &mut impl SuccessiveDeleteOwner, adapter: Adapter) {
    owner.select_adapter_and_seed(adapter, 64).await;
    owner.reset_counters();
    for index in 0..64 {
        owner
            .delete_batch(index, 1)
            .await
            .expect("64-row one-leaf delete must commit");
    }
    assert_eq!(owner.row_indices().await.expect("read 64-row result"), []);
    owner.cold_reopen().await.expect("reopen 64-row result");
    assert_eq!(owner.row_indices().await.expect("cold 64-row result"), []);
    assert_eq!(
        owner.counters(),
        OwnerCounters {
            publication_begin_reads: 64,
            tree_get_calls: 64,
            tree_get_keys: 64,
            tree_node_puts: 64,
            publication_begin_writes: 64,
            publication_commits: 64,
            selector_writes: 64,
        }
    );
}

async fn run_65_sequential(owner: &mut impl SuccessiveDeleteOwner, adapter: Adapter) {
    owner.select_adapter_and_seed(adapter, 65).await;
    owner.reset_counters();
    for index in 0..65 {
        owner
            .delete_batch(index, 1)
            .await
            .expect("65-row successive delete must commit");
    }
    assert_eq!(owner.row_indices().await.expect("read 65-row result"), []);
    owner.cold_reopen().await.expect("reopen 65-row result");
    assert_eq!(owner.row_indices().await.expect("cold 65-row result"), []);
    // The accepted fixed-block model's first split is 64 + 1. Deletes 0..62
    // each read and copy root+left leaf. Delete 63 reads root+left
    // leaf+unchanged sibling, authenticates that sibling, and emits no new
    // tree node. Delete 64 reads/copies the now-root leaf.
    assert_eq!(
        owner.counters(),
        OwnerCounters {
            publication_begin_reads: 65,
            tree_get_calls: 130,
            tree_get_keys: 130,
            tree_node_puts: 127,
            publication_begin_writes: 65,
            publication_commits: 65,
            selector_writes: 65,
        }
    );
}

async fn run_65_batch_control(owner: &mut impl SuccessiveDeleteOwner, adapter: Adapter) {
    owner.select_adapter_and_seed(adapter, 65).await;
    owner.reset_counters();
    owner
        .delete_batch(0, 65)
        .await
        .expect("65-row b100 control must commit");
    assert_eq!(owner.row_indices().await.expect("read b100 result"), []);
    owner.cold_reopen().await.expect("reopen b100 result");
    assert_eq!(owner.row_indices().await.expect("cold b100 result"), []);
    assert_eq!(
        owner.counters(),
        OwnerCounters {
            publication_begin_reads: 1,
            tree_get_calls: 2,
            tree_get_keys: 3,
            tree_node_puts: 1,
            publication_begin_writes: 1,
            publication_commits: 1,
            selector_writes: 1,
        }
    );
}

async fn run_fail_closed_control(
    owner: &mut impl SuccessiveDeleteOwner,
    adapter: Adapter,
    fault: CollapseSiblingFault,
) {
    owner.select_adapter_and_seed(adapter, 65).await;
    for index in 0..63 {
        owner
            .delete_batch(index, 1)
            .await
            .expect("pre-collapse delete must commit");
    }
    owner.arm_collapse_sibling_fault(fault).await;
    owner.reset_counters();
    let error = owner
        .delete_batch(63, 1)
        .await
        .expect_err("invalid unchanged sibling must fail closed");
    assert!(
        error.contains("corrupt")
            || error.contains("absent")
            || error.contains("kind")
            || error.contains("authentication"),
        "fault must remain an explicit authentication/corruption error: {error}"
    );
    assert_eq!(
        owner.counters(),
        OwnerCounters {
            publication_begin_reads: 1,
            tree_get_calls: 3,
            tree_get_keys: 3,
            tree_node_puts: 0,
            publication_begin_writes: 0,
            publication_commits: 0,
            selector_writes: 0,
        }
    );
    assert_eq!(
        owner.row_indices().await.expect("read after rejected collapse"),
        (63..65).collect::<Vec<_>>()
    );
    owner
        .cold_reopen()
        .await
        .expect("reopen after rejected collapse");
    assert_eq!(
        owner.row_indices().await.expect("cold rejected-collapse state"),
        (63..65).collect::<Vec<_>>()
    );
}
