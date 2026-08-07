#[path = "adapter_deterministic_sequence_corruption.rs"]
mod deterministic_sequence_corruption;

use lix::storage::Memory;

#[tokio::test]
async fn memory_deterministic_sequence_member_closure_fails_closed() {
    let initial = Memory::new();
    deterministic_sequence_corruption::initialize_with_deterministic_mode(initial.clone()).await;
    let snapshot = initial
        .export_snapshot()
        .expect("initial deterministic storage should snapshot");
    drop(initial);
    let initial = Memory::from_snapshot(&snapshot).expect("initial storage should reopen");
    deterministic_sequence_corruption::assert_next_uuid(initial.clone(), "000000000000").await;
    let snapshot = initial
        .export_snapshot()
        .expect("published sequence should snapshot");
    drop(initial);
    let valid = Memory::from_snapshot(&snapshot).expect("published sequence should reopen");
    deterministic_sequence_corruption::assert_next_uuid(valid, "000000000001").await;

    let corrupt = Memory::from_snapshot(&snapshot).expect("corruption fixture should reopen");
    deterministic_sequence_corruption::replace_selected_sequence_member_with_unrelated(&corrupt)
        .await;
    let snapshot = corrupt
        .export_snapshot()
        .expect("corrupt sequence storage should snapshot");
    drop(corrupt);
    let corrupt = Memory::from_snapshot(&snapshot).expect("corrupt storage should reopen");
    deterministic_sequence_corruption::assert_missing_sequence_member_fails_closed(corrupt).await;
}
