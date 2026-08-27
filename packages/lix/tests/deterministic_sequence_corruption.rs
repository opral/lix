mod adapter_deterministic_sequence_corruption;

use lix::storage::Memory;

#[tokio::test]
async fn memory_deterministic_sequence_member_closure_fails_closed() {
    let initial = Memory::new();
    adapter_deterministic_sequence_corruption::initialize_with_deterministic_mode(initial.clone())
        .await;
    let initial = initial
        .fork()
        .expect("initial deterministic storage should fork");
    adapter_deterministic_sequence_corruption::assert_next_uuid(initial.clone(), "000000000000")
        .await;
    let valid = initial
        .fork()
        .expect("published sequence storage should fork");
    adapter_deterministic_sequence_corruption::assert_next_uuid(valid, "000000000001").await;

    let corrupt = initial.fork().expect("corruption fixture should fork");
    adapter_deterministic_sequence_corruption::replace_selected_sequence_member_with_unrelated(
        &corrupt,
    )
    .await;
    let corrupt = corrupt
        .fork()
        .expect("corrupt sequence storage should fork");
    adapter_deterministic_sequence_corruption::assert_missing_sequence_member_fails_closed(corrupt)
        .await;
}
