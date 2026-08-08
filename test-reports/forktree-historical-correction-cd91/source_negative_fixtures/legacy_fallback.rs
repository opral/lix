fn plan_scan() {
    let historical = provider.forktree_reader.clone();
    TrackedStateContext::new().reader(store);
    historical.latest_checkpoint_for_branch(head, branch);
    load_rows(&historical, checkpoint, head);
}
