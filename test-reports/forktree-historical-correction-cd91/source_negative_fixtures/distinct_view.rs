fn plan_scan() {
    let historical = other_provider.forktree_reader.clone();
    historical.latest_checkpoint_for_branch(head, branch);
    load_rows(&historical, checkpoint, head);
}
