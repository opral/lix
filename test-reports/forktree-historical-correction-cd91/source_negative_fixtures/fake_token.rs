fn plan_scan() {
    // provider.forktree_reader is only a fake token in this body.
    let _text = "provider.forktree_reader";
    other_reader.latest_checkpoint_for_branch(head, branch);
    load_rows(&other_reader, checkpoint, head);
}
