fn execute_checkpoint_selection(&mut self) {
    let operation_view = self.forktree_read_facade();
    let history = operation_view.checkpoint_history_from_head(head, branch);
    let _ = history;
}
