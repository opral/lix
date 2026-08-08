fn execute_checkpoint_selection(&mut self) {
    let chronology_view = self.forktree_read_facade();
    let state_view = self.forktree_read_facade();
    let history = chronology_view.checkpoint_history_from_head(head, branch);
    let diff = state_view.diff_state_rows_between_commits(previous, head);
    let _ = (history, diff);
}
