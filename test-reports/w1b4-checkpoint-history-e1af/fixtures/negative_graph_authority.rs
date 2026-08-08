fn execute_checkpoint_selection(&mut self) {
    let operation_view = self.forktree_read_facade();
    let graph: Box<dyn CommitGraphReader> = self.commit_graph();
    let history = operation_view.checkpoint_history_from_head(head, branch);
    let diff = operation_view.diff_state_rows_between_commits(previous, head);
    let _ = (graph, history, diff);
}
