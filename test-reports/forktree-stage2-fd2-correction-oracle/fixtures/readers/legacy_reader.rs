fn plan_scan(&self) {
    scan_row_source(
        schema,
        (self.branch_ref.clone(), self.forktree_reader.clone(), route),
        move |(_branch_ref, historical, route)| async move {
            let _ = TrackedStateStoreReader::diff_commits(&historical, before, after).await;
            let _ = historical.latest_checkpoint_for_branch(head, branch).await;
            let _ = load_rows(&historical, checkpoint, head, branch).await;
            let _ = route;
        },
    );
}
