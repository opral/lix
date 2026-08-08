fn plan_scan(&self) {
    let other_reader = self.other_reader.clone();
    scan_row_source(
        schema,
        (self.branch_ref.clone(), other_reader, route),
        move |(_branch_ref, historical, route)| async move {
            let _ = self.forktree_reader.clone();
            let _ = historical.latest_checkpoint_for_branch(head, branch).await;
            let _ = load_rows(&historical, checkpoint, head, branch).await;
            let _ = route;
        },
    );
}
