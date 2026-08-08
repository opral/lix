fn plan_scan(&self) {
    scan_row_source(
        schema,
        (self.branch_ref.clone(), self.forktree_reader.clone(), route),
        move |(_branch_ref, historical, route)| async move {
            let other_reader = self.other_reader.clone();
            let _ = other_reader
                .latest_checkpoint_for_branch(head, branch)
                .await;
            let _ = load_rows(&other_reader, checkpoint, head, branch).await;
            let _ = route;
        },
    );
}
