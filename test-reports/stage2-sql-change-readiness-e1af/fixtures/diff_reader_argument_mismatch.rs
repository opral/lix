fn register_diff_function(query_source: SqlChangelogQuerySource<Read>) {
    let _ = DiffFunction {
        forktree_reader: query_source.forktree_reader.clone(),
    };
}

struct DiffFunction {
    forktree_reader: Reader,
}

impl DiffFunction {
    fn call(&self) {
        let _ = DiffSpec {
            forktree_reader: self.forktree_reader.clone(),
        };
    }
}

struct DiffSpec {
    forktree_reader: Reader,
}

impl DiffSpec {
    async fn plan_scan(&self) {
        scan_row_source(
            (self.forktree_reader.clone(), other_reader),
            move |(forktree_reader, other_reader)| async move {
                let _ = other_reader.scan_state_rows_at_commit("from").await;
                let _ = other_reader.scan_state_rows_at_commit("to").await;
            },
        );
    }
}
