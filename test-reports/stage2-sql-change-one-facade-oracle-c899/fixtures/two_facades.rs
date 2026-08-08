fn read_session() {
    let query_source = ForkTreeReadFacade::new(read_scope.clone());
    let history_source = ForkTreeReadFacade::new(read_scope);
    register_lix_change(query_source.clone());
    register_lix_diff(history_source);
}
