fn plan_scan() {
    let historical = ForkTreeReadFacade::new(store);
    historical.latest_checkpoint_for_branch(head, branch);
    load_rows(&historical, checkpoint, head);
}
