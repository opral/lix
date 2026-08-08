fn operation() {
    let view = begin_read();
    let publication = PreparedPublication::new(view);
    let other = PreparedPublication::new(view);
    let plan = publication.into_storage_plan();
    let prepared = prepare_write_set(plan);
    prepared_commit.commit();
}
