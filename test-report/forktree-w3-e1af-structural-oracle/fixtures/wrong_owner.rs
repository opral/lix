fn operation<R>(read: &R, selector: SelectorExpect, owner: OwnerId, epoch: u64, metadata: Metadata, idempotency: Idempotency) {
    let view: CoherentView<R> = open_coherent_view_on_read(read, selector).await;
    let publication: PreparedPublication = PreparedPublication::from_view(&view, selector, other_owner, epoch);
    publication.bind_selector_epoch_owner_cas(selector, owner, epoch);
    let plan = publication.into_storage_plan(metadata, idempotency);
    let prepared = prepare_write_set(plan);
    prepared.commit();
}
