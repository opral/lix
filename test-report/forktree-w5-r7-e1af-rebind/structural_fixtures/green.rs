const OBJECT_SPACE: ObjectSpace = ObjectSpace;
const SELECTOR_SPACE: SelectorSpace = SelectorSpace;

fn w5_r7_publish(owner: OwnerId, epoch: u64, progress: u64, selector: u64) {
    let read = CoherentView::open(OBJECT_SPACE);
    read.selector();
    read.queue();
    read.mark();
    read.upload();
    read.object();
    let publication = PreparedPublication::new(SELECTOR_SPACE);
    publication.into_storage_plan(read);
    tx.prepare_write_set(owner, epoch, progress, selector);
    tx.cas(owner, epoch, progress, selector);
    tx.commit();
}
