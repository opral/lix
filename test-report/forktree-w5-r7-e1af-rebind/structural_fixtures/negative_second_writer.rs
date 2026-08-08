const OBJECT_SPACE: ObjectSpace = ObjectSpace;
const SELECTOR_SPACE: SelectorSpace = SelectorSpace;
struct StorageRead;
struct CoherentView;

fn w5_r7_publish(read: &StorageRead, owner: OwnerId, epoch: u64, progress: u64, selector: u64) {
    let view = CoherentView::open(read);
    view.owner;
    view.view_id;
    view.snapshot;
    view.selector();
    view.queue();
    view.mark();
    view.upload();
    view.object();
    let first = PreparedPublication::new(&view);
    let second = PreparedPublication::new(&view);
    first.into_storage_plan(&view);
    second.into_storage_plan(&view);
    tx.prepare_write_set(owner, epoch, progress, selector);
    tx.cas(owner, epoch, progress, selector);
    tx.commit();
}
