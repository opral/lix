const OBJECT_SPACE: ObjectSpace = ObjectSpace;
const SELECTOR_SPACE: SelectorSpace = SelectorSpace;
struct StorageRead;
struct CoherentView;

fn w5_r7_publish(read: &StorageRead, owner: OwnerId, epoch: u64, progress: u64, selector: u64) {
    let view = CoherentView::open(read);
    view.owner;
    view.view_id;
    view.snapshot;
    let alias = view;
    view.selector();
    view.queue();
    view.mark();
    view.upload();
    view.object();
    let publication = PreparedPublication::new(&view);
    publication.into_storage_plan(&alias);
    tx.prepare_write_set(owner, epoch, progress, selector);
    tx.cas(owner, epoch, progress, selector);
    tx.commit();
}
