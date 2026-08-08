use super::view::{ForkTreeReadFacade, OpeningStorageRead};

pub(crate) fn authenticated_serving_boundary(read: &OpeningStorageRead) {
    let facade = ForkTreeReadFacade::new(read);
    facade.load_semantic_row("file-a");
}
