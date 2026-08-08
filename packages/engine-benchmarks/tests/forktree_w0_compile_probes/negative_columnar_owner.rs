// MUST fail with E0599: these are real public crate-root storage APIs, but the
// deleted columnar owner is not represented by a reader method or space forge.
use lix::storage::{StorageSpace, ValueSemantics};
use lix::storage_adapter::StorageAdapterRead;

fn probe<R: StorageAdapterRead>(read: &R) {
    let _ = read.load_columnar_row_group();
}

fn main() {
    let _ = StorageSpace::columnar(ValueSemantics::Immutable);
}
