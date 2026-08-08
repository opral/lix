// MUST fail with E0599: selector/legacy ownership is not represented by a
// public storage-adapter reader method or forgeable storage space.
use lix::storage::{StorageSpace, ValueSemantics};
use lix::storage_adapter::StorageAdapterRead;

fn probe<R: StorageAdapterRead>(read: &R) {
    let _ = read.load_branch_head_control();
}

fn main() {
    let _ = StorageSpace::branch_head_control(ValueSemantics::Mutable);
}
