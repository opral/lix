// MUST fail with E0599: binary-CAS ownership is not exposed as an adapter read
// method or forgeable storage space.
use lix::storage::{StorageSpace, ValueSemantics};
use lix::storage_adapter::StorageAdapterRead;

fn probe<R: StorageAdapterRead>(read: &R) {
    let _ = read.load_binary_cas_manifest();
}

fn main() {
    let _ = StorageSpace::binary_cas(ValueSemantics::Immutable);
}
