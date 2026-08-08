// MUST fail: adapters cannot forge an engine-declared raw storage space.
use lix::storage::{SpaceId, StorageSpace};

fn main() {
    let _ = SpaceId(7);
    let _ = StorageSpace::mutable(SpaceId(7), "forged");
}
