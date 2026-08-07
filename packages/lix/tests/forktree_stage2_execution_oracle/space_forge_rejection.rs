//! Compile-fail probe for equivalent-token object-space mutation.

extern crate lix;

use lix::storage::{PutBatch, SpaceId, StorageSpace, StorageWrite};

#[allow(dead_code)]
fn smuggle_raw_object_space<W: StorageWrite>(write: &mut W, batch: PutBatch) {
    let equivalent = StorageSpace::immutable(SpaceId(0x0009_0001), "forktree.object.v1");
    drop(write.put_many(equivalent, batch));
    drop(write.delete_many(equivalent, &[]));
}

fn main() {}
