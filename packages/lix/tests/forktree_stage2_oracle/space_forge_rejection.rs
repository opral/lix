//! This is a compile-fail source, not a Cargo integration test.
//!
//! The first runnable hard-cut head must reject this external consumer. It
//! proves that hiding only the spelling `OBJECT_SPACE` is insufficient: an
//! equivalent raw descriptor must also be impossible to construct and pass to
//! generic storage mutation methods.

extern crate lix;

use lix::storage::{PutBatch, SpaceId, StorageSpace, StorageWrite};

#[allow(dead_code)]
fn smuggle_raw_object_space<W: StorageWrite>(write: &mut W, batch: PutBatch) {
    let equivalent = StorageSpace::immutable(SpaceId(0x0009_0001), "forktree.object.v1");
    drop(write.put_many(equivalent, batch));
    drop(write.delete_many(equivalent, &[]));
}

fn main() {}
