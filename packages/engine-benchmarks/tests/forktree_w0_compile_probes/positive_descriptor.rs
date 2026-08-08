// Compile-pass probe for the actual public descriptor surface. Constructors
// remain engine-owned; consumers may receive and inspect a descriptor without
// being able to forge a space.
use lix::storage::{StorageSpace, ValueSemantics};

fn accepts_engine_descriptor(space: StorageSpace) -> ValueSemantics {
    space.value_semantics()
}

fn main() {
    let _ = accepts_engine_descriptor;
}
