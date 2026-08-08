//! Fixed engine-minted descriptors for adapter conformance.
//!
//! This module deliberately has no arbitrary-ID constructor. Adapters may
//! inspect and use these descriptors in their own tests, but safe external
//! code cannot retarget one to a reserved engine-owned space.

use crate::storage::{StorageSpace, ValueSemantics};

macro_rules! fixed_space {
    ($name:ident, $id:expr, $label:literal, $semantics:ident) => {
        pub const $name: StorageSpace =
            StorageSpace::engine_declared($id, $label, ValueSemantics::$semantics);
    };
}

fixed_space!(MUTABLE_0, 0, "storage.conformance.mutable.0", Mutable);
fixed_space!(MUTABLE_1, 1, "storage.conformance.mutable.1", Mutable);
fixed_space!(MUTABLE_2, 2, "storage.conformance.mutable.2", Mutable);
fixed_space!(MUTABLE_7, 7, "storage.conformance.mutable.7", Mutable);
fixed_space!(MUTABLE_8, 8, "storage.conformance.mutable.8", Mutable);
fixed_space!(MUTABLE_9, 9, "storage.conformance.mutable.9", Mutable);
fixed_space!(MUTABLE_10, 10, "storage.conformance.mutable.10", Mutable);
fixed_space!(MUTABLE_11, 11, "storage.conformance.mutable.11", Mutable);
fixed_space!(MUTABLE_12, 12, "storage.conformance.mutable.12", Mutable);
fixed_space!(MUTABLE_13, 13, "storage.conformance.mutable.13", Mutable);
fixed_space!(
    MUTABLE_BYTE_PATTERN,
    0x0102_0304,
    "storage.conformance.mutable.bytes",
    Mutable
);
fixed_space!(
    MUTABLE_MAX,
    u32::MAX,
    "storage.conformance.mutable.max",
    Mutable
);
fixed_space!(
    MUTABLE_BENCH_1,
    0x00ff_0001,
    "storage.conformance.mutable.bench.1",
    Mutable
);
fixed_space!(IMMUTABLE_7, 7, "storage.conformance.immutable.7", Immutable);
fixed_space!(IMMUTABLE_9, 9, "storage.conformance.immutable.9", Immutable);
fixed_space!(
    IMMUTABLE_BENCH_1,
    0x00ff_0001,
    "storage.conformance.immutable.bench.1",
    Immutable
);
