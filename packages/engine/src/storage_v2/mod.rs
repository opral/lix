//! Experimental v2 storage adapter.
//!
//! This module is the Lix-neutral layer between domain stores and
//! `backend_v2`. Domain stores own schemas and key layouts; storage_v2 owns
//! shared scopes, batching, lowering, cursors, and adapter stats.

mod context;
mod point;
mod read_scope;
mod reader;
mod scan;
mod spaces;
mod stats;
mod write_set;

#[cfg(test)]
mod conformance;

pub use context::StorageContext;
pub(crate) use point::{
    get_many_borrowed_indexed_values_for_physical_plan,
    get_many_borrowed_indexed_values_for_physical_plan_with_stats,
    get_many_borrowed_indexed_values_for_plan,
    get_many_borrowed_indexed_values_for_plan_with_stats, get_many_caller_order,
    get_many_caller_order_with_stats, get_many_indexed_values_caller_order,
    get_many_indexed_values_caller_order_with_stats, get_many_indexed_values_for_physical_plan,
    get_many_indexed_values_for_physical_plan_into,
    get_many_indexed_values_for_physical_plan_into_with_stats,
    get_many_indexed_values_for_physical_plan_with_stats, get_many_indexed_values_for_plan,
    get_many_indexed_values_for_plan_into, get_many_indexed_values_for_plan_into_with_stats,
    get_many_indexed_values_for_plan_with_stats, get_many_values_caller_order,
    get_many_values_caller_order_with_stats, visit_unique_point_values_for_physical_plan,
    visit_unique_point_values_for_plan,
};
pub use point::{
    BorrowedIndexedPointValues, BufferedIndexedPointValues, IndexedPointValues,
    PhysicalPointRequestPlan, PointRequestPlan, PointSlot, PointValueBuffer, RequestedToUnique,
    RequestedToUniqueRef,
};
pub use read_scope::StorageReadScope;
pub use reader::StorageReader;
pub(crate) use scan::{
    scan_prefix, scan_prefix_into, scan_prefix_with_stats, scan_range, scan_range_into,
    scan_range_with_stats, visit_scan_prefix, visit_scan_range,
};
pub use scan::{BorrowedScanChunk, StorageScanBuffer};
pub(crate) use spaces::decode_logical_key_ref;
pub use spaces::StorageSpace;
pub use stats::{StorageReadResult, StorageReadStats, StorageWriteSetStats};
pub use write_set::{StorageWriteSet, StorageWriteSetError};
