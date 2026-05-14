use crate::backend_v2::PredicateSupportLevel;

#[derive(Clone, Debug)]
pub struct BackendCapabilities {
    pub profile: BackendProfile,
    pub projection: ProjectionCapabilities,
    pub scan: ScanCapabilities,
    pub write: WriteCapabilities,
    pub pushdown: PushdownCapabilities,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendProfile {
    /// Ordered byte keys, coherent read views, paged forward scans,
    /// caller-order get_many, readable writes with read-your-writes, and atomic
    /// write commit.
    V0 { write_concurrency: WriteConcurrency },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteConcurrency {
    SingleWriter,
    ConcurrentWithConflictDetection,
    ConcurrentWithoutConflictDetection,
}

#[derive(Clone, Debug, Default)]
pub struct ProjectionCapabilities {
    pub header: bool,
    pub refs: bool,
    pub header_and_refs: bool,
    pub payload: bool,
}

#[derive(Clone, Debug, Default)]
pub struct ScanCapabilities {
    pub native_prefix_scan: bool,
    pub reverse: bool,
    pub limit_bytes: bool,
    pub unordered_points: bool,
    pub key_ordered_points: bool,
    pub long_lived_cursors: bool,
    pub parallel_partitions: bool,
}

#[derive(Clone, Debug, Default)]
pub struct WriteCapabilities {
    pub delete_range: bool,
    pub preconditions: bool,
    pub idempotent_commit: bool,
}

#[derive(Clone, Debug, Default)]
pub struct PushdownCapabilities {
    pub key: PredicateSupportLevel,
    pub header: PredicateSupportLevel,
    pub refs: PredicateSupportLevel,
    pub object_pruning: PredicateSupportLevel,
}

impl BackendCapabilities {
    pub fn v0(write_concurrency: WriteConcurrency) -> Self {
        Self {
            profile: BackendProfile::V0 { write_concurrency },
            projection: ProjectionCapabilities::default(),
            scan: ScanCapabilities::default(),
            write: WriteCapabilities::default(),
            pushdown: PushdownCapabilities::default(),
        }
    }
}
