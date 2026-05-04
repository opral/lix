/// One row returned by a backend KV scan.
///
/// Keys and values are byte-oriented on purpose. Higher layers own encoding,
/// ordering, and schema decisions so storage can move from SQLite to a prolly
/// tree without changing higher-level callers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendKvScanRow {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

impl BackendKvScanRow {
    pub fn new(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            key: key.into(),
            value: Some(value.into()),
        }
    }

    pub fn key_only(key: impl Into<Vec<u8>>) -> Self {
        Self {
            key: key.into(),
            value: None,
        }
    }

    pub fn for_projection(
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
        projection: BackendKvScanProjection,
    ) -> Self {
        match projection {
            BackendKvScanProjection::KeysOnly => Self::key_only(key),
            BackendKvScanProjection::KeysAndValues => Self::new(key, value),
        }
    }
}

/// Ordered byte range for backend KV scans.
///
/// Ranges are half-open: `start <= key < end`. `Prefix` is explicit because it
/// is a common access pattern and lets each backend choose the safest
/// implementation for its storage engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendKvScanRange {
    Prefix(Vec<u8>),
    Range { start: Vec<u8>, end: Vec<u8> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendKvGetRequest {
    pub groups: Vec<BackendKvGetGroup>,
    pub projection: BackendKvGetProjection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendKvGetGroup {
    pub namespace: String,
    pub keys: Vec<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendKvGetBatch {
    pub groups: Vec<BackendKvGetBatchGroup>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendKvGetBatchGroup {
    pub namespace: String,
    pub entries: Vec<BackendKvGetEntry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendKvGetProjection {
    Values,
    Existence,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendKvGetEntry {
    pub exists: bool,
    pub value: Option<Vec<u8>>,
}

impl BackendKvGetEntry {
    pub fn missing() -> Self {
        Self {
            exists: false,
            value: None,
        }
    }

    pub fn value(value: impl Into<Vec<u8>>) -> Self {
        Self {
            exists: true,
            value: Some(value.into()),
        }
    }

    pub fn exists() -> Self {
        Self {
            exists: true,
            value: None,
        }
    }

    pub fn for_projection(value: Option<Vec<u8>>, projection: BackendKvGetProjection) -> Self {
        match (value, projection) {
            (Some(value), BackendKvGetProjection::Values) => Self::value(value),
            (Some(_), BackendKvGetProjection::Existence) => Self::exists(),
            (None, _) => Self::missing(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendKvScanProjection {
    KeysOnly,
    KeysAndValues,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendKvScanRequest {
    pub namespace: String,
    pub range: BackendKvScanRange,
    pub after: Option<Vec<u8>>,
    pub limit: usize,
    pub projection: BackendKvScanProjection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendKvScanBatch {
    pub rows: Vec<BackendKvScanRow>,
    pub resume_after: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BackendKvWriteBatch {
    pub groups: Vec<BackendKvWriteGroup>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendKvWriteGroup {
    pub namespace: String,
    pub puts: Vec<BackendKvPut>,
    pub deletes: Vec<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendKvPut {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BackendKvWriteStats {
    pub puts: usize,
    pub deletes: usize,
    pub bytes_written: usize,
}

impl BackendKvScanRange {
    pub fn prefix(prefix: impl Into<Vec<u8>>) -> Self {
        Self::Prefix(prefix.into())
    }

    pub fn range(start: impl Into<Vec<u8>>, end: impl Into<Vec<u8>>) -> Self {
        Self::Range {
            start: start.into(),
            end: end.into(),
        }
    }
}
