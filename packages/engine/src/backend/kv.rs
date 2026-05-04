/// One key/value pair returned by a backend KV scan.
///
/// Keys and values are byte-oriented on purpose. Higher layers own encoding,
/// ordering, and schema decisions so storage can move from SQLite to a prolly
/// tree without changing higher-level callers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendKvPair {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl BackendKvPair {
    pub fn new(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendKvGetGroup {
    pub namespace: String,
    pub keys: Vec<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendKvGetResult {
    pub groups: Vec<BackendKvGetResultGroup>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendKvGetResultGroup {
    pub namespace: String,
    pub values: Vec<Option<Vec<u8>>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendKvScanRequest {
    pub namespace: String,
    pub range: BackendKvScanRange,
    pub after: Option<Vec<u8>>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendKvScanResult {
    pub rows: Vec<BackendKvPair>,
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
