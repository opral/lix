#![allow(dead_code)]

/// One key/value pair returned by a backend KV scan.
///
/// Keys and values are byte-oriented on purpose. Higher layers own encoding,
/// ordering, and schema decisions so storage can move from SQLite to a prolly
/// tree without changing higher-level callers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvPair {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl KvPair {
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
pub enum KvScanRange {
    Prefix(Vec<u8>),
    Range { start: Vec<u8>, end: Vec<u8> },
}

impl KvScanRange {
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
