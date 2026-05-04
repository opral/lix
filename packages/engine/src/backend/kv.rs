/// Compact logical batch of backend KV rows.
///
/// Keys and values are byte-oriented on purpose. Higher layers own encoding,
/// ordering, and schema decisions so storage can move from SQLite to a prolly
/// tree without changing higher-level callers.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BackendKvRowBatch {
    rows: Vec<BackendKvRow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BackendKvRow {
    key: Vec<u8>,
    exists: bool,
    value: Option<Vec<u8>>,
}

impl BackendKvRowBatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            rows: Vec::with_capacity(capacity),
        }
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn key(&self, index: usize) -> Option<&[u8]> {
        self.rows.get(index).map(|row| row.key.as_slice())
    }

    pub fn exists(&self, index: usize) -> bool {
        self.rows.get(index).is_some_and(|row| row.exists)
    }

    pub fn value(&self, index: usize) -> Option<&[u8]> {
        self.rows.get(index).and_then(|row| row.value.as_deref())
    }

    pub fn set_exists(&mut self, index: usize) {
        if let Some(row) = self.rows.get_mut(index) {
            row.exists = true;
        }
    }

    pub fn set_value(&mut self, index: usize, value: Vec<u8>) {
        if let Some(row) = self.rows.get_mut(index) {
            row.exists = true;
            row.value = Some(value);
        }
    }

    pub fn clear_values(&mut self) {
        for row in &mut self.rows {
            row.value = None;
        }
    }

    pub fn remove(&mut self, index: usize) {
        self.rows.remove(index);
    }

    pub fn truncate(&mut self, len: usize) {
        self.rows.truncate(len);
    }

    pub fn last_key_cloned(&self) -> Option<Vec<u8>> {
        self.rows.last().map(|row| row.key.clone())
    }

    pub fn push_missing(&mut self, key: impl Into<Vec<u8>>) {
        self.rows.push(BackendKvRow {
            key: key.into(),
            exists: false,
            value: None,
        });
    }

    pub fn push_exists(&mut self, key: impl Into<Vec<u8>>) {
        self.rows.push(BackendKvRow {
            key: key.into(),
            exists: true,
            value: None,
        });
    }

    pub fn push_value(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) {
        self.rows.push(BackendKvRow {
            key: key.into(),
            exists: true,
            value: Some(value.into()),
        });
    }

    pub fn push_key_only(&mut self, key: impl Into<Vec<u8>>) {
        self.rows.push(BackendKvRow {
            key: key.into(),
            exists: true,
            value: None,
        });
    }

    pub fn pop_value(&mut self) -> Option<Vec<u8>> {
        self.rows.pop().and_then(|row| row.value)
    }

    pub fn into_parts(self) -> Vec<(Vec<u8>, bool, Option<Vec<u8>>)> {
        self.rows
            .into_iter()
            .map(|row| (row.key, row.exists, row.value))
            .collect()
    }

    pub fn push_scan_projection(
        &mut self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
        projection: BackendKvScanProjection,
    ) {
        match projection {
            BackendKvScanProjection::KeysOnly => self.push_key_only(key),
            BackendKvScanProjection::KeysAndValues => self.push_value(key, value),
        }
    }

    pub fn push_get_projection(
        &mut self,
        key: impl Into<Vec<u8>>,
        value: Option<Vec<u8>>,
        projection: BackendKvGetProjection,
    ) {
        match (value, projection) {
            (Some(value), BackendKvGetProjection::Values) => self.push_value(key, value),
            (Some(_), BackendKvGetProjection::Existence) => self.push_exists(key),
            (None, _) => self.push_missing(key),
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
    pub rows: BackendKvRowBatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendKvGetProjection {
    Values,
    Existence,
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
    pub rows: BackendKvRowBatch,
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
