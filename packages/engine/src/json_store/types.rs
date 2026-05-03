#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct JsonRef {
    hash: [u8; 32],
}

impl JsonRef {
    pub(crate) fn from_hash(hash: blake3::Hash) -> Self {
        Self {
            hash: *hash.as_bytes(),
        }
    }

    pub(crate) fn from_hash_bytes(hash: [u8; 32]) -> Self {
        Self { hash }
    }

    pub(crate) fn as_hash_bytes(&self) -> &[u8] {
        &self.hash
    }

    pub(crate) fn to_hex(&self) -> String {
        self.hash.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct JsonProjectionPath(String);

impl JsonProjectionPath {
    pub(crate) fn new(pointer: impl Into<String>) -> Self {
        Self(pointer.into())
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct JsonProjection {
    values: Vec<Option<serde_json::Value>>,
}

impl JsonProjection {
    pub(crate) fn new(values: Vec<Option<serde_json::Value>>) -> Self {
        Self { values }
    }

    pub(crate) fn values(&self) -> &[Option<serde_json::Value>] {
        &self.values
    }
}
