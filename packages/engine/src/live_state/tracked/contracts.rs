#[cfg(test)]
use async_trait::async_trait;

use std::collections::BTreeMap;

#[cfg(test)]
pub(crate) use crate::live_state::BatchRowRequest as BatchTrackedRowRequest;
#[cfg(test)]
#[async_trait(?Send)]
pub trait TrackedReadView {
    async fn load_exact_rows(
        &self,
        request: &BatchTrackedRowRequest,
    ) -> Result<Vec<TrackedRow>, crate::LixError>;

    async fn scan_rows(
        &self,
        request: &TrackedScanRequest,
    ) -> Result<Vec<TrackedRow>, crate::LixError>;
}
#[cfg(test)]
#[async_trait(?Send)]
pub trait TrackedTombstoneView {
    async fn scan_tombstones(
        &self,
        request: &TrackedScanRequest,
    ) -> Result<Vec<TrackedTombstoneMarker>, crate::LixError>;
}
pub(crate) use crate::live_state::{
    ExactRowRequest as ExactTrackedRowRequest, ScanRequest as TrackedScanRequest,
};
#[cfg(test)]
use crate::LixBackend;
#[cfg(test)]
use crate::LixError;
use crate::Value;

/// Decoded tracked live row.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TrackedRow {
    pub entity_id: String,
    pub schema_key: String,
    pub schema_version: String,
    pub file_id: Option<String>,
    pub version_id: String,
    pub global: bool,
    pub plugin_key: Option<String>,
    pub metadata: Option<String>,
    pub change_id: Option<String>,
    pub writer_key: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub values: BTreeMap<String, Value>,
}

#[cfg(test)]
impl TrackedRow {
    pub fn property_text(&self, property_name: &str) -> Option<String> {
        self.values
            .get(property_name)
            .and_then(value_as_text)
            .map(ToString::to_string)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TrackedTombstoneMarker {
    pub entity_id: String,
    pub schema_key: String,
    pub file_id: Option<String>,
    pub version_id: String,
    pub global: bool,
    pub schema_version: Option<String>,
    pub plugin_key: Option<String>,
    pub metadata: Option<String>,
    pub writer_key: Option<String>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
    pub change_id: Option<String>,
}

#[cfg(test)]
fn value_as_text(value: &Value) -> Option<&str> {
    match value {
        Value::Text(value) => Some(value.as_str()),
        _ => None,
    }
}

#[cfg(test)]
#[async_trait(?Send)]
impl<T> TrackedReadView for T
where
    T: LixBackend,
{
    async fn load_exact_rows(
        &self,
        request: &BatchTrackedRowRequest,
    ) -> Result<Vec<TrackedRow>, LixError> {
        let mut executor = self;
        super::read::load_exact_rows_with_executor(&mut executor, request).await
    }

    #[cfg(test)]
    async fn scan_rows(&self, request: &TrackedScanRequest) -> Result<Vec<TrackedRow>, LixError> {
        let mut executor = self;
        super::read::scan_rows_with_executor(&mut executor, request).await
    }
}

#[cfg(test)]
#[async_trait(?Send)]
impl<T> TrackedTombstoneView for T
where
    T: LixBackend,
{
    async fn scan_tombstones(
        &self,
        request: &TrackedScanRequest,
    ) -> Result<Vec<TrackedTombstoneMarker>, LixError> {
        let mut executor = self;
        super::read::scan_tombstones_with_executor(&mut executor, request).await
    }
}
