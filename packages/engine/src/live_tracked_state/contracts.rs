use std::collections::BTreeMap;

use async_trait::async_trait;

use crate::constraints::ScanConstraint;
use crate::{LixBackend, LixError, LixTransaction, Value};

/// Point lookup by full key.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ExactTrackedRowRequest {
    pub schema_key: String,
    pub version_id: String,
    pub entity_id: String,
    pub file_id: Option<String>,
}

/// Batch point lookup by entity IDs.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub struct BatchTrackedRowRequest {
    pub schema_key: String,
    pub version_id: String,
    pub entity_ids: Vec<String>,
    pub file_id: Option<String>,
}

/// Bounded scan with structured constraints.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
pub struct TrackedScanRequest {
    pub schema_key: String,
    pub version_id: String,
    #[serde(default)]
    pub constraints: Vec<ScanConstraint>,
    #[serde(default)]
    pub required_columns: Vec<String>,
}

/// Decoded tracked live row.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TrackedRow {
    pub entity_id: String,
    pub schema_key: String,
    pub schema_version: String,
    pub file_id: String,
    pub version_id: String,
    pub global: bool,
    pub plugin_key: String,
    pub metadata: Option<String>,
    pub change_id: Option<String>,
    pub writer_key: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub values: BTreeMap<String, Value>,
}

impl TrackedRow {
    pub fn property_text(&self, property_name: &str) -> Option<String> {
        self.values
            .get(property_name)
            .and_then(super::shared::text_from_value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TrackedWriteOperation {
    Upsert,
    Tombstone,
}

/// Single tracked live-state write operation.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TrackedWriteRow {
    pub entity_id: String,
    pub schema_key: String,
    pub schema_version: String,
    pub file_id: String,
    pub version_id: String,
    pub global: bool,
    pub plugin_key: String,
    pub metadata: Option<String>,
    pub change_id: String,
    pub writer_key: Option<String>,
    pub snapshot_content: Option<String>,
    pub created_at: Option<String>,
    pub updated_at: String,
    pub operation: TrackedWriteOperation,
}

pub type TrackedWriteBatch = Vec<TrackedWriteRow>;

#[async_trait(?Send)]
pub trait TrackedReadView {
    async fn load_exact_row(
        &self,
        request: &ExactTrackedRowRequest,
    ) -> Result<Option<TrackedRow>, LixError>;

    async fn load_exact_rows(
        &self,
        request: &BatchTrackedRowRequest,
    ) -> Result<Vec<TrackedRow>, LixError>;

    async fn scan_rows(&self, request: &TrackedScanRequest) -> Result<Vec<TrackedRow>, LixError>;
}

#[async_trait(?Send)]
impl<T> TrackedReadView for T
where
    T: LixBackend,
{
    async fn load_exact_row(
        &self,
        request: &ExactTrackedRowRequest,
    ) -> Result<Option<TrackedRow>, LixError> {
        let mut executor = self;
        super::read::load_exact_row_with_executor(&mut executor, request).await
    }

    async fn load_exact_rows(
        &self,
        request: &BatchTrackedRowRequest,
    ) -> Result<Vec<TrackedRow>, LixError> {
        let mut executor = self;
        super::read::load_exact_rows_with_executor(&mut executor, request).await
    }

    async fn scan_rows(&self, request: &TrackedScanRequest) -> Result<Vec<TrackedRow>, LixError> {
        let mut executor = self;
        super::read::scan_rows_with_executor(&mut executor, request).await
    }
}

#[async_trait(?Send)]
pub trait TrackedWriteParticipant {
    async fn apply_write_batch(&mut self, batch: &[TrackedWriteRow]) -> Result<(), LixError>;
}

#[async_trait(?Send)]
impl<T> TrackedWriteParticipant for T
where
    T: LixTransaction,
{
    async fn apply_write_batch(&mut self, batch: &[TrackedWriteRow]) -> Result<(), LixError> {
        super::write::apply_write_batch_in_transaction(self, batch).await
    }
}
