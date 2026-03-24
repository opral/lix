use std::collections::BTreeMap;

use async_trait::async_trait;

pub use crate::live_state::shared::query::{
    BatchRowRequest as BatchUntrackedRowRequest, ExactRowRequest as ExactUntrackedRowRequest,
    ScanRequest as UntrackedScanRequest,
};
use crate::{LixBackend, LixError, LixTransaction, Value};

/// Decoded untracked/helper live row.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct UntrackedRow {
    pub entity_id: String,
    pub schema_key: String,
    pub schema_version: String,
    pub file_id: String,
    pub version_id: String,
    pub global: bool,
    pub plugin_key: String,
    pub metadata: Option<String>,
    pub writer_key: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub values: BTreeMap<String, Value>,
}

impl UntrackedRow {
    pub fn property_text(&self, property_name: &str) -> Option<String> {
        self.values
            .get(property_name)
            .and_then(crate::live_state::storage::text_from_value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum UntrackedWriteOperation {
    Upsert,
    Delete,
}

/// Single untracked/helper write operation.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct UntrackedWriteRow {
    pub entity_id: String,
    pub schema_key: String,
    pub schema_version: String,
    pub file_id: String,
    pub version_id: String,
    pub global: bool,
    pub plugin_key: String,
    pub metadata: Option<String>,
    pub writer_key: Option<String>,
    pub snapshot_content: Option<String>,
    pub created_at: Option<String>,
    pub updated_at: String,
    pub operation: UntrackedWriteOperation,
}

pub type UntrackedWriteBatch = Vec<UntrackedWriteRow>;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ActiveVersionRow {
    pub entity_id: String,
    pub version_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct VersionRefRow {
    pub version_id: String,
    pub commit_id: String,
}

#[async_trait(?Send)]
pub trait UntrackedReadView {
    async fn load_exact_row(
        &self,
        request: &ExactUntrackedRowRequest,
    ) -> Result<Option<UntrackedRow>, LixError>;

    async fn load_exact_rows(
        &self,
        request: &BatchUntrackedRowRequest,
    ) -> Result<Vec<UntrackedRow>, LixError>;

    async fn scan_rows(
        &self,
        request: &UntrackedScanRequest,
    ) -> Result<Vec<UntrackedRow>, LixError>;
}

#[async_trait(?Send)]
impl<T> UntrackedReadView for T
where
    T: LixBackend,
{
    async fn load_exact_row(
        &self,
        request: &ExactUntrackedRowRequest,
    ) -> Result<Option<UntrackedRow>, LixError> {
        let mut executor = self;
        super::read::load_exact_row_with_executor(&mut executor, request).await
    }

    async fn load_exact_rows(
        &self,
        request: &BatchUntrackedRowRequest,
    ) -> Result<Vec<UntrackedRow>, LixError> {
        let mut executor = self;
        super::read::load_exact_rows_with_executor(&mut executor, request).await
    }

    async fn scan_rows(
        &self,
        request: &UntrackedScanRequest,
    ) -> Result<Vec<UntrackedRow>, LixError> {
        let mut executor = self;
        super::read::scan_rows_with_executor(&mut executor, request).await
    }
}

#[async_trait(?Send)]
pub trait UntrackedWriteParticipant {
    async fn apply_write_batch(&mut self, batch: &[UntrackedWriteRow]) -> Result<(), LixError>;
}

#[async_trait(?Send)]
impl<T> UntrackedWriteParticipant for T
where
    T: LixTransaction,
{
    async fn apply_write_batch(&mut self, batch: &[UntrackedWriteRow]) -> Result<(), LixError> {
        super::write::apply_write_batch_in_transaction(self, batch).await
    }
}

#[async_trait(?Send)]
impl UntrackedWriteParticipant for dyn LixTransaction + '_ {
    async fn apply_write_batch(&mut self, batch: &[UntrackedWriteRow]) -> Result<(), LixError> {
        super::write::apply_write_batch_in_transaction(self, batch).await
    }
}
