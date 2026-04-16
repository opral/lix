#[cfg(test)]
use async_trait::async_trait;

use std::collections::BTreeMap;

#[cfg(test)]
pub(crate) use crate::live_state::BatchRowRequest as BatchUntrackedRowRequest;
#[cfg(test)]
#[async_trait(?Send)]
pub trait UntrackedReadView {
    async fn load_exact_rows(
        &self,
        request: &BatchUntrackedRowRequest,
    ) -> Result<Vec<UntrackedRow>, crate::LixError>;

    async fn scan_rows(
        &self,
        request: &UntrackedScanRequest,
    ) -> Result<Vec<UntrackedRow>, crate::LixError>;
}
pub(crate) use crate::live_state::{
    ExactRowRequest as ExactUntrackedRowRequest, ScanRequest as UntrackedScanRequest,
};
#[cfg(test)]
use crate::LixBackend;
#[cfg(test)]
use crate::LixError;
use crate::Value;

/// Decoded live row from the untracked visibility lane.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct UntrackedRow {
    pub entity_id: String,
    pub schema_key: String,
    pub schema_version: String,
    pub file_id: Option<String>,
    pub version_id: String,
    pub global: bool,
    pub plugin_key: Option<String>,
    pub metadata: Option<String>,
    pub change_id: String,
    pub created_at: String,
    pub updated_at: String,
    pub values: BTreeMap<String, Value>,
}

impl UntrackedRow {
    pub fn property_text(&self, property_name: &str) -> Option<String> {
        self.values
            .get(property_name)
            .and_then(value_as_text)
            .map(ToString::to_string)
    }
}

fn value_as_text(value: &Value) -> Option<&str> {
    match value {
        Value::Text(value) => Some(value.as_str()),
        _ => None,
    }
}

#[cfg(test)]
#[async_trait(?Send)]
impl<T> UntrackedReadView for T
where
    T: LixBackend,
{
    async fn load_exact_rows(
        &self,
        request: &BatchUntrackedRowRequest,
    ) -> Result<Vec<UntrackedRow>, LixError> {
        let mut executor = self;
        super::read::load_exact_rows_with_executor(&mut executor, request).await
    }

    #[cfg(test)]
    async fn scan_rows(
        &self,
        request: &UntrackedScanRequest,
    ) -> Result<Vec<UntrackedRow>, LixError> {
        let mut executor = self;
        super::read::scan_rows_with_executor(&mut executor, request).await
    }
}
