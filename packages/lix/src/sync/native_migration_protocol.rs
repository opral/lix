//! Explicit migration request. Selection remains authority-derived native data.
use crate::LixError;
use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct NativeMigrationMergeRequest {
    pub request: super::PartialMergeRequest,
    pub source_branch_id: String,
}
impl NativeMigrationMergeRequest {
    pub(crate) fn validate(&self) -> Result<(), LixError> {
        self.request.validate()?;
        crate::storage_codec::id_string::uuid_bytes_from_canonical(&self.source_branch_id)
            .ok_or_else(|| {
                LixError::new(
                    "LIX_MIGRATION_MERGE_SCOPE_UNSUPPORTED",
                    "migration source pin must be a canonical UUID",
                )
            })?;
        if self.source_branch_id == self.request.branch_id
            || self.source_branch_id == crate::GLOBAL_BRANCH_ID
        {
            return Err(LixError::new(
                "LIX_MIGRATION_MERGE_SCOPE_UNSUPPORTED",
                "migration source pin must be isolated",
            ));
        }
        Ok(())
    }
}
