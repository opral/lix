use std::sync::Arc;

use crate::LixError;

use super::context::SessionContext;

/// Options for switching a session to another version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwitchVersionOptions {
    pub version_id: String,
}

/// Receipt returned after switching to another version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwitchVersionReceipt {
    pub version_id: String,
}

impl SessionContext {
    /// Returns a new session pinned to `version_id`.
    ///
    /// Switching is intentionally an in-memory selector change. It validates
    /// that the target version ref exists, then reuses the same engine services
    /// without writing changelog, live_state, tracked_state, or version_ref.
    pub async fn switch_version(
        &self,
        options: SwitchVersionOptions,
    ) -> Result<(SessionContext, SwitchVersionReceipt), LixError> {
        let head = self
            .version_ref
            .reader(Arc::clone(&self.backend))
            .load_head_commit_id(&options.version_id)
            .await?;
        if head.is_none() {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "cannot switch to missing version ref '{}'",
                    options.version_id
                ),
            ));
        }

        let session = SessionContext::new(
            options.version_id.clone(),
            Arc::clone(&self.backend),
            Arc::clone(&self.live_state),
            Arc::clone(&self.tracked_state),
            Arc::clone(&self.binary_cas),
            Arc::clone(&self.changelog),
            Arc::clone(&self.version_ref),
            Arc::clone(&self.schema_registry),
        );
        Ok((
            session,
            SwitchVersionReceipt {
                version_id: options.version_id,
            },
        ))
    }
}
