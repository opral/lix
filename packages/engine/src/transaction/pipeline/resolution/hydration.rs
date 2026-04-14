use super::effective_state::resolve_exact_effective_state_row_with_pending_overlay;
use super::version_admin::load_version_admin_state_with_backend;
use crate::transaction::overlay::PendingOverlay;
use crate::transaction::pipeline::resolution::prepared_artifacts::{
    ExactEffectiveStateRow, ExactEffectiveStateRowRequest,
};
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixBackend, LixError};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone)]
pub(crate) struct HydratedVersionAdminRow {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) hidden: bool,
    pub(crate) commit_id: String,
    pub(crate) descriptor_change_id: Option<String>,
    pub(crate) has_local_head: bool,
}

pub(crate) struct PublicWriteHydrator<'a> {
    backend: &'a dyn LixBackend,
    pending_overlay: Option<&'a dyn PendingOverlay>,
    version_admin_rows: BTreeMap<String, Option<HydratedVersionAdminRow>>,
    validated_version_targets: BTreeSet<String>,
}

impl<'a> PublicWriteHydrator<'a> {
    pub(crate) fn new(
        backend: &'a dyn LixBackend,
        pending_overlay: Option<&'a dyn PendingOverlay>,
    ) -> Self {
        Self {
            backend,
            pending_overlay,
            version_admin_rows: BTreeMap::new(),
            validated_version_targets: BTreeSet::new(),
        }
    }

    pub(crate) fn backend(&self) -> &dyn LixBackend {
        self.backend
    }

    pub(crate) fn pending_overlay(&self) -> Option<&'a dyn PendingOverlay> {
        self.pending_overlay
    }

    pub(crate) async fn load_version_admin_row(
        &mut self,
        version_id: &str,
    ) -> Result<Option<HydratedVersionAdminRow>, LixError> {
        if let Some(row) = self.version_admin_rows.get(version_id) {
            return Ok(row.clone());
        }

        let row = self.fetch_version_admin_row(version_id).await?;
        self.version_admin_rows
            .insert(version_id.to_string(), row.clone());
        Ok(row)
    }

    async fn fetch_version_admin_row(
        &self,
        version_id: &str,
    ) -> Result<Option<HydratedVersionAdminRow>, LixError> {
        let Some(admin_state) =
            load_version_admin_state_with_backend(self.backend, version_id).await?
        else {
            return Ok(None);
        };
        let has_local_head = admin_state.head_commit_id.is_some();
        Ok(Some(HydratedVersionAdminRow {
            id: admin_state.version_id,
            name: admin_state.name,
            hidden: admin_state.hidden,
            commit_id: admin_state.head_commit_id.unwrap_or_default(),
            descriptor_change_id: admin_state.descriptor_change_id,
            has_local_head,
        }))
    }

    pub(crate) async fn validate_version_target(
        &mut self,
        version_id: &str,
    ) -> Result<(), LixError> {
        if version_id == GLOBAL_VERSION_ID
            || !self
                .validated_version_targets
                .insert(version_id.to_string())
        {
            return Ok(());
        }

        let Some(row) = self.load_version_admin_row(version_id).await? else {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("version with id '{version_id}' does not exist"),
            ));
        };
        if !row.has_local_head {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "public write invariant violation: version with id '{version_id}' exists but its local version head is missing"
                ),
            ));
        }

        Ok(())
    }

    pub(crate) async fn resolve_exact_effective_state_row(
        &self,
        request: &ExactEffectiveStateRowRequest,
    ) -> Result<Option<ExactEffectiveStateRow>, LixError> {
        resolve_exact_effective_state_row_with_pending_overlay(
            self.backend,
            request,
            self.pending_overlay,
        )
        .await
    }
}
