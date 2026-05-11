use crate::commit_graph::{CommitGraphCommit, CommitGraphReader};
use crate::common::validate_non_empty_identity_value;
use crate::LixError;

use super::{VersionHead, VersionRefReader};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VersionOperation {
    CreateVersion,
    SwitchVersion,
    MergeVersion,
    MergeVersionPreview,
    LoadWorkspaceSelector,
}

impl VersionOperation {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::CreateVersion => "create_version",
            Self::SwitchVersion => "switch_version",
            Self::MergeVersion => "merge_version",
            Self::MergeVersionPreview => "merge_version_preview",
            Self::LoadWorkspaceSelector => "load_workspace_version_id",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VersionReferenceRole {
    Source,
    Target,
    WorkspaceSelector,
    CommitSource,
}

impl VersionReferenceRole {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Target => "target",
            Self::WorkspaceSelector => "workspace_selector",
            Self::CommitSource => "commit_source",
        }
    }
}

/// Shared domain service for resolving public version references.
///
/// Built-in version schemas describe row shape. This service owns semantic
/// ref validation: non-empty ids, global sentinel handling, and missing refs.
pub(crate) struct VersionLifecycle<'a> {
    refs: &'a dyn VersionRefReader,
}

impl<'a> VersionLifecycle<'a> {
    pub(crate) fn new(refs: &'a dyn VersionRefReader) -> Self {
        Self { refs }
    }

    pub(crate) fn require_non_empty_id(
        version_id: &str,
        operation: VersionOperation,
        role: VersionReferenceRole,
    ) -> Result<(), LixError> {
        require_non_empty_public_id("version_id", version_id, operation, role)
    }

    pub(crate) async fn require_existing_commit(
        commit_graph: &mut dyn CommitGraphReader,
        commit_id: &str,
        operation: VersionOperation,
        role: VersionReferenceRole,
    ) -> Result<CommitGraphCommit, LixError> {
        require_non_empty_public_id("commit_id", commit_id, operation, role)?;
        commit_graph
            .load_commit(commit_id)
            .await?
            .ok_or_else(|| LixError::version_not_found(commit_id, operation.label(), role.label()))
    }

    pub(crate) async fn require_existing_ref(
        &self,
        version_id: &str,
        operation: VersionOperation,
        role: VersionReferenceRole,
    ) -> Result<VersionHead, LixError> {
        Self::require_non_empty_id(version_id, operation, role)?;
        self.require_existing_stored_ref(version_id, operation, role)
            .await
    }

    pub(crate) async fn require_existing_commit_id(
        &self,
        version_id: &str,
        operation: VersionOperation,
        role: VersionReferenceRole,
    ) -> Result<String, LixError> {
        Ok(self
            .require_existing_ref(version_id, operation, role)
            .await?
            .commit_id)
    }

    async fn require_existing_stored_ref(
        &self,
        version_id: &str,
        operation: VersionOperation,
        role: VersionReferenceRole,
    ) -> Result<VersionHead, LixError> {
        self.refs
            .load_head(version_id)
            .await?
            .ok_or_else(|| LixError::version_not_found(version_id, operation.label(), role.label()))
    }
}

fn require_non_empty_public_id(
    label: &str,
    value: &str,
    operation: VersionOperation,
    role: VersionReferenceRole,
) -> Result<(), LixError> {
    validate_non_empty_identity_value(label, value)
        .map(|_| ())
        .map_err(|_| {
            LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!(
                    "{} {} {label} must be non-empty",
                    operation.label(),
                    role.label()
                ),
            )
        })
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;

    use super::*;

    #[tokio::test]
    async fn require_existing_ref_returns_head() {
        let reader = RowsVersionRefReader::new(vec![VersionHead {
            version_id: "version-a".to_string(),
            commit_id: "commit-a".to_string(),
        }]);
        let lifecycle = VersionLifecycle::new(&reader);

        let head = lifecycle
            .require_existing_ref(
                "version-a",
                VersionOperation::SwitchVersion,
                VersionReferenceRole::Target,
            )
            .await
            .expect("version should resolve");

        assert_eq!(head.commit_id, "commit-a");
    }

    #[tokio::test]
    async fn require_existing_ref_rejects_empty_id_as_invalid_param() {
        let reader = RowsVersionRefReader::new(Vec::new());
        let lifecycle = VersionLifecycle::new(&reader);

        let error = lifecycle
            .require_existing_ref(
                "",
                VersionOperation::SwitchVersion,
                VersionReferenceRole::Target,
            )
            .await
            .expect_err("empty version id should be rejected before lookup");

        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
    }

    #[tokio::test]
    async fn require_existing_ref_reports_missing_version() {
        let reader = RowsVersionRefReader::new(Vec::new());
        let lifecycle = VersionLifecycle::new(&reader);

        let error = lifecycle
            .require_existing_ref(
                "missing",
                VersionOperation::SwitchVersion,
                VersionReferenceRole::Target,
            )
            .await
            .expect_err("missing version should be rejected");

        assert_eq!(error.code, LixError::CODE_VERSION_NOT_FOUND);
    }

    struct RowsVersionRefReader {
        heads: Vec<VersionHead>,
    }

    impl RowsVersionRefReader {
        fn new(heads: Vec<VersionHead>) -> Self {
            Self { heads }
        }
    }

    #[async_trait]
    impl VersionRefReader for RowsVersionRefReader {
        async fn load_head(&self, version_id: &str) -> Result<Option<VersionHead>, LixError> {
            Ok(self
                .heads
                .iter()
                .find(|head| head.version_id == version_id)
                .cloned())
        }

        async fn scan_heads(&self) -> Result<Vec<VersionHead>, LixError> {
            Ok(self.heads.clone())
        }
    }
}
