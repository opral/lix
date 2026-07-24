use crate::LixError;
use crate::changelog::CommitId;
use crate::commit_graph::{CommitGraphCommit, CommitGraphReader};
use crate::common::validate_non_empty_identity_value;

use super::{BranchHead, BranchRefReader};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BranchOperation {
    CreateBranch,
    SwitchBranch,
    MergeBranch,
    MergeBranchPreview,
    CreateCheckpoint,
    LoadWorkspaceSelector,
}

impl BranchOperation {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::CreateBranch => "create_branch",
            Self::SwitchBranch => "switch_branch",
            Self::MergeBranch => "merge_branch",
            Self::MergeBranchPreview => "merge_branch_preview",
            Self::CreateCheckpoint => "create_checkpoint",
            Self::LoadWorkspaceSelector => "load_workspace_branch_id",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BranchReferenceRole {
    Source,
    Target,
    WorkspaceSelector,
    CommitSource,
}

impl BranchReferenceRole {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Target => "target",
            Self::WorkspaceSelector => "workspace_selector",
            Self::CommitSource => "commit_source",
        }
    }
}

/// Shared domain service for resolving public branch references.
///
/// Built-in branch schemas describe row shape. This service owns semantic
/// ref validation: non-empty ids, global sentinel handling, and missing refs.
pub(crate) struct BranchLifecycle<'a> {
    refs: &'a dyn BranchRefReader,
}

impl<'a> BranchLifecycle<'a> {
    pub(crate) fn new(refs: &'a dyn BranchRefReader) -> Self {
        Self { refs }
    }

    pub(crate) fn require_non_empty_id(
        branch_id: &str,
        operation: BranchOperation,
        role: BranchReferenceRole,
    ) -> Result<(), LixError> {
        require_non_empty_public_id("branch_id", branch_id, operation, role)
    }

    pub(crate) async fn require_existing_commit(
        commit_graph: &mut dyn CommitGraphReader,
        commit_id: CommitId,
        operation: BranchOperation,
        role: BranchReferenceRole,
    ) -> Result<CommitGraphCommit, LixError> {
        commit_graph
            .load_commit(&commit_id)
            .await?
            .ok_or_else(|| LixError::commit_not_found(commit_id, operation.label(), role.label()))
    }

    pub(crate) fn parse_commit_id(
        commit_id: &str,
        operation: BranchOperation,
        role: BranchReferenceRole,
    ) -> Result<CommitId, LixError> {
        require_non_empty_public_id("commit_id", commit_id, operation, role)?;
        CommitId::parse_lix(commit_id, "branch lifecycle commit_id")
    }

    pub(crate) async fn require_existing_ref(
        &self,
        branch_id: &str,
        operation: BranchOperation,
        role: BranchReferenceRole,
    ) -> Result<BranchHead, LixError> {
        Self::require_non_empty_id(branch_id, operation, role)?;
        self.require_existing_stored_ref(branch_id, operation, role)
            .await
    }

    pub(crate) async fn require_existing_commit_id(
        &self,
        branch_id: &str,
        operation: BranchOperation,
        role: BranchReferenceRole,
    ) -> Result<CommitId, LixError> {
        Ok(self
            .require_existing_ref(branch_id, operation, role)
            .await?
            .commit_id)
    }

    async fn require_existing_stored_ref(
        &self,
        branch_id: &str,
        operation: BranchOperation,
        role: BranchReferenceRole,
    ) -> Result<BranchHead, LixError> {
        self.refs
            .load_head(branch_id)
            .await?
            .ok_or_else(|| LixError::branch_not_found(branch_id, operation.label(), role.label()))
    }
}

fn require_non_empty_public_id(
    label: &str,
    value: &str,
    operation: BranchOperation,
    role: BranchReferenceRole,
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
    use crate::changelog::CommitId;

    #[tokio::test]
    async fn require_existing_ref_returns_head() {
        let reader = RowsBranchRefReader::new(vec![BranchHead {
            branch_id: "branch-a".to_string(),
            commit_id: CommitId::for_test_label("commit-a"),
        }]);
        let lifecycle = BranchLifecycle::new(&reader);

        let head = lifecycle
            .require_existing_ref(
                "branch-a",
                BranchOperation::SwitchBranch,
                BranchReferenceRole::Target,
            )
            .await
            .expect("branch should resolve");

        assert_eq!(head.commit_id, "commit-a");
    }

    #[tokio::test]
    async fn require_existing_ref_rejects_empty_id_as_invalid_param() {
        let reader = RowsBranchRefReader::new(Vec::new());
        let lifecycle = BranchLifecycle::new(&reader);

        let error = lifecycle
            .require_existing_ref(
                "",
                BranchOperation::SwitchBranch,
                BranchReferenceRole::Target,
            )
            .await
            .expect_err("empty branch id should be rejected before lookup");

        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
    }

    #[tokio::test]
    async fn require_existing_ref_reports_missing_branch() {
        let reader = RowsBranchRefReader::new(Vec::new());
        let lifecycle = BranchLifecycle::new(&reader);

        let error = lifecycle
            .require_existing_ref(
                "missing",
                BranchOperation::SwitchBranch,
                BranchReferenceRole::Target,
            )
            .await
            .expect_err("missing branch should be rejected");

        assert_eq!(error.code, LixError::CODE_BRANCH_NOT_FOUND);
    }

    struct RowsBranchRefReader {
        heads: Vec<BranchHead>,
    }

    impl RowsBranchRefReader {
        fn new(heads: Vec<BranchHead>) -> Self {
            Self { heads }
        }
    }

    #[async_trait]
    impl BranchRefReader for RowsBranchRefReader {
        async fn load_head(&self, branch_id: &str) -> Result<Option<BranchHead>, LixError> {
            Ok(self
                .heads
                .iter()
                .find(|head| head.branch_id == branch_id)
                .cloned())
        }

        async fn scan_heads(&self) -> Result<Vec<BranchHead>, LixError> {
            Ok(self.heads.clone())
        }
    }
}
