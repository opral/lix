use datafusion::error::DataFusionError;

use crate::GLOBAL_BRANCH_ID;
use crate::LixError;
use crate::branch::BranchRefReader;

pub(crate) enum SqlBranchScope {
    Active(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BranchBinding {
    Active { branch_id: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WriteBranchScope {
    pub(crate) branch_id: String,
    pub(crate) global: bool,
}

impl BranchBinding {
    pub(crate) fn active(branch_id: impl Into<String>) -> Self {
        Self::Active {
            branch_id: branch_id.into(),
        }
    }

    pub(crate) fn active_branch_id(&self) -> Option<&str> {
        match self {
            Self::Active { branch_id } => Some(branch_id),
        }
    }
}

pub(crate) fn resolve_write_branch_scope(
    explicit_global: Option<bool>,
    fallback_branch_id: Option<&str>,
    action: &str,
) -> Result<WriteBranchScope, DataFusionError> {
    if explicit_global == Some(true) {
        return Ok(WriteBranchScope {
            branch_id: GLOBAL_BRANCH_ID.to_string(),
            global: true,
        });
    }

    let branch_id = fallback_branch_id
        .map(ToOwned::to_owned)
        .ok_or_else(|| DataFusionError::Execution(format!("{action} requires an active branch")))?;
    Ok(WriteBranchScope {
        global: explicit_global.unwrap_or(branch_id == GLOBAL_BRANCH_ID),
        branch_id,
    })
}

impl SqlBranchScope {
    pub(crate) fn from_provider(
        binding: &BranchBinding,
        _requested_branch_ids: Vec<String>,
    ) -> Self {
        match binding {
            BranchBinding::Active { branch_id } => Self::Active(branch_id.clone()),
        }
    }
}

pub(crate) async fn resolve_sql_branch_scope(
    branch_ref: &dyn BranchRefReader,
    scope: SqlBranchScope,
) -> Result<Vec<String>, LixError> {
    match scope {
        SqlBranchScope::Active(branch_id) => {
            if branch_ref.load_head(&branch_id).await?.is_none() {
                return Err(LixError::branch_not_found(
                    branch_id,
                    "resolve SQL active branch scope",
                    "active branch",
                ));
            }
            Ok(vec![branch_id])
        }
    }
}

pub(crate) async fn resolve_provider_branch_ids(
    branch_ref: &dyn BranchRefReader,
    binding: &BranchBinding,
    requested_branch_ids: Vec<String>,
) -> Result<Vec<String>, LixError> {
    resolve_sql_branch_scope(
        branch_ref,
        SqlBranchScope::from_provider(binding, requested_branch_ids),
    )
    .await
}


#[cfg(test)]
mod tests {
    use async_trait::async_trait;

    use super::*;
    use crate::branch::BranchHead;
    use crate::changelog::CommitId;

    #[tokio::test]
    async fn active_scope_uses_session_branch() {
        let branch_ref = RowsBranchRefReader::new(vec![BranchHead {
            branch_id: "main".to_string(),
            commit_id: CommitId::for_test_label("commit-main"),
        }]);
        let ids =
            resolve_provider_branch_ids(&branch_ref, &BranchBinding::active("main"), Vec::new())
                .await
                .expect("scope should resolve");

        assert_eq!(ids, vec!["main".to_string()]);
    }

    #[tokio::test]
    async fn active_scope_rejects_missing_branch_ref() {
        let branch_ref = RowsBranchRefReader::new(Vec::new());
        let error =
            resolve_provider_branch_ids(&branch_ref, &BranchBinding::active("main"), Vec::new())
                .await
                .expect_err("missing active branch should be rejected");

        assert_eq!(error.code, LixError::CODE_BRANCH_NOT_FOUND);
        assert!(error.message.contains("branch 'main' was not found"));
    }




    #[test]
    fn write_scope_uses_fallback_branch_when_branch_is_implicit() {
        let scope = resolve_write_branch_scope(
            None,
            Some("active-branch"),
            "INSERT into surface",
        )
        .expect("scope should resolve");

        assert_eq!(
            scope,
            WriteBranchScope {
                branch_id: "active-branch".to_string(),
                global: false,
            }
        );
    }

    #[test]
    fn write_scope_requires_branch_without_fallback() {
        let error = resolve_write_branch_scope(None, None, "INSERT into surface")
            .expect_err("missing branch should be rejected");

        assert!(
            error
                .to_string()
                .contains("INSERT into surface requires an active branch")
        );
    }

    #[test]
    fn write_scope_derives_global_from_global_branch_id() {
        let scope = resolve_write_branch_scope(
            None,
            Some(GLOBAL_BRANCH_ID),
            "INSERT into surface",
        )
        .expect("scope should resolve");

        assert_eq!(
            scope,
            WriteBranchScope {
                branch_id: GLOBAL_BRANCH_ID.to_string(),
                global: true,
            }
        );
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
