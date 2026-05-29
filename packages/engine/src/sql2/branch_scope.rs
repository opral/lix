use std::collections::BTreeSet;

use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::scalar::ScalarValue;

use crate::GLOBAL_BRANCH_ID;
use crate::LixError;
use crate::branch::BranchRefReader;

/// Branch scope requested by a SQL surface.
///
/// Active surfaces read through one session branch. By-branch surfaces either
/// read explicitly filtered branches or, without a branch predicate, enumerate
/// every visible branch scope before handing the request to live_state.
pub(crate) enum SqlBranchScope {
    Active(String),
    Explicit(Vec<String>),
    AllVisible,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BranchBinding {
    Active { branch_id: String },
    Explicit,
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

    pub(crate) fn explicit() -> Self {
        Self::Explicit
    }

    pub(crate) fn active_branch_id(&self) -> Option<&str> {
        match self {
            Self::Active { branch_id } => Some(branch_id),
            Self::Explicit => None,
        }
    }
}

pub(crate) fn resolve_write_branch_scope(
    explicit_global: Option<bool>,
    explicit_branch_id: Option<String>,
    fallback_branch_id: Option<&str>,
    action: &str,
    surface: &str,
) -> Result<WriteBranchScope, DataFusionError> {
    if explicit_global == Some(true) {
        if explicit_branch_id
            .as_deref()
            .is_some_and(|branch_id| branch_id != GLOBAL_BRANCH_ID)
        {
            return Err(DataFusionError::Execution(format!(
                "{surface} cannot set lixcol_global=true with non-global lixcol_branch_id"
            )));
        }
        return Ok(WriteBranchScope {
            branch_id: GLOBAL_BRANCH_ID.to_string(),
            global: true,
        });
    }

    let branch_id = explicit_branch_id
        .or_else(|| fallback_branch_id.map(ToOwned::to_owned))
        .ok_or_else(|| DataFusionError::Execution(format!("{action} requires lixcol_branch_id")))?;
    if explicit_global == Some(false) && branch_id == GLOBAL_BRANCH_ID {
        return Err(DataFusionError::Execution(format!(
            "{surface} cannot set lixcol_global=false with global lixcol_branch_id"
        )));
    }
    Ok(WriteBranchScope {
        global: explicit_global.unwrap_or(branch_id == GLOBAL_BRANCH_ID),
        branch_id,
    })
}

impl SqlBranchScope {
    pub(crate) fn from_provider(
        binding: &BranchBinding,
        requested_branch_ids: Vec<String>,
    ) -> Self {
        match binding {
            BranchBinding::Active { branch_id } => Self::Active(branch_id.clone()),
            BranchBinding::Explicit if requested_branch_ids.is_empty() => Self::AllVisible,
            BranchBinding::Explicit => Self::Explicit(requested_branch_ids),
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
        SqlBranchScope::Explicit(branch_ids) => {
            for branch_id in &branch_ids {
                if branch_id == GLOBAL_BRANCH_ID {
                    continue;
                }
                if branch_ref.load_head(branch_id).await?.is_none() {
                    return Err(LixError::branch_not_found(
                        branch_id.clone(),
                        "resolve SQL explicit branch scope",
                        "requested branch",
                    ));
                }
            }
            Ok(branch_ids)
        }
        SqlBranchScope::AllVisible => visible_branch_ids(branch_ref).await,
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

pub(crate) fn explicit_branch_ids_from_dml_filters(filters: &[Expr]) -> Vec<String> {
    filters
        .iter()
        .flat_map(branch_ids_from_filter)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn branch_ids_from_filter(expr: &Expr) -> Vec<String> {
    match expr {
        Expr::BinaryExpr(binary_expr) if binary_expr.op == Operator::And => {
            let mut values = branch_ids_from_filter(&binary_expr.left);
            values.extend(branch_ids_from_filter(&binary_expr.right));
            values
        }
        Expr::BinaryExpr(binary_expr) => branch_id_from_binary_filter(binary_expr)
            .map(|value| vec![value])
            .unwrap_or_default(),
        Expr::InList(in_list) => branch_ids_from_in_list_filter(in_list).unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn branch_id_from_binary_filter(binary_expr: &BinaryExpr) -> Option<String> {
    if binary_expr.op != Operator::Eq {
        return None;
    }

    branch_id_from_column_literal_filter(&binary_expr.left, &binary_expr.right)
        .or_else(|| branch_id_from_column_literal_filter(&binary_expr.right, &binary_expr.left))
}

fn branch_ids_from_in_list_filter(in_list: &InList) -> Option<Vec<String>> {
    if in_list.negated {
        return None;
    }
    let Expr::Column(column) = in_list.expr.as_ref() else {
        return None;
    };
    if column.name != "lixcol_branch_id" {
        return None;
    }

    let values = in_list
        .list
        .iter()
        .map(string_expr_literal)
        .collect::<Option<Vec<_>>>()?;
    if values.is_empty() {
        return None;
    }
    Some(values)
}

fn branch_id_from_column_literal_filter(column_expr: &Expr, literal_expr: &Expr) -> Option<String> {
    let Expr::Column(column) = column_expr else {
        return None;
    };
    if column.name != "lixcol_branch_id" {
        return None;
    }
    string_expr_literal(literal_expr)
}

fn string_expr_literal(expr: &Expr) -> Option<String> {
    let Expr::Literal(literal, _) = expr else {
        return None;
    };
    match literal {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => Some(value.clone()),
        _ => None,
    }
}

async fn visible_branch_ids(branch_ref: &dyn BranchRefReader) -> Result<Vec<String>, LixError> {
    let mut branch_ids = branch_ref
        .scan_heads()
        .await?
        .into_iter()
        .map(|head| head.branch_id)
        .collect::<BTreeSet<_>>();
    branch_ids.insert(GLOBAL_BRANCH_ID.to_string());
    Ok(branch_ids.into_iter().collect())
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

    #[tokio::test]
    async fn explicit_scope_keeps_requested_branches() {
        let branch_ref = RowsBranchRefReader::new(vec![BranchHead {
            branch_id: "branch-a".to_string(),
            commit_id: CommitId::for_test_label("commit-branch-a"),
        }]);
        let ids = resolve_provider_branch_ids(
            &branch_ref,
            &BranchBinding::explicit(),
            vec!["branch-a".to_string(), "global".to_string()],
        )
        .await
        .expect("scope should resolve");

        assert_eq!(ids, vec!["branch-a".to_string(), "global".to_string()]);
    }

    #[tokio::test]
    async fn explicit_scope_rejects_missing_branch_ref() {
        let branch_ref = RowsBranchRefReader::new(Vec::new());
        let error = resolve_provider_branch_ids(
            &branch_ref,
            &BranchBinding::explicit(),
            vec!["missing-branch".to_string()],
        )
        .await
        .expect_err("missing explicit branch should be rejected");

        assert_eq!(error.code, LixError::CODE_BRANCH_NOT_FOUND);
        assert!(
            error
                .message
                .contains("branch 'missing-branch' was not found")
        );
    }

    #[tokio::test]
    async fn all_visible_scope_loads_branch_refs_and_global() {
        let branch_ref = RowsBranchRefReader::new(vec![
            BranchHead {
                branch_id: "branch-b".to_string(),
                commit_id: CommitId::for_test_label("commit-branch-b"),
            },
            BranchHead {
                branch_id: "branch-a".to_string(),
                commit_id: CommitId::for_test_label("commit-branch-a"),
            },
        ]);
        let ids = resolve_provider_branch_ids(&branch_ref, &BranchBinding::explicit(), Vec::new())
            .await
            .expect("scope should resolve");

        assert_eq!(
            ids,
            vec![
                "branch-a".to_string(),
                "branch-b".to_string(),
                "global".to_string(),
            ]
        );
    }

    #[test]
    fn write_scope_uses_fallback_branch_when_branch_is_implicit() {
        let scope = resolve_write_branch_scope(
            None,
            None,
            Some("active-branch"),
            "INSERT into surface",
            "surface",
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
        let error = resolve_write_branch_scope(None, None, None, "INSERT into surface", "surface")
            .expect_err("missing branch should be rejected");

        assert!(
            error
                .to_string()
                .contains("INSERT into surface requires lixcol_branch_id")
        );
    }

    #[test]
    fn write_scope_derives_global_from_global_branch_id() {
        let scope = resolve_write_branch_scope(
            None,
            Some(GLOBAL_BRANCH_ID.to_string()),
            None,
            "INSERT into surface",
            "surface",
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

    #[test]
    fn write_scope_rejects_non_global_with_global_branch_id() {
        let error = resolve_write_branch_scope(
            Some(false),
            Some(GLOBAL_BRANCH_ID.to_string()),
            None,
            "INSERT into surface",
            "surface",
        )
        .expect_err("conflicting global/branch scope should be rejected");

        assert!(
            error
                .to_string()
                .contains("surface cannot set lixcol_global=false with global lixcol_branch_id")
        );
    }

    #[test]
    fn write_scope_rejects_global_with_non_global_branch_id() {
        let error = resolve_write_branch_scope(
            Some(true),
            Some("branch-a".to_string()),
            None,
            "INSERT into surface",
            "surface",
        )
        .expect_err("conflicting global/branch scope should be rejected");

        assert!(
            error
                .to_string()
                .contains("surface cannot set lixcol_global=true with non-global lixcol_branch_id")
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
